"""Quality rules, as specified in docs/design.md §5.2 (events: D6, masters: D7).

The policy is impute first, quarantine last: a row is rejected only when it contradicts itself or
cannot be placed at all. A row that is incomplete but unambiguous is repaired and flagged, because
quarantining those would discard 4.92% of all cost (D6).

The notebook `01_landing_exploration.ipynb` measured the baselines with an inline copy of these
predicates so that it runs in Colab without the repo; this module is the definition the pipeline
uses and `tests/test_quality_rules.py` pins.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# metric determines unit 1:1 in the data with zero contradictions, which makes imputing a missing
# unit a derivation rather than a guess.
UNIT_BY_METRIC = {
    "requests": "count",
    "cpu_hours": "hours",
    "storage_gb_hours": "gb_hours",
}

NEGATIVE_COST_TOLERANCE = -0.01   # cost_usd_increment below this is flagged, never dropped
CSAT_RANGE = (1, 5)               # our assumption, design §5.2
NPS_RANGE = (-100, 100)           # aggregate NPS scale, design §5.2

QUARANTINE = "quarantine"
PASSED = "passed"


def try_double(column: str) -> Column:
    """Cast to double, yielding null instead of raising on a malformed value.

    Spark 4 runs with ANSI SQL mode on, so a plain cast throws on bad input instead of returning
    null and would stop the job instead of sending the row to Quarantine (D2). Written as a SQL
    expression rather than Column.try_cast so it also runs on the Spark 3.5 in Colab.
    """
    return F.expr(f"try_cast(`{column}` AS DOUBLE)")


def expected_unit() -> Column:
    pairs = [lit for metric, unit in UNIT_BY_METRIC.items() for lit in (F.lit(metric), F.lit(unit))]
    return F.create_map(*pairs)[F.col("metric")]


def out_of_range(column: str, bounds: tuple) -> Column:
    low, high = bounds
    return F.col(column).isNotNull() & ((F.col(column) < low) | (F.col(column) > high))


def attach_references(events: DataFrame, orgs: DataFrame, resources: DataFrame) -> DataFrame:
    """Left-join the dimensions an event points to.

    The joins do two jobs: they enrich the event with organization and resource attributes, and a
    missing match is what the referential rules read. Both dimensions are a few hundred rows, so
    they are broadcast and the event side never shuffles.
    """
    org_side = orgs.select(
        "org_id",
        F.lit(True).alias("_org_found"),
        F.col("industry").alias("org_industry"),
        F.col("plan_tier").alias("org_plan_tier"),
        F.col("is_enterprise").alias("org_is_enterprise"),
    )
    resource_side = resources.select(
        "resource_id",
        F.lit(True).alias("_resource_found"),
        F.col("org_id").alias("resource_org_id"),
        F.col("state").alias("resource_state"),
    )
    return (events
            .join(F.broadcast(org_side), "org_id", "left")
            .join(F.broadcast(resource_side), "resource_id", "left"))


def rejecting_rules() -> list[tuple[str, Column]]:
    """The rules that send an event to Quarantine, as (name, predicate) pairs, in priority order.

    The referential rules read the columns that attach_references adds.
    """
    unit_expected = expected_unit()
    return [
        ("event_id_null", F.col("event_id").isNull()),
        ("timestamp_unparseable", F.col("timestamp").isNull()),
        ("metric_unknown", F.col("metric").isNull() | ~F.col("metric").isin(list(UNIT_BY_METRIC))),
        ("unit_contradicts_metric", F.col("unit").isNotNull() & (F.col("unit") != unit_expected)),
        ("value_not_castable", F.col("value").isNotNull() & try_double("value").isNull()),
        ("org_id_unknown", F.col("_org_found").isNull()),
        ("resource_id_unknown", F.col("_resource_found").isNull()),
        # The resource exists but belongs to another organization: two keys contradict each other.
        ("resource_org_mismatch", F.col("resource_org_id") != F.col("org_id")),
    ]


def classify(df: DataFrame) -> DataFrame:
    """Add the verdict and the repair columns to a Bronze-shaped event frame with references.

    Adds rule_name (the first rule violated, else null), dq_status (passed / quarantine),
    unit_imputed, value_double and cost_anomaly_flag. Nothing is dropped: the caller splits the
    frame, so both sides come from the same pass.
    """
    rule = F.lit(None).cast("string")
    for name, predicate in reversed(rejecting_rules()):
        rule = F.when(predicate, F.lit(name)).otherwise(rule)

    return (
        df.withColumn("rule_name", rule)
          .withColumn("dq_status", F.when(F.col("rule_name").isNotNull(), F.lit(QUARANTINE))
                                    .otherwise(F.lit(PASSED)))
          # Repairs apply to rows that passed; a quarantined row keeps what arrived.
          .withColumn("unit_imputed", (F.col("dq_status") == PASSED) & F.col("unit").isNull())
          .withColumn("unit", F.when(F.col("unit_imputed"), expected_unit()).otherwise(F.col("unit")))
          # A null value stays null: it contributes nothing, not zero, to a usage sum (D6).
          .withColumn("value_double", try_double("value"))
          .withColumn("cost_anomaly_flag",
                      F.coalesce(F.col("cost_usd_increment") < NEGATIVE_COST_TOLERANCE, F.lit(False)))
    )


def deduplicate(df: DataFrame, key: list, order: list) -> DataFrame:
    """Keep one row per key, the first by `order`. Duplicates come from re-runs, not the source (D8).

    A window instead of dropDuplicates so the surviving row is chosen, not arbitrary.
    """
    w = Window.partitionBy(*key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
