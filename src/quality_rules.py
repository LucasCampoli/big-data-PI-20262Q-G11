"""Quality rules for usage events, as specified in docs/design.md §5.7.

The policy is impute-first, quarantine-last: a row is rejected only when the data contradicts
itself or cannot be placed at all. A row that is merely incomplete but unambiguous is repaired and
flagged, because quarantining those would have discarded 4.92% of all cost (see DECISIONS.md D8).

`notebooks/01_landing_exploration.ipynb` inlines the same rules rather than importing this module,
so that it stays runnable in Colab with no repo on the path. Delivery 2 unifies the two once the
package is installable; until then this module is the canonical definition and
`tests/test_quality_rules.py` is what pins it.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

#: `metric` determines `unit` 1:1 in the data, with zero contradictions, which is what makes
#: imputing a missing `unit` a derivation rather than a guess.
UNIT_BY_METRIC = {
    "requests": "count",
    "cpu_hours": "hours",
    "storage_gb_hours": "gb_hours",
}

#: One explicit schema for both event layouts (DECISIONS.md D1). `carbon_kg` and `genai_tokens`
#: are nullable so v1 rows load with them null; `value` is a string so a malformed measurement
#: survives the read and can be quarantined instead of silently becoming null (D2).
USAGE_EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType(),    nullable=False),
    StructField("timestamp",          TimestampType(), nullable=False),
    StructField("org_id",             StringType(),    nullable=False),
    StructField("resource_id",        StringType(),    nullable=False),
    StructField("service",            StringType(),    nullable=False),
    StructField("region",             StringType(),    nullable=False),
    StructField("metric",             StringType(),    nullable=False),
    StructField("value",              StringType(),    nullable=True),
    StructField("unit",               StringType(),    nullable=True),
    StructField("cost_usd_increment", DoubleType(),    nullable=True),
    StructField("schema_version",     IntegerType(),   nullable=False),
    StructField("carbon_kg",          DoubleType(),    nullable=True),
    StructField("genai_tokens",       LongType(),      nullable=True),
])

QUARANTINE = "quarantine"
PASSED = "passed"


def try_double(column: str):
    """Cast to double, yielding null instead of raising on a malformed value.

    Spark 4 enables ANSI SQL mode by default, so a plain `cast` *throws* on bad input rather than
    returning null. The whole quarantine policy depends on a failed cast being observable as a
    null, so every numeric cast in this project goes through `try_cast`. Written as a SQL
    expression rather than `Column.try_cast` so it also works on the Spark 3.5 in Colab.
    """
    return F.expr(f"try_cast({column} AS DOUBLE)")


def _expected_unit():
    return F.create_map(*[
        lit for metric, unit in UNIT_BY_METRIC.items()
        for lit in (F.lit(metric), F.lit(unit))
    ])


def rejecting_rules(known_org_ids=None, known_resource_ids=None):
    """The rules that send a row to Quarantine, as (name, predicate) pairs.

    Referential rules are included only when the corresponding dimension keys are supplied, so the
    function is usable before the dimensions exist.
    """
    expected = _expected_unit()[F.col("metric")]
    rules = [
        ("event_id_null", F.col("event_id").isNull()),
        ("metric_unknown", ~F.col("metric").isin(list(UNIT_BY_METRIC))),
        ("timestamp_unparseable", F.col("timestamp").isNull()),
        ("unit_contradicts_metric", F.col("unit").isNotNull() & (F.col("unit") != expected)),
        ("value_not_castable",
         F.col("value").isNotNull() & try_double("value").isNull()),
    ]
    if known_org_ids is not None:
        rules.append(("org_id_unknown", ~F.col("org_id").isin(list(known_org_ids))))
    if known_resource_ids is not None:
        rules.append(("resource_id_unknown", ~F.col("resource_id").isin(list(known_resource_ids))))
    return rules


def classify(df: DataFrame, known_org_ids=None, known_resource_ids=None) -> DataFrame:
    """Add the quality verdict and the repair columns to a Bronze-shaped event frame.

    Adds `dq_status` ("passed" / "quarantine"), `dq_rule` (the first rule violated, else null),
    `unit_imputed`, `value_double` and `cost_anomaly_flag`. Nothing is dropped — splitting the
    frame is the caller's job, so both sides of the split come from one pass.
    """
    rules = rejecting_rules(known_org_ids, known_resource_ids)

    # First violated rule wins, so a row carries one reason rather than a list.
    rule_col = F.lit(None).cast("string")
    for name, predicate in reversed(rules):
        rule_col = F.when(predicate, F.lit(name)).otherwise(rule_col)

    expected = _expected_unit()[F.col("metric")]
    return (
        df.withColumn("dq_rule", rule_col)
          .withColumn("dq_status", F.when(F.col("dq_rule").isNotNull(), F.lit(QUARANTINE))
                                    .otherwise(F.lit(PASSED)))
          # Repairs apply to rows that passed; a quarantined row keeps its original values.
          .withColumn("unit_imputed",
                      (F.col("dq_status") == PASSED) & F.col("unit").isNull()
                      & F.col("metric").isin(list(UNIT_BY_METRIC)))
          .withColumn("unit", F.when(F.col("unit_imputed"), expected).otherwise(F.col("unit")))
          # A null value stays null: it contributes nothing, not zero, to a usage sum (D9).
          .withColumn("value_double", try_double("value"))
          .withColumn("cost_anomaly_flag", F.coalesce(F.col("cost_usd_increment") < -0.01,
                                                      F.lit(False)))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop repeated `event_id`s. Duplicates come from re-runs, not from the source (D11)."""
    return df.dropDuplicates(["event_id"])
