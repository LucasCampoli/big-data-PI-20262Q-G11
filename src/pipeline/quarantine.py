"""Quarantine rows share one layout across sources and stages:
entity, rule_name, raw_record, source_file, quarantined_ts, run_id, partitioned by quarantine_date.

Tables, one writer each (a streaming sink owns its directory, so batch writers never share it):
  quarantine/usage_events_parse   streaming parse failures, append, own checkpoint
  quarantine/masters_parse        CSV parse failures in the batch load, append, guarded by the registry
  quarantine/usage_events_rules   Silver rule violations on events, rebuilt with Silver
  quarantine/masters_rules        Silver rule violations on masters, rebuilt with Silver
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

COLUMNS = ["entity", "rule_name", "raw_record", "source_file", "quarantined_ts", "run_id",
           "quarantine_date"]


def shape(df: DataFrame, entity: str, rule: Column, raw: Column, run_id: str, ts) -> DataFrame:
    quarantined_ts = F.lit(ts).cast("timestamp") if ts is not None else F.current_timestamp()
    return df.select(
        F.lit(entity).alias("entity"),
        rule.alias("rule_name"),
        raw.alias("raw_record"),
        F.col("source_file"),
        quarantined_ts.alias("quarantined_ts"),
        F.lit(run_id).alias("run_id"),
    ).withColumn("quarantine_date", F.to_date("quarantined_ts"))


def raw_json(columns: list) -> Column:
    """The row as it arrived in Bronze, so a reviewer sees the original values, not repairs."""
    return F.to_json(F.struct(*[F.col(c) for c in columns]))
