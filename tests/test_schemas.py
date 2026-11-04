"""The explicit schemas read the committed sample without a single corrupt record (D1)."""
from pipeline.config import REPO_ROOT
from pipeline.schemas import CORRUPT_RECORD, MASTER_SCHEMAS, USAGE_EVENT_SCHEMA, with_corrupt_column

SAMPLE = REPO_ROOT / "data" / "sample"


def test_events_v1_and_v2_share_one_schema(spark):
    df = (spark.read.schema(with_corrupt_column(USAGE_EVENT_SCHEMA))
          .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
          .json(str(SAMPLE / "usage_events_stream")))
    df.cache()
    assert df.filter(df[CORRUPT_RECORD].isNotNull()).count() == 0
    versions = {r.schema_version for r in df.select("schema_version").distinct().collect()}
    assert versions == {1, 2}
    # v1 rows carry the v2-only fields as null, never as a parse failure.
    assert df.filter((df.schema_version == 1) & df.carbon_kg.isNotNull()).count() == 0


def test_every_master_header_matches_its_schema(spark):
    for name, schema in MASTER_SCHEMAS.items():
        df = (spark.read.schema(with_corrupt_column(schema))
              .options(header=True, enforceSchema=False, escape='"',
                       columnNameOfCorruptRecord=CORRUPT_RECORD)
              .csv(str(SAMPLE / f"{name}.csv")))
        df.cache()
        assert df.count() > 0, name
        assert df.filter(df[CORRUPT_RECORD].isNotNull()).count() == 0, name
