"""
Sections B + D - RAW (CSV) -> CURATED (Parquet partitioned by event_date).

  * explicit schema (no inferSchema)
  * cleaning: required fields, valid event types, price > 0, exact duplicates
  * nulls in brand / category_code -> 'unknown'
  * derived columns: event_date, hour, day_of_week, cat_l1, cat_l2
  * written with mode('overwrite') -> running it twice gives the same result

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/processing/etl.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone, local_csv  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (StructType, StructField, StringType, LongType,  # noqa: E402
                               DoubleType, TimestampType)

RAW_SCHEMA = StructType([
    StructField("event_time",    TimestampType(), True),
    StructField("event_type",    StringType(),    True),
    StructField("product_id",    LongType(),      True),
    StructField("category_id",   LongType(),      True),
    StructField("category_code", StringType(),    True),
    StructField("brand",         StringType(),    True),
    StructField("price",         DoubleType(),    True),
    StructField("user_id",       LongType(),      True),
    StructField("user_session",  StringType(),    True),
])


def read_raw(spark, cfg):
    raw_file = f"{zone(cfg, 'raw')}/{os.path.basename(local_csv(cfg))}"
    return (spark.read
            .option("header", True)
            .option("timestampFormat", cfg["dataset"]["csv_timestamp_format"])
            .option("mode", "PERMISSIVE")          # unparseable values become null
            .schema(RAW_SCHEMA)
            .csv(raw_file))


def clean(df, cfg):
    valid_types = cfg["dataset"]["valid_event_types"]
    category = F.split(F.col("category_code"), r"\.")
    return (df
            .dropna(subset=["event_time", "event_type", "product_id", "user_id", "user_session"])
            .filter(F.col("event_type").isin(valid_types))
            .filter(F.col("price") > 0)
            .dropDuplicates()
            .withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
            .withColumn("category_code", F.coalesce(F.col("category_code"), F.lit("unknown")))
            # F.get returns NULL for a missing index (getItem fails in Spark 4 / ANSI mode)
            .withColumn("cat_l1", F.get(category, 0))
            .withColumn("cat_l2", F.coalesce(F.get(category, 1), F.lit("unknown")))
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("hour", F.hour("event_time"))
            .withColumn("day_of_week", F.dayofweek("event_time")))   # 1 = Sunday ... 7 = Saturday


def main():
    cfg = load_config()
    spark = get_spark(cfg, "02-etl-raw-to-curated")
    part_col = cfg["dataset"]["partition_column"]
    out = zone(cfg, "curated")

    t0 = time.time()
    raw = read_raw(spark, cfg)
    curated = clean(raw, cfg)

    (curated.write
     .mode("overwrite")
     .partitionBy(part_col)
     .parquet(out))
    elapsed = time.time() - t0

    n_raw = raw.count()
    n_cur = spark.read.parquet(out).count()
    print(f"[etl] raw rows     : {n_raw:,}")
    print(f"[etl] curated rows : {n_cur:,}  (removed {n_raw - n_cur:,})")
    print(f"[etl] written to   : {out}  partitioned by {part_col}")
    print(f"[etl] write time   : {elapsed:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()