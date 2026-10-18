"""
Section B - Format comparison: CSV (raw) vs Parquet (curated).

Measures:
  * size in HDFS (logical, and on disk including replication)
  * read time of the same two queries on each format:
      - count(*)               (full scan for CSV; Parquet answers from file metadata)
      - avg(price) by event_type (only 2 columns: Parquet reads just those)

Each query runs twice; the first run warms up the JVM, the second is the one to report.
Results -> evidence/format_comparison.json

Note: curated has slightly fewer rows than raw (cleaning), mention it in the report.

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/ingest/compare_formats.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone, local_csv, hdfs_du  # noqa: E402
from processing.etl import RAW_SCHEMA  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402


def timed(fn, runs=2):
    times = []
    for _ in range(runs):
        t0 = time.time()
        fn()
        times.append(round(time.time() - t0, 2))
    return times


def main():
    cfg = load_config()
    spark = get_spark(cfg, "03-compare-formats")

    csv_path = f"{zone(cfg, 'raw')}/{os.path.basename(local_csv(cfg))}"
    pq_path = zone(cfg, "curated")

    csv_df = (spark.read.option("header", True)
              .option("timestampFormat", cfg["dataset"]["csv_timestamp_format"])
              .schema(RAW_SCHEMA).csv(csv_path))
    pq_df = spark.read.parquet(pq_path)

    results = {"sample_mode": cfg["sample_mode"]}
    for name, path, df in (("csv", csv_path, csv_df), ("parquet", pq_path, pq_df)):
        logical, on_disk = hdfs_du(spark, path)
        results[name] = {
            "path": path,
            "size_mb": round(logical / 1e6, 1),
            "size_on_disk_with_replication_mb": round(on_disk / 1e6, 1),
            "rows": df.count(),
            "count_seconds": timed(lambda: df.count()),
            "avg_price_seconds": timed(
                lambda: df.groupBy("event_type").agg(F.avg("price")).collect()),
        }

    results["size_ratio_csv_over_parquet"] = round(
        results["csv"]["size_mb"] / results["parquet"]["size_mb"], 2)

    print(json.dumps(results, indent=2))

    os.makedirs(cfg["local"]["evidence_dir"], exist_ok=True)
    out = os.path.join(cfg["local"]["evidence_dir"],
                       "format_comparison_sample.json" if cfg["sample_mode"]
                       else "format_comparison.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"[compare] saved to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
