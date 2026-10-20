"""
Section F - Challenge 4: data quality layer with QUARANTINE zone and quality report.

Every raw event is checked against validation rules:

  ERRORS (the event is rejected -> quarantine zone, with the reasons)
    missing_required      event_time / event_type / product_id / user_id / user_session is null
    invalid_event_type    event_type not in {view, cart, purchase}
    invalid_price         price is null or <= 0
    out_of_period         event outside the configured period (November 2019)
    duplicate             exact copy of an earlier row (the first copy is kept)

  WARNINGS (the event is kept, but counted in the report)
    price_outlier         price > 10x the median price of its category
    inconsistent_brand    the same product_id appears with more than one brand
    missing_brand         brand is null
    missing_category      category_code is null

Outputs:
  * HDFS  <root>/quarantine/events   rejected events + array column `reject_reasons` (Parquet)
  * evidence/quality_layer_report.json   counts per rule, % rejected, examples
  * consistency check: valid events here == rows in the curated zone written by etl.py

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/processing/quality_layer.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone, hdfs_root, hdfs_exists  # noqa: E402
from processing.etl import read_raw  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

RAW_COLS = ["event_time", "event_type", "product_id", "category_id", "category_code",
            "brand", "price", "user_id", "user_session"]


def flag(cond, name):
    """Returns `name` when cond is true, else null (nulls are dropped from the reasons array)."""
    return F.when(cond, F.lit(name))


def main():
    cfg = load_config()
    q = cfg["quality"]
    spark = get_spark(cfg, "08-quality-layer")
    quarantine = f"{hdfs_root(cfg)}/quarantine/events"
    t0 = time.time()

    raw = read_raw(spark, cfg)

    # ------------------------------------------------ helper columns ----
    # duplicates: number each group of identical rows, keep the first one
    dup_w = Window.partitionBy(*RAW_COLS).orderBy(F.lit(1))
    # price outliers: median price per category (small table -> broadcast join)
    cat_median = (raw.filter(F.col("price") > 0)
                  .groupBy("category_id")
                  .agg(F.percentile_approx("price", 0.5).alias("cat_median_price")))
    # products seen with more than one brand
    multi_brand = (raw.filter(F.col("brand").isNotNull())
                   .groupBy("product_id").agg(F.countDistinct("brand").alias("n_brands"))
                   .filter(F.col("n_brands") > 1).select("product_id")
                   .withColumn("inconsistent", F.lit(True)))

    ev = (raw.withColumn("dup_n", F.row_number().over(dup_w))
          .join(F.broadcast(cat_median), "category_id", "left")
          .join(multi_brand, "product_id", "left"))

    date = F.to_date("event_time")
    errors = F.filter(F.array(
        flag(F.col("event_time").isNull() | F.col("event_type").isNull() | F.col("product_id").isNull()
             | F.col("user_id").isNull() | F.col("user_session").isNull(), "missing_required"),
        flag(~F.coalesce(F.col("event_type").isin(cfg["dataset"]["valid_event_types"]), F.lit(False)),
             "invalid_event_type"),
        flag(F.col("price").isNull() | (F.col("price") <= 0), "invalid_price"),
        flag(date.isNotNull() & ((date < q["date_from"]) | (date > q["date_to"])), "out_of_period"),
        flag(F.col("dup_n") > 1, "duplicate"),
    ), lambda x: x.isNotNull())

    warnings = F.filter(F.array(
        flag(F.col("price") > 10 * F.col("cat_median_price"), "price_outlier"),
        flag(F.coalesce(F.col("inconsistent"), F.lit(False)), "inconsistent_brand"),
        flag(F.col("brand").isNull(), "missing_brand"),
        flag(F.col("category_code").isNull(), "missing_category"),
    ), lambda x: x.isNotNull())

    checked = (ev.withColumn("reject_reasons", errors)
               .withColumn("warnings", warnings)
               .select(*RAW_COLS, "reject_reasons", "warnings")
               .cache())

    # ------------------------------------------- quarantine (idempotent) ----
    rejected = checked.filter(F.size("reject_reasons") > 0)
    (rejected.select(*RAW_COLS, "reject_reasons")
     .withColumn("quarantined_at", F.current_timestamp())
     .write.mode("overwrite").parquet(quarantine))

    # ---------------------------------------------------------- report ----
    total = checked.count()
    n_rejected = spark.read.parquet(quarantine).count()
    n_valid = total - n_rejected

    def counts(col):
        rows = (checked.select(F.explode(col).alias("rule")).groupBy("rule").count().collect())
        return {r["rule"]: r["count"] for r in sorted(rows, key=lambda r: -r["count"])}

    examples = {}
    for row in (rejected.select(F.explode("reject_reasons").alias("rule"), *RAW_COLS)
                .dropDuplicates(["rule"]).collect()):
        examples[row["rule"]] = {k: str(row[k]) for k in RAW_COLS}

    curated_rows = (spark.read.parquet(zone(cfg, "curated")).count()
                    if hdfs_exists(spark, zone(cfg, "curated")) else None)

    report = {
        "sample_mode": cfg["sample_mode"],
        "total_raw_events": total,
        "valid_events": n_valid,
        "quarantined_events": n_rejected,
        "quarantined_pct": round(100.0 * n_rejected / max(total, 1), 4),
        "errors_per_rule": counts("reject_reasons"),
        "warnings_per_rule": counts("warnings"),
        "warnings_pct_of_total": {k: round(100.0 * v / max(total, 1), 2)
                                  for k, v in counts("warnings").items()},
        "example_rejected_event_per_rule": examples,
        "quarantine_path": quarantine,
        "consistency_with_curated_zone": {
            "curated_rows": curated_rows,
            "valid_events_here": n_valid,
            "match": curated_rows == n_valid,
            # etl.py does not filter by date; out-of-period rows are caught later by quality.py
            "difference_explained_by_out_of_period":
                curated_rows is not None
                and curated_rows - n_valid == counts("reject_reasons").get("out_of_period", 0),
        },
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(report, indent=2))

    os.makedirs(cfg["local"]["evidence_dir"], exist_ok=True)
    out = os.path.join(cfg["local"]["evidence_dir"],
                       "quality_layer_report_sample.json" if cfg["sample_mode"]
                       else "quality_layer_report.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"[quality-layer] quarantine: {quarantine}")
    print(f"[quality-layer] report    : {out}")
    spark.stop()


if __name__ == "__main__":
    main()
