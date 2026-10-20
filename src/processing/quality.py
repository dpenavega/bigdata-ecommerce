"""
Section E - Data quality checks on the CURATED zone.

Checks (thresholds in config/pipeline.yaml -> quality):
  1. the table is not empty
  2. no nulls in required columns
  3. price inside a reasonable range
  4. event_type only takes valid values
  5. all dates inside the expected period
  6. no exact duplicate rows (optional, needs a shuffle)

Writes evidence/quality_report.json. If a check fails and fail_on_error is true, exits with error
(so the pipeline stops before using bad data).

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/processing/quality.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

REQUIRED = ["event_time", "event_type", "product_id", "price", "user_id", "user_session",
            "brand", "category_code", "event_date"]


def run_checks(spark, cfg):
    q = cfg["quality"]
    ev = spark.read.parquet(zone(cfg, "curated"))
    checks = []

    def add(name, passed, detail):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    # one pass over the data for the cheap checks
    row = ev.agg(
        F.count("*").alias("rows"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in REQUIRED],
        F.sum(((F.col("price") < q["price_min"]) | (F.col("price") > q["price_max"])).cast("int")).alias("bad_price"),
        F.sum((~F.col("event_type").isin(cfg["dataset"]["valid_event_types"])).cast("int")).alias("bad_type"),
        F.sum(((F.col("event_date") < q["date_from"]) | (F.col("event_date") > q["date_to"])).cast("int")).alias("bad_date"),
        F.min("price").alias("min_price"), F.max("price").alias("max_price"),
        F.min("event_date").alias("min_date"), F.max("event_date").alias("max_date"),
    ).collect()[0]

    rows = row["rows"]
    add("not_empty", rows > 0, {"rows": rows})

    null_pct = {c: round(100.0 * row[f"null_{c}"] / rows, 4) if rows else None for c in REQUIRED}
    add("no_nulls_in_required_columns",
        rows > 0 and all(v <= q["max_null_pct"] for v in null_pct.values()),
        {"null_pct": null_pct, "max_allowed_pct": q["max_null_pct"]})

    add("price_in_range", row["bad_price"] == 0,
        {"out_of_range": row["bad_price"], "min": row["min_price"], "max": row["max_price"],
         "allowed": [q["price_min"], q["price_max"]]})

    add("valid_event_types", row["bad_type"] == 0, {"invalid": row["bad_type"]})

    add("dates_in_period", row["bad_date"] == 0,
        {"out_of_period": row["bad_date"], "min": str(row["min_date"]), "max": str(row["max_date"]),
         "expected": [q["date_from"], q["date_to"]]})

    if q.get("check_duplicates", True):
        dups = rows - ev.dropDuplicates().count()
        add("no_duplicates", dups == 0, {"duplicate_rows": dups})

    return checks


def main():
    cfg = load_config()
    spark = get_spark(cfg, "06-data-quality")
    checks = run_checks(spark, cfg)
    spark.stop()

    for c in checks:
        print(f"[quality] {'PASS' if c['passed'] else 'FAIL'}  {c['check']:<30} {c['detail']}")

    report = {"sample_mode": cfg["sample_mode"], "all_passed": all(c["passed"] for c in checks),
              "checks": checks}
    os.makedirs(cfg["local"]["evidence_dir"], exist_ok=True)
    out = os.path.join(cfg["local"]["evidence_dir"],
                       "quality_report_sample.json" if cfg["sample_mode"] else "quality_report.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"[quality] report: {out}")

    if not report["all_passed"] and cfg["quality"]["fail_on_error"]:
        raise SystemExit("[quality] some checks FAILED - stopping (fail_on_error = true)")


if __name__ == "__main__":
    main()
