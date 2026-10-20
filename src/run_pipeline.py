"""
Section E - SINGLE ENTRY POINT of the pipeline.

  ingest  -> raw CSV into HDFS (zones raw / curated / results)
  etl     -> CSV -> clean Parquet partitioned by event_date
  quality -> data quality checks (stops the pipeline if they fail)
  queries -> the 3 analytic questions + optimization -> results zone
  ml      -> MLlib baseline

Everything is read from config/pipeline.yaml (paths, partition column, thresholds, steps).
Every step overwrites its output, so running the pipeline twice gives the same result
(idempotent); the summary at the end makes it easy to check.

Run everything:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/run_pipeline.py
Run only some steps:
  ... src/run_pipeline.py --steps quality,ml

(The Hadoop Streaming job of section C runs on YARN from the namenode container,
 so it is launched separately: docker exec namenode bash /mapreduce/run_mapreduce.sh)
"""
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load_config, get_spark, zone, hdfs_exists  # noqa: E402
from ingest import ingest_raw  # noqa: E402
from processing import etl, quality, queries  # noqa: E402
from ml import ml_baseline  # noqa: E402

STEPS = {
    "ingest": ingest_raw.main,
    "etl": etl.main,
    "quality": quality.main,
    "queries": queries.main,
    "ml": ml_baseline.main,
}
RESULT_TABLES = ["q1_daily_funnel", "q2_top_brands_by_category", "q3_daily_revenue_trend"]


def summary(cfg):
    """Row counts of everything the pipeline produced (used to prove idempotency)."""
    spark = get_spark(cfg, "pipeline-summary")
    out = {"curated_rows": spark.read.parquet(zone(cfg, "curated")).count()
           if hdfs_exists(spark, zone(cfg, "curated")) else None}
    for t in RESULT_TABLES:
        path = f"{zone(cfg, 'results')}/{t}"
        out[t] = spark.read.parquet(path).count() if hdfs_exists(spark, path) else None
    spark.stop()
    return out


def main():
    cfg = load_config()
    steps = cfg["pipeline"]["steps"]
    if "--steps" in sys.argv:
        steps = sys.argv[sys.argv.index("--steps") + 1].split(",")
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f"unknown steps {unknown}; valid: {list(STEPS)}")

    print(f"[pipeline] sample_mode={cfg['sample_mode']}  steps={steps}")
    timings, t_all = {}, time.time()
    for step in steps:
        print(f"\n[pipeline] ===== {step} =====")
        t0 = time.time()
        STEPS[step]()                         # each step raises/exits on error -> pipeline stops
        timings[step] = round(time.time() - t0, 1)
        print(f"[pipeline] {step} done in {timings[step]} s")

    report = {"finished_at": datetime.now().isoformat(timespec="seconds"),
              "sample_mode": cfg["sample_mode"], "steps": steps,
              "seconds_per_step": timings, "total_seconds": round(time.time() - t_all, 1),
              "outputs": summary(cfg)}
    print("\n[pipeline] SUMMARY")
    print(json.dumps(report, indent=2))

    os.makedirs(cfg["local"]["evidence_dir"], exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(cfg["local"]["evidence_dir"], f"pipeline_run_{stamp}.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"[pipeline] report: {out}")


if __name__ == "__main__":
    main()
