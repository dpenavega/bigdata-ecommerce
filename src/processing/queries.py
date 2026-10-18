"""
Section D - 3 analytic questions (increasing difficulty) + 1 optimization with before/after.

  Q1 (groupBy)               Daily funnel: views, carts, purchases, conversion and revenue per day.
  Q2 (groupBy + window)      Top 3 brands by revenue inside each main category (rank()).
  Q3 (several windows)       Daily revenue with day-over-day change (lag) and 7-day moving average.

  OPTIMIZATION               Partition pruning: "Black Friday week" filtered on the partition
                             column (event_date) vs on a normal column (event_time).
                             Same result, but the first one only reads 6 of the 30 folders.

Outputs:
  * results zone in HDFS:   q1_daily_funnel, q2_top_brands_by_category, q3_daily_revenue_trend
  * evidence/explain_*.txt  execution plans (explain)
  * evidence/optimization.json  timings before / after

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/processing/queries.py
  add  --pause  at the end to keep the Spark UI (localhost:4040) open for screenshots
"""
import contextlib
import io
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402


# ------------------------------------------------------------------ queries ----
def q1_daily_funnel(ev):
    return (ev.groupBy("event_date")
            .agg(F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
                 F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
                 F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                 F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))
                               .otherwise(0.0)), 2).alias("revenue"))
            .withColumn("conversion_pct", F.round(F.col("purchases") / F.col("views") * 100, 3))
            .orderBy("event_date"))


def q2_top_brands_by_category(ev, top_n=3):
    revenue = (ev.filter((F.col("event_type") == "purchase") & (F.col("brand") != "unknown"))
               .groupBy("cat_l1", "brand")
               .agg(F.round(F.sum("price"), 2).alias("revenue"),
                    F.count("*").alias("purchases")))
    w = Window.partitionBy("cat_l1").orderBy(F.col("revenue").desc())
    return (revenue.withColumn("rank", F.rank().over(w))
            .filter(F.col("rank") <= top_n)
            .orderBy("cat_l1", "rank"))


def q3_daily_revenue_trend(ev):
    daily = (ev.filter(F.col("event_type") == "purchase")
             .groupBy("event_date")
             .agg(F.round(F.sum("price"), 2).alias("revenue")))
    # only 30 rows (one per day) -> a window without partitionBy is fine here
    w = Window.orderBy("event_date")
    w7 = w.rowsBetween(-6, 0)
    return (daily
            .withColumn("prev_day_revenue", F.lag("revenue", 1).over(w))
            .withColumn("change_vs_prev_day_pct",
                        F.round((F.col("revenue") - F.col("prev_day_revenue"))
                                / F.col("prev_day_revenue") * 100, 1))
            .withColumn("moving_avg_7d", F.round(F.avg("revenue").over(w7), 2))
            .orderBy("event_date"))


# ------------------------------------------------------------- helpers ----
def explain_to_file(df, path):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(buf.getvalue())
    return buf.getvalue()


def timed(fn, runs=2):
    times = []
    for _ in range(runs):
        t0 = time.time()
        result = fn()
        times.append(round(time.time() - t0, 2))
    return result, times


def main():
    cfg = load_config()
    spark = get_spark(cfg, "05-analytic-queries")
    evidence = cfg["local"]["evidence_dir"]
    os.makedirs(evidence, exist_ok=True)
    suffix = "_sample" if cfg["sample_mode"] else ""

    ev = spark.read.parquet(zone(cfg, "curated"))
    results = zone(cfg, "results")

    # -------------------------------------------------- the 3 questions ----
    for name, df, rows in (("q1_daily_funnel", q1_daily_funnel(ev), 31),
                           ("q2_top_brands_by_category", q2_top_brands_by_category(ev), 60),
                           ("q3_daily_revenue_trend", q3_daily_revenue_trend(ev), 31)):
        print(f"\n=== {name} ===")
        t0 = time.time()
        df.write.mode("overwrite").parquet(f"{results}/{name}")        # idempotent
        elapsed = time.time() - t0
        spark.read.parquet(f"{results}/{name}").orderBy(*df.columns[:1]).show(rows, truncate=False)
        explain_to_file(df, os.path.join(evidence, f"explain_{name}{suffix}.txt"))
        print(f"[{name}] {elapsed:.1f} s -> {results}/{name}")

    # ------------------------------------------------- the optimization ----
    # Same question (purchases and revenue during Black Friday week), two ways:
    start, end = "2019-11-25", "2019-11-30"
    agg = [F.count("*").alias("purchases"), F.round(F.sum("price"), 2).alias("revenue")]

    before = (ev.filter((F.to_date("event_time") >= start) & (F.to_date("event_time") <= end))
              .filter(F.col("event_type") == "purchase").agg(*agg))
    after = (ev.filter((F.col("event_date") >= start) & (F.col("event_date") <= end))
             .filter(F.col("event_type") == "purchase").agg(*agg))

    plan_before = explain_to_file(before, os.path.join(evidence, f"explain_opt_before{suffix}.txt"))
    plan_after = explain_to_file(after, os.path.join(evidence, f"explain_opt_after{suffix}.txt"))

    res_before, t_before = timed(lambda: before.collect())
    res_after, t_after = timed(lambda: after.collect())

    report = {
        "question": f"purchases and revenue between {start} and {end}",
        "before": {"filter_on": "event_time (normal column) -> scans all partitions",
                   "seconds": t_before, "result": res_before[0].asDict(),
                   "partition_filters_in_plan": "PartitionFilters: [" in plan_before
                                                and "PartitionFilters: []" not in plan_before},
        "after": {"filter_on": "event_date (partition column) -> partition pruning",
                  "seconds": t_after, "result": res_after[0].asDict(),
                  "partition_filters_in_plan": "PartitionFilters: [" in plan_after
                                               and "PartitionFilters: []" not in plan_after},
        "same_result": res_before[0].asDict() == res_after[0].asDict(),
    }
    print("\n=== optimization: partition pruning ===")
    print(json.dumps(report, indent=2, default=str))
    with open(os.path.join(evidence, f"optimization{suffix}.json"), "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    if "--pause" in sys.argv:
        input("\nSpark UI is open at http://localhost:4040 - take your screenshots, then press Enter...")
    spark.stop()


if __name__ == "__main__":
    main()
