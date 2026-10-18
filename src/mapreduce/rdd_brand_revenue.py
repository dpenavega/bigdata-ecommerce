"""
Section C - the SAME aggregation as mapper.py + reducer.py, with Spark RDDs.

  textFile -> flatMap(parse) -> reduceByKey -> result

Then:
  * saves the result in the results zone
  * compares it with the MapReduce output (if it exists): both must give the same numbers
  * writes timings + lines of code to evidence/mapreduce_vs_rdd.json

Run (after run_mapreduce.sh):
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/mapreduce/rdd_brand_revenue.py
"""
import inspect
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone, local_csv, hdfs_exists, hdfs_rm  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def parse(line):
    """Same rules as mapper.py: purchases with a valid positive price, empty brand -> unknown."""
    f = line.split(",")
    if len(f) != 9 or f[1] != "purchase":
        return []
    try:
        price = float(f[6])
    except ValueError:
        return []
    if price <= 0:
        return []
    return [(f[5].strip() or "unknown", (1, price))]


def code_lines(path):
    """Lines of code without blanks, comments or docstrings (rough complexity measure)."""
    n, in_doc = 0, False
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            s = raw.strip()
            if s.startswith('"""'):
                in_doc = not in_doc if s.count('"""') == 1 else in_doc
                continue
            if in_doc or not s or s.startswith("#"):
                continue
            n += 1
    return n


def main():
    cfg = load_config()
    spark = get_spark(cfg, "04-rdd-brand-revenue")
    sc = spark.sparkContext

    raw_csv = f"{zone(cfg, 'raw')}/{os.path.basename(local_csv(cfg))}"
    rdd_out = f"{zone(cfg, 'results')}/rdd_brand_revenue"
    mr_out = f"{zone(cfg, 'results')}/mr_brand_revenue"

    # ------------------------------------------------------------ the job ----
    t0 = time.time()
    result = (sc.textFile(raw_csv)
              .flatMap(parse)                                          # map
              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))    # shuffle + reduce
              .collect())
    elapsed = time.time() - t0

    rdd = {brand: (n, round(rev, 2)) for brand, (n, rev) in result}
    top = sorted(rdd.items(), key=lambda kv: kv[1][1], reverse=True)[:15]

    print(f"[rdd] input   : {raw_csv}")
    print(f"[rdd] brands  : {len(rdd):,}")
    print(f"[rdd] elapsed : {elapsed:.1f} s")
    print("[rdd] top 15 brands by revenue:")
    for brand, (n, rev) in top:
        print(f"      {brand:<20} {n:>9,} purchases   {rev:>16,.2f}")

    # ----------------------------------------------- save (idempotent) ----
    hdfs_rm(spark, rdd_out)
    (sc.parallelize(sorted(rdd.items()), 1)
       .map(lambda kv: f"{kv[0]}\t{kv[1][0]}\t{kv[1][1]:.2f}")
       .saveAsTextFile(rdd_out))
    print(f"[rdd] saved   : {rdd_out}")

    # ------------------------------------------ compare with MapReduce ----
    report = {"sample_mode": cfg["sample_mode"], "input": raw_csv,
              "rdd_seconds": round(elapsed, 1), "brands": len(rdd)}
    if hdfs_exists(spark, mr_out):
        mr = {}
        for line in sc.textFile(mr_out + "/part-*").collect():
            b, n, rev = line.split("\t")
            mr[b] = (int(n), float(rev))
        diff = [b for b in set(mr) | set(rdd)
                if b not in mr or b not in rdd
                or mr[b][0] != rdd[b][0] or abs(mr[b][1] - rdd[b][1]) > 0.01]
        report["same_result_as_mapreduce"] = not diff
        print("[compare] MapReduce and RDD results are IDENTICAL" if not diff
              else f"[compare] {len(diff)} brands differ, e.g. {diff[:5]}")
    else:
        print(f"[compare] {mr_out} not found: run run_mapreduce.sh first to compare")

    report["lines_of_code"] = {
        "mapreduce (mapper + reducer + run script)":
            code_lines(os.path.join(HERE, "mapper.py"))
            + code_lines(os.path.join(HERE, "reducer.py"))
            + code_lines(os.path.join(HERE, "run_mapreduce.sh")),
        # parse() + the 3-line textFile/flatMap/reduceByKey pipeline
        "rdd (parse + pipeline)": len([s for s in inspect.getsource(parse).splitlines()
                                       if s.strip() and not s.strip().startswith(('"""', "#"))]) + 3,
    }

    os.makedirs(cfg["local"]["evidence_dir"], exist_ok=True)
    out = os.path.join(cfg["local"]["evidence_dir"],
                       "mapreduce_vs_rdd_sample.json" if cfg["sample_mode"] else "mapreduce_vs_rdd.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"[rdd] report  : {out}  (add the MapReduce time from run_mapreduce.sh)")
    spark.stop()


if __name__ == "__main__":
    main()
