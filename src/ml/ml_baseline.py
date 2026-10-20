"""
Section E - Minimal MLlib baseline.

Target (registered): will a user who was active in the 1st half of November
                     buy something in the 2nd half?

  features  <- what each user did BEFORE split_date (Nov 1-15)
  label     <- 1 if the user has a purchase ON/AFTER split_date (Nov 16-30), else 0

Features only use the first half, so the model never sees the future (no leakage).

  VectorAssembler -> StandardScaler -> LogisticRegression -> AUC on a test set

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/ml/ml_baseline.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import load_config, get_spark, zone  # noqa: E402

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import LogisticRegression  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.ml.feature import StandardScaler, VectorAssembler  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

FEATURES = ["n_events", "n_views", "n_carts", "n_purchases", "spent", "n_sessions",
            "n_products", "n_categories", "avg_price_viewed", "active_days", "days_since_last"]


def is_type(t):
    return (F.col("event_type") == t).cast("int")


def build_dataset(ev, split_date):
    first = ev.filter(F.col("event_date") < split_date)
    second = ev.filter(F.col("event_date") >= split_date)

    features = (first.groupBy("user_id").agg(
        F.count("*").alias("n_events"),
        F.sum(is_type("view")).alias("n_views"),
        F.sum(is_type("cart")).alias("n_carts"),
        F.sum(is_type("purchase")).alias("n_purchases"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("spent"),
        F.countDistinct("user_session").alias("n_sessions"),
        F.countDistinct("product_id").alias("n_products"),
        F.countDistinct("cat_l1").alias("n_categories"),
        F.avg(F.when(F.col("event_type") == "view", F.col("price"))).alias("avg_price_viewed"),
        F.countDistinct("event_date").alias("active_days"),
        F.datediff(F.lit(split_date), F.max("event_date")).alias("days_since_last"),
    ).fillna(0.0, subset=["avg_price_viewed"]))

    buyers_2nd = (second.filter(F.col("event_type") == "purchase")
                  .select("user_id").distinct().withColumn("label", F.lit(1.0)))

    return (features.join(buyers_2nd, "user_id", "left")      # only users active in the 1st half
            .fillna(0.0, subset=["label"]))


def main():
    cfg = load_config()
    m = cfg["ml"]
    spark = get_spark(cfg, "07-ml-baseline")
    t0 = time.time()

    ev = spark.read.parquet(zone(cfg, "curated"))
    data = build_dataset(ev, m["split_date"]).cache()

    n_users = data.count()
    n_pos = data.filter(F.col("label") == 1.0).count()
    print(f"[ml] users active before {m['split_date']}: {n_users:,}")
    print(f"[ml] of which buy afterwards (label=1): {n_pos:,} ({100.0 * n_pos / max(n_users, 1):.2f}%)")
    if n_pos == 0 or n_pos == n_users:
        print("[ml] only one class present (e.g. the sample covers only the first days): "
              "run with sample_mode: false to train the model")
        spark.stop()
        return

    train, test = data.randomSplit([1 - m["test_fraction"], m["test_fraction"]], seed=m["seed"])

    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=20),
    ])
    model = pipeline.fit(train)
    pred = model.transform(test)

    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
    acc = pred.filter(F.col("prediction") == F.col("label")).count() / max(pred.count(), 1)
    baseline_acc = 1 - n_pos / n_users                    # always predicting "no purchase"
    coefs = dict(zip(FEATURES, [round(float(c), 4) for c in model.stages[-1].coefficients]))

    report = {
        "sample_mode": cfg["sample_mode"],
        "target": "user active before split_date buys on/after split_date",
        "split_date": m["split_date"],
        "users": n_users, "positives": n_pos,
        "positive_rate_pct": round(100.0 * n_pos / n_users, 2),
        "train_rows": train.count(), "test_rows": test.count(),
        "model": "LogisticRegression (standardized features)",
        "test_auc": round(auc, 4),
        "test_accuracy": round(acc, 4),
        "accuracy_if_always_predict_0": round(baseline_acc, 4),
        "coefficients": coefs,
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(report, indent=2))

    os.makedirs(cfg["local"]["evidence_dir"], exist_ok=True)
    out = os.path.join(cfg["local"]["evidence_dir"],
                       "ml_baseline_sample.json" if cfg["sample_mode"] else "ml_baseline.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"[ml] report: {out}")
    spark.stop()


if __name__ == "__main__":
    main()
