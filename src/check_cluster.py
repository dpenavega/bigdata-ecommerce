"""
Smoke test for the environment (Entrega 1, section A).

Checks that, from the Jupyter container:
  1. the driver connects to the Spark standalone master,
  2. tasks actually run on the worker container(s),
  3. Spark can write Parquet to HDFS and read it back.

Run it with:
  docker exec -it -u jovyan jupyter python /home/jovyan/work/src/check_cluster.py
"""
import os
import socket

from pyspark.sql import SparkSession, functions as F

MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS = os.getenv("HDFS_URL", "hdfs://namenode:9000")

builder = (
    SparkSession.builder
    .appName("check-cluster")
    .master(MASTER)
    .config("spark.executor.memory", "1g")
)
if MASTER.startswith("spark://"):
    # executors on the worker reach the driver by the Jupyter container name
    builder = builder.config("spark.driver.host", "jupyter")

spark = builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("Spark version :", spark.version)
print("Master        :", sc.master)
print("Driver host   :", socket.gethostname())

# 1) A real job on the cluster
df = spark.range(0, 1_000_000).withColumn("bucket", F.col("id") % 10)
print("Row count     :", df.count())

# 2) Where did the tasks run? (should be spark-worker-1, not jupyter)
hosts = (
    sc.parallelize(range(100), 4)
    .map(lambda _: socket.gethostname())
    .distinct()
    .collect()
)
print("Tasks ran on  :", sorted(hosts))

# 3) HDFS round trip
path = f"{HDFS}/tmp/check_cluster"
df.groupBy("bucket").count().write.mode("overwrite").parquet(path)
back = spark.read.parquet(path)
print("Written to    :", path)
print("Rows read back:", back.count(), "(expected 10)")
back.orderBy("bucket").show()

print("ENVIRONMENT OK")
print("=" * 60)
spark.stop()
