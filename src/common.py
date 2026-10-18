"""
Shared helpers: read config/pipeline.yaml, build the SparkSession, resolve HDFS paths,
and talk to HDFS through Spark's own Hadoop client (no hdfs CLI needed in Jupyter).
"""
import os

import yaml
from pyspark.sql import SparkSession

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG = os.path.join(REPO_ROOT, "config", "pipeline.yaml")


# ----------------------------------------------------------------- config ----
def load_config(path=None):
    path = path or os.getenv("PIPELINE_CONFIG", DEFAULT_CONFIG)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def hdfs_root(cfg):
    root = cfg["hdfs"]["root"].rstrip("/")
    if cfg["sample_mode"]:
        root += "_sample"
    return cfg["hdfs"]["url"].rstrip("/") + root


def zone(cfg, name):
    """Full URI of a zone, e.g. zone(cfg, 'raw') -> hdfs://namenode:9000/data/raw/ecommerce"""
    return f"{hdfs_root(cfg)}/{cfg['zones'][name]}"


def local_csv(cfg):
    return cfg["local"]["sample_csv"] if cfg["sample_mode"] else cfg["local"]["full_csv"]


# ------------------------------------------------------------------ spark ----
def get_spark(cfg, app_name):
    s = cfg["spark"]
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(s["master"])
        .config("spark.executor.memory", s["executor_memory"])
        .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", s["timezone"])
    )
    if s["master"].startswith("spark://"):
        builder = builder.config("spark.driver.host", s["driver_host"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ------------------------------------------------------------------- hdfs ----
def _fs(spark, uri):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(conf), path


def hdfs_mkdirs(spark, uri):
    fs, path = _fs(spark, uri)
    fs.mkdirs(path)


def hdfs_exists(spark, uri):
    fs, path = _fs(spark, uri)
    return fs.exists(path)


def hdfs_file_size(spark, uri):
    fs, path = _fs(spark, uri)
    return fs.getFileStatus(path).getLen()


def hdfs_put(spark, local_path, uri):
    """Copy a local file into HDFS, overwriting (= hdfs dfs -put -f)."""
    fs, dst = _fs(spark, uri)
    src = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("file://" + os.path.abspath(local_path))
    fs.copyFromLocalFile(False, True, src, dst)   # delSrc=False, overwrite=True


def hdfs_rm(spark, uri):
    """Delete a file or folder if it exists (= hdfs dfs -rm -r -f)."""
    fs, path = _fs(spark, uri)
    if fs.exists(path):
        fs.delete(path, True)


def hdfs_du(spark, uri):
    """(logical bytes, bytes on disk including replication) of a file or folder."""
    fs, path = _fs(spark, uri)
    summary = fs.getContentSummary(path)
    return summary.getLength(), summary.getSpaceConsumed()
