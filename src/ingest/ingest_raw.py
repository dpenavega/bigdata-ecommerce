"""
Section B - Ingestion into the RAW zone.

  1. Creates the three zones in HDFS: raw / curated / results
  2. Copies the original CSV, untouched, into raw/ecommerce/

Idempotent: if the file is already in HDFS with the same size, it is skipped;
otherwise it is overwritten (never duplicated).

Run:
  docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/ingest/ingest_raw.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import (load_config, get_spark, zone, local_csv, hdfs_mkdirs,  # noqa: E402
                    hdfs_exists, hdfs_file_size, hdfs_put)


def main():
    cfg = load_config()
    spark = get_spark(cfg, "01-ingest-raw")

    # 1) zones
    for name in ("raw", "curated", "results"):
        hdfs_mkdirs(spark, zone(cfg, name))
        print(f"[zone] {zone(cfg, name)}")

    # 2) raw copy
    src = local_csv(cfg)
    if not os.path.exists(src):
        sys.exit(f"ERROR: {src} not found. Put the CSV in the data/ folder of the repo.")

    dst = f"{zone(cfg, 'raw')}/{os.path.basename(src)}"
    local_size = os.path.getsize(src)

    if hdfs_exists(spark, dst) and hdfs_file_size(spark, dst) == local_size:
        print(f"[raw] already in HDFS with the same size, skipping: {dst}")
    else:
        print(f"[raw] uploading {src} ({local_size / 1e9:.2f} GB) -> {dst}")
        t0 = time.time()
        hdfs_put(spark, src, dst)
        print(f"[raw] done in {time.time() - t0:.1f} s")

    print(f"[raw] size in HDFS: {hdfs_file_size(spark, dst) / 1e9:.2f} GB")
    spark.stop()


if __name__ == "__main__":
    main()
