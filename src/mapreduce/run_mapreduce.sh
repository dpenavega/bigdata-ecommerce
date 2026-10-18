#!/bin/bash
# Section C - runs the Hadoop Streaming job on YARN.
# Executed INSIDE the namenode container (src/mapreduce is mounted at /mapreduce).
#
# Usage:
#   docker exec namenode bash /mapreduce/run_mapreduce.sh <input_hdfs> <output_hdfs>
# Example (sample):
#   docker exec namenode bash /mapreduce/run_mapreduce.sh /data_sample/raw/ecommerce/sample.csv /data_sample/results/mr_brand_revenue

set -e
IN=${1:-/data/raw/ecommerce/2019-Nov.csv}
OUT=${2:-/data/results/mr_brand_revenue}
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)

# idempotent: Hadoop refuses to write into an existing folder, so delete the old result first
hdfs dfs -rm -r -f -skipTrash "$OUT" > /dev/null

START=$(date +%s)
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="brand-revenue-streaming" \
  -D mapreduce.job.reduces=2 \
  -files /mapreduce/mapper.py,/mapreduce/reducer.py \
  -mapper  "python3 mapper.py" \
  -combiner "python3 reducer.py" \
  -reducer "python3 reducer.py" \
  -input  "$IN" \
  -output "$OUT"
END=$(date +%s)

echo ""
echo "[mapreduce] input   : $IN"
echo "[mapreduce] output  : $OUT"
echo "[mapreduce] elapsed : $((END - START)) s"
echo "[mapreduce] top 15 brands by revenue (brand, purchases, revenue):"
hdfs dfs -cat "$OUT"/part-* | sort -t$'\t' -k3,3 -g -r | head -15
