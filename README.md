# Big Data & Cloud Computing — Entrega 1

Local Big Data architecture over the **eCommerce behavior data (multi-category store)** dataset
(`2019-Nov.csv`, Kaggle — mkechinov).

## Architecture of the environment

```
                     docker compose up -d
 ┌──────────────────────────────────────────────────────────────────┐
 │  HDFS                         YARN (Hadoop Streaming jobs)        │
 │  namenode :9870 :9000         resourcemanager :8088               │
 │  datanode1   datanode2        nodemanager    historyserver :8188  │
 │       ▲  replication = 2                                          │
 │       │  hdfs://namenode:9000                                     │
 │  ┌────┴──────────────┐       ┌──────────────────────────────┐     │
 │  │ jupyter (driver)  │──────►│ spark-master :8080 (:7077)   │     │
 │  │ :8888  UI :4040   │◄──────│ spark-worker-1 :8081         │     │
 │  └───────────────────┘ tasks └──────────────────────────────┘     │
 └──────────────────────────────────────────────────────────────────┘
```

Spark master, worker and Jupyter all use `quay.io/jupyter/pyspark-notebook`, so the driver and
the executors always run the same Python and Spark versions.

## Requirements

- Docker Desktop running (Windows: WSL2 backend)
- About **8 GB of RAM available to Docker** and ~25 GB of free disk
  (9 GB CSV + 2 HDFS replicas of the raw file + Parquet)
- Free ports: 8080, 8081, 8088, 8188, 8888, 4040, 9000, 9870

## 1. Install and start

```bash
git clone <repo-url>
cd bigdata-ecommerce
# put 2019-Nov.csv inside ./data   (download from Kaggle, unzip)
docker compose up -d --build
```

The first run takes several minutes (it downloads the images). Check it:

```bash
docker ps        # 9 containers
```

| Container | Role | UI |
|---|---|---|
| namenode | HDFS NameNode | http://localhost:9870 |
| datanode1, datanode2 | HDFS DataNodes | – |
| resourcemanager | YARN ResourceManager | http://localhost:8088 |
| nodemanager | YARN NodeManager | – |
| historyserver | Job history | http://localhost:8188 |
| spark-master | Spark master | http://localhost:8080 |
| spark-worker-1 | Spark worker (2 cores, 2 GB) | http://localhost:8081 |
| jupyter | PySpark dev environment | http://localhost:8888/?token=bigdata |

Spark application UI (only while a SparkSession is running): http://localhost:4040

## 2. Verify the environment

```bash
# HDFS: should report "Live datanodes (2)"
docker exec -it namenode hdfs dfsadmin -report

# Spark + HDFS end-to-end test (must end with "ENVIRONMENT OK")
docker exec -it -u jovyan jupyter /usr/local/spark/bin/spark-submit /home/jovyan/work/src/check_cluster.py
```

## 3. Using Spark from a notebook

Open http://localhost:8888/?token=bigdata → folder `work/` (the repo itself) → new notebook:

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("bigdata-ecommerce")
         .master("spark://spark-master:7077")
         .config("spark.driver.host", "jupyter")
         .config("spark.executor.memory", "1g")
         .getOrCreate())

spark.read.parquet("hdfs://namenode:9000/tmp/check_cluster").show()
```

## 4. Pipeline steps

All parameters are in `config/pipeline.yaml`. Set `sample_mode: true` for development
(uses `data/sample.csv` and the `/data_sample` tree in HDFS) and `false` for final runs.

```bash
# development sample: first 2M rows of the full file
docker exec -u jovyan jupyter bash -c "head -n 2000001 /home/jovyan/work/data/2019-Nov.csv > /home/jovyan/work/data/sample.csv"

# B. zones + raw ingestion -> CSV to Parquet -> format comparison
docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/ingest/ingest_raw.py
docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/processing/etl.py
docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/ingest/compare_formats.py

# B. evidence of blocks and replication
docker exec namenode hdfs fsck /data/raw/ecommerce/2019-Nov.csv -files -blocks -locations
```

```bash
# C. MapReduce (Hadoop Streaming on YARN, runs inside the namenode container) vs Spark RDD
docker exec namenode bash /mapreduce/run_mapreduce.sh /data/raw/ecommerce/2019-Nov.csv /data/results/mr_brand_revenue
docker exec -it -u jovyan -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit src/mapreduce/rdd_brand_revenue.py
```

The `nodemanager` image is the class image plus `python3` (`docker/nodemanager/Dockerfile`),
because Hadoop Streaming runs the Python mapper/reducer there. It is built automatically by
`docker compose up -d --build`.

HDFS layout:

```
/data/raw/ecommerce/2019-Nov.csv              original file, untouched
/data/curated/events/event_date=2019-11-01/   clean Parquet, one folder per day
/data/results/                                query outputs
```

## 5. Stop

```bash
docker compose down        # stops containers, HDFS data is KEPT (volumes)
docker compose down -v     # deletes everything, including HDFS data
```

## Repository layout

```
docker-compose.yml   hadoop.env      environment (section A)
config/              pipeline configuration (section E)
data/                local dataset, not committed
src/ingest/          download + HDFS ingestion (section B)
src/mapreduce/       Hadoop Streaming + RDD version (section C)
src/processing/      ETL, queries, data quality (sections D, E)
src/ml/              MLlib baseline (section E)
notebooks/           exploration only
evidence/            screenshots, explain() plans, fsck output, timings
docs/                report and architecture diagram
```

## Troubleshooting

- **Containers keep restarting / exit 137** → Docker is out of memory. Windows: create
  `%UserProfile%\.wslconfig` with `[wsl2]` and `memory=8GB`, then `wsl --shutdown` and reopen Docker Desktop.
- **"name already in use"** → an old container with that name exists (e.g. `spark-course` from class
  is fine, but an old `namenode` is not): `docker rm -f namenode` or run `docker compose down` in the
  class `docker-hadoop` folder first.
- **Port already allocated** → stop the class containers (`docker stop spark-course`,
  `docker compose down` in `docker-hadoop`).
- **Spark job stuck at "Initial job has not accepted any resources"** → the worker has less memory than
  `spark.executor.memory`; keep it at `1g`, or check the worker in http://localhost:8080.
- **Jupyter login** → token is `bigdata`.
