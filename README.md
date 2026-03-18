# DEP303x — PySpark Project

> Practical Apache Spark with Python — covering RDD, DataFrame, SparkSQL, Structured Streaming, and Kafka integration.

---

## 📋 Table of Contents

- [Requirements](#requirements)
- [Setup](#setup)
- [Project Structure](#project-structure)
- [Running Jobs](#running-jobs)
- [Jobs Reference](#jobs-reference)
- [Kafka Setup](#kafka-setup)
- [Adding a New Job](#adding-a-new-job)

---

## ✅ Requirements

| Tool | Version |
|---|---|
| Python | 3.11 |
| Java | 17 (via Homebrew) |
| PySpark | 4.1.1 |
| Conda | Miniconda |
| Docker | For Kafka labs |

---

## ⚙️ Setup

```bash
# 1. Create conda environment
conda create -n spark-env python=3.11 -y
conda activate spark-env

# 2. Set Java 17 (add to ~/.zshrc)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# 3. Install dependencies
pip install -r requirements.txt

# 4. Verify environment
python check_env.py
```

---

## 🗂 Project Structure

```
spark_project/
├── run.sh                          # Interactive menu — entry point
├── check_env.py                    # Environment check (Python, Java, Spark)
├── docker-compose.yml              # Kafka + Zookeeper for streaming labs
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── input/                      # Source datasets (not committed to git)
│   └── output/                     # Job outputs (not committed to git)
│
└── src/
    ├── utils/
    │   ├── spark_session.py        # SparkSession factory — single init point
    │   ├── logger.py               # Colored console logger (✔ ⚠ ✘ →)
    │   ├── df_helpers.py           # read_csv, read_parquet, save_parquet, show_summary
    │   ├── kafka_producer.py       # Send invoice JSON to Kafka topic "invoices"
    │   └── kafka_trades_producer.py # Send trade data to Kafka topic "trades"
    │
    └── jobs/
        ├── word_count.py           # Lab 2 — Word Count with regex + sort
        ├── sales_agg.py            # Lab 2 — Sales aggregation, write Parquet
        ├── ratings_counter.py      # Lab 2 — MovieLens ratings histogram (RDD)
        ├── avg_friends_by_age.py   # Lab 2.1 — Average friends by age (RDD)
        ├── min_temperature.py      # Lab 2.2 — Min temperature by station (RDD)
        ├── flight_source_sink.py   # Lab 3 — Read CSV/Parquet, repartition, write
        ├── hello_spark_sql.py      # Lab 4.1 — SparkSQL temp view query
        ├── spark_sql_table_demo.py # Lab 4.2 — Managed Table in Spark catalog
        ├── row_demo.py             # Lab 5.1 — Convert string dates to DateType
        ├── log_file_demo.py        # Lab 5.2 — Extract fields from log with regex
        ├── column_demo.py          # Lab 5.3 — Column String vs Object vs Expression
        ├── udf_demo.py             # Lab 5.4 — UDF with regex, two registration methods
        ├── agg_demo.py             # Lab 6.1 — Aggregation: count, sum, avg, groupBy
        ├── windowing_demo.py       # Lab 6.2 — Running total with Window functions
        ├── join_demo.py            # Lab 6.3 — Inner and Left Outer Join
        ├── most_popular_superhero.py # Lab 7.1 — Graph analysis with Marvel dataset
        ├── file_stream_demo.py     # Lab 8.1 — Structured Streaming from JSON files
        ├── multi_query_demo.py     # Lab 9.1 — Kafka source → dual streaming sinks
        └── tumbling_window_demo.py # Lab 10.1 — Tumbling Window on Kafka stream
```

---

## 🚀 Running Jobs

All jobs are accessible through the interactive menu:

```bash
conda activate spark-env
cd spark_project
./run.sh
```

```
DEP303x — PySpark Project
────────────────────────────────────────────────────
  1   Check: Environment
  2   Job: Word Count
  3   Job: Sales Aggregation
  4   Job: Ratings Histogram (MovieLens)
  5   Job: Average Friends by Age
  6   Job: Minimum Temperature
  7   Job: Flight Source and Sink
  8   Job: Hello SparkSQL
  9   Job: SparkSQL Table Demo
  10  Job: Row Demo
  11  Job: Log File Demo
  12  Job: Column Demo
  13  Job: UDF Demo
  14  Job: Aggregation Demo
  15  Job: Window Function Demo
  16  Job: Join Demo
  17  Job: Most Popular Superhero
  18  Job: File Stream Demo
  19  Job: Multi Query Demo (Kafka)
  20  Kafka Producer (invoices)
  21  Install dependencies
  22  Clean __pycache__
  23  Job: Tumbling Window Demo (Kafka)
  24  Kafka Trades Producer
  q   Exit
```

Or run any job directly:

```bash
python -m src.jobs.word_count
python -m src.jobs.sales_agg
```

---

## 📚 Jobs Reference

### Batch Jobs

| # | Job | Dataset | Key Concepts |
|---|---|---|---|
| 2 | `word_count` | inline | Regex split, lowercase, sort |
| 3 | `sales_agg` | `sales.csv` | GroupBy, agg, write Parquet |
| 4 | `ratings_counter` | `ml-100k/u.data` | RDD, countByValue |
| 5 | `avg_friends_by_age` | `fakefriends.csv` | RDD mapValues, reduceByKey |
| 6 | `min_temperature` | `1800.csv` | RDD filter, reduceByKey, °F conversion |
| 7 | `flight_source_sink` | `flight-time.csv` | StructType schema, repartition, partitionBy |
| 8 | `hello_spark_sql` | `survey.csv` | createOrReplaceTempView, spark.sql |
| 9 | `spark_sql_table_demo` | `flight-time.parquet` | Managed Table, Spark catalog |
| 10 | `row_demo` | inline | to_date, withColumn, StructType |
| 11 | `log_file_demo` | `apache_logs.txt` | regexp_extract, substring_index |
| 12 | `column_demo` | `airlines.csv` | Column String vs Object vs selectExpr |
| 13 | `udf_demo` | `survey.csv` | UDF regex, udf() vs spark.udf.register() |
| 14 | `agg_demo` | `invoices.csv` | count, sum, avg, countDistinct, SparkSQL |
| 15 | `windowing_demo` | `summary.parquet` | Window, partitionBy, rowsBetween, running total |
| 16 | `join_demo` | inline | Inner Join, Left Outer Join, drop duplicate key |
| 17 | `most_popular_superhero` | `Marvel_Names.csv` + `Marvel_Graph.txt` | Graph, split, size, groupBy, join |

### Streaming Jobs

| # | Job | Source | Sink | Key Concepts |
|---|---|---|---|---|
| 18 | `file_stream_demo` | JSON files | JSON files | readStream, explode, writeStream append |
| 19 | `multi_query_demo` | Kafka `invoices` | Kafka `notifications` + JSON files | Dual queries, from_json, awaitAnyTermination |
| 23 | `tumbling_window_demo` | Kafka `trades` | Console | Tumbling Window 15min, update mode |

---

## 🐳 Kafka Setup

Required for jobs 19, 20, 23, 24.

```bash
# Start Kafka + Zookeeper
docker-compose up -d

# Verify running
docker-compose ps

# Stop
docker-compose down
```

### Send data to Kafka

```bash
# Send invoice data (for job 19)
./run.sh  # option 20

# Send trade data (for job 23)
./run.sh  # option 24
```

### Run streaming jobs

```bash
# Multi Query Demo (job 19)
./run.sh  # option 19

# Tumbling Window Demo (job 23)
./run.sh  # option 23

# Stop with Ctrl+C when done
```

---

## ➕ Adding a New Job

1. Create `src/jobs/my_job.py`
2. Follow this template:

```python
"""
Job: My Job
Brief description of what this job does.

Run:   python -m src.jobs.my_job
Input: data/input/my_data.csv
"""

from __future__ import annotations

from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)


def run() -> None:
    spark = get_spark("MyJob")
    log.header("Job: My Job")

    # your logic here

    log.ok("Done!")
    spark.stop()


if __name__ == "__main__":
    run()
```

3. Add to `run.sh` — new function + menu entry + case

```bash
run_my_job() {
    header "Job: My Job"
    python -m src.jobs.my_job
}
```

---

## 🔧 Utils Reference

```python
from src.utils import get_spark, get_logger, read_csv, read_parquet, save_parquet, show_summary

spark = get_spark("AppName")          # SparkSession — local[*], ERROR log level
log   = get_logger(__name__)          # Colored logger: log.ok / .warn / .error / .info / .header

df = read_csv(spark, "path/to/file.csv")        # Read CSV with header
df = read_parquet(spark, "path/to/file.parquet") # Read Parquet
save_parquet(df, "path/to/output")               # Write Parquet (overwrite)
show_summary(df, "Label")                        # Print schema + 5 rows + count
```