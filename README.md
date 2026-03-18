# DEP303x — PySpark Project Template

## Cấu trúc

```
spark_project/
├── check_env.py              # Kiểm tra môi trường
├── requirements.txt
├── config/                   # Cấu hình (app.yaml, logging.yaml, ...)
├── data/
│   └── input/                # Để CSV / JSON đầu vào ở đây
├── notebooks/                # Jupyter notebooks
├── src/
│   ├── utils/
│   │   ├── spark_session.py  # SparkSession factory — dùng lại ở mọi job
│   │   ├── logger.py         # Colored logger
│   │   └── df_helpers.py     # read_csv, save_parquet, show_summary, ...
│   └── jobs/
│       ├── word_count.py     # Job mẫu 1
│       └── sales_agg.py      # Job mẫu 2
└── tests/                    # Unit tests
```

## Setup

```bash
conda activate spark-env
pip install -r requirements.txt
```

## Chạy

```bash
# Kiểm tra môi trường
python check_env.py

# Chạy job
python -m src.jobs.word_count
python -m src.jobs.sales_agg
```

## Thêm job mới

1. Tạo file `src/jobs/ten_job.py`
2. Import utils: `from src.utils import get_spark, get_logger`
3. Viết hàm `run()` và `if __name__ == "__main__": run()`

```python
from src.utils import get_spark, get_logger

log = get_logger(__name__)

def run():
    spark = get_spark("TenJob")
    log.header("Job: Ten Job")
    # ... logic của bạn
    spark.stop()

if __name__ == "__main__":
    run()
```
