from src.utils import get_spark, get_logger
import collections

log = get_logger(__name__)

def run() -> None:
    spark = get_spark("RatingsHistogram")
    sc = spark.sparkContext  # lấy SparkContext từ session có sẵn

    lines = sc.textFile("data/input/ml-100k/u.data")
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()

    sorted_results = collections.OrderedDict(sorted(result.items()))
    for key, value in sorted_results.items():
        log.info(f"Rating {key}: {value}")

    spark.stop()

if __name__ == "__main__":
    run()
