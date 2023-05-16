import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

flights_path = os.path.join(os.path.dirname(__file__), 'airlines.parquet')
result_path = os.path.join(Path(__name__).parent, 'result2')


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    flights_data = spark.read.format("parquet") \
        .option("header", "true") \
        .load(flights_path)
    popular_directions = flights_data \
        .groupBy(flights_data['ORIGIN_AIRPORT'],flights_data['DESTINATION_AIRPORT'])  \
        .agg(F.count(flights_data['TAIL_NUMBER']).alias('tail_count'), F.avg(flights_data['AIR_TIME']).alias("avg_air_time")) \
        .select(F.col('ORIGIN_AIRPORT'),
                F.col('DESTINATION_AIRPORT'),
                F.col('tail_count'),
                F.col('avg_air_time')) \
        .orderBy(F.col('tail_count').desc()) \
        .limit(10)

    popular_directions.write.format("parquet").mode("overwrite").save(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
