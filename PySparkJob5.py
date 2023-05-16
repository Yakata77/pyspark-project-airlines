import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum, when, col
import os
from pathlib import Path

flights_path = os.path.join(os.path.dirname(__file__), 'flights.parquet')
airlines_path = os.path.join(os.path.dirname(__file__), 'airlines.parquet')
result_path = os.path.join(Path(__name__).parent, 'result2')

def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights_path = spark.read.format("parquet") \
        .option("header", "true") \
        .load(flights_path)
    airlines_path = spark.read.format("parquet") \
        .option("header", "true") \
        .load(airlines_path)

    airlines_data = airlines_path \
        .select(col('AIRLINE').alias('AIRLINE_NAME'),
                col('IATA_CODE'))

    flights_data = flights_path.select(flights_path.columns)

    joined_datamart = flights_data \
        .join(other=airlines_data, on=airlines_data['IATA_CODE'] == col('AIRLINE'), how='inner') \
        .groupBy(airlines_data['AIRLINE_NAME']) \
        .agg(sum(when(flights_data.CANCELLED == 0, 1).otherwise(0)).alias('correct_count'),
         sum(when(flights_data.DIVERTED == 1, 1).otherwise(0)).alias('diverted_count'),
         sum(when(flights_data.CANCELLED == 1, 1).otherwise(0)).alias('cancelled_count'),
         avg(flights_data.DISTANCE).alias('avg_distance'),
         avg(flights_data.AIR_TIME).alias('avg_air_time'),
         sum(when(flights_data.CANCELLATION_REASON == 'A', 1).otherwise(0)).alias('airline_issue_count'),
         sum(when(flights_data.CANCELLATION_REASON == 'B', 1).otherwise(0)).alias('weather_issue_count'),
         sum(when(flights_data.CANCELLATION_REASON == 'C', 1).otherwise(0)).alias('nas_issue_count'),
         sum(when(flights_data.CANCELLATION_REASON == 'D', 1).otherwise(0)).alias('security_issue_count')
        ) \
        .select(airlines_data['AIRLINE_NAME'],
                col('correct_count'),
                col('diverted_count'),
                col('cancelled_count'),
                col('avg_distance'),
                col('avg_air_time'),
                col('airline_issue_count'),
                col('weather_issue_count'),
                col('nas_issue_count'),
                col('security_issue_count'))\
        .orderBy(airlines_data['AIRLINE_NAME'])


    joined_datamart.write.format("parquet").mode("overwrite").save(result_path)

def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
