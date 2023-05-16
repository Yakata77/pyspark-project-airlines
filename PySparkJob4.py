import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

flights_path = os.path.join(os.path.dirname(__file__), 'flights.parquet')
airlines_path = os.path.join(os.path.dirname(__file__), 'airlines.parquet')
airports_path = os.path.join(os.path.dirname(__file__), 'airports.parquet')
result_path = os.path.join(Path(__name__).parent, 'result2')


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    flights_path = spark.read.format("parquet") \
        .option("header", "true") \
        .load(flights_path)
    airlines_path = spark.read.format("parquet") \
        .option("header", "true") \
        .load(airlines_path)
    airports_path = spark.read.format("parquet") \
        .option("header", "true") \
        .load(airports_path)

    airlines_data = airlines_path \
        .select(F.col('AIRLINE').alias('AIRLINE_NAME'),
                F.col('IATA_CODE'))

    flights_data = flights_path \
        .select(F.col('TAIL_NUMBER'),
                F.col('AIRLINE'),
                F.col('DESTINATION_AIRPORT'),  # переименовываем столбец
                F.col('ORIGIN_AIRPORT'))  # переименовываем столбец

    airports_data = airports_path \
        .select(F.col('COUNTRY').alias('ORIGIN_COUNTRY'),
                F.col('IATA_CODE').alias('ORIGIN_AIRPORT_ID'),  # переименовываем столбец
                F.col('AIRPORT').alias('ORIGIN_AIRPORT_NAME'),
                F.col('LATITUDE').alias('ORIGIN_LATITUDE'),
                F.col('LONGITUDE').alias('ORIGIN_LONGITUDE'))

    airports_data2 = airports_data.withColumnRenamed('ORIGIN_AIRPORT_ID', 'DESTINATION_AIRPORT_ID')
    airports_data3 = airports_data2.withColumnRenamed('ORIGIN_COUNTRY', 'DESTINATION_COUNTRY')
    airports_data4 = airports_data3.withColumnRenamed('ORIGIN_AIRPORT_NAME', 'DESTINATION_AIRPORT_NAME')
    airports_data5 = airports_data4.withColumnRenamed('ORIGIN_LATITUDE', 'DESTINATION_LATITUDE')
    airports_data6 = airports_data5.withColumnRenamed('ORIGIN_LONGITUDE', 'DESTINATION_LONGITUDE')

    joined_datamart = flights_data \
        .join(other=airlines_data, on=airlines_data['IATA_CODE'] == F.col('AIRLINE'), how='inner') \
        .join(other=airports_data, on=airports_data['ORIGIN_AIRPORT_ID'] == F.col('ORIGIN_AIRPORT'), how='inner') \
        .join(other=airports_data6, on=airports_data6['DESTINATION_AIRPORT_ID'] == F.col('DESTINATION_AIRPORT'), how='inner') \
        .select(airlines_data['AIRLINE_NAME'],
                F.col('TAIL_NUMBER'),
                airports_data['ORIGIN_COUNTRY'],
                airports_data['ORIGIN_AIRPORT_NAME'],
                airports_data['ORIGIN_LATITUDE'],
                airports_data['ORIGIN_LONGITUDE'],
                airports_data6['DESTINATION_COUNTRY'],
                airports_data6['DESTINATION_AIRPORT_NAME'],
                airports_data6['DESTINATION_LATITUDE'],
                airports_data6['DESTINATION_LONGITUDE']
                )

    joined_datamart.write.format("parquet").mode("overwrite").save(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet',
                        help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
