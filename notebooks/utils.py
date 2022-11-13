from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

def init_spark():
    spark = SparkSession.builder.master("local[4]").appName('spark') \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")\
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")\
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")\
        .getOrCreate()

    return spark

@udf(StringType())
def median_age_class(age):
    response = None
    if age >= 37:
        response = 'acima_37'
    if age >=29 and age < 37:
        response = 'ate_37'
    if age >= 18 and age < 29:
        response = 'ate_29'
    if age < 18:
        response = 'de_0_ate_18'

    return response

@udf(StringType())
def north_south_latitude(latitude):
    return 'norte' if latitude > 36 else 'sul'

@udf(StringType())
def west_east_longitude(longitude):
    return 'oeste' if longitude < -119 else 'leste'