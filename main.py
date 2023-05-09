import pyspark
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sequence, to_utc_timestamp, to_timestamp, expr, lit, hour, date_add
from pyspark.sql.types import TimestampType

# References
# https://stackoverflow.com/questions/50624745/pyspark-how-to-duplicate-a-row-n-time-in-dataframe
# https://stackoverflow.com/questions/33681487/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark
# https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.array_repeat.html


# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('app_name') \
        .config('spark.jars.packages', 'org.apache.spark:spark-hadoop-cloud_2.12:3.4.0') \
        .config('spark.driver.logLevel', 'INFO') \
        .config('spark.executor.logLevel', 'INFO') \
        .getOrCreate()
    start_hour, end_hour = 0, 23
    df = spark.createDataFrame([('2022-01-01', '2022-02-01', '0', '23')],
                               ['start_date', 'end_date', 'start_hour', 'end_hour'])
    # Create a sequence between the two dates and pass each value to a row
    # df = df.select(
    #     explode(
    #         sequence(
    #             to_timestamp(df.start_date),
    #             to_timestamp(df.end_date)
    #         )
    #     ).alias('date')
    # )
    # df = df.select(sequence(to_timestamp(df.start_date), to_timestamp(df.end_date)).alias('date'))
    df = df.select(list(range(int(df.first()['start_hour']), int(df.first()['end_hour']) + 1)))
    # df = df.select('date', sequence(df.start_hour, df.end_hour).alias('hour'))
    df.show()

    # Create a new column with a constant value of 24 (hours of the day)
    # df = df.withColumn('n', lit(24))
    # df = df.withColumn('hour', sequence('0', '24'))
    # Create an array with 24 values and explode it
    # df = df.withColumn('n', expr('explode(array_repeat(n,int(n)))'))
    # df = df.withColumn('hour', hour(date_add('date', 'hour_offset')))
    # df.show()

#
# from pyspark.sql.functions import explode, col, concat, lit
# from pyspark.sql.types import TimestampType
#
# # create DataFrame with start and end dates and hours
# df = spark.createDataFrame([('2022-01-01', '2022-02-01', '0', '23')], ['start_date', 'end_date', 'start_hour', 'end_hour'])
#
# # create a sequence of timestamps between start_date and end_date with hourly granularity
# timestamps = df.select(
#     explode(
#         # create a list of hourly offsets from start_hour to end_hour
#         list(range(int(df.first()['start_hour']), int(df.first()['end_hour']) + 1))
#     ).alias('hour_offset')
# ).withColumn(
#     # add the hour offset to the start_date to create a timestamp
#     'timestamp',
#     concat(col('hour_offset'), lit(':00:00'))
# ).withColumn(
#     # convert the timestamp to a TimestampType column
#     'timestamp',
#     col('timestamp').cast(TimestampType())
# ).withColumn(
#     # add the date to the timestamp
#     'timestamp',
#     concat(df.first()['start_date'], lit(' '), col('timestamp'))
# ).withColumn(
#     # convert the timestamp to a TimestampType column
#     'timestamp',
#     col('timestamp').cast(TimestampType())
# )
#
# # show the result
# timestamps.show()
