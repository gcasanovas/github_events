import os
import requests
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import datetime
from search import GetData
import sys

# ToDo: there must not be input_files in the final commit, everything should work without previously saved input_files

# spark = SparkSession.builder.appName('github_events').getOrCreate()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName('app_name') \
    .config('spark.jars.packages', 'org.apache.spark:spark-hadoop-cloud_2.12:3.4.0') \
    .config('spark.driver.logLevel', 'INFO') \
    .config('spark.executor.logLevel', 'INFO') \
    .getOrCreate()


class Aggregate:
    def __init__(self, start_date, end_date, input_files_dir=None, joined_files_dir=None):
        self.start_date = start_date
        self.end_date = end_date
        self.input_files_dir = 'input_files' if input_files_dir is None else input_files_dir
        self.joined_files_dir = 'joined_files' if joined_files_dir is None else joined_files_dir

    def aggregate(self):
        # get_data = GetData(start_date=self.start_date,
        #                    end_date=self.end_date,
        #                    input_files_dir=self.input_files_dir,
        #                    joined_files_dir=self.joined_files_dir)
        # filepath = get_data.get()
        filepath = 'joined_files/output_file.json.gz'
        df = spark.read.json(filepath)
        df.show()

aggregator = Aggregate(start_date='2015-01-01 00:00:00', end_date='2015-01-03 00:00:00')
aggregator.aggregate()
