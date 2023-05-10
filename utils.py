import os
import requests
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from datetime import datetime, timedelta
import datetime
from search import GetData
import sys
from functools import reduce

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

    # def _generateFile(self):
        # get_data = GetData(start_date=self.start_date,
        #                    end_date=self.end_date,
        #                    input_files_dir=self.input_files_dir,
        #                    joined_files_dir=self.joined_files_dir)
        # return get_data.get()
    def aggregate(self):
        def _get_aggregated_df(
                filepath_to_data: str,
                key: list,
                field_to_aggregate: str,
                aggregates: list) -> pyspark.sql.dataframe:

            dfs = []
            df = spark.read.json(filepath_to_data)

            if 'fork_count' in aggregates:
                fork_count = \
                    df.filter(col('type') == 'ForkEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('fork_count'))
                dfs.append(fork_count)
            if 'issue_count' in aggregates:
                issue_count = \
                    df.filter(col('type') == 'IssuesEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('issue_count'))
                dfs.append(issue_count)
            if 'star_count' in aggregates:
                star_count = \
                    df.filter(col('type') == 'WatchEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('star_count'))
                dfs.append(star_count)
            if 'pull_request_count' in aggregates:
                pull_request_count = \
                    df.filter(col('type') == 'PullRequestEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('pull_request_count'))
                dfs.append(pull_request_count)

            # Join all DataFrames on key
            result = reduce(lambda x, y: x.join(y, [key[0].split('.')[1], key[1].split('.')[1]]), dfs)
            return result

        # filepath = self._generate_file()
        filepath = 'joined_files/output_file.json.gz'

        # Repo aggregate data
        repo_df = _get_aggregated_df(
            filepath_to_data=filepath,
            key=['repo.id', 'repo.name'],
            field_to_aggregate='actor.id',
            # For each repo: WatchEvent, ForkEvent, IssuesEvent, PullRequestEvent,
            aggregates=['fork_count', 'issue_count', 'star_count', 'pull_request_count']
        )

        # User aggregate data
        user_df = _get_aggregated_df(
            filepath_to_data=filepath,
            key=['actor.id', 'actor.login'],
            field_to_aggregate='repo.id',
            # For each user: WatchEvent, IssuesEvent, PullRequestEvent
            aggregates=['fork_count', 'issue_count', 'star_count']
        )

        # ToDo: Save data to json file

aggregator = Aggregate(start_date='2015-01-01 00:00:00', end_date='2015-01-03 00:00:00')
aggregator.aggregate()
