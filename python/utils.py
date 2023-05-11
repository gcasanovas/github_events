import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date
import sys
from functools import reduce
import logging

# ToDo: there must not be input_files in the final commit, everything should work without previously saved input_files

logging.basicConfig(level=logging.INFO)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName('app_name') \
    .config('spark.jars.packages', 'org.apache.spark:spark-hadoop-cloud_2.12:3.4.0') \
    .config('spark.driver.logLevel', 'INFO') \
    .config('spark.executor.logLevel', 'INFO') \
    .getOrCreate()


class Aggregator:
    """This class is used to generate data using the module search.py from gharchive.org, which contains data related
    to GitHub events. Once the data is retrieved, multiple aggregations are done and the results are saved into json
    files.

    Parameters:
        start_date: start date of the date range.
        end_date: end date of the date range.
        input_files_dir: directory to store the files obtained through the http requests.
        joined_files_dir: directory to store the files joined from the input_files_dir.

    Aggregations:
        repo_aggregates.json: for each repository and day, count the number of users that starred it
        (type = WatchEvent), the number of users that forked it (type = ForkEvent), the number issues created
        (type = IssuesEvent) and the number of PRs created (type = PullRequestEvent).

        user_aggregates.json: for each user and day, count the number of starred projects
        (type = WatchEvent), the number issues created (type = IssuesEvent) and the number of PRs created
        (type = PullRequestEvent).
    """
    def __init__(self, start_date: str, end_date: str, input_files_dir: str = None, joined_files_dir: str = None):
        self.start_date = start_date
        self.end_date = end_date
        self.input_files_dir = 'input_files' if input_files_dir is None else input_files_dir
        self.joined_files_dir = 'joined_files' if joined_files_dir is None else joined_files_dir

    def aggregate(self):
        def _get_aggregated_df(
                df: pyspark.sql.dataframe,
                key: list,
                field_to_aggregate: str,
                event_types: list) -> pyspark.sql.dataframe:
            """This method uses PySpark to process the data and returns a PySpark dataframe. It Aggregates each event
            filtering through the event type and saves each result to dfs list. Once all event types are processed,
            they're joined through their key.

            Parameters:
                df: PySpark dataframe with the data.
                key: key that identifies each row of the dataframe.
                field_to_aggregate: field to aggregate for, users or repositories.
                event_types: list with the type of events to aggregate.
            """

            logging.info('Aggregating data')

            dfs = []

            if 'fork_count' in event_types:
                fork_count = \
                    df.filter(col('type') == 'ForkEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('fork_count'))
                dfs.append(fork_count)
            if 'issue_count' in event_types:
                issue_count = \
                    df.filter(col('type') == 'IssuesEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('issue_count'))
                dfs.append(issue_count)
            if 'star_count' in event_types:
                star_count = \
                    df.filter(col('type') == 'WatchEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('star_count'))
                dfs.append(star_count)
            if 'pull_request_count' in event_types:
                pull_request_count = \
                    df.filter(col('type') == 'PullRequestEvent')\
                        .groupBy(key)\
                        .agg(countDistinct(field_to_aggregate).alias('pull_request_count'))
                dfs.append(pull_request_count)

            # Join all DataFrames on key
            result = reduce(lambda x, y: x.join(y, [key[0].split('.')[1], key[1].split('.')[1], key[2]]), dfs)
            return result

        # get_data = GetData(start_date=self.start_date,
        #                    end_date=self.end_date,
        #                    input_files_dir=self.input_files_dir,
        #                    joined_files_dir=self.joined_files_dir)
        #
        # filepath = get_data.import_data()

        # filepath = self._generate_file()
        # data = spark.read.json(filepath)
        data = spark.read.json('joined_files/output_file.json.gz')
        data = data.withColumn('date', to_date('created_at'))

        # filepath = _generateFile()
        filepath = '../joined_files/output_file.json.gz'

        # Repo aggregate data
        repo_df = _get_aggregated_df(
            df=data,
            key=['repo.id', 'repo.name', 'date'],
            field_to_aggregate='actor.id',
            # For each repo aggregate WatchEvent, ForkEvent, IssuesEvent and PullRequestEvent,
            event_types=['fork_count', 'issue_count', 'star_count', 'pull_request_count']
        )

        # User aggregate data
        user_df = _get_aggregated_df(
            df=data,
            key=['actor.id', 'actor.login', 'date'],
            field_to_aggregate='repo.id',
            # For each user aggregate WatchEvent, IssuesEvent and PullRequestEvent
            event_types=['fork_count', 'issue_count', 'star_count']
        )

        # ToDo: Save data to json file
        # repo_df.write.json('output/repo_aggregates.json')
        # user_df.write.json('output/user_aggregates.json')
        logging.info('Final results saved inside the output directory')

aggregator = Aggregator(start_date='2015-01-01 00:00:00', end_date='2015-01-03 00:00:00')
aggregator.aggregate()
