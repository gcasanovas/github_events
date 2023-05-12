import datetime
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date
import sys
from functools import reduce
import logging
import shutil
from search import GetData

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

    Aggregations:
        repo_aggregates.json: for each repository and day, count the number of users that starred it
        (type = WatchEvent), the number of users that forked it (type = ForkEvent), the number issues created
        (type = IssuesEvent) and the number of PRs created (type = PullRequestEvent).

        user_aggregates.json: for each user and day, count the number of starred projects
        (type = WatchEvent), the number issues created (type = IssuesEvent) and the number of PRs created
        (type = PullRequestEvent).
    """
    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.input_files_dir = os.path.join(script_dir, '../input_files')
        self.joined_files_dir = os.path.join(script_dir, '../joined_files')
        self.output_file_dir = os.path.join(script_dir, '../output')
        self.execution_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")\
            .replace(' ', '-').replace(':', '-')

    def aggregate(self):
        def _get_aggregated_df(
                df: pyspark.sql.dataframe,
                key: list,
                field_to_aggregate: str,
                event_types: list,
                filename: str,
                output_dir: str
        ) -> pyspark.sql.dataframe:
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
            return result, os.path.join(output_dir, filename)

        get_data = GetData(start_date=self.start_date,
                           end_date=self.end_date,
                           input_files_dir=self.input_files_dir,
                           joined_files_dir=self.joined_files_dir,
                           execution_datetime=self.execution_datetime)

        filepath = get_data.get()

        data = spark.read.json(filepath)
        data = data.withColumn('date', to_date('created_at'))

        # Repo aggregate data
        repo_df, repo_output_filepath = _get_aggregated_df(
            df=data,
            key=['repo.id', 'repo.name', 'date'],
            field_to_aggregate='actor.id',
            # For each repo aggregate WatchEvent, ForkEvent, IssuesEvent and PullRequestEvent
            event_types=['fork_count', 'issue_count', 'star_count', 'pull_request_count'],
            filename=f'repo_aggregates_{self.execution_datetime}.csv',
            output_dir=self.output_file_dir
        )

        # User aggregate data
        user_df, user_output_filepath = _get_aggregated_df(
            df=data,
            key=['actor.id', 'actor.login', 'date'],
            field_to_aggregate='repo.id',
            # For each user aggregate WatchEvent, IssuesEvent and PullRequestEvent
            event_types=['fork_count', 'issue_count', 'star_count'],
            filename=f'user_aggregates_{self.execution_datetime}.csv',
            output_dir=self.output_file_dir
        )

        try:
            # Try to write results to a csv with PySpark
            repo_df.write.csv(repo_output_filepath, mode='overwrite')
            user_df.write.csv(user_output_filepath, mode='overwrite')
            logging.info('Process finished with exit. Final results saved inside the output directory')
        except Exception as e:
            logging.error(f'Error while writing csv file with PySpark: {e}')
            try:
                logging.info('Trying to write csv with pandas')
                # If the PySpark write.csv fails, it leaves files/directories that can make the pandas .to_csv to fail,
                # the entire output directory is removed to prevent this to happen
                shutil.rmtree(self.output_file_dir)
                # Create the directory again
                os.makedirs(self.output_file_dir)
                repo_pd_df = repo_df.toPandas()
                user_pd_df = user_df.toPandas()
                # Write results to a csv with pandas
                repo_pd_df.to_csv(repo_output_filepath, index=False)
                user_pd_df.to_csv(user_output_filepath, index=False)
                logging.info('Process finished with exit. Final results saved inside the output directory')
            except Exception as e:
                raise e
