import datetime
from datetime import timedelta
import requests
import os
import json
import gzip
import logging
import time

logging.basicConfig(level=logging.INFO)

# ToDo: Use more try-except
# ToDo: Use logging module to give info about the % of files loaded
# ToDo: Use logging to log more info


class GetData:
    """This class is used to make http requests to gharchive.org website using a specified daterange. For each hour
    between the date range, a http request is made and the data received is stored into a single json.gz file. The
    resulting files are joined into a single json.gz file.

    Parameters:
        start_date: start date of the date range.
        end_date: end date of the date range.
        input_files_dir: directory to store the files obtained through the http requests.
        joined_files_dir: directory to store the files joined from the input_files_dir.

    Methods:
        import_data: makes the http request for each hour between the date range and saves the results into json.gz
        files. The files are stored into the directory specified to the input_files_dir parameter.

        join_files: joins all json.gz files inside the directory specified in the input_files_dir parameter and saves
        the result into the directory specified in the joined_files_dir parameter."""

    def __init__(self, start_date: str, end_date: str, input_files_dir: str, joined_files_dir: str):
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        self.input_files_dir = input_files_dir
        self.joined_files_dir = joined_files_dir

    def _generate_urls(self) -> list:
        """This internal method is used to generate a url for each hour between the date range"""
        logging.info('Generating urls')
        current_date = self.start_date
        result = []

        def _parse_link(date) -> str:
            """This internal method parses the link to make the http request using the dates and hours extracted
            from the date range"""
            string_date = date.strftime('%Y-%m-%d %H:%M:%S')
            link_date = string_date.replace(' ', '-')[:13]
            return f'https://data.gharchive.org/{link_date}.json.gz'

        while current_date < self.end_date:
            result.append(_parse_link(current_date))
            current_date += timedelta(hours=1)

        # Append the last hour of the end date
        result.append(_parse_link(self.end_date.replace(hour=23)))
        return result

    # ToDo: I think it would have been very beneficial to use threads to make the http requests, but I hadn't more time to investigate about it
    def import_data(self):
        """Makes the http request for each hour between the date range and saves the results into json.gz files.
        The files are stored into the directory specified to the input_files_dir parameter."""
        urls = self._generate_urls()
        start_time = time.time()
        for i, url in enumerate(urls):
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    file_path = os.path.join(self.input_files_dir, os.path.basename(url))
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
            except requests.exceptions.Timeout:
                logging.error(f'Request for the following url failed: {url}')
            if i > len(urls)/2:
                logging.info(f'Making http requests, less than 50% remaining. Time elapsed: {start_time}')
            else:
                logging.info(f'Making http requests, more than 50% remaining. Time elapsed: {start_time}')
        logging.info(f'All http requests done. Total time: {time.time() - start_time}')

    def join_files(self) -> str:
        """Joins all json.gz files inside the directory specified in the input_files_dir parameter and saves the
        result into the directory specified in the joined_files_dir parameter."""
        logging.info('Joining all files into one')
        joined_files_filepath = f'{self.joined_files_dir}/output_file.json.gz'

        # Open the output file for writing
        with gzip.open(joined_files_filepath, 'wt', encoding='utf-8') as output_file:

            # Iterate over the input input_files
            for file_name in os.listdir(self.input_files_dir):
                if file_name.endswith('.json.gz'):
                    with gzip.open(os.path.join(self.input_files_dir, file_name), 'rt', encoding='utf-8') as input_file:

                        # Iterate over the lines in the input file and write them to the output file
                        for line in input_file:
                            data = json.loads(line)
                            json.dump(data, output_file)
                            output_file.write('\n')
            logging.info(f'All files joined and saved into: {output_file}')
        return joined_files_filepath
