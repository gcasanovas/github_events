import datetime
from datetime import timedelta
import requests
import os
import json
import gzip

# ToDo: Use more try-except
# ToDo: Use logging module to give info about the % of files loaded
# ToDo: Use logging to log more info

class GetData:

    def __init__(self, start_date, end_date, input_files_dir, joined_files_dir):
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        self.input_files_dir = input_files_dir
        self.joined_files_dir = joined_files_dir

    def _generate_urls(self):
        current_date = self.start_date
        result = []

        def _parse_link(date):
            string_date = date.strftime('%Y-%m-%d %H:%M:%S')
            link_date = string_date.replace(' ', '-')[:13]
            return f'https://data.gharchive.org/{link_date}.json.gz'

        while current_date < self.end_date:
            result.append(_parse_link(current_date))
            current_date += timedelta(hours=1)

        # Append the last hour of the end date
        result.append(_parse_link(self.end_date.replace(hour=23)))
        return result

    def _import_data(self):
        urls = self._generate_urls()
        for url in urls:
            response = requests.get(url)
            if response.status_code == 200:
                file_path = os.path.join(self.input_files_dir, os.path.basename(url))
                with open(file_path, 'wb') as f:
                    f.write(response.content)

    def _join_files(self):

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
        return joined_files_filepath

    def get(self):
        self._import_data()
        return self._join_files()

