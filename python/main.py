import sys
from utils import Aggregator
from datetime import datetime

start_date = datetime.strptime(f'{sys.argv[1]} 00:00:00', '%Y-%m-%d %H:%M:%S')
end_date = datetime.strptime(f'{sys.argv[2]} 00:00:00', '%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    try:
        aggregate = Aggregator(
            start_date=start_date,
            end_date=end_date
        )

        aggregate.aggregate()

    except Exception as e:
        print(f'Error: {e}')
