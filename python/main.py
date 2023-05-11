import sys
from utils import Aggregator

start_date = sys.argv[1]
end_date = sys.argv[2]

if __name__ == '__main__':
    try:
        aggregate = Aggregator(
            start_date=start_date,
            end_date=end_date
        )

        aggregate.aggregate()

    except Exception as e:
        print(f'Error: {e}')