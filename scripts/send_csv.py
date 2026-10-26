import argparse
import csv
from pathlib import Path
import uuid
from app.broker import producer, publish


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', nargs='?', default='examples/transactions.csv')
    args = parser.parse_args()
    client = producer()
    with Path(args.csv).open() as source:
        for index, row in enumerate(csv.DictReader(source)):
            tid = uuid.uuid4().hex
            publish(client, 'transactions', {'transaction_id': tid, 'data': row}, tid)
    print(f'Sent {index+1} transactions')


if __name__ == '__main__':
    main()
