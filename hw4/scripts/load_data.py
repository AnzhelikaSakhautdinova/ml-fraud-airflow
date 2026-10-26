"""Load labelled CSV into the source table; no Kafka dependency for dbt homework."""
import argparse
import base64
import csv
import hashlib
import json
import math
import os
from pathlib import Path
from urllib.request import Request, urlopen


def query(sql):
    auth = base64.b64encode((os.getenv('CLICKHOUSE_USER', 'homework') + ':' + os.getenv('CLICKHOUSE_PASSWORD', 'homework_local')).encode()).decode()
    host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    request = Request(f'http://{host}:8123', data=sql.encode(), headers={'Authorization': 'Basic '+auth})
    with urlopen(request, timeout=120) as response:
        return response.read().decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv')
    parser.add_argument('--replace', action='store_true')
    args = parser.parse_args()
    query('CREATE DATABASE IF NOT EXISTS transactions_db')
    query('''CREATE TABLE IF NOT EXISTS transactions_db.transactions (
        transaction_id String, transaction_time DateTime('UTC'), merch String, cat_id String,
        amount Float64, name_1 String, name_2 String, gender String, us_state String,
        lat Float64, lon Float64, merchant_lat Float64, merchant_lon Float64, target UInt8
    ) ENGINE=MergeTree ORDER BY tuple()''')
    if not args.replace and int(query('SELECT count() FROM transactions_db.transactions')):
        print('Source already loaded; use --replace to load a different CSV')
        return
    # Publish the new source only after the entire file has been loaded successfully.
    query('DROP TABLE IF EXISTS transactions_db.transactions_load')
    query('CREATE TABLE transactions_db.transactions_load AS transactions_db.transactions')
    path = Path(args.csv)
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    batch = []
    count = 0
    with path.open(encoding='utf-8-sig', newline='') as source:
        for index, row in enumerate(csv.DictReader(source)):
            target = row.get('target')
            if target not in ('0', '1'):
                raise ValueError('Labelled train.csv required; missing/invalid target')
            item = {key: row[key] for key in ['transaction_time', 'merch', 'cat_id', 'name_1', 'name_2', 'gender', 'us_state']}
            if len(item['transaction_time']) == 16:
                item['transaction_time'] += ':00'
            for key in ['amount', 'lat', 'lon', 'merchant_lat', 'merchant_lon']:
                item[key] = float(row[key])
                if not math.isfinite(item[key]):
                    raise ValueError(f'Invalid {key}')
            item.update(transaction_id=f'{digest[:24]}-{index}', target=int(target))
            batch.append(json.dumps(item))
            count += 1
            if len(batch) == 5000:
                query('INSERT INTO transactions_db.transactions_load FORMAT JSONEachRow\n'+'\n'.join(batch))
                batch.clear()
        if batch:
            query('INSERT INTO transactions_db.transactions_load FORMAT JSONEachRow\n'+'\n'.join(batch))
    if not count:
        raise ValueError('Empty dataset')
    query('EXCHANGE TABLES transactions_db.transactions AND transactions_db.transactions_load')
    query('DROP TABLE transactions_db.transactions_load')
    print(f'Loaded {count} labelled rows; sha256={digest}', flush=True)


if __name__ == '__main__':
    main()
