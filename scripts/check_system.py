"""Manual Airflow API runs plus Kafka scoring/version checks, no model shortcuts."""
import base64
import csv
import json
import time
from urllib.request import Request, urlopen
import uuid
from pathlib import Path
from confluent_kafka import Consumer
from app.broker import BOOTSTRAP, producer, publish
from app.worker import Scorer
from pipeline import storage


def api(path, value=None):
    request = Request('http://airflow:8080/api/v1/' + path,
                      data=json.dumps(value).encode() if value is not None else None,
                      headers={'Content-Type': 'application/json',
                               'Authorization': 'Basic ' + base64.b64encode(b'admin:admin').decode()})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def trigger(dag):
    run_id = 'verify_' + uuid.uuid4().hex
    api(f'dags/{dag}/dagRuns', {'dag_run_id': run_id})
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        state = api(f'dags/{dag}/dagRuns/{run_id}')['state']
        if state == 'success':
            return run_id
        if state == 'failed':
            raise AssertionError(f'DAG failed: {dag}/{run_id}')
        time.sleep(3)
    raise AssertionError(f'DAG timeout: {dag}')


def kafka_probe():
    with Path('examples/transactions.csv').open() as source:
        row = next(csv.DictReader(source))
    tid = 'verify-' + uuid.uuid4().hex
    expected = Scorer().score({'transaction_id': tid, 'data': row})
    client = producer()
    publish(client, 'transactions', {'transaction_id': tid, 'data': row}, tid)
    consumer = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': tid,
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe(['scores'])
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if not message:
                continue
            if message.error():
                raise RuntimeError(message.error())
            value = json.loads(message.value())
            if value['transaction_id'] == tid:
                assert value['model_version'] == expected['model_version']
                assert abs(value['score'] - expected['score']) < 1e-10
                assert value['fraud_flag'] == expected['fraud_flag']
                return value
        raise AssertionError('No score received')
    finally:
        consumer.close()


def main():
    # Pause schedules during deterministic manual verification, restore afterwards.
    dags = ['supply_labeled_month', 'retrain_fraud_model', 'data_quality_report']
    for dag in dags:
        req = Request('http://airflow:8080/api/v1/dags/' + dag,
            data=b'{"is_paused":true}', method='PATCH', headers={'Content-Type':'application/json',
            'Authorization':'Basic '+base64.b64encode(b'admin:admin').decode()})
        with urlopen(req):
            pass
    # Airflow 2 permits manual runs of paused DAGs once unpaused; each stage is unpaused in order.
    def set_paused(dag, paused):
        req = Request('http://airflow:8080/api/v1/dags/' + dag,
            data=json.dumps({'is_paused':paused}).encode(), method='PATCH',
            headers={'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode(b'admin:admin').decode()})
        with urlopen(req):
            pass
    runs = {}
    try:
        time.sleep(5)
        before = kafka_probe()
        for dag in dags:
            set_paused(dag, False)
            runs[dag] = trigger(dag)
            set_paused(dag, True)
        after = kafka_probe()
        pointer = storage.active()
        assert (storage.ROOT / 'models' / pointer['version'] / 'model.cbm').exists()
        assert (storage.ROOT / 'reports/dq.html').exists()
        report = {'passed': True, 'runs': runs, 'before': before, 'after': after,
                  'active': pointer, 'dq': json.loads((storage.ROOT / 'reports/dq.json').read_text())}
        storage.atomic_json(storage.ROOT / 'reports/system_check.json', report)
        print(json.dumps(report, indent=2))
        print('PASS: Airflow DAGs, quality report and versioned Kafka scoring')
    finally:
        for dag in dags:
            set_paused(dag, False)


if __name__ == '__main__':
    main()
