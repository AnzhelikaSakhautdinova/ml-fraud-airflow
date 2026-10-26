"""Task functions; DAG files only assemble dependencies and schedules."""
import hashlib
import json
from pathlib import Path

from catboost import CatBoostClassifier
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

from app.features import CATEGORIES, FEATURES, prepare
from pipeline import storage
from pipeline.months import shift, window

BATCH_SIZE = 3000
HISTORY_MONTHS = 36


def seed():
    """Read seed CSV, insert labelled months into mart; primary keys make retries harmless."""
    with storage.lock(), storage.connect() as connection:
        if connection.execute('SELECT count() FROM transactions').fetchone()[0]:
            return
        data = pd.read_csv('/opt/project/data/seed.csv').fillna('')
        for index, row in data.iterrows():
            payload = row.to_dict()
            month = str(payload['transaction_time'])[:7]
            connection.execute('INSERT OR IGNORE INTO transactions VALUES (?, ?, ?)',
                               (f'seed-{index}', month, json.dumps(payload)))


def supply_month():
    """Read latest published month; create exactly one next month and prune old history.

    The publication checkpoint is never advanced here. The unique month/row IDs
    and the lock make retries and overlapping DAG runs idempotent.
    """
    with storage.lock(), storage.connect() as connection:
        current = storage.active()
        if current is None:
            return {'status': 'waiting_for_initial_model'}
        month = shift(current['processed_month'])
        if connection.execute('SELECT count() FROM transactions WHERE month=?', (month,)).fetchone()[0]:
            return {'month': month, 'status': 'already_supplied'}
        source = storage.rows(connection, [current['processed_month']])
        if not source:
            raise ValueError('No source month')
        data = pd.DataFrame(source).sample(n=min(BATCH_SIZE, len(source)), random_state=42).reset_index(drop=True)
        # Preserve relative day/hour, clamp days for February and shorter months.
        start = pd.Timestamp(month + '-01')
        dates = pd.to_datetime(data.transaction_time, utc=True, format='mixed')
        days = dates.dt.day.clip(upper=start.days_in_month)
        data['transaction_time'] = [f'{month}-{day:02d} {hour:02d}:{minute:02d}:00'
                                    for day, hour, minute in zip(days, dates.dt.hour, dates.dt.minute)]
        for index, row in data.iterrows():
            connection.execute('INSERT OR IGNORE INTO transactions VALUES (?, ?, ?)',
                               (f'{month}-{index}', month, json.dumps(row.to_dict())))
        connection.execute('DELETE FROM transactions WHERE month < ?', (shift(month, -HISTORY_MONTHS+1),))
        return {'month': month, 'rows': len(data)}


def prepare_window():
    """Read next unprocessed month and write immutable train/test snapshots.

    A pending plan is reused after task failure; later supplied data cannot shift it.
    State advances only in the successful final publication decision.
    """
    with storage.lock(), storage.connect() as connection:
        current = storage.active()
        latest = connection.execute('SELECT max(month) FROM transactions').fetchone()[0]
        month = shift(current['processed_month']) if current else latest
        if month > latest:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException('No unprocessed month: wait for supply DAG')
        plan = storage.ROOT / 'work' / month
        metadata_path = plan / 'plan.json'
        if metadata_path.exists():
            return json.loads(metadata_path.read_text())
        months = window(month)
        data = pd.DataFrame(storage.rows(connection, months))
        found = set(data.transaction_time.str[:7])
        if set(months) != found:
            raise ValueError(f'Need all three months: {months}, found {found}')
        train = data[data.transaction_time.str[:7] < month]
        test = data[data.transaction_time.str[:7] == month]
        for part in (train, test):
            if set(part.target.astype(int)) != {0, 1}:
                raise ValueError('Both classes required in train and test')
        prepare(train)
        prepare(test)
        plan.mkdir(parents=True, exist_ok=True)
        train.to_csv(plan / 'train.csv', index=False)
        test.to_csv(plan / 'test.csv', index=False)
        result = {'month': month, 'months': months, 'directory': str(plan),
                  'base_version': current['version'] if current else None}
        storage.atomic_json(metadata_path, result)
        return result


def train_candidate(plan):
    """Read frozen train CSV; save candidate only, retries reuse the same candidate."""
    directory = Path(plan['directory'])
    path = directory / 'candidate.cbm'
    if not path.exists():
        data = pd.read_csv(directory / 'train.csv')
        model = CatBoostClassifier(iterations=80, depth=4, learning_rate=0.08,
            loss_function='Logloss', auto_class_weights='Balanced', random_seed=42,
            thread_count=2, task_type='CPU', verbose=False, allow_writing_files=False)
        model.fit(prepare(data), data.target.astype(int), cat_features=CATEGORIES)
        model.save_model(str(directory / 'candidate.tmp'))
        (directory / 'candidate.tmp').replace(path)
    return plan


def evaluate_candidate(plan):
    """Evaluate candidate and incumbent on identical latest-month rows, no fitting.

    The report is overwritten atomically, so retry cannot partially publish metrics.
    AP is the deployment metric; equal values are accepted. No test threshold tuning.
    """
    directory = Path(plan['directory'])
    data = pd.read_csv(directory / 'test.csv')
    features, target = prepare(data), data.target.astype(int)
    candidate = CatBoostClassifier()
    candidate.load_model(str(directory / 'candidate.cbm'))
    probability = candidate.predict_proba(features, thread_count=2)[:, 1]
    candidate_ap = float(average_precision_score(target, probability))
    incumbent_ap = None
    if plan['base_version']:
        model = CatBoostClassifier()
        model.load_model(str(storage.ROOT / 'models' / plan['base_version'] / 'model.cbm'))
        incumbent_ap = float(average_precision_score(target, model.predict_proba(features, thread_count=2)[:, 1]))
    report = {'candidate_ap': candidate_ap, 'incumbent_ap': incumbent_ap,
              'candidate_roc_auc': float(roc_auc_score(target, probability)),
              'accepted': incumbent_ap is None or candidate_ap >= incumbent_ap,
              'test_month': plan['month'], 'test_rows': len(data), 'threshold': 0.5}
    storage.atomic_json(directory / 'evaluation.json', report)
    return plan


def publish_candidate(plan):
    """Save immutable winner, then atomically update model pointer and month together.

    Rejected candidates keep the old version but checkpoint the evaluated month.
    A crash before the pointer swap safely repeats; after swap the month is already done.
    """
    directory = Path(plan['directory'])
    report = json.loads((directory / 'evaluation.json').read_text())
    with storage.lock():
        current = storage.active()
        if current and current['processed_month'] >= plan['month']:
            return current
        if (current['version'] if current else None) != plan['base_version']:
            raise ValueError('Incumbent changed; refusing stale plan')
        version = current['version'] if current else None
        if report['accepted']:
            version = plan['month'] + '-' + storage.digest(directory / 'candidate.cbm')[:12]
            target = storage.ROOT / 'models' / version
            target.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.copyfile(directory / 'candidate.cbm', target / 'model.tmp')
            (target / 'model.tmp').replace(target / 'model.cbm')
            storage.atomic_json(target / 'metadata.json', {'version': version, 'features': FEATURES,
                'sha256': storage.digest(target / 'model.cbm'), 'threshold': 0.5, **report})
        if version is None:
            raise ValueError('No deployable model')
        # Persist the decision before moving the cursor, including rejection.
        # No month can be consumed without a durable artifact and saved active model.
        storage.atomic_json(storage.ROOT / 'decisions' / (plan['month'] + '.json'),
                            {'month': plan['month'], 'version': version, **report})
        pointer = {'version': version, 'processed_month': plan['month'],
                   'accepted': report['accepted'], 'evaluation': report}
        storage.atomic_json(storage.ROOT / 'models/active.json', pointer)
        return pointer


def bootstrap():
    """Initialise mart and first model once; existing active pointer is preserved."""
    seed()
    if storage.active() is None:
        plan = prepare_window()
        publish_candidate(evaluate_candidate(train_candidate(plan)))


if __name__ == '__main__':
    bootstrap()
