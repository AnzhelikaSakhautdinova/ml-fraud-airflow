"""Independent dataset checks and HTML reporting."""
import html
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pipeline import storage


def check_data():
    """Read a consistent mart snapshot and atomically save pass/fail rules in JSON.

    No data is changed; each run replaces only its latest report snapshot.
    """
    with storage.lock(), storage.connect() as connection:
        records = connection.execute('SELECT id, month, payload FROM transactions').fetchall()
    data = pd.DataFrame([json.loads(row[2]) for row in records])
    if data.empty:
        report = {'created_at': datetime.now(timezone.utc).isoformat(), 'rows': 0,
                  'passed': False, 'rules': {'dataset_not_empty': False}}
        storage.atomic_json(storage.ROOT / 'reports/dq.json', report)
        return report
    dates = pd.to_datetime(data.transaction_time, utc=True, errors='coerce', format='mixed')
    numbers = data[['amount', 'lat', 'lon', 'merchant_lat', 'merchant_lon', 'population_city']].apply(pd.to_numeric, errors='coerce')
    rules = {
        'dataset_not_empty': len(data) > 0,
        'unique_ids': len({row[0] for row in records}) == len(records),
        'valid_transaction_time': dates.notna().all(),
        'month_matches_transaction_time': all(str(row[1]) == str(value)[:7] for row, value in zip(records, data.transaction_time)),
        'binary_target': data.target.isin([0, 1]).all(),
        'finite_numeric_values': np.isfinite(numbers).all().all(),
        'nonnegative_amount_population': ((numbers.amount >= 0) & (numbers.population_city >= 0)).all(),
        'valid_coordinates': ((numbers.lat.abs() <= 90) & (numbers.merchant_lat.abs() <= 90) & (numbers.lon.abs() <= 180) & (numbers.merchant_lon.abs() <= 180)).all(),
        'nonempty_categories': all(data[name].fillna('').astype(str).str.strip().ne('').all() for name in ['cat_id', 'gender', 'us_state']),
        'both_classes_per_month': all(set(pd.to_numeric(group.target, errors='coerce')) == {0, 1} for _, group in data.groupby(data.transaction_time.str[:7])),
    }
    report = {'created_at': datetime.now(timezone.utc).isoformat(), 'rows': len(data),
              'passed': all(rules.values()), 'rules': {key: bool(value) for key, value in rules.items()}}
    storage.atomic_json(storage.ROOT / 'reports/dq.json', report)
    return report


def render_report(report):
    """Read check result and save HTML before failing the DAG when any rule fails.

    Atomic replacement keeps the prior report readable during retries.
    """
    rows = ''.join(f'<tr><td>{html.escape(key)}</td><td>{"PASS" if value else "FAIL"}</td></tr>' for key, value in report['rules'].items())
    document = f'<!doctype html><meta charset="utf-8"><title>Data quality</title><h1>Data Quality</h1><p>{html.escape(report["created_at"])} · rows: {report["rows"]}</p><table border="1"><tr><th>Rule</th><th>Result</th></tr>{rows}</table>'
    path = storage.ROOT / 'reports/dq.html'
    temporary = path.with_suffix('.tmp')
    temporary.write_text(document)
    temporary.replace(path)
    if not report['passed']:
        raise ValueError('Data quality rules failed; see reports/dq.html')
    return str(path)
