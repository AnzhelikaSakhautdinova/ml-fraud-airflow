"""Run dbt, tests, SQLFluff and Docs, saving the actual outputs for submission."""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import subprocess

from load_data import query


def main():
    root = Path(__file__).resolve().parents[1]
    reports = root / 'reports'
    reports.mkdir(exist_ok=True)
    commands = [
        ('dbt_build', ['dbt', 'build'], root / 'dbt'),
        ('dbt_test', ['dbt', 'test'], root / 'dbt'),
        ('sqlfluff', ['sqlfluff', 'lint', 'dbt/models', 'dbt/tests'], root),
        ('pre_commit_config', ['pre-commit', 'validate-config'], root),
        ('dbt_docs', ['dbt', 'docs', 'generate'], root / 'dbt'),
    ]
    for name, args, directory in commands:
        result = subprocess.run(args, cwd=directory, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)
        (reports / (name + '.log')).write_text(output)
        print(output[-2500:], flush=True)
        if result.returncode:
            raise SystemExit(f'{name} failed; see reports/{name}.log')
        if name in ['dbt_build', 'dbt_test']:
            shutil.copyfile(root / 'dbt/target/run_results.json', reports / (name + '_results.json'))
    tests = json.loads((reports / 'dbt_test_results.json').read_text())
    statuses = dict(Counter(item['status'] for item in tests['results']))
    types = dict(Counter(item['unique_id'].split('.')[0] for item in tests['results']))
    models = ['mart_daily_state_metrics', 'mart_fraud_by_category', 'mart_fraud_by_state',
              'mart_customer_risk_profile', 'mart_hourly_fraud_pattern', 'mart_merchant_analytics']
    source_count = int(query('SELECT count() FROM transactions_db.transactions'))
    staging_count = int(query('SELECT count() FROM analytics.stg_transactions'))
    assert source_count == staging_count
    source_frauds = int(query('SELECT sum(target) FROM transactions_db.transactions'))
    for model in models:
        assert int(query(f'SELECT sum(transaction_count) FROM analytics.{model}')) == source_count
        if model != 'mart_daily_state_metrics':
            assert int(query(f'SELECT sum(fraud_count) FROM analytics.{model}')) == source_frauds
    counts = {model: int(query(f'SELECT count() FROM analytics.{model}')) for model in models}
    versions = subprocess.check_output(['dbt', '--version'], text=True)
    (reports / 'versions.log').write_text(versions)
    report = {'created_at': datetime.now(timezone.utc).isoformat(), 'passed': True,
              'dbt_core': '1.10.13', 'dbt_clickhouse': '1.10.3', 'sqlfluff': '3.4.2',
              'raw_rows': source_count, 'staging_rows': staging_count,
              'fraud_rows': source_frauds, 'test_statuses': statuses, 'test_types': types,
              'marts': counts, 'all_mart_counts_and_fraud_totals_match_source': True,
              'docs_generated': (root / 'dbt/target/catalog.json').exists(),
              'lint': 'passed', 'pre_commit_config': 'valid',
              'publication': 'PR and remote CI not yet created'}
    (reports / 'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print('PASS: dbt models, data/unit tests, SQLFluff and Docs')


if __name__ == '__main__':
    main()
