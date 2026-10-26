from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality import check_data, render_report

with DAG('data_quality_report', start_date=datetime(2024, 1, 1), schedule='*/5 * * * *',
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         default_args={'retries': 1, 'retry_delay': timedelta(seconds=15)}, tags=['hw3', 'quality']) as dag:
    check = PythonOperator(task_id='check_dataset', python_callable=check_data)
    render = PythonOperator(task_id='save_html_report', python_callable=render_report,
                            op_args=[check.output])
    check >> render
