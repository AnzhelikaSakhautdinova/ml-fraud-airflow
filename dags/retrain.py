from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.tasks import prepare_window, train_candidate, evaluate_candidate, publish_candidate

with DAG('retrain_fraud_model', start_date=datetime(2024, 1, 1), schedule='*/5 * * * *',
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         default_args={'retries': 2, 'retry_delay': timedelta(seconds=15)},
         tags=['hw3', 'training']) as dag:
    prepare = PythonOperator(task_id='prepare_window', python_callable=prepare_window)
    train = PythonOperator(task_id='train_candidate', python_callable=train_candidate,
                           op_args=[prepare.output])
    evaluate = PythonOperator(task_id='compare_on_latest_month', python_callable=evaluate_candidate,
                              op_args=[train.output])
    publish = PythonOperator(task_id='publish_or_keep_incumbent', python_callable=publish_candidate,
                             op_args=[evaluate.output])
    prepare >> train >> evaluate >> publish
