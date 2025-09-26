from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='load_fact_booking',
    default_args=default_args,
    description='Run Talend Load_Fact_Booking Job',
    schedule_interval=None,  # Run manually first
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['talend'],
) as dag:

    run_load_fact_booking = BashOperator(
        task_id='run_load_fact_booking',
        bash_command='bash -c "/opt/airflow/jobs/Load_Fact_Booking/Load_Fact_Booking_run.sh"',
        do_xcom_push=False,
    )
