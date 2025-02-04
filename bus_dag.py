from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'bus_travel_analysis',
    default_args=default_args,
    description='Orchestrate bus travel data pipeline',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Generate Bus Travel Data
generate_csv = BashOperator(
    task_id='generate_csv',
    bash_command='python3 /opt/airflow/dags/bus_travel_generator.py',
    dag=dag,
)

# Task 2: Process CSV with PySpark
process_data = BashOperator(
    task_id='process_data',
    bash_command='spark-submit /opt/airflow/dags/bus_travel_analysis.py',
    dag=dag,
)

# Task 3: Notify Completion
notify = BashOperator(
    task_id='notify',
    bash_command='echo "Bus Travel Analysis Completed"',
    dag=dag,
)

# Task dependencies
generate_csv >> process_data >> notify
