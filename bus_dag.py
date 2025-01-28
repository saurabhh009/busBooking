from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime

def extract():
    df = pd.read_csv('/opt/airflow/dags/bus_bookings.csv')
    return df

def transform(df):
    spark = SparkSession.builder.appName("BusBookingETL").getOrCreate()
    spark_df = spark.createDataFrame(df)

    # Group by route and sum passengers_count to find heavily traveled routes
    result_df = spark_df.groupBy("route").agg({"passengers_count": "sum"}).withColumnRenamed("sum(passengers_count)", "total_passengers")
    
    # Convert back to Pandas DataFrame for further processing if needed
    result_pd_df = result_df.toPandas()
    
    return result_pd_df

def load(result_df):
    # Here you would typically load the data into a database or another storage system.
    print(result_df)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 28),
}

dag = DAG('bus_booking_etl', default_args=default_args, schedule_interval='@daily')

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=lambda: transform(extract_task.output),
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=lambda: load(transform_task.output),
    dag=dag,
)

extract_task >> transform_task >> load_task
