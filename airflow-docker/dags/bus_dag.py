from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import pandas as pd

# Function to generate random bus travel data
def generate_data(**kwargs):
    routes = [f"Route {i}" for i in range(1, 21)]
    years = [2021, 2022, 2023]
    months = list(range(1, 13))

    data = []

    for _ in range(1000):
        year = random.choice(years)
        month = random.choice(months)
        route = random.choice(routes)
        num_trips = random.randint(100, 1000)
        passengers = num_trips * random.randint(20, 50)
        data.append([year, month, route, num_trips, passengers])

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["Year", "Month", "Route", "Num_Trips", "Passengers"])

    # Save to CSV (or pass the data through XCom)
    csv_filename = "bus_travel_data.csv"
    df.to_csv(csv_filename, index=False)

    # Push the DataFrame to XCom
    kwargs['ti'].xcom_push(key='bus_travel_data', value=df)

    print(f"CSV file '{csv_filename}' generated with {len(df)} records.")


def process_csv(**kwargs):
    # Pull data from XCom
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='generate_csv', key='bus_travel_data')

    # Aggregate total passengers per route per month
    agg_df = df.groupby(['Route', 'Month'])['Passengers'].sum().reset_index()
    agg_df.rename(columns={'Passengers': 'Total_Passengers'}, inplace=True)

    # Rank months based on total passengers per route
    agg_df['rank'] = agg_df.groupby('Route')['Total_Passengers'].rank(method='first', ascending=False)

    # Extract top travel month per route
    top_months_per_route = agg_df[agg_df['rank'] == 1].drop(columns='rank')

    # ---------------- Find top routes per month ----------------
    agg_route_df = df.groupby(['Month', 'Route'])['Passengers'].sum().reset_index()
    agg_route_df.rename(columns={'Passengers': 'Total_Passengers'}, inplace=True)

    # Rank routes based on total passengers per month
    agg_route_df['rank'] = agg_route_df.groupby('Month')['Total_Passengers'].rank(method='first', ascending=False)

    # Extract top route per month
    top_routes_per_month = agg_route_df[agg_route_df['rank'] == 1].drop(columns='rank')

    # Save both insights as CSV
    output_top_months = "bus_travel_top_months.csv"
    output_top_routes = "bus_travel_top_routes.csv"

    top_months_per_route.to_csv(output_top_months, index=False)
    top_routes_per_month.to_csv(output_top_routes, index=False)

    # Push processed data to XCom
    kwargs['ti'].xcom_push(key='top_months_per_route', value=top_months_per_route)
    kwargs['ti'].xcom_push(key='top_routes_per_month', value=top_routes_per_month)

    print(f"Insights saved to '{output_top_months}' and '{output_top_routes}'.")


# Define default arguments for the DAG
default_args = {
    'owner': 'saurabh',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
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
generate_csv_task = PythonOperator(
    task_id='generate_csv',
    python_callable=generate_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Process CSV with PySpark
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_csv,
    provide_context=True,
    dag=dag,
)

# Task 3: Notify Completion
def notify_completion(**kwargs):
    # Pull data from XCom
    ti = kwargs['ti']
    top_months_per_route = ti.xcom_pull(task_ids='process_data', key='top_months_per_route')
    top_routes_per_month = ti.xcom_pull(task_ids='process_data', key='top_routes_per_month')

    # Do something with the data or just print completion message
    print("Bus Travel Analysis Completed!")
    print(f"Top Months Per Route: \n{top_months_per_route}")
    print(f"Top Routes Per Month: \n{top_routes_per_month}")

notify_task = PythonOperator(
    task_id='notify',
    python_callable=notify_completion,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
generate_csv_task >> process_data_task >> notify_task
