# Import necessary libraries
from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from weather.python_files.weather_tasks import _fetch_weather, _process_weather, _load_data

# Define the DAG
dag = DAG(
    dag_id="weather_data",
    start_date=datetime(2025, 10, 28),
    schedule=None,
    catchup=False
)

# Define tasks
fetch_weather = PythonOperator(
    task_id="fetch_weather",
    python_callable=_fetch_weather,
    dag=dag
)

process_weather = PythonOperator(
    task_id="process_weather",
    python_callable=_process_weather,
    dag=dag
)

load_data = PythonOperator(
    task_id="load_data",
    python_callable=_load_data,
    dag=dag
)


# Define task dependencies
fetch_weather >> process_weather >> load_data

