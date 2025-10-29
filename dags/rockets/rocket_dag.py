from airflow.sdk import DAG, Variable
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from rockets.python_files.rocket_launch import _get_pictures
import os


output_path = "/opt/airflow/dags/rockets/launches/launches.json"
api_url = Variable.get("api_url")

with DAG(
    dag_id="rocket_images",
    start_date=datetime(2025, 10, 28),
    schedule="@daily",
    catchup=False
):

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command=f"curl -o {output_path} -L {api_url}"
    )

    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures
    )

    def _get_image_count(ti):
        image_count = len(os.listdir("/opt/airflow/dags/rockets/images/"))
        ti.xcom_push(key="image_count", value=image_count)

    get_image_count = PythonOperator(
        task_id="get_image_count",
        python_callable=_get_image_count
    )


    # notify = BashOperator(
    #     task_id="notify",
    #     bash_command='echo "There are now $(ls /opt/airflow/dags/rockets/images/ | wc -l) images"'
    # )

    send_notification = EmailOperator(
        task_id="send_notification",
        to=["tobby398@gmail.com", "oluwatobiojo453@gmail.com", "ogunsakinevelyn30@gmail.com"],
        subject="Rocket Image Download Complete for {{ ds }}",
        html_content="""
        <h3>Rocket Launches Update</h3>
        <p>Hi Guys, The Airflow pipeline ran successfully</p>
        <p>Total Tocket images downloaded: <b>{{ti.xcom_pull(task_ids='get_image_count', key='image_count')}}</b> </p>
        <p>Find attached the images</p>
        <p>Kind Regards, Your superstar, Oluwatobi</p>
        """,
        conn_id="smtp_default"
        )

    download_launches >> get_pictures >> get_image_count >> send_notification
