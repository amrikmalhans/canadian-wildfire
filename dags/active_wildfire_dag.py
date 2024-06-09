from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "wildfire_data_pipeline",
    default_args=default_args,
    description="A simple data pipeline for wildfire data",
    schedule_interval="0 */3 * * *",
    catchup=False,
) as dag:

    fetch_and_save_activefires = DockerOperator(
        task_id="fetch_and_save_activefires",
        image="95289528/docker-wildfire-tasks:latest",
        container_name="fetch_and_save_activefires",
        command=["python", "/app/tasks/scraping/scrape_activefires.py"],
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "AWS_ACCESS_KEY": Variable.get("AWS_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_REGION": Variable.get("AWS_REGION"),
            "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        },
        entrypoint=None,
        mount_tmp_dir=False,
    )

    fetch_and_save_activefires
