# Importing required modules
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from kafka.admin import KafkaAdminClient, NewTopic
import yaml


# Configuration for the DAG's start date
DAG_START_DATE = datetime(2024, 2, 6, 23, 00)
TODAY = date.today()
END_DATE = str(TODAY)[:10]
# END_DATE = "2024-05-31"

# delta is 7 to run it weekly.
DELTA = timedelta(days=1)
START_DATE = str(TODAY - DELTA)
# START_DATE = "2024-04-31"
SEASON = 2025

# Default arguments for the DAG
DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with open('/opt/airflow/dags/spark_config.yaml', 'r') as file:
    config_file = yaml.safe_load(file)

import socket

def get_container_ip(container_name):
    return socket.gethostbyname(container_name)

spark_master_ip = get_container_ip('spark_master')

def cleanup_kafka_topic():
    admin = KafkaAdminClient(bootstrap_servers='broker:9092')
    admin.delete_topics(['shot_charts'])
    admin.create_topics([NewTopic('shot_charts', num_partitions=1, replication_factor=1)])
    admin.close()

# Creating the DAG with its configuration
with DAG(
    'scrapper_dag',  # Renamed for uniqueness
    default_args=DAG_DEFAULT_ARGS,
    schedule='05 10 * * *',
    catchup=False,
    description='runs scrapper and streams to kafka topic',
    max_active_runs=1
) as dag:
    
    kafka_stream_task = DockerOperator(
        task_id='docker_stream_to_kafka_topic',
        image='ccalderon911217/shot_chart_scraper:latest',
        api_version='auto',
        auto_remove='success',
        mount_tmp_dir=False,
        docker_url="tcp://docker-proxy:2375",
        network_mode='updating-datasets-data-engineering_default',
        command= "scrapy crawl basketball-reference -a season={} -a topic=shot_charts -a kafka_listener='broker:9092' -a start_date='{}' -a end_date='{}'".format(SEASON, START_DATE, END_DATE),
        dag=dag
    )

    spark_stream_task = BashOperator(
    task_id="run_spark_job",
    bash_command=f"PYSPARK_PYTHON=python3.10 PYSPARK_DRIVER_PYTHON=python3.10 /opt/spark/bin/spark-submit --master spark://{spark_master_ip}:7077 --jars /opt/airflow/dependencies/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/airflow/dependencies/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/dependencies/hadoop-aws-3.3.4.jar,/opt/airflow/dependencies/kafka-clients-3.5.0.jar,/opt/airflow/dependencies/commons-pool2-2.11.1.jar,/opt/airflow/dependencies/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/airflow/scripts/spark_processing.py"
    )

    spark_merge_dfs_task = BashOperator(
    task_id="run_merge_job",
    bash_command=f"PYSPARK_PYTHON=python3.10 PYSPARK_DRIVER_PYTHON=python3.10 /opt/spark/bin/spark-submit --master spark://{spark_master_ip}:7077 --jars /opt/airflow/dependencies/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/airflow/dependencies/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/dependencies/hadoop-aws-3.3.4.jar,/opt/airflow/dependencies/kafka-clients-3.5.0.jar,/opt/airflow/dependencies/commons-pool2-2.11.1.jar,/opt/airflow/dependencies/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/airflow/scripts/spark_merge_dfs.py {SEASON}"
    )

    cleanup_kafka = PythonOperator(
        task_id="cleanup_kafka_topic",
        python_callable=cleanup_kafka_topic
    )


    kafka_stream_task >> spark_stream_task >> spark_merge_dfs_task >> cleanup_kafka
    # kafka_stream_task
    # spark_stream_task

