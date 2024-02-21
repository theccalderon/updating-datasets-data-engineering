# Importing required modules
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from kafka_streaming_service import initiate_stream


# Configuration for the DAG's start date
DAG_START_DATE = datetime(2024, 2, 6, 23, 00)

# Default arguments for the DAG
DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Creating the DAG with its configuration
with DAG(
    'scrapper_dag',  # Renamed for uniqueness
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval='05 04 * * 0',
    catchup=False,
    description='runs scrapper and streams to kafka topic',
    max_active_runs=1
) as dag:
    
    # kafka_stream_task = DockerOperator(
    #     task_id='docker_stream_to_kafka_topic',
    #     image='ccalderon911217/shot_chart_scraper:latest',
    #     api_version='auto',
    #     auto_remove=True,
    #     mount_tmp_dir=False,
    #     docker_url="tcp://docker-proxy:2375",
    #     network_mode='docker_streaming',
    #     command= "scrapy crawl basketball-reference -a season=2019 -a topic=shot_charts -a kafka_listener='broker:9092'",
    #     dag=dag
    # )

    spark_stream_task = SparkSubmitOperator(
        task_id = "run_spark_job",
        application = "/opt/airflow/dags/scripts/spark_processing.py",
        conn_id = "spark-docker",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        jars = "opt/airflow/dags/dependencies/aws-java-sdk-bundle-1.11.1026.jar, /opt/airflow/dags/dependencies/hadoop-aws-3.3.2.jar",
        verbose = False
    )
    
    # DockerOperator(
    #     task_id="pyspark_consumer",
    #     image="rappel-conso/spark:latest",
    #     api_version="auto",
    #     auto_remove=True,
    #     command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_streaming.py",
    #     docker_url='tcp://docker-proxy:2375',
    #     environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
    #     network_mode="airflow-kafka",
    #     dag=dag,
    # )


    # kafka_stream_task >> spark_stream_task
    # kafka_stream_task
    spark_stream_task

