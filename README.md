# Data Streaming Pipeline with Airflow, Kafka, and Spark

A data pipeline that scrapes basketball data, streams it through Kafka, processes it with Spark, and stores results in Amazon S3.

## Architecture

This pipeline consists of the following components:

- **Apache Airflow** (v3.1.0) - Workflow orchestration
- **Apache Kafka** (Confluent) - Message streaming
- **Apache Spark** (v3.5.7) - Data processing
- **Kafka UI** - Kafka cluster monitoring
- **PostgreSQL** - Airflow metadata database
- **Redis** - Celery broker for Airflow
- **Amazon S3** - Data storage

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM allocated to Docker
- At least 2 CPUs allocated to Docker
- AWS account with S3 access
- AWS credentials (Access Key and Secret Key)

## Project Structure

```
.
├── dags/                           # Airflow DAG files
│   └── kafka_stream_dag.py
├── scripts/                        # Spark processing scripts
│   ├── spark_processing.py
│   └── spark_merge_dfs.py
├── dependencies/                   # JAR files for Spark
│   ├── spark-sql-kafka-0-10_2.12-3.5.0.jar
│   ├── aws-java-sdk-bundle-1.12.262.jar
│   ├── hadoop-aws-3.3.4.jar
│   ├── kafka-clients-3.5.0.jar
│   ├── commons-pool2-2.11.1.jar
│   └── spark-token-provider-kafka-0-10_2.12-3.5.0.jar
├── airflow_custom/                 # Custom Airflow Dockerfile
│   └── Dockerfile
├── spark-worker-entrypoint.sh      # Spark worker startup script
├── docker-compose.yml
└── .env                           # Environment variables
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd <project-directory>
```

### 2. Download Required JAR Files

Download the following JAR files and place them in the `dependencies/` folder:

- [spark-sql-kafka-0-10_2.12-3.5.0.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar)
- [hadoop-aws-3.3.4.jar](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar)
- [aws-java-sdk-bundle-1.12.262.jar](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar)
- [kafka-clients-3.5.0.jar](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar)
- [commons-pool2-2.11.1.jar](https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar)
- [spark-token-provider-kafka-0-10_2.12-3.5.0.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar)

### 3. Create Environment File

Create a `.env` file in the project root with the following variables:

```bash
# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Kafka
KAFKA_CLUSTERS_0_NAME=local
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=broker:9092
DYNAMIC_CONFIG_ENABLED=true

# Spark
SPARK_UI_PORT=8080
SPARK_MODE=master
SPARK_RPC_AUTHENTICATION_ENABLED=no
SPARK_RPC_ENCRYPTION_ENABLED=no
```

### 4. Create Configuration File

Create a configuration file for your DAG with AWS credentials and Kafka settings. Example structure:

```python
config_file = {
    'aws': {
        'AWS_ACCESS_KEY': 'your-access-key',
        'AWS_SECRET_KEY': 'your-secret-key',
        'S3_BUCKET_PATH': 's3a://your-bucket/path/'
    },
    'kafka': {
        'KAFKA_BROKERS': 'broker:9092',
        'KAFKA_TOPIC': 'shot_charts'
    }
}
```

### 5. Make Spark Worker Script Executable

```bash
chmod +x spark-worker-entrypoint.sh
```

### 6. Build Custom Airflow Image

```bash
docker-compose build
```

### 7. Initialize Airflow

```bash
docker-compose up airflow-init
```

### 8. Start All Services

```bash
docker-compose up -d
```

## Accessing Services

Once all services are running, you can access:

- **Airflow UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **Kafka UI**: http://localhost:8888
- **Spark Master UI**: http://localhost:8085

## Configuration

### Airflow Connections

The pipeline uses an Airflow connection named `spark-docker`. This is configured programmatically, but if you need to set it manually:

1. Go to Airflow UI → Admin → Connections
2. Add new connection:
   - Connection Id: `spark-docker`
   - Connection Type: `Spark`
   - Host: (leave empty)
   - Port: (leave empty)
   - Extra: `{"deploy-mode": "client"}`

### Docker Proxy Permissions

The docker-proxy service requires specific permissions to pull images. The following environment variables are set:

- `IMAGES=1` - List images
- `CONTAINERS=1` - Manage containers
- `PULL=1` - Pull images from registries
- `POST=1` - Allow POST requests
- `SERVICES=1`, `NETWORK=1`, `TASKS=1`, `INFO=1` - Additional Docker API access

## Running the Pipeline

### Trigger the DAG Manually

1. Open Airflow UI at http://localhost:8080
2. Find the `scrapper_dag` in the DAGs list
3. Toggle the DAG to "On" if it's paused
4. Click the play button to trigger a manual run

### Scheduled Execution

The DAG is scheduled to run weekly at 04:05 AM every Sunday (`05 04 * * 0`).

## Pipeline Workflow

1. **DockerOperator** (`docker_stream_to_kafka_topic`): Runs a Scrapy spider in a Docker container to scrape basketball data and stream it to Kafka topic
2. **BashOperator** (`run_spark_job`): Processes streaming data from Kafka using Spark and writes to S3
3. **BashOperator** (`run_merge_job`): Merges processed dataframes and updates S3 storage

## Important Notes

### Spark Version Compatibility

- All Spark components (Airflow, spark_master, spark_worker) must use the same version (3.5.7)
- JAR files must match the Spark and Scala versions (Spark 3.5.x, Scala 2.12)

### Python Version Compatibility

- Airflow and Spark workers must use the same Python version (3.10)
- PySpark requires matching minor versions between driver and executors

### Network Configuration

- All services run on the default Docker Compose network
- The DockerOperator dynamically resolves the network name (typically `<project-directory>_default`)
- Spark master IP is resolved dynamically to avoid hardcoding

### Resource Requirements

- Spark worker is configured with 2 CPU cores and 2GB memory
- Ensure Docker has sufficient resources allocated

## Monitoring

### Check Service Status

```bash
docker-compose ps
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark_master
docker-compose logs -f broker
```

### Monitor Kafka Topics

Access Kafka UI at http://localhost:8888 to view:
- Topic messages
- Consumer groups
- Broker status

### Monitor Spark Jobs

Access Spark Master UI at http://localhost:8085 to view:
- Running applications
- Completed jobs
- Worker status

## Stopping the Pipeline

```bash
# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## License


## Contributors

[Cesar Calderon Muro
