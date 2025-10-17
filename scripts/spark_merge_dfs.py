import logging
import shutil
import tarfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import datetime
import pyspark.pandas as ps
import pandas as pd
import os
import time
import sys

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def initialize_spark_session(app_name, access_key, secret_key):
    """
    Initialize the Spark Session with provided configurations.
    
    :param app_name: Name of the spark application.
    :param access_key: Access key for S3.
    :param secret_key: Secret key for S3.
    :return: Spark session object or None if there's an error.
    """
    try:
        spark = SparkSession \
                .builder \
                .appName(app_name) \
                .config("spark.hadoop.fs.s3a.access.key", access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized successfully')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_s3_dataframe(spark, path, season, type):
    """
    Get a streaming dataframe from Kafka.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    # TODO: Modify this function
    if type == "season":
        # differentiating here in case I have to specify compression and such
        try:
            df = spark.read.csv(path+"/shots-"+season+".tgz")
            logger.info(df.show())
            logger.info("Seasong dataframe fetched successfully")
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
            return None
    else:
        # ongoing
        try:
            df = spark.read.csv(path)
            logger.info("Ongoing dataframe fetched successfully")
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
            return None


def merge_dfs(season_df, ongoing_df):
    return season_df.union(ongoing_df)


def initiate_streaming_to_bucket(df, path, season):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.
    
    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :return: None
    """
    # TODO: modify function to write to root S3 bucket in tgz format.
    logger.info("Writing to S3 now...")
    try:
        # Define file name
        file_name = "season-"+season+".tgz"

        # Write DataFrame to local directory as CSV
        local_dir = "/opt/airflow/temp"
        local_file_path = os.path.join(local_dir, file_name)
        df.write.mode("overwrite").option("header", "true").csv(local_file_path)

        # Compress the CSV file to tar.gz
        with tarfile.open(local_file_path + ".tar.gz", "w:gz") as tar:
            tar.add(local_file_path, arcname=file_name)

        # Copy the compressed file to S3
        shutil.copy(local_file_path + ".tar.gz", path)

        # Remove the local CSV file and its compressed version
        os.remove(local_file_path)
        os.remove(local_file_path + ".tar.gz")
    except Exception as e:
        logger.error("Error writing to CSV in S3 {}".format(e))
        return
        
    # stream_query.awaitTermination()


def main():
    app_name = "SparkStructuredStreamingToS3"
    access_key = os.environ['AWS_ACCESS_KEY']
    secret_key = os.environ['AWS_SECRET_KEY']
    s3_bucket = os.environ['S3_BUCKET_PATH']
    s3_path = "s3a://{}/ongoing".format(s3_bucket)
    logger.info("S3 path: {}".format(s3_path))
    logger.info("Arguments: {}".format(sys.argv))
    season = sys.argv[1]
    checkpoint_location = "s3a://{}/checkpoints".format(s3_bucket)

    spark = initialize_spark_session(app_name, access_key, secret_key)
    if spark:
        season_df = get_s3_dataframe(spark, s3_bucket, season, "season")
        ongoing_df = get_s3_dataframe(spark, s3_path, season, "ongoing")
        # TODO:
        # 1. Merge/concat dfs
        # 2. Delete ongoing dataframe
        # 3. Delete season dataframe
        # 4. Write merged_df as tgz into s3_bucket
        if season_df and ongoing_df:
            merged_df = merge_dfs(season_df, ongoing_df)
            initiate_streaming_to_bucket(merged_df, s3_bucket, season)


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
