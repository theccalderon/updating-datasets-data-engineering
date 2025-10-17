from io import BytesIO
import logging
import shutil
import tarfile
from pyspark.sql import SparkSession
import boto3
import os
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


def get_s3_dataframe(spark, path, season, type, access_key, secret_key):
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
            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            obj = s3.get_object(Bucket=path, Key=f"shots-{season}.tgz")
            
            with tarfile.open(fileobj=BytesIO(obj['Body'].read()), mode='r:gz') as tar:
                member = tar.getmembers()[0]
                csv_content = tar.extractfile(member).read()
                csv_lines = csv_content.decode('utf-8').splitlines()
                rdd = spark.sparkContext.parallelize(csv_lines)
                df = spark.read.option("header", "true").csv(rdd)
                
            logger.info("Season dataframe fetched successfully")
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch season dataframe. Error: {e}")
            raise e
    else:
        # ongoing
        try:
            df = spark.read.csv(path)
            logger.info("Ongoing dataframe fetched successfully")
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
            raise e

def merge_dfs(season_df, ongoing_df):
    return season_df.unionByName(ongoing_df, allowMissingColumns=True)


def initiate_streaming_to_bucket(df, bucket, season, access_key, secret_key):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.
    
    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :return: None
    """
    try:
        
        temp_s3_path = f"s3a://{bucket}/temp/season-{season}-output"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_s3_path)
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=f"temp/season-{season}-output/")
        csv_key = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')][0]
        
        csv_obj = s3.get_object(Bucket=bucket, Key=csv_key)
        csv_content = csv_obj['Body'].read()
        
        tgz_buffer = BytesIO()
        with tarfile.open(fileobj=tgz_buffer, mode='w:gz') as tar:
            info = tarfile.TarInfo(name=f"shots-{season}.csv")
            info.size = len(csv_content)
            tar.addfile(info, BytesIO(csv_content))
        
        tgz_buffer.seek(0)
        s3.upload_fileobj(tgz_buffer, bucket, f"shots-{season}.tgz")
        s3.put_object_acl(
            Bucket=bucket,
            Key=f"shots-{season}.tgz",
            GrantRead='uri=http://acs.amazonaws.com/groups/global/AllUsers'
        )
        logger.info(f"Granted public read access to shots-{season}.tgz")
        
        s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': obj['Key']} for obj in response['Contents']]})
        
        logger.info(f"Successfully wrote shots-{season}.tgz to S3")
    except Exception as e:
        logger.error(f"Error writing to S3: {e}")
        raise e
        
    # stream_query.awaitTermination()


def main():
    app_name = "SparkStructuredStreamingToS3"
    access_key = os.environ['AWS_ACCESS_KEY']
    secret_key = os.environ['AWS_SECRET_KEY']
    s3_bucket = os.environ['S3_BUCKET_PATH']
    base_path = f"s3a://{s3_bucket}"
    ongoing_s3_path = f"{base_path}/ongoing".format(s3_bucket)
    logger.info("S3 path: {}".format(ongoing_s3_path))
    logger.info("Arguments: {}".format(sys.argv))
    season = sys.argv[1]
    logger.info(f"Season argument is {season}")
    checkpoint_location = "s3a://{}/checkpoints".format(s3_bucket)
    try:
        spark = initialize_spark_session(app_name, access_key, secret_key)
        if spark:
            season_df = get_s3_dataframe(spark, s3_bucket, season, "season", access_key, secret_key)
            ongoing_df = get_s3_dataframe(spark, ongoing_s3_path, season, "ongoing", access_key, secret_key)
            if season_df and ongoing_df:
                merged_df = merge_dfs(season_df, ongoing_df)
                initiate_streaming_to_bucket(merged_df, s3_bucket, season, access_key, secret_key)
                s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
                response = s3.list_objects_v2(Bucket=s3_bucket, Prefix='ongoing/')
                if 'Contents' in response:
                    s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': [{'Key': obj['Key']} for obj in response['Contents']]})
                    logger.info("Cleaned up ongoing S3 location")
    except Exception as e:
        logger.error(e)
        sys.exit(1)


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
