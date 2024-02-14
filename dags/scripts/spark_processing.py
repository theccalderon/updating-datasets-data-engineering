import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import datetime
import pyspark.pandas as ps
import pandas as pd
import os


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


def get_streaming_dataframe(spark, brokers, topic):
    """
    Get a streaming dataframe from Kafka.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("delimiter", ",") \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info("Streaming dataframe fetched successfully")
        return df

    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None


def transform_streaming_data(df):
    """
    Transform the initial dataframe to get the final structure.
    
    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """
    # working with pandas
    shots_df = df.pandas_api()
    game_ids = shots_df.drop_duplicates(subset='game_id')['game_id'].tolist()
    list_of_columns = shots_df.columns.values.tolist() + ["time_remaining","quarter"]
    final_df = pd.DataFrame(columns=list_of_columns)
    for game in game_ids:
        temp_df = shots_df.loc[df['game_id']==game]
        temp_df['time_remaining'] = temp_df['play'].apply(lambda x: datetime.datetime.strptime(str(x).split(' ')[2], '%H:%M.%S')) 
        temp_df['quarter'] = temp_df['play'].apply(lambda x: str(x).split(' ')[0][0]) 

        temp_df = temp_df.sort_values(by=['quarter','time_remaining'],ascending=[True,False])
    #     print(temp_df.head())
        final_df = final_df.append(temp_df)

    final_df['time_remaining'] = final_df['play'].apply(lambda x: str(x).split(' ')[2]) 
    final_df['shots_by'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[0]+" "+str(x).split('<br>')[1].split(' ')[1]) 
    final_df['outcome'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[2]) 
    final_df['attempt'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[3]) 
    final_df['distance'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[-2]+str(x).split('<br>')[1].split(' ')[-1]) 
    final_df['team'] = final_df['play'].apply(lambda x: get_team(x)) 
    final_df['winner_score'] = final_df.apply(lambda x: get_winner_score(x.winner, x.team, x.play), axis=1) 
    final_df['loser_score'] = final_df.apply(lambda x: get_loser_score(x.loser, x.team, x.play), axis=1) 
    return  ps.from_pandas(final_df)

def get_team(play):
    team = play.split('<br>')[2].split(' ')
    if team[0] == "LA" or team[0] == "New" or team[0] == 'San' or team[0] == 'Golden':
        return team[0]+" "+team[1]
    else:
        return team[0]

def get_winner_score(winner, team, play):
    desc = play.split('<br>')[2].split(' ')
    if desc[0] == "LA" or desc[0] == "New" or desc[0] == 'San' or desc[0] == 'Golden':
        if desc[2] == "trails" or desc[2] == "leads":
            #team score is in desc[2].split('-')[0]
            if winner == team:
                return desc[3].split('-')[0]
            else:
                return desc[3].split('-')[1]
        elif desc[3] == "trails" or desc[3] == "leads":
            #team score is in desc[3].split('-')[0]
            if winner == team:
                return desc[4].split('-')[0]
            else:
                return desc[4].split('-')[1]
        elif desc[2] == "tied":
            #"tied"
            return desc[3].split('-')[0]
        else:
            #now tied
            return desc[4].split('-')[0]
    else:
        if desc[1] == "trails" or desc[1] == "leads":
            #team score is in desc[2].split('-')[0]
            if winner == team:
                return desc[2].split('-')[0]
            else:
                return desc[2].split('-')[1]
        elif desc[2] == "trails" or desc[2] == "leads":
            #accounting for phrase "now leads" or "now trails"
            #team score is in desc[3].split('-')[0]
            if winner == team:
                return desc[3].split('-')[0]
            else:
                return desc[3].split('-')[1]
        elif desc[1] == "tied":
            #"tied"
            return desc[2].split('-')[0]
        else:
            #now tied
            return desc[3].split('-')[0]
        
def get_loser_score(loser, team, play):
    desc = play.split('<br>')[2].split(' ')
    if desc[0] == "LA" or desc[0] == "New" or desc[0] == 'San' or desc[0] == 'Golden':
        if desc[2] == "trails" or desc[2] == "leads":
            #team score is in desc[2].split('-')[2]
            if loser == team:
                return desc[3].split('-')[0]
            else:
                return desc[3].split('-')[1]
        elif desc[3] == "trails" or desc[3] == "leads":
            #team score is in desc[3].split('-')[2]
            if loser == team:
                return desc[4].split('-')[0]
            else:
                return desc[4].split('-')[1]
        elif desc[2] == "tied":
            #"tied"
            return desc[3].split('-')[0]
        else:
            #now tied
            return desc[4].split('-')[0]
    else:
        if desc[1] == "trails" or desc[1] == "leads":
            #team score is in desc[2].split('-')[2]
            if loser == team:
                return desc[2].split('-')[0]
            else:
                return desc[2].split('-')[1]
        elif desc[2] == "trails" or desc[2] == "leads":
            #team score is in desc[3].split('-')[2]
            if loser == team:
                return desc[3].split('-')[0]
            else:
                return desc[3].split('-')[1]
        elif desc[1] == "tied":
            #"tied"
            return desc[2].split('-')[0]
        else:
            #now tied
            return desc[3].split('-')[0]

def initiate_streaming_to_bucket(df, path):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.
    
    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :return: None
    """
    logger.info("Initiating streaming process...")
    stream_query = (df.writeStream
                    .format("parquet")
                    .outputMode("append")
                    .option("path", path)
                    .start())
    stream_query.awaitTermination()


def main():
    app_name = "SparkStructuredStreamingToS3"
    access_key = os.environ['AWS_ACCESS_KEY']
    secret_key = os.environ['AWS_SECRET_KEY']
    brokers = "broker:19092"
    topic = "shot_charts"
    path = "s3://nba-shot-charts"
    # checkpoint_location = "CHECKPOINT_LOCATION"

    spark = initialize_spark_session(app_name, access_key, secret_key)
    if spark:
        df = get_streaming_dataframe(spark, brokers, topic)
        if df:
            transformed_df = transform_streaming_data(df)
            initiate_streaming_to_bucket(transformed_df, path)


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
