import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import datetime
import pyspark.pandas as ps
import pandas as pd
import os
import time

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


def transform_streaming_data(spark, df):
    """
    Transform the initial dataframe to get the final structure.
    
    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """
    
    # base_df = df.selectExpr("CAST(value as STRING)")
    json_schema = (
        StructType()
        .add("game_id", StringType())
        .add("year", StringType())
        .add("month", StringType())
        .add("day", StringType())
        .add("winner", StringType())
        .add("loser", StringType())
        .add("x", StringType())
        .add("y", StringType())
        .add("play", StringType())
    )

    parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("parsed_value", from_json(col("value"), json_schema)) \
    .select("parsed_value.*")

    # # get unique list of game ids
    # game_ids = parsed_df.drop_duplicates(['game_id']).select(['game_id']).rdd.flatMap(lambda x: x).collect()
    # #add columns to dataframe
    # list_of_columns = parsed_df.columns + ["time_remaining","quarter"]
    # # create empty dataframe with list of columns
    # # final_df = pd.DataFrame(columns=list_of_columns)
    # final_df = spark.createDataFrame(data=[],schema=list_of_columns)

    # # for each game
    # for game in game_ids:
    #     # get dataframe of shots on that game
    #     temp_df = parsed_df.filter(col("game_id")==game)
    #     # populate the time_remaining column
    #     # temp_df['time_remaining'] = temp_df['play'].apply(lambda x: datetime.datetime.strptime(str(x).split(' ')[2], '%H:%M.%S')) 
    #     time_remaining_UDF = udf(lambda x:time_remaining(x),TimestampType()) 
    #     temp_df = temp_df.withColumn("time_remaining", time_remaining_UDF(col("play")))

    #     # populate the quarter column
    #     # temp_df['quarter'] = temp_df['play'].apply(lambda x: str(x).split(' ')[0][0]) 
    #     quarter_UDF = udf(lambda x:quarter(x), StringType())
    #     temp_df = temp_df.withColumn("quarter", quarter_UDF(col("play")))

    #     #  sort the plays by quarter and time_remaining
    #     temp_df = temp_df.sort(['quarter','time_remaining'],ascending=[True,False])
    #     # add the new dataframe to the final df
    #     final_df = final_df.union(temp_df)

    time_remaining_UDF = udf(lambda x:time_remaining(x),TimestampType()) 
    parsed_df = parsed_df.withColumn("time_remaining", time_remaining_UDF(col("play")))

    # populate the quarter column
    # temp_df['quarter'] = temp_df['play'].apply(lambda x: str(x).split(' ')[0][0]) 
    quarter_UDF = udf(lambda x:quarter(x), StringType())
    parsed_df = parsed_df.withColumn("quarter", quarter_UDF(col("play")))

    # bunch of transformations
    # final_df['time_remaining'] = final_df['play'].apply(lambda x: str(x).split(' ')[2]) 
    time_remaining_final_UDF = udf(lambda x: time_remaining_final(x), StringType())
    parsed_df = parsed_df.withColumn("time_remaining", time_remaining_final_UDF(col("play")))

    # final_df['shots_by'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[0]+" "+str(x).split('<br>')[1].split(' ')[1]) 
    shots_by_final_UDF = udf(lambda x: shots_by_final(x), StringType())
    parsed_df = parsed_df.withColumn("shots_by", shots_by_final_UDF(col("play")))

    # final_df['outcome'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[2]) 
    outcome_final_UDF = udf(lambda x: outcome_final(x), StringType())
    parsed_df = parsed_df.withColumn("outcome", outcome_final_UDF(col("play")))

    # final_df['attempt'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[3]) 
    attempt_final_UDF = udf(lambda x: attempt_final(x), StringType())
    parsed_df = parsed_df.withColumn("attempt", attempt_final_UDF(col("play")))

    # final_df['distance'] = final_df['play'].apply(lambda x: str(x).split('<br>')[1].split(' ')[-2]+str(x).split('<br>')[1].split(' ')[-1]) 
    distance_final_UDF = udf(lambda x: distance_final(x), StringType())
    parsed_df = parsed_df.withColumn("distance", distance_final_UDF(col("play")))

    # final_df['team'] = final_df['play'].apply(lambda x: get_team(x)) 
    team_UDF = udf(lambda x: get_team(x), StringType())
    parsed_df = parsed_df.withColumn("team", team_UDF(col("play")))

    # final_df['winner_score'] = final_df.apply(lambda x: get_winner_score(x.winner, x.team, x.play), axis=1) 
    winner_score_UDF = udf(lambda x: get_winner_score(x), StringType())
    parsed_df = parsed_df.withColumn("winner_score", winner_score_UDF(col("play")))

    # final_df['loser_score'] = final_df.apply(lambda x: get_loser_score(x.loser, x.team, x.play), axis=1) 
    loser_score_UDF = udf(lambda x: get_loser_score(x), StringType())
    parsed_df = parsed_df.withColumn("loser_score", loser_score_UDF(col("play")))

    return  parsed_df

def time_remaining(x):
    return datetime.datetime.strptime(str(x).split(' ')[2], '%H:%M.%S')

def quarter(x):
    return str(x).split(' ')[0][0]

def time_remaining_final(x):
    return str(x).split(' ')[2]

def shots_by_final(x):
    return str(x).split('<br>')[1].split(' ')[0]+" "+str(x).split('<br>')[1].split(' ')[1]

def outcome_final(x):
    return str(x).split('<br>')[1].split(' ')[2]

def attempt_final(x):
    return str(x).split('<br>')[1].split(' ')[3]

def distance_final(x):
    return str(x).split('<br>')[1].split(' ')[-2]+str(x).split('<br>')[1].split(' ')[-1]

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

def initiate_streaming_to_bucket(df, path, checkpoint_location):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.
    
    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :return: None
    """
    logger.info("Initiating streaming process...")
    stream_query = df.writeStream.format("parquet").outputMode("append").option("path", path).option("checkpointLocation", checkpoint_location).start()
    stream_query.awaitTermination()


def main():
    app_name = "SparkStructuredStreamingToS3"
    # TODO: move the access keys to env variable
    access_key = os.environ['AWS_ACCESS_KEY']
    secret_key = os.environ['AWS_SECRET_KEY']
    brokers = "broker:19092"
    topic = "shot_charts"
    path = "s3a://nba-shot-charts"
    checkpoint_location = "/opt/airflow/dags/"

    spark = initialize_spark_session(app_name, access_key, secret_key)
    if spark:
        df = get_streaming_dataframe(spark, brokers, topic)
        if df:
            transformed_df = transform_streaming_data(spark, df)
            initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
