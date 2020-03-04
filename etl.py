import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import desc,asc
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ 
    This method perform the creation of the spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    This method is in charge of performing the ETL and creation of the tables related to the song_data using parquet, these tables are: songs and artists.
    
    parameters: 
         spark= represent the spark session. 
         input_data = represent the source root path of the data that will be processed
         output_data = represent the destination root path where the parquet tables will be created.
    """
    # get filepath to song data file
    song_data = input_data+"/song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs.parquet",mode='overwrite',partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists.parquet",mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """ 
     This method is in charge of performing the ETL and creation of the tables related to the log_data using parquet, these tables are: users, time and songplays
     
     parameters:
         spark= represent the spark session. 
         input_data = represent the source root path of the data that will be processed
         output_data = represent the destination root path where the parquet tables will be created.
     
    """
    
    # get filepath to log data file
    log_data = input_data+"/log_data/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = filtered_log_data=df.where("page='NextSong'")

    # extract columns for users table    
    users_columns=["userId as user_id","firstName as first_name","lastName as last_name","gender","level"]
    users_table=df.selectExpr(users_columns).orderBy(desc("ts")).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users.parquet",mode="ignore")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda epoch_in_millis:int(epoch_in_millis/1000) ,LongType())
    df = df.withColumn("timestamp",get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda epoch_in_seconds:datetime.fromtimestamp(epoch_in_seconds) ,TimestampType())
    df = df.withColumn("datetime",get_datetime("timestamp"))
    
    # extract columns to create time table
    time_columns=[col("datetime").alias("start_time"),hour("datetime").alias("hour"),
              dayofmonth("datetime").alias("day"),weekofyear("datetime").alias("week"),
              month("datetime").alias("month"),year("datetime").alias("year")]
    time_table = df.select(time_columns).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+"time.parquet",mode="ignore",partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+"/song_data/*/*/*")

    # extract columns from joined song and log datasets to create songplays table 
    song_plays_columns = [monotonically_increasing_id().alias('songplay_id'),
                        get_datetime(get_timestamp("ts")).alias("start_time"),col("userId").alias("user_id"),
                        "level","song_id","artist_id",col("sessionId").alias("session_id"), "location",
                        col("userAgent").alias("user_agent")]
    songplays_table = df.join(song_df,df.song==song_df.title,"inner").select(song_plays_columns)

    # write songplays table to parquet files partitioned by artist_id
    songplays_table.write.parquet(output_data+"songplays.parquet",mode="ignore",partitionBy="artist_id") 


def main():
    """ 
      Main method where the execution of the other methods is orchestrated.
    """
    spark = create_spark_session()
    input_data = config['PATHS']['INPUT_DATA']
    output_data = config['PATHS']['OUTPUT_DATA']
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
