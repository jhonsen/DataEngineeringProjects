import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T 
from pyspark.sql import SQLContext
from spyspark.sql import functions as F 

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    artists_table = df.select('userId','firstName', 'lastName', 'gender','level')
    
    # write users table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), T.TimestampType()) 
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), T.DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('start_time', df.timestamp)
    df = df.withColumn('hour', hour(df.timestamp))
    df = df.withColumn('day', dayofmonth(df.timestamp))
    df = df.withColumn('week', weekofyear(df.timestamp))
    df = df.withColumn('month', month(df.timestamp))
    df = df.withColumn('year', year(df.timestamp))
    df = df.withColumn('weekday', date_format(df.timestamp, 'E'))

    time_table = df.select('start_time', 'hour','day','week','month','year','weekday') 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time')

    # read in song data to use for songplays table
    sqlContext = SQLContext(spark)
    song_df = sqlContext.read.parquet(output_data+'songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration), 'left_outer') \
                        .select(df.start_time,
                                df.userId.alias("user_id"),
                                df.level,
                                song_df.song_id,
                                song_df.artist_id,
                                df.sessionId.alias("session_id"),
                                df.location,
                                df.userAgent.alias("user_agent"),
                                df.year,
                                df.month) \
                        .withColumn("songplay_id", F.monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://dend-for-spark/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
