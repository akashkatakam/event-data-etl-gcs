from datetime import datetime
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window, SQLContext
from pyspark.sql.functions import (udf, col, year, month, dayofmonth, hour,
                                   weekofyear, date_format, dayofweek, max, monotonically_increasing_id)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType)
import argparse
import sys


def process_song_data(spark, input_data, output_data):
    """
    Transform raw song data from GCS into analytics tables on GCS

    This function reads in song data in JSON format from GCS; defines the schema
    of songs and artists analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    GCS.

    Args:
        spark: a Spark session
        input_data: an GCS bucket to read song data in from
        output_data: an GCS bucket to write analytics tables to
    """

    print(input_data)
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    song_data_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    df = spark.read.json(song_data, schema=song_data_schema)
    print("read song json done")
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs_table.parquet",
        mode="overwrite",
        partitionBy=["year"]
    )


    # extract columns to create artists table
    artists_table = (
        df
            .select(
            "artist_id",
            col("artist_name").alias("name"),
            col("artist_location").alias("location"),
            col("artist_latitude").alias("latitude"),
            col("artist_longitude").alias("longitude"))
            .distinct()
    )

    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists_table.parquet", mode="overwrite"
    )
    print('Success: Wrote artists_table to parquet')


def process_log_data(spark, input_data, output_data):
    """
    Transform raw log data from GCS into analytics tables on GCS

    This function reads in log data in JSON format from GCS; defines the schema
    of songplays, users, and time analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    GCS.

    Args:
        spark: a Spark session
        input_data: an GCS bucket to read log data in from
        output_data: an GCS bucket to write analytics tables to
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    df = spark.read.json(log_data, schema=log_data_schema)

    print('Success: Read log_data from GCS')

    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table
    users_table = (
        df
            .withColumn("max_ts_user", max("ts").over(Window.partitionBy("userID")))
            .filter(
            (col("ts") == col("max_ts_user")) &
            (col("userID") != "") &
            (col("userID").isNotNull())
        )
            .select(
            col("userID").alias("user_id"),
            col("firstName").alias("first_name"),
            col("lastName").alias("last_name"),
            "gender",
            "level"
        )
    )

    # write users table to parquet files
    users_table.write.parquet(
        output_data + "users_table.parquet", mode="overwrite"
    )

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn("start_time", get_datetime("ts"))

    # extract columns to create time table
    time_table = (
        df
            .withColumn("hour", hour("start_time"))
            .withColumn("day", dayofmonth("start_time"))
            .withColumn("week", weekofyear("start_time"))
            .withColumn("month", month("start_time"))
            .withColumn("year", year("start_time"))
            .withColumn("weekday", dayofweek("start_time"))
            .select("start_time", "hour", "day", "week", "month", "year", "weekday")
            .distinct()
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        output_data + "time_table.parquet",
        mode="overwrite",
        partitionBy=["year"]
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table
    artists_table = spark.read.parquet(output_data + "artists_table.parquet")
    songs = (
        song_df
            .join(artists_table, "artist_id", "full")
            .select("song_id", "title", "artist_id", "name", "duration")
    )
    songplays_table = df.join(
        songs,
        [
            df.song == songs.title,
            df.artist == songs.name,
            df.length == songs.duration
        ],
        "left"
    )
    songplays_table = (
        songplays_table
            .join(time_table, "start_time", "left")
            .select(
            "start_time",
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent"),
            "year",
            "month"
        )
            .withColumn("songplay_id", monotonically_increasing_id())
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        output_data + "songplays_table.parquet",
        mode="overwrite",
        partitionBy=["year"]
    )


def main(spark, bucket):
    """Run ETL pipeline"""

    # GCS bucket name to create and output tables to
    input_data = bucket + "raw/"
    output_data = bucket + "transformed/"
    print(spark)
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.setAppName('Sparkify etl')
    spark_context = SparkContext(conf=spark_conf)
    spark_context._jsc.hadoopConfiguration().set("fs.gs.implicit.dir.repair.enable", "false")
    sqlContext = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bucket',
        dest='bucket',
        required=True,
        help='Specify the full GCS wildcard path to the json files to enhance.'
    )

    known_args, _ = parser.parse_known_args(None)

    main(spark=spark,
         bucket=known_args.bucket)
