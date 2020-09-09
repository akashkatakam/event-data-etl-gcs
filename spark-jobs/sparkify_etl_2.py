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


def process_log_data(spark, input_data, output_data, input_file_name):
    """
    Transform raw log data from GCS into analytics tables on GCS

    This function reads in log data in JSON format from GCS; defines the schema
    of songplays, users, and time analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    GCS.

    Args:
        spark: a Spark session
        input_data: an GCS bucket to read event data in from
        output_data: an GCS bucket to write analytics tables to
        :param input_file_name: raw File name
    """

    # get filepath to log data file
    log_data = input_data + input_file_name
    print(log_data)

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
    print("Success:Read json")

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
    users_table.coalesce(1).write.parquet(
        output_data + "partitioned/users_table.parquet", mode="overwrite"
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

    print(time_table.count())

    # time_table.write.partitionBy(df['month']).parquet(output_data+"time_table.parquet")
    # write time table to parquet files partitioned by year and month
    time_table.coalesce(1).write.parquet(
        output_data + "/partitioned/time_table.parquet",
        mode="overwrite"
    )


def main(spark, bucket, input_file_name):
    """Run ETL pipeline"""

    # GCS bucket name to create and output tables to
    input_data = "gs://" + bucket + "/raw/"
    output_data = "gs://" + bucket + "/transformed/"
    print(spark)
    # process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data, input_file_name)


if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.setAppName('Sparkify etl')
    spark_context = SparkContext(conf=spark_conf)
    sqlContext = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bucket',
        dest='bucket',
        required=True,
        help='Specify the full GCS wildcard path to the json files to enhance.'
    )

    parser.add_argument(
        '--raw_file_name',
        dest='raw_file_name',
        required=True,
        help='Specify the full GCS wildcard path to the json files to enhance.'
    )

    known_args, _ = parser.parse_known_args(None)

    main(spark=spark,
         bucket=known_args.bucket, input_file_name=known_args.raw_file_name)
