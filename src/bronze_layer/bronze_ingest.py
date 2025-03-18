import os
from delta import *
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from functools import reduce


def read_bronze_csv(spark: SparkSession, root_path: str, file_name: str) -> DataFrame:
    subfolders = ["playstation", "steam", "xbox"]
    dataframes = []
    
    for folder in subfolders:
        file_path = f"{root_path}/{folder}/{file_name}"

        try:
            raw_df = spark.read.format("csv").option("header", "true").load(file_path)
        except Exception as e:
            print(f"An error occured during loading a file: {file_path}: {e}")
            continue

        bronze_df = (
            raw_df
            .withColumn("source_file", f.input_file_name())
            .withColumn("ingestion_timestamp", f.current_timestamp())
            .withColumn("source_folder", f.lit(folder))
        )

        dataframes.append(bronze_df)

    if not dataframes:
        raise ValueError("No data to process!")

    return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes)

def load_bronze(spark: SparkSession, df: DataFrame, table_name: str, mode: str = "overwrite"):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"Dane zapisane w {table_name}")

def process_table(spark: SparkSession, file_name: str, table_name: str, unique_id_column: str):
    df = read_bronze_csv(spark, "s3a://gaming/raw", file_name)

    if unique_id_column in df.columns:
        df = df.withColumn(f"unique_{unique_id_column}", f.concat_ws("-", df[unique_id_column], df["source_folder"]))

    load_bronze(spark, df, f"bronze.{table_name}")

def main(spark: SparkSession):
    process_table(spark, "games.csv", "games_titles", "gameid")
    process_table(spark, "achievements.csv", "achievements", "achievementid")
    process_table(spark, "history.csv", "achievements_history", "achievementid")
    process_table(spark, "players.csv", "players", "playerid")
    process_table(spark, "prices.csv", "games_prices", "gameid")
    process_table(spark, "purchased_games.csv", "purchased_games", "playerid")


if __name__ == "__main__":

    builder: SparkSession = (
        SparkSession.builder
        .appName("Gaming")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.3.101:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.sql.warehouse.dir", "s3a://gaming/tmp/spark-warehouse")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    main(spark)
    
    # df_bronze.show()
    # df_bronze.printSchema()
    # df_bronze.select("platform").distinct().show()
    # df_bronze.select("developers").distinct().show()
    # df_bronze.select("publishers").distinct().show()
    # df_bronze.select("genres").distinct().show()
    # df_bronze.select("supported_languages").distinct().show()
    # df_bronze.groupBy("unique_game_id").count().filter(f.col("count") > 1).show()
    # df_bronze.select([f.sum(f.col(c).isNull().cast("int")).alias(c) for c in df_bronze.columns]).show()
