"""Module responsibles for ingest raw data to bronze layer."""
import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from functools import reduce
from common.spark_session import get_spark


def read_bronze_csv(spark: SparkSession, root_path: str, file_name: str) -> DataFrame:
    """Reads raw csv data from each konsole subfolder.

    Args:
        spark (SparkSession): SparkSession instance.
        root_path (str): Root path to source folder.
        file_name (str): File name to read.

    Raises:
        ValueError: If there is no data to read.

    Returns:
        DataFrame: Appended dataframe with data from each console.
    """
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
    """Loads data to bronze schema.

    Args:
        spark (SparkSession): SparkSession instance.
        df (DataFrame): DataFrame wit data to be loaded.
        table_name (str): Destination table name.
        mode (str, optional): Writing mode. Defaults to "overwrite".
    """
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql(f"DROP TABLE IF EXISTS bronze.{table_name}" )
    spark.sql(f"""
        CREATE TABLE bronze.{table_name}
        USING DELTA
        LOCATION 's3a://gaming/bronze/{table_name}'
    """)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(f"s3a://gaming/bronze/{table_name}")

def process_bronze_table(spark: SparkSession, file_name: str, table_name: str, unique_id_column: str):
    """Combines read and load data for bronze layer.

    Args:
        spark (SparkSeprocess_bronze_tablession): SparkSession instance.
        file_name (str): CSV source file.
        table_name (str): Destination table name.
        unique_id_column (str): Column used to create unique id with source_folder value.
    """
    df = read_bronze_csv(spark, "s3a://gaming/raw", file_name)

    if unique_id_column in df.columns:
        df = df.withColumn(f"unique_{unique_id_column}", f.concat_ws("-", df[unique_id_column], df["source_folder"]))

    load_bronze(spark, df, table_name)

def main(spark: SparkSession):
    """Main function for processing bronze layer.

    Args:
        spark (SparkSession): SparkSession instance.
    """
    process_bronze_table(spark, "games.csv", "games_titles", "gameid")
    process_bronze_table(spark, "achievements.csv", "achievements", "achievementid")
    process_bronze_table(spark, "history.csv", "achievements_history", "achievementid")
    process_bronze_table(spark, "players.csv", "players", "playerid")
    process_bronze_table(spark, "prices.csv", "games_prices", "gameid")
    process_bronze_table(spark, "purchased_games.csv", "purchased_games", "playerid")


if __name__ == "__main__":

    spark = get_spark()

    main(spark)
