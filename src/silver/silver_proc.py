"""Module responsibles for processing data from bronze to silver layer."""
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.types import DateType, FloatType, IntegerType, LongType
from typing import Callable


sys.path.append("../")
from utils import convert_to_array, remove_quotation
from common.spark_session import get_spark, get_storage_path


GAMES_COLUMNS_TO_CONVERT = ["developers", "publishers", "genres", "supported_languages"]


def transform_silver_table(spark: SparkSession, bronze_table: str, transformation: Callable) -> DataFrame:
    """
    Applies transformation to a Bronze table and returns the transformed Silver DataFrame.

    Args:
        spark (SparkSession): Spark session.
        bronze_table (str): Bronze table name.
        transformation (Callable): Transformation function.

    Returns:
        DataFrame: Transformed Silver DataFrame.
    """
    df = spark.sql(f"SELECT * FROM bronze.{bronze_table}")
    transformed_df = df.transform(transformation)
    return transformed_df

def load_silver(spark: SparkSession, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Saves the DataFrame to Delta format in the silver layer and registers the table.

    Args:
        spark (SparkSession): SparkSession instance.
        df (DataFrame): DataFrame to be saved.
        table_name (str): Destination table name.
        mode (str): Write mode. Defaults to "overwrite".
    """
    silver_path = get_storage_path(spark, "silver.container")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}" )
    spark.sql(f"""
        CREATE TABLE silver.{table_name}
        USING DELTA
        LOCATION '{silver_path}/{table_name}'
    """)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(f"{silver_path}/{table_name}")

def transform_bronze_games(df: DataFrame) -> DataFrame:
    """Transforms games data from bronze to cleaned silver version."""
    return (
        df
        .filter(f.col("title").isNotNull())
        .withColumn("platform", f.when(f.left(df.platform, f.lit(2)) == "PS", df.platform)
                                .otherwise(f.lit("N/A"))
        )
        .withColumn("release_date", df.release_date.cast(DateType()))
        .withColumnRenamed("platform", "subplatform")
        .withColumnRenamed("source_folder", "platform")
        .fillna("N/A")
        .transform(convert_to_array, GAMES_COLUMNS_TO_CONVERT, r"[\[\]'\" ]", "array<string>")
    )

def transform_bronze_achievements(df: DataFrame) -> DataFrame:
    """Cleans and filters achievements data from bronze layer."""
    string_columns = ["title", "description", "rarity"]
    return (
        df
        .filter(
            (f.col("achievementid").rlike(r"^\d+_\d+$"))
            & (f.col("gameid").isNotNull())
        )
        .transform(remove_quotation, string_columns)
        .select(
            "unique_achievementid", "achievementid", "gameid", "title",
            "description", "source_file", "ingestion_timestamp", "source_folder",
        )
        .fillna("N/A")
    )

def transform_bronze_prices(df: DataFrame) -> DataFrame:
    """Extracts price change ranges from historical prices data."""
    w = Window.partitionBy("unique_gameid").orderBy("start_date")
    prices_df = (
        df
        .filter(f.col("usd").isNotNull())
        .withColumnRenamed("usd", "price")
        .withColumnRenamed("date_acquired", "start_date")
        .withColumn("currency", f.lit("USD"))
        .withColumn("prev_price",
            f.lag("price", 1).over(w)
        )
        .withColumn("price_changes",
            f.when((f.col("price") != f.col("prev_price")) | (f.col("prev_price").isNull()), 1)
            .otherwise(0)
        )
        .filter(f.col("price_changes") == 1)
        .withColumn("next_start", f.lead("start_date", 1).over(w))
        .withColumn("end_date", f.date_sub(f.col("next_start").cast(DateType()), 1))
        .withColumn("price", f.col("price").cast(FloatType()))
        .withColumn("start_date", f.col("start_date").cast(DateType()))
        .withColumn("gameid", df.gameid.cast(IntegerType()))
    )
    return prices_df

def transform_bronze_players(df: DataFrame) -> DataFrame:
    """Cleans player data by trimming and filling missing values."""
    return (
        df
        .fillna("N/A")
        .withColumn("country", f.trim(f.col("country")))
        .withColumn("nickname", f.trim(f.col("nickname")))
    )

def transform_bronze_purchased_games(df: DataFrame) -> DataFrame:
    """Cleans and prepares purchased games data with array size info."""
    return (
        df
        .transform(convert_to_array, ["library"], r"[\\[\\] ]", "array<int>")
        .withColumn("no_purchased_games", f.size("library"))
    )

def transform_bronze_achievement_history(df: DataFrame) -> DataFrame:
    """Transforms achievement history data with correct IDs and date formatting."""
    return (
        df
        .withColumn(
            "unique_achievementid",
            f.concat_ws("_", f.col("playerid"), f.col("unique_achievementid"))
        )
        .withColumn("date_acquired", df.date_acquired.cast(DateType()))
        .withColumn("playerid", df.playerid.cast(LongType()))
    )

def process_silver_table(spark: SparkSession, table_name: str, transform_func: Callable) -> None:
    """Processes and saves one silver table."""
    silver_df = transform_silver_table(spark, table_name, transform_func)
    load_silver(spark, silver_df, table_name)

def main(spark: SparkSession) -> None:
    """Main function to run silver layer transformations for all datasets."""
    process_silver_table(spark, "games_titles", transform_bronze_games)
    process_silver_table(spark, "achievements", transform_bronze_achievements)
    process_silver_table(spark, "games_prices", transform_bronze_prices)
    process_silver_table(spark, "achievements_history", transform_bronze_achievement_history)
    process_silver_table(spark, "purchased_games", transform_bronze_purchased_games)
    process_silver_table(spark, "players", transform_bronze_players)


if __name__ == "__main__":
    spark = get_spark()

    main(spark)
