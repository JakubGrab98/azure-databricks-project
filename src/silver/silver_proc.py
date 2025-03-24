import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.types import DateType, FloatType, IntegerType
from typing import List


GAMES_COLUMNS_TO_CONVERT = ["developers", "publishers", "genres", "supported_languages"]

def convert_to_array(df: DataFrame, columns: List[str], array_type: str) -> DataFrame:
    for column in columns:
        df = df.withColumn(
            column,
            f.expr(f"split(regexp_replace({column}, '^\[|\]$', ''), ',')").cast(array_type)
    )
    return df

def remove_quotation(df: DataFrame, columns: List[str]) -> DataFrame:
    for column in columns:
        df = df.withColumn(
            column,
            f.trim(f.regexp_replace(column, '^"|"$', ''))
        )
    return df

def convert_to_float(df: DataFrame, columns: List[str]):
    for column in columns:
        df = df.withColumn(column, df.column.cast(FloatType()))
    return df

def transform_silver_table(spark: SparkSession, bronze_table: str, transformation):
    df = spark.sql(f"SELECT * FROM bronze.{bronze_table}")
    transformed_df = df.transform(transformation, df)
    return transformed_df

def load_silver(spark: SparkSession, df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Loads data to bronze schema.

    Args:
        spark (SparkSession): SparkSession instance.
        df (DataFrame): DataFrame wit data to be loaded.
        table_name (str): Destination table name.
        mode (str, optional): Writing mode. Defaults to "overwrite".
    """
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}" )
    spark.sql(f"""
        CREATE TABLE silver.{table_name}
        USING DELTA
        LOCATION 's3a://gaming/silver/{table_name}'
    """)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(f"s3a://gaming/silver/{table_name}")

def transform_bronze_games(df: DataFrame):
    df_silver_games = (
        df
        .filter(f.col("title").isNotNull())
        .withColumn("platform", f.when(f.left(df.platform, f.lit(2)) == "PS", df.platform)
                                .otherwise(f.lit("N/A"))
        )
        .withColumn("release_date", df.release_date.cast(DateType()))
        .withColumnRenamed("platform", "subplatform")
        .withColumnRenamed("source_folder", "platform")
        .fillna("N/A")
        .transform(convert_to_array, GAMES_COLUMNS_TO_CONVERT, "array<string>")
    )
    return df_silver_games

def transform_bronze_achievements(df: DataFrame):
    string_columns = ["title", "description", "rarity"]
    df_silver_achievements = (
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
    return df_silver_achievements

def transform_bronze_prices(df: DataFrame):
    w = Window.partitionBy("unique_gameid").orderBy("start_date")
    prices_lag_df = (
        df
        .filter(f.col("usd").isNotNull())
        .withColumnRenamed("usd", "price")
        .withColumnRenamed("date_acquired", "start_date")
        .withColumn("currency", f.lit("USD"))
        .withColumn("prev_price",
            f.lag("price", 1).over(w)
        )
    )

    prices_flagged_df = (
        prices_lag_df
        .withColumn("price_changes",
            f.when((f.col("price") != f.col("prev_price")) | (f.col("prev_price").isNull()), 1)
            .otherwise(0)
        )
    )

    filtered_prices = (
        prices_flagged_df
        .filter(f.col("price_changes") == 1)
        .withColumn("next_start", f.lead("start_date", 1).over(w))
        .withColumn("end_date", f.date_sub(f.col("next_start").cast(DateType()), 1))
        .withColumn("price", prices_flagged_df.price.cast(FloatType()))
        .withColumn("start_date", prices_flagged_df.start_date.cast(DateType()))
        .withColumn("gameid", prices_flagged_df.gameid.cast(IntegerType()))
    )
    return filtered_prices

def transform_bronze_players(df: DataFrame):
    df_silver_players = (
        df
        .fillna("N/A")
        .withColumn("country", f.trim(f.col("country")))
        .withColumn("nickname", f.trim(f.col("nickname")))
    )
    return df_silver_players

def process_silver_table(spark: SparkSession, table_name: str, transform_func):
    silver_df = transform_silver_table(spark, table_name, transform_func)
    load_silver(spark, silver_df, table_name)

def main(spark: SparkSession):
    process_silver_table(spark, "games_titles", transform_bronze_games)
    process_silver_table(spark, "achievements", transform_bronze_achievements)
    process_silver_table(spark, "games_prices", transform_bronze_prices)


if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder
        .appName("Gaming")  # type: ignore
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.3.101:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", "s3a://gaming/")
        .enableHiveSupport()
        .getOrCreate()
    )

    main(spark)
