"""Module responsibles for processing raw data to bronze layer."""
from pyspark.sql import SparkSession
from bronze_ingest import process_bronze_table
from common.spark_session import get_spark


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
