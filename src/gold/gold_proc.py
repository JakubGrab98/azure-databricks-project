from pyspark.sql import SparkSession
import views as v
from common.spark_session import get_spark



def main(spark: SparkSession) -> None:
    v.create_bridge_tables(spark, "publishers", "publishers_key", "publisher_name")
    v.create_bridge_tables(spark, "genres", "genres_key", "genre")
    v.create_bridge_tables(spark, "developers", "developers_key", "developer_name")
    v.create_bridge_tables(spark, "supported_languages", "languages_key", "language")
    v.create_dim_players(spark)
    v.create_dim_prices(spark)
    v.create_dim_achievement(spark)
    v.create_dim_games(spark)
    v.create_games_flat(spark)
    v.create_fact_achievement(spark)

if __name__ == "__main__":
    spark = get_spark()
    main(spark)
    