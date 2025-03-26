from pyspark.sql import SparkSession


def create_dim_players(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.dim_players AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY unique_playerid) AS player_key,
        nickname,
        country,
        source_folder AS console
    FROM silver.players
    """)
