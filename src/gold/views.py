from pyspark.sql import SparkSession


def create_dim_players(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.dim_players AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY unique_playerid) AS player_key,
        playerid,
        nickname,
        country,
        source_folder AS console
    FROM silver.players
    """)

def create_dim_prices(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.dim_prices AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY unique_gameid) AS player_key,
        gameid,
        source_folder AS console,
        price,
        currency,
        start_date,
        end_date
    FROM silver.games_prices
    """)
