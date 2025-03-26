from pyspark.sql import SparkSession


def create_dim_players(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.dim_players AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY p.unique_playerid) AS player_key,
        p.playerid,
        p.nickname,
        p.country,
        p.source_folder AS console,
        COALESCE(g.no_purchased_games, 0) AS total_owned_games
    FROM silver.players p
    LEFT JOIN silver.purchased_games g
        ON p.unique_playerid = g.unique_playerid
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

def create_dim_achievement(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.dim_achievement AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY unique_achievementid) AS achievement_key,
        achievementid,
        source_folder AS console,
        title,
        description
    FROM silver.achievements
    """)
