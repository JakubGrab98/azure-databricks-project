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
        ROW_NUMBER() OVER (ORDER BY unique_gameid) AS price_key,
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

def create_bridge_tables(
    spark: SparkSession, 
    array_column: str,
    dim_key: str,
    dim_value: str
):
    spark.sql(f"""
    CREATE OR REPLACE VIEW gold.dim_{array_column} AS
    WITH exploded AS (
        SELECT 
            explode({array_column}) AS {dim_value}
        FROM silver.games_titles
    ),
    dim_{array_column} AS (
        SELECT DISTINCT {dim_value},
            ROW_NUMBER() OVER (ORDER BY {dim_value}) AS {dim_key}
        FROM exploded
    )
    SELECT * FROM dim_{array_column}
    """)

    spark.sql(f"""
    CREATE OR REPLACE VIEW gold.bridge_game_{array_column} AS
    WITH exploded AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY unique_gameid) AS game_key, 
        explode({array_column}) AS {dim_value}
    FROM silver.games_titles
    ),
    dim_{array_column} AS (
    SELECT DISTINCT {dim_value},
            ROW_NUMBER() OVER (ORDER BY {dim_value}) AS {dim_key}
    FROM exploded
    )
    SELECT e.game_key, d.{dim_key}
    FROM exploded e
    JOIN gold.dim_{array_column} d ON e.{dim_value} = d.{dim_value}
    """)

def create_dim_games(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.dim_games AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY unique_gameid) AS game_key,
        gameid,
        title,
        platform AS console,
        subplatform AS platform,
        release_date
    FROM silver.games_titles
    """)

def create_games_flat(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.games_flat AS
    SELECT 
        g.game_key,
        g.gameid,
        g.title,
        g.console,
        g.platform,
        g.release_date,
        p.publisher_name,
        d.developer_name,
        ge.genre,
        l.language
    FROM gold.dim_games g
    LEFT JOIN gold.bridge_game_publishers bgp ON g.game_key = bgp.game_key
    LEFT JOIN gold.dim_publishers p ON bgp.publishers_key = p.publishers_key

    LEFT JOIN gold.bridge_game_developers bgd ON g.game_key = bgd.game_key
    LEFT JOIN gold.dim_developers d ON bgd.developers_key = d.developers_key

    LEFT JOIN gold.bridge_game_genres bgg ON g.game_key = bgg.game_key
    LEFT JOIN gold.dim_genres ge ON bgg.genres_key = ge.genres_key

    LEFT JOIN gold.bridge_game_supported_languages bgl ON g.game_key = bgl.game_key
    LEFT JOIN gold.dim_supported_languages l ON bgl.languages_key = l.languages_key
""")


def create_fact_achievement(spark: SparkSession):
    spark.sql("""
    CREATE OR REPLACE VIEW gold.fact_achievement AS
    WITH fact AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY date_acquired) AS fact_key, 
        playerid,
        SUBSTRING(achievementid, 1, INSTR(achievementid, '_')) AS gameid,
        SUBSTRING(
              achievementid, INSTR(achievementid, '_') + 1, LENGTH(achievementid)
        ) AS achievementid,
        source_file AS console,
        date_acquired
    FROM silver.achievements_history
    )
    SELECT 
        f.fact_key,
        f.date_acquired,
        f.console,
        g.game_key,
        p.player_key,
        pr.price_key,
        a.achievement_key
    FROM fact f
    JOIN gold.dim_players p
        ON f.playerid = p.playerid AND f.console = p.console
    JOIN gold.dim_prices pr
        ON f.gameid = pr.gameid AND f.console = pr.console
    JOIN gold.dim_achievement a
        ON f.achievementid = a.achievementid AND f.console = a.console
    JOIN gold.dim_games g
        ON f.gameid = g.gameid AND f.console = g.console
    """)
