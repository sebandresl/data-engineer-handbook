from pyspark.sql import SparkSession

query = """
WITH 
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY gd.game_id) as row_num
    FROM game_details gd
)
SELECT
    game_id,
    team_id,
    player_id
FROM deduped
WHERE row_num = 1
"""

def dedupe_game_details(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master('local') \
        .appName('HW4') \
        .getOrCreate()
    output_df = dedupe_game_details(spark, spark.table("players"))
    output_df.write.mode('overwrite').insertInto('players_scd')