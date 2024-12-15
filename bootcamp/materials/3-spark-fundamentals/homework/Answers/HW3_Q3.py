from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Q1 | Create session with disabled AutoBroadcast
spark = SparkSession.builder \
                    .appName("HW3") \
                    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
                    .getOrCreate()

# Q2 | Broadcast join medals & maps

# Read medals into df
df_medals = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("../../data/medals.csv")

# Read maps into df
df_maps = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("../../data/maps.csv")

# Read match_details into df
df_match_details = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("../../data/match_details.csv")

# Read matches into df
df_matches = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("../../data/matches.csv")

# Read medals_matches_players into df
df_medals_matches_players = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("../../data/medals_matches_players.csv")

# Broadbast join df_medals_matches_players and df_matches
df_matches_with_map = df_medals_matches_players.join(broadcast(df_matches), "match_id", "inner")

# Broadbast join df_matches_with_map and df_maps
df_maps_medals_joined = df_matches_with_map.join(broadcast(df_maps), "mapid", "inner")

# Q3 | Bucket join match_details, matches, medals_matches_players on match_id
df_match_details.write \
        .mode("overwrite") \
        .bucketBy(16, "match_id") \
        # .saveAsTable("df_match_details_bucketed")

df_matches.write \
        .mode("overwrite") \
        .partitionBy("completion_date") \
        .bucketBy(16, "match_id") \
        # .saveAsTable("df_matches_bucketed")

df_medals_matches_players.select('match_id', 'medal_id', 'count') \
        .write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        # .saveAsTable("df_medals_matches_players_bucketed")

df_bucketjoin1 = df_match_details.alias('md').join(df_matches.alias('m'), on = "match_id", how = "inner")
df_bucketjoin2 = df_bucketjoin1.join(df_medals_matches_players.alias('mmp') \
                                        , on = (F.col("md.match_id") == F.col("mmp.match_id")) 
                                                & (F.col("md.player_gamertag") == F.col("mmp.player_gamertag")) \
                                        , how = "inner")

df_selected_join = df_bucketjoin2.select(
    F.col('m.match_id'),
    F.col('m.mapid'),
    F.col('m.playlist_id'),
    F.col('md.player_gamertag'),
    F.col('md.player_total_kills'),
    F.col('mmp.medal_id'),
    F.col('mmp.count')
)