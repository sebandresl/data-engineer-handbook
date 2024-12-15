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