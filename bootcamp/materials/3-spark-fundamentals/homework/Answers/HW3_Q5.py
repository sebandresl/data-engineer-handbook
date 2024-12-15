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

# Q4 | Aggregate joined data

# Player with highest kill average per game
df_player_highest_kill_avg = (
    df_selected_join
    .groupBy('player_gamertag')  # Group by game and player
    .agg(F.avg('player_total_kills').alias('kill_average'))  # Calculate kill average
    .withColumn(
        'rank', 
        F.row_number().over(
            Window.orderBy(F.desc('kill_average'))
        )  # Rank players by kill average within each game
    )
    .filter(F.col('rank') == 1)  # Keep only the top-ranked player
    .select('player_gamertag', 'kill_average')  # Select relevant columns
)

# Most played playlist
df_most_played_playlist = (
    df_selected_join
    .groupBy('playlist_id')
    .agg(F.countDistinct('match_id').alias('playlist_count'))
    .withColumn(
        'rank',
        F.row_number().over(
            Window.orderBy(F.desc('playlist_count'))
        )
    )
    .filter(F.col('rank') == 1)
    .select('playlist_id', 'playlist_count')
)

# Most played map
df_most_played_map = (
    df_selected_join
    .groupBy('mapid')
    .agg(F.countDistinct('match_id').alias('map_count'))
    .withColumn(
        'rank',
        F.row_number().over(
            Window.orderBy(F.desc('map_count'))
        )
    )
    .filter(F.col('rank') == 1)
    .select('mapid', 'map_count')
)

# Map with most killing spree medals
df_selected_killingspree = df_selected_join.filter(F.col('medal_id') == 2430242797)

df_joined_mapname = df_selected_join.join(df_maps, on = "mapid", how = "inner") \
                                        .select(df_selected_join['*'], df_maps['name'].alias('map_name'))
df_joined_medalname = df_joined_mapname.join(df_medals, on = 'medal_id', how = 'inner') \
                                        .select(df_joined_mapname['*'], df_medals['name'].alias('medal_name'))

df_filtered_ks = df_joined_medalname.filter(F.col('medal_name') == 'Killing Spree')

df_map_most_killingspree = (
    df_filtered_ks
    .groupBy('mapid')
    .agg(F.sum('count'). alias('killingspree_count'))
    .withColumn(
        'rank',
        F.row_number().over(
            Window.orderBy(F.desc('killingspree_count'))
        )
    )
    .filter(F.col('rank') == 1)
    .select('map_name', 'killingspree_count')
)


# Q5 | Try different .sortWithinPartitions
# Inspecting mapid
df_selected_mapid = df_selected_join.sortWithinPartitions(F.col('mapid'))
df_selected_mapid.cache()
print(f"Data size for playlists: {df_selected_mapid.rdd.mapPartitions(lambda it: [sum(len(str(row)) for row in it)]).sum()} bytes")


# Inspecting playlist_id
df_selected_playlist_id = df_selected_join.sortWithinPartitions(F.col('playlist_id'))
df_selected_playlist_id.cache()
print(f"Data size for playlists: {df_selected_playlist_id.rdd.mapPartitions(lambda it: [sum(len(str(row)) for row in it)]).sum()} bytes")


# Inspecting match_id
df_selected_match_id = df_selected_join.sortWithinPartitions(F.col('match_id'))
df_selected_match_id.cache()
print(f"Data size for playlists: {df_selected_match_id.rdd.mapPartitions(lambda it: [sum(len(str(row)) for row in it)]).sum()} bytes")

# Inspecting player_gamertag
df_selected_player_gamertag = df_selected_join.sortWithinPartitions(F.col('player_gamertag'))
df_selected_player_gamertag.cache()
print(f"Data size for playlists: {df_selected_player_gamertag.rdd.mapPartitions(lambda it: [sum(len(str(row)) for row in it)]).sum()} bytes")


# Inspecting medal_id
df_selected_medal_id = df_selected_join.sortWithinPartitions(F.col('medal_id'))
df_selected_medal_id.cache()
print(f"Data size for playlists: {df_selected_medal_id.rdd.mapPartitions(lambda it: [sum(len(str(row)) for row in it)]).sum()} bytes")
