from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Q1 | Create session with disabled AutoBroadcast
spark = SparkSession.builder \
                    .appName("HW3") \
                    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
                    .getOrCreate()