from pyspark.sql import SparkSession
from pyspark.sql import functions as F

query = """
WITH 
users_devices as (
    SELECT 
        *
    from user_devices_cumulated
    WHERE date = DATE('2023-01-01')
),
series AS (
    SELECT '2023-01-01' AS series_date UNION ALL
    SELECT '2023-01-02' UNION ALL
    SELECT '2023-01-03' UNION ALL
    SELECT '2023-01-04' UNION ALL
    SELECT '2023-01-05' UNION ALL
    SELECT '2023-01-06' UNION ALL
    SELECT '2023-01-07' UNION ALL
    SELECT '2023-01-08' UNION ALL
    SELECT '2023-01-09' UNION ALL
    SELECT '2023-01-10' UNION ALL
    SELECT '2023-01-11' UNION ALL
    SELECT '2023-01-12' UNION ALL
    SELECT '2023-01-13' UNION ALL
    SELECT '2023-01-14' UNION ALL
    SELECT '2023-01-15' UNION ALL
    SELECT '2023-01-16' UNION ALL
    SELECT '2023-01-17' UNION ALL
    SELECT '2023-01-18' UNION ALL
    SELECT '2023-01-19' UNION ALL
    SELECT '2023-01-20' UNION ALL
    SELECT '2023-01-21' UNION ALL
    SELECT '2023-01-22' UNION ALL
    SELECT '2023-01-23' UNION ALL
    SELECT '2023-01-24' UNION ALL
    SELECT '2023-01-25' UNION ALL
    SELECT '2023-01-26' UNION ALL
    SELECT '2023-01-27' UNION ALL
    SELECT '2023-01-28' UNION ALL
    SELECT '2023-01-29' UNION ALL
    SELECT '2023-01-30' UNION ALL
    SELECT '2023-01-31'
),
place_holder_int AS (
    SELECT 
        CASE
            WHEN ARRAY_CONTAINS(device_activity_datelist, series_date)
                THEN POW(2, 31 - DATEDIFF(date , series_date))
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users_devices
    CROSS JOIN series
)
SELECT 
    user_id,
    browser_type,
    LPAD(REVERSE(BIN(CAST(SUM(placeholder_int_value) AS BIGINT))), 32, '0') AS datelist_int
FROM place_holder_int
GROUP BY user_id, browser_type
"""

def generate_datelist_int(spark, dataframe):
    dataframe.createOrReplaceTempView("user_devices_cumulated")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master('local') \
        .appName('HW4') \
        .getOrCreate()
    output_df = generate_datelist_int(spark, spark.table("players"))
    output_df.write.mode('overwrite').insertInto('players_scd')