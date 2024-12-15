from chispa.dataframe_comparer import *

from ..jobs.HW4_datelist_int import generate_datelist_int
from collections import namedtuple

user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type device_activity_datelist date")
datelist_int = namedtuple("datelist_int", "user_id browser_type datelist_int")

def test_datelist_int_generation(spark):
    input_data = [
        user_devices_cumulated('70132547320211180', 'Other', ["2023-01-01"], '2023-01-01'),
    ]

    input_dataframe = spark.createDataFrame(input_data)

    actual_df = generate_datelist_int(spark, input_dataframe)

    expected_output = [
        datelist_int('70132547320211180', 'Other', '00000000000000000000000000000001')
    ]

    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df)

