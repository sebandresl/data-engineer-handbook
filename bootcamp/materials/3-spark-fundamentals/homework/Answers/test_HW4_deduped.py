from chispa.dataframe_comparer import *

from ..jobs.HW4_deduped_job import dedupe_game_details
from collections import namedtuple

deduped_game_details = namedtuple("deduped", "game_id team_id player_id")
duped_game_details = namedtuple("game_details", "game_id team_id player_id")

def test_dedupe_game_details(spark):
    input_data = [
        duped_game_details(11, 22, 33),
        duped_game_details(11, 22, 33)
    ]

    input_dataframe = spark.createDataFrame(input_data)

    actual_df = dedupe_game_details(spark, input_dataframe)

    expected_output = [
        deduped_game_details(11, 22, 33)
    ]

    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df)

