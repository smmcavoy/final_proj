from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types as st
from datetime import datetime

dp.create_streaming_table(
    name = 'dim_region',
    schema = """
        region_code string,
        country string,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)
dp.create_streaming_table(
    name = 'dim_language',
    schema = """
        language_code string,
        iso_language_names string,
        endonyms string,
        scope string,
        type string,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)
dp.create_streaming_table(
    name = "dim_person",
    schema = """
        person_id bigint generated always as identity,
        nconst string,
        primaryname string,
        birthyear bigint,
        deathyear bigint,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)
dp.create_streaming_table(
    name = "dim_title",
    schema = """
        title_id bigint generated always as identity,
        tconst string,
        title_type string,
        primary_title string,
        original_title string,
        is_adult boolean,
        start_year int,
        end_year int,
        runtime_minutes int,
        is_episode boolean,
        parent_tconst string,
        season_number int,
        episode_number int,
        rating double,
        num_votes int,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)

@dp.table
def pl3_gold_region():
    df = spark.read.table("workspace.imdb.pl2_silver_country_codes")
    dummy_df = spark.createDataFrame(
        [
            ('N/A', datetime.now(), 'SYSTEM', datetime.now(), 'unk')
        ],
        df.columns
    )
    return df.union(dummy_df)
dp.create_auto_cdc_flow(
    target = 'dim_region',
    source = 'pl3_gold_region',
    keys = ['region_code'],
    sequence_by = 'load_dt',
    ignore_null_updates = True
)

@dp.table
def pl3_gold_language():
    df = spark.read.table("workspace.imdb.pl2_silver_language_codes")
    dummy_df = spark.createDataFrame(
        [
            ('N/A', 'N/A', 'N/A', 'N/A', datetime.now(), 'SYSTEM', datetime.now(), 'unk')
        ],
        df.columns
    )
    return df.union(dummy_df)
dp.create_auto_cdc_flow(
    target = 'dim_language',
    source = 'pl3_gold_language',
    keys = ['language_code'],
    sequence_by = 'load_dt',
    ignore_null_updates = True
)

@dp.table
def pl3_gold_person():
    df = spark.read.table("workspace.imdb.pl2_silver_names")
    dummy_df = spark.createDataFrame(
        [
            ('N/A', 'N/A', 0, 0, datetime.now(), 'SYSTEM', datetime.now())
        ],
        df.columns
    )
    return df.union(dummy_df)
dp.create_auto_cdc_flow(
    target = 'dim_person',
    source = 'pl3_gold_person',
    keys = ['nconst'],
    sequence_by='load_dt',
    ignore_null_updates = True
)

@dp.table
def pl3_gold_title():
    df = spark.read.table("workspace.imdb.pl2_silver_titles_ratings").selectExpr(
        "tconst",
        "title_type",
        "primary_title",
        "original_title",
        "cast(is_adult as boolean)",
        "start_year",
        "end_year",
        "runtime_minutes",
        "is_episode",
        "parent_tconst",
        "season_number",
        "episode_number",
        "rating",
        "num_votes",
        "load_dt",
        "source_file_path",
        "source_file_version"
    )
    return df
dp.create_auto_cdc_flow(
    target = 'dim_title',
    source = 'pl3_gold_title',
    keys = ['tconst'],
    sequence_by='load_dt',
    ignore_null_updates = True
)
