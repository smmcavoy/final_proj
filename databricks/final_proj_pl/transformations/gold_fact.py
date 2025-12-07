from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types as st

dp.create_streaming_table(
    name = "fact_genre",
    schema = """
        genre_surr_key bigint generated always as identity,
        title_id bigint,
        tconst string,
        genre string,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dp.table
def pl3_gold_genre():
    df = spark.sql(
        """
        select
            t.title_id as title_id,
            t.tconst as tconst,
            explode(g.genres) as genre,
            t.load_dt as load_dt,
            t.source_file_path as source_file_path,
            t.source_file_version as source_file_version
        from
            workspace.imdb.pl2_silver_titles_ratings g
            join workspace.imdb.dim_title t
            on t.tconst = g.tconst
        """
    )
    return df
dp.create_auto_cdc_flow(
    source="pl3_gold_genre",
    target="fact_genre",
    keys = ['tconst', 'genre'],
    sequence_by = 'load_dt',
    ignore_null_updates = True
)

dp.create_streaming_table(
    name = "fact_regional_release",
    schema = """
        regional_release_surr_key bigint generated always as identity,
        title_id bigint,
        tconst string,
        region_id string,
        language_id string,
        title string,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dp.table
def pl3_gold_regional_release():
    df = spark.sql(
        """
        select
            t.title_id as title_id,
            r.tconst as tconst,
            ifnull(r.region_code, 'unk') as region_id,
            ifnull(r.language_code, 'unk') as language_id,
            r.title as title,
            r.load_dt as load_dt,
            r.source_file_path as source_file_path,
            r.source_file_version as source_file_version
        from
            workspace.imdb.pl2_silver_regional_release r
            join workspace.imdb.dim_title t
            on t.tconst = r.tconst
        """
    )
    return df

dp.create_auto_cdc_flow(
    source="pl3_gold_regional_release",
    target="fact_regional_release",
    keys = ['tconst', 'region_id', 'language_id'],
    sequence_by = 'load_dt',
    ignore_null_updates = True
)

dp.create_streaming_table(
    name = "fact_work",
    schema = """
        work_surr_key bigint generated always as identity,
        title_id bigint,
        tconst string,
        person_id bigint,
        nconst string,
        category string,
        job string,
        character string,
        load_dt timestamp,
        source_file_path string,
        source_file_version timestamp
    """,
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dp.table
def pl3_gold_work():
    df = spark.sql(
    """
    select
        t.title_id as title_id,
        w.tconst as tconst,
        p.person_id as person_id,
        w.nconst as nconst,
        w.category as category,
        w.job as job,
        w.characters as `character`,
        w.load_dt as load_dt,
        w.source_file_path as source_file_path,
        w.source_file_version as source_file_version
    from
        workspace.imdb.pl2_silver_principals w
        join workspace.imdb.dim_title t
        on t.tconst = w.tconst
        join workspace.imdb.dim_person p
        on p.nconst = w.nconst
    """
    )
    return df

dp.create_auto_cdc_flow(
    source="pl3_gold_work",
    target="fact_work",
    keys = ['tconst', 'nconst', 'category', 'job', 'character'],
    sequence_by = 'load_dt',
    ignore_null_updates = True
)
