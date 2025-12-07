from pyspark import pipelines as dp
from pyspark.sql import functions as sf
from pyspark.sql import types as st

@dp.table
@dp.expect_or_drop("titles_tconst_nn", "tconst is not null")
@dp.expect_or_drop("title_parsed_correctly", "is_adult in (0, 1)")
def pl2_silver_titles_ratings():
    df = spark.sql(
    """select 
        titles.tconst as tconst,
        titles.titletype as title_type,
        titles.primarytitle as primary_title,
        titles.originaltitle as original_title,
        titles.isadult as is_adult,
        titles.startyear as start_year,
        titles.endyear as end_year,
        cast(titles.runtimeminutes as int) as runtime_minutes,
        isnotnull(episodes.parenttconst) as is_episode,
        episodes.parenttconst as parent_tconst,
        episodes.seasonnumber as season_number,
        episodes.episodenumber as episode_number,
        ratings.averageRating as rating,
        ratings.numvotes as num_votes,
        sort_array(split(titles.genres, ',')) as genres,
        titles.load_dt as load_dt,
        titles.source_file_path as source_file_path,
        titles.source_file_version as source_file_version
    from workspace.imdb.pl1_bronze_title_basics titles
    left join workspace.imdb.pl1_bronze_title_ratings ratings
        on titles.tconst = ratings.tconst
    left join workspace.imdb.pl1_bronze_title_episode episodes
        on episodes.tconst = titles.tconst
    """
    )
    return df

@dp.table
@dp.expect_or_drop("principals_tconst_nn", "tconst is not null")
@dp.expect_or_drop("principals_nconst_nn", "nconst is not null")
def pl2_silver_principals():
    # data from principals
    df = spark.read.table("workspace.imdb.pl1_bronze_title_principals")
    df = df.withColumn("characters", sf.explode_outer(
        sf.from_json("characters", st.ArrayType(st.StringType()))
    ))
    df = df \
        .drop("ordering")
    
    # writer data from crew
    df2 = spark.read.table("workspace.imdb.pl1_bronze_title_crew")
    df2_writers = df2.drop("directors").withColumn(
        "nconst", sf.explode(
            sf.split(sf.col("writers"), ",")
        )
    ) \
        .drop("writers") \
        .withColumn("category", sf.lit("writer"))

    # director data from crew
    df2_directors = df2.drop("writers").withColumn(
        "nconst", sf.explode(
            sf.split(sf.col("directors"), ",")
        )
    ) \
        .drop("directors") \
        .withColumn("category", sf.lit("director"))
    return df.unionByName(df2_writers, allowMissingColumns=True).unionByName(df2_directors, allowMissingColumns=True)

@dp.table
@dp.expect_or_drop("regional_release_tconst_nn", "titleid is not null")
@dp.expect_or_drop("has_region_or_lang", "region_code is not null or language_code is not null")
def pl2_silver_regional_release():
    df = spark.read.table("workspace.imdb.pl1_bronze_title_akas") \
        .withColumn("tconst", sf.col("titleid")) \
        .withColumn("region_code", sf.lower("region")) \
        .withColumn("language_code", sf.lower("language")) \
        .drop("titleid", "ordering", "types", "attributes", "isoriginaltitle", "region", "language")
    return df

@dp.table
@dp.expect_or_drop("name_nconst_nn", "nconst is not null")
def pl2_silver_names():
    df = spark.read.table("workspace.imdb.pl1_bronze_name_basics") \
        .drop("primaryprofession") \
        .drop("knownfortitles") \
        .withColumn("birthyear", sf.col("birthyear").cast(st.IntegerType())) \
        .withColumn("deathyear", sf.col("deathyear").cast(st.IntegerType()))
    return df

@dp.table
@dp.expect_or_drop("region_code_nn", "region_code is not null")
def pl2_silver_country_codes():
    df = spark.read.table("workspace.imdb.pl1_bronze_country_codes") \
        .withColumn("region_code", sf.lower("code")) \
        .drop("code") \
        .fillna("Unknown", subset = ["country"])
    return df

@dp.table
@dp.expect_or_drop("language_code_nn", "language_code is not null")
def pl2_silver_language_codes():
    df = spark.read.table("workspace.imdb.pl1_bronze_language_codes") \
        .withColumn("language_code", sf.lower("set_1")) \
        .drop("set_1", "set_2_a", "set_2_b", "set_2_t","set_3", "other_names_[note_1]", "notes")
    return df