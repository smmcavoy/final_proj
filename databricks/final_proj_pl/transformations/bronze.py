from pyspark import pipelines as dp
from pyspark.sql import functions as sf
import re
from datetime import datetime

def read_tsv(filepath: str):
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("inferSchema", "true") \
        .load(filepath)
    for column in df.columns:
        df = df.withColumnRenamed(column, re.sub(r'[,;\{\}\(\)=]', '', re.sub(r'\s+', '_', column).lower()))
    df = df.withColumn("load_dt", sf.lit(datetime.now()))
    df = df.withColumn("source_file_path", sf.lit(filepath))
    df = df.withColumn("source_file_version", sf.lit(datetime.fromtimestamp(dbutils.fs.ls(filepath)[0].modificationTime / 1000)))

    return df

@dp.table()
def pl1_bronze_country_codes():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/country_codes.tsv")

@dp.table()
def pl1_bronze_language_codes():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/language_codes.tsv")

@dp.table()
def pl1_bronze_name_basics():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/name.basics.tsv")

@dp.table()
def pl1_bronze_title_akas():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/title.akas.tsv")

@dp.table()
def pl1_bronze_title_basics():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/title.basics.tsv")

@dp.table()
def pl1_bronze_title_crew():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/title.crew.tsv")

@dp.table()
def pl1_bronze_title_episode():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/title.episode.tsv")

@dp.table()
def pl1_bronze_title_principals():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/title.principals.tsv")

@dp.table()
def pl1_bronze_title_ratings():
    return read_tsv("/Volumes/workspace/imdb/imdb_data/data/title.ratings.tsv")