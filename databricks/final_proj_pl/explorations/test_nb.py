# Databricks notebook source
spark.sql("USE CATALOG `workspace`")
spark.sql("USE SCHEMA `imdb`")

# COMMAND ----------

for file in dbutils.fs.ls("/Volumes/workspace/imdb/imdb_data/data/"):
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("inferSchema", "true") \
        .option("nullValue", "\\N") \
        .load(file.path)
    # display(df)
    print(f'{file.name}: {df.count()}')

# COMMAND ----------

# MAGIC %sql
# MAGIC select ba.titletype, count(*)
# MAGIC from workspace.imdb.pl1_bronze_title_episode ep
# MAGIC left join workspace.imdb.pl1_bronze_title_basics ba
# MAGIC on ep.tconst = ba.tconst
# MAGIC group by ba.titletype

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct titletype
# MAGIC from workspace.imdb.pl1_bronze_title_basics

# COMMAND ----------

# MAGIC %sql
# MAGIC select ba.titletype, count(*)
# MAGIC from workspace.imdb.pl1_bronze_title_basics ba
# MAGIC where ba.endyear is not null
# MAGIC group by ba.titletype
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from workspace.imdb.pl1_bronze_title_basics ba
# MAGIC where ba.endyear is not null and ba.titletype = 'tvEpisode'

# COMMAND ----------

# MAGIC %sql
# MAGIC select category, count(*)
# MAGIC from workspace.imdb.pl1_bronze_title_principals
# MAGIC group by category

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct genres
# MAGIC from workspace.imdb.pl2_silver_titles_ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC select job, count(*) as cnt
# MAGIC from workspace.imdb.pl1_bronze_title_principals
# MAGIC group by job
# MAGIC order by cnt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select types, count(*) as cnt
# MAGIC from workspace.imdb.pl1_bronze_title_akas
# MAGIC group by types
# MAGIC order by cnt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select attributes, count(*) as cnt
# MAGIC from workspace.imdb.pl1_bronze_title_akas
# MAGIC group by attributes
# MAGIC order by cnt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select primaryname, array_size(split(primaryname, ' ')) as split_len--, count(*) as cnt
# MAGIC from workspace.imdb.pl1_bronze_name_basics
# MAGIC -- group by split_len
# MAGIC order by split_len desc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists workspace.imdb.dim_language;
# MAGIC drop table if exists workspace.imdb.dim_region;
# MAGIC drop table if exists workspace.imdb.dim_person;
