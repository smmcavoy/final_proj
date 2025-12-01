from pyspark import pipelines as dp

def read_bronze(filepath: str):
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("inferSchema", "true") \
        .load(filepath)
    for column in df.columns:
        df = df.withColumnRenamed(column, re.sub(r'\s+', '_', column).lower())
    return df


