# Bronze Layer

spark.sql("CREATE SCHEMA IF NOT EXISTS pyspark_pipeline")

df = spark.read.csv(
    "/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/data/orders.csv",
    header=True,
    inferSchema=True
)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("pyspark_pipeline.bronze_orders")