# Bronze Layer

df = spark.read.csv(
    "/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/data/orders.csv",
    header=True,
    inferSchema=True
)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")