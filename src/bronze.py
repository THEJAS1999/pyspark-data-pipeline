# Bronze Layer - Load raw data

df = spark.read.csv("/FileStore/orders.csv", header=True, inferSchema=True)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")