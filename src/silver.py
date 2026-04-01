# Silver Layer - Transform data

from pyspark.sql.functions import col

bronze_df = spark.table("pyspark_pipeline.bronze_orders")

silver_df = bronze_df.withColumn(
    "total_sales",
    col("price") * col("quantity")
)

silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("pyspark_pipeline.silver_orders")