# Gold Layer - Aggregations

from pyspark.sql.functions import sum

silver_df = spark.table("silver_orders")

gold_df = silver_df.groupBy("product_name").agg(
    sum("total_sales").alias("revenue")
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_product_sales")