# GOLD LAYER - Business Aggregations & Reporting
# Create aggregated analytics for BI tools and dashboards

from pyspark.sql.functions import sum, count

silver_df = spark.table("pyspark_pipeline.silver_orders")

# Aggregate by product for key metrics
gold_df = silver_df.groupBy("product_name").agg(
    sum("total_sales").alias("total_revenue"),  # Revenue per product
    count("order_id").alias("total_orders")  # Orders per product
)

# Store aggregated metrics for BI consumption
gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("pyspark_pipeline.gold_product_sales")