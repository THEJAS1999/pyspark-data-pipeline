# SILVER LAYER - Data Transformation & Cleaning
# Apply business rules, validation, and type conversions

from pyspark.sql.functions import col, to_date

bronze_df = spark.table("pyspark_pipeline.bronze_orders")

# Transform and clean data
silver_df = bronze_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("total_sales", col("price") * col("quantity")) \
    .filter(col("price").isNotNull()) \
    .filter(col("quantity") > 0)  # Keep positive quantities only

# Store cleaned data ready for analytics
silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("pyspark_pipeline.silver_orders")