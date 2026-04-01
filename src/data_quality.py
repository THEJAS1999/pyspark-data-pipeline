# DATA QUALITY CHECKS - Validation & Monitoring
# Verify data integrity after transformations

df = spark.table("pyspark_pipeline.silver_orders")

# Check: Total record count
print("Total records:", df.count())

# Check: Null price values (should be filtered)
print("Null price records:")
df.filter("price IS NULL").show()

# Check: Duplicate order IDs
print("Duplicate order IDs:")
df.groupBy("order_id").count().filter("count > 1").show()