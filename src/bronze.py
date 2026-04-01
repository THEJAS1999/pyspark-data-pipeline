# BRONZE LAYER - Raw Data Landing
# Load raw CSV data without transformations

spark.sql("CREATE SCHEMA IF NOT EXISTS pyspark_pipeline")

# Read CSV with automatic schema inference
df = spark.read.csv(
    "/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/data/orders.csv",
    header=True,
    inferSchema=True
)

# Store in Delta for ACID compliance and versioning
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("pyspark_pipeline.bronze_orders")