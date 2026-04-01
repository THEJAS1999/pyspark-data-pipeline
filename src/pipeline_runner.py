# Pipeline Runner

# Bronze
exec(open("/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/src/bronze.py").read())

# Silver
exec(open("/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/src/silver.py").read())

# Data Quality Checks
exec(open("/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/src/data_quality.py").read())

# Gold
exec(open("/Workspace/Users/thejasthejas30@gmail.com/pyspark-data-pipeline/src/gold.py").read())

