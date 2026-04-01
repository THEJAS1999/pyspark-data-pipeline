# PySpark Data Pipeline using Databricks

## Project Overview

This project implements an ETL data pipeline using PySpark and Databricks following the Medallion Architecture (Bronze, Silver, Gold).
The pipeline reads raw CSV data, performs transformations and data quality checks, and creates aggregated analytics tables stored in Delta Lake.

## Pipeline Architecture

Raw Data → Bronze Layer → Silver Layer → Data Quality Checks → Gold Layer → Analytics Tables

## Technologies Used

* PySpark
* Databricks
* Delta Lake
* Data Engineering Pipeline
* GitHub

## Pipeline Layers

### Bronze Layer

Loads raw CSV data into a Delta table without transformations.

### Silver Layer

Performs data cleaning, type conversion, filtering, and creates calculated columns such as total_sales.

### Data Quality Layer

Checks for null values, duplicate records, and validates data quality before loading into Gold layer.

### Gold Layer

Creates aggregated analytics tables such as total revenue and total orders by product.

## Tables Created

* pyspark_pipeline.bronze_orders
* pyspark_pipeline.silver_orders
* pyspark_pipeline.gold_product_sales

## Pipeline Execution

Run pipeline in this order:

1. bronze.py
2. silver.py
3. data_quality.py
4. gold.py

## Architecture Diagram

See architecture/architecture.png

## Author

Thejas P Y Data Engineering

GitHub: https://github.com/THEJAS1999 LinkedIn: https://linkedin.com/in/thejasyatheendran
