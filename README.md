# PySpark Data Pipeline using Databricks

## Project Overview

This project implements a data pipeline using PySpark and Databricks following Medallion Architecture (Bronze, Silver, Gold).

## Pipeline Architecture

Raw Data → Bronze Layer → Silver Layer → Gold Layer → Analytics

## Technologies Used

* PySpark
* Databricks
* Delta Lake
* Data Engineering Pipeline
* GitHub

## Pipeline Layers

**Bronze:** Load raw CSV data
**Silver:** Data transformations and enrichment
**Gold:** Aggregations and analytics tables

## Tables Created

* bronze_orders
* silver_orders
* gold_product_sales

## Pipeline Execution

Run pipeline in this order:

1. bronze.py
2. silver.py
3. gold.py
