#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Iceberg Schema Update Script for example_entity_name
==============================================

This script updates the schema for both bronze and silver tables 
by adding new columns identified during schema comparison.

Database: example_db
Entity: example_entity_name
Tables:
- example_db.bronze_example_entity_name
- example_db.silver_example_entity_name

New columns:
- Phone (STRING)
- Department (STRING)
- IsActive (BOOLEAN)

Generated on: 2025-08-21 10:53:49
'''

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Iceberg Schema Update - example_entity_name") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

print("Starting Iceberg schema update for example_entity_name...")

# First, truncate tables to clear existing data
print("Truncating tables...")
spark.sql("TRUNCATE TABLE example_db.bronze_example_entity_name")
spark.sql("TRUNCATE TABLE example_db.silver_example_entity_name")

# Bronze table DDLs
print("Adding new columns to bronze table...")
spark.sql("""
ALTER TABLE example_db.bronze_example_entity_name
ADD COLUMNS (
    Phone STRING,
    Department STRING,
    IsActive BOOLEAN
)
""")

# Position each new column in bronze table
print("Positioning new columns in bronze table...")
spark.sql("""
ALTER TABLE example_db.bronze_example_entity_name ALTER COLUMN Phone AFTER LastModifiedDate
""")
spark.sql("""
ALTER TABLE example_db.bronze_example_entity_name ALTER COLUMN Department AFTER Phone
""")
spark.sql("""
ALTER TABLE example_db.bronze_example_entity_name ALTER COLUMN IsActive AFTER Department
""")

# Silver table DDLs
print("Adding new columns to silver table...")
spark.sql("""
ALTER TABLE example_db.silver_example_entity_name
ADD COLUMNS (
    Phone STRING,
    Department STRING,
    IsActive BOOLEAN
)
""")

# Position each new column in silver table
print("Positioning new columns in silver table...")
spark.sql("""
ALTER TABLE example_db.silver_example_entity_name ALTER COLUMN Phone AFTER LastModifiedDate
""")
spark.sql("""
ALTER TABLE example_db.silver_example_entity_name ALTER COLUMN Department AFTER Phone
""")
spark.sql("""
ALTER TABLE example_db.silver_example_entity_name ALTER COLUMN IsActive AFTER Department
""")

print("Schema update completed successfully!")
