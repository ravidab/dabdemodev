# Databricks notebook source
my_catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('display_target')

set_default_catalog = spark.sql(f'USE CATALOG {my_catalog}')

print(f'Using the {my_catalog} catalog.')
print(f'Deploying as the {target} pipeline.')

# COMMAND ----------

# Create or replace the silver table from bronze table
spark.sql(f"""
CREATE OR REPLACE TABLE {my_catalog}.default.silver_health_1 AS
SELECT * 
FROM {my_catalog}.default.Bronze_health_1
""")

# COMMAND ----------

print ('this is silver table')

print ('this is from git')
