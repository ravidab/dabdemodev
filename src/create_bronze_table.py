# Databricks notebook source
my_catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('display_target')

set_default_catalog = spark.sql(f'USE CATALOG {my_catalog}')

print(f'Using the {my_catalog} catalog.')
print(f'Deploying as the {target} pipeline.')

# COMMAND ----------

spark.sql(f'''
CREATE OR REPLACE TABLE {my_catalog}.default.Bronze_health_1 AS
SELECT 
  *
FROM read_files(
  '/Volumes/dabdemo-dev/default/health',
  format => 'csv',
  header => true
)
''')

# COMMAND ----------

print ('this is bronze table')

#this is from git
