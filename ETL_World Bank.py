# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ETL World Bank

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. Different languages: `%LANGUAGE` syntax: Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ETL stands for Extract, Transform, Load.
# MAGIC 
# MAGIC World Bank Indicator Data - This data contains socio-economic indicators for countries around the world. A few example indicators include population, arable land, and central government debt.
# MAGIC 
# MAGIC World Bank Project Data - This data set contains information about World Bank project lending since 1947.
# MAGIC 
# MAGIC Outline of this notebook:
# MAGIC 
# MAGIC Extract data from different sources such as:
# MAGIC csv files
# MAGIC json files
# MAGIC APIs
# MAGIC 
# MAGIC Transform data:
# MAGIC combining data from different sources
# MAGIC data cleaning
# MAGIC data types
# MAGIC parsing dates
# MAGIC file encodings
# MAGIC missing data
# MAGIC duplicate data
# MAGIC dummy variables
# MAGIC remove outliers
# MAGIC scaling features
# MAGIC engineering features
# MAGIC 
# MAGIC Load:
# MAGIC Send the transformed data to a database
# MAGIC 
# MAGIC ETL Pipeline
# MAGIC Code an ETL pipeline

# COMMAND ----------

# MAGIC %python
# MAGIC spark.catalog.listFunctions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Extraction

# COMMAND ----------

# MAGIC %python
# MAGIC # File location and type
# MAGIC file_location = "/FileStore/tables/population_data.csv"
# MAGIC file_type = "csv"
# MAGIC 
# MAGIC # CSV options
# MAGIC infer_schema = "false"
# MAGIC first_row_is_header = "true"
# MAGIC skiprows = 4
# MAGIC delimiter = ","
# MAGIC 
# MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC pop_data = spark.read.format(file_type) \
# MAGIC   .option("inferSchema", infer_schema) \
# MAGIC   .option("header", first_row_is_header) \
# MAGIC   .option("sep", delimiter) \
# MAGIC   .option("skiprows", skiprows) \
# MAGIC   .load(file_location)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC display(pop_data)

# COMMAND ----------

# MAGIC %scala
# MAGIC // File location and type
# MAGIC val file_location = "/FileStore/tables/projects_data.csv"
# MAGIC val file_type = "csv"
# MAGIC 
# MAGIC // CSV options
# MAGIC val infer_schema = "false"
# MAGIC val first_row_is_header = "true"
# MAGIC val delimiter = ","
# MAGIC 
# MAGIC // The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC val proj_data_scala = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC display(proj_data_scala)

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.io.Source
# MAGIC 
# MAGIC // File location and type
# MAGIC val file_location = "/FileStore/tables/mystery.csv"
# MAGIC val file_type = "csv"
# MAGIC 
# MAGIC // CSV options
# MAGIC val delimiter = ","
# MAGIC 
# MAGIC // The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC var gdp_data_scala = Source.fromFile("/FileStore/tables/mystery.csv")
# MAGIC 
# MAGIC println(gdp_data_scala)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- File location and type
# MAGIC SET file_location = "/FileStore/tables/rural_population_percent.csv";
# MAGIC SET file_type = "csv";
# MAGIC 
# MAGIC -- -- CSV options
# MAGIC SET infer_schema = "false";
# MAGIC SET header = "true";
# MAGIC SET delimiter = ",";
# MAGIC SET skiprows = 4;
# MAGIC 
# MAGIC -- The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC DROP TABLE IF EXISTS rural_popPercent_sql;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS rural_popPercent_sql
# MAGIC USING CSV
# MAGIC LOCATION "/FileStore/tables/rural_population_percent.csv"
# MAGIC OPTIONS (skiprows 4, infer_schema "false", header "true", delimiter ",");
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM rural_popPercent_sql;
# MAGIC -- display(rural_pop_sql)

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC # File location and type
# MAGIC file_location = "/FileStore/tables/gdp_data.csv"
# MAGIC file_type = "csv"
# MAGIC 
# MAGIC # CSV options
# MAGIC infer_schema = "false"
# MAGIC header = "true"
# MAGIC delimiter = ","
# MAGIC 
# MAGIC 
# MAGIC # The applied options are for CSV files. For other file types, these will be ignore
# MAGIC gdp_data_r <- read.df(file_location, source = file_type, header = header, inferSchema = infer_schema)
# MAGIC 
# MAGIC head(gdp_data_r)

# COMMAND ----------

# MAGIC %python
# MAGIC pop_data.head(5)

# COMMAND ----------

# MAGIC %r
# MAGIC head(pop_data)

# COMMAND ----------

pop_data.columns

# COMMAND ----------

rural_ppop_dataop_data.dtypes

# COMMAND ----------

# MAGIC %python
# MAGIC # Create a view or table
# MAGIC 
# MAGIC temp_table_name = "mystery_csv"
# MAGIC 
# MAGIC rural_pop_data.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC SELECT * FROM mystery_csv
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %python
# MAGIC # This table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# MAGIC 
# MAGIC 
# MAGIC permanent_table_name = "mystery_csv"
# MAGIC df.write.format("parquet").saveAsTable(permanent_table_name)
# MAGIC 
# MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transformation

# COMMAND ----------


