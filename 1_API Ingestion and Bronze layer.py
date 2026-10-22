# Databricks notebook source
# DBTITLE 1,Import the Required Library
import requests 
import json
from pyspark.sql.functions import *
from pyspark.sql.types import * # for schema and structures 


# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.create_api_project")

base_path = "/Volumes/workspace/default/create_api_project"


# COMMAND ----------

# DBTITLE 1,Calling cricket API
API_KEYS = "02b4dfcb-74f8-4553-a2d7-6d4a3e3435d2"
# https://api.cricapi.com/v1/currentMatches?apikey=02b4dfcb-74f8-4553-a2d7-6d4a3e3435d2&offset=0
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEYS}&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent=4)[:20])





# COMMAND ----------

# DBTITLE 1,SAVE RAW API Response in the volumes
raw_file_path = f"{base_path}/current_matches_raw.json"

with open(raw_file_path, "w") as file:
    json.dump(api_data, file)

print("RAW API data is saved at the :", raw_file_path)


# COMMAND ----------

# DBTITLE 1,CREATE Bronze Layer Dataframe or Table

bronze_data = [
    {
        "source_api": api_url,
        "raw_json": json.dumps(api_data),
        "ingestion_time": None
    }
]


bronze_schema = [
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
]

bronze_df = spark.createDataFrame(bronze_data, StructType(bronze_schema))\
    .withColumn("ingestion_time", current_timestamp())

display(bronze_df)

# Dataframe is in the RAM we need to store permanently

# COMMAND ----------

# DBTITLE 1,Save the Bronze Table
bronze_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

print("Bronze Table Created successfully ",)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from workspace.default.cricket_bronze_current_matches 

# COMMAND ----------

