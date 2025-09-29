# Databricks notebook source
# MAGIC %md
# MAGIC # OpenWeather API Load 2 Bronze
# MAGIC
# MAGIC In this notebook, we load city informations and weather data for different cities from the Openweather API into a bronze table. </br></br>
# MAGIC
# MAGIC
# MAGIC An API key from Openweather is required to run this notebook. You can create a free account on https://openweathermap.org and generate the key there.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

# DBTITLE 1,Import libraries
import requests
import json
import uuid
import re
from datetime import datetime
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType, ArrayType, LongType, MapType

# COMMAND ----------

def transform_email(email):
    match = re.match(r'^([^@]+)@', email)
    if match:
        username = match.group(1)
        username = re.sub(r'[._]', '_', username)
        return username
    else:
        return None

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_id
user_catalog_name = transform_email(user_id)
print(f"user_catalog_name: {user_catalog_name}")

# COMMAND ----------

# DBTITLE 1,variables
api_key             = dbutils.secrets.get("keyvault", "API-Key-Stefan-Koch")
load_id             = str(uuid.uuid4())
utc_timestamp       = datetime.utcnow()
target_cities_table = f"{user_catalog_name}.elt.target_cities"
bronze_table        = f"{user_catalog_name}.bronze.current"

print(f"api_key: {api_key}")
print(f"load_id: {load_id}")
print(f"utc_timestamp: {utc_timestamp}")
print(f"target_cities_table: {target_cities_table}")
print(f"bronze_table: {bronze_table}")

# COMMAND ----------

# Define the schema for the response data
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ]), True),
    
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),
    
    StructField("base", StringType(), True),
    
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("sea_level", IntegerType(), True),
        StructField("grnd_level", IntegerType(), True)
    ]), True),
    
    StructField("visibility", IntegerType(), True),
    
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True)
    ]), True),
    
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),
    
    StructField("dt", LongType(), True),
    
    StructField("sys", StructType([
        StructField("type", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True)
    ]), True),
    
    StructField("timezone", IntegerType(), True),
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("cod", IntegerType(), True)
])

city_schema = ArrayType(StructType([
    StructField("name", StringType(), True),
    StructField("local_names", MapType(StringType(), StringType(), True), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
]))

# COMMAND ----------

# DBTITLE 1,define functions
def get_response(url):
    """
    Sends a GET request to the specified URL and returns the response.

    Args:
        url (str): The URL to send the GET request to.

    Returns:
        response: The response object from the GET request.

    Raises:
        Exception: If an error occurs during the GET request.
    """
    try:
        response = requests.get(url)
        return response
    except Exception as e:
        print(e)  
        raise
        
def create_dataframe(response, schema):
    """
    Creates a Spark DataFrame from the JSON response and adds LoadID and LoadTimeStamp columns.

    Args:
        response: The response object containing JSON data.

    Returns:
        DataFrame: A Spark DataFrame with the JSON data and additional columns.
    """
    data = response.json()
    json_data = data if isinstance(data, list) else [data]
    df = spark.createDataFrame(json_data, schema=schema)
    df = (
        df.withColumn("LoadID", lit(load_id))
          .withColumn("LoadTimeStamp", lit(utc_timestamp))
    )
    return df

# COMMAND ----------

df_target_cities = spark.read.table(target_cities_table)
target_cities_rows = df_target_cities.select("city").collect()
target_cities = [city["city"] for city in target_cities_rows]
target_cities

# COMMAND ----------

# DBTITLE 1,get target cities
df_cities = None
for city in target_cities:
    print(f"Load metadata for: {city}")
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}"
    response = get_response(url)
    df = create_dataframe(response, city_schema)
    if df_cities is None:
        df_cities = df
    else:
        df_cities = df_cities.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

display(df_cities)

# COMMAND ----------

# DBTITLE 1,add columns to the cities dataframe
df_cities = (df_cities          
                .withColumnRenamed("name", "City")
                .withColumnRenamed("lon", "Longitude")
                .withColumnRenamed("lat", "Latitude")
)
display(df_cities)



# COMMAND ----------

# DBTITLE 1,create temp view from dataframe
df_cities.createOrReplaceTempView("TempViewCities")

# COMMAND ----------

# Execute the following code only if the table is empty, if not, skip
if spark.sql(f"SELECT COUNT(*) FROM {user_catalog_name}.bronze.cities").first()[0] == 0:
    print("Write dataframe to table.")
    (
        df_cities.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{user_catalog_name}.bronze.cities")
    )
else: 
    print("There's already data in the table.")

# COMMAND ----------

spark.sql(f"SELECT City, Longitude, Latitude FROM {user_catalog_name}.bronze.cities").display()

# COMMAND ----------

# DBTITLE 1,create a list with city, lon and lat
# Create a list of cities from the bronze.weather.cities table, containing Longitude and Latitude
cities_list = spark.sql(f"SELECT City, Longitude, Latitude FROM {user_catalog_name}.bronze.cities").collect()
print(cities_list)

# COMMAND ----------

# DBTITLE 1,download current weather
df_current = None
for c in cities_list:
    print(f"Load current weather data for: {c.City}")
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={c.Latitude}&lon={c.Longitude}&appid={api_key}&units=metric"
    response = get_response(url)

    df = create_dataframe(response, schema=weather_schema)
    if df_current is None:
        df_current = df
    else:
        df_current = df_current.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

display(df_current)

# COMMAND ----------

# DBTITLE 1,save current weather data into bronze table
(
    df_current.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{user_catalog_name}.bronze.current")
)

# COMMAND ----------

spark.sql(f"SELECT * FROM {user_catalog_name}.bronze.current").display()

# COMMAND ----------

# DBTITLE 1,exit notebook
dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`

# COMMAND ----------


