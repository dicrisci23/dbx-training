import dlt
from pyspark.sql.functions import *
from utilities import utils


custom_catalog =    spark.conf.get("custom_catalog")
base_path =         f"/Volumes/{custom_catalog}/raw/files/CoinGecko"

markets_path =      f"{base_path}/markets"
coin_location =     f"{base_path}/coins"

@dlt.table(
    name = "dlt_coingecko_markets",
    comment = "This are CoinGecko Data",
    table_properties = {"quality": "bronze"}
)
def build_table():
    df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("mergeSchema", "true")
                .load(markets_path)
                .select(
                            "*"
                            , col('_metadata')
                            , current_timestamp().alias('load_timestamp')
                        )
            )

    df = (df       
                .withColumn("file_name", col("_metadata").getField("file_name"))
                .withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{8}T\d{6}Z)", 1))
                .withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMdd'T'HHmmss'Z'"))
        )

    return df

# get all coin folders and save as list
coin_folders = [f for f in dbutils.fs.ls(coin_location) if f.isDir()]
# get all coin files and save as list
coin_files = [c.name.rstrip('/') for c in coin_folders]

for coin in coin_files:
    @dlt.table(
    name = f"dlt_coingecko_{coin}",
    comment = "This are CoinGecko Data",
    table_properties = {"quality": "bronze"}
    )

    def build_table():
        df = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "parquet")
                    .option("mergeSchema", "true")
                    .load(f"{coin_location}/{coin}")
                    .select(
                                "*"
                                , col('_metadata')
                                , current_timestamp().alias('load_timestamp')
                            )
                )

        df = (df       
                .withColumn("file_name", col("_metadata").getField("file_name"))
                .withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{8}T\d{6}Z)", 1))
                .withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMdd'T'HHmmss'Z'"))
        )

        return df
