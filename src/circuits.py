# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from pyspark.sql  import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType
from pyspark.sql.functions import upper, col,round,trim,md5,concat_ws,lit,current_timestamp

#creating spark session
spark = SparkSession.builder.appName('f1_dwh').getOrCreate()

#creating mount  point 
s3_mnt()

# COMMAND ----------

circuits_schema  = StructType([
    StructField("circuitId",IntegerType(),False), \
    StructField("circuitRef",StringType(),True), \
    StructField("name",StringType(),True), \
    StructField("location", StringType(), True), \
    StructField("country", StringType(), True), \
    StructField("lat", FloatType(), True), \
    StructField("lng", FloatType(), True), \
    StructField("alt", IntegerType(), True), \
    StructField("url", StringType(), True) \
                        ])
circuits_df = spark.read.option('header',True).schema(circuits_schema).csv('dbfs:/mnt/f1-s3-mnt/raw_files/circuits.csv')
#convert columns name to  upper case
#apply some data cleanisng for stringType like upper() ,trim 
for columns in circuits_df.columns:
    if circuits_df.schema[columns].dataType == StringType()and columns != 'url' :
        circuits_df = circuits_df.withColumn(columns, trim(upper(col(columns))))
    elif circuits_df.schema[columns].dataType == FloatType():
        circuits_df = circuits_df.withColumn(columns, round(col(columns),4))
    circuits_df = circuits_df.withColumnRenamed(columns, columns.upper())

#renaming the location columns location  to city
circuits_df = circuits_df.withColumnRenamed('LOCATION', 'CITY')
#creating data vault columns  in dataframe like load_date,CIRCUITID_HK,CIRCUITS_HASDIFF
circuits_df = circuits_df.withColumn('CIRCUITID_HK',upper(md5(col('CIRCUITID').cast(StringType()))))
#calculating has diff
circuits_df = circuits_df.withColumn('CIRCUITS_HASDIFF',upper(md5(concat_ws('',*circuits_df.columns[1:])).cast(StringType())))
#adding record_source & Load date
circuits_df = circuits_df.withColumn('RECORD_SOURCE',lit('CIRCUITS.CSV')).withColumn('LOAD_DATE',current_timestamp())
#verifying the changes
circuits_df.show()
#adding processed file in STAGE_TABLE/
circuits_df.write.parquet('/mnt/f1-s3-mnt/processed_file/STAGE_TABLE/CIRCUITS.parquet',mode = 'overwrite')
#adding HUB_circuits in RDV_TABLE/
circuits_df.select(col('CIRCUITID_HK'),col('CIRCUITID'),col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CIRCUITS.parquet')
#adding HUB_circuits in RDV_TABLE/
circuits_df.select(col('CIRCUITS_HASDIFF'),col('CIRCUITREF'),\
                   col('NAME'),col('CITY'),col('COUNTRY'),col('LAT'),col('LNG'),col('ALT'),\
                   col('URL'),col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_CIRCUITS.parquet')

