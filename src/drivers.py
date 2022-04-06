# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from pyspark.sql  import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType,DateType
from pyspark.sql.functions import upper, col,round,trim,md5,concat_ws,lit,current_timestamp,to_date

#creating spark session
spark = SparkSession.builder.appName('f1_dwh').getOrCreate()

#creating mount  point 
s3_mnt()

# COMMAND ----------

driver_schema =  StructType([
    StructField("driverId",IntegerType(),False), \
    StructField("driverRef",StringType(),True), \
    StructField("number",IntegerType(),True), \
    StructField("code", StringType(), True), \
    StructField("forename", StringType(), True), \
    StructField("surname", StringType(), True), \
    StructField("dob", StringType(), True), \
    StructField("nationality", StringType(), True), \
    StructField("url", StringType(), True) \
                        ])
driver_df = spark.read.option('header',True).schema(driver_schema).csv('dbfs:/mnt/f1-s3-mnt/raw_files/drivers.csv')
# driver_df.show()

#cleansing 
for columns in driver_df.columns:
    if driver_df.schema[columns].dataType == StringType()and columns != 'url' :
        driver_df = driver_df.withColumn(columns, trim(upper(col(columns)))).replace("\\N","",subset= [columns])
    elif driver_df.schema[columns].dataType == IntegerType():
        driver_df = driver_df.fillna(-1,subset = [columns])
    driver_df = driver_df.withColumnRenamed(columns, 'd_'+columns.upper())

#changing the DOB columns to datetype
driver_df = driver_df.withColumn('d_DOB',to_date(col('d_DOB'),'yyyy-mm-dd'))
#creating data vault columns  in dataframe like load_date,CIRCUITID_HK,CIRCUITS_HASDIFF
driver_df = driver_df.withColumn('d_DRIVERID_HK',upper(md5(col('d_DRIVERID').cast(StringType()))))
#calculating has diff
driver_df = driver_df.withColumn('d_DRIVER_HASDIFF',upper(md5(concat_ws('',*driver_df.columns[1:])).cast(StringType())))
#adding record_source & Load date
driver_df = driver_df.withColumn('d_RECORD_SOURCE',upper(lit('driver.CSV'))).withColumn('d_LOAD_DATE',current_timestamp())
#verifying the changes
driver_df.printSchema()
driver_df.show(5)

# COMMAND ----------

# MAGIC %md LOADING DATA INTO HUB_driver & SAT_driver

# COMMAND ----------

from pyspark.sql.functions import broadcast

#adding HUB_drivers in RDV_TABLE/
#checking if there is already hub circuits ,if so then read data from exisitng hub & do left outer join to insert new records

#check if hub_circuits file exists
file_check = {'HUB_DRIVERS':False,'SAT_DRIVERS':False}
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_DRIVERS")
        file_check['HUB_DRIVERS'] = True
except Exception as e:
      file_check['HUB_DRIVERS'] = False
        
#check if hub_circuits file exists
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_DRIVERS")
        file_check['SAT_DRIVERS'] = True
except Exception as e:
        file_check['SAT_DRIVERS'] =  False
print(file_check)
#adding HUB_circuits in RDV_TABLE/
#checking if there is already hub circuits ,if so then read data from exisitng hub & do left outer join to insert new records
try:
    if file_check['HUB_DRIVERS']:
        hub_df = spark.read.parquet('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_DRIVERS')  
        temp_hub_df = hub_df.join(broadcast(driver_df.select(col('d_DRIVERID_HK'),col('d_DRIVERID') ,\
                                                           col('d_RECORD_SOURCE'),col('d_LOAD_DATE'))),\
                                                           driver_df.d_DRIVERID_HK == hub_df.DRIVERID_HK,'rightouter').\
                            filter(col('DRIVERID_HK').isNull()).\
                            select(col('d_DRIVERID_HK'),col('d_DRIVERID'),col('d_RECORD_SOURCE'),col('d_LOAD_DATE'))
        for columns in temp_hub_df.columns:
             temp_hub_df = temp_hub_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema 
        temp_hub_df.printSchema()
        temp_hub_df.show()
        #apending new df DATA to HUB_CIRCUITS
        temp_hub_df.write.mode('append').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_DRIVERS')
    else:
        print('May be file is missing or data being loaded freshly in HUB_driver')
        temp_df = driver_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        #write the file for if there is no hub file
        temp_df.select(col('DRIVERID_HK'),col('DRIVERID'),col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_DRIVERS')
except Exception as e:
    print(e)
# adding SAT_CIRCUITS in RDV_TABLE/
try:
    if file_check['SAT_DRIVERS']:
        sat_df = spark.read.parquet('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_DRIVERS')
        temp_sat_df =  sat_df.join(broadcast(driver_df.select( col('d_DRIVERREF'),col('d_NUMBER'),col('d_CODE'),col('d_FORENAME'), col('d_SURNAME'), \
                                                            col('d_DOB'), col('d_NATIONALITY'),col('d_URL'),col('d_DRIVERID_HK'), \
                                                            col('d_RECORD_SOURCE'), col('d_LOAD_DATE'),col('d_DRIVER_HASDIFF'))),sat_df.DRIVERID_HK==driver_df.d_DRIVERID_HK,'rightouter').\
                                                            filter(col('DRIVERID_HK').isNull()).\
                                                                select(col('d_RECORD_SOURCE'), col('d_LOAD_DATE'),col('d_DRIVERID_HK'),\
                                                                       col('d_DRIVER_HASDIFF'), col('d_DRIVERREF'), col('d_NUMBER'), col('d_CODE'),\
                                                                       col('d_FORENAME'), col('d_SURNAME'), col('d_DOB'), col('d_NATIONALITY'), col('d_URL'))
        for columns in temp_sat_df.columns:
            temp_sat_df = temp_sat_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema
        temp_sat_df.printSchema()
        temp_sat_df.show()
        #adding computed file to sat_circuits table
        temp_df.write.mode('append').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_DRIVERS')
    else:
        print('May be file is missing or data being loaded freshly')
        temp_df = driver_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        temp_df.write.mode('overwrite').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_DRIVERS')
except  Exception as e:
    print(e)
  


# COMMAND ----------

dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_DRIVERS")
