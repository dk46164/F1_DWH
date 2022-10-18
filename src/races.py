# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from pyspark.sql  import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType
from pyspark.sql.functions import upper, col,round,trim,md5,concat_ws,lit,current_timestamp,concat,to_timestamp

#creating spark session
spark = SparkSession.builder.appName('f1_dwh').getOrCreate()

#creating mount  point 
s3_mnt()

# COMMAND ----------

races_schema = StructType([
    StructField("raceId",IntegerType(),True), \
    StructField("year",IntegerType(),True), \
    StructField("circuitId",IntegerType(),False), \
    StructField("round", IntegerType(), True), \
    StructField("name", StringType(), True), \
    StructField("date", StringType(), True), \
    StructField("time", StringType(), True), \
    StructField("url", StringType(), True) \
                        ])
races_df = spark.read.option('header',True).schema(races_schema).csv('dbfs:/mnt/f1-s3-mnt/raw_files/races.csv')

for columns in races_df.columns:
    if races_df.schema[columns].dataType == StringType()and columns != 'url' :
        races_df = races_df.withColumn(columns, trim(upper(col(columns))))
    elif races_df.schema[columns].dataType == FloatType():
        races_df = races_df.withColumn(columns, round(col(columns),4))
    races_df = races_df.withColumnRenamed(columns, 'r_'+columns.upper())

#type converstion
races_df = races_df.withColumn('r_DATETIME',to_timestamp(concat(col('r_DATE'),lit(' '),col('r_TIME')),'yyyy-MM-dd HH:mm:ss')).drop(*['r_DATE','r_TIME'])
#creating has_diff columns
races_df = races_df.withColumn('r_RACES_HASDIFF',upper(md5(concat(col('r_YEAR').cast(StringType()),\
                                                                     col('r_ROUND').cast(StringType()),\
                                                                     col('r_NAME'),col('r_DATETIME').cast(StringType()),\
                                                                     col('r_URL')))))
races_df = races_df.withColumn('r_CIRCUITID_HK',upper(md5(col('r_CIRCUITID').cast(StringType()))))
races_df = races_df.withColumn('r_RACEID_HK',upper(md5(col('r_RACEID').cast(StringType()))))
#createing hash key for lnk tables
races_df = races_df.withColumn('r_RACEID_CIRCUITID_HK',upper(md5(concat(col('r_CIRCUITID').cast(StringType()),col('r_RACEID').cast(StringType())))))
#adding record_source & Load date
races_df = races_df.withColumn('r_RECORD_SOURCE',upper(lit('races.CSV'))).withColumn('r_LOAD_DATE',current_timestamp())
races_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import broadcast

#adding HUB_drivers in RDV_TABLE/
#checking if there is already hub circuits ,if so then read data from exisitng hub & do left outer join to insert new records

file_check = {'HUB_RACES':False,'SAT_LNK_RACES':False,'LNK_RACES':False,'HUB_CIRCUITS':False}
#check if hub_circuits file exists
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CIRCUITS")
        file_check['HUB_CIRCUITS'] = True
except Exception as e:
      file_check['HUB_CIRCUITS'] = False
#check if HUB_RACES file exists
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_RACES")
        file_check['HUB_RACES'] = True
except Exception as e:
      file_check['HUB_RACES'] = False        
#check if SAT_RACES file exists
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_LNK_RACES")
        file_check['SAT_LNK_RACES'] = True
except Exception as e:
        file_check['SAT_RACES'] =  False
#check if lnk_races file exists
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/LNK_RACES")
        file_check['LNK_RACES'] = True
except Exception as e:
        file_check['LNK_RACES'] =  False
print(file_check)

# COMMAND ----------

#data mapping for hub_circuits
try:
    if file_check['HUB_CIRCUITS'] :
        hub_df = spark.read.format("delta").load('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CIRCUITS')  
        temp_hub_df = hub_df.join(broadcast(races_df.select(col('r_CIRCUITID_HK'), 
                                                           col('r_CIRCUITID') ,
                                                           col('r_RECORD_SOURCE'),
                                                           col('r_LOAD_DATE'))),
                                                           races_df.r_CIRCUITID_HK == hub_df.CIRCUITID_HK,'rightouter').\
                                            filter(col('CIRCUITID_HK').isNull()).\
                                            select(col('r_CIRCUITID_HK'),col('r_CIRCUITID'),col('r_RECORD_SOURCE'),col('r_LOAD_DATE'))
        #adding computed file to sat_circuits table
        for columns in temp_hub_df.columns:
            temp_hub_df = temp_hub_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema 
        temp_hub_df.printSchema()
        temp_hub_df.show()
        #apending new df DATA to HUB_CIRCUITS
        temp_hub_df.write.mode('append').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CIRCUITS')
    else:
        print('May be file is missing or data being loaded freshly hub_circuits')
        temp_df = races_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        #write the file for if there is no hub file
        temp_df.select(col('CIRCUITID_HK'),col('CIRCUITID'),col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CIRCUITS')
except Exception as e:
    print(e)
    
#data mapping for hub_circuits
try:
    if file_check['HUB_RACES'] :
        hub_df = spark.read.format("delta").load('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_RACES')  
        temp_hub_df = hub_df.join(broadcast(races_df.select(col('r_RACEID_HK'), 
                                                           col('r_RACEID') ,
                                                           col('r_RECORD_SOURCE'),
                                                           col('r_LOAD_DATE'))),
                                                           races_df.r_RACEID_HK == hub_df.RACEID_HK,'rightouter').\
                                            filter(col('RACEID_HK').isNull()).\
                                            select(col('r_RACEID_HK'),col('r_RACEID'),col('r_RECORD_SOURCE'),col('r_LOAD_DATE'))
        #adding computed file to sat_circuits table
        for columns in temp_hub_df.columns:
            temp_hub_df = temp_hub_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema 
        temp_hub_df.printSchema()
        temp_hub_df.show()
        #apending new df DATA to HUB_CIRCUITS
        temp_hub_df.write.mode('append').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_RACES')
    else:
        print('May be file is missing or data being loaded freshly hub_RACES')
        temp_df = races_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        #write the file for if there is no hub file
        temp_df.select(col('RACEID_HK'),col('RACEID'),col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_RACES')
except Exception as e:
    print(e)

# COMMAND ----------

# adding LNK_RACES in RDV_TABLE/
try:
    if file_check['LNK_RACES'] :
        lnk_df = spark.read.format("delta").load('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/LNK_RACES')  
        temp_lnk_df = lnk_df.join(broadcast(races_df.select(col('r_RACEID_HK'), 
                                                           col('r_CIRCUITID_HK') ,
                                                            col('r_RACEID_CIRCUITID_HK'),
                                                           col('r_RECORD_SOURCE'),
                                                           col('r_LOAD_DATE'))),
                                                           races_df.r_RACEID_CIRCUITID_HK == lnk_df.RACEID_CIRCUITID_HK,'rightouter').\
                                            filter(col('RACEID_HK').isNull()).\
                                            select(col('r_RACEID_HK'),col('r_CIRCUITID_HK'),col('r_RACEID_CIRCUITID_HK'),\
                                                           col('r_RECORD_SOURCE'),col('r_LOAD_DATE'))
        #adding computed file to sat_circuits table
        for columns in temp_lnk_df.columns:
            temp_lnk_df = temp_lnk_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema 
        temp_lnk_df.printSchema()
        temp_lnk_df.show()
        #apending new df DATA to HUB_CIRCUITS
        temp_lnk_df.write.mode('append').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_RACES')
    else:
        print('May be file is missing or data being loaded freshly LNK_RACES')
        temp_df = races_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        #write the file for if there is no hub file
        temp_df.select(col('RACEID_HK'),col('CIRCUITID_HK'),col('RACEID_CIRCUITID_HK'),\
                       col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/LNK_RACES')
except Exception as e:
    print(e)

# COMMAND ----------

races_df.show(2)

# COMMAND ----------

# adding SAT_CIRCUITS in RDV_TABLE/
try:
    if  file_check['SAT_LNK_RACES']:
        sat_df = spark.read.format("delta").load('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_LNK_RACES')
        sat_df.printSchema()
        temp_sat_df =  sat_df.join(broadcast(races_df.select( col('r_YEAR'),col('r_ROUND'),\
                                                            col('r_NAME'),col('r_URL'),col('r_DATETIME'), col('r_RACES_HASDIFF'), \
                                                            col('r_CIRCUITID_HK'), col('r_RACEID_HK'),col('r_RACEID_CIRCUITID_HK'),
                                                            col('r_RECORD_SOURCE'), col('r_LOAD_DATE'))),\
                               races_df.r_CIRCUITID_HK == sat_df.CIRCUITID_HK,'rightouter').\
                                       filter(col('r_RACEID_CIRCUITID_HK').isNull()).\
                                            select(col('r_RECORD_SOURCE'), col('r_LOAD_DATE'),col('r_RACEID_CIRCUITID_HK'),\
                                                                       col('r_RACES_HASDIFF'), col('r_CIRCUITID_HK'), col('r_RACEID_HK'), col('r_DATETIME'),\
                                                                       col('r_NAME'), col('r_ROUND'), col('r_YEAR'),col('r_URL'))
        #adding computed file to sat_circuits table
        for columns in temp_sat_df.columns:
            temp_sat_df = temp_sat_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema
        temp_sat_df.printSchema()
        temp_sat_df.show()
        #write changes into same location
        temp_sat_df.write.mode('append').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_LNK_RACES')
    else:
        print('May be file is missing or data being loaded freshly sat_LNK_RACES')
        temp_df = races_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        temp_df.select(col('RECORD_SOURCE'), col('LOAD_DATE'),col('RACEID_CIRCUITID_HK'),\
                                                                       col('RACES_HASDIFF'), col('DATETIME'),\
                                                                       col('NAME'), col('ROUND'), col('YEAR'),col('URL')).\
        write.mode('overwrite').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_LNK_RACES')
except Exception as e:
    print(e)

# COMMAND ----------


