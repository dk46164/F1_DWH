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

constr_schema = StructType([
                    StructField("constructorId",StringType(),False),\
                    StructField("constructorRef",StringType(),False),\
                    StructField("name",StringType(),False),\
                    StructField("nationality",StringType(),False),\
                    StructField("url",StringType(),False),\
    
])

constr_df = spark.read.option('header',True).schema(constr_schema).csv('dbfs:/mnt/f1-s3-mnt/raw_files/constructors.csv')
# constr_df.show()

#columns level transformation
for columns in constr_df.columns:
    if constr_df.schema[columns].dataType == StringType()and columns != 'url' :
        constr_df = constr_df.withColumn(columns, trim(upper(col(columns)))).replace("\\N","",subset= [columns])
    elif constr_df.schema[columns].dataType == IntegerType():
        constr_df = constr_df.fillna(-1,subset = [columns])
    constr_df = constr_df.withColumnRenamed(columns, 'c_'+columns.upper())

#verifying changes
constr_df.show()

#adding data vault columns hash diff, hask key ,load date, record source
constr_df = constr_df.withColumn('c_CONSTRUCTORID_HK',upper(md5(col('c_CONSTRUCTORID').cast(StringType()))))
#calculating has diff
constr_df = constr_df.withColumn('c_CONSTRUCTOR_HASDIFF',upper(md5(concat_ws('',*constr_df.columns[1:])).cast(StringType())))
#adding record_source & Load date
constr_df = constr_df.withColumn('c_RECORD_SOURCE',upper(lit('constructors.CSV'))).withColumn('c_LOAD_DATE',current_timestamp())
#verifying the changes
constr_df.printSchema()
constr_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import broadcast

#adding HUB_CONSTRUCTORS in RDV_TABLE/
#checking if there is already HUB_CONSTRUCTORS ,if so then read data from exisitng hub & do left outer join to insert new records

#check if hub_circuits file exists
file_check = {'HUB_CONSTRUCTORS':False,'SAT_CONSTRUCTORS':False}
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CONSTRUCTORS")
        file_check['HUB_CONSTRUCTORS'] = True
except Exception as e:
      file_check['HUB_CONSTRUCTORS'] = False
        
#check if hub_circuits file exists
try:
        dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_CONSTRUCTORS")
        file_check['SAT_CONSTRUCTORS'] = True
except Exception as e:
        file_check['SAT_CONSTRUCTORS'] =  False
print(file_check)
#adding HUB_circuits in RDV_TABLE/
#checking if there is already hub circuits ,if so then read data from exisitng hub & do left outer join to insert new records
try:
    if file_check['HUB_CONSTRUCTORS']:
        hub_df = spark.read.format("delta").load('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CONSTRUCTORS')  
        temp_hub_df = hub_df.join(broadcast(constr_df.select(col('c_CONSTRUCTORID'),col('c_CONSTRUCTORID_HK') ,\
                                                           col('c_RECORD_SOURCE'),col('c_LOAD_DATE'))),\
                                                           constr_df.c_CONSTRUCTORID_HK == hub_df.CONSTRUCTORID_HK,'rightouter').\
                            filter(col('CONSTRUCTORID_HK').isNull()).\
                            select(col('c_CONSTRUCTORID_HK'),col('c_CONSTRUCTORID'),col('c_RECORD_SOURCE'),col('c_LOAD_DATE'))
        for columns in temp_hub_df.columns:
             temp_hub_df = temp_hub_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema 
        temp_hub_df.printSchema()
        temp_hub_df.show()
        #apending new df DATA to HUB_CIRCUITS
        temp_hub_df.write.mode('append').format("delta").save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CONSTRUCTORS')
    else:
        print('May be file is missing or data being loaded freshly in HUB_CONSTRUCTORS')
        temp_df = constr_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        #write the file  if there is no hub file
        temp_df.select(col('CONSTRUCTORID_HK'),col('CONSTRUCTORID'),col('RECORD_SOURCE'),col('LOAD_DATE')).write.mode('overwrite').format('parquet').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CONSTRUCTORS')
except Exception as e:
    print(e)
# adding SAT_CIRCUITS in RDV_TABLE/
try:
    if file_check['SAT_CONSTRUCTORS']:
        sat_df = spark.read.format("delta").load('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_CONSTRUCTORS')
        temp_sat_df =  sat_df.join(broadcast(constr_df.select(col('c_RECORD_SOURCE'),col('c_LOAD_DATE'),col('c_CONSTRUCTORID_HK') ,\
                                                              col('c_CONSTRUCTOR_HASDIFF'),col('c_CONSTRUCTORREF'),col('c_NAME'),col('c_NATIONALITY'),col('c_URL')\
                                                           )),sat_df.CONSTRUCTORID_HK==constr_df.c_CONSTRUCTORID_HK,'rightouter').\
                                                            filter(col('CONSTRUCTORID_HK').isNull()).\
                                                                select(col('c_RECORD_SOURCE'),col('c_LOAD_DATE'),col('c_CONSTRUCTORID_HK') ,\
                                                              col('c_CONSTRUCTOR_HASDIFF'),col('c_CONSTRUCTORREF'),col('c_NAME'),col('c_NATIONALITY'),col('c_URL'))
        for columns in temp_sat_df.columns:
            temp_sat_df = temp_sat_df.withColumnRenamed(columns, columns[2:])
        #verifying  New Schema
        temp_sat_df.printSchema()
        temp_sat_df.show()
        #adding computed file to sat_circuits table
        temp_df.write.mode('append').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_CONSTRUCTORS')
    else:
        print('May be file is missing or data being loaded freshly')
        temp_df = constr_df
        for columns in temp_df.columns:
            temp_df = temp_df.withColumnRenamed(columns, columns[2:])
        temp_df.printSchema()
        temp_df.write.mode('overwrite').format('delta').save('/mnt/f1-s3-mnt/processed_file/RDV_TABLE/SAT_CONSTRUCTORS')
except  Exception as e:
    print(e)
  


# COMMAND ----------

dbutils.fs.ls ("/mnt/f1-s3-mnt/processed_file/RDV_TABLE/HUB_CONSTRUCTORS")