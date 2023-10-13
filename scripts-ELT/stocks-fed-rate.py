# Databricks notebook source
# MAGIC %md
# MAGIC ### USA FED rate monthly ingestion

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType 
from datetime import date, datetime, timedelta
import requests
import xml.etree.ElementTree as et

start_time = datetime.now()

# COMMAND ----------

# MAGIC
# MAGIC %run ../utilities/parameters
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run ../utilities/elt_utils
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run ../utilities/mongodb_utils
# MAGIC

# COMMAND ----------


util = elt_util()
prm = Parameters()
params = prm.get_params()

#log information to be saved into DynamoDB
log = {}
log['project_name'] = params['FOLDER_SYSTEM']
log['entity_name'] = params['TABLE_EFFR']
log['start_time'] = start_time
log['source_type'] = 'API'
log['source_path'] = params['API_FED_EFFR']
log['source_entity'] = 'effr'
log['reprocessed'] = False

log = util.write_log(log)

# COMMAND ----------


ACCESS_KEY = util.get_access_key()
SECRET_KEY = util.get_secret_key()

sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", f"s3.amazonaws.com") 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 


# COMMAND ----------


# to get the first record into gold layer 
try:
    effr_df = util.read_delta_table('GOLD', 'EFFR', 'date < "2014-01-01"')
    effr_date_max = effr_df.select( F.max('date').alias('effr_date_max') ).collect()[0]['effr_date_max']
    effr_date_max = date.fromisoformat(str(effr_date_max))
except:
    effr_date_max = date.fromisoformat('2013-01-01')

url = params['API_FED_EFFR']

print(effr_date_max)

# COMMAND ----------

# DBTITLE 1,Getting data from Brazil Central Bank API

schema_bronze = StructType([ 
                    StructField('typeRate',StringType(), nullable=False), 
                    StructField('effectiveDate',StringType(), nullable=False), 
                    StructField('percentRate',StringType(), nullable=False),
                    StructField('ymd',StringType(), nullable=True),
                ])

try:
    response = requests.get(url=url)
    if response.status_code == 200:
        content = et.fromstring(response.content)
        xml = et.tostring(content, encoding='utf-8').decode('utf-8')

        data=[]

        for rate in content.findall(".//rate"):
            rate_dict = {}
            effective_date = rate.find('effectiveDate').text
            rate_dict['effectiveDate'] = effective_date
            rate_dict['percentRate'] = rate.find('percentRate').text
            rate_dict['typeRate'] = 'EFFR'
            
            data.append(rate_dict)

    else:
        print(f"HTTP request failed with status code {response.status_code}")

except Exception as ex:
    raise Exception(f"USA FED API Error: {ex}")


effr_df_new = spark.createDataFrame(data=data, schema=schema_bronze)
effr_df_new = (effr_df_new.withColumn('year', F.split(F.col('effectiveDate'), '-', -1)[0])
                          .withColumn('month', F.split(F.col('effectiveDate'), '-', -1)[1])
                          .groupBy(
                                'typeRate', 'year', 'month')
                          .agg( 
                               F.first('percentRate').alias('first_rate')
                               )
                          .withColumn('effectiveDate', F.concat_ws('-','year','month', F.lit('01')))
                          .withColumn('percentRate', F.col('first_rate'))
                          .withColumn('ymd', F.lit(date.today().strftime('%Y%m%d')))
                          .drop( 'first_rate', 'year', 'month')
                          )


# COMMAND ----------

# DBTITLE 1,Ingestion of the new data to Bronze layer

args = {'merge_filter'    : 'old.typeRate = new.typeRate and old.effectiveDate = new.effectiveDate',
       'update_condition' : f"old.percentRate <> new.percentRate",
       'partition'        : 'ymd'}

util.merge_delta_table(effr_df_new, 'BRONZE', 'EFFR', args)

# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'BRONZE', 'EFFR')


# COMMAND ----------

# DBTITLE 1,transformation into Silver layer

schema_silver = StructType([ 
                    StructField('rate_type',StringType(), nullable=False), 
                    StructField('rate_date',DateType(), nullable=False), 
                    StructField('rate_value',DoubleType(), nullable=False),
                    StructField('ymd',StringType(), nullable=True),
                ])
effr_df = util.change_column_names(effr_df_new, schema_bronze, schema_silver)

effr_df = (effr_df.withColumn('rate_date',  F.col('rate_date').cast(DateType()))
                  .withColumn('rate_value', F.col('rate_value').cast(DoubleType())))

args = {'merge_filter'    : 'old.rate_type = new.rate_type and old.rate_date = new.rate_date',
       'update_condition' : f"old.rate_value <> new.rate_value",
       'partition'        : 'rate_type'}

util.merge_delta_table(effr_df, 'SILVER', 'EFFR', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'SILVER', 'EFFR')


# COMMAND ----------

# DBTITLE 1,Loading Selic rate to gold layer

args = {'merge_filter'    : 'old.rate_type = new.rate_type and old.rate_date = new.rate_date',
       'update_condition' : f"old.rate_value <> new.rate_value",
       'partition'        : 'rate_type'}

util.merge_delta_table(effr_df, "GOLD", "EFFR", args)

# COMMAND ----------

# DBTITLE 1,logging

end_time = datetime.utcnow()
log['end_time']     = end_time
log['elapsed_time'] = str(end_time - start_time)

log = util.write_log(log, 'GOLD', 'EFFR')

print('Elapsed time: ', log['elapsed_time'])


# COMMAND ----------


