# Databricks notebook source
# MAGIC %md
# MAGIC ### Selic rate monthly ingestion

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType 
from datetime import date, datetime, timedelta
import requests

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
log['entity_name'] = params['TABLE_SELIC']
log['start_time'] = start_time
log['source_type'] = 'API'
log['source_path'] = 'bcb.gov.br/dadosabertos'
log['source_entity'] = 'selic_rate'
log['reprocessed'] = False

log = util.write_log(log)

# COMMAND ----------


ACCESS_KEY = util.get_access_key()
SECRET_KEY = util.get_secret_key()

sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 


# COMMAND ----------


# to get the first record into gold layer 
try:
    selic_df = util.read_delta_table('GOLD', 'SELIC', 'date < "2014-01-01"')
    selic_date_max = selic_df.select( F.max('date').alias('selic_date_max') ).collect()[0]['selic_date_max']
    selic_date_max = date.fromisoformat(str(selic_date_max))
except:
    selic_date_max = date.fromisoformat('2013-01-01')

selic_date_start = selic_date_max.strftime('%d/%m/%Y')
selic_date_end = start_time.strftime('%d/%m/%Y')

params = prm.get_params()
url_selic = f"{params['API_BCB_SELIC']}&{params['API_BCB_SELIC_STARTDATE']}={selic_date_start}&{params['API_BCB_SELIC_ENDDATE']}={selic_date_end}"


# COMMAND ----------

# DBTITLE 1,Getting data from Brazil Central Bank API

schema_bronze = StructType([ 
                    StructField('tipo',StringType(), nullable=False),
                    StructField('data',StringType(), nullable=False), 
                    StructField('valor',StringType(), nullable=False),
                    StructField('ymd',StringType(), nullable=True),
                ])

try:
    response = requests.get(url=url_selic)
    content = json.loads(response.content)

except Exception as ex:
    raise Exception(f"Brazil Central Bank API Error: {ex}")

selic_df_new = spark.createDataFrame(data=content,)
selic_df_new = (selic_df_new.withColumn('tipo', F.lit('SELIC'))
                            .withColumn('ymd', F.lit(date.today().strftime('%Y%m%d')))
                            .select('tipo', 'data', 'valor', 'ymd'))
                            


# COMMAND ----------

# DBTITLE 1,Ingestion of the new data to Bronze layer

args = {'merge_filter'    : 'old.tipo = new.tipo and old.data = new.data',
       'update_condition' : "old.valor <> new.valor",
       'partition'        : 'ymd'}

util.merge_delta_table(selic_df_new, 'BRONZE', 'SELIC', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'BRONZE', 'SELIC')


# COMMAND ----------

# DBTITLE 1,transformation into Silver layer

schema_silver = StructType([ 
                    StructField('rate_type',StringType(), nullable=False), 
                    StructField('rate_date',DateType(), nullable=False), 
                    StructField('rate_value',DoubleType(), nullable=False),
                    StructField('ymd',StringType(), nullable=True),
                ])
selic_df = util.change_column_names(selic_df_new, schema_bronze, schema_silver)

selic_df = (selic_df.withColumn('rate_date', F.concat_ws('-', F.substring('rate_date',7,4),
                                                              F.substring('rate_date',4,2),
                                                              F.substring('rate_date',1,2))
                                                .cast(DateType()))
                    .withColumn('rate_value', F.col('rate_value').cast(DoubleType())))

args = {'merge_filter'    : 'old.rate_type = new.rate_type and old.rate_date = new.rate_date',
       'update_condition' : "old.rate_value <> new.rate_value",
       'partition'        : 'rate_type'}

util.merge_delta_table(selic_df, 'SILVER', 'SELIC', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'SILVER', 'SELIC')


# COMMAND ----------

# DBTITLE 1,Loading Selic rate to gold layer

args = {'merge_filter'    : 'old.rate_type = new.rate_type and old.rate_date = new.rate_date',
       'update_condition' : "old.rate_value <> new.rate_value",
       'partition'        : 'rate_type'}

util.merge_delta_table(selic_df, "GOLD", "SELIC", args)

# COMMAND ----------

# DBTITLE 1,logging

end_time = datetime.utcnow()
log['end_time']     = end_time
log['elapsed_time'] = str(end_time - start_time)

log = util.write_log(log, 'GOLD', 'SELIC')

print('Elapsed time: ', log['elapsed_time'])


# COMMAND ----------


