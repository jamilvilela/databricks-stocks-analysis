# Databricks notebook source
# MAGIC %md
# MAGIC ### Ticker list
# MAGIC ##### this script runs a pipeline starting from MongoDB Database and storing bronze, silver, and gold data layers on AWS S3 buckets.

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

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, desc
from delta import *
from datetime import datetime, timedelta

# COMMAND ----------

start_time = datetime.utcnow()

util = elt_util()
prm = Parameters()
params = prm.get_params()
mongodb = MongoDB()

#log information to be saved into DynamoDB
log = {}
log['project_name'] = params['FOLDER_SYSTEM']
log['entity_name'] = params['TABLE_TICKER']
log['start_time'] = start_time
log['source_type'] = 'MongoDB'
log['source_path'] = 'stocks_analysis.tickers'
log['source_entity'] = 'tickers'
log['reprocessed'] = False

log = util.write_log(log)


# COMMAND ----------

ACCESS_KEY = util.get_access_key()
SECRET_KEY = util.get_secret_key()

sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 


# COMMAND ----------

def get_schema(tier:str = 'source') -> StructType:
        if tier == 'source': 
            st = StructType([ 
                        StructField('_id',StringType(), nullable=False), 
                        StructField('Type',StringType(), nullable=False), 
                        StructField('Ticker',StringType(), nullable=False), 
                        StructField('Name',StringType(), nullable=True), 
                        StructField('Exchange',StringType(), nullable=True), 
                        StructField('Category_Name',StringType(), nullable=True), 
                        StructField('Country',StringType(), nullable=True)
                ])
        elif tier == 'target': 
            st = StructType([ 
                        StructField('ticker_id',StringType(), nullable=False), 
                        StructField('ticker_type',StringType(), nullable=False), 
                        StructField('ticker',StringType(), nullable=False), 
                        StructField('ticker_name',StringType(), nullable=True), 
                        StructField('ticker_exchange',StringType(), nullable=True), 
                        StructField('ticker_category',StringType(), nullable=True), 
                        StructField('ticker_country',StringType(), nullable=True),
                        StructField('ymd',StringType(), nullable=True)
                ])
        return st

# COMMAND ----------

def read_mongodb_tickers():

    pipeline = "[ {$match: {'Ticker': {$ne: null} } } ]"

    ymd = datetime.utcnow().strftime('%Y%m%d')

    df = ( spark.read
                .format('mongo')
                .option('spark.mongodb.input.uri', params['MONGO_CONNECTION_STRING'])
                .option('database', params['MONGO_DATABASE_NAME'])
                .option('collection', params['MONGO_COLLECTION_TICKER'])
                .option('pipeline', pipeline)
                .schema(get_schema('source'))
                .load() )
    
    df = df.withColumn('ymd', lit(ymd))

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - reading data source
# MAGIC #### reading mongodb collection data source
# MAGIC

# COMMAND ----------


df_ticker = read_mongodb_tickers()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - ingestion into bronze layer 
# MAGIC #### extraction data from mongodb to AWS S3

# COMMAND ----------


args = {'merge_filter'    : 'old.Type = new.Type and old.Ticker = new.Ticker',
       'update_condition' : "old.Name <> new.Name or old.Exchange <> new.Exchange or old.Category_Name <> new.Category_Name or old.Country <> new.Country",
       'partition'        : 'ymd'}

util.merge_delta_table(df_ticker, "BRONZE", "TICKER", args)

#df_ticker_target = read_delta_table( 'BRONZE', 'TICKER')

# changing dataframe to the target schema 
#df_ticker_target = spark.createDataFrame(df_ticker_target.rdd, get_schema('target'))


# COMMAND ----------


log = util.write_log(log, 'BRONZE', 'TICKER')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - loading to silver layer
# MAGIC #### loading data from bronze to silver S3 buckets 

# COMMAND ----------


df_ticker_new = util.change_column_names(df_ticker, get_schema('source'), get_schema('target'))

args = {'merge_filter'    : 'old.ticker_type = new.ticker_type and old.ticker = new.ticker',
       'update_condition' : "old.ticker_name <> new.ticker_name or old.ticker_exchange <> new.ticker_exchange or old.ticker_category <> new.ticker_category or old.ticker_country <> new.ticker_country",
       'partition'        : 'ticker_type'}

util.merge_delta_table(df_ticker_new, "SILVER", "TICKER", args)


# COMMAND ----------


log = util.write_log(log, 'SILVER', 'TICKER')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - loading to gold tier
# MAGIC #### loading data from silver to gold S3 buckets 

# COMMAND ----------


args = {'merge_filter'    : 'old.ticker_type = new.ticker_type and old.ticker = new.ticker',
       'update_condition' : "old.ticker_name <> new.ticker_name or old.ticker_exchange <> new.ticker_exchange or old.ticker_category <> new.ticker_category or old.ticker_country <> new.ticker_country",
       'partition'        : 'ticker_type'}

util.merge_delta_table(df_ticker_new, "GOLD", "TICKER", args)


# COMMAND ----------


end_time = datetime.utcnow()
log['end_time']     = end_time
log['elapsed_time'] = str(end_time - start_time)

log = util.write_log(log, 'GOLD', 'TICKER')

print('Elapsed time: ', log['elapsed_time'])


# COMMAND ----------


