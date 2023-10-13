# Databricks notebook source
# DBTITLE 1,Parameter that indicates reprocess historical load
# to inform the start date for reloading
start_load = ''
 

# COMMAND ----------

# DBTITLE 1,To installing necessary libraries
# MAGIC %%capture --no-display
# MAGIC
# MAGIC !pip install -q requests #==2.28.2
# MAGIC !pip install -q yfinance==0.2.28

# COMMAND ----------

# DBTITLE 1,Stock Prices - ELT process - Data extraction from Yahoo Finance API and load into AWS S3 lakehouse - PySpark and Delta Lake frameworks 
import pandas as pd
import yfinance as yf
from delta import *
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from pyspark.sql.functions import lit, asc, desc, sum, avg, stddev, mean, median, lag
from pyspark.sql.functions import window, round, date_format, first
from pyspark.sql.column import cast
from pyspark.sql import Window

start_time = datetime.utcnow()

# COMMAND ----------

# DBTITLE 1,To import project dependencies
# MAGIC
# MAGIC %run ../utilities/parameters 
# MAGIC

# COMMAND ----------

# DBTITLE 0,importing project dependencies
# MAGIC
# MAGIC %run ../utilities/mongodb_utils
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run ../utilities/elt_utils
# MAGIC

# COMMAND ----------

# DBTITLE 1,to get schema definition function

def get_schema(layer:str) -> StructType:
        if layer == 'BRONZE': 
            st = StructType([ 
                        StructField('Ticker',StringType(), nullable=False), 
                        StructField('Date',StringType(), nullable=False), 
                        StructField('Open',DoubleType(), nullable=False), 
                        StructField('High',DoubleType(), nullable=False), 
                        StructField('Low',DoubleType(), nullable=False), 
                        StructField('Close',DoubleType(), nullable=False), 
                        StructField('AdjClose',DoubleType(), nullable=True), 
                        StructField('Volume',DoubleType(), nullable=True), 
                        StructField('Dividends',DoubleType(), nullable=True), 
                        StructField('StockSplits',DoubleType(), nullable=True), 
                        StructField('CapitalGains',DoubleType(), nullable=True), 
                        StructField('ymd',StringType(), nullable=True)
                ])
        elif layer == 'SILVER': 
            st = StructType([ 
                        StructField('ticker',StringType(), nullable=False), 
                        StructField('price_date',StringType(), nullable=False), 
                        StructField('vl_open',DoubleType(), nullable=False), 
                        StructField('vl_high',DoubleType(), nullable=False), 
                        StructField('vl_low',DoubleType(), nullable=False), 
                        StructField('vl_close',DoubleType(), nullable=False), 
                        StructField('vl_adj_close',DoubleType(), nullable=True), 
                        StructField('vl_volume',DoubleType(), nullable=True), 
                        StructField('vl_dividends',DoubleType(), nullable=True), 
                        StructField('vl_splits',DoubleType(), nullable=True), 
                        StructField('vl_capital_gains',DoubleType(), nullable=True),
                        StructField('ymd',StringType(), nullable=True),
                        StructField('z_score',DoubleType(), nullable=True), 
                        StructField('vl_change_day',DoubleType(), nullable=True),
                        StructField('vl_change_week',DoubleType(), nullable=True),
                        StructField('vl_change_month',DoubleType(), nullable=True),
                        StructField('perc_change_day',DoubleType(), nullable=True),
                        StructField('perc_change_week',DoubleType(), nullable=True),
                        StructField('perc_change_month',DoubleType(), nullable=True),
                        StructField('vl_moving_avg_21_days',DoubleType(), nullable=True),
                        StructField('vl_moving_avg_200_days',DoubleType(), nullable=True),
                        StructField('update_dt',StringType(), nullable=True)
                ])
        elif layer == 'GOLD': 
            st = StructType([ 
                        StructField('ticker',StringType(), nullable=False), 
                        StructField('price_date',DateType(), nullable=False), 
                        StructField('vl_open',DoubleType(), nullable=False), 
                        StructField('vl_high',DoubleType(), nullable=False), 
                        StructField('vl_low',DoubleType(), nullable=False), 
                        StructField('vl_close',DoubleType(), nullable=False), 
                        StructField('vl_adj_close',DoubleType(), nullable=True), 
                        StructField('vl_volume',DoubleType(), nullable=True), 
                        StructField('ymd',StringType(), nullable=True),
                        StructField('z_score',DoubleType(), nullable=True), 
                        StructField('vl_change_day',DoubleType(), nullable=True),
                        StructField('vl_change_week',DoubleType(), nullable=True),
                        StructField('vl_change_month',DoubleType(), nullable=True),
                        StructField('perc_change_day',DoubleType(), nullable=True),
                        StructField('perc_change_week',DoubleType(), nullable=True),
                        StructField('perc_change_month',DoubleType(), nullable=True),
                        StructField('vl_moving_avg_21_days',DoubleType(), nullable=True),
                        StructField('vl_moving_avg_200_days',DoubleType(), nullable=True),
                        StructField('update_dt',StringType(), nullable=True)
                ])
        return st
    

# COMMAND ----------

# DBTITLE 1,Yahoo Finance's API data extraction
def get_stock_prices(ticker_list) -> pd.DataFrame:
    '''
    This method get stocks history data from a list of tickers.
    return a pandas dataframe.
    '''        

    schema = get_schema('BRONZE')
    df = spark.createDataFrame(data=[], schema=schema)
    columns = df.columns
    
    try:
        df = yf.download(ticker_list, group_by='Ticker', period='10y', threads=True, actions=True)

    except Exception as ex:
        raise Exception(f"Yahoo Finance API Error: {ex}")
    
    # adjusting the dataframe received to a flat colunar dataframe
    if not df.empty:
        df = df.stack(level=0)
        df = df.reset_index(level=1, col_level=1)
        df = df.rename(columns={'level_1':'Ticker'})
        df = df.reset_index(drop=False)
        df = df.reindex(columns=columns)
        df['Volume'] = df['Volume'].apply(float)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

    return df

# COMMAND ----------

# DBTITLE 1,variables and objects init

util = elt_util()
prm = Parameters()
params = prm.get_params()

#log information to be saved into DynamoDB
log = {}
log['project_name'] = params['FOLDER_SYSTEM']
log['entity_name']  = params['TABLE_PRICE']
log['start_time']   = start_time
log['source_type']  = 'API'
log['source_path']  = 'yfinance.download.history'
log['source_entity']= 'history'

log = util.write_log(log)


# COMMAND ----------

# DBTITLE 1,Time for load reprocess in amount of days
if start_load:
    year = int(start_load.split('-')[0])
    month= int(start_load.split('-')[1].split('-')[0])
    day  = int(start_load.split('-')[2])

    param_reprocess = (date.today() - date(year,month,day)).days
    log['reprocessed'] = True
else:
    param_reprocess = 5
    log['reprocessed'] = False


# COMMAND ----------

# DBTITLE 1,Authorization to AWS S3 access 
ACCESS_KEY = util.get_access_key()
SECRET_KEY = util.get_secret_key()

sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", f"s3.amazonaws.com") 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Extraction from Yahoo Finance API 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Getting data from Yahoo Finance API and MongoDB

ticker_list = util.get_wallets_tickers()

df_prices = get_stock_prices(ticker_list)

log['total_records_source'] = len(df_prices)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Data ingestion into Bronze layer 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ingestion of the new data from Yahoo Finance to S3 bucket

ymd = start_time.strftime('%Y%m%d')

df_price_new = spark.createDataFrame(df_prices, get_schema('BRONZE'))
df_price_new = df_price_new.withColumn('ymd', lit(ymd))

args = {'merge_filter'    : 'old.Ticker = new.Ticker and old.Date = new.Date',
       'update_condition' : f"old.Date >= (current_date() - INTERVAL '{param_reprocess}' DAY) ",
       'partition'        : 'ymd'}

util.merge_delta_table(df_price_new, 'BRONZE', 'PRICE', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'BRONZE', 'PRICE')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - loading to Silver layer
# MAGIC

# COMMAND ----------

# DBTITLE 1,Data quality

source_schema = get_schema('BRONZE')
destiny_schema = get_schema('SILVER')

quality_result = util.check_data_quality(df_price_new, source_schema)

print('Is data quality valid: ', quality_result['is_valid'])

if quality_result['is_valid'] == False:
    util.process_data_quality_error(quality_result)
    raise Exception('Error: Checking Bronze layer data quality.')

df_price_new = util.change_column_names(df_price_new, source_schema, destiny_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI's calculation:
# MAGIC - $ variation by day
# MAGIC - $ variation by week
# MAGIC - $ variation by month
# MAGIC - % variation by day
# MAGIC - % variation by week
# MAGIC - % variation by month
# MAGIC - $ moving avarage 20 days
# MAGIC - $ moving avarage 200 days

# COMMAND ----------

# DBTITLE 1, Data transformation   

window_date    = Window.partitionBy('ticker').orderBy(asc('price_date')) 
window_month   = Window.partitionBy('ticker', 'dt_year', 'dt_month').orderBy('price_date')
window_20_day  = Window.partitionBy('ticker').orderBy('price_date').rowsBetween(-20,0) 
window_200_day = Window.partitionBy('ticker').orderBy('price_date').rowsBetween(-200,0) 
df_price_new = (df_price_new    .withColumn('dt_year', date_format('price_date', 'y'))
                                .withColumn('dt_month', date_format('price_date', 'M'))
                                ## z_score = (current - average(close)) / stddev(close)  
                                .withColumn("z_score", round( (col("vl_close") - avg("vl_close").over(window_date)) / stddev("vl_close").over(window_date), 4))

                                ## $ var = (current - previous_day) 
                                .withColumn('vl_change_day', round((col('vl_close') - lag('vl_close',1).over(window_date)),2) )
                                ## % var = (current - previous_day) / current
                                .withColumn('perc_change_day', round( (100 * col('vl_change_day') / lag('vl_close',1).over(window_date)),2) )

                                ## $ var = (current - previous_day) 
                                .withColumn('vl_change_week', round((col('vl_close') - lag('vl_close',5).over(window_date)),2) )
                                ## % var = (current - previous_day) / current
                                .withColumn('perc_change_week', round((100 * col('vl_change_week') / lag('vl_close',5).over(window_date)),2) )

                                ## $ var = (current - previous_day) 
                                .withColumn('vl_change_month', round((col('vl_close') - first('vl_close').over(window_month)),2) )
                                ## % var = (current - previous_day) / current
                                .withColumn('perc_change_month', round((100 * col('vl_change_month') / lag('vl_close',1).over(window_month)),2) )

                                ## simple average over 20 days
                                .withColumn('vl_moving_avg_20_days', round(avg(col('vl_close')).over(window_20_day),2) )  
                                ## simple average over 200 days
                                .withColumn('vl_moving_avg_200_days', round(avg(col('vl_close')).over(window_200_day),2) )
                                .withColumn('price_date', col('price_date').cast(DateType()))
                                .withColumn('vl_open', round('vl_open',2))
                                .withColumn('vl_high', round('vl_high',2))
                                .withColumn('vl_low', round('vl_low',2))
                                .withColumn('vl_close', round('vl_close',2))
                                .drop('dt_year', 'dt_month'))

args = {'merge_filter'    : 'old.ticker = new.ticker and old.price_date = new.price_date',
       'update_condition' : f"old.price_date >= (current_date() - INTERVAL '{param_reprocess}' DAY)",
       'partition'        : 'ticker'}

util.merge_delta_table(df_price_new, 'SILVER', 'PRICE', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'SILVER', 'PRICE')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - loading to Gold layer
# MAGIC

# COMMAND ----------

# DBTITLE 1,Loading Dividends to gold layer

df_dividends = (df_price_new.filter( f"vl_dividends > 0 ")
                            .select('ticker', col('price_date').alias('ex_date'), 'vl_dividends'))

args = {'merge_filter'    : 'old.ticker = new.ticker and old.ex_date = new.ex_date',
       'update_condition' : f"old.ex_date >= (current_date() - INTERVAL '{param_reprocess}' DAY)",
       'partition'        : 'ticker'}

util.merge_delta_table(df_dividends, 'GOLD', 'DIVIDEND', args)


# COMMAND ----------

# DBTITLE 1,loading prices data to Gold layer into S3 bucket 

df_price_new = df_price_new.drop('vl_dividends','vl_splits','vl_capital_gains')

args = {'merge_filter'    : 'old.ticker = new.ticker and old.price_date = new.price_date',
       'update_condition' : f"old.price_date >= (current_date() - INTERVAL '{param_reprocess}' DAY)",
       'partition'        : 'ticker'}

util.merge_delta_table(df_price_new, 'GOLD', 'PRICE', args)


# COMMAND ----------

# DBTITLE 1,logging and finish

end_time = datetime.utcnow()
log['end_time']     = end_time
log['elapsed_time'] = str(end_time - start_time)

log = util.write_log(log, 'GOLD', 'PRICE')
print('Elapsed time: ', log['elapsed_time'])


# COMMAND ----------


