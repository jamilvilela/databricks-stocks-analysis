# Databricks notebook source
# MAGIC %md
# MAGIC ### Annual and Quarterly Companies' Income Statement  

# COMMAND ----------

# MAGIC %%capture --no-display
# MAGIC
# MAGIC !pip install -q requests #==2.28.2
# MAGIC !pip install -q yfinance==0.2.28

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType 
from datetime import date, datetime, timedelta
import yfinance as yf
import pandas as pd

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
log['entity_name'] = params['TABLE_INCOME']
log['start_time'] = start_time
log['source_type'] = 'API'
log['source_path'] = 'yfinance.ticker.income_stmt'
log['source_entity'] = 'income'
log['reprocessed'] = False

log = util.write_log(log)

# COMMAND ----------


ACCESS_KEY = util.get_access_key()
SECRET_KEY = util.get_secret_key()

sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", f"s3.amazonaws.com") 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 


# COMMAND ----------

# DBTITLE 1,Getting data from Yahoo Finance API 

schema_bronze = StructType([ 
                    StructField('Period',StringType(), nullable=False), 
                    StructField('Ticker',StringType(), nullable=False), 
                    StructField('Description',StringType(), nullable=False), 
                    StructField('Date',StringType(), nullable=False), 
                    StructField('Value',DoubleType(), nullable=False), 
                    StructField('ymd',StringType(), nullable=True)
                ])

df = spark.createDataFrame(data=[], schema=schema_bronze)
columns = df.columns

ticker_list = util.get_wallets_tickers()

df_list = []
for item in ticker_list:
    try:
        ticker = yf.Ticker(item)
        df_annual = ticker.income_stmt
        df_quarter = ticker.quarterly_income_stmt
        
        # adjusting the dataframe received to a flat colunar dataframe
        if not df_annual.empty:
            df_annual = (df_annual.stack(level=0)
                                .reset_index(level=1)
                                .reset_index(drop=False)
                                .rename(columns={'index': 'Description', 'level_1':'Date', 0:'Value'})
                                .reindex(columns=columns))
            df_annual['Period'] = 'Annual'
            df_annual['Ticker'] = str(item)
            df_list.append( df_annual )
            
        if not df_quarter.empty:
            df_quarter = (df_quarter.stack(level=0)
                                .reset_index(level=1)
                                .reset_index(drop=False)
                                .rename(columns={'index': 'Description', 'level_1':'Date', 0:'Value'})
                                .reindex(columns=columns))
            df_quarter['Ticker'] = str(item)
            df_quarter['Period'] = 'Quarter'
            df_list.append( df_quarter )

    except Exception as ex:
        raise Exception(f"Yahoo Finance API Error: {ex}")

df = pd.concat(df_list)
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
df['ymd'] = date.today().strftime('%Y%m%d')

income_df = spark.createDataFrame(df, schema_bronze)


# COMMAND ----------

# DBTITLE 1,Ingestion of the new data from Yahoo Finance to Bronze layer

args = {'merge_filter'    : 'old.Ticker = new.Ticker and old.Description = new.Description and old.Date = new.Date and old.Period = new.Period',
       'update_condition' : f"old.Value <> new.Value",
       'partition'        : 'ymd'}

util.merge_delta_table(income_df, 'BRONZE', 'INCOME', args)

# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'BRONZE', 'INCOME')


# COMMAND ----------

# DBTITLE 1,transformation into Silver layer

schema_silver = StructType([ 
                    StructField('income_period',StringType(), nullable=False), 
                    StructField('ticker',StringType(), nullable=False), 
                    StructField('income_description',StringType(), nullable=False), 
                    StructField('income_report_date',DateType(), nullable=False), 
                    StructField('income_value',DoubleType(), nullable=False), 
                    StructField('ymd',StringType(), nullable=True)
                ])

income_df_new = util.change_column_names(income_df, schema_bronze, schema_silver)

args = {'merge_filter'    : 'old.ticker = new.ticker and old.income_description = new.income_description and old.income_report_date = new.income_report_date and old.income_period = new.income_period',
       'update_condition' : f"old.income_value <> new.income_value",
       'partition'        : 'income_period'}

util.merge_delta_table(income_df_new, 'SILVER', 'INCOME', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'SILVER', 'INCOME')


# COMMAND ----------


args = {
    "merge_filter": "old.ticker = new.ticker and old.income_description = new.income_description and old.income_report_date = new.income_report_date and old.income_period = new.income_period",
    "update_condition": f"old.income_value <> new.income_value",
    "partition": "ticker",
}

util.merge_delta_table(income_df_new, "GOLD", "INCOME", args)


# COMMAND ----------

# DBTITLE 1,logging

end_time = datetime.utcnow()
log['end_time']     = end_time
log['elapsed_time'] = str(end_time - start_time)

log = util.write_log(log, 'GOLD', 'INCOME')
print('Elapsed time: ', log['elapsed_time'])


# COMMAND ----------


