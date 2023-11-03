# Databricks notebook source
# MAGIC %md
# MAGIC ### Companies' Profile  

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType, BooleanType
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


util = elt_util()
prm = Parameters()
params = prm.get_params()


# COMMAND ----------


#log information to be saved into DynamoDB
log = {}
log['project_name'] = params['FOLDER_SYSTEM']
log['entity_name'] = params['TABLE_PROFILE']
log['start_time'] = start_time
log['source_type'] = 'API'
log['source_path'] = 'yfinance.ticker.info'
log['source_entity'] = 'info'
log['reprocessed'] = False

log = util.write_log(log)

# COMMAND ----------


ACCESS_KEY = util.get_access_key()
SECRET_KEY = util.get_secret_key()

sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) 
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) 


# COMMAND ----------

# DBTITLE 1,Getting data from Yahoo Finance API 

ticker_list = util.get_wallets_tickers(filter={'stocks.type': {'$in':['Currency', 'ETF', 'Stock', 'Index']}})

df_list = []
for item in ticker_list:

    try:
        ticker = yf.Ticker(item)
        info_json = ticker.info
        
        info_json['ticker'] = item

        if 'companyOfficers' in info_json:
            del info_json['companyOfficers']

        # adjusting the dataframe received to a flat colunar dataframe
        if len(info_json) > 0:
            info_df = pd.DataFrame(data=info_json, index=['uuid'])
            df_list.append( info_df )
            
    except Exception as ex:
        #raise Exception(f"Yahoo Finance API Error: {ex}")
        break

if df_list:
    info_df = pd.concat(df_list)
    info_df['ymd'] = date.today().strftime('%Y%m%d')

    info_df = (spark.createDataFrame(data=info_df,)
                    .distinct())
else:
    info_df = util.read_delta_table('BRONZE', 'PROFILE', 'ymd=20231023')


# COMMAND ----------

# DBTITLE 1,Ingestion of the new data from Yahoo Finance to Bronze layer

key = ['uuid']
updt_str = util.update_conditions(info_df.columns, key, 'OR', False )

args = {'merge_filter'    : 'old.uuid = new.uuid',
       'update_condition' : f"{updt_str}",
       'partition'        : 'ymd'}

util.merge_delta_table(info_df, 'BRONZE', 'PROFILE', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'BRONZE', 'PROFILE')


# COMMAND ----------

# DBTITLE 1,transformation into Silver layer
profile_fields = [  'uuid',
                    'ticker',
                    'quoteType',
                    'city',
                    'state',
                    'zip',
                    'country',
                    'phone',
                    'website',
                    'industrykey',
                    'industry',
                    'sectorkey',
                    'sector',
                    'longBusinessSummary',
                    'fullTimeEmployees',
                    'currency',
                    'payoutRatio',
                    'marketCap',
                    'enterpriseValue',
                    'floatShares',
                    'sharesOutstanding',
                    'shortName',
                    'longName',
                    'timeZoneFullName',
                    'ymd'
                    ]

info_df = info_df.select(profile_fields)

schema_bronze = info_df.schema

schema_silver = StructType([ 
                    StructField('id_profile',StringType(), nullable=False), 
                    StructField('ticker',StringType(), nullable=False), 
                    StructField('ticker_type',StringType(), nullable=False), 
                    StructField('city',StringType(), nullable=False), 
                    StructField('state',StringType(), nullable=False), 
                    StructField('zip',StringType(), nullable=False), 
                    StructField('country',StringType(), nullable=False), 
                    StructField('phone',StringType(), nullable=False), 
                    StructField('website',StringType(), nullable=False), 
                    StructField('industrykey',StringType(), nullable=False), 
                    StructField('industry',StringType(), nullable=False), 
                    StructField('sectorkey',StringType(), nullable=False), 
                    StructField('sector',StringType(), nullable=False), 
                    StructField('business_summary',StringType(), nullable=False), 
                    StructField('employees',IntegerType(), nullable=False), 
                    StructField('currency',StringType(), nullable=False), 
                    StructField('payout_ratio',DoubleType(), nullable=False), 
                    StructField('market_cap',DoubleType(), nullable=False), 
                    StructField('enterprise_value',IntegerType(), nullable=False), 
                    StructField('shares_float',DoubleType(), nullable=False), 
                    StructField('shares_outstanding',IntegerType(), nullable=False), 
                    StructField('short_name',StringType(), nullable=False), 
                    StructField('long_name',StringType(), nullable=False), 
                    StructField('time_Zone',StringType(), nullable=False), 
                    StructField('ymd',StringType(), nullable=True)
                ])

profile_df = util.change_column_names(info_df, schema_bronze, schema_silver)

key = ['id_profile']
updt_str = util.update_conditions(profile_df.columns, key, 'OR', False )

args = {'merge_filter'    : 'old.id_profile = new.id_profile',
       'update_condition' : f"{updt_str}",
       'partition'        : 'ticker_type'}

util.merge_delta_table(profile_df, 'SILVER', 'PROFILE', args)


# COMMAND ----------

# DBTITLE 1,logging

log = util.write_log(log, 'SILVER', 'INCOME')


# COMMAND ----------


def slowly_chg_dim_type_2( df, tier: str, table: str, partition: str, key: list ):

    updt_str = util.update_conditions(df.columns, key, 'OR', False )

    # to create a temp view
    df.createOrReplaceTempView('vw_snapshot')

    path = util.params['DTBRCS_PREFIX'] + util.get_path(tier, table)
    database = util.params['DTBRCS_DB_NAME']
    target = util.params[f'TABLE_{table}']

    if not DeltaTable.isDeltaTable(spark, path):
        ( df
            .write
            .format('delta')
            .mode('append')
            .partitionBy(partition)
            .option('overwriteSchema', 'true')
            .save(path) )
    else:
        merge_sql = f"""
                        MERGE INTO {database}.{target} AS old
                        USING (
                            SELECT 
                                id_profile AS merge_key, 
                                * 
                            FROM 
                                vw_snapshot
                            UNION ALL
                            SELECT 
                                NULL AS merge_key, 
                                new.* 
                            FROM vw_snapshot new
                            JOIN {database}.{target} old 
                                ON old.id_profile = new.id_profile 
                                AND ( {updt_str} )
                        ) AS new
                        ON  new.merge_key = old.id_profile
                        
                        WHEN MATCHED AND (old.c_u_r_r_e_n_t = TRUE AND ( {updt_str} )) THEN
                            UPDATE SET
                                old.end_dt = current_timestamp()-1,
                                old.c_u_r_r_e_n_t = FALSE
                        WHEN NOT MATCHED THEN
                            INSERT ( {', '.join(profile_df.columns)} , c_u_r_r_e_n_t, end_dt  )
                            VALUES ( new.{', new.'.join(profile_df.columns)} , TRUE, null)
                    """

        spark.sql(merge_sql)


# COMMAND ----------


profile_df = (profile_df            
                    .withColumn('end_dt', F.lit('-'))
                    .withColumn('c_u_r_r_e_n_t', F.lit(True) ))



# COMMAND ----------


key = ['id_profile']
slowly_chg_dim_type_2(profile_df, 'GOLD', 'PROFILE', 'ticker_type', key)


# COMMAND ----------

# DBTITLE 1,logging

end_time = datetime.utcnow()
log['end_time']     = end_time
log['elapsed_time'] = str(end_time - start_time)

log = util.write_log(log, 'GOLD', 'INCOME')
print('Elapsed time: ', log['elapsed_time'])


# COMMAND ----------


