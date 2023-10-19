# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating database and tables on Databricks cluster for stocks-analysis project
# MAGIC #### this script will create the data objects in order to be available for analysis application
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./elt_utils

# COMMAND ----------


util = elt_util()
params = util.get_params()


# COMMAND ----------


def stocks_create_table(table):
    
    location_table = f"/mnt/{util.get_path('GOLD', table)}"

    spark.sql(f"""
                create table if not exists {params['DTBRCS_DB_NAME']}.{params['TABLE_' + table]}
                using delta
                location '{location_table}'          
            """)
    print('Table created from: ', location_table)
    

# COMMAND ----------

# DBTITLE 1,1 - Create database 
spark.sql(f"""
    create database if not exists {params['DTBRCS_DB_NAME']} 
""")

spark.sql(f"USE {params['DTBRCS_DB_NAME']}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - Create tables:
# MAGIC #####   2.1 calendar
# MAGIC #####   2.2 prices
# MAGIC #####   2.3 prices monthly sumarized
# MAGIC #####   2.4 tickers 
# MAGIC #####   2.5 dividends
# MAGIC #####   2.6 company financial events ( Income Statement, Balance Sheet)
# MAGIC #####   2.7 Interest rate (USA FED Fund rate, Brazil Selic rate)
# MAGIC #####   2.8 company profile 
# MAGIC

# COMMAND ----------


tables = ['CALENDAR', 'TICKER', 'PRICE', 'PRICE_MONTH', 'DIVIDEND', 'INTEREST_RATE', 'FINANCIAL_EVENT']

for table in tables:
    stocks_create_table(table)
    


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------


