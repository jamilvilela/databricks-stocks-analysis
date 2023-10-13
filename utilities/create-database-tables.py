# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating database and tables on Databricks cluster for stocks-analysis project
# MAGIC #### this script will create the data objects in order to be available for analysis application
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./elt_utils

# COMMAND ----------

# MAGIC %run ../scripts-ELT/dim-date

# COMMAND ----------

# MAGIC %run ../scripts-ELT/stocks-prices
# MAGIC

# COMMAND ----------

# MAGIC %run ../scripts-ELT/stocks-balance
# MAGIC

# COMMAND ----------

# MAGIC %run ../scripts-ELT/stocks-income
# MAGIC

# COMMAND ----------

# MAGIC %run ../scripts-ELT/stocks-selic
# MAGIC

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
# MAGIC #####   2.3 tickers 
# MAGIC #####   2.4 dividends
# MAGIC #####   2.5 company income
# MAGIC #####   2.6 company balance
# MAGIC #####   2.7 Selic rate
# MAGIC #####   2.8 FED Fund rate
# MAGIC #####   2.9 company profile 

# COMMAND ----------


tables = ['CALENDAR','TICKER','PRICE', 'INCOME', 'BALANCE', 'DIVIDEND', 'SELIC', 'EFFR']

for table in tables:
    stocks_create_table(table)
    


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------


