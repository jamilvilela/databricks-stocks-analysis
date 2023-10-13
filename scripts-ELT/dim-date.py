# Databricks notebook source
# DBTITLE 1,This script generates the calendar table on Gold layer  
# MAGIC %md
# MAGIC #### Date dimension table
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, DateType, StringType 
from pyspark.sql import Row, Window
from datetime import date, datetime, timedelta

start_time = datetime.now()

# COMMAND ----------

# MAGIC
# MAGIC %run ../utilities/elt_utils
# MAGIC

# COMMAND ----------


util = elt_util()

lit_YES = 'Yes'
lit_NO = 'No'
lit_SEMESTER = ' Semester'
gold_layer = 'GOLD'
price_table = 'PRICE'
calendar_table = 'CALENDAR'

# to get the first price date into gold layer 
df_price = util.read_delta_table(gold_layer, price_table, 'ticker = "MSFT" and price_date < "2013-12-31" ')
price_date_min = df_price.select( F.min('price_date').alias('price_date_min') ).collect()[0]['price_date_min']
price_date_min = date.fromisoformat(price_date_min)


# COMMAND ----------

# to create a list of dates starting by the first price date in the gold layer
start_date = price_date_min
last_date = date.today() 
date_list=[]
while start_date <= last_date:
    date_list.append(Row(start_date.isoformat()))
    start_date +=  timedelta(days=1)

# to create date dimension
df_dates = spark.createDataFrame(date_list, schema=['dt_date'])
df_dates = (df_dates.withColumn('dt_year',              F.year('dt_date'))
                    .withColumn('dt_year_short',        F.date_format('dt_date', 'yy'))
                    .withColumn('dt_month',             F.lpad(F.month('dt_date'), 2, '0'))
                    .withColumn('dt_day',               F.lpad(F.day('dt_date'), 2, '0'))
                    .withColumn('dt_day_of_week',       F.dayofweek('dt_date'))
                    .withColumn('dt_day_of_week_short', F.date_format('dt_date', 'E'))
                    .withColumn('dt_day_of_week_full',  F.date_format('dt_date', 'EEEE'))
                    .withColumn('dt_week_of_year',      F.weekofyear('dt_date'))
                    .withColumn('dt_month_short',       F.date_format('dt_date', 'MMM'))
                    .withColumn('dt_month_full',        F.date_format('dt_date', 'MMMM'))
                    .withColumn('dt_first_day_of_month',F.lit(1))
                    .withColumn('dt_last_day_of_month', F.day(F.last_day('dt_date')))
                    .withColumn('dt_quarter',           F.date_format('dt_date', 'Q'))
                    .withColumn('dt_quarter_short',     F.date_format('dt_date', 'QQ'))
                    .withColumn('dt_quarter_long',      F.concat(F.lit('Q'), 
                                                                 col('dt_quarter'), 
                                                                 F.lit('.'), 
                                                                 col('dt_year_short')))
                    .withColumn('dt_semester',          F.when(col('dt_month') < '07', 1).otherwise(2))
                    .withColumn('dt_semester_desc',     F.concat(F.col('dt_semester'), 
                                                                (F.when(F.col('dt_semester') == 1, 'st')
                                                                   .when(F.col('dt_semester') == 2, 'nd')),
                                                                 F.lit(lit_SEMESTER)))
                    .withColumn('dt_full_date',         F.concat_ws(" '", F.concat_ws(', ', 
                                                                          F.concat_ws(' ', 
                                                                                      col('dt_day_of_week_short'), 
                                                                                      col('dt_day')), 
                                                                                      col('dt_month_short')), 
                                                                                      col('dt_year_short')))
                    .withColumn('dt_is_weekend',        F.when((F.col('dt_day_of_week') == 1) | 
                                                               (F.col('dt_day_of_week') == 7), 
                                                               F.lit(lit_YES)).otherwise(F.lit(lit_NO)))
                    .withColumn('dt_is_hollyday', (     F.when((F.col('dt_month') == '01') & (F.col('dt_day') == '01'), F.lit(lit_YES))
                                                         .when((F.col('dt_month') == '05') & (F.col('dt_day') == '01'), F.lit(lit_YES))
                                                         .when((F.col('dt_month') == '09') & (F.col('dt_day') == '07'), F.lit(lit_YES))
                                                         .when((F.col('dt_month') == '10') & (F.col('dt_day') == '12'), F.lit(lit_YES))
                                                         .when((F.col('dt_month') == '11') & (F.col('dt_day') == '02'), F.lit(lit_YES))
                                                         .when((F.col('dt_month') == '11') & (F.col('dt_day') == '15'), F.lit(lit_YES))
                                                         .when((F.col('dt_month') == '12') & (F.col('dt_day') == '25'), F.lit(lit_YES))
                                                        .otherwise(F.lit(lit_NO)))))

args = {'merge_filter' : 'old.dt_date = new.dt_date',
       'update_condition' : "old.dt_date >= (current_date() - INTERVAL '5' DAY)",
       'partition': 'dt_year'
       }

#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
util.merge_delta_table(df_dates, gold_layer, calendar_table, args)


# COMMAND ----------

end_time = datetime.now()

print('Elapsed time: ', end_time - start_time)

# COMMAND ----------


