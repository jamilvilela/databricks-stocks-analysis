# Databricks notebook source
def generate_dedup_keys_str(primary_keys: list):
    str_keys = ""
    for index in range(0, len(primary_keys)):
        if len(primary_keys) > 1:
            mystr = f"snapshot.{primary_keys[index]} = updates.{primary_keys[index]}"
            if index == len(primary_keys) - 1:
                str_keys += mystr
            else:
                str_keys += mystr + " AND "
        else:
            str_keys = f"snapshot.{primary_keys[index]} = updates.{primary_keys[index]}"
    return str_keys

# COMMAND ----------

# DBTITLE 1,These methods offer some facilities to read and merge delta tables, data quality, give AWS S3 bucket path, and more.
# MAGIC %%capture
# MAGIC ###### Class Methods:
# MAGIC - get_params()
# MAGIC - get_access_key()
# MAGIC - get_secret_key()
# MAGIC - get_path()
# MAGIC - write_log()
# MAGIC - check_dtypes()
# MAGIC - check_table_header()
# MAGIC - check_negative_columns()
# MAGIC - check_date_columns()
# MAGIC - change_column_names()
# MAGIC - read_delta_table()
# MAGIC - merge_delta_table()
# MAGIC - get_wallets_tickers()
# MAGIC - get_operation_metrics()
# MAGIC

# COMMAND ----------

# MAGIC %run ./parameters
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run ./mongodb_utils
# MAGIC

# COMMAND ----------

import json
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, LongType
from delta import *

class elt_util():
    
    def __init__(self) -> None:
        self.params = self.get_params()
    
    def get_params(self)-> dict:
        prm = Parameters()
        return prm.get_params()
    
    def get_access_key(self):
        return self.params['ACCESS_KEY']

    def get_secret_key(self):
        return self.params['SECRET_KEY']

    def get_path(self, tier, table, folder=None):
        return  self.params[f'BUCKET_{tier}'] +"/"+ \
                self.params['FOLDER_SYSTEM'] +"/"+ \
                self.params[f'TABLE_{table}'] + \
                ("/"+ self.params[f'FOLDER_{folder}'] if folder else '') 

    def check_table_dtypes(self, df, schema)-> bool:
        # df_types = {col: dtype for col, dtype in df.dtypes}
        # correct_dtypes = all(df_types[col] == str(field.dataType) for col, field in schema)
        return True #correct_dtypes

    def check_table_header(self, df, schema_cols)-> bool:    
        for col in schema_cols:
            if col not in df.columns:
                return False
        return True

    def check_negative_columns(self, df, number_cols)-> bool:
        negatives = [item for item in number_cols if (item in df.columns) and df.filter(col(item) < 0).count() > 0]
        return True if negatives else False

    def check_null_columns(self, df, notnull_cols)-> bool:
        nulls =[]
        for item in notnull_cols:
            if (item in df.columns) and df.filter(col(item).isNull()).count() > 0:
                nulls.append(item)
        return True if nulls else False

    def check_date_columns(self, df, date_cols)-> bool:
        invalid_date = [item for item in date_cols if (item in df.columns) and df.filter(to_date(col(item), self.date_format).isNull())]
        return True if invalid_date else False

    def check_data_quality(self, df, schema)-> dict:
        schema_cols = schema.fieldNames()
        number_cols = [field.name for field in schema.fields if isinstance(field.dataType, (DoubleType, LongType))]
        notnull_cols = [field.name for field in schema.fields if not field.nullable]
        
        result = {}
        result['check_header'] = self.check_table_header(df, schema_cols)
        result['has_negatives'] = self.check_negative_columns(df, number_cols)
        result['has_nulls'] = self.check_null_columns(df, notnull_cols)
    #    result['has_invalid_dates'] = self.check_date_columns(df, date_cols)
        result['check_dtypes'] = self.check_table_dtypes(df, schema)    
        
        result['is_valid'] = (
                                result['check_header'] and 
                            not result['has_negatives'] and 
                            not result['has_nulls'] and 
                            # not result['has_invalid_dates'] and
                                result['check_dtypes'])
        return result
    
    def process_data_quality_error(self, quality: dict):
        #TODO
        # send email 
        pass


    def change_column_names(self, df, source_schema, destiny_schema):
        '''
        this method receives two different schemas and 
        changes the column names of the dataframe (df) 
        from the source schema to the destiny schema
        '''
        
        schema_mapping = {source.name: destiny.name for source, destiny in zip(source_schema, destiny_schema)}
        new_columns = [col(old_name).alias(new_name) for old_name, new_name in schema_mapping.items()]
        
        df = df.select(*new_columns)
        
        return df

    def read_delta_table(self, tier, table, filter=None):
        
        path = self.params['DTBRCS_PREFIX'] + self.get_path(tier, table)

        if not filter:
            filter = '1=1'

        df = ( spark.read
                    .format('delta')
                    .load(path).where(filter) )

        return df

    def merge_delta_table(self, df_new, tier, table, args):

        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        
        path = self.params['DTBRCS_PREFIX'] + self.get_path(tier, table)

        if DeltaTable.isDeltaTable(spark, path):
            delta_table = DeltaTable.forPath(spark, path)

            (delta_table.alias('old')
                        .merge(df_new.alias('new'), args['merge_filter'])
                        .whenMatchedUpdateAll(  condition=args['update_condition'] )
                        .whenNotMatchedInsertAll()
                        .execute() )
        else:
            ( df_new.write
                    .format('delta')
                    .mode('append')
                    .partitionBy(args['partition'])
                    .option('overwriteSchema', 'true')
                    .save(path) )

        return path
    
    def get_operation_metrics(self, tier, table):

        path = util.params['DTBRCS_PREFIX'] + util.get_path(tier, table)

        deltaTable = DeltaTable.forPath(spark, path)
        df_hist = deltaTable.history(1)

        return df_hist.filter("operation in ('MERGE', 'WRITE') ").collect()[0]['operationMetrics']
    


    
    def write_log(self, log:dict, layer:str=None, table:str=None) -> dict:

        def update_log(log: dict, layer, table):
            '''
            this method updates log keys according to the layer informed
            '''
            delta_metrics = util.get_operation_metrics(layer, table)

            if 'numTargetRowsInserted' in delta_metrics:
                log[f'inserted_records_{layer.lower()}'] = int(delta_metrics['numTargetRowsInserted'])

            if 'numTargetRowsUpdated' in delta_metrics:
                log[f'updated_records_{layer.lower()}']  = int(delta_metrics['numTargetRowsUpdated'])

            if 'numTargetRowsDeleted' in delta_metrics:
                log[f'deleted_records_{layer.lower()}']  = int(delta_metrics['numTargetRowsDeleted'])

            if 'numOutputRows' in delta_metrics:
                log[f'inserted_records_{layer.lower()}'] = int(delta_metrics['numOutputRows'])

            log[f'location_{layer.lower()}'] = self.params['BUCKET_PREFIX'] + util.get_path(layer, table)

            return log

        log_table = self.params['LOG_COLLECTION']

        try:
            log_db = MongoDB(self.params['LOG_DATABASE_NAME'])

            if layer and table:
                log = update_log(log, layer, table)

            if '_id' in log:
                # update log
                log_db.mongo_update_items( Filter={"_id": log['_id']}, Update={'$set': log}, Collection=log_table, Upsert=True)
            else:
                #insert log
                log_db.mongo_insert_items(Items=log, Collection=log_table)

            return log

        except Exception as ex:
            raise Exception(ex)
        
    def get_wallets_tickers(self, filter=None) -> list:
        ''' 
        This method get the list of company tickers into the user App's wallets 
        '''

        if not filter:
            filter = {'_id' : {'$ne': None}}
        
        mongodb = MongoDB()
        pipeline = [{'$unwind': '$stocks'},
                    {'$match': filter},
                    {'$group': 
                        {'_id': '$stocks.ticker', 
                        'inserted_date': {'$min': "$stocks.inserted_date" },
                        'qtty': {'$sum': 1}
                        }}, 
                    {'$sort': {'qqty': -1, 'inserted_date': 1}
                    }]
        stocks = mongodb.mongo_aggregate(Pipeline=pipeline, Collection=self.params['MONGO_COLLECTION_WALLET'])

        return [stock['_id'] for stock in stocks]
    

# COMMAND ----------


