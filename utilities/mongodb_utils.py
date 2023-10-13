# Databricks notebook source
# MAGIC %%capture --no-display
# MAGIC #!pip install --upgrade pip
# MAGIC #!pip install -q pymongo==4.5.0
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run ./parameters
# MAGIC

# COMMAND ----------

import urllib.parse as parse
from pymongo import MongoClient

class MongoDB():
    """
    This class gives a connection to the MongoDB database 
    and executes basic operations (Insert, Insert many, Update)
    """    

    def __init__(self, database=None):
        self._db = ''
        self._params = ''
        self._database = database
        self._db_connect()

    def _db_connect(self):
        
        prm = Parameters()
        self._params = prm.get_params()
        self._database = self._database if self._database else self._params['MONGO_DATABASE_NAME']

        if self._database not in [self._params['MONGO_DATABASE_NAME'], self._params['LOG_DATABASE_NAME']]:
            raise (f'Error: Database name {self._database} is not valid.')

        if self._database == self._params['MONGO_DATABASE_NAME']:
            str_conn = 'MONGO_CONNECTION_STRING'
        else:
            str_conn = 'LOG_CONNECTION_STRING'

        _stringConn = self._params[str_conn]

        try:
            cluster  = MongoClient(_stringConn)
            self._db = cluster[self._database]
            
        except Exception as ex:
            raise Exception(f'Database connection error: {ex}')
    
    def mongo_aggregate(self, Pipeline, Collection) -> list:
        
        try:            
            coll = self._db[Collection]
            cursor = coll.aggregate(pipeline=Pipeline)
            result = [doc for doc in cursor if doc]
            return result 
        
        except Exception as ex:
            raise Exception(f'Aggregate MongoDB Error: {Collection} \n {ex.args[0]}')
    
    def mongo_find(self, Filter, Collection, Limit=0, Sort=None):
        try:
            coll = self._db[Collection]
            
            if Filter == None and (Limit == 0 or Limit == None):
                Limit = 1000
            
            if Limit == 1:
                cursor = coll.find_one(Filter)
                result = cursor
            else:
                cursor = coll.find(filter=Filter, limit=Limit) 
                result = [doc for doc in cursor if doc]
                if Sort:
                    result.sort()
            return result
        
        except Exception as ex:
            raise Exception(f'Database connection error: {ex}')

    def mongo_insert_items(self, Items, Collection):
        """ Insert a set of documents"""
        try:
            coll = self._db[Collection]      
            if type(Items) is dict:   
                insList = coll.insert_one(Items)
            else:
                insList = coll.insert_many(Items)
                
            return insList 
        
        except Exception as ex:
            raise Exception(f'Error inserting collection: {Collection}. {ex}')
    
    def mongo_update_items(self, Filter, Update, Collection, Upsert):
        """ Update a set of documents"""
        try:
            coll    = self._db[Collection]
        
            if Filter == None:
                raise 'There is no filter for update command.'
        
            upd_rslt = coll.update_many(Filter, Update, upsert=Upsert)
            return upd_rslt
        
        except Exception as ex:
            raise Exception(f'Error updating collection: {Collection}. {ex}')

    def mongo_delete_items(self, Filter, Collection):
        ''' 
            delete the documents regarding the Filter parameter
        '''
        try:            
            coll = self._db[Collection]
            coll.delete_many(filter=Filter)
            
        except Exception as ex:
            raise Exception(f'Error deleting collection: {Collection} when {Filter}. \n{ex}')


# COMMAND ----------


