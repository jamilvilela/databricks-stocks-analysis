# Databricks notebook source
# MAGIC %%capture --no-display
# MAGIC #!pip install --upgrade pip
# MAGIC !pip install -q pymongo==4.5.0
# MAGIC

# COMMAND ----------

# MAGIC %run ./parameters
# MAGIC

# COMMAND ----------

import urllib.parse as parse
from pymongo import MongoClient

class DynamoDB():
    """
    This class gives a connection to DynamoDB database 
    and executes basic operations (Insert, Insert many, Update)
    """    

    def __init__(self):
        self._db = ''
        self._params = ''
        self._db_connect()

    def _db_connect(self):
        
        prm = Parameters()
        self._params = prm.get_params()

        _stringConn = self._params['MONGO_CONNECTION_STRING']

        try:
            cluster  = MongoClient(_stringConn)
            self._db = cluster[self._params['MONGO_DATABASE_NAME']]
            
        except Exception as ex:
            raise Exception(f'Database connection error: {ex}')
    
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

    def mongo_update_items(self, Filter, Update, Collection, Upsert:bool=True):
        """ Update/Insert a set of documents"""
        try:
            coll    = self._db[Collection]
        
            if Filter == None:
                raise 'There is no filter for update command.'
        
            upd_rslt = coll.update_many(Filter, Update, upsert=Upsert)
            return upd_rslt
        
        except Exception as ex:
            raise Exception(f'Error updating document: {ex}')

    def mongo_delete_items(self, Filter, Collection):
        ''' 
            delete the documents regarding the Filter parameter
        '''
        try:            
            coll = self._db[Collection]
            coll.delete_many(filter=Filter)
            
        except Exception as ex:
            raise Exception(f'Error deleting MongoDB collection: {Collection} when {Filter}. \n{ex}')
        
        

   

# COMMAND ----------


