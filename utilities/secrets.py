# Databricks notebook source

class Secrets():
    def __init__(self) -> None:
        pass
        #self.secrets = self.get_secrets()
        
    def get_secrets(self)-> dict:
        secrets = {
                "MONGO_PASSWORD": "",
                "MONGO_USER_NAME": "",
                "MONGO_CLUSTER_NAME": "",
                "MONGO_PORT_NUMBER": "",

                "REGION": "",
                "ACCESS_KEY": "",
                "SECRET_KEY": "",
        }
        secrets['MONGO_PASSWORD'] = dbutils.secrets.get(scope = "stocks", key = "MONGO_PASSWORD")
        secrets['MONGO_USER_NAME'] = dbutils.secrets.get(scope = "stocks", key = "MONGO_USER_NAME")
        secrets['MONGO_CLUSTER_NAME'] = dbutils.secrets.get(scope = "stocks", key = "MONGO_CLUSTER_NAME")
        secrets['MONGO_PORT_NUMBER'] = dbutils.secrets.get(scope = "stocks", key = "MONGO_PORT_NUMBER")
        secrets['REGION'] = dbutils.secrets.get(scope = "stocks", key = "REGION")
        secrets['ACCESS_KEY'] = dbutils.secrets.get(scope = "stocks", key = "ACCESS_KEY")
        secrets['SECRET_KEY'] = dbutils.secrets.get(scope = "stocks", key = "SECRET_KEY")
        secrets['SECRET_KEY'] = secrets['SECRET_KEY'].replace("/", "%2F")
        
        return secrets
    

# COMMAND ----------


