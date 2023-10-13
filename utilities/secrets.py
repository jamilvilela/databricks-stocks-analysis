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
        secrets['SECRET_KEY'] = secrets['SECRET_KEY'].replace("/", "%2F")
        return secrets
