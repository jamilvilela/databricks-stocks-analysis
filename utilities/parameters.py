# Databricks notebook source
# MAGIC %run ./secrets
# MAGIC

# COMMAND ----------

class Parameters():
    def __init__(self) -> None:
        pass
        #self.params = self.get_params()
    
    def get_params(self)-> dict:
        sec = Secrets()
        secrets = sec.get_secrets()

        params = {
                "MONGO_DATABASE_NAME": "stocks_analysis",
                "MONGO_COLLECTION_WALLET": "wallet",
                "MONGO_COLLECTION_TICKER": "tickers",
                "MONGO_CONNECTION_STRING": "mongodb+srv://${MONGO_USER_NAME}:${MONGO_PASSWORD}@${MONGO_CLUSTER_NAME}.mongodb.net/${MONGO_DATABASE_NAME}",            

                "LOG_DATABASE_NAME": "etl_control",
                "LOG_COLLECTION": "etl_log",
                "LOG_CONNECTION_STRING": "mongodb+srv://${MONGO_USER_NAME}:${MONGO_PASSWORD}@${MONGO_CLUSTER_NAME}.mongodb.net/${LOG_DATABASE_NAME}",            

                "Period_history": 10,
                "Period_history_unit": "year",
                "Stocks_Price_Granularity": "1day",

                "FILE_NAME_DT_FMT": "%Y%m%d_%H%M%S",
                "DATA_FIELD_DT_FMT": "%Y-%m-%d %H:%M:%S",
                "TIMEZONE": "America/Sao_Paulo",

                "DTBRCS_PREFIX": "dbfs:/mnt/",
                "BUCKET_PREFIX": "s3a://",
                "BUCKET_BRONZE": "jamil-lakehouse-bronze",
                "BUCKET_SILVER": "jamil-lakehouse-silver",
                "BUCKET_GOLD"  : "jamil-lakehouse-gold",

                "FOLDER_REJECTED": "_rejected",
                "FOLDER_PROCESSED": "_processed",
                "FOLDER_LOGS": "_logs",

                "FOLDER_SYSTEM": "stocks-analysis",
                "TABLE_PRICE": "prices",
                "TABLE_INCOME": "income",
                "TABLE_TICKER": "tickers",
                "TABLE_DIVIDEND": "dividends",
                "TABLE_CALENDAR": "calendar",
                "TABLE_SELIC": "selic",
                "TABLE_EFFR": "effr",
                "TABLE_PROFILE": "profile",
                "TABLE_BALANCE": "balance",

                "DTBRCS_DB_NAME": "stocks_analysis",

                "API_BCB_SELIC": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=json",
                "API_BCB_SELIC_STARTDATE": "dataInicial",
                "API_BCB_SELIC_ENDDATE": "dataFinal",

                "API_FED_EFFR": "https://markets.newyorkfed.org/read?productCode=50&eventCodes=500&limit=2700&startPosition=0&sort=postDt:-1&format=xml",
                "API_FED_DATE": "effectiveDate",
                "API_FED_RATE": "percentRate"
        }

        params = {**params, **secrets}

        params['MONGO_CONNECTION_STRING'] = f"mongodb+srv://{params['MONGO_USER_NAME']}:{params['MONGO_PASSWORD']}@{params['MONGO_CLUSTER_NAME']}.mongodb.net/{params['MONGO_DATABASE_NAME']}" 
        params['LOG_CONNECTION_STRING']   = f"mongodb+srv://{params['MONGO_USER_NAME']}:{params['MONGO_PASSWORD']}@{params['MONGO_CLUSTER_NAME']}.mongodb.net/{params['LOG_DATABASE_NAME']}" 

        return params

# COMMAND ----------


