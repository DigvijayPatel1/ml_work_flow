import sys 

from us_visa.exception import USVisaException
from us_visa.logger import logging

import os
from us_visa.constants import MONGODB_URL_KEY, DATABASE_NAME
import pymongo
import certifi
from dotenv import load_dotenv
load_dotenv()

ca = certifi.where()

class MongoDBClient:
    """
    Class Name :   export_data_into_feature_store
    Description :   This method exports the dataframe from mongodb feature store as dataframe 
    
    Output      :   connection to mongodb database
    On Failure  :   raises an exception
    """
    client = None

    def __init__(self, database_name = DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise USVisaException(f"MongoDB URL not found in environment variables with key: {MONGODB_URL_KEY}")
            MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info(f"Connected to MongoDB database: {database_name}")
        except Exception as e:
            raise USVisaException(e, sys)