from us_visa.configuration.mongo_db_connection import MongoDBClient
from us_visa.constants import DATABASE_NAME
from us_visa.exception import USvisaException
import pandas as pd
import sys
from typing import Optional
import numpy as np

class USVisaData:
    """
    this help to import entire mongo db collection as dataframe
    """
    def __init__(self):
        try:
            self.mongo_db_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise USvisaException(e, sys)
    
    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        this method exports the dataframe from mongodb collection as dataframe 
        
        Output      :   dataframe
        On Failure  :   raises an exception
        """
        try:
            if database_name is None:
                database_name = self.mongo_db_client.database_name
                
            collection = self.mongo_db_client.client[database_name][collection_name]
            
            data = list(collection.find())
            df = pd.DataFrame(data)
            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)
            return df
        except Exception as e:
            raise USvisaException(e, sys)
