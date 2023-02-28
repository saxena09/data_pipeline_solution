import os
import csv
from sqlalchmey import create_engine
import pandas as pd
import logging as logger

class CsvToDatabase:
    def __init__(self, csv_paths, db_host, db_schema):
        self.csv_paths = csv_paths
	self.fileName = "file_to_log.log"
	self.logger=logger
	self.logger.basicConfig(level=logger.DEBUG,filename=self.fileName,filemode='w+',format=f'%(levelname)s - %(lineno)s = %(asctime)s - %(message)s')
        self.db_host = db_host
	self.db_username=db_username
	self.db_password=db_password
	self.db_name=db_name
	self.db_port=db_port

    def read_csv(self):
        final_data=pd.DataFrame()
        for files in self.csv_paths:
            if os.path.exists(files):
	       df=pd.read_csv(files)
	       final_data=final_data.append(df)
        self.logger.info("Csv's read successfully")
        return final_data


    def write_to_database(self, final_data):
	engine=create_engine('postgresql://'+self.db_username+':'+self.db_password+'@'+self.db_host+':'+self.db_port+'/'+self.db_name')
        final_data.to_sql('table_name',engine)
	self.logger.info("Written to table successfully")
        

    def process_csv_files(self):
	try:
	    final_data=self.read_csv()
	    if len(final_data)>0:
	       self.write_to_database(final_data)
	except Exception as e:
	    self.logger.info("Error occurrred",e)
