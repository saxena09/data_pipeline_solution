import os
import csv
from sqlalchmey import create_engine
import pandas as pd

class CsvToDatabase:
    def __init__(self, csv_paths, db_host, db_schema):
        self.csv_paths = csv_paths
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
        return final_data


    def write_to_database(self, final_data):
	engine=create_engine('postgresql://'+self.db_username+':'+self.db_password+'@'+self.db_host+':'+self.db_port+'/'+self.db_name')
        final_data.to_sql('table_name',engine)
        

    def process_csv_files(self):
        final_data=self.read_csv()
	if len(final_data)>0:
           self.write_to_database(final_data)
