
from pyspark.sql import SparkSession
import logging as logger

class CsvToDatabase:
    def __init__(self, csv_paths, db_host, db_schema):
        self.csv_paths = csv_paths
        self.fileName = "file_to_log.log"
	    self.logger=logger
	    self.logger.basicConfig(level=logger.DEBUG,filename=self.fileName,filemode='w+',format=f'%(levelname)s - %(lineno)s = %(asctime)s - %(message)s')
        self.db_host = db_host
        self.db_schema = db_schema
        self.spark = SparkSession.builder.appName("CsvToDatabase").getOrCreate()

    def read_csv_files(self):
        df = self.spark.read.option("header", "true").csv(self.csv_paths)
        self.logger.info("Csv's read successfully")
        return df

    def write_to_database(self, df, table_name):
        db_url = "jdbc:postgresql://{}/{}".format(self.db_host, self.db_schema)
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "your-username",
            "password": "your-password"
        }
        df.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)
        self.logger.info("written output to DB")

    def execute(self, table_name):
        try:
            df = self.read_csv_files()
            if(df.count()!=0):
                self.write_to_database(df, table_name)
        except Exception as e:
            self.logger.info("Error occurred",e)
        
