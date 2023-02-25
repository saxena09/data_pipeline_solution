
from pyspark.sql import SparkSession

class CsvToDatabase:
    def __init__(self, csv_paths, db_host, db_schema):
        self.csv_paths = csv_paths
        self.db_host = db_host
        self.db_schema = db_schema
        self.spark = SparkSession.builder.appName("CsvToDatabase").getOrCreate()

    def read_csv_files(self):
        df = self.spark.read.option("header", "true").csv(self.csv_paths)
        return df

    def write_to_database(self, df, table_name):
        db_url = "jdbc:postgresql://{}/{}".format(self.db_host, self.db_schema)
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "your-username",
            "password": "your-password"
        }
        df.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)

    def execute(self, table_name):
        df = self.read_csv_files()
        self.write_to_database(df, table_name)
