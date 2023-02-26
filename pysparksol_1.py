
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,when,col
from functools import reduce

class CsvToDatabase:
    def __init__(self, csv_paths, db_host, db_schema,table_name):
        self.csv_paths = csv_paths
        self.db_host = db_host
        self.db_schema = db_schema
	  self.table_name=table_name
        self.spark = SparkSession.builder.appName("CsvToDatabase").getOrCreate()

    def read_csv_files(self):
        df = self.spark.read.option("header", "true").csv(self.csv_path)
	  input_df=df.withColumnRenamed("Field3","input_Field3").withColumnRenamed("Field4","input_Field4").withColumnRenamed("Field5","input_Field5")
        return input_df

    def read_from_database(self):
	  db_url = "jdbc:postgresql://{}/{}".format(self.db_host, self.db_schema)
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "your-username",
            "password": "your-password"
        }

	  target_df = spark.read \
    		.format("jdbc") \
   		 .option("url", db_url) \
   		 .option("dbtable", self.tablename) \
   		 .option("user", db_properties.username) \
    	       .option("password", db_properties.password) \
   		 .option("driver", db_properties.driver) \
   		 .load()

	  return target_df


     def execute_steps(self,input_df,target_df):

	   pk_cols = ["Field1","Field2"]

	   joined_df = input_df.join(target_df, on=pk_cols, how="fullouter")
         deleted_df = joined_df.join(input_df, on=pk_cols, how="left_anti").withColumn("operation", lit("D")).drop()

         deleted_df = deleted_df.withColumn("operation", lit("D"))

	   inserted_df_new = joined_df.filter(target_df[pk_cols[0]].isNull()).withColumn("operation", lit("I"))
         inserted_df_exists = joined_df.filter(((target_df["Field1"].isNotNull()) & (target_df["Field2"].isNotNull())) & (input_df["Field1"].isNotNull()) & (input_df["Field2"].isNotNull())) \
                     			.filter(reduce(lambda a, b: a & b, [target_df[d] == input_df[c] for c,d in zip(input_df.columns,target_df.columns) if ~(c in pk_cols)])) \
                     			 .withColumn("operation", when(((target_df["Field1"] == input_df["Field1"])& (target_df["Field2"] == input_df["Field2"])), "I"))

	   inserted_df=inserted_df_new.union(inserted_df_exists)

	   updated_df = joined_df.filter(((target_df["Field1"].isNotNull()) & (target_df["Field2"].isNotNull())) & (input_df["Field1"].isNotNull()) & (input_df["Field2"].isNotNull())) \
                     .filter(reduce(lambda a, b: a | b, [target_df[d] != input_df[c] for c,d in zip(input_df.columns,target_df.columns) if ~(c in pk_cols)])) \
                     .withColumn("operation", when(((target_df["Field1"] == input_df["Field1"])& (target_df["Field2"] == input_df["Field2"])), "U"))

	   when_clause = [
    			when(col(c).isNotNull(), col(c)).otherwise(col(d)).alias(c)
    			for c,d in zip(input_df.columns,target_df.columns) if c not in pk_cols]

	  final_df = inserted_df.union(updated_df).union(deleted_df).select(
    				[col(c) for c in pk_cols] + when_clause + [col("operation")])

	  return final_df
	  

    def write_to_database(self, df):
        db_url = "jdbc:postgresql://{}/{}".format(self.db_host, self.db_schema)
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "your-username",
            "password": "your-password"
        }
        df.write.jdbc(url=db_url, table=self.table_name, mode="overwrite", properties=db_properties)


    def execute(self, table_name):
        input_df = self.read_csv_files()
	  target_df=self.read_from_database()
	  final_df=self.execute_steps(input_df,target_df)
        self.write_to_database(final_df)
