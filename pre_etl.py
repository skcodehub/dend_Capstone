import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
import boto3

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['keypair']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['keypair']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function is used to create a handler for Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_csv_data(spark):
    """
    This function processes song_data in the S3 bucket, creates partitioned data and writes to the output S3 bucket in parquet format. 
    It takes input argument
    spark session handler
    """
    #Use this section to load all the necessary files to S3 for further processing
    s3 = boto3.resource('s3')
    BUCKET = "capstoneimmi"
   
    #Load demographic data to S3
    s3.Bucket(BUCKET).upload_file("us-cities-demographics.csv", "demo/us-cities-demographics.csv")

    #Load I94_Port data to S3
    s3.Bucket(BUCKET).upload_file("I94_Port.csv", "port/I94Port.csv")

    #Load I94_Mode data to S3
    s3.Bucket(BUCKET).upload_file("I94_Mode.csv", "mode/I94Mode.csv")

    #Load I94_Visa data to S3
    s3.Bucket(BUCKET).upload_file("I94_Visa.csv", "visa/I94Visa.csv")

    #Load I94ADDR_State data to S3
    s3.Bucket(BUCKET).upload_file("I94ADDR_State.csv", "addrstate/I94AddrState.csv")

    #Load I94City_Res data to S3
    s3.Bucket(BUCKET).upload_file("I94City_Res.csv", "rescitycntry/I94CityRes.csv")
    

def main():
    """
    This is the main function that 
    1. sets the arguments to send to functions and
    2. invokes functions to process_csv_data
    """
    spark = create_spark_session()
    process_csv_data(spark)


if __name__ == "__main__":
    main()
