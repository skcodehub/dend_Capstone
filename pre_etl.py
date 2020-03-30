import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
#import psycopg2

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
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark

def process_csv_data():
    """
    This function reads csv data from local drive and writes to the output S3 bucket.
    This data were manually extracted from SAS Lables description and manual cleaning done 
    1. to check for nulls
    2. set the delimiter
    3. remove tabs
    4. trim some records
    5. cleaned up row 636 in I94_Port.csv ('AG') - to remove table and set delimiter to ;
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

def process_sas_data(spark):
    """
    This function reads cleaned sas_data from local drive and writes to the output S3 bucket.
    For the purpose of this project, I used the note book to read SAS data and converted it to parquet format to store in the local drive.
    Assumes that the source data is in parquet format
    """
    #get the source data from local drive
    df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    #write to parquet
    df_spark.write.parquet("sas_data_parquet", "overwrite")
    
    #Create a temporary view to perform data cleansing. After analyzing the data, came upon 
    #one column that is part of data model that needed cleansing
    df_spark.createOrReplaceTempView("immi_fact")
    #Found the invalid dates in the note book and hardcoded the values here. Assign null to all invalid values
       # +--------+
       # | dtaddto|
       # +--------+
       # |     183|
       # |10 02003|
       # |     D/S|
       # |06 02002|
       # |/   183D|
       # |12319999|
       # +--------+
    i94_valid_date = spark.sql("""SELECT DISTINCT CICID, I94YR, I94MON, I94CIT,I94RES, I94PORT,ARRDATE,I94MODE, 
                                I94ADDR, DEPDATE, I94BIR, I94VISA, COUNT, 
                                DTADFILE,
                                VISAPOST, OCCUP, ENTDEPA, ENTDEPD, ENTDEPU, MATFLAG, BIRYEAR, 
                                CASE WHEN TRIM(dtaddto) IN ('183','10 02003','D/S','06 02002','/   183D','12319999') 
                                THEN NULL 
                                ELSE dtaddto END as DTADDTO, GENDER, INSNUM, AIRLINE, ADMNUM, FLTNO, VISATYPE
                            FROM immi_fact """)
    #Write cleansed data to local drive 
    #i94_valid_date.write.parquet("sas_data_parquet_clean", "overwrite")
    #Read cleansed data from local drive
    #df_spark=spark.read.parquet("sas_data_parquet_clean/")
    
    #Load cleansed immi data to S3
    i94_valid_date.write.parquet("s3a://capstoneimmi/sas-data-parquet/", mode="overwrite")

def main():
    """
    This is the main function that 
    1. sets the arguments to send to functions and
    2. invokes functions to process_sas_data, process_csv_data, load_date_dim
    """
 
    spark = create_spark_session()
    process_sas_data(spark)
    process_csv_data()


if __name__ == "__main__":
    main()
