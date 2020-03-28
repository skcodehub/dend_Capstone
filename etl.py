import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import datetime
from pyspark.sql import SparkSession
import os

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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    return spark

def load_date_dim(cur, conn):
    """
    This function builds date_dim
    Parameters: This function requires connection and connection cursor as parameters
    """
    date_dim_insert = ("""INSERT INTO Date_Dim(calendar_date, date_id, day, month, year, weekday) VALUES( %s, %s, %s, %s, %s, %s);""")
    # create timestamp column from original timestamp column
    epoch = datetime.datetime(1960, 1, 1)
    start_date = datetime.datetime(2016,1,1)
    end_date = datetime.datetime(2020,12,31)
    date_id = 20454
    interval = datetime.timedelta(days =1)

    while start_date <= end_date:
        cur.execute(date_dim_insert, [start_date, date_id, start_date.day, start_date.month, start_date.year, start_date.weekday()])
        conn.commit()
        date_id= date_id+1
        start_date = start_date+interval

    start_date = datetime.datetime(2015,12,31)
    end_date = datetime.datetime(1900,1, 1)
    date_id = 20453
    interval = datetime.timedelta(days = 1)

    while start_date >= end_date:
        cur.execute(date_dim_insert, [start_date, date_id, start_date.day, start_date.month, start_date.year, start_date.weekday()])
        conn.commit()
        date_id= date_id-1
        start_date = start_date-interval

def load_dim_tables(spark, cur, conn):
    """
    This function builds dimension tables
    Parameters: This function requires spark session, connection, connection cursor and config as parameters
    """
    demo_dim_insert = ("""INSERT INTO CITY_STATE_DEMOGRAPHIC_DIM(CITY, STATE_CODE,STATE,MEDIAN_AGE, MALE_POP,FEMALE_POP, TOTAL_POP,
                    NUM_VETERANS, FOREIGN_BORN, AVG_HOUSEHOLD_SIZE, RACE, COUNT
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """)

    #path=config['S3']['DEMOGR_DATA'] +'/us-cities-demographics.csv'
    #df = spark.read.csv('s3://capstoneimmi/demo/us-cities-demographics.csv').options(header='true', delimiter=';')
   # df_demogr = df[['City', 'State Code', 'State', 'Median Age', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Average Household Size', 'Race', 'Count']].dropDuplicates()
    #df.head()
   # for i, row in df_demogr.iterrows():
    #    cur.execute(demo_dim_insert, row)
    
    df = spark.read.csv("s3://capstoneimmi/mode/I94Mode.csv").options(header='true')
    df.head()
    
def main():
    """
    Connect to the database and call functions to copy data into
    stage tables and insert into final tables.
    Reads the configuration params from the config file.
    Parameters: This function requires connection and connection cursor as parameters.
    """
    spark = create_spark_session()
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    #load_date_dim(cur, conn)
    load_dim_tables(spark,cur,conn)

    conn.close()


if __name__ == "__main__":
    main()