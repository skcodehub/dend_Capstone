import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, dim_table_queries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import os

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['keypair']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['keypair']['AWS_SECRET_ACCESS_KEY']


def load_dim_tables(cur, conn):
    """
    This function builds dimension tables
    Parameters: This function requires spark session, connection, connection cursor and config as parameters
    """
    for query in dim_table_queries:
        cur.execute(query)
        conn.commit()

def load_date_dim(cur, conn):
    """
    This function builds date_dim
    Parameters: This function requires connection and connection cursor as parameters
    If more date range is required, end_date (higher) and end_date_min can be set
    Referred github for ideas on how to seed the date
    """
    #Truncate date_dim before loading
    date_dim_truncate = """ TRUNCATE TABLE date_dim;"""
    cur.execute(date_dim_truncate)
    conn.commit()
    
    date_dim_insert = ("""
                    INSERT INTO Date_Dim(calendar_date, date_id, day, month, year, weekday) VALUES( %s, %s, %s, %s, %s, %s);""")
    # create timestamp column from original timestamp column
    epoch = datetime.datetime(1960, 1, 1)
    start_date = datetime.datetime(2016,1,1)
    end_date = datetime.datetime(2016,12,31)
    date_id = 20454
    interval = datetime.timedelta(days =1)
    
    while start_date <= end_date:
        cur.execute(date_dim_insert, [start_date, date_id, start_date.day, start_date.month, start_date.year, start_date.weekday()])
        conn.commit()
        date_id= date_id+1
        start_date = start_date+interval

    start_date = datetime.datetime(2015,12,31)
    end_date_min = datetime.datetime(2015,1, 1)
    date_id = 20453
    interval = datetime.timedelta(days = 1)

    while start_date >= end_date_min:
        cur.execute(date_dim_insert, [start_date, date_id, start_date.day, start_date.month, start_date.year, start_date.weekday()])
        conn.commit()
        date_id= date_id-1
        start_date = start_date-interval

        
def load_staging_tables(cur, conn):
    """ This function loads sas data from S3 to stage table
        Takes 2 arguments -database connection and connection cursor
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def load_fact_data(cur, conn):
    """
    This function builds Immigrant_Fact
    Parameters: This function requires connection and connection cursor as parameters
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    
def main():
    """
    Connect to the database and call functions to copy data into
    stage tables and insert into final tables.
    Reads the configuration params from the config file.
    Parameters: This function requires connection and connection cursor as parameters.
    """
       
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    load_dim_tables(cur,conn)
    load_fact_data(cur, conn)
    load_date_dim(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
