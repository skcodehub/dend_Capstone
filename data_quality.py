import configparser
import psycopg2
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import os
import logging

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['keypair']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['keypair']['AWS_SECRET_ACCESS_KEY']

def check_dim_data_quality(cur, conn):
    """
        Perform data quality checks against the data loaded into dimension tables
        Takes 2 input arguments: connection cursor and connection
    """
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('****Checking dimension data quality****')
    
    #addr_state_dim
    stg_query = "select count(*) from addr_state_stg"
    dw_query = "select count(*) from addr_state_dim"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("Addr stg_rows: multiple rows returned for count of table")
   
    cur.execute(dw_query)
    dw_rows = cur.fetchall()
    if(len(dw_rows) > 1):
        logging.warn("Addr dw_rows: multiple rows returned for count of table")
    
    if(stg_rows[0] == dw_rows[0]):
        logging.info("ADDR_STATE_STG to ADDR_STATE_DIM counts match, check passed")
    else:
        logging.info("ADDR_STATE_STG to ADDR_STATE_DIM counts do not match, check failed")
   
    #city_state_demographic_dim
    stg_query = "select count(*) from city_state_demographic_stg"
    dw_query = "select count(*) from city_state_demographic_dim"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("CITY_STATE_DEMOGRAPHIC_STG stg_rows: multiple rows returned for count of table")
   
    cur.execute(dw_query)
    dw_rows = cur.fetchall()
    if(len(dw_rows) > 1):
        logging.warn("CITY_STATE_DEMOGRAPHIC_DIM dw_rows: multiple rows returned for count of table")
    
    if(stg_rows[0] == dw_rows[0]):
        logging.info("CITY_STATE_DEMOGRAPHIC_STG to CITY_STATE_DEMOGRAPHIC_DIM counts match, check passed")
    else:
        logging.warn("CITY_STATE_DEMOGRAPHIC_STG to CITY_STATE_DEMOGRAPHIC_DIM counts do not match, check failed")
   
    #Port_Dim
    stg_query = "select count(*) from port_stg"
    dw_query = "select count(*) from port_dim"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("PORT_STG stg_rows: multiple rows returned for count of table")
   
    cur.execute(dw_query)
    dw_rows = cur.fetchall()
    if(len(dw_rows) > 1):
        logging.warn("PORT_DIM dw_rows: multiple rows returned for count of table")
    
    if(stg_rows[0] == dw_rows[0]):
        logging.info("PORT_STG to PORT_DIM counts match, check passed")
    else:
        logging.warn("PORT_STG to PORT_DIM counts do not match, check failed")
    
    #Visa_Type_Dim
    stg_query = "select count(*) from visa_stg"
    dw_query = "select count(*) from Visa_Type_Dim"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("VISA_STG stg_rows: multiple rows returned for count of table")
   
    cur.execute(dw_query)
    dw_rows = cur.fetchall()
    if(len(dw_rows) > 1):
        logging.warn("VISA_TYPE_DIM dw_rows: multiple rows returned for count of table")
    
    if(stg_rows[0] == dw_rows[0]):
        logging.info("VISA_STG to VISA_TYPE_DIM counts match, check passed")
    else:
        logging.warn("VISA_STG to VISA_TYPE_DIM counts do not match, check failed")
    
    #res_city_country_dim
    stg_query = "select count(*) from res_city_country_stg"
    dw_query = "select count(*) from res_city_country_dim"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("RES_CITY_COUNTRY_STG stg_rows: multiple rows returned for count of table")
   
    cur.execute(dw_query)
    dw_rows = cur.fetchall()
    if(len(dw_rows) > 1):
        logging.warn("RES_CITY_COUNTRY_DIM dw_rows: multiple rows returned for count of table")
    
    if(stg_rows[0] == dw_rows[0]):
        logging.info("RES_CITY_COUNTRY_STG to RES_CITY_COUNTRY_DIM counts match, check passed")
    else:
        logging.warn("RES_CITY_COUNTRY_STG to RES_CITY_COUNTRY_DIM counts do not match, check failed")
        
def check_fact_data_quality(cur, conn):
    """
        Perform data quality checks against the data loaded into fact table
        Takes 2 input arguments: connection cursor and connection
    """
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('****Checking fact data quality****')
    
    #immigrant_fact
    stg_query = "select count(*) from immigrant_stage"
    dw_query = "select count(*) from immigrant_fact"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("immigrant_stg stg_rows: multiple rows returned for count of table")
   
    cur.execute(dw_query)
    dw_rows = cur.fetchall()
    if(len(dw_rows) > 1):
        logging.warn("immigrant_fact dw_rows: multiple rows returned for count of table")
    
    if(stg_rows[0] == dw_rows[0]):
        logging.info("IMMIGRANT_FACT to IMMIGRANT_STAGE counts match, check passed")
    else:
        logging.info("IMMIGRANT_FACT to IMMIGRANT_STAGE counts do not match, check failed")
    
    #immigrant_fact check uniqueness of cicid
    stg_query = "select cicid, count(*) from immigrant_stage group by cicid having count(*) > 1"
    
    cur.execute(stg_query)
    stg_rows = cur.fetchall()
    if(len(stg_rows) > 1):
        logging.warn("IMMIGRANT_STAGE CICID is not unique")
    else:
        logging.info("IMMIGRANT_STAGE CICID is unique")

def main():
    """
    Connect to the database and call functions check quality of data
    Reads the configuration params from the config file.
    Parameters: This function requires connection and connection cursor as parameters.
    """
       
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    check_dim_data_quality(cur, conn)
    check_fact_data_quality(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()