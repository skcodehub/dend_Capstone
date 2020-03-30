# Project Description:

##### This project loads SAS_Data containing immigrant information along with relevant fact and dimension into Redshift.  
##### It creates six dimensions and a fact. Five dimensions are created using the information provided in I94_SAS_Labels_Descriptions.SAS file
##### and one dimension (date_dim) is seeded using SAS epoch date. The fact table is created after creating SAS data in parquet format, cleansing it and loading it to S3.
##### These files can be used so that analysis can be conducted on the data for number of arrivals per month, peak months for visitors, etc.

###### The following file folders are created after the files are loaded to S3
###### PARENT_BUCKET='capstoneimmi'
###### SAS_DATA=s3a://capstoneimmi/sas-data-parquet/
###### DEMOGR_DATA=s3://capstoneimmi/demo/
###### VISA_DATA=s3://capstoneimmi/visa/
###### PORT_DATA=s3://capstoneimmi/port/
###### ADDR_ST_DATA=s3://capstoneimmi/addrstate/
###### RES_CNTRY_DATA=s3://capstoneimmi/rescitycntry/

###### sql_queries.py
###### This script contains the queries needed to
######    1. drop and create stage, dimension and fact tables, 
######    2. copy data from S3 to Redshift stage tables
######    3. insert data into dimension and fact tables

###### create_tables.py
###### This script creates stage, dimension and fact tables in Redshift

###### pre_etl.py
###### This script can be used to copy cleansed files from local to S3 bucket. Running this script is optional if data is already available in S3.

#### etl.py
###### This scirpt creates connection handler and cursor to the database and call functions to
###### load staging tables
###### load dimension tables
###### load fact data
###### load date dimension

#### Requirement:
###### Python2 or later, Spark, Redshift, S3

#### Usage:
#### Command Line:
###### python3 -m create_tables
###### python3 -m pre_etl
###### python3 -m etl
###### python3 -m data_quality
