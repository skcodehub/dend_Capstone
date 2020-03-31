import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
immi_stage_drop = "DROP TABLE IF EXISTS Immigrant_stage"
immi_fact_drop = "DROP TABLE IF EXISTS Immigrant_Fact"

date_dim_drop = "DROP TABLE IF EXISTS Date_Dim"
demo_dim_drop = "DROP TABLE IF EXISTS CITY_STATE_DEMOGRAPHIC_DIM"
visa_dim_drop = "DROP TABLE IF EXISTS VISA_TYPE_DIM"
port_dim_drop = "DROP TABLE IF EXISTS PORT_DIM"
addr_state_dim_drop = "DROP TABLE IF EXISTS addr_state_dim"
res_city_cntry_drop = "DROP TABLE IF EXISTS Res_City_Country_dim"

demo_stage_drop="DROP TABLE IF EXISTS city_state_demographic_stg"
visa_stage_drop="DROP TABLE IF EXISTS visa_stg"
port_stage_drop="DROP TABLE IF EXISTS port_stg"
addr_state_stage_drop="DROP TABLE IF EXISTS addr_state_stg"
res_city_cntry_stg_drop="DROP TABLE IF EXISTS res_city_country_stg"


# CREATE TABLES
# Staging tables to hold the source data as is
immi_stg_create= ("""CREATE TABLE IF NOT EXISTS immigrant_stage(
    cicid DOUBLE PRECISION, i94yr DOUBLE PRECISION, i94mon DOUBLE PRECISION, i94cit DOUBLE PRECISION,
    i94res DOUBLE PRECISION,  i94port VARCHAR, arrdate DOUBLE PRECISION, i94mode DOUBLE PRECISION,
    i94addr VARCHAR, depdate DOUBLE PRECISION, i94bir DOUBLE PRECISION,  i94visa DOUBLE PRECISION,
    count DOUBLE PRECISION, dtadfile VARCHAR, visapost VARCHAR, occup VARCHAR,
    entdepa VARCHAR, entdepd VARCHAR, entdepu VARCHAR, matflag VARCHAR,
    biryear DOUBLE PRECISION, dtaddto VARCHAR, gender VARCHAR, insnum VARCHAR,
    airline VARCHAR,  admnum DOUBLE PRECISION, fltno VARCHAR, visatype VARCHAR)""")

# Fact table
immi_fact_create = ("""CREATE TABLE IF NOT EXISTS Immigrant_Fact(
    cicid DOUBLE PRECISION, i94yr DOUBLE PRECISION, i94mon DOUBLE PRECISION, i94cit DOUBLE PRECISION,
    i94res DOUBLE PRECISION, I94PORT VARCHAR, arrdate DOUBLE PRECISION, i94mode DOUBLE PRECISION, I94ADDR VARCHAR,
    depdate DOUBLE PRECISION, i94bir DOUBLE PRECISION,  i94visa DOUBLE PRECISION,
    count DOUBLE PRECISION, DTADFILE TIMESTAMP, VISAPOST VARCHAR, ENTDEPA VARCHAR, ENTDEPD VARCHAR,
    ENTDEPU VARCHAR, MATFLAG VARCHAR, BIRYEAR INTEGER, DTADDTO TIMESTAMP,
    GENDER VARCHAR, AIRLINE VARCHAR, ADMNUM DECIMAL, FLTNO VARCHAR,
    VISATYPE VARCHAR, PRIMARY KEY(CICID)
)""")

# Dimension tables
date_dim_create = ("""CREATE TABLE IF NOT EXISTS date_dim(calendar_date date, date_id INTEGER, 
    day INTEGER, month INTEGER, year INTEGER, weekday INTEGER, PRIMARY KEY(calendar_date))
""")
#date_dim_create = (""" create table date_dim(
#          date_id integer not null distkey sortkey,
#          calendar_date date not null,
#          year smallint encode delta not null,
#          month smallint encode bytedict  not null,
#          month_name char(3) encode bytedict not null,
#          day_of_mon smallint encode bytedict not null,
#          day_of_week_num smallint encode bytedict not null,
#          day_of_week varchar(9) encode bytedict not null,
#          is_weekend boolean
#    );
#""")

demo_stage_create = ("""CREATE TABLE IF NOT EXISTS city_state_demographic_stg(
    CITY VARCHAR,
    STATE VARCHAR,
    MEDIAN_AGE DOUBLE PRECISION,
    MALE_POP DOUBLE PRECISION,
    FEMALE_POP DOUBLE PRECISION,
    TOTAL_POP DOUBLE PRECISION,
    NUM_VETERANS DOUBLE PRECISION,
    FOREIGN_BORN DOUBLE PRECISION,
    AVG_HOUSEHOLD_SIZE DOUBLE PRECISION,
    STATE_CODE VARCHAR,
    RACE VARCHAR,
    COUNT DOUBLE PRECISION
)
""")

demo_dim_create = ("""CREATE TABLE IF NOT EXISTS CITY_STATE_DEMOGRAPHIC_DIM(
    CITY VARCHAR,
    STATE_CODE VARCHAR,
    STATE VARCHAR,
    MEDIAN_AGE NUMERIC(7,4),
    MALE_POP NUMERIC(20,4),
    FEMALE_POP NUMERIC(20,4),
    TOTAL_POP NUMERIC(20,4),
    NUM_VETERANS NUMERIC(20,4),
    FOREIGN_BORN NUMERIC(20,4),
    AVG_HOUSEHOLD_SIZE NUMERIC(7,3),
    RACE VARCHAR,
    COUNT NUMERIC(20,4),
    PRIMARY KEY(CITY, STATE_CODE)
)
""")

visa_stage_create = ("""CREATE TABLE IF NOT EXISTS visa_stg(
    VISA_CODE INTEGER,
    VISA_TYPE VARCHAR
)""")

visa_dim_create = ("""CREATE TABLE IF NOT EXISTS VISA_TYPE_DIM(
    VISA_CODE INTEGER,
    VISA_TYPE VARCHAR,
    PRIMARY KEY(VISA_TYPE)
)""")

port_stage_create = ("""CREATE TABLE IF NOT EXISTS port_stg(
    PORT_CODE VARCHAR,
    PORT_NAME VARCHAR
)""")


port_dim_create = ("""CREATE TABLE IF NOT EXISTS port_dim(
    PORT_CODE VARCHAR,
    PORT_NAME VARCHAR,
    PRIMARY KEY(PORT_CODE)
)""")


addr_state_stage_create = ("""CREATE TABLE IF NOT EXISTS addr_state_stg(
    STATE_CODE VARCHAR,
    STATE VARCHAR
)""")

addr_state_dim_create = ("""CREATE TABLE IF NOT EXISTS addr_state_dim(
    STATE_CODE VARCHAR,
    STATE VARCHAR,
    PRIMARY KEY(STATE_CODE)
)""")

res_city_cntry_stage_create = ("""CREATE TABLE IF NOT EXISTS res_city_country_stg(
    RES_CODE VARCHAR,
    RES_STATE VARCHAR
)""")

res_city_cntry_create = ("""CREATE TABLE IF NOT EXISTS res_city_country_dim(
    RES_CODE VARCHAR,
    RES_STATE VARCHAR,
    PRIMARY KEY(RES_CODE)
)""")


# STAGING TABLES
#IAM_ROLE
#'s3://capstoneimmi/sas-data/'
immigrant_stage_copy = ("""
                TRUNCATE TABLE immigrant_stage;
                COPY immigrant_stage FROM '{}'
                   IAM_ROLE '{}'
                  FORMAT parquet;
    """).format(config['S3']['SAS_DATA'], config['IAM_ROLE']['ARN'])

#'s3://capstoneimmi/demo/'
demo_stage_copy= ("""
                    TRUNCATE TABLE city_state_demographic_stg;
                    COPY city_state_demographic_stg FROM '{}'
                       IAM_ROLE '{}'
                      FORMAT csv
                      IGNOREHEADER 1;
    """).format(config['S3']['DEMOGR_DATA'], config['IAM_ROLE']['ARN'])

#'s3://capstoneimmi/visa/'
visa_stage_copy= ("""
                    TRUNCATE TABLE visa_stg;
                    COPY visa_stg FROM '{}'
                       IAM_ROLE '{}'
                      FORMAT csv
                      IGNOREHEADER 1;
    """).format(config['S3']['VISA_DATA'],config['IAM_ROLE']['ARN'])

#'s3://capstoneimmi/port/'
port_stage_copy= ("""
                    TRUNCATE TABLE port_stg;
                    COPY port_stg FROM '{}'
                       IAM_ROLE '{}'
                      FORMAT csv
                      IGNOREHEADER 1
                      DELIMITER ';';
        """).format(config['S3']['PORT_DATA'],config['IAM_ROLE']['ARN'])

# 's3://capstoneimmi/addrstate/'
addr_state_stage_copy=("""
                    TRUNCATE TABLE addr_state_stg;
                    COPY addr_state_stg FROM '{}'
                       IAM_ROLE '{}'
                      FORMAT csv
                      IGNOREHEADER 1;
        """).format(config['S3']['ADDR_ST_DATA'],config['IAM_ROLE']['ARN'])

#'s3://capstoneimmi/rescitycntry/'
res_city_cntry_stage_copy=("""
                    TRUNCATE TABLE res_city_country_stg;
                    COPY res_city_country_stg FROM '{}'
                       IAM_ROLE '{}'
                      FORMAT csv
                      IGNOREHEADER 1
                      DELIMITER ';';
        """).format(config['S3']['RES_CNTRY_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
immigrant_fact_insert = ("""
                        DELETE FROM immigrant_fact WHERE cicid in 
                                (select immigrant_fact.cicid from immigrant_fact, immigrant_stage
                                    where immigrant_fact.cicid = immigrant_stage.cicid);
                        INSERT INTO immigrant_fact(CICID, I94YR, I94MON, I94CIT,I94RES, I94PORT, ARRDATE,
                            I94MODE, I94ADDR, DEPDATE, I94BIR, I94VISA, COUNT, DTADFILE, VISAPOST, ENTDEPA,
                            ENTDEPD, ENTDEPU, MATFLAG, BIRYEAR, DTADDTO, GENDER, AIRLINE, ADMNUM, FLTNO, VISATYPE 
                        ) SELECT DISTINCT CICID, I94YR, I94MON, I94CIT,I94RES, I94PORT,ARRDATE,I94MODE, 
                            I94ADDR, DEPDATE, I94BIR, I94VISA, COUNT, 
                            TO_DATE(DTADFILE, 'yyyymmdd'),
                            VISAPOST, ENTDEPA, ENTDEPD, ENTDEPU, MATFLAG, BIRYEAR, 
                            CASE WHEN dtaddto IS NULL 
                            THEN NULL 
                            ELSE TO_DATE(dtaddto, 'mmddyyyy') END as dtaddto, GENDER, AIRLINE, ADMNUM, FLTNO, VISATYPE
                            FROM immigrant_stage
""")

demographic_dim_insert = ("""
                    TRUNCATE TABLE city_state_demographic_dim;
                    INSERT INTO CITY_STATE_DEMOGRAPHIC_DIM SELECT CITY, STATE_CODE,STATE,MEDIAN_AGE, MALE_POP,FEMALE_POP, TOTAL_POP,
                                NUM_VETERANS, FOREIGN_BORN, AVG_HOUSEHOLD_SIZE, RACE, COUNT FROM  city_state_demographic_stg ;
""")

visa_type_dim_insert = ("""
                    TRUNCATE TABLE visa_type_dim;
                    INSERT INTO VISA_TYPE_DIM(VISA_CODE, VISA_TYPE) SELECT VISA_CODE, VISA_TYPE FROM visa_stg;
                    """)

port_dim_insert = ("""
                TRUNCATE TABLE port_dim;
                INSERT INTO PORT_DIM(PORT_CODE, PORT_NAME) SELECT  PORT_CODE, PORT_NAME FROM port_stg ;
                """)

addr_state_dim_insert = ("""
                    TRUNCATE TABLE addr_state_dim;
                    INSERT INTO addr_state_dim(state_code, state) 
                    SELECT  substring(state_code, 2, 2) as state_code, 
                    substring(state, 2, length(state)-2) as state
                    from addr_state_stg;
                    """)

res_city_country_insert = ("""
                        TRUNCATE TABLE res_city_country_dim;
                        INSERT INTO res_city_country_dim(res_code, res_state) SELECT res_code, res_state FROM res_city_country_stg ;
                    """)

#date_dim_insert = ("""
#                     TRUNCATE TABLE date_dim;
#                    insert into date_dim
#                        select datediff(day, '1960-01-01', dat) as date_id,
#                        dat as calendar_date,
#                        date_part(year,dat) as year,
#                        date_part(mm,dat) as month,
#                        to_char(dat, 'Mon') as month_name,
#                        date_part(day,dat) as day_of_mon,
#                        date_part(dow,dat) as day_of_week_num,
#                        to_char(dat, 'Day') as day_of_week,
#                        decode(date_part(dow,dat),0,true,6,true,false) as is_weekend
#                    from
#                    (select
#                        trunc(dateadd(day, ROW_NUMBER () over ()-1, '1900-01-01')) as dat
#                        from immigrant_stage
#                    );
#                """)

# QUERY LISTS

drop_table_queries = [immi_fact_drop,  date_dim_drop, demo_dim_drop, visa_dim_drop, port_dim_drop, addr_state_dim_drop,res_city_cntry_drop]
drop_stage_queries =[immi_stage_drop,demo_stage_drop,visa_stage_drop,port_stage_drop,addr_state_stage_drop,res_city_cntry_stg_drop]

create_stage_queries=[immi_stg_create, demo_stage_create,visa_stage_create,port_stage_create,addr_state_stage_create,res_city_cntry_stage_create]
create_table_queries = [immi_fact_create,date_dim_create,demo_dim_create,visa_dim_create, port_dim_create,addr_state_dim_create,res_city_cntry_create]

copy_table_queries = [immigrant_stage_copy,demo_stage_copy,visa_stage_copy,addr_state_stage_copy,res_city_cntry_stage_copy,port_stage_copy]
insert_table_queries = [immigrant_fact_insert]

dim_table_queries=[ demographic_dim_insert,visa_type_dim_insert,port_dim_insert,res_city_country_insert,addr_state_dim_insert]