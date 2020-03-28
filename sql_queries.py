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

# CREATE TABLES
# Staging tables to hold the source data as is
immi_stg_create= ("""CREATE TABLE IF NOT EXISTS Immigrant_stage(
    cicid BIGINT, i94yr INTEGER, i94mon INTEGER, i94cit INTEGER,
    i94res INTEGER,  i94port VARCHAR, arrdate INTEGER, i94mode INTEGER,
    i94addr VARCHAR, depdate INTEGER, i94bir INTEGER,  i94visa INTEGER,
    count INTEGER, dtadfile INTEGER, visapost VARCHAR, occup VARCHAR,
    entdepa VARCHAR, entdepd VARCHAR, entdepu VARCHAR, matflag VARCHAR,
    biryear INTEGER, dtaddto INTEGER, gender VARCHAR, insnum BIGINT,
    airline VARCHAR,  admnum BIGINT, fltno VARCHAR, visatype VARCHAR)""")

# Fact table
immi_fact_create = ("""CREATE TABLE IF NOT EXISTS Immigrant_Fact(
    CICID BIGINT, I94YR INTEGER, I94MON INTEGER, I94CIT INTEGER, I94RES INTEGER,
    I94PORT VARCHAR, ARRDATE INTEGER, I94MODE INTEGER, I94ADDR VARCHAR,
    DEPDATE INTEGER, I94BIR INTEGER, I94VISA INTEGER, COUNT INTEGER,
    DTADFILE BIGINT, VISAPOST VARCHAR, ENTDEPA VARCHAR, ENTDEPD VARCHAR,
    ENTDEPU VARCHAR, MATFLAG VARCHAR, BIRYEAR INTEGER, DTADDTO BIGINT,
    GENDER VARCHAR, AIRLINE VARCHAR, ADMNUM DECIMAL, FLTNO INTEGER,
    VISATYPE VARCHAR, PRIMARY KEY(CICID)
)""")

# Dimension tables
date_dim_create = ("""CREATE TABLE IF NOT EXISTS Date_Dim(calendar_date TIMESTAMP, date_id INTEGER, 
     day INTEGER, month INTEGER, year INTEGER, weekday INTEGER, PRIMARY KEY(calendar_date))
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

visa_dim_create = ("""CREATE TABLE IF NOT EXISTS VISA_TYPE_DIM(
    VISA_CODE INTEGER,
    VISA_TYPE VARCHAR,
    PRIMARY KEY(VISA_TYPE)
)""")


port_dim_create = ("""CREATE TABLE IF NOT EXISTS PORT_DIM(
    PORT_CODE VARCHAR,
    PORT_NAME VARCHAR,
    PRIMARY KEY(PORT_CODE)
)""")


addr_state_dim_create = ("""CREATE TABLE IF NOT EXISTS addr_state_dim(
    STATE_CODE VARCHAR,
    STATE VARCHAR,
    PRIMARY KEY(STATE_CODE)
)""")

res_city_cntry_create = ("""CREATE TABLE IF NOT EXISTS Res_City_Country_dim(
    RES_CODE VARCHAR,
    RES_STATE VARCHAR,
    PRIMARY KEY(RES_CODE)
)""")


# STAGING TABLES

immigrant_stage_copy = ("""copy Immigrant_stage from {}
    credentials 'aws_iam_role={}' 
    format parquet
    compupdate off region 'us-west-2';
    """).format(config.get('S3','SAS_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
immigrant_fact_insert = ("""INSERT INTO immigrant_fact(CICID, I94YR, I94MON,I94CIT,I94RES, I94PORT, ARRDATE,
                            I94MODE, I94ADDR, DEPDATE, I94BIR, I94VISA, COUNT, DTADFILE, VISAPOST, ENTDEPA,
                            ENTDEPD, ENTDEPU, MATFLAG, BIRYEAR, DTADDTO, GENDER, AIRLINE, ADMNUM, FLTNO, VISATYPE 
                        ) SELECT DISTINCT CICID, I94YR, I94MON,I94CIT,I94RES, I94PORT, ARRDATE,
                            I94MODE, I94ADDR, DEPDATE, I94BIR, I94VISA, COUNT, DTADFILE, VISAPOST, ENTDEPA,
                            ENTDEPD, ENTDEPU, MATFLAG, BIRYEAR, DTADDTO, GENDER, AIRLINE, ADMNUM, FLTNO, VISATYPE FROM immigrant_stage
                            WHERE INSNUM is null
                            ON CONFLICT(CICID) 
                            DO NOTHING
""")

demographic_dim_insert = ("""INSERT INTO CITY_STATE_DEMOGRAPHIC_DIM(CITY, STATE_CODE,STATE,MEDIAN_AGE, MALE_POP,FEMALE_POP, TOTAL_POP,
                    NUM_VETERANS, FOREIGN_BORN, AVG_HOUSEHOLD_SIZE, RACE, COUNT
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    ON CONFLICT(CITY, STATE_CODE) DO UPDATE
                        SET MEDIAN_AGE = EXCLUDED.MEDIAN_AGE,
                            MALE_POP = EXCLUDED.MALE_POP,
                            FEMALE_POP = EXCLUDED.FEMALE_POP,
                            TOTAL_POP = EXCLUDED.TOTAL_POP,
                            NUM_VETERANS = EXCLUDED.NUM_VETERANS,
                            FOREIGN_BORN = EXCLUDED.FOREIGN_BORN,
                            AVG_HOUSEHOLD_SIZE = EXCLUDED.AVG_HOUSEHOLD_SIZE,
                            RACE = EXCLUDED.RACE,
                            COUNT = EXCLUDED.COUNT
""")

visa_type_dim_insert = ("""INSERT INTO VISA_TYPE_DIM(VISA_CODE, VISA_TYPE) VALUES(%s, %s) ON CONFLICT(VISA_TYPE) 
                            DO NOTHING
""")

port_dim_insert = ("""INSERT INTO PORT_DIM(PORT_CODE, PORT_NAME) VALUES(%s, %s) ON CONFLICT(PORT_CODE) 
                            DO NOTHING
""")

addr_state_dim_insert = ("""INSERT INTO addr_state_dim(state_code, state) VALUES(%s, %s) ON CONFLICT(state_code) 
                            DO NOTHING
""")

res_city_country_insert = ("""INSERT INTO Res_City_Country_dim(RES_CODE, RES_STATE) VALUES(%s, %s) ON CONFLICT(RES_CODE) 
                            DO NOTHING
""")

# QUERY LISTS

create_table_queries = [immi_stg_create, immi_fact_create, date_dim_create, demo_dim_create, visa_dim_create, port_dim_create, addr_state_dim_create,res_city_cntry_create]
drop_table_queries = [immi_stage_drop, immi_fact_drop, date_dim_drop, demo_dim_drop, visa_dim_drop, port_dim_drop, addr_state_dim_drop,res_city_cntry_drop]
copy_table_queries = [immigrant_stage_copy]
insert_table_queries = [demographic_dim_insert, visa_type_dim_insert, port_dim_insert, addr_state_dim_insert, res_city_country_insert,immigrant_fact_insert]
