import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries,drop_stage_queries,create_stage_queries


def drop_tables(cur, conn):
    """
    This function goes through the drop queries in sql_queries and executes them against the database. 
    Tables are dropped if they already exist to prevent the program from throwing an error.
    Parameters: This function requires connection and connection cursor as parameters 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    for query in drop_stage_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    This function goes through the DDL create queries in sql_queries and executes them against the database.
    Parameters: This function requires connection and connection cursor as parameters
    """
    for query in create_stage_queries:
        cur.execute(query)
        conn.commit()
        
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



def main():
    """
    Connect to the database, create a connection cursor
    and call functions to drop and create tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()