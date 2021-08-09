# Importing important libraries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Description: This function will perform the following operations: 
    - Connect to the redshift database.
    - Run the queries for copying data from S3 to redshift.
    - Load data in the staging tables in redshift cluster.
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
        Discription: This function will run the queries (sql_queries.py) for inserting data from Staging tables to the fact and  
        dimension tables in redshift.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """ 
        - Discription: This function will read CLUSTER credentials from 'dwh.cfg' file, connect to the database.
        - Assign a cursor to execute the queries, pass arguments in functions load_staging_tables and insert_tables.
        - Close conncetion to the database.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

   
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
   
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    
    conn.close()


if __name__ == "__main__":
    main()