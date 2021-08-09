# Import libraries and scripts.
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
      Description: This function will drop tables if exists in the database before creating them.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
      Description: This function will create tables in redshift cluster database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """ 
        - Discription: This function will read CLUSTER credentials from 'dwh.cfg' file.
        - Make connections to the database and assign a cursor to execute the queries, pass arguments in functions 
          load_staging_tables and insert_tables.
        - Close connection to the database.
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