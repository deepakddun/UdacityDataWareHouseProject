import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f'Executing query {query} \n')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f'Executing query {query} \n')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect(
        host=config.get('CLUSTER', 'HOST'),
        database=config.get('CLUSTER', 'DB_NAME'),
        user=config.get('CLUSTER', 'DB_USER'),
        password=config.get('CLUSTER', 'DB_PASSWORD'),
        port=config.get('CLUSTER', 'DB_PORT')
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()