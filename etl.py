"""
Usage:
    This is a simple etl for looping through source data directory to dynamically perform
    drop, creation and insertion into local SQLite database
Author:
    traanhn - 2021.09.22
"""

# import from standard libraries
import sqlite3  # Using SQLite db for the easy set up and light-weight testing
import glob
import csv
# import from 3rd party libraries
# import from my source

data_path = "./data/"


def db_connection():
    """
    Create and connect to the local SQLite db
    :return: conn and cursor
    """
    try:
        conn = sqlite3.connect('SQLite.db')
        cursor = conn.cursor()

    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)

    return cursor, conn


def drop_and_create_tables_queries(schema_name):
    """
    Create sql tables from files in the data directory
    sql tables have the same name as the csv filename
    :return: array of queries for tables creation
    """
    table_creation_queries = []
    table_drop_queries = []
    for file_name in glob.glob(data_path + "*.csv"):
        # Extract table name from csv file names
        table_name = file_name.replace(data_path, "").replace(".csv", "")
        # Extract column names from corresponding file names
        column_names = open(file_name, "r", encoding="utf-8-sig").readline().strip().split(",")
        # Forming the single query for dropping table dynamically
        query_table_drop = "DROP TABLE IF EXISTS " + schema_name + '.'+table_name
        # Forming the single query for creating table dynamically
        query_table_creation = 'CREATE TABLE IF NOT EXISTS ' + schema_name + '.' + table_name + "("
        for column in column_names:
            query_table_creation += column + " VARCHAR(64),\n"
        query_table_creation = query_table_creation[:-2]
        query_table_creation += ");"
        # Append the drop and creation queries into the arrays
        table_creation_queries.append(query_table_creation)
        table_drop_queries.append(query_table_drop)

    return table_drop_queries, table_creation_queries


def insertion_queries_execution(cur, conn):
    """
    Execute the data insertion from the files into corresponding tables in db
    :param cur: cursor
    :param conn: connection to db
    """
    for file_name in glob.glob(data_path + "*.csv"):
        table_name = file_name.replace(data_path, "").replace(".csv", "")
        with open(file_name, 'r') as f:
            reader = csv.reader(f)
            columns = next(reader)
            # Forming the insertion query
            insert_query = 'INSERT INTO ' + table_name + ' VALUES ({1})'
            insert_query = insert_query.format(','.join(columns), ','.join('?' * len(columns)))
            for data in reader:
                cur.execute(insert_query, data)
            conn.commit()


def query_execution(cur, conn, queries):
    """
    Execute the inserted sql query
    :param cur: cursor
    :param conn: connection
    :param queries: array of queries
    :return:
    """
    for query in queries:
        cur.executescript(query)
    conn.commit()


def main():
    cur, conn = db_connection()
    schema_name = 'main'
    table_drop_queries, table_creation_queries = drop_and_create_tables_queries(schema_name)
    # Drop tables
    query_execution(cur, conn, table_drop_queries)
    # Create tables
    query_execution(cur, conn, table_creation_queries)
    # Insert data from source files
    insertion_queries_execution(cur, conn)
    # Close the cursor
    cur.close()


if __name__ == '__main__':
    main()
