import mysql.connector
import pandas as pd

def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            user='root',
            password='<password>',
            host='127.0.0.1',
            port='3306',
            database='test_db'
        )
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

def load_third_party(connection, file_path_csv):
    print("Loading 3rd party data file (" + file_path_csv + ") into " + connection.database + " database.")
    cursor = connection.cursor()
    third_party_data = pd.read_csv(file_path_csv, header=None)
    # Iterate through the CSV file and execute insert statement; Ignore data that already exists in the table
    for _, row in third_party_data.iterrows():
        sql = """INSERT IGNORE INTO Sales (`ticket_id`,`trans_date`,`event_id`,`event_name`,`event_date`,`event_type`,`event_city`,`customer_id`,`price`,`num_tickets`) 
            VALUES(""" + """%s,"""*(len(row)-1) + """%s)"""
        print("Inserting row #" + str(row[0]) + " into the Sales table")
        cursor.execute(sql, tuple(row))
    connection.commit()
    cursor.close()
    return

def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    sql_statement = """ SELECT event_name
                        FROM Sales
                        GROUP BY event_name
                        ORDER BY COUNT(*) DESC, event_id
                        LIMIT 3;"""
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = list(cursor.fetchall())
    cursor.close()
    return records

# Given a set of records, return a user-friendly list of the most popular tickets
def show_popular_tix(records):
    print("Here are the most popular tickets in the past month: ")
    for row in records:
        print(" - " + row[0])

# Load the 3rd party csv file into the Sales table
load_third_party(get_db_connection(), 'C:/GitRepos/Springboard/DataPipeline_MiniProject/third_party_sales_1.csv')

# Query the Sales table and format the output for the user
show_popular_tix(records=query_popular_tickets(get_db_connection()))