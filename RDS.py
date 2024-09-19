"""

@Author:Suresh
@Date:19-09-2024
@Last Modified By:Suresh
@Last Modified Date:19-09-2024
@Title: AWS RDS CRUD operations using Boto3 with Python and MSSQL

"""


import boto3
import pyodbc 
import os
import pandas as pd
from dotenv import load_dotenv


def describe_rds_instances():
    client = boto3.client('rds', region_name='us-east-2')
    response = client.describe_db_instances()
    for instance in response['DBInstances']:
        print(f"Instance ID: {instance['DBInstanceIdentifier']}")
        print(f"Endpoint: {instance['Endpoint']['Address']}")
        print(f"Port: {instance['Endpoint']['Port']}")
        print()
    return response


load_dotenv()


SERVER = os.getenv("SERVER")
db_name = os.getenv("db_name")
UID = os.getenv("UID")
PASSWD = os.getenv("PASSWD")
AWS_ACC_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SEC_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def check_connection():
    connection_str = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={db_name};UID={UID};PWD={PASSWD}'
    
    try:
        conn = pyodbc.connect(connection_str)
        print("Connection successful!")
        return conn
    except pyodbc.Error as e:
        print(f"Connection error: {e}")
        return None


def conn_cursor(conn):
    if conn:
        cursor = conn.cursor()
        return cursor
    return None


def create_table(conn, cursor):
    
    query = 'CREATE TABLE employee(Empid int PRIMARY KEY, Ename VARCHAR(200))'
    cursor.execute(query)
    conn.commit()


def insert_details(conn, cursor, Empid,Ename):
      
    query = 'INSERT INTO Employee (Empid, Ename) VALUES (?,?)'
    response = cursor.execute(query, (Empid, Ename))
    conn.commit()
    return response


def update_details(conn, cursor):

    query = "UPDATE Employee SET company = 'Apexon' WHERE Empid = 1"
    response = cursor.execute(query)
    conn.commit()
    return response


def delete_record(conn,cursor):

    query ='Delete FROM Employee WHERE Empid=2'
    response = cursor.execute(query)
    conn.commit()
    return response


def add_column(conn,cursor):

    query = 'ALTER TABLE Employee ADD company VARCHAR(200)'
    response= cursor.execute(query)
    conn.commit()
    return response


def delete_table(conn,cursor):

    query = 'Drop TABLE Employee'
    response = cursor.execute(query)
    conn.commit()
    return response


def import_data(conn,cursor, csv_file_path):
    
    df = pd.read_csv(csv_file_path)
    for index, row in df.iterrows():
        res = insert_details(conn,cursor, row['Empid'], row['Ename'])
        print("Row inserted")
    
    print("Data is imported successfully")


def export_data(cursor, csv_file_path):

    query = 'SELECT * FROM employee'
    df = pd.read_sql(query, cursor.connection)
    df.to_csv(csv_file_path, index=False)
    print(f"Data exported to {csv_file_path} successfully.")


def main():

    describe_rds_instances()
    connection = check_connection()
    
    if connection:
        cursor = conn_cursor(connection)

        if cursor:
            print("Cursor created successfully.")

            create_table(connection, cursor)

            respo = insert_details(connection, cursor)
            print(f"Data inserted successfully {respo}")

            respo = update_details(connection, cursor)
            print(f"Data Updated Successfully {respo}")

            respo = delete_record(connection,cursor)
            print(f"Record deleted Successfully {respo}")

            respo = add_column(connection,cursor)
            print(f"Company column added Successfully {respo}")

            respo = delete_table(connection,cursor)
            print(f"Table deleted Successfully {respo}")

            csv_file_path = csv_file_path = r'C:\Users\Suresh\Desktop\pythonBl\AWS\RDS.csv.txt'
            import_data(connection,cursor, csv_file_path)

            export_data(cursor, 'exported_data.csv')  


        connection.close()
        print("Connection closed.")
        

if __name__ == '__main__':
    main()
