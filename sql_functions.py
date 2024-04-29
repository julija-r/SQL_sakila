import mysql.connector

hostname = "localhost"
username = "root"
password = "Sharabanas1"
database = "sakila"



def execute_mysql_query(hostname, port, username, password, database, query, column_names):
    try:
        connection = mysql.connector.connect(
            host=hostname, port=port, user=username, password=password, database=database
        )
        print("Connection successful!")

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        table_data = []  # List to store the table data

        for row in results:
            row_data = {}
            for col, val in zip(column_names, row):
                row_data[col] = val
            table_data.append(row_data)

        return table_data

    except mysql.connector.Error as err:
        print(f"Connection error: {err}")

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
            print("Connection closed.")