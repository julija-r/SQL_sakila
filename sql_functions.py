import mysql.connector
import pandas
import csv

#function to accesss data from MySQL
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
        df = pandas.DataFrame(table_data, columns=column_names)

        return df

    except mysql.connector.Error as err:
        print(f"Connection error: {err}")

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
            print("Connection closed.")

#function to create a nested dictionary with store data
def rental_data(csv_file):
    store_data = {}
    distinct_customers = set()
    distinct_rental_ids = set()

    with open(csv_file, 'r', encoding='utf-8') as csv_file:
        read_file = csv.DictReader(csv_file)
        for row in read_file:
            store = row["Store ID"]
            amount = float(row["Amount"])
            customer = row["Customer ID"]
            rental_id = row["Rental ID"]
            distinct_customers.add(customer)
            distinct_rental_ids.add(rental_id)

            if store not in store_data:
                store_data[store] = {'customer_count': 0, 'rental_id_count': 0, 'amount_sum': 0.0}
            store_data[store]['customer_count'] = len(distinct_customers)
            store_data[store]['rental_id_count'] = len(distinct_rental_ids)
            store_data[store]['amount_sum'] = round(store_data[store]['amount_sum'] + amount, 2)
    return store_data