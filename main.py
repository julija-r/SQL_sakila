import mysql.connector

hostname = "localhost"
username = "root"
password = "Sharabanas1"
database = "sakila"
connection = None
cursor = None
try:
    connection = mysql.connector.connect(host=hostname, port = 3317, user=username, password=password, database=database)
    print("Connection successful!")

    cursor = connection.cursor()
    query = "SELECT * FROM actor" # Replace with your desired query
    cursor.execute(query)
    results = cursor.fetchall()

    for row in results:
        print(f"ID: {row[0]}, Name: {row[1]}, Surname: {row[2]}") # Access column data by index

except mysql.connector.Error as err:
    print(f"Connection error: {err}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    print("Connection closed.")