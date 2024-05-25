import sql_functions as sqlf
import pandas
import csv

hostname = "localhost"
username = "root"
password = "Sharabanas1"
database = "sakila"
port = 3317

# 1. atvaizduoti visus customerius

query = "SELECT * FROM customer"
colNames = ["customer_id", "store_id", "first_name", "last_name", "email", "address_id", "active", "create_date", "last_update"]
customers = sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)

# print(customers.to_string())

#2. atvaizduoti visus customerius ir stulpelį kuriame būtų atvaizduota kiek pinigų kiekvienas jų yra
# išleidęs nuomai ir kiek filmų nuomavesis

query = ("SELECT "
         "customer.customer_id, "
         "customer.first_name, "
         "customer.last_name, "
         "SUM(payment.amount) AS amount, "
         "COUNT(payment.rental_id) AS nuomu_skaicius "
         "FROM sakila.customer AS customer "
         "LEFT JOIN sakila.payment AS payment ON customer.customer_id = payment.customer_id "
         "GROUP BY customer.customer_id;")

colNames = ["Customer ID", "Name", "Surname", "Amount spent", "Rental count"]

customers_info = sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)
# print(customers_info.to_string())

# 3. atvaizduoti aktorius ir keliuose filmuose jie yra filmavesi

query = ("SELECT "
         "actor.actor_id,"
         "actor.first_name,"
         "actor.last_name,"
         "COUNT(filmactor.actor_id) AS film_count "
         "FROM sakila.actor AS actor "
         "LEFT JOIN sakila.film_actor AS filmactor ON actor.actor_id = filmactor.actor_id "
         "GROUP BY actor.actor_id;")

colNames = ["Actor ID", "Name", "Surname", "Film count"]

actors = sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)
# print(actors.to_string())

# 4. atvaizduoti visus filmus ir kiek aktorių juose vaidino

query = ("SELECT "
         "film.title, "
         "COUNT(filmactor.actor_id) AS num_of_actors "
         "FROM sakila.film AS film "
         "LEFT JOIN sakila.film_actor AS filmactor "
         "ON film.film_id = filmactor.film_id "
         "GROUP BY film.film_id;")

colNames = ["Title", "Number of actors"]

film = sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)
# print(film.to_string())

# su pitono pagalba: nustatyti kuris nuomos punktas:

query = ("SELECT "
         "staff.store_id, "
         "payment.payment_id, "
         "payment.customer_id, "
         "payment.staff_id, "
         "payment.rental_id, "
         "payment.amount "
         "FROM sakila.payment AS payment "
         "LEFT JOIN sakila.staff AS staff ON payment.staff_id = staff.staff_id "
         "GROUP BY payment.payment_id;")

colNames = ["Store ID", "Payment ID", "Customer ID", "Staff ID", "Rental ID", "Amount"]
data = sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)
data.to_csv('output.csv', index=False)

#--turi daugiau customerių

# Read the CSV file into a DataFrame
df = pandas.read_csv("output.csv")

# Group the data by "Store ID" and count unique customers for each store
customer_count = df.groupby("Store ID")["Customer ID"].nunique()

max_customers = customer_count.max()
stores_with_max_customers = customer_count[customer_count == max_customers].index.tolist()

if len(stores_with_max_customers) == 1:
    print(f"Store {stores_with_max_customers[0]} has the most customers with {max_customers} customers.")
else:
    print(f"Multiple stores have the most customers with {max_customers} customers: {', '.join(map(str, stores_with_max_customers))}")

#--išnuomavo daugiau(ir kiek kiekvienas) filmų

#--kiek sugeneravo pajamų

store_data = {}
distinct_customers = set()
distinct_rental_ids = set()

with open("output.csv", 'r', encoding='utf-8') as csv_file:
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

max_customers = max(store_data.values(), key=lambda x: x['customer_count'])['customer_count']
stores_with_most_customers = [store for store, data in store_data.items() if data['customer_count'] == max_customers]

if len(stores_with_most_customers) > 1:
    print(f"Stores with the most customers ({max_customers}): {', '.join(stores_with_most_customers)}")
else:
    print(f"Store {stores_with_most_customers[0]} has the most customers ({max_customers})")

