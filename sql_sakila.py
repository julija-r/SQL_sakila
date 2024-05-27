import sql_functions as sqlf
import pandas

hostname = "localhost"
username = "root"
password = "password" #enter a real password
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
data.to_csv('output.csv', index=False) # save store info in a csv file
print(pandas.read_csv("output.csv"))
store_data = sqlf.rental_data("output.csv")

#--turi daugiau customerių

max_customers = max(store_data.values(), key=lambda x: x['customer_count'])['customer_count']
stores_with_most_customers = [store for store, data in store_data.items() if data['customer_count'] == max_customers]

if len(stores_with_most_customers) > 1:
    print(f"Stores with the most customers ({max_customers}): {', '.join(stores_with_most_customers)}")
else:
    print(f"Store {stores_with_most_customers[0]} has the most customers ({max_customers})")

#--išnuomavo daugiau(ir kiek kiekvienas) filmų
max_rentals = max(store_data.values(), key=lambda x: x['rental_id_count'])['rental_id_count']
stores_with_most_rentals = [store for store, data in store_data.items() if data['rental_id_count'] == max_rentals]

if len(stores_with_most_rentals) > 1:
    print(f"Stores with the most rentals ({max_rentals}): {', '.join(stores_with_most_rentals)}")
else:
    print(f"Store {stores_with_most_rentals[0]} has the most rentals ({max_rentals})")


#--kiek sugeneravo pajamų
max_income = max(store_data.values(), key=lambda x: x['amount_sum'])['amount_sum']
stores_with_income = [store for store, data in store_data.items() if data['amount_sum'] == max_income]

if len(stores_with_most_rentals) > 1:
    print(f"Stores that generated most income ({max_income}): {', '.join(stores_with_income)}")
else:
    print(f"Store {stores_with_income[0]} generated most income ({max_income})")
