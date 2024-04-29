# atvaizduoti visus customerius

# atvaizduoti visus customerius ir stulpelį kuriame būtų atvaizduota kiek pinigų kiekvienas jų yra išleidęs nuomai, ir kiek filmų nuomavesis
# atvaizduoti aktorius ir keliuose filmuose jie yra filmavesi
# atvaizduoti visus filmus ir kiek aktorių juose vaidino
# su pitono pagalba: nustatyti kuris nuomos punktas:
#--turi daugiau customerių
#--išnuomavo daugiau(ir kiek kiekvienas) filmų
#--kiek sugeneravo pajamų

import sql_functions as sqlf
hostname = "localhost"
username = "root"
password = "Sharabanas1"
database = "sakila"
port = 3317

# 1. atvaizduoti visus customerius

query = "SELECT * FROM customer"
colNames = ["customer_id", "store_id", "first_name", "last_name", "email", "address_id", "active", "create_date", "last_update"]
sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)

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

sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)

# 3. atvaizduoti aktorius ir keliuose filmuose jie yra filmavesi

query = ("SELECT "
         "actor.first_name,"
         "actor.last_name,"
         "COUNT(filmactor.actor_id) AS film_count "
         "FROM sakila.actor AS actor "
         "LEFT JOIN sakila.film_actor AS filmactor ON actor.actor_id = filmactor.actor_id "
         "GROUP BY actor.actor_id;")

colNames = ["Name", "Surname", "Film count"]

print(sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames))

# 4. atvaizduoti visus filmus ir kiek aktorių juose vaidino

query = ("SELECT "
         "film.title, "
         "COUNT(filmactor.actor_id) AS num_of_actors "
         "FROM sakila.film AS film "
         "LEFT JOIN sakila.film_actor AS filmactor "
         "ON film.film_id = filmactor.film_id "
         "GROUP BY film.film_id;")

colNames = ["Title", "Number of actors"]

sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)

# su pitono pagalba: nustatyti kuris nuomos punktas:

query = ("SELECT payment.*,staff.store_id "
         "FROM sakila.payment AS payment "
         "LEFT JOIN sakila.staff AS staff "
         "ON payment.staff_id = staff.staff_id "
         "GROUP BY payment.payment_id;")

sqlf.execute_mysql_query(hostname, port, username, password, database, query, colNames)

#--turi daugiau customerių
#--išnuomavo daugiau(ir kiek kiekvienas) filmų
#--kiek sugeneravo pajamų