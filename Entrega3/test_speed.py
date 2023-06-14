import psycopg2
import time

# Connect to the database
conn = psycopg2.connect(
    host="localhost",
    database="db",
    user="db",
    password="db"
)
cur = conn.cursor()

# Example SELECT query
start_time = time.time()

cur.execute("SELECT order_no FROM orders JOIN contains USING (order_no) JOIN product USING (SKU) WHERE price > 50 AND EXTRACT(YEAR FROM date) = 2023")

# Fetch all rows
rows = cur.fetchall()

end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")

# Close the connection
conn.close()