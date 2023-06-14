import random
import string
import psycopg2  # Assuming you're using PostgreSQL

# Function to generate random email addresses
def generate_email():
    letters = string.ascii_lowercase
    domain = "@example.com"
    random_part = ''.join(random.choice(letters) for _ in range(10))
    return random_part + domain

# Function to generate random phone numbers
def generate_phone():
    return ''.join(random.choice(string.digits) for _ in range(10))

# Connect to the database
conn = psycopg2.connect(
    host="localhost",
    database="db",
    user="db",
    password="db"
)
cur = conn.cursor()

for i in range(6, 1001):
    name = "Customer " + str(i)
    email = generate_email()
    phone = generate_phone()
    address = "Address " + str(i)
    cur.execute(f"INSERT INTO customer (cust_no, name, email, phone, address) VALUES ({i}, '{name}', '{email}', '{phone}', '{address}')")

# Generate a million rows for orders
for i in range(11, 1000000):
    # Generate a new customer for each order

    year = random.randint(2010, 2025)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  
    cust_no = random.randint(1, 1000)

    date = "%i-%i-%i" % (year, month, day)  # Set your desired date here or generate random dates
    cur.execute(f"INSERT INTO orders (order_no, cust_no, date) VALUES ({i}, {cust_no}, '{date}')")

    SKU = "Product " + str(i)
    name = "Product " + str(i)
    description = "Description " + str(i)
    price = random.uniform(1, 1000)  # Adjust the range as per your requirement
    ean = random.randint(1000000000000, 9999999999999)  # Generate a 13-digit random EAN
    cur.execute(f"INSERT INTO product (SKU, name, description, price, ean) VALUES ('{SKU}', '{name}', '{description}', {price}, {ean})")

    qty = random.randint(1, 10)  # Adjust the range as per your requirement
    cur.execute(f"INSERT INTO contains (order_no, SKU, qty) VALUES ({i}, '{SKU}', {qty})")
    # Add products for each order
    #num_products = random.randint(1, 5)  # Adjust the range as per your requirement
    #or _ in range(10):
        # Generate a new product for each order
        

    # Commit the changes periodically to avoid overwhelming the database
    if i % 10000 == 0:
        conn.commit()

# Commit any remaining changes and close the connection
conn.commit()
conn.close()