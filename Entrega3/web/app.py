#!/usr/bin/python3
import os
from logging.config import dictConfig

import psycopg
from flask import flash
from flask import Flask
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

from flask_paginate import Pagination, get_page_args

# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://me:me@localhost/me")

pool = ConnectionPool(conninfo=DATABASE_URL)
# the pool starts connecting immediately.

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
log = app.logger
app.secret_key = 'super secret key'


@app.route("/", methods=("GET",))
def main_page():
    return render_template("main.html")

@app.route("/customers", methods=("GET",))
def customer_index():
    """Show all the customers, most recent first."""

    page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    per_page = 10

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("SELECT COUNT(*) FROM customer;")
            customers_count = cur.fetchone()[0]
            total_pages = (customers_count + per_page - 1) // per_page
            customers = cur.execute(
                """
                SELECT cust_no, name, email
                FROM customer
                ORDER BY cust_no DESC
                LIMIT %s OFFSET %s;
                """,
                (per_page, offset),
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    pagination = Pagination(page=page, per_page=per_page, total=customers_count, css_framework='bootstrap4')

    # API-like response is returned to customers that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(customers)

    return render_template("customer/index.html", customers=customers, pagination=pagination)

# funciton to create a new product
@app.route("/products/create", methods=("POST", "GET"))
def product_create():
    """Create new product."""

    if request.method == "POST":
        sku = request.form["sku"]
        name = request.form["name"]
        description = request.form["description"]
        price = request.form["price"]
        ean = request.form["ean"]

        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                cur.execute("""SELECT product FROM product WHERE sku = %(sku)s;""", {"sku": sku})
                if cur.fetchone() is not None:
                    error = f"Product with sku \"{sku}\" already exists."
                    flash(error)
                    return render_template("product/create.html")
                
                query = """
                    INSERT INTO product
                    VALUES(
                        %(sku)s,
                        %(name)s,
                        %(description)s,
                        %(price)s,
                        %(ean)s
                    );
                """
                if not description:
                    description = None
                if not ean:
                    ean = None
                else:
                    cur.execute("""SELECT product FROM product WHERE ean = %(ean)s;""", {"ean": ean})
                    if cur.fetchone() is not None:
                        error = f"Product with ean \"{ean}\" already exists."
                        flash(error)
                        return render_template("product/create.html")
                    
                params = {"sku": sku, "name": name, "description": description, "price": price, "ean": ean}
                cur.execute(query, params)
            conn.commit()
        return redirect(url_for("product_index"))
    else:
        return render_template("product/create.html")

@app.route("/orders", methods=("GET",))
def order_index():
    """Show all the orders with products and quantities"""

    page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    page = int(page)
    per_page = int(per_page)
    offset = (page - 1) * per_page

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("SELECT COUNT(*) FROM orders;")
            orders_count = cur.fetchone()[0]
            total_pages = (orders_count + per_page - 1) // per_page

            cur.execute(
                """
                SELECT o.order_no, o.cust_no, o.date, 
                    array_agg(json_build_object('name', p.name, 'qty', c.qty)) AS products
                FROM orders o
                LEFT JOIN contains c ON o.order_no = c.order_no
                LEFT JOIN product p ON c.SKU = p.SKU
                GROUP BY o.order_no, o.cust_no, o.date
                ORDER BY o.order_no ASC
                LIMIT %s OFFSET %s;
                """,
                (per_page, offset),
            )
            orders = cur.fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    pagination = Pagination(page=page, per_page=per_page, total=orders_count, css_framework='bootstrap4')


    # API-like response is returned to orders that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(orders)

    return render_template("order/index.html", orders=orders, pagination=pagination)


@app.route("/customers/<cust_no>/order", methods=("POST", "GET"))
def order_create(cust_no):
    """Create new order."""

    if request.method == "POST":
        print("Creating order... for customer: ", cust_no)
        date = request.form["date"]

        if len(date.split("-")[0]) > 4:
            error = f"Invalid year. Please insert a year in the format YYYY."
            flash(error)
            return render_template("order/create.html", cust_no=cust_no)

        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                for key, value in request.form.items():
                    if key.startswith("sku-"):
                        sku = value
                        if sku:
                            cur.execute("""SELECT product FROM product WHERE sku = %(sku)s;""", {"sku": sku})
                            if cur.fetchone() is None:
                                error = f"Product with sku \"{sku}\" does not exist."
                                flash(error)
                                return render_template("order/create.html", cust_no=cust_no)
                
                query = """
                    INSERT INTO orders
                    VALUES(
                        (SELECT order_no
                        FROM orders
                        ORDER BY order_no DESC LIMIT 1)+1,
                        %(cust_no)s,
                        %(date)s
                    );
                """
                params = {"cust_no": cust_no, "date": date}
                cur.execute(query, params)
                conn.commit()

                # Insert the products into the contains table
                i = 1
                for key, value in request.form.items():
                    print(key, value)
                    if key.startswith("sku-"):
                        sku = value
                        quantity = int(request.form.get(f"quantity-{i}"))

                        if sku and quantity > 0:
                            product_query = """
                                INSERT INTO contains (order_no, SKU, qty)
                                VALUES ((SELECT MAX(order_no) FROM orders), %(sku)s, %(qty)s);
                            """
                            product_params = {"sku": sku, "qty": quantity}
                            cur.execute(product_query, product_params)
                            conn.commit()
                        i += 1

        return redirect(url_for("main_page", cust_no=cust_no))
    else:
        return render_template("order/create.html", cust_no=cust_no)

@app.route("/customers/create", methods=("POST", "GET"))
def customer_create():
    """Create new customer."""

    if request.method == "POST":
        name = request.form["name"]
        email = request.form["email"]
        phone = request.form["phone"]
        address = request.form["address"]

        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                cur.execute("""SELECT customer FROM customer WHERE email = %(email)s;""", {"email": email})
                if cur.fetchone() is not None:
                    error = f"Customer with email \"{email}\" already exists."
                    flash(error)
                    return render_template("customer/create.html")

                query = """
                    INSERT INTO customer
                    VALUES(
                        (SELECT cust_no 
                        FROM customer 
                        ORDER BY cust_no DESC LIMIT 1)+1,
                        %(name)s,
                        %(email)s,
                        %(phone)s,
                        %(address)s
                        );
                    """
                if not phone:
                    phone = None
                if not address:
                    address = None
                params = {"name": name, "email": email, "phone": phone, "address": address}
                cur.execute(query, params)
            conn.commit()
        return redirect(url_for("customer_index"))
    else:
        return render_template("customer/create.html")

#product info
@app.route("/products/<sku>", methods=("POST", "GET"))
def product_info(sku):
    """Show product info."""
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            prod = cur.execute(
                """
                SELECT sku, name, description, price, ean
                FROM product
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    return render_template("product/update.html", prod=prod)

@app.route("/customers/<cust_no>", methods=("POST", "GET"))
def customer_info(cust_no):
    """Show customer info."""
    print(cust_no)
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cust = cur.execute(
                """
                SELECT cust_no, name, email, phone, address
                FROM customer
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    return render_template("customer/update.html", cust=cust)


@app.route("/customers/<cust_no>/delete", methods=("POST",))
def customer_delete(cust_no):
    """Delete the customer."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                WITH deleted_orders AS (
                SELECT order_no
                FROM orders
                WHERE cust_no = %(cust_no)s
                )
                DELETE FROM process
                WHERE order_no IN (SELECT order_no FROM deleted_orders);
                """,
                {"cust_no": cust_no},
            )
            cur.execute(
                """
                WITH deleted_orders AS (
                SELECT order_no
                FROM orders
                WHERE cust_no = %(cust_no)s
                )
                DELETE FROM contains
                WHERE order_no IN (SELECT order_no FROM deleted_orders);
                """,
                {"cust_no": cust_no},
            )
            cur.execute(
                """
                DELETE FROM pay
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )
            cur.execute(
                """
                DELETE FROM orders
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )
            cur.execute(
                """
                DELETE FROM customer
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )
        conn.commit()
    return redirect(url_for("customer_index"))


@app.route("/products/<sku>/delete", methods=("POST",))
def product_delete(sku):
    """Delete the product."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                UPDATE supplier
                SET sku = NULL
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )
            cur.execute(
                """
                DELETE FROM contains
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )
            cur.execute(
                """
                DELETE FROM product
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )
        conn.commit()
    return redirect(url_for("product_index"))


@app.post("/products/<sku>/update")
def product_update(sku):
    """Update the product."""

    description = request.form["description"]
    price = request.form["price"]

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                UPDATE product
                SET description = %(description)s, price = %(price)s
                WHERE sku = %(sku)s;
                """,
                {"description": description, "price": price, "sku": sku}
            )
        conn.commit()
    return redirect(url_for("product_index"))

@app.route("/customers/<cust_no>/list", methods=("POST", "GET"))
def list_unpaid_orders(cust_no):
    """List unpaid orders made by this customer."""

    page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    page = int(page)
    per_page = int(per_page)
    offset = (page - 1) * per_page

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("""SELECT COUNT(*)
                        FROM orders o
                        LEFT JOIN pay p ON o.order_no = p.order_no
                        WHERE o.cust_no = %(cust_no)s
                        AND p.order_no IS NULL;""", 
                        {"cust_no": cust_no})
            orders_count = cur.fetchone()[0]
            total_pages = (orders_count + per_page - 1) // per_page

            orders = cur.execute(
                """
                SELECT o.order_no, o.date
                FROM orders o
                LEFT JOIN pay p ON o.order_no = p.order_no
                WHERE o.cust_no = %(cust_no)s
                AND p.order_no IS NULL
                LIMIT %(per_page)s OFFSET %(offset)s;
                """,
                {"cust_no": cust_no, "per_page": per_page, "offset": offset}
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    pagination = Pagination(page=page, per_page=per_page, total=orders_count, css_framework='bootstrap4')

    return render_template("order/pay.html", orders=orders, cust_no=cust_no, pagination=pagination)

@app.route("/customers/<cust_no>/list/<order_no>/pay", methods=("POST",))
def pay_order(order_no, cust_no):
    """Pay an order."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                INSERT INTO pay(order_no, cust_no)
                VALUES (%(order_no)s, %(cust_no)s);
                """,
                {"order_no": order_no, "cust_no": cust_no}
            )
        conn.commit()
    return redirect(url_for("customer_index"))


@app.get("/products")
def product_index():
    """Show all the products, most recent first."""

    page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    per_page = 10

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("SELECT COUNT(*) FROM product;")
            product_count = cur.fetchone()[0]
            total_pages = (product_count + per_page - 1) // per_page

            products = cur.execute(
                """
                SELECT sku, name, description, price, ean
                FROM product
                ORDER BY name DESC
                LIMIT %s OFFSET %s;
                """,
                (per_page, offset),
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    pagination = Pagination(page=page, per_page=per_page, total=product_count, css_framework='bootstrap4')

    # API-like response is returned to customers that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(products)

    return render_template("product/index.html", products=products, pagination=pagination)


@app.post("/products/<sku>/price")
def product_price(sku):
    """Change the product price."""
    price = request.form["price"]

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                UPDATE product
                SET price=%(price)s
                WHERE sku = %(sku)s;
                """,
                {"price": price, "sku": sku,}
            )
        conn.commit()
    return redirect(url_for("product_index"))

#add a product to an order
@app.route("/order/<order_no>/add", methods=("POST", "GET"))
def order_add(order_no):
    """Add a product to an order."""

    if request.method == "POST":
        sku = request.form["sku"]
        quantity = request.form["quantity"]

        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                query = """
                    INSERT INTO contains (order_no, sku, qty)
                    VALUES (%(order_no)s, %(sku)s, %(quantity)s);
                """
                params = {"order_no": order_no, "sku": sku, "quantity": quantity}
                cur.execute(query, params)
            conn.commit()
        return redirect(url_for("order_info", order_no=order_no))

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            products = cur.execute(
                """
                SELECT sku, name, description, price, ean
                FROM product
                ORDER BY name;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    return render_template("order/add.html", products=products, order_no=order_no)
    
@app.route("/supplier/create", methods=("POST", "GET"))
def supplier_create():
    """Create a new supplier."""

    if request.method == "POST":
        tin = request.form["tin"]
        name = request.form["name"]
        address = request.form["address"]
        sku = request.form["sku"]
        date = request.form["date"]

        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                cur.execute("""SELECT supplier FROM supplier WHERE tin = %(tin)s;""", {"tin": tin})
                if cur.fetchone() is not None:
                    error = f"Supplier with tin \"{tin}\" already exists."
                    flash(error)
                    return render_template("supplier/create.html")
                
                query = """
                    INSERT INTO supplier
                    VALUES (%(tin)s, %(name)s, %(address)s
                """
                params = {"tin": tin, "name": name, "address": address}
                if sku:
                    cur.execute("""SELECT product FROM product WHERE sku = %(sku)s;""", {"sku": sku})
                    if cur.fetchone() is None:
                        error = f"Product with sku \"{sku}\" does not exist."
                        flash(error)
                        return render_template("supplier/create.html")
                    query += ", %(sku)s"
                    params['sku'] = sku
                if date:
                    if len(date.split("-")[0]) > 4:
                        error = f"Invalid year. Please insert a year in the format YYYY."
                        flash(error)
                        return render_template("supplier/create.html")
                    query += ", %(date)s"
                    params['date'] = date
                query += ");"
                cur.execute(query, params)
            conn.commit()
        return redirect(url_for("supplier_index"))
    else:
        return render_template("supplier/create.html")

#delete supplier 
@app.route("/supplier/<tin>/delete", methods=("POST",))
def supplier_delete(tin):
    """Delete the supplier."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                DELETE FROM delivery
                WHERE tin = %(tin)s;
                """,
                {"tin": tin},
            )
            cur.execute(
                """
                DELETE FROM supplier
                WHERE tin = %(tin)s;
                """,
                {"tin": tin},
            )
        conn.commit()
    return redirect(url_for("supplier_index"))


    

@app.get("/supplier")
def supplier_index():
    """Show all the products, most recent first."""
    
    page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    per_page = 10

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("SELECT COUNT(*) FROM supplier;")
            supplier_count = cur.fetchone()[0]
            total_pages = (supplier_count + per_page - 1) // per_page

            supplier = cur.execute(
                """
                SELECT tin, name, address, sku, date
                FROM supplier
                ORDER BY tin DESC
                LIMIT %s OFFSET %s;
                """,
                (per_page, offset),
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    pagination = Pagination(page=page, per_page=per_page, total=supplier_count, css_framework='bootstrap4')

    # API-like response is returned to customers that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(supplier)

    return render_template("supplier/index.html", supplier=supplier, pagination=pagination)

@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run(debug=True)
