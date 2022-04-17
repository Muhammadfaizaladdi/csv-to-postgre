import pandas as pd
import psycopg2
from psycopg2 import OperationalError as Error
from random import randint
from time import sleep

HOST='localhost'
DATABASE='superstore'
USER='postgres'
PASSWORD='RumahMakan02'

orders_data=pd.read_csv('output/orders_clean.csv')
customers_data=pd.read_csv('output/customers_clean.csv')
products_data = pd.read_csv("output/products_clean.csv")
transactions_data = pd.read_csv("output/transactions_clean.csv")

create_orders_table = """
CREATE TABLE orders (
    order_id VARCHAR(25) NOT NULL PRIMARY KEY,
    order_date DATE NOT NULL,
    ship_date date NOT NULL,
    ship_mode VARCHAR(25) NOT NULL
);
"""
create_customers_table = """
CREATE TABLE customers (
    customer_id VARCHAR(15) NOT NULL PRIMARY KEY,
    customer_name VARCHAR(50) NOT NULL,
    segment VARCHAR(15) NOT NULL,
    country VARCHAR(25) NOT NULL,
    city VARCHAR(25) NOT NULL,
    state VARCHAR(25) NOT NULL,
    postal_code INT NOT NULL,
    region VARCHAR(10) NOT NULL
);
"""
create_products_table = """
CREATE TABLE products (
    product_id VARCHAR(25) NOT NULL PRIMARY KEY,
    category VARCHAR(25) NOT NULL,
    sub_category VARCHAR(25) NOT NULL,
    product_name VARCHAR(130) NOT NULL
);
"""
create_transactions_table = """
CREATE TABLE transactions (
    order_id VARCHAR(25)  NOT NULL,
    customer_id VARCHAR(25) NOT NULL,
    product_id VARCHAR(25) NOT NULL,
    sales NUMERIC(15,5)  NOT NULL,
    quantity INT NOT NULL,
    discount NUMERIC(5,2) NOT NULL,
    profit NUMERIC(15,5)  NULL,
    PRIMARY KEY(order_id, customer_id, product_id)
);
"""

data = {
        "orders" : {
                    "table" : create_orders_table,
                    "value" : orders_data,
                    },
        "customers" : {
                    "table" : create_customers_table,
                    "value" : customers_data,
                    },
         "products" : {
                    "table" : create_products_table,
                    "value" : products_data,
                    },
        "transactions" : {
                    "table" : create_transactions_table,
                    "value" : transactions_data,
                    },
        }


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = psycopg2.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name)
        print("PostgreSQL database connection successfull")
    except Error as err:
        print(f"Error: {err}")
    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query berhasil dieksekusi")      
    except Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

        
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        
        
def insert_data(dataset, host, user, password, database, table=None, max_sleep=3):
    connection = create_db_connection(host, user, password, database)
    
    for i in range(len(dataset)):
        c = []
        c.append(tuple(i for i in dataset.iloc[i]))

        query = f"""INSERT INTO {table} values {c[0]};"""
        execute_query(connection, query)

        if (i%20==0) and (i!=0):
            sleep(randint(2,max_sleep))
            
    print(f"---------------Inser data {table} done------------------")
            

def run(data):
    for i in data:
        connection = create_db_connection(HOST, USER, PASSWORD, DATABASE)
        execute_query(connection, data[i]["table"])
        insert_data(data[i]["value"], HOST, USER, PASSWORD, DATABASE, table=i)
        
    
if __name__ == "__main__":
    run(data)
    print("---------------------Insert All Data Done-------------------")