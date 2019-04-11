import psycopg2
import csv
from urllib.parse import urlparse, uses_netloc
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']


def initialize():
    # this function will get called once, when the application starts.
    #this function initializes the connection as well. 
        
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS customers(id SERIAL PRIMARY KEY, firstName text, lastName text, street int, city text, state text, zip int)')
        cursor.execute('CREATE TABLE IF NOT EXISTS products(id SERIAL PRIMARY KEY, name text, price real)')
        cursor.execute('CREATE TABLE IF NOT EXISTS orders(id SERIAL PRIMARY KEY, customerID int, productID int, date text, FOREIGN KEY (customerID) REFERENCES customers(id) ON UPDATE CASCADE ON DELETE SET NULL, FOREIGN KEY (productID) REFERENCES products(id) ON UPDATE CASCADE ON DELETE SET NULL)')
        conn.commit()
        
def get_customers():
    with conn.cursor() as curs:
        curs.execute("Select * from customers")
        data = list()
        for customerInfo in curs:
                customerData = dict()
                customerData['id'] = customerInfo[0]
                customerData['firstName'] = customerInfo[1]
                customerData['lastName']=customerInfo[2]
                customerData['street']=customerInfo[3]
                customerData['city']=customerInfo[4]
                customerData['state']=customerInfo[5]
                customerData['zip']=customerInfo[6]
                data.append(customerData)
        print(data)
        return data    


def get_customer(id):
    print('inside get customer')
    with conn.cursor() as curs:
         curs.execute("Select * from customers where id = %s;", (id,))
         customerInfo = curs.fetchone()
         customerData = dict()
         customerData['id'] = customerInfo[0]
         customerData['firstName'] = customerInfo[1]
         customerData['lastName']=customerInfo[2]
         customerData['street']=customerInfo[3]
         customerData['city']=customerInfo[4]
         customerData['state']=customerInfo[5]
         customerData['zip']=customerInfo[6]
    print(customerData)
    return customerData


def upsert_customer(customer):
    with conn.cursor() as curs:  
            if 'id' in customer :
                 curs.execute('Update customers SET firstName = %s, lastName = %s, street = %s, city =%s, state=%s, zip =%s where id = %s ', (customer.get('firstName'),customer.get('lastName'),customer.get('street'), customer.get('city'), customer.get('state'), customer.get('zip'), customer.get('id')))
            else:
                 curs.execute('Insert into customers( firstName, lastName, street, city, state, zip) values (%s, %s, %s, %s, %s, %s)',(customer.get('firstName'),customer.get('lastName'),customer.get('street'), customer.get('city'), customer.get('state'), customer.get('zip')))       
            #to update ON CONFLICT DO UPDATE SET firsName = ..., lastName = ...
    conn.commit()    


def delete_customer(id):
    print(type(id))
    print(id)
    with conn.cursor() as curs:
            curs.execute('delete from customers where id = (%s)',(id,))
    conn.commit()          

       
def get_products():
    with conn.cursor() as curs:
        curs.execute("Select * from products")
        data = list()

        for productInfo in curs:
                productData = dict()
                productData['id']=productInfo[0]
                productData['name'] = productInfo[1]
                productData['price']=productInfo[2] 
                data.append(productData)
        return data


def get_product(id):
    with conn.cursor() as curs:
        curs.execute("Select * from products where id = %s;", (id,))
        productInfo = curs.fetchone()
        productData = dict()
        productData['id']=productInfo[0]
        productData['name']= productInfo[1]
        productData['price']=productInfo[2]
        return productData


def upsert_product(product):
        
    with conn.cursor() as curs:
            if 'id' in product:
                 curs.execute('Update products SET name = %s, price =%s where id =%s',(product.get('name'),product.get('price'), product.get('id')))
            else:
                 curs.execute('Insert into products( name, price) values( %s, %s)',(product.get('name'), product.get('price')))
    

def delete_product(id):
    with conn.cursor() as curs:
            curs.execute("Delete from products where id = %s;",(id,))
    

def get_orders():
    
    with conn.cursor() as curs:
            curs.execute('Select * from orders')
            data = list()
            
            for ordersInfo in curs:
                 orderData = dict()
                 orderData['id']= ordersInfo[0]
                 orderData['customerID']=ordersInfo[1]
                 orderData['productID']=ordersInfo[2]
                 orderData['date']=ordersInfo[3]
                 data.append(orderData)
            for order in data:
                order['product']= get_product(order['productID'])
                order['customer']=get_customer(order['customerID'])
            
    return data


def get_order(id):
   with conn.cursor() as curs:
           curs.execute('Select * from orders where id=%s;', (id,))
           orderInfo = curs.fetchone()
           orderData = dict()
           orderData['id']=orderInfo[0]
           orderData['customerID']=orderInfo[1]
           orderData['productID']=orderInfo[2]
           orderData['date']=orderInfo[3]
           return orderData


def upsert_order(order):
    with conn.cursor() as curs:
            curs.execute('Insert into orders( customerID, productID, date) values( %s, %s, %s )', (order.get('customerId'), order.get('productId'),order.get('date')))
    conn.commit()

                        
def delete_order(id):
    with conn.cursor() as curs:
            curs.execute('Delete from orders where id=%s;',(id,))
   

# Return a list of products.  For each product, build
# create and populate a last_order_date, total_sales, and
# gross_revenue property.  Use JOIN and aggregation to avoid
# accessing the database more than once, and retrieving unnecessary
# information
def sales_report():
    with conn.cursor() as curs:
            curs.execute("select name, avg(price)*count(orders.productID), count(orders.productID), max(date)  from products JOIN orders on products.id = orders.productID Group By products.id")
            data = list()

            for salesInfo in curs:
                    salesData = dict()
                    salesData['name']=salesInfo[0]
                    salesData['gross_revenue']=salesInfo[1]
                    salesData['total_sales'] =salesInfo[2]
                    salesData['last_order_date']=salesInfo[3]
                    data.append(salesData)
            return data 
    


#Establishes a connection with the database and is called by initialize() function to establish a connection
def connect_to_db(conn_str):
    uses_netloc.append("postgres")
    url = urlparse(conn_str)

    conn = psycopg2.connect(database=url.path[1:],
                            user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port)

    return conn


conn = connect_to_db(connection_string)