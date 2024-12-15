### Utility Functions
#import pandas as pd

import sqlite3
from sqlite3 import Error
import os
import datetime


def create_connection(db_file, delete_db=False):
    """
    Create a database connection or delete and recreate it if specified.
    """
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
        return conn
    except Error as e:
        print(f"Error creating connection: {e}")
        return None


def create_table(conn, create_table_sql, drop_table_name=None):
    """
    Create a table using the provided SQL. Drop it first if the name is provided.
    """
    try:
        c = conn.cursor()
        if drop_table_name:
            c.execute("PRAGMA foreign_keys = OFF;")
            c.execute(f"DROP TABLE IF EXISTS {drop_table_name}")
            conn.commit()
            c.execute("PRAGMA foreign_keys = ON;")

        c.execute(create_table_sql)
    except Error as e:
        print(f"Error creating table: {e}")


def execute_sql_statement(sql_statement, conn):
    """
    Execute an SQL query and return the results.
    """
    try:
        cur = conn.cursor()
        cur.execute(sql_statement)
        return cur.fetchall()
    except Error as e:
        print(f"Error executing SQL statement: {e}")
        return []


def step1_create_region_table(data_filename, normalized_database_filename):
    """
    Create the Region table, populating it with unique, sorted regions from the file.
    """
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    if not conn:
        print("Failed to connect.")
        return

    try:
        regions = set()
        with open(data_filename, 'r') as file:
            next(file)
            for line in file:
                data = line.strip().split('\t')
                region = data[4]
                regions.add(region)

        sorted_regions = sorted(regions)

        create_table_sql = '''
        CREATE TABLE Region (
            RegionID INTEGER PRIMARY KEY,
            Region TEXT NOT NULL
        );
        '''
        create_table(conn, create_table_sql, drop_table_name='Region')

        cur = conn.cursor()
        cur.executemany('INSERT INTO Region (Region) VALUES (?)', [(region,) for region in sorted_regions])

        conn.commit()
        print("Region table task is successfull.")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Error as e:
        print(f"Error during operation: {e}")
    finally:
        conn.close()


def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    cur = conn.cursor()
    cur.execute("SELECT RegionID, Region FROM Region")
    region_to_regionid_dict = {row[1]: row[0] for row in cur.fetchall()}
    conn.close()
    return region_to_regionid_dict


def step3_create_country_table(data_filename, normalized_database_filename):
    """
    Create the Country table, associating countries with regions.
    """
    # Inputs: Name of the data and normalized database filename
    # Output: None
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    region_to_regionid_dict = step2_create_region_to_regionid_dictionary(normalized_database_filename)

    countries = set()
    with open(data_filename, 'r') as file:
        next(file)
        for line in file:
            data = line.strip().split('\t')
            country, region = data[3], data[4]
            countries.add((country, region))

    sorted_countries = sorted(countries)

    create_table_sql = '''
    CREATE TABLE Country (
        CountryID INTEGER PRIMARY KEY,
        Country TEXT NOT NULL,
        RegionID INTEGER NOT NULL,
        FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
    );
    '''
    create_table(conn, create_table_sql, drop_table_name='Country')

    cur = conn.cursor()
    cur.executemany('INSERT INTO Country (Country, RegionID) VALUES (?, ?)',
                    [(country, region_to_regionid_dict[region]) for country, region in sorted_countries])

    conn.commit()
    conn.close()


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    cur = conn.cursor()
    cur.execute("SELECT CountryID, Country FROM Country")
    country_to_countryid_dict = {row[1]: row[0] for row in cur.fetchall()}
    conn.close()
    return country_to_countryid_dict


def step5_create_customer_table(data_filename, normalized_database_filename):
    """
    Create the Customer table, associating customers with countries.
    """
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    country_to_countryid_dict = step4_create_country_to_countryid_dictionary(normalized_database_filename)

    customers = set()
    with open(data_filename, 'r') as file:
        next(file)
        for line in file:
            data = line.strip().split('\t')
            name_parts = data[0].split()
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
            address, city, country = data[1], data[2], data[3]
            customers.add((first_name, last_name, address, city, country))

    sorted_customers = sorted(customers, key=lambda x: (x[0], x[1]))

    create_table_sql = '''
    CREATE TABLE Customer (
        CustomerID INTEGER PRIMARY KEY,
        FirstName TEXT NOT NULL,
        LastName TEXT NOT NULL,
        Address TEXT NOT NULL,
        City TEXT NOT NULL,
        CountryID INTEGER NOT NULL,
        FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
    );
    '''
    create_table(conn, create_table_sql, drop_table_name='Customer')

    cur = conn.cursor()
    cur.executemany('INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES (?, ?, ?, ?, ?)',
                    [(c[0], c[1], c[2], c[3], country_to_countryid_dict[c[4]]) for c in sorted_customers])

    conn.commit()
    conn.close()


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    cur = conn.cursor()
    cur.execute("SELECT CustomerID, FirstName, LastName FROM Customer")
    customer_to_customerid_dict = {f"{row[1]} {row[2]}".strip(): row[0] for row in cur.fetchall()}
    conn.close()
    return customer_to_customerid_dict


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    """
    Create the ProductCategory table.
    """
    # Inputs: Name of the data and normalized database filename
    # Output: None
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)

    product_categories = set()
    with open(data_filename, 'r') as file:
        next(file)  # Skip header row
        for line in file:
            data = line.strip().split('\t')
            categories = data[6].split(';')
            descriptions = data[7].split(';')
            for category, description in zip(categories, descriptions):
                product_categories.add((category, description))

    sorted_product_categories = sorted(product_categories)

    create_table_sql = '''
    CREATE TABLE ProductCategory (
        ProductCategoryID INTEGER PRIMARY KEY,
        ProductCategory TEXT NOT NULL,
        ProductCategoryDescription TEXT NOT NULL
    );
    '''
    create_table(conn, create_table_sql, drop_table_name='ProductCategory')

    cur = conn.cursor()
    cur.executemany('INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES (?, ?)',
                    sorted_product_categories)

    conn.commit()
    conn.close()


def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    cur = conn.cursor()
    cur.execute("SELECT ProductCategoryID, ProductCategory FROM ProductCategory")
    productcategory_to_productcategoryid_dict = {row[1]: row[0] for row in cur.fetchall()}
    conn.close()
    return productcategory_to_productcategoryid_dict


def step9_create_product_table(data_filename, normalized_database_filename):
    """
    Create the Product table, associating products with categories.
    """
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    productcategory_to_productcategoryid_dict = step8_create_productcategory_to_productcategoryid_dictionary(
        normalized_database_filename)

    products = set()
    with open(data_filename, 'r') as file:
        next(file)
        for line in file:
            data = line.strip().split('\t')
            product_names = data[5].split(';')
            categories = data[6].split(';')
            prices = data[8].split(';')
            for name, category, price in zip(product_names, categories, prices):
                products.add((name, float(price), category))

    sorted_products = sorted(products, key=lambda x: x[0])

    create_table_sql = '''
    CREATE TABLE Product (
        ProductID INTEGER PRIMARY KEY,
        ProductName TEXT NOT NULL,
        ProductUnitPrice REAL NOT NULL,
        ProductCategoryID INTEGER NOT NULL,
        FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
    );
    '''
    create_table(conn, create_table_sql, drop_table_name='Product')

    cur = conn.cursor()
    cur.executemany('INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?, ?, ?)',
                    [(p[0], p[1], productcategory_to_productcategoryid_dict[p[2]]) for p in sorted_products])

    conn.commit()
    conn.close()


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    cur = conn.cursor()
    cur.execute("SELECT ProductID, ProductName FROM Product")
    product_to_productid_dict = {row[1]: row[0] for row in cur.fetchall()}
    conn.close()
    return product_to_productid_dict


def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # WRITE YOUR CODE HERE
    conn = create_connection(normalized_database_filename)
    customer_to_customerid_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    product_to_productid_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)

    orders = []
    with open(data_filename, 'r') as file:
        next(file)
        for line in file:
            data = line.strip().split('\t')
            customer_name = data[0]
            product_names = data[5].split(';')
            quantities = data[9].split(';')
            order_dates = data[10].split(';')

            for product_name, quantity, order_date in zip(product_names, quantities, order_dates):
                orders.append((
                    customer_to_customerid_dict[customer_name],
                    product_to_productid_dict[product_name],
                    datetime.datetime.strptime(order_date, '%Y%m%d').strftime('%Y-%m-%d'),
                    int(quantity)
                ))

    create_table_sql = '''
    CREATE TABLE OrderDetail (
        OrderID INTEGER PRIMARY KEY,
        CustomerID INTEGER NOT NULL,
        ProductID INTEGER NOT NULL,
        OrderDate TEXT NOT NULL,
        QuantityOrdered INTEGER NOT NULL,
        FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
        FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
    );
    '''
    create_table(conn, create_table_sql, drop_table_name='OrderDetail')

    cur = conn.cursor()
    cur.executemany('INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?, ?, ?, ?)',
                    orders)

    conn.commit()
    conn.close()


def ex1(conn, customer_name):
    # Simply, you are fetching all the rows for a given CustomerName.
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    # WRITE YOUR CODE HERE
    customer_to_customerid = step6_create_customer_to_customerid_dictionary('normalized.db')
    customer_id = customer_to_customerid.get(customer_name)

    sql_statement = f"""
    SELECT 
        Customer.FirstName || ' ' || Customer.LastName AS Name, 
        Product.ProductName, 
        OrderDetail.OrderDate, 
        Product.ProductUnitPrice, 
        OrderDetail.QuantityOrdered, 
        ROUND(Product.ProductUnitPrice * OrderDetail.QuantityOrdered, 2) AS Total
    FROM 
        OrderDetail
    JOIN 
        Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN 
        Product ON OrderDetail.ProductID = Product.ProductID
    WHERE 
        OrderDetail.CustomerID = {customer_id};
    """
    return sql_statement


def ex2(conn, CustomerName):
    # Simply, you are summing the total for a given CustomerName.
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    # WRITE YOUR CODE HERE

    sql_statement = f"""
    SELECT 
        (Customer.FirstName || ' ' || Customer.LastName) AS Name,
        ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 2) AS Total
    FROM 
        OrderDetail
    JOIN 
        Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN 
        Product ON OrderDetail.ProductID = Product.ProductID
    WHERE 
        (Customer.FirstName || ' ' || Customer.LastName) = '{CustomerName}'
    GROUP BY
        Customer.CustomerID
    """
    return sql_statement


def ex3(conn):
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns.
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending
    # WRITE YOUR CODE HERE
    sql_statement = """
    SELECT 
        (c.FirstName || ' ' || c.LastName) AS Name,
        ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
    FROM 
        OrderDetail od
    JOIN 
        Product p ON od.ProductID = p.ProductID
    JOIN 
        Customer c ON od.CustomerID = c.CustomerID
    GROUP BY 
        c.CustomerID
    ORDER BY 
        Total DESC
    """
    return sql_statement


def ex4(conn):
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and
    # Region tables.
    # Pull out the following columns.
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending
    # WRITE YOUR CODE HERE
    sql_statement = """
    SELECT 
        r.Region,
        ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
    FROM 
        OrderDetail od
    JOIN 
        Product p ON od.ProductID = p.ProductID
    JOIN 
        Customer c ON od.CustomerID = c.CustomerID
    JOIN 
        Country co ON c.CountryID = co.CountryID
    JOIN 
        Region r ON co.RegionID = r.RegionID
    GROUP BY 
        r.RegionID
    ORDER BY 
        Total DESC
    """
    return sql_statement


def ex5(conn):
    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns.
    # Country
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending
    # WRITE YOUR CODE HERE

    sql_statement = """
    SELECT 
        Country.Country,
        ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 0) AS Total
    FROM 
        OrderDetail
    JOIN 
        Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN 
        Product ON OrderDetail.ProductID = Product.ProductID
    JOIN 
        Country ON Customer.CountryID = Country.CountryID
    GROUP BY 
        Country.Country
    ORDER BY 
        Total DESC
    """

    return sql_statement


def ex6(conn):
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # WRITE YOUR CODE HERE

    sql_statement = """
    WITH CountryTotals AS (
        SELECT 
            r.Region,
            c.Country,
            ROUND(SUM(od.QuantityOrdered * p.ProductUnitPrice)) AS CountryTotal
        FROM 
            OrderDetail od
            JOIN Customer cu ON od.CustomerID = cu.CustomerID
            JOIN Country c ON cu.CountryID = c.CountryID
            JOIN Region r ON c.RegionID = r.RegionID
            JOIN Product p ON od.ProductID = p.ProductID
        GROUP BY 
            r.Region, c.Country
    )
    SELECT 
        Region,
        Country,
        CountryTotal,
        RANK() OVER (PARTITION BY Region ORDER BY CountryTotal DESC) AS TotalRank
    FROM 
        CountryTotals
    ORDER BY 
        Region ASC, CountryTotal DESC
    """
    return sql_statement


def ex7(conn):
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    # WRITE YOUR CODE HERE

    sql_statement = """
    WITH CountryTotals AS (
        SELECT 
            r.Region,
            c.Country,
            ROUND(SUM(od.QuantityOrdered * p.ProductUnitPrice)) AS CountryTotal,
            RANK() OVER (PARTITION BY r.Region ORDER BY SUM(od.QuantityOrdered * p.ProductUnitPrice) DESC) AS CountryRegionalRank
        FROM 
            OrderDetail od
            JOIN Customer cu ON od.CustomerID = cu.CustomerID
            JOIN Country c ON cu.CountryID = c.CountryID
            JOIN Region r ON c.RegionID = r.RegionID
            JOIN Product p ON od.ProductID = p.ProductID
        GROUP BY 
            r.Region, c.Country
    )
    SELECT Region, Country, CountryTotal, CountryRegionalRank
    FROM CountryTotals
    WHERE CountryRegionalRank = 1
    ORDER BY Region ASC
    """
    return sql_statement


def ex8(conn):
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    # WRITE YOUR CODE HERE
    sql_statement = """
    WITH CustomerSales AS (
        SELECT 
            CASE 
                WHEN CAST(SUBSTR(od.OrderDate, 6, 2) AS INTEGER) IN (1, 2, 3) THEN 'Q1'
                WHEN CAST(SUBSTR(od.OrderDate, 6, 2) AS INTEGER) IN (4, 5, 6) THEN 'Q2'
                WHEN CAST(SUBSTR(od.OrderDate, 6, 2) AS INTEGER) IN (7, 8, 9) THEN 'Q3'
                ELSE 'Q4'
            END AS Quarter,
            CAST(SUBSTR(od.OrderDate, 1, 4) AS INTEGER) AS Year,
            od.CustomerID,
            ROUND(SUM(od.QuantityOrdered * p.ProductUnitPrice)) AS Total
        FROM 
            OrderDetail od
            JOIN Product p ON od.ProductID = p.ProductID
        GROUP BY 
            Quarter, Year, od.CustomerID
    )
    SELECT Quarter, Year, CustomerID, Total
    FROM CustomerSales
    ORDER BY Year, Quarter, CustomerID
    """
    return sql_statement


def ex9(conn):
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    # WRITE YOUR CODE HERE

    sql_statement = """
    WITH CustomerSales AS (
        SELECT 
            CASE 
                WHEN CAST(SUBSTR(od.OrderDate, 6, 2) AS INTEGER) IN (1, 2, 3) THEN 'Q1'
                WHEN CAST(SUBSTR(od.OrderDate, 6, 2) AS INTEGER) IN (4, 5, 6) THEN 'Q2'
                WHEN CAST(SUBSTR(od.OrderDate, 6, 2) AS INTEGER) IN (7, 8, 9) THEN 'Q3'
                ELSE 'Q4'
            END AS Quarter,
            CAST(SUBSTR(od.OrderDate, 1, 4) AS INTEGER) AS Year,
            od.CustomerID,
            ROUND(SUM(od.QuantityOrdered * p.ProductUnitPrice)) AS Total
        FROM 
            OrderDetail od
            JOIN Product p ON od.ProductID = p.ProductID
        GROUP BY 
            Quarter, Year, od.CustomerID
    ),
    RankedSales AS (
        SELECT 
            Quarter, 
            Year, 
            CustomerID, 
            Total,
            RANK() OVER (PARTITION BY Quarter, Year ORDER BY Total DESC) AS CustomerRank
        FROM 
            CustomerSales
    )
    SELECT Quarter, Year, CustomerID, Total, CustomerRank
    FROM RankedSales
    WHERE CustomerRank <= 5
    ORDER BY Year, Quarter, CustomerRank
    """
    return sql_statement


def ex10(conn):
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total

    # WRITE YOUR CODE HERE

    sql_statement = """
    WITH MonthlySales AS (
        SELECT 
            SUM(ROUND(Product.ProductUnitPrice * OrderDetail.QuantityOrdered, 0)) AS Total,
            CASE CAST(SUBSTR(OrderDetail.OrderDate, 6, 2) AS INTEGER)
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February'
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'
                WHEN 8 THEN 'August'
                WHEN 9 THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
            END AS Month
        FROM OrderDetail
        JOIN Product ON OrderDetail.ProductID = Product.ProductID
        GROUP BY Month
    )
    SELECT 
        Month,
        Total,
        ROW_NUMBER() OVER (ORDER BY Total DESC) AS TotalRank
    FROM MonthlySales
    ORDER BY Total DESC
    """
    return sql_statement


def ex11(conn):
    # Find the MaxDaysWithoutOrder for each customer
    # Output Columns:
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate,
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    # WRITE YOUR CODE HERE
    sql_statement = """
    WITH OrderDates AS (
        SELECT
            CustomerID,
            OrderDate,
            LAG(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousOrderDate
        FROM
            OrderDetail
    ),
    DaysBetweenOrders AS (
        SELECT
            od.CustomerID,
            c.FirstName,
            c.LastName,
            co.country AS Country,
            od.OrderDate,
            od.PreviousOrderDate,
            JULIANDAY(od.OrderDate) - JULIANDAY(od.PreviousOrderDate) AS DaysBetweenOrders
        FROM
            OrderDates od
            JOIN Customer c ON od.CustomerID = c.CustomerID
            JOIN Country co ON c.CountryID = co.CountryID
        WHERE
            od.PreviousOrderDate IS NOT NULL
    )
    SELECT
        CustomerID,
        FirstName,
        LastName,
        Country,
        OrderDate,
        PreviousOrderDate,
        MAX(DaysBetweenOrders) AS MaxDaysWithoutOrder
    FROM
        DaysBetweenOrders
    GROUP BY
        CustomerID
    ORDER BY
        MaxDaysWithoutOrder DESC
    """
    return sql_statement


