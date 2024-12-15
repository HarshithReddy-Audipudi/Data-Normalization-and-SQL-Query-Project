# Data-Normalization-and-SQL-Query-Project
This project focuses on normalizing a database and executing various SQL queries to analyze customer orders, sales, and regional performance. The code is written in Python and utilizes SQLite for database operations.

# The project consists of several key components:
Utility Functions
These functions handle database connections, table creation, and SQL execution:
create_connection: Establishes a database connection
create_table: Creates tables in the database
execute_sql_statement: Executes SQL queries and returns results

# Data Normalization Steps
The project includes 11 steps to normalize the database:
Create Region table
Create Region to RegionID dictionary
Create Country table
Create Country to CountryID dictionary
Create Customer table
Create Customer to CustomerID dictionary
Create ProductCategory table
Create ProductCategory to ProductCategoryID dictionary
Create Product table
Create Product to ProductID dictionary
Create OrderDetail table
SQL Queries (Exercises)
The project features 11 SQL exercises that analyze the normalized data:
Fetch order details for a specific customer
Calculate total sales for a given customer
Calculate total sales for all customers
Calculate total sales by region
Calculate total sales by country
Rank countries within regions based on order total
Select top-ranking country in each region
Sum customer sales by quarter and year
Rank top 5 customers by sales in each quarter and year
Rank monthly sales
Find maximum days without order for each customer

#  Usage
To use this project:
Ensure you have Python and the required libraries (pandas, sqlite3) installed.
Place your input data file in the same directory as the script.
Run the normalization steps in order (step1 through step11).
Execute the SQL queries (ex1 through ex11) to analyze the data.

# Note
The code assumes a specific input data format. Ensure your data matches the expected structure.
Some functions require the output of previous steps. Run the steps in the correct order to avoid errors.
Modify the database and data filenames as needed in the function calls.

# Customization
You can customize the SQL queries in the exercise functions (ex1 through ex11) to suit your specific analysis needs. Each function returns an SQL statement string that can be modified to change the query logic.
