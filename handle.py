# Libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
from sqlalchemy import create_engine

plt.style.use('default')

# Load CSV file
df = pd.read_csv(r'ecommerce_sales_data.csv',encoding='utf-8')

# Peek at data
print(df.head())

# Convert OrderDate to datetime
df['OrderDate'] = pd.to_datetime(df['OrderDate'])

# Missing Values
print("Missing data:\n", df.isnull().sum())

# Fill or drop
df['Country'].fillna('Unknown', inplace=True)
df['Quantity'].fillna(0, inplace=True)
df['Price'].fillna(df['Price'].mean(), inplace=True)

df.drop_duplicates(inplace=True)

# Add Total Sales
df['TotalSales'] = df['Quantity'] * df['Price']

# Add Year and Month for Time Analysis
df['Year'] = df['OrderDate'].dt.year
df['Month'] = df['OrderDate'].dt.month

monthly_sales = df.groupby(['Year','Month'])['TotalSales'].sum().reset_index()

plt.figure(figsize=(12,5))
sns.lineplot(x='Month', y='TotalSales', hue='Year', data=monthly_sales)
plt.title('Monthly Total Sales Trend')
plt.xlabel('Month')
plt.ylabel('Sales')
plt.show()

top_products = df.groupby('ProductID')['TotalSales'].sum().sort_values(ascending=False).head(10)

plt.figure(figsize=(10,6))
sns.barplot(x=top_products.values, y=top_products.index)
plt.title('Top 10 Products by Sales')
plt.xlabel('Total Sales')
plt.ylabel('ProductID')
plt.show()

country_sales = df.groupby('Country')['TotalSales'].sum().sort_values(ascending=False)

plt.figure(figsize=(12,6))
sns.barplot(x=country_sales.values, y=country_sales.index)
plt.title('Revenue by Country')
plt.show()

conn = sqlite3.connect('ecommerce.db')
df.to_sql('sales', conn, if_exists='replace', index=False)

# Query 1 (MySQL compatible)
query1 = '''
SELECT Category,
       SUM(TotalSales) AS TotalRevenue
FROM sales
GROUP BY Category
ORDER BY TotalRevenue DESC
LIMIT 5
'''

top_categories = pd.read_sql(query1, conn)
print(top_categories)

# Query 2 (MySQL compatible)
query2 = '''
SELECT CustomerID,
       SUM(TotalSales) AS CustomerRevenue
FROM sales
GROUP BY CustomerID
ORDER BY CustomerRevenue DESC
LIMIT 10
'''

top_customers = pd.read_sql(query2, conn)
print(top_customers)

# Query 3 (MySQL syntax used instead of SQLite strftime)
query3 = '''
SELECT DATE_FORMAT(OrderDate, '%Y-%m') AS Month,
       COUNT(OrderID) AS Orders
FROM sales
GROUP BY Month
ORDER BY Month
'''

monthly_orders = pd.read_sql(query3, conn)
print(monthly_orders)

top_products.to_csv('top_products.csv', index=False)
country_sales.to_csv('country_sales.csv', index=False)
monthly_sales.to_csv('monthly_sales.csv', index=False)

print("Exports Completed!")