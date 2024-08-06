import os, sys, sqlite3, polars as pl

# Create a DataFrame
data = {
    'employee_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Ella'],
    'email': ['alice@example.com', 'bob@example.com', 'bob@example.com', 'david@example.com', 'ella@example']
}

df = pl.DataFrame(data)
print("Original DataFrame:")
print(df)

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db")

# create table based on dataframe
df.write_database(
    table_name = "test123",
    connection = "C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db",
    #engine = "sqlalchemy",
)
