import pandas as pd, sqlite3

# read csv file and skip malformed records
df = pd.read_csv(file_name, on_bad_lines='skip')
print(df)


# drop duplicates
df = df.drop_duplicates()

# drop any records with nulls
df = df.dropna(how='any',axis=0)

# count records in the dataframe
csv_row_count = len(df.index)

# Convert DataFrame to list of tuples
# required to use bulk load
df = list(df.itertuples(index=False, name=None))
print(df)

# clear target table
cur = conn.cursor()
cur.execute('delete from test')
conn.commit()

conn.executemany("INSERT INTO test (Name, Age, City, Salary) VALUES (?, ?, ?, ?)",data_tuples)

# Commit the changes 
conn.commit()

# Verify by querying the table
#print('quering table:')
table_row_count = pd.read_sql('select count(*) from test', conn)
table_row_count = table_row_count['count(*)'][0]

# close the connection
conn.close()

# check row counts
print(f'Number of rows in original file: {csv_row_count}')
print(f'Number of rows in target table: {table_row_count}')