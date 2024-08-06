import os, sys, sqlite3, pandas as pd, json

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# change directory to file path
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# read the JSON data
with open('data files\\sample_json_nested.json', 'r') as file:
    json_data = json.load(file)
#json_data = pd.read_json('data files\\sample_json_nested_dups.json')

# print json
print(json_data)

# Count the number of rows
json_row_count = 0
for row in json_data:
    json_row_count += 1

print(f'Number of rows: {json_row_count}')


# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db')


# normalize json
data = pd.json_normalize(json_data)
print(data)

# Convert DataFrame to list of tuples
# required to use bulk load
data_tuples = list(data.itertuples(index=False, name=None))
print(data_tuples)

# clear target table
cur = conn.cursor()
cur.execute('delete from test')
conn.commit()

conn.executemany("INSERT INTO test (Name, Age, City, Salary) VALUES (?, ?, ?, ?)",data_tuples)

# Commit the changes 
conn.commit()

# Verify by querying the table
#print('quering table:')
df = pd.read_sql('select count(*) from test', conn)
table_row_count = df['count(*)'][0]

# close the connection
conn.close()

# check row counts
print(f'Number of rows in original file: {json_row_count}')
print(f'Number of rows in target table: {table_row_count}')
