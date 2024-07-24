import os, sqlite3, pandas as pd

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# change directory to file path
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# read file
# df = pd.read_csv('test_data.csv')
df = pd.read_csv('test_data_unclean.csv')

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db')

# Load data into a new table
# Convert DataFrame to list of tuples
# required to use bulk load
data_tuples = list(df.itertuples(index=False, name=None))

# clear target table
cur = conn.cursor()
cur.execute('delete from sample_data')
conn.commit()

conn.executemany("INSERT INTO sample_data (submission_key, submission_date, first_name, last_name, email, ip_address) VALUES (?, ?, ?, ?, ?, ?)",data_tuples)

# Commit the changes 
conn.commit()