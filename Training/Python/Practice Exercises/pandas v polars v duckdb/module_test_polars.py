import os, sys, sqlite3, polars as pl, json 
from datetime import datetime

def switch_current_dir():
    # current directory
    print("Current working directory: {0}".format(os.getcwd()))

    # change directory to file path
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    # current directory
    print("Current working directory: {0}".format(os.getcwd()))

def load_nested_json_to_table(json_file, db_conn, db_table):
    try:
        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Start Time: {formatted_datetime}')

        # read the JSON data
        print('read json data file')
        # with open(json_file, 'r') as file:
        #     json_data = json.load(file)
        json_data = pl.read_json(json_file)

        # print json
        # print(json_data)
        # print(json_data.head(5))

        # Prepare and insert the data
        #for employee in json_data:
        #    Name = employee['Name']
        #    Age = employee['Profile']['Age']
        #    City = employee['Profile']['City']
        #    Salary = employee['Salary']
        #    Bonus = employee['Salary'] * 0.2

        # normalize json
        print('normalise nested json elements')
        #data = pd.json_normalize(json_data, 'Profile',['Age','City'])
        # data = pl.json_normalize(json_data)
        data = json_data.unnest("Profile")
        # print(data.head(5))
        

        # rename
        #data.rename(columns={
        #    'Profile.Age': 'Age',
        #    'Profile.City': 'City'
        #}, inplace=True)
        #print(data)

        # Convert DataFrame to list of tuples
        #data_tuples = list(data.itertuples(index=False, name=None))
        #print(data_tuples)
        print('convert dataframe to list of tuples ready for load')
        data_tuples = data.to_numpy().tolist()
        # data_tuples = data.to_dicts()

        # Connect to the SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(db_conn)
        print('connect to db')
        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Detailed Timestamp: {formatted_datetime}')

        # Create a cursor object
        #cursor = conn.cursor()

        # insert
        conn.executemany("INSERT INTO " + db_table + " (Name, Age, City, Salary) VALUES (?, ?, ?, ?)", data_tuples)


        # Commit the changes 
        conn.commit()

        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Detailed Timestamp: {formatted_datetime}')

        # Verify by querying the table
        #print('quering table:')
        #df = pd.read_sql('SELECT * FROM test', conn)
        #print(df.all)

        # close the connection
        conn.close()

        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'End Time: {formatted_datetime}')

    except Exception as e:
        # Handle any other exceptions
        print(f"An error occurred:")
        print(f"{e}")
        # close the connection
        conn.close()
        return []


def load_nested_json_to_table_slow(json_file, db_conn, db_table):
    try:
        # read the JSON data
        with open(json_file, 'r') as file:
            json_data = json.load(file)
        #json_data = pd.read_json('sample_json_nested_dups.json')

        # print json
        # print(json_data)
        print('read json data file')

        # Connect to the SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(db_conn)
        print('connect to db')
        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Detailed Timestamp: {formatted_datetime}')

        # Create a cursor object
        cursor = conn.cursor()

        # Prepare and insert the data
        for employee in json_data:
            Name = employee['Name']
            Age = employee['Profile']['Age']
            City = employee['Profile']['City']
            Salary = employee['Salary']
            #Bonus = employee['Salary'] * 0.2
            
            insert_query = '''
            INSERT INTO test (Name, Age, City, Salary)
            VALUES (?, ?, ?, ?)
            '''
            cursor.execute(insert_query, (Name, Age, City, Salary))

        # Commit the changes 
        conn.commit()

        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Detailed Timestamp: {formatted_datetime}')

        # Verify by querying the table
        #print('quering table:')
        #df = pd.read_sql('SELECT * FROM test', conn)
        #print(df.all)

        # close the connection
        conn.close()
    except Exception as e:
        # Handle any other exceptions
        print(f"An error occurred:")
        print(f"{e}")
        # close the connection
        conn.close()
        return []
    
def load_csv_to_table(file_name, db_conn, db_table):
    try:
        # read csv
        df = pl.read_csv(file_name)

        # Connect to the SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(db_conn)
        print('connect to db')
        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Detailed Timestamp: {formatted_datetime}')

        # Create a cursor object
        cursor = conn.cursor()

        # create table
        create_query = '''create table if not exists uber_data (
                                driver_id INT NOT NULL,
                                ride_key VARCHAR(255) NOT NULL,
                                fare_amount DECIMAL(10, 2) NOT NULL,
                                pickup_datetime TIMESTAMP NOT NULL,
                                pickup_longitude DECIMAL(9, 6) NOT NULL,
                                pickup_latitude DECIMAL(9, 6) NOT NULL,
                                dropoff_longitude DECIMAL(9, 6),
                                dropoff_latitude DECIMAL(9, 6),
                                passenger_count INT NOT NULL
                            );'''

        # execute create statement    
        cursor.execute(create_query)

        # convert to tuples so batch load works
        data_tuples = df.to_numpy().tolist()

        # insert
        conn.executemany("INSERT INTO " + db_table + " (driver_id,ride_key,fare_amount,pickup_datetime,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count) VALUES (?,?,?,?,?,?,?,?,?)", data_tuples)

        # Commit the changes 
        conn.commit()

        # Get current date and time
        current_datetime = datetime.now()
        # Format the datetime object into a string
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # Print the formatted timestamp
        print(f'Detailed Timestamp: {formatted_datetime}')

        # Verify by querying the table
        #print('quering table:')
        # Query to count the rows in the target table
        cursor.execute("select count(*) from " + db_table)
        row_count = cursor.fetchone()[0]
        print(row_count)

        # close the connection
        conn.close()
    except Exception as e:
        # Handle any other exceptions
        print(f"An error occurred:")
        print(f"{e}")
        # close the connection
        conn.close()
        return []
