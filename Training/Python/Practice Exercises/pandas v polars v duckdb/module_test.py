import os, sys, sqlite3, pandas as pd, json 
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
        with open(json_file, 'r') as file:
            json_data = json.load(file)
        #json_data = pd.read_json('sample_json_nested_dups.json')

        # print json
        # print(json_data)
        print('read json data file')

        # Prepare and insert the data
        #for employee in json_data:
        #    Name = employee['Name']
        #    Age = employee['Profile']['Age']
        #    City = employee['Profile']['City']
        #    Salary = employee['Salary']
        #    Bonus = employee['Salary'] * 0.2

        # normalize json
        #data = pd.json_normalize(json_data, 'Profile',['Age','City'])
        data = pd.json_normalize(json_data)
        #print(data)
        print('normalise nested json elements')

        # rename
        #data.rename(columns={
        #    'Profile.Age': 'Age',
        #    'Profile.City': 'City'
        #}, inplace=True)
        #print(data)

        # Convert DataFrame to list of tuples
        data_tuples = list(data.itertuples(index=False, name=None))
        #print(data_tuples)
        print('convert dataframe to list of tuples ready for load')

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

        conn.executemany("INSERT INTO " + db_table + " (Name, Age, City, Salary) VALUES (?, ?, ?, ?)",data_tuples)


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