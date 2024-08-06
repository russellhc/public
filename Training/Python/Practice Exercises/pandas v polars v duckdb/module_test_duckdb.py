import os, sys, sqlite3, duckdb as ddb, json 
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

        # Create a DuckDB connection
        conn = ddb.connect()
        
        # Load and unnest JSON file into DuckDB
        # query = f"""
        # SELECT 
        #     Name,          
        #     Profile.Age,
        #     Profile.City,
        #     Salary,
        # FROM read_json_auto('{json_file}');
        # """
        #con.execute(query)
        
        query = f"""
        SELECT 
            Name,          
            Profile.Age,
            Profile.City,
            Salary,
        FROM '{json_file}';
        """

        # query = f"""
        # SELECT 
        #     d.Name,          
        #     p.Age,
        #     p.City,
        #     d.Salary,
        # FROM '{json_file}' as d,
        # unnest(d.Profile, recursive:= false) as p(Age, City);
        # """

        # Fetch all data from the query
        data = conn.execute(query).fetchall()
        # columns = [desc[0] for desc in con.execute("DESCRIBE json_data").fetchall()]
        
        # Close DuckDB connection
        conn.close()

        #print(data.head(5))

        #return

        # Convert DataFrame to list of tuples
        #data_tuples = list(data.itertuples(index=False, name=None))
        #print(data_tuples)
        print('convert dataframe to list of tuples ready for load')
        # data_tuples = data.to_numpy().tolist()

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

        conn.executemany("INSERT INTO " + db_table + " (Name, Age, City, Salary) VALUES (?, ?, ?, ?)", data)


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
