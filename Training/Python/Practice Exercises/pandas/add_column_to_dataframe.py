import pandas as pd
from datetime import datetime

# Sample data
data = {
    'column1': [1, 2, 3],
    'column2': ['A', 'B', 'C']
}

# Create a DataFrame
df = pd.DataFrame(data)

# Add an ingest_time field with the current timestamp
df['ingest_time'] = datetime.now()

# Display the DataFrame
print(df)