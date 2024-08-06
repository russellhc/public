import pandas as pd

data = {
    'employee_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Ella'],
    'email': ['alice@example.com', 'bob@example.com', 'bob@example.com', 'david@example.com', 'ella@example']
}

df = pd.DataFrame(data)
print(df)


# check for duplicated email addresses
unique_emails = df['email'].is_unique
dups = df['email'].duplicated()
print(dups)


# Identify invalid email addresses
#invalid_emails = df[~df['email'].str.contains(r'^[^@]+@[^@]+\.[^@]+$', regex=True)]
invalid_emails = df[~df['email'].str.contains(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b', regex=True)]
print(invalid_emails)