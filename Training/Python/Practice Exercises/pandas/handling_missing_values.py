import pandas as pd

data = {
    'customer_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', None, 'David', 'Ella'],
    'age': [25, None, 22, 35, None],
    'email': ['alice@example.com', None, 'charlie@example.com', 'david@example.com', 'ella@example.com']
}

df = pd.DataFrame(data)
print(df)

# Identify columns with null values
null_values = df.isnull().sum()
print(null_values)

# update age null values with mean age
age_mean = df.loc[:, 'age'].mean().astype('int64')
print(age_mean)
df.fillna({'age':age_mean}, inplace=True)
print(df)

# drop records where name or email is null
df.dropna(subset=["name","email"], inplace=True)
print(df)


# drop all records where any field is null
df = df.dropna(how='any',axis=0)