import pandas as pd

data = {
    'product_id': ['001', '002', '003', '004', '005'],
    'price': ['10.5', '20.3', '30.0', '25.0', '15.8'],
    'in_stock': [1, 0, 1, 1, 0]
}

df = pd.DataFrame(data)
print(df)

# Convert the 'product_id' column to integer type
df['product_id'] = df['product_id'].astype('Int64')
print(df)

# Convert the 'product_id' column to integer type
df['price'] = df['price'].astype('float')
print(df)

# Convert the 'in_stock' column to boolean type
df['in_stock'] = df['in_stock'].astype('bool')
print(df)

# confirm column data types
print(df.dtypes)