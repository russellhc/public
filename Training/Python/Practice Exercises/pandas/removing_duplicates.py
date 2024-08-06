import pandas as pd

data = {
    'transaction_id': [1, 2, 3, 3, 4, 5, 5, 5],
    'customer_id': [1, 1, 2, 2, 3, 4, 4, 4],
    'amount': [100, 150, 200, 200, 50, 75, 75, 75]
}

df = pd.DataFrame(data)
print(df)

# drop duplicates based on 'transaction_id' keep first
df_unique_transaction = df.drop_duplicates(subset=['transaction_id'], keep="first", inplace=False)
print(df_unique_transaction)

# drop duplicates based on 'customer_id','amount' keep last
df_unique_customer_amount = df.drop_duplicates(subset=['customer_id','amount'], keep="last", inplace=False)
print(df_unique_customer_amount)

# drop all duplicates keep last
df = df.drop_duplicates(keep='last')
