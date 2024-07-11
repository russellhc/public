import pandas as pd

data = {
    'sale_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'sale_amount': [100, 150, 200, 250, 300, 4000, 1200, 1300, 1400, 9999]
}

df = pd.DataFrame(data)
print(df)


# identify outliers
print(df.describe()['sale_amount'])

# create low and high outliers and IQR value
Q1 = df['sale_amount'].quantile(0.25)
print(Q1)
Q3 = df['sale_amount'].quantile(0.75)
print(Q3)
IQR = Q3 - Q1
print(IQR)

# outliers
threshold = 1.5
outliers = df[(df['sale_amount'] < Q1 - threshold * IQR) | (df['sale_amount'] > Q3 + threshold * IQR)]
print(outliers)

# Remove rows with outliers
df_no_outliers = df[~df.index.isin(outliers.index)]
print(df_no_outliers)