import pandas as pd

df = pd.DataFrame({
    'sale_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'sale_amount': [100, 150, 200, 250, 300, 1000, 1200, 1300, 1400, 9999]
})
print(df)


# identify outliers
print(df.describe()['sale_amount'])

def IQR_outliers(df, column):

     Q1 = df[column].quantile(0.25)
     Q3 = df[column].quantile(0.75)
     IQR = Q3 - Q1
     df = df.loc[lambda df: ~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
     return df
    

revised_df = IQR_outliers(df, 'sale_amount')
print(revised_df)

