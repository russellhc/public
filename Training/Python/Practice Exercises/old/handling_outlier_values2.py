import pandas as pd

df = pd.DataFrame({'Height': [170, 160, 130, 190, 180, 150, 140, 200, 175, 165]})
print(df)


# identify outliers
print(df.describe()['Height'])

# calculate IQR for column Height
Q1 = df['Height'].quantile(0.25)
Q3 = df['Height'].quantile(0.75)
IQR = Q3 - Q1

# identify outliers
threshold = 1.5
outliers = df[(df['Height'] < Q1 - threshold * IQR) | (df['Height'] > Q3 + threshold * IQR)]
print(outliers)

