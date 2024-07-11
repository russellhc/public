import pandas as pd, os

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# change directory to file path
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# read csv
df = pd.read_csv('data files\\uber.csv')
print(df.head)


# identify outliers
print(df.describe()['fare_amount'])

# function to return data frame with outliers
def IQR_find_outliers(df, column):

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
    return outliers

# function to return data frame minus the outliers
def IQR_remove_outliers(df, column):

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    revised_data = df.loc[lambda df: ~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
    return revised_data

# function to return data frame minus the outliers
# potentially slower but includes old row index id
def drop_outliers_IQR(df, column):

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    not_outliers = df[~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
    outliers_dropped = not_outliers.dropna().reset_index()
    return outliers_dropped    

#revised_df = IQR_outliers(df, 'fare_amount')
revised_df = df.pipe(IQR_remove_outliers, 'fare_amount')
print(revised_df)

