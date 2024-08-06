import pandas as pd


# read primary data file
df_pandas = pd.read_csv(file_name, on_bad_lines='skip')
print("\nRecord count after loading to pandas dataframe: " + str(len(df_pandas)))

# read second data file
df_blocked_ips_file_name = pd.read_csv(self.blocked_ips_file_name)


# the two lines perform an anti join
# left join on desired field
merged_df = df_pandas.merge(df_blocked_ips_file_name, on='ip_address', how='left', indicator=True)
# filter rows that are only in df1 (left DataFrame)
anti_join_df = merged_df[merged_df['_merge'] == 'left_only']


# load
anti_join_df.to_sql('submissions', con=conn)