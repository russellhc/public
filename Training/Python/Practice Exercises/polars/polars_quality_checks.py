import os, polars as pl

# change directory to current script file path
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Set polars display options to avoid truncation
# pl.Config.set_tbl_cols(100)  # Increase the number of displayed columns
pl.Config.set_tbl_rows(15)  # Increase the number of displayed rows
pl.Config.set_fmt_str_lengths(100)  # Increase the string length

# Load the CSV file - polars
# data_file = "test_data_unclean.csv"
# df = pl.read_csv(data_file)

# for testing. comment out when loading proper file.
df = pl.DataFrame({
    "A": ["foo", "bar", "foo", "baz", "foo", "bar", "foo", "coo", None, "grr", "lax", "lax"],
    "B": [1, 2, 1, 3, 1, 2, None, 9, 5, 4, None,  None],
    "C": [10, 20, 10, 30, 40, 50, 60, 99, 22, None, 7, 7]
})
df2 = pl.DataFrame({
    "C": [99, 22, 7]
})
df3 = pl.DataFrame({
    "A": ["foo", "bar", "baz", "coo", "grr", "lax"],
    "name": ["David", "Gary", "Michelle", "Rose", "George", "Steve"]
})

# print total row count for dataframe
total_rows = df.height
print(f"\nTotal row count of the dataframe: {total_rows}")

# Display the first few rows of the dataframe
print("\nFirst few rows of the dataframe:")
print(df.head(10))

# Check for null values
null_values = df.null_count()
print("\nNull values in each column:")
print(null_values)

# Check for duplicate rows
duplicate_rows = df.filter(df.is_duplicated()).height
print("\nNumber of duplicate rows:")
print(duplicate_rows)
# if duplicate row count > 0 then print the duplicates
if duplicate_rows > 0:
    duplicate_rows_values = df.filter(df.is_duplicated())
    print(duplicate_rows_values.head(10)) # limiting to 10 

# removing duplicate rows
print("\nRemoving duplicate rows")
print(f"Old total row count: " + str(total_rows))
clean_df = df.unique(keep="last", maintain_order=True)
print(f"New total row count: " + str(clean_df.height))
print(f"Rows removed: " + str(total_rows - clean_df.height))

# Check for duplicate values in each column
print("\nchecking for duplicates in individual columns:")
# loop on columns
for c in df.columns:
    duplicate_count = (
        df.group_by(c)
        .agg(pl.len())
        .filter(pl.col("len") > 1)
        .height
    )
    print("Column Name: " + c)
    print("Column Duplicate Count: " + str(duplicate_count))
    print("\n")

# look for duplicates across column combinations
print("\nchecking for duplicates in combinations of columns:")
# set columns to check for duplicates over
column_spec = ["A","B"]
print(f"Duplicates based on columns {column_spec}:")
# count duplicates and filter to them
dups = (
    df.with_columns(
        pl.struct(column_spec).count().over(column_spec).alias("dup_count")
    )
   .filter(pl.col("dup_count") > 1)
)
# Display the duplicates
print(dups)

# removing duplicate rows based on column combination
print("\nRemoving duplicate rows based on column combination")
# set columns to check for duplicates over
# column_spec_dups = ['submission_key','submission_date','first_name','last_name','email','ip_address']
column_spec_dups = ("A", "B")
print(f"Duplicates based on columns {column_spec_dups}:")
print(f"Old total row count: " + str(total_rows))
# clean_df_columns = (
#     df.with_columns(
#         pl.struct(column_spec_dups).cum_count().over(column_spec).alias("dup_count")
#     )
#    .filter(pl.col("dup_count") == 1)
# )
clean_df_columns = df.unique(subset=["A", "B"], keep="first", maintain_order=True) # cannot seem to parameterise the subset columns without it throwing an error
print(f"New total row count: " + str(clean_df_columns.height))
print(f"Rows removed: " + str(total_rows - clean_df_columns.height))
print(clean_df_columns)


# add a derived column to the dataframe
print("\nAdd a derived column to the dataframe")
df_add_column = df.with_columns(
    (pl.col("C") * 5).alias("D")
)
print(df_add_column)

# remove the column
print("\nRemove the column from the dataframe")
df_add_column = df_add_column.drop("D")
print(df_add_column)


# joins
# add a column from another dataframe
print("\nJoin on another dataframe to add a column")
df_joined = df.join(df3, on="A", how="left")
print(df_joined)

# filter out rows based on values in another dataframe
print("\nFilter dataframe based on values in another dataframe")
# filter_values = df2.get_column("C").to_list()
# df_filtered = df.filter(~pl.col("C").is_in(filter_values)) # removes nulls as well...
df_filtered2 = df.join(df2, on="C", how="anti") # does not remove nulls
print("Original dataframe:")
print(df)
#print(df_filtered)
print("Filtered dataframe:")
print(df_filtered2)


# # Examine data types of each column
# data_types = df.dtypes
# print("\nData types of each column:")
# print(data_types)

