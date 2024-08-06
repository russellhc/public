import polars as pl

# Create a DataFrame
data = {
    'employee_id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Ella'],
    'email': ['alice@example.com', 'bob@example.com', 'bob@example.com', 'david@example.com', 'ella@example']
}

df = pl.DataFrame(data)
print("Original DataFrame:")
print(df)

# Check for duplicated email addresses
unique_emails = df.select(pl.col('email').is_unique())
print("\nUnique Emails Status:")
print(unique_emails)

dups = df.select(pl.col('email').is_duplicated())
print("\nDuplicated Emails Status:")
print(dups)


# substring the final part from an email address
print("\nSubstring the email domains:")
countries = df.with_columns(
    df['email'].apply(lambda x: x.split(".")[-1] if x is not None else None).alias("country_code")
)
print(countries)

# Identify invalid email addresses
# Polars does not have a direct equivalent for regex validation in the same way as pandas
# We need to use a combination of functions to filter invalid emails

valid_email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
invalid_emails = df.filter(~pl.col('email').str.contains(valid_email_pattern))
remove_invalid_emails = df.filter(pl.col('email').str.contains(valid_email_pattern))
print("\nInvalid Email Addresses:")
print(invalid_emails)
print("\nValid Email Addresses:")
print(remove_invalid_emails)

