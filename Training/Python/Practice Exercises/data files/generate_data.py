import json
import random
import os

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# change directory to file path
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# current directory
print("Current working directory: {0}".format(os.getcwd()))

# Load the existing 10,000 records from the provided JSON file
with open('sample_json_nested_10k.json', 'r') as file:
    data = json.load(file)

# Get the initial 10,000 records
initial_records = data

# Function to create a new record with slight modifications
def create_new_record(base_record, index):
    new_record = base_record.copy()
    new_record["Name"] += f"_{index}"
    new_record["Salary"] = int(base_record["Salary"] * random.uniform(0.8, 1.2))
    new_record["Profile"]["Age"] = int(base_record["Profile"]["Age"] * random.uniform(0.9, 1.1))
    new_record["Profile"]["City"] += f"_{index}"
    return new_record

# Generate additional records to reach 1 million
records_needed = 1000000 - len(initial_records)
new_records = []

for i in range(records_needed):
    base_record = random.choice(initial_records)
    new_record = create_new_record(base_record, i)
    new_records.append(new_record)

# Combine initial records with the new records
combined_records = initial_records + new_records

# Save the new dataset with 1 million records to a JSON file
output_file_path = 'sample_json_nested_1m.json'
with open(output_file_path, 'w') as output_file:
    json.dump(combined_records, output_file)

print(f"New dataset with 1 million records saved to {output_file_path}")