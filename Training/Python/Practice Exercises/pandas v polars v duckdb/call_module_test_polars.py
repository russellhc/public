import os, sys, sqlite3, pandas as pd, json, polars as pl
import module_test_polars as md

md.switch_current_dir()

# md.load_nested_json_to_table('sample_json_nested_1m.json','C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db','test')

md.load_csv_to_table('uber.csv','C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db','uber_data')

