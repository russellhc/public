import os, sys, sqlite3, pandas as pd, json
import module_test as md

md.switch_current_dir()

md.load_nested_json_to_table('sample_json_nested_1m.json','C:\\Users\\caffi\\OneDrive\\Documents\\SQLiteStudio\\db\\demo.db','test')

print('error handling is working')

