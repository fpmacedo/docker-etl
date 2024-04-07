import csv
import datetime
import json
import psycopg2
import os
import configparser
from sql_queries import create_raw_temp_table, insert_raw_temp_table, upsert_raw_table

config = configparser.ConfigParser()
config.read('./etl.cfg')

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
conn.autocommit = True
cur = conn.cursor()

# List to store the names of CSV files
csv_files = []

# Loop through each file in the specified folder
for file_name in os.listdir('./data'):
    # Check if the file is a CSV by its extension
    if file_name.endswith('.csv'):
        csv_files.append(file_name)
    
    # Sort the list of CSV file names
    csv_files_sorted = sorted(csv_files)
    

# Open the CSV file
for csv_file_name in csv_files_sorted:
    print(f"Start insert data: {datetime.datetime.now()}")
    #create raw_temp table
    cur.execute(create_raw_temp_table)
    #conn.commit()

    with open(f'data/{csv_file_name}', mode='r') as file:
        reader = csv.DictReader(file)

    # Iterate over the CSV rows
        for row in reader:
            # Parse the JSON string in 'array_trackingEvents' column
            tracking_events = json.loads(row['array_trackingEvents'].replace("'", '"').replace("None", 'null'))
            # Execute the INSERT query with the parsed JSON and other column values
            cur.execute(insert_raw_temp_table, (row['oid__id'],
                                                row['Op'],
                                                row['oid__id'],
                                                row['createdAt'],
                                                row['updatedAt'],
                                                row['lastSyncTracker'],
                                                json.dumps(tracking_events)))
    
    cur.execute(upsert_raw_table)

print(f"Data inserted successfully: {datetime.datetime.now()}")

# Close the cursor and the connection
cur.close()
conn.close()