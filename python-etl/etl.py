import csv
import json
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('./etl.cfg')
print(*config['DWH'].values())

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
conn.autocommit = True
cursor = conn.cursor()

# Define the path to your CSV file
csv_file_path = './20230904-130010492.csv'

# Define the SQL query to create a new table (modify the types as needed)
create_table_query = """
CREATE TABLE teste1 (
      Op VARCHAR(50),
      oid__id UUID,
      createdAt BIGINT,
      updatedAt BIGINT,
      lastSyncTracker BIGINT,
      array_trackingEvents JSONB,
      PRIMARY KEY (oid__id)
        );
"""
cursor.execute(create_table_query)

# Open the CSV file
with open(csv_file_path, mode='r') as file:
    reader = csv.DictReader(file)

    # Iterate over the CSV rows
    for row in reader:
        # Parse the JSON string in 'array_trackingEvents' column
        tracking_events = json.loads(row['array_trackingEvents'].replace("'", '"').replace("None", 'null'))

        # Prepare the INSERT query. Make sure to list all columns you want to insert data into
        insert_query = """
        INSERT INTO teste1 (Op, oid__id, createdAt, updatedAt, lastSyncTracker, array_trackingEvents)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        # Execute the INSERT query with the parsed JSON and other column values
        cursor.execute(insert_query, (row['Op'],
                                      row['oid__id'],
                                      row['createdAt'],
                                      row['updatedAt'],
                                      row['lastSyncTracker'],
                                      json.dumps(tracking_events)))

print("Data inserted successfully")

# Close the cursor and the connection
cursor.close()
conn.close()