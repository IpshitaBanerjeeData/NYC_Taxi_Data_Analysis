import pandas as pd
import numpy as np

file_path = r'C:\Users\ipshi\OneDrive\Desktop\Analyzing Urban Traffic and Public Transit for City Planning\yellow_tripdata_2025-07.parquet'

import pandas as pd
import numpy as np

# Define the path to your downloaded Parquet file
file_path = r'C:\Users\ipshi\OneDrive\Desktop\Analyzing Urban Traffic and Public Transit for City Planning\yellow_tripdata_2025-07.parquet'

# Load the data into a Pandas DataFrame
try:
    df = pd.read_parquet(file_path)
    print("Data loaded successfully!")
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# Calculate trip duration in minutes and add it as a new column
df['trip_duration_minutes'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60

# --- Initial Inspection ---
# Display information about the DataFrame
print("\nInitial Data Information:")
print(df.info())

# Show the first 5 rows to understand the structure
print("\nFirst 5 rows of the data:")
print(df.head())

# --- Step-by-Step Data Cleaning and Preparation ---

# 1. Handle Missing Values:
# For our analysis, let's assume missing values in a few columns can be replaced with 0.
df['Airport_fee'] = df['Airport_fee'].fillna(0)
df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)
df['cbd_congestion_fee'] = df['cbd_congestion_fee'].fillna(0)

# 2. Filter Out Illogical Data:
# Trip duration cannot be negative or extremely long.
# A reasonable upper limit is 24 hours (1440 minutes).
df = df[(df['trip_duration_minutes'] >= 0) & (df['trip_duration_minutes'] <= 1440)]

# Trip distance cannot be zero or negative.
df = df[df['trip_distance'] > 0]

# Passenger count cannot be zero or negative.
df = df[df['passenger_count'] > 0]

# Fare amount should be positive.
df = df[df['fare_amount'] > 0]

# 3. Handle Duplicates:
# We'll check for any full-row duplicates and remove them.
initial_rows = len(df)
df.drop_duplicates(inplace=True)
duplicates_removed = initial_rows - len(df)
if duplicates_removed > 0:
    print(f"\nRemoved {duplicates_removed} duplicate rows.")

# 4. Correct data types (optional but good practice)
# If passenger_count` is a float, let's convert it to an integer.
df['passenger_count'] = df['passenger_count'].astype('int64')

# --- Final Inspection of Cleaned Data ---

print(f"\nInitial row count: {initial_rows}")
print(f"Final row count after filtering: {len(df)}")
print(f"Total rows removed: {initial_rows - len(df)}")

print("\nCleaned Data Information:")
print(df.info())

print("\nCleaned Data Summary Statistics:")
print(df.describe())

# --- Save the Cleaned Data ---
cleaned_file_path_csv = 'cleaned_nyc_taxi_data.csv'
cleaned_file_path_parquet = 'cleaned_nyc_taxi_data.parquet'

df.to_csv(cleaned_file_path_csv, index=False)
df.to_parquet(cleaned_file_path_parquet, index=False)

print(f"\nCleaned data saved to: {cleaned_file_path_csv}")
print(f"Cleaned data saved to: {cleaned_file_path_parquet}")

#--- Loading data into database---
import sqlite3

# Load the cleaned Parquet file
df = pd.read_parquet('cleaned_nyc_taxi_data.parquet')

# Create a new SQLite database file
conn = sqlite3.connect('nyc_taxi.db')

# Load the DataFrame into a new SQL table called 'taxi_trips'
# The 'if_exists' parameter will replace the table if it already exists
df.to_sql('taxi_trips', conn, if_exists='replace', index=False)

print("Data successfully loaded into 'taxi_trips' table in 'nyc_taxi.db'")

# Close the connection
conn.close()

#--- Load the taxi zone data into database---
import sqlite3

# Define the path to your downloaded taxi_zones.csv file
file_path = r'C:\Users\ipshi\OneDrive\Desktop\Analyzing Urban Traffic and Public Transit for City Planning\taxi_zone_lookup.csv'

# Create a connection to your existing database
conn = sqlite3.connect('nyc_taxi.db')

# Load the CSV file into a Pandas DataFrame
zones_df = pd.read_csv(file_path)

# Load the DataFrame into a new SQL table called 'taxi_zones'
zones_df.to_sql('taxi_zones', conn, if_exists='replace', index=False)

print("Data from 'taxi_zones.csv' successfully loaded into 'taxi_zones' table.")

# Close the connection
conn.close()