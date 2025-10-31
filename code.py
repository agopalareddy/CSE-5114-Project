import os
import warnings
import pybaseball as pyb
import pandas as pd
from snowflake_connect import setup_snowflake_connection, upload_data_to_snowflake
from dotenv import load_dotenv

con = setup_snowflake_connection()  # Capture the returned connection


# Suppress FutureWarnings from pybaseball's postprocessing
warnings.filterwarnings("ignore", category=FutureWarning, module="pybaseball")

# Set pandas to show all columns
pd.set_option("display.max_columns", None)

# CONFIGURATIONS
export_dir = "exports"
eda_dir = "eda"
docs_dir = "docs"

# --- RE-RUN YOUR CODE TO GET DATA ---
data = pyb.statcast(start_dt="2024-07-01", end_dt="2024-07-02")

# Drop these columns as they are empty
data = data.drop(
    columns=[
        "spin_dir",
        "spin_rate_deprecated",
        "break_angle_deprecated",
        "break_length_deprecated",
        "tfs_deprecated",
        "tfs_zulu_deprecated",
        "umpire",
        "sv_id",
    ]
)

# export column names to statcast_columns_from_fetch.txt
with open(os.path.join(docs_dir, "statcast_columns_from_fetch.txt"), "w") as f:
    f.write("\n".join(data.columns))

# Exploratory Data Analysis (EDA)
os.makedirs(eda_dir, exist_ok=True)
with open(os.path.join(eda_dir, "zzz.txt"), "w") as f:
    f.write(str(data.info()))

data.describe().to_csv(os.path.join(eda_dir, "describe.csv"))
data.head().to_csv(os.path.join(eda_dir, "head.csv"))
data.tail().to_csv(os.path.join(eda_dir, "tail.csv"))
# pd.DataFrame(data.columns, columns=["columns"]).to_csv(
#     os.path.join(eda_dir, "columns.csv")
# ) # Not needed since columns are already documented

# count na values per column and export to na_counts.csv
na_counts = data.isna().sum()
na_counts = na_counts[na_counts > 0]
na_counts.to_csv(os.path.join(eda_dir, "na_counts.csv"))

# Reset index to avoid Pandas index warnings
data = data.reset_index(drop=True)

# Upload data to Snowflake with smart deduplication
# (checks what's already in Snowflake and only uploads new rows)
print("\n" + "=" * 80)
print("UPLOADING DATA TO SNOWFLAKE")
print("=" * 80)

load_dotenv()
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
STATCAST_TABLE = "STATCAST"

upload_stats = upload_data_to_snowflake(
    connection=con,
    dataframe=data,
    table_name=STATCAST_TABLE,
    database=SF_DATABASE,
    schema=SF_SCHEMA,
    max_retries=3,
    retry_delay=2,
)

print("\n" + "=" * 80)
print("UPLOAD SUMMARY")
print("=" * 80)
print(f"Success: {upload_stats['success']}")
print(f"Total Rows Fetched: {len(data)}")
print(f"Rows Skipped (Already in Snowflake): {upload_stats['rows_skipped']}")
print(f"Rows Uploaded: {upload_stats['rows_uploaded']}")
print(f"Rows Inserted: {upload_stats['rows_inserted']}")
print(f"Timestamp: {upload_stats['timestamp']}")
print(f"Message: {upload_stats['message']}")
if upload_stats["errors"]:
    print(f"Errors: {upload_stats['errors']}")

# Show tables in snowflake
print("\n" + "=" * 80)
print("TABLES IN SNOWFLAKE")
print("=" * 80)
cs = con.cursor()
try:
    cs.execute("SHOW TABLES;")
    tables = cs.fetchall()
    print("Tables in Snowflake:")
    for table in tables:
        print(f"  - {table[1]}")
finally:
    cs.close()
    con.close()  # Close the connection
