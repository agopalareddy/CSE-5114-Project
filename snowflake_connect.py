import os
import base64
import time
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
import pandas as pd


def setup_snowflake_connection():
    """
    Set up and verify Snowflake connection.

    Returns:
        snowflake.connector.SnowflakeConnection: The established connection object.

    Raises:
        ValueError: If neither SF_PRIVATE_KEY_FILE nor SF_PRIVATE_KEY_B64 is provided.
    """
    env_path = os.path.join(os.path.dirname(__name__), ".env")
    load_dotenv(env_path)

    # Setup Snowflake Connection
    SF_ACCOUNT = os.getenv("SF_ACCOUNT")
    SF_USER = os.getenv("SF_USER")
    SF_PRIVATE_KEY_B64 = os.getenv("SF_PRIVATE_KEY_B64")
    SF_AUTHENTICATOR = os.getenv("SF_AUTHENTICATOR", "snowflake")
    SF_DATABASE = os.getenv("SF_DATABASE")
    SF_SCHEMA = os.getenv("SF_SCHEMA")
    SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
    SF_PRIVATE_KEY_FILE = os.getenv("SF_PRIVATE_KEY_FILE")
    private_key = None

    # Prepare private key - use file if available, otherwise decode base64
    if SF_PRIVATE_KEY_FILE and os.path.exists(SF_PRIVATE_KEY_FILE):
        with open(SF_PRIVATE_KEY_FILE, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
    elif SF_PRIVATE_KEY_B64:
        # Decode base64-encoded private key
        private_key_bytes = base64.b64decode(SF_PRIVATE_KEY_B64)
        private_key = serialization.load_der_private_key(
            private_key_bytes, password=None, backend=default_backend()
        )
    else:
        raise ValueError(
            "Either SF_PRIVATE_KEY_FILE or SF_PRIVATE_KEY_B64 must be provided"
        )

    con = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=private_key,
        authenticator=SF_AUTHENTICATOR,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
        session_parameters={
            "QUERY_TAG": "CSE 5114 Project Statcast Data Fetch",
        },
    )

    # verify connection
    cs = con.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        if one_row:
            print(one_row[0])
        else:
            print("No data returned from query.")
    finally:
        cs.close()

    print("Snowflake connection verified successfully!")
    return con  # Return the connection object


def get_existing_keys_from_snowflake(
    connection, table_name, database, schema, key_columns
):
    """
    Fetch all existing composite keys from Snowflake table.

    Key: (game_pk, at_bat_number, pitch_number)

    Args:
        connection: Snowflake connection object
        table_name: Target table name
        database: Database name
        schema: Schema name
        key_columns: List of column names that make up the composite key

    Returns:
        set: Set of tuples representing existing composite keys, or empty set if table doesn't exist
    """
    cursor = connection.cursor()
    full_table_name = f"{database}.{schema}.{table_name.upper()}"

    try:
        # Check if table exists first
        cursor.execute(f"DESC TABLE {full_table_name}")
    except snowflake.connector.errors.ProgrammingError:
        # Table doesn't exist yet
        return set()

    try:
        # Fetch all composite keys from the table
        # Use uppercase column names as Snowflake stores them
        key_cols_str = ", ".join([f'"{col}"' for col in key_columns])
        query = f"SELECT DISTINCT {key_cols_str} FROM {full_table_name}"
        cursor.execute(query)

        # Convert result to set of tuples for fast lookup
        existing_keys = set()
        for row in cursor.fetchall():
            existing_keys.add(row)

        print(f"[INFO] Found {len(existing_keys)} existing records in Snowflake")
        return existing_keys

    finally:
        cursor.close()


def filter_new_rows(dataframe, existing_keys, key_columns):
    """
    Filter dataframe to only include rows with keys not in Snowflake.

    Args:
        dataframe: pandas DataFrame to filter
        existing_keys: set of tuples representing existing composite keys in Snowflake
        key_columns: List of column names that make up the composite key

    Returns:
        tuple: (filtered_df, num_new_rows, num_duplicates)
    """
    if not existing_keys:
        # No existing data, all rows are new
        return dataframe, len(dataframe), 0

    # Create a composite key column in the dataframe
    df_copy = dataframe.copy()
    df_copy["_composite_key"] = df_copy[key_columns].apply(tuple, axis=1)

    # Filter to only rows not in existing_keys
    mask = ~df_copy["_composite_key"].isin(existing_keys)
    filtered_df = df_copy[mask].copy()

    # Remove the temporary key column
    filtered_df = filtered_df.drop(columns=["_composite_key"])

    num_new_rows = len(filtered_df)
    num_duplicates = len(dataframe) - num_new_rows

    return filtered_df, num_new_rows, num_duplicates


def remove_internal_duplicates(dataframe, key_columns):
    """
    Remove duplicate rows from dataframe based on composite key.

    Args:
        dataframe: pandas DataFrame to deduplicate
        key_columns: List of column names that make up the composite key

    Returns:
        tuple: (deduplicated_df, num_removed)
    """
    initial_count = len(dataframe)
    deduplicated_df = dataframe.drop_duplicates(subset=key_columns, keep="first").copy()
    num_removed = initial_count - len(deduplicated_df)

    return deduplicated_df, num_removed


def validate_data_quality(dataframe):
    """
    Validate data quality before uploading to Snowflake.

    Args:
        dataframe: pandas DataFrame to validate

    Returns:
        tuple: (is_valid: bool, issues: list of strings)
    """
    issues = []

    # Check for critical columns
    required_columns = [
        "game_pk",
        "pitcher",
        "batter",
        "pitch_number",
        "game_date",
        "home_team",
        "away_team",
    ]
    missing_cols = [col for col in required_columns if col not in dataframe.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")

    # Check for null values in key columns
    for col in required_columns:
        if col in dataframe.columns and dataframe[col].isna().any():
            null_count = dataframe[col].isna().sum()
            issues.append(f"Column '{col}' has {null_count} null values")

    # Check row count
    if len(dataframe) == 0:
        issues.append("DataFrame is empty")

    is_valid = len(issues) == 0
    return is_valid, issues


def validate_table_schema(connection, table_name, database, schema, dataframe):
    """
    Validate or create table schema in Snowflake.

    Args:
        connection: Snowflake connection object
        table_name: Target table name
        database: Database name
        schema: Schema name
        dataframe: pandas DataFrame (used for schema inference)

    Returns:
        bool: True if schema is valid or created successfully
    """
    cursor = connection.cursor()
    full_table_name = f"{database}.{schema}.{table_name}"

    try:
        # Check if table exists
        cursor.execute(f"DESC TABLE {full_table_name}")
        print(
            f"Table {full_table_name} exists. Schema will be validated by write_pandas."
        )
        return True
    except snowflake.connector.errors.ProgrammingError:
        # Table doesn't exist, will be created by write_pandas
        print(
            f"Table {full_table_name} does not exist. It will be created during data upload."
        )
        return True
    finally:
        cursor.close()


def upload_data_to_snowflake(
    connection, dataframe, table_name, database, schema, max_retries=3, retry_delay=2
):
    """
    Upload a pandas DataFrame to Snowflake using smart deduplication.

    Only uploads rows with composite keys not already in Snowflake.
    Composite key: (game_pk, at_bat_number, pitch_number)
    These three fields uniquely identify each individual pitch in the dataset.

    Args:
        connection: Snowflake connection object
        dataframe: pandas DataFrame to upload
        table_name: Target table name
        database: Database name
        schema: Schema name
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay (seconds) between retries (exponential backoff)

    Returns:
        dict: Upload statistics including:
            - success: bool
            - rows_uploaded: int (new rows only)
            - rows_skipped: int (already in Snowflake)
            - rows_inserted: int
            - timestamp: datetime of upload
            - message: str description
            - errors: list of errors (if any)
    """
    stats = {
        "success": False,
        "rows_uploaded": 0,
        "rows_skipped": 0,
        "rows_inserted": 0,
        "timestamp": datetime.now(),
        "message": "",
        "errors": [],
    }

    # Define composite key columns - these should uniquely identify each pitch
    # game_pk: Unique game ID
    # at_bat_number: Unique plate appearance number within the game
    # pitch_number: Unique pitch number within that at-bat
    key_columns = ["game_pk", "at_bat_number", "pitch_number"]

    # Step 1: Data quality validation
    print("\n[STEP 1] Validating data quality...")
    is_valid, issues = validate_data_quality(dataframe)
    if not is_valid:
        for issue in issues:
            print(f"  [WARNING] {issue}")
            stats["errors"].append(issue)
        stats["message"] = "Data quality validation failed"
        return stats
    print(f"  [OK] Data quality validation passed ({len(dataframe)} rows)")

    # Step 1b: Remove internal duplicates in the fetched data
    print("\n[STEP 1b] Removing internal duplicates...")
    df_deduped = dataframe.drop_duplicates(subset=key_columns, keep="first")
    internal_dupes = len(dataframe) - len(df_deduped)
    if internal_dupes > 0:
        print(
            f"  [INFO] Removed {internal_dupes} internal duplicate rows ({len(dataframe)} -> {len(df_deduped)})"
        )
    else:
        print(f"  [OK] No internal duplicates found")

    # Step 2: Check for existing keys in Snowflake
    print("\n[STEP 2] Checking Snowflake for existing records...")
    existing_keys = get_existing_keys_from_snowflake(
        connection, table_name, database, schema, key_columns
    )

    # Step 3: Filter out rows that already exist
    print("\n[STEP 3] Filtering out rows already in Snowflake...")
    filtered_df, num_new, num_duplicates = filter_new_rows(
        df_deduped, existing_keys, key_columns
    )

    if num_duplicates > 0:
        print(f"  [INFO] Skipping {num_duplicates} rows already in Snowflake")
    else:
        print(f"  [OK] No rows already in Snowflake")
    print(f"  [OK] {num_new} new rows ready to upload")

    stats["rows_skipped"] = num_duplicates

    # If no new rows, return early
    if num_new == 0:
        print("\n[STEP 4] No new data to upload")
        stats["success"] = True
        stats["message"] = "No new rows to upload (all rows already in Snowflake)"
        return stats

    # Step 4: Schema validation
    print("\n[STEP 4] Validating/Creating table schema...")
    try:
        validate_table_schema(connection, table_name, database, schema, filtered_df)
        print("  [OK] Table schema validated/created")
    except Exception as e:
        stats["errors"].append(str(e))
        stats["message"] = f"Schema validation failed: {str(e)}"
        return stats

    # Step 5: Upload with retry logic
    print("\n[STEP 5] Uploading new data to Snowflake...")
    full_table_name = f"{table_name.upper()}"

    for attempt in range(max_retries):
        try:
            # Use write_pandas for efficient bulk loading
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=filtered_df,
                table_name=full_table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
                overwrite=False,  # Important: don't overwrite, append instead
                use_logical_type=True,
            )

            if success:
                print(
                    f"  [OK] Successfully uploaded {nrows} new rows in {nchunks} chunks"
                )
                stats["success"] = True
                stats["rows_uploaded"] = nrows
                stats["rows_inserted"] = nrows
                stats["message"] = (
                    f"Successfully uploaded {nrows} new rows (skipped {num_duplicates} duplicates)"
                )

                return stats
            else:
                raise Exception("write_pandas returned success=False")

        except Exception as e:
            stats["errors"].append(str(e))

            if attempt < max_retries - 1:
                wait_time = retry_delay * (2**attempt)  # Exponential backoff
                print(f"  [RETRY] Attempt {attempt + 1} failed: {str(e)}")
                print(
                    f"  [WAIT] Retrying in {wait_time} seconds... (Attempt {attempt + 2}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                print(f"  [ERROR] All {max_retries} upload attempts failed")
                stats["message"] = f"Upload failed after {max_retries} attempts"
                return stats

    return stats


if __name__ == "__main__":
    con = setup_snowflake_connection()
    # Close the connection after use
    con.close()
