import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import re
import urllib
import logging

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()  # loads .env if present

# --- Database Configuration (Reads Azure SQL Server credentials) ---
DB_BACKEND = os.getenv("DB_BACKEND", "mysql").lower()  # Should be 'mssql' from .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME_RAW = os.getenv("DB_NAME") # e.g., "server/database"
DATA_PATH = os.getenv("DATA_PATH", "musemotion_databse.csv") # Local CSV file path

# --- Column Definitions ---
column_names = [
    "vin",
    "city",
    "year",
    "make",
    "model",
    "vehicle_type",
    "eligibility",
    "electric_range",
    "vehicle_id",
    "location",
    "utility"
]

# Regex for extracting lat/long from 'location' column
point_re = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

# --- Engine Setup ---
def get_db_engine():
    """Configures and returns the SQLAlchemy engine for Azure SQL Server."""
    
    # Strip the database name in case it includes the server prefix
    DB_NAME = DB_NAME_RAW.split('/')[-1]
    
    if DB_BACKEND and DB_BACKEND.lower() == "mssql":
        # Ensure the password is URL-encoded
        quoted_pw = urllib.parse.quote_plus(DB_PASSWORD)
        
        # Use ODBC Driver 17 or 18. This is a common requirement for pyodbc
        DRIVER = 'ODBC Driver 17 for SQL Server' 
        
        # Build the engine URL using mssql+pyodbc dialect
        engine_url = (
            f"mssql+pyodbc://{DB_USER}:{quoted_pw}@{DB_HOST}/"
            f"{DB_NAME}?driver={DRIVER}"
        )
        logger.info("Using Azure SQL Server Engine.")
        # 'fast_executemany=True' is optimized for large pandas inserts
        return create_engine(engine_url, fast_executemany=True)
    
    else:
        raise ValueError(f"DB_BACKEND '{DB_BACKEND}' is not supported. This script requires 'mssql'.")

# --- Transformation Functions ---
def extract_latlon(point_str):
    """Extracts latitude and longitude from POINT (lon lat)"""
    try:
        if not isinstance(point_str, str):
            return (pd.NA, pd.NA)
        match = point_re.search(str(point_str))
        if match:
            # POINT (lon lat) -> (lat, lon)
            lon = float(match.group(1)) 
            lat = float(match.group(2)) 
            return (lat, lon)
    except Exception:
        pass
    return (pd.NA, pd.NA)

# --- Main Execution ---
if __name__ == "__main__":
    try:
        engine = get_db_engine()
    except Exception as e:
        logger.error(f"FATAL: Could not connect to database. Error: {e}")
        exit(1)

    # --- Read CSV ---
    data_path = Path(DATA_PATH)
    if not data_path.exists():
        logger.error(f"Data file not found: {data_path}")
        exit(1)

    # Read the CSV assuming no headers, and assign the correct column names
    df = pd.read_csv(data_path, header=None, names=column_names)
    logger.info(f"Loaded CSV with shape: {df.shape}")

    # --- Data Cleaning and Transformation ---
    
    # 1. Convert datatypes
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["electric_range"] = pd.to_numeric(df["electric_range"], errors="coerce").astype("Int64")
    df["vehicle_id"] = pd.to_numeric(df["vehicle_id"], errors="coerce").astype("Int64")

    # 2. Extract lat/long
    if "location" in df.columns:
        lat_lon = df["location"].apply(extract_latlon)
        df["latitude"] = lat_lon.apply(lambda t: t[0])
        df["longitude"] = lat_lon.apply(lambda t: t[1])
        df.drop(columns=["location"], inplace=True) # Drop the raw location field
    else:
        df["latitude"] = pd.NA
        df["longitude"] = pd.NA

    # 3. Final Column Order
    final_cols = [c for c in column_names if c != "location"] + ["latitude", "longitude"]
    df = df[final_cols]
    
    # 4. Drop rows with missing VIN/City (critical fields)
    df.dropna(subset=["vin", "city"], inplace=True)
    logger.info(f"Cleaned and transformed data shape: {df.shape}")

    # --- Create Table Statement (for MS SQL Server) ---
    create_table_stmt = """
    CREATE TABLE musemotion_data (
      vin VARCHAR(50),
      city VARCHAR(100),
      year INT,
      make VARCHAR(50),
      model VARCHAR(100),
      vehicle_type VARCHAR(255),
      eligibility VARCHAR(255),
      electric_range INT,
      vehicle_id BIGINT,
      utility VARCHAR(255),
      latitude FLOAT,
      longitude FLOAT
    );
    """

    # --- Load data ---
    TABLE_NAME = "musemotion_data"
    
    with engine.connect() as conn:
        # Drop the table first to ensure a clean schema creation and load
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        conn.execute(text(create_table_stmt))
        conn.commit()
    logger.info(f"Recreated table '{TABLE_NAME}'.")

    # Load data using pandas' to_sql
    df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False, method="multi", chunksize=500)
    logger.info(f"Successfully loaded {len(df)} rows to '{TABLE_NAME}'.")