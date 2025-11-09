import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import re

load_dotenv()  # loads .env if present

DB_BACKEND = os.getenv("DB_BACKEND", "mysql").lower()  # 'mysql' or 'postgres'
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")   # default mysql port
DB_NAME = os.getenv("DB_NAME", "musemotion_db")
# --- Use the CSV file path ---
DATA_PATH = os.getenv("DATA_PATH", "musemotion_databse.csv")

# driver strings
if DB_BACKEND == "mysql":
    # requires pymysql
    DB_PORT = int(DB_PORT)
    engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
elif DB_BACKEND == "postgres" or DB_BACKEND == "postgresql":
    # requires psycopg2-binary
    if DB_PORT is None or DB_PORT == "":
        DB_PORT = 5432
    DB_PORT = int(DB_PORT)
    engine_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    raise ValueError("Unsupported DB_BACKEND. Use 'mysql' or 'postgres'.")

print("Using engine:", engine_url)
engine = create_engine(engine_url, echo=False)

# --- Read CSV ---
data_path = Path(DATA_PATH)
if not data_path.exists():
    raise FileNotFoundError(f"Data file not found: {data_path}")

# --- Define your 11 columns in order ---
# We will use these as the column names for the DataFrame
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

# Read the CSV. 
# We assume the first row is data, not headers (header=None).
# We assign the correct column_names.
df = pd.read_csv(data_path, header=None, names=column_names)
print("Loaded CSV with shape:", df.shape)

# --- Clean columns: (Already done by assigning names) ---

# --- Convert datatypes ---
# Use the lowercase column names we just assigned
if "year" in df.columns:
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
if "electric_range" in df.columns:
    df["electric_range"] = pd.to_numeric(df["electric_range"], errors="coerce").astype("Int64")
if "vehicle_id" in df.columns:
    df["vehicle_id"] = pd.to_numeric(df["vehicle_id"], errors="coerce").astype("Int64")


# --- Extract lat/long from 'location' column ---
# This regex is for: POINT (lon lat)
point_re = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

def extract_latlon(point_str):
    try:
        if not isinstance(point_str, str):
            return (None, None)
        match = point_re.search(str(point_str))
        if match:
            lon = float(match.group(1)) # Longitude is first
            lat = float(match.group(2)) # Latitude is second
            return (lat, lon)
    except Exception:
        pass
    return (None, None)

# Check for the 'location' column (lowercase)
if "location" in df.columns:
    lat_lon = df["location"].apply(lambda w: extract_latlon(w))
    df["latitude"] = lat_lon.apply(lambda t: t[0])
    df["longitude"] = lat_lon.apply(lambda t: t[1])
else:
    df["latitude"] = None
    df["longitude"] = None

# Reorder columns so latitude/longitude at end (optional)
cols = [c for c in df.columns if c not in ("latitude", "longitude")] + ["latitude", "longitude"]
df = df[cols]

# --- Create table if not exists (DML differs slightly between MySQL and Postgres) ---
# Updated to match your 11 columns + lat/lon
if DB_BACKEND == "mysql":
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS musemotion (
      id INT AUTO_INCREMENT PRIMARY KEY,
      vin VARCHAR(50) UNIQUE,
      city VARCHAR(100),
      year INT,
      make VARCHAR(50),
      model VARCHAR(100),
      vehicle_type VARCHAR(255),
      eligibility VARCHAR(255),
      electric_range INT,
      vehicle_id BIGINT,
      location VARCHAR(255),
      utility VARCHAR(255),
      latitude DOUBLE,
      longitude DOUBLE
    );
    """
else: # Postgres
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS musemotion (
      id SERIAL PRIMARY KEY,
      vin VARCHAR(50) UNIQUE,
      city VARCHAR(100),
      year INT,
      make VARCHAR(50),
      model VARCHAR(100),
      vehicle_type VARCHAR(255),
      eligibility VARCHAR(255),
      electric_range INT,
      vehicle_id BIGINT,
      location VARCHAR(255),
      utility VARCHAR(255),
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION
    );
    """

with engine.connect() as conn:
    conn.execute(text(create_table_stmt))
    conn.commit()
print("Ensured table 'musemotion' exists.")

# --- Load data ---
# We use 'append'. If you run this script multiple times,
# it will add duplicate data unless you clear the table first.
# (The UNIQUE constraint on 'vin' might cause errors if you re-run,
# which is why the 'upsert' logic in your other script is a good idea)
df.to_sql("musemotion", con=engine, if_exists="append", index=False, method="multi", chunksize=500)
print("Loaded rows:", len(df))