import pandas as pd
import numpy as np
import re
import os
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

#Load environment variables
load_dotenv()  # read .env in working dir

USER = os.getenv("DB_USER")
PW = os.getenv("DB_PASSWORD") # Standardized to DB_PASSWORD
HOST = os.getenv("DB_HOST", "localhost")
DB = os.getenv("DB_NAME", "musemotion_db")

# --- Use an environment variable, not a hardcoded path ---
DATA_FILE = os.getenv("DATA_PATH", "musemotion_databse.csv")
csv_file = Path(DATA_FILE)
if not csv_file.exists():
    raise FileNotFoundError(f"Data file not found: {csv_file}. Make sure DATA_PATH is set in .env file.")

chunksize = 2000

# SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{USER}:{PW}@{HOST}/{DB}")

# --- CORRECTED column names to match CSV file ---
# Use lowercase_with_underscores to be consistent
columns_to_use = [
    "vin",
    "city",
    "year",
    "make",
    "model",
    "vehicle_type", # Corrected (was "Vehicle Type")
    "eligibility",
    "electric_range", # Corrected (was "Electric Range")
    "vehicle_id",     # Corrected (was "Base MSRP")
    "location",
    "utility"
]

# latitude/longitude extraction
point_re = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

def extract_lat_lon(point_str):
    """Extract latitude and longitude from POINT (lon lat)"""
    if pd.isna(point_str):
        return pd.NA, pd.NA
    match = point_re.search(str(point_str))
    if not match:
        return pd.NA, pd.NA
    lon, lat = match.groups()
    try:
        return float(lat), float(lon)
    except ValueError:
        return pd.NA, pd.NA

# process the file in chunks
cleaned_rows = 0
print(f"Starting to process {csv_file}...")

for i, chunk in enumerate(pd.read_csv(csv_file, usecols=range(len(columns_to_use)), chunksize=chunksize, header=None)):
    # Assign column names
    chunk.columns = columns_to_use

    # Clean text fields
    text_cols = ["vin", "city", "make", "model", "vehicle_type", "eligibility", "utility"]
    for c in text_cols:
        chunk[c] = chunk[c].astype(str).str.strip().replace({"nan": pd.NA, "": pd.NA})

    # Drop rows missing VIN or City
    before = len(chunk)
    chunk.dropna(subset=["vin", "city"], inplace=True)
    after = len(chunk)

    # Convert numeric fields safely (using correct columns)
    chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce").astype("Int64")
    chunk["electric_range"] = pd.to_numeric(chunk["electric_range"], errors="coerce")
    chunk["vehicle_id"] = pd.to_numeric(chunk["vehicle_id"], errors="coerce")

    # Extract Latitude & Longitude from Location
    latitudes, longitudes = zip(*chunk["location"].map(extract_lat_lon))
    chunk["latitude"] = latitudes
    chunk["longitude"] = longitudes

    # Append cleaned chunk to MySQL
    # We must match the table name from the other scripts, e.g., "musemotion"
    chunk.to_sql("musemotion_data", con=engine, if_exists="append", index=False, method="multi", chunksize=1000)

    cleaned_rows += after
    print(f"Chunk {i+1}: {after}/{before} rows kept and loaded to MySQL")

    if i == 0:
        print("\nFirst few cleaned rows of first chunk:")
        print(chunk.head())

print(f"\n Total rows imported to MySQL: {cleaned_rows}")