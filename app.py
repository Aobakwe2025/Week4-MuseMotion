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
PW = os.getenv("DB_PASS")
HOST = os.getenv("DB_HOST", "localhost")
DB = os.getenv("DB_NAME", "musemotion_db")

# file path --since its my local file path, please put yours
csv_file = Path(r"C:\Users\luyan\Documents\Luyanda_Dev\capaciti projects\Practice App\musemotion_dataset.csv")
chunksize = 2000

# SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{USER}:{PW}@{HOST}/{DB}")

# column names ---
columns_to_keep = [
    "VIN",
    "City",
    "Year",
    "Make",
    "Model",
    "Vehicle Type",
    "Eligibility",
    "Electric Range",
    "Base MSRP",
    "Location",
    "Utility"
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

for i, chunk in enumerate(pd.read_csv(csv_file, usecols=range(len(columns_to_keep)), chunksize=chunksize, header=None)):
    # Assign column names
    chunk.columns = columns_to_keep

    # Clean text fields
    text_cols = ["VIN", "City", "Make", "Model", "Vehicle Type", "Eligibility", "Utility"]
    for c in text_cols:
        chunk[c] = chunk[c].astype(str).str.strip().replace({"nan": pd.NA, "": pd.NA})

    # Drop rows missing VIN or City
    before = len(chunk)
    chunk.dropna(subset=["VIN", "City"], inplace=True)
    after = len(chunk)

    # Convert numeric fields safely
    chunk["Year"] = pd.to_numeric(chunk["Year"], errors="coerce").astype("Int64")
    chunk["Electric Range"] = pd.to_numeric(chunk["Electric Range"], errors="coerce")
    chunk["Base MSRP"] = pd.to_numeric(chunk["Base MSRP"], errors="coerce")

    # Extract Latitude & Longitude from Location
    latitudes, longitudes = zip(*chunk["Location"].map(extract_lat_lon))
    chunk["Latitude"] = latitudes
    chunk["Longitude"] = longitudes

    # Append cleaned chunk to MySQL
    chunk.to_sql("musemotion_data", con=engine, if_exists="append", index=False, method="multi", chunksize=1000)

    cleaned_rows += after
    print(f"Chunk {i+1}: {after}/{before} rows kept and loaded to MySQL")

    if i == 0:
        print("\nFirst few cleaned rows of first chunk:")
        print(chunk.head())

print(f"\n Total rows imported to MySQL: {cleaned_rows}")
