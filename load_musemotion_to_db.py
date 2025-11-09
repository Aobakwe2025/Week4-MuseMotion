"""
Load musemotion_databse.csv into an Azure MySQL or Postgres DB table 'musemotion'.
This script:
 - reads DATA_PATH (from .env or defaults to musemotion_databse.csv)
 - normalizes column names to: vin,city,year,make,model,vehicle_type,eligibility,electric_range,vehicle_id,location,utility
 - writes to a temporary table then upserts into musemotion (if vin unique)
"""

import os, re
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()  # loads .env if present

DB_BACKEND = os.getenv("DB_BACKEND", "mysql").lower()   # 'mysql' or 'postgres'
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "musemotion_db")
# --- Use the CSV file path ---
DATA_PATH = os.getenv("DATA_PATH", "musemotion_databse.csv")

if DB_BACKEND == "mysql":
    DB_PORT = int(DB_PORT)
    engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
elif DB_BACKEND in ("postgres", "postgresql"):
    DB_PORT = int(DB_PORT) if DB_PORT else 5432
    engine_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    raise ValueError("Unsupported DB_BACKEND. Use 'mysql' or 'postgres'.")

logger.info("Using engine: %s", engine_url)
engine = create_engine(engine_url, echo=False)

# --- Read CSV ---
data_path = Path(DATA_PATH)
if not data_path.exists():
    raise FileNotFoundError(f"Data file not found: {data_path}")

# --- Define your 11 columns in order (lowercase) ---
target_cols = [
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
df = pd.read_csv(data_path, header=None, names=target_cols)
logger.info("Loaded CSV shape: %s", df.shape)

# --- Normalize column names (already done by pd.read_csv) ---

# --- Convert types (using the correct column names) ---
df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
df["electric_range"] = pd.to_numeric(df["electric_range"], errors="coerce").astype("Int64")
df["vehicle_id"] = pd.to_numeric(df["vehicle_id"], errors="coerce").astype("Int64")

# --- Extract lat/long from 'location' column ---
point_re = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

def extract_latlon(location_str):
    try:
        if not isinstance(location_str, str):
            return (None, None)
        match = point_re.search(str(location_str))
        if match:
            lon = float(match.group(1)) # Longitude is first
            lat = float(match.group(2)) # Latitude is second
            return (lat, lon)
    except Exception:
        pass
    return (None, None)

if "location" in df.columns:
    latlon = df["location"].apply(lambda w: extract_latlon(w))
    df["latitude"] = latlon.apply(lambda t: t[0])
    df["longitude"] = latlon.apply(lambda t: t[1])
else:
    df["latitude"] = None; df["longitude"] = None

# Reorder to include latitude/longitude at end
cols_final = target_cols + ["latitude","longitude"]
df = df[cols_final]

# --- Write to temporary table then upsert into musemotion
temp_table = "musemotion_tmp"

logger.info("Writing to temporary table %s (rows=%s)...", temp_table, len(df))
df.to_sql(temp_table, con=engine, if_exists="replace", index=False, method="multi", chunksize=1000)

with engine.begin() as conn:
    if DB_BACKEND == "mysql":
        # MySQL upsert using ON DUPLICATE KEY UPDATE (assumes UNIQUE on vin)
        upsert_sql = f"""
        INSERT INTO musemotion (vin, city, year, make, model, vehicle_type, eligibility,
                               electric_range, vehicle_id, location, utility, latitude, longitude)
        SELECT vin, city, year, make, model, vehicle_type, eligibility,
               electric_range, vehicle_id, location, utility, latitude, longitude
        FROM {temp_table}
        ON DUPLICATE KEY UPDATE
          city = VALUES(city),
          year = VALUES(year),
          make = VALUES(make),
          model = VALUES(model),
          vehicle_type = VALUES(vehicle_type),
          eligibility = VALUES(eligibility),
          electric_range = VALUES(electric_range),
          vehicle_id = VALUES(vehicle_id),
          location = VALUES(location),
          utility = VALUES(utility),
          latitude = VALUES(latitude),
          longitude = VALUES(longitude);
        """
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
    else:
        # Postgres upsert using ON CONFLICT (vin) DO UPDATE
        # Ensure unique index on vin exists.
        upsert_sql = f"""
        INSERT INTO musemotion (vin, city, year, make, model, vehicle_type, eligibility,
                               electric_range, vehicle_id, location, utility, latitude, longitude)
        SELECT vin, city, year, make, model, vehicle_type, eligibility,
               electric_range, vehicle_id, location, utility, latitude, longitude
        FROM {temp_table}
        ON CONFLICT (vin) DO UPDATE
        SET city = EXCLUDED.city,
            year = EXCLUDED.year,
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            vehicle_type = EXCLUDED.vehicle_type,
            eligibility = EXCLUDED.eligibility,
            electric_range = EXCLUDED.electric_range,
            vehicle_id = EXCLUDED.vehicle_id,
            location = EXCLUDED.location,
            utility = EXCLUDED.utility,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
        """
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

logger.info("Upsert finished. Rows processed: %s", len(df))