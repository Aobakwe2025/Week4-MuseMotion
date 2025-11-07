import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import re

load_dotenv()  # loads .env if present

DB_BACKEND = os.getenv("DB_BACKEND", "mysql").lower()  # 'mysql' or 'postgres'
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "LuyandaZuma007")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")   # default mysql port
DB_NAME = os.getenv("DB_NAME", "musemotion_db")
EXCEL_PATH = os.getenv("EXCEL_PATH", r"C:\Users\luyan\Documents\Luyanda_Dev\capaciti projects\practiceapp\MuseMotion_data.xlsx")

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

# --- Read Excel
excel_path = Path(EXCEL_PATH)
if not excel_path.exists():
    raise FileNotFoundError(f"Excel file not found: {excel_path}")

df = pd.read_excel(excel_path)
print("Loaded Excel with shape:", df.shape)

# --- Clean columns: make strings, replace spaces and weird chars
df.columns = [str(c).strip() for c in df.columns]

# If your file has ambiguous headers (like VIN values as header), you may want to manually set column names.
# Example mapping — adjust if the first 15 columns are known:
if len(df.columns) == 11:
    df.columns = [
        "VIN",
        "City",
        "Year",
        "Make",
        "Model",
        "Vehicle_Type",
        "Eligibility",
        "Electric_Range",
        "Latitude",
        "Longitude",
        "Utility"
    ]
else:
    # Generic normalized names
    df.columns = [re.sub(r"\\s+", "_", c).replace("-", "_") for c in df.columns]

# Convert datatypes
if "year" in df.columns:
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
if "odometer" in df.columns:
    df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")
if "some_id" in df.columns:
    df["some_id"] = pd.to_numeric(df["some_id"], errors="coerce").astype("Int64")

# Extract lat/long from geom_wkt if present
def extract_latlon(wkt):
    try:
        if not isinstance(wkt, str):
            return (None, None)
        # Expect: POINT (_lon_ _lat_) or POINT (_lon_ _lat)
        m = re.search(r"POINT\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s+([+-]?[0-9]*\.?[0-9]+)\s*\)", wkt)
        if m:
            lon = float(m.group(1))
            lat = float(m.group(2))
            return (lat, lon)
    except Exception:
        pass
    return (None, None)

if "geom_wkt" in df.columns:
    lat_lon = df["geom_wkt"].apply(lambda w: extract_latlon(w))
    df["latitude"] = lat_lon.apply(lambda t: t[0])
    df["longitude"] = lat_lon.apply(lambda t: t[1])
else:
    df["latitude"] = None
    df["longitude"] = None

# Reorder columns so latitude/longitude at end (optional)
cols = [c for c in df.columns if c not in ("latitude", "longitude")] + ["latitude", "longitude"]
df = df[cols]

# --- Create table if not exists (DML differs slightly between MySQL and Postgres)
if DB_BACKEND == "mysql":
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS musemotion (
      id INT AUTO_INCREMENT PRIMARY KEY,
      vin VARCHAR(50),
      city VARCHAR(100),
      year INT,
      make VARCHAR(50),
      model VARCHAR(100),
      vehicle_type VARCHAR(255),
      eligibility_reason VARCHAR(255),
      odometer INT,
      some_id BIGINT,
      geom_wkt VARCHAR(255),
      utility VARCHAR(255),
      latitude DOUBLE,
      longitude DOUBLE
    );
    """
else:
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS musemotion (
      id SERIAL PRIMARY KEY,
      vin VARCHAR(50),
      city VARCHAR(100),
      year INT,
      make VARCHAR(50),
      model VARCHAR(100),
      vehicle_type VARCHAR(255),
      eligibility_reason VARCHAR(255),
      odometer INT,
      some_id BIGINT,
      geom_wkt VARCHAR(255),
      utility VARCHAR(255),
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION
    );
    """

with engine.connect() as conn:
    conn.execute(text(create_table_stmt))
    conn.commit()
print("Ensured table exists.")

# --- Load data
# For idempotence you might want upsert logic. For simplicity we append.
# Use to_sql for small/medium tables. For large loads use COPY (Postgres) or LOAD DATA (MySQL).
df.to_sql("musemotion", con=engine, if_exists="append", index=False, method="multi", chunksize=500)
print("Loaded rows:", len(df))