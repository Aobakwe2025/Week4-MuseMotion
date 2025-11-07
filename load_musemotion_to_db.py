"""
Load MuseMotion_data.xlsx into an Azure MySQL or Postgres DB table 'musemotion'.
This script:
 - reads EXCEL_PATH (from .env or defaults)
 - normalizes column names to: vin,city,year,make,model,vehicle_type,eligibility_reason,odometer,some_id,geom_wkt,utility
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
EXCEL_PATH = os.getenv("EXCEL_PATH", r"C:\Users\luyan\Documents\Luyanda_Dev\capaciti projects\practiceapp\MuseMotion_data.xlsx")

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

# --- Read Excel
excel_path = Path(EXCEL_PATH)
if not excel_path.exists():
    raise FileNotFoundError(f"Excel file not found: {excel_path}")
df = pd.read_excel(excel_path)
logger.info("Loaded Excel shape: %s", df.shape)
logger.info("Columns read: %s", df.columns.tolist())

# --- Normalize column names (exact target names)
target_cols = ["vin","city","year","make","model","vehicle_type",
               "eligibility_reason","odometer","some_id","geom_wkt","utility"]

# Heuristic: if header row is messy, coerce to strings and map first 11 columns
df.columns = [str(c).strip() for c in df.columns]
if len(df.columns) >= 11:
    df = df.iloc[:, :11]  # keep first 11 columns
    df.columns = target_cols
else:
    # fallback: try to map by substring match
    mapped = []
    for c in df.columns:
        s = c.lower()
        if "vin" in s:
            mapped.append("vin")
        elif "city" in s:
            mapped.append("city")
        elif "year" in s:
            mapped.append("year")
        elif "make" in s:
            mapped.append("make")
        elif "model" in s:
            mapped.append("model")
        elif "type" in s:
            mapped.append("vehicle_type")
        elif "elig" in s or "reason" in s:
            mapped.append("eligibility_reason")
        elif "odometer" in s or "mile" in s:
            mapped.append("odometer")
        elif "id" in s:
            mapped.append("some_id")
        elif "point" in s or "geom" in s:
            mapped.append("geom_wkt")
        elif "util" in s:
            mapped.append("utility")
        else:
            mapped.append(s.replace(" ", "_")[:30])
    df.columns = mapped
    # ensure all target cols exist
    for c in target_cols:
        if c not in df.columns:
            df[c] = None
    df = df[target_cols]

# Convert types
df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")
df["some_id"] = pd.to_numeric(df["some_id"], errors="coerce").astype("Int64")

# Extract lat/long from geom_wkt if present (optional)
def extract_latlon(wkt):
    try:
        if not isinstance(wkt, str): return (None, None)
        m = re.search(r"POINT\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s+([+-]?[0-9]*\.?[0-9]+)\s*\)", wkt)
        if m:
            lon = float(m.group(1)); lat = float(m.group(2))
            return lat, lon
    except Exception:
        pass
    return (None, None)

if "geom_wkt" in df.columns:
    latlon = df["geom_wkt"].apply(lambda w: extract_latlon(w))
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
        INSERT INTO musemotion (vin, city, year, make, model, vehicle_type, eligibility_reason,
                               odometer, some_id, geom_wkt, utility, latitude, longitude)
        SELECT vin, city, year, make, model, vehicle_type, eligibility_reason,
               odometer, some_id, geom_wkt, utility, latitude, longitude
        FROM {temp_table}
        ON DUPLICATE KEY UPDATE
          city = VALUES(city),
          year = VALUES(year),
          make = VALUES(make),
          model = VALUES(model),
          vehicle_type = VALUES(vehicle_type),
          eligibility_reason = VALUES(eligibility_reason),
          odometer = VALUES(odometer),
          some_id = VALUES(some_id),
          geom_wkt = VALUES(geom_wkt),
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
        INSERT INTO musemotion (vin, city, year, make, model, vehicle_type, eligibility_reason,
                               odometer, some_id, geom_wkt, utility, latitude, longitude)
        SELECT vin, city, year, make, model, vehicle_type, eligibility_reason,
               odometer, some_id, geom_wkt, utility, latitude, longitude
        FROM {temp_table}
        ON CONFLICT (vin) DO UPDATE
        SET city = EXCLUDED.city,
            year = EXCLUDED.year,
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            vehicle_type = EXCLUDED.vehicle_type,
            eligibility_reason = EXCLUDED.eligibility_reason,
            odometer = EXCLUDED.odometer,
            some_id = EXCLUDED.some_id,
            geom_wkt = EXCLUDED.geom_wkt,
            utility = EXCLUDED.utility,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
        """
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

logger.info("Upsert finished. Rows processed: %s", len(df))
