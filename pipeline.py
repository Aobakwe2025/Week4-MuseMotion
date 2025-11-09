import os
import re
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
import logging
import urllib

# --- Setup Logging ---
# Sets up basic logging to show INFO level messages in the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load environment variables ---
load_dotenv()

# --- Configuration (Reads from .env file) ---
# Azure Blob Storage (Source)
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

# Azure SQL Server (Destination)
DB_BACKEND = os.getenv("DB_BACKEND")
DB_HOST = os.getenv("DB_HOST")
DB_NAME_RAW = os.getenv("DB_NAME") # Raw value from .env, e.g., "server/database"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Correct column names for the CSV data (11 columns)
COLUMN_NAMES = [
    "VIN", "City", "Year", "Make", "Model", "Vehicle_Type",
    "Eligibility", "Electric_Range", "Vehicle_ID", "Location", "Utility"
]

# Regex for extracting latitude and longitude from the 'Location' point string
POINT_RE = re.compile(r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)")

# --- Database Engine Setup ---
def get_db_engine():
    """Configures and returns the SQLAlchemy engine for Azure SQL Server."""
    
    # We strip the database name in case it includes the server prefix from the .env
    DB_NAME = DB_NAME_RAW.split('/')[-1]
    
    if DB_BACKEND and DB_BACKEND.lower() == "mssql":
        # Ensure the password is URL-encoded for the connection string
        quoted_pw = urllib.parse.quote_plus(DB_PASSWORD)
        
        # Use ODBC Driver 17 or 18. User must ensure this driver is installed.
        DRIVER = 'ODBC Driver 17 for SQL Server' 
        
        # Build the engine URL using mssql+pyodbc dialect
        engine_url = (
            f"mssql+pyodbc://{DB_USER}:{quoted_pw}@{DB_HOST}/"
            f"{DB_NAME}?driver={DRIVER}"
        )
        
        # 'fast_executemany=True' helps pandas bulk-insert data to MS SQL Server
        return create_engine(engine_url, fast_executemany=True)
    
    else:
        raise ValueError(f"Unsupported DB_BACKEND: {DB_BACKEND}. Must be 'mssql'.")

# --- Transformation Function (T) ---
def extract_lat_lon(point_str):
    """Extracts latitude and longitude from the POINT (lon lat) string."""
    try:
        if pd.isna(point_str) or not isinstance(point_str, str):
            return pd.NA, pd.NA
        match = POINT_RE.search(point_str)
        if match:
            # NOTE: POINT (lon lat) -> we return (lat, lon)
            lon, lat = match.groups()
            return float(lat), float(lon)
    except Exception as e:
        logger.warning(f"Error parsing location string '{point_str}': {e}")
    return pd.NA, pd.NA

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Applies all cleaning and transformation rules to the DataFrame."""
    logger.info(f"Initial dataframe shape: {df.shape}")
    
    # 1. Assign/Correct Column Names
    # Assuming the merged CSVs have no header row (header=None in pd.read_csv)
    if len(df.columns) == len(COLUMN_NAMES):
        df.columns = COLUMN_NAMES
    else:
        # Fallback for unexpected column count
        logger.error(f"Merged dataframe has {len(df.columns)} columns, expected {len(COLUMN_NAMES)}. Dropping extras.")
        if len(df.columns) >= len(COLUMN_NAMES):
             df = df.iloc[:, :len(COLUMN_NAMES)]
             df.columns = COLUMN_NAMES
        else:
             raise ValueError("Column count mismatch in merged data. Cannot proceed with cleaning.")

    # 2. Clean Text Fields (Strip whitespace, convert 'nan' strings to NA)
    text_cols = ["VIN", "City", "Make", "Model", "Vehicle_Type", "Eligibility", "Utility"]
    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})

    # 3. Drop rows missing critical data (VIN and City)
    df.dropna(subset=["VIN", "City"], inplace=True)
    
    # 4. Convert Numeric Fields Safely (Int64 preserves NA)
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Electric_Range"] = pd.to_numeric(df["Electric_Range"], errors="coerce").astype("Int64")
    df["Vehicle_ID"] = pd.to_numeric(df["Vehicle_ID"], errors="coerce").astype("Int64")

    # 5. Extract Latitude & Longitude from Location
    # 'zip(*...)' unpacks the tuples returned by the mapping
    latitudes, longitudes = zip(*df["Location"].apply(extract_lat_lon))
    df["Latitude"] = latitudes
    df["Longitude"] = longitudes
    
    # 6. Final Column Selection
    final_cols = COLUMN_NAMES + ["Latitude", "Longitude"]
    df = df[final_cols]

    logger.info(f"Cleaned dataframe shape: {df.shape}")
    return df

# --- Extraction Function (E) ---
def download_and_merge_csvs(container_client):
    """Downloads all CSV blobs from Azure and merges them."""
    all_dfs = []
    logger.info(f"Starting download from Azure Blob Container: {AZURE_CONTAINER_NAME}")
    
    for blob in container_client.list_blobs():
        if blob.name.endswith(".csv"):
            logger.info(f"Downloading {blob.name}...")
            blob_client = container_client.get_blob_client(blob.name)
            stream = blob_client.download_blob()
            
            # Use 'header=None' as the CSV data often lacks headers
            try:
                df = pd.read_csv(stream, header=None)
                all_dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to read CSV {blob.name}: {e}")
                
    if all_dfs:
        merged_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Merged {len(all_dfs)} CSV files. Total rows: {len(merged_df)}")
        return merged_df
    else:
        logger.warning("No CSV files found or merged from the container.")
        return pd.DataFrame()

# --- Load Function (L) ---
def upload_df_to_db(df, engine, table_name="musemotion_data"):
    """Uploads the DataFrame to the Azure SQL Server database using 'replace'."""
    if df.empty:
        logger.warning("No data to upload. Skipping database load.")
        return
        
    logger.info(f"Uploading {len(df)} rows to Azure SQL Server table '{table_name}'.")
    
    # Use 'replace' to drop and recreate the table, ensuring a clean and consistent schema.
    try:
        df.to_sql(
            table_name, 
            con=engine, 
            if_exists='replace', # Crucial: drops the old table and creates a new one
            index=False, 
            method='multi',
            chunksize=500
        )
        logger.info(f"Successfully uploaded {len(df)} rows to '{table_name}'.")
    except Exception as e:
        logger.error(f"FATAL: Failed to upload data to DB. Ensure the ODBC driver is installed and the firewall is open to your IP. Error: {e}")
        # The Streamlit app (`app.py`) also uses this table name, so success here is critical.

# --- Main Pipeline Execution ---
if __name__ == "__main__":
    
    # 1. Setup Connections
    try:
        engine = get_db_engine()
        
        connection_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={AZURE_ACCOUNT_NAME};"
            f"AccountKey={AZURE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        logger.info("Database and Azure Blob connections established.")
    except Exception as e:
        logger.critical(f"FATAL: Failed to establish required connections. Check your .env credentials, DB_BACKEND setting, or ODBC drivers. Error: {e}")
        exit(1)

    # 2. Extraction
    raw_df = download_and_merge_csvs(container_client)
    
    if raw_df.empty:
        logger.warning("Pipeline finished: No data extracted.")
        exit(0)
        
    # 3. Transformation
    cleaned_df = clean_dataframe(raw_df)
    
    # 4. Loading
    upload_df_to_db(cleaned_df, engine, table_name="musemotion_data")
    
    logger.info("Pipeline completed successfully!")