from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import pandas as pd

# --- Load environment variables ---
load_dotenv()

# --- Verify required environment variables ---
required_env_vars = [
    'RDS_USER', 'RDS_PASSWORD', 'RDS_HOST', 'RDS_DB', 'RDS_PORT',
    'AZURE_STORAGE_ACCOUNT_NAME', 'AZURE_STORAGE_ACCOUNT_KEY', 'AZURE_CONTAINER_NAME'
]

for var in required_env_vars:
    if os.getenv(var) is None:
        raise ValueError(f"Environment variable {var} is missing!")

# --- MySQL / Azure RDS Configuration ---
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_HOST = os.getenv('RDS_HOST')
RDS_DB = os.getenv('RDS_DB')
RDS_PORT = int(os.getenv('RDS_PORT', 3306))

# Optional SSL configuration (remove if not needed)
ssl_ca_path = os.getenv('RDS_SSL_CA', None)
ssl_arg = f"?ssl_ca={ssl_ca_path}" if ssl_ca_path else ""

engine = create_engine(
    f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DB}{ssl_arg}"
)
print(" MySQL connection ready!")

# --- Azure Blob Storage Configuration ---
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

connection_str = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={AZURE_ACCOUNT_NAME};"
    f"AccountKey={AZURE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_str)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
print(" Azure Blob Storage connection successful!")

# --- Function: Download all CSV blobs and merge into a DataFrame ---
def download_and_merge_csvs():
    all_dfs = []
    for blob in container_client.list_blobs():
        if blob.name.endswith(".csv"):
            print(f"📥 Downloading {blob.name}...")
            blob_client = container_client.get_blob_client(blob.name)
            stream = blob_client.download_blob()
            df = pd.read_csv(stream)
            all_dfs.append(df)
    if all_dfs:
        merged_df = pd.concat(all_dfs, ignore_index=True)
        print(f" Merged {len(all_dfs)} CSV files.")
        return merged_df
    else:
        print(" No CSV files found in the container.")
        return pd.DataFrame()

# --- Function: Upload DataFrame to MySQL ---
def upload_df_to_mysql(df, table_name):
    if df.empty:
        print(" No data to upload.")
        return
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f" Uploaded DataFrame to MySQL table '{table_name}'.")

# --- Function: Upload a local file back to Azure Blob ---
def upload_file_to_blob(local_file_path, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f" Uploaded {local_file_path} as {blob_name} to Azure container.")

# --- Main Pipeline ---
if __name__ == "__main__":
    # Step 1: Download & merge CSVs
    df = download_and_merge_csvs()
    
    # Step 2: Optional preprocessing
    # Example: df.dropna(inplace=True)
    
    # Step 3: Upload merged DataFrame to MySQL
    upload_df_to_mysql(df, table_name="musemotion_data")
    
    # Step 4: Save merged CSV locally & upload back to Azure
    merged_csv_path = "merged_musemotion.csv"
    df.to_csv(merged_csv_path, index=False)
    upload_file_to_blob(merged_csv_path, "merged_musemotion.csv")
    
    print(" Pipeline completed successfully!")