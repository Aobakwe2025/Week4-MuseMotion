from dotenv import load_dotenv
import os
import boto3
from sqlalchemy import create_engine
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# --- RDS ---
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_HOST = os.getenv('RDS_HOST')
RDS_DB = os.getenv('RDS_DB')
RDS_PORT = int(os.getenv('RDS_PORT', 3306))

# --- AWS S3 ---
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

print("S3 connection successful!")