### script gets zip files from NHTSA website, uploading all years of CSV data to our S3 glue-scripts bucket, to be implemented in AWS glue
### 1. Create IAM Role for the Glue job in AWS Console (Roles > Create Role > AWS service > Glue), create Standard AWSGlueServiceRole policy
### 2. Grant s3:PutObject and s3:GetObject permissions for the raw Data bucket (rawData), name policy (GlueS3WriteAccess)
### 3. Give IAM role a name (CrashRiskRole)
### 4. Create AWS Glue job: Data Integration and ETL > ETL Jobs > Create Job in scripts tab > Python Shell (FARS_Data_Downloader), select created IAM role above, select uploaded script (download_fars_data.py) > Create Job
### 5. Run Job! Check that the s3 rawData bucket contains zips from the specified years


import requests
import boto3
from io import BytesIO
import logging

# --- CONFIGURATION ---
S3_BUCKET_NAME = "CrashRiskRadar2025"  # Replace with your actual S3 bucket name
S3_PREFIX = "rawData/NHTSA-zips/"      # The folder within your bucket to store the raw ZIP files
START_YEAR = 1975
END_YEAR = 2023
BASE_URL = 'https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip'

# --- SETUP ---
# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the S3 client from boto3
s3_client = boto3.client('s3')

def download_and_upload_to_s3(year):
    """
    Downloads a FARS data ZIP file for a given year in memory
    and uploads it directly to an S3 bucket.
    """
    # 1. URL Formatting for a given year
    url = BASE_URL.format(year=year)
    s3_key = f"{S3_PREFIX}FARS{year}NationalCSV.zip"

    try:
        logging.info(f"Downloading data for year {year} from {url}...")

        # 2. Download the file content in memory
        # The 'stream=True' good practice for large files
        response = requests.get(url, stream=True)
        response.raise_for_status()  # This will raise an exception for bad responses (4xx or 5xx)

        # 3. BytesIO to treat the binary content of the response as an in-memory file
        in_memory_zip = BytesIO(response.content)

        logging.info(f"Uploading {s3_key} to bucket {S3_BUCKET_NAME}...") # update log

        # 4. Upload the in-memory file to S3
        s3_client.upload_fileobj(in_memory_zip, S3_BUCKET_NAME, s3_key)

        logging.info(f"Successfully uploaded data for year {year}.")
        return True

    except requests.exceptions.HTTPError as e:
        # Catching errors for data scraping. 
        logging.warning(f"Could not download data for year {year}. URL may be invalid. Error: {e}")
        return False
    except Exception as e:
        logging.error(f"An error occurred for year {year}: {e}")
        return False

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    logging.info("Starting FARS data ingestion process...")
    successful_uploads = 0
    for year in range(START_YEAR, END_YEAR + 1): # loop through year range
        if download_and_upload_to_s3(year):
            successful_uploads += 1 # increase count of successful uploads

    logging.info(f"--- Ingestion Complete ---")
    logging.info(f"Successfully uploaded {successful_uploads} ZIP files to s3://{S3_BUCKET_NAME}/{S3_PREFIX}")
