#Python Shell Script - run once to download all the zips


import requests
import boto3
from io import BytesIO
import logging

# --- CONFIGURATION ---
S3_BUCKET_NAME = "CrashRiskRadar2025"  # Bucket Location
S3_PREFIX = "rawData/NHTSA-zips/"      # Raw Zip Files (not yet unzipping!)
START_YEAR = 1975
END_YEAR = 2023
BASE_URL = 'https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip'

# --- SETUP ---
# Logging
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
