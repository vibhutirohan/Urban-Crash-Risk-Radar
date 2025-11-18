#Python Shell Script - run once to download all the zips


import requests
import boto3
from io import BytesIO
import logging
<<<<<<< HEAD

# --- CONFIGURATION ---
S3_BUCKET_NAME = "CrashRiskRadar2025"  # Bucket Location
=======
import time
from concurrent.futures import ThreadPoolExecutor, as_completed #to download multiple ZIP files at the same time


# --- CONFIGURATION ---
S3_BUCKET_NAME = "crash-risk-radar2025"  # Bucket Location
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
S3_PREFIX = "rawData/NHTSA-zips/"      # Raw Zip Files (not yet unzipping!)
START_YEAR = 1975
END_YEAR = 2023
BASE_URL = 'https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip'

# --- SETUP ---
# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

<<<<<<< HEAD
=======
# Setting up max workers (parrallel downloads)
MAX_WORKERS = 10

>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
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
<<<<<<< HEAD
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
=======
        if e.response.status_code == 404:
            return f"⚠️ No data found for year {year} (404 Error). Skipping."
        else:
            return f"❌ HTTP Error for {year}: {e}"
    except Exception as e:
        return f"❌ Failed to process {year}: {e}"

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    start_time = time.time()
    print(f"Starting parallel download of NHTSA data from {START_YEAR} to {END_YEAR}...")
    print(f"Using up to {MAX_WORKERS} parallel workers.")

    years_to_process = list(range(START_YEAR, END_YEAR + 1))
    
    # ThreadPoolExecutor to manage parallel downloads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_and_upload_to_s3, year): year for year in years_to_process}
        
        for future in as_completed(futures):
            result = future.result()
            print(result)

    end_time = time.time()
    print("-" * 50)
    print(f"All downloads complete in {end_time - start_time:.2f} seconds.")
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
