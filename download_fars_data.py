### script gets zip files from NHTSA website, uploading all years of CSV data to our S3 glue-scripts bucket, to be implemented in AWS glue
### 1. Create IAM Role for the Glue job in AWS Console (Roles > Create Role > AWS service > Glue), create Standard AWSGlueServiceRole policy
### 2. Grant s3:PutObject and s3:GetObject permissions for the raw Data bucket (raw-crash-data), name policy (GlueS3WriteAccess)
### 3. Give IAM role a name (GlueFarsDownloadRole)
### 4. Create AWS Glue job: Data Integration and ETL > ETL Jobs > Create Job in scripts tab > Python Shell (FARS_Data_Downloader), select created IAM role above, select uploaded script (download_fars_data.py) > Create Job
### 5. Run Job! Check that the s3 raw-crash-data bucket contains zips from the specified years


import requests
import boto3
from io import BytesIO

# AWS S3 Configuration
S3_BUCKET_NAME = 'raw-crash-data' # 

# Initialize the S3 client
s3 = boto3.client('s3')

# Base URL for the FARS data
BASE_URL = 'https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip'

def download_and_upload_to_s3(year):
    """
    Downloads the FARS data for a given year and uploads it to S3.
    """
    url = BASE_URL.format(year=year)
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Upload the file to S3 directly from the stream
        s3.upload_fileobj(
            Fileobj=BytesIO(response.content),
            Bucket=S3_BUCKET_NAME,
            Key=f'raw/FARS{year}NationalCSV.zip'
        )
        print(f'Successfully uploaded FARS{year}NationalCSV.zip to {S3_BUCKET_NAME}')

    except requests.exceptions.RequestException as e:
        print(f'Error downloading data for {year}: {e}')

if __name__ == '__main__':
    # Loop through the years from 1975 to 2023
    for year in range(1975, 2024):
        download_and_upload_to_s3(year)