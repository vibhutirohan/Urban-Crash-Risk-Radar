import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, regexp_extract, lit, col
import zipfile
from io import BytesIO
import boto3

## --- CONFIGURATION ---
S3_BUCKET_NAME = "crash-risk-radar-2025"
RAW_ZIP_PATH = "rawData/NHTSA-zips/"
PROCESSED_PATH = "processedData/"
MIN_YEAR_TO_PROCESS = 2016  # <-- Year filter since schemas are not consistent before that point. 

# --- SCHEMA MAP ---
#  "Standard Name" to a list of names found in the CSVs.
# Since we're processing 2016+, these names should be very consistent.
SCHEMA_MAP = {
    # Standard Name: [List of possible names, lowercase]
    'YEAR': ['year'],
    'MONTH': ['month'],
    'LGT_COND': ['lgt_cond'],
    'DAY': ['day'],
    'DAY_WEEK': ['day_week'],
    'HOUR': ['hour'],
    'FUNC_SYS': ['func_sys'],
    'RD_OWNER': ['rd_owner'],
    'RELJCT2': ['reljct2'],
    'WEATHER': ['weather'],
    'ROUTE': ['route'],
    'TWAY_ID': ['tway_id'],
    'TYP_INT': ['typ_int'],
    'REL_ROAD': ['rel_road'],
    'LATITUDE': ['latitude', 'lat'], 
    'LONGITUD': ['longitud', 'long', 'lon'], 
    'CITYNAME': ['cityname', 'city'], 
    'STATENAME': ['statename', 'state']
}

# The final list of columns we require, based on the map keys
REQUIRED_COLUMNS = list(SCHEMA_MAP.keys())

# --- Initialize Glue/Spark Context ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Boto3 S3 client to list files ---
s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=RAW_ZIP_PATH)

list_of_dfs = []

print(f"Starting processing for files in s3://{S3_BUCKET_NAME}/{RAW_ZIP_PATH}")
print(f"Filtering for years >= {MIN_YEAR_TO_PROCESS}")

for page in pages:
    for obj in page.get('Contents', []):
        s3_key = obj['Key']
        
        if s3_key.endswith('.zip'):
            
            try:
                # Extract year from filename
                year_match_df = spark.createDataFrame([("",)]).select(
                    regexp_extract(lit(s3_key), r'FARS(\d{4})NationalCSV\.zip', 1).alias("YEAR")
                )
                year_str = year_match_df.first().YEAR
                if not year_str:
                    print(f"Could not extract year from {s3_key}. Skipping.")
                    continue
                
                year = int(year_str)
                
                # --- NEW: Year Filter ---
                if year < MIN_YEAR_TO_PROCESS:
                    print(f"Skipping {s3_key} (Year {year} < {MIN_YEAR_TO_PROCESS})")
                    continue
                # --- END: Year Filter ---

                print(f"Processing {s3_key} (Year {year})...")
                zip_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                zip_content = zip_obj['Body'].read()
                
                with zipfile.ZipFile(BytesIO(zip_content), 'r') as z:
                    accident_file = next((f for f in z.namelist() if f.lower().endswith('accident.csv')), None)
                    
                    if accident_file:
                        with z.open(accident_file) as f:
                            # Read CSV content into a list, decode with ignore for errors
                            csv_content_list = [f.read().decode('utf-8', 'ignore')]
                            rdd = sc.parallelize(csv_content_list)
                            df = spark.read \
                                .option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(rdd)
                            
                            # Add the YEAR column
                            df = df.withColumn("YEAR", lit(int(year)))
                            
                            # --- SCHEMA NORMALIZATION LOGIC ---
                            current_cols_lower = {c.lower(): c for c in df.columns}
                            final_cols = []
                            
                            for standard_name in REQUIRED_COLUMNS:
                                found = False
                                possible_names = SCHEMA_MAP.get(standard_name, [])
                                
                                for alias in possible_names:
                                    if alias in current_cols_lower:
                                        original_col_name = current_cols_lower[alias]
                                        final_cols.append(col(original_col_name).alias(standard_name))
                                        found = True
                                        break 
                                
                                if not found:
                                    # Since we're using 2016+, these warnings should be minimal or gone
                                    print(f"Warning: Column '{standard_name}' (or aliases) not found in {s3_key}. Adding as null.")
                                    final_cols.append(lit(None).alias(standard_name))
                            
                            list_of_dfs.append(df.select(final_cols))
                            
                    else:
                        print(f"No 'accident.csv' found in {s3_key}. Skipping.")
                        
            except Exception as e:
                print(f"Error processing {s3_key}: {e}")

print(f"Successfully processed {len(list_of_dfs)} files. Unioning all DataFrames...")

if not list_of_dfs:
    raise Exception("No dataframes were processed. Check S3 path and file contents.")

from functools import reduce
from pyspark.sql import DataFrame
# Use unionByName to safely combine DataFrames even if column order is different
final_df = reduce(DataFrame.unionByName, list_of_dfs)

print(f"Final DataFrame has {final_df.count()} rows.")

final_df.write \
    .partitionBy("YEAR") \
    .mode("overwrite") \
    .parquet(f"s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")

print(f"Successfully wrote combined Parquet data (2016-2023) to s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")

job.commit()