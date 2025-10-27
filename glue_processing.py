import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, regexp_extract, lit
import zipfile
from io import BytesIO
import boto3

## --- Configuration ---
S3_BUCKET_NAME = "CrashRiskRadar2025"
RAW_ZIP_PATH = "rawData/NHTSA-zips/" #where the ingested raw zip files live
PROCESSED_PATH = "processedData/" #where we want the processed data to live

# --- These are the only columns we need for the ML model ---
# --- Standardizes the schema across 50 years ---
REQUIRED_COLUMNS = [
    'YEAR', 'MONTH', 'LGT_COND', 'DAY', 'DAY_WEEK', 'HOUR', 'FUNC_SYS', 
    'RD_OWNER', 'RELJCT2', 'WEATHER', 'ROUTE', 'TWAY_ID', 
    'TYP_INT', 'REL_ROAD', 'LATITUDE', 'LONGITUD', 'CITYNAME', 'STATENAME'
]

# --- Initialize Glue/Spark ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=RAW_ZIP_PATH)

# All 50 dataframes
list_of_dfs = []

print(f"Starting processing for files in s3://{S3_BUCKET_NAME}/{RAW_ZIP_PATH}")

for page in pages:
    for obj in page.get('Contents', []):
        s3_key = obj['Key']
        
        if s3_key.endswith('.zip'):
            print(f"Processing {s3_key}...")
            
            try:
                # Get the year from the filename
                year_match = regexp_extract(lit(s3_key), r'FARS(\d{4})NationalCSV\.zip', 1)
                if year_match == "":
                    print(f"Could not extract year from {s3_key}. Skipping.")
                    continue

                # Read the ZIP file from S3 into memory
                zip_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                zip_content = zip_obj['Body'].read()
                
                with zipfile.ZipFile(BytesIO(zip_content), 'r') as z:
                    # Find 'accident.csv' (case-insensitive)
                    accident_file = next((f for f in z.namelist() if f.lower().endswith('accident.csv')), None)
                    
                    if accident_file:
                        with z.open(accident_file) as f:
                            # Read CSV content into a Spark DataFrame
                            df = spark.read \
                                .option("header", "true") \
                                .option("inferSchema", "true") \
                                .csv(spark.sparkContext.parallelize([f.read().decode('utf-8', 'ignore')]))
                            
                            # Add the YEAR column
                            df = df.withColumn("YEAR", year_match.cast("int"))
                            
                            # --- Standardize Schema ---
                            current_cols_lower = {c.lower(): c for c in df.columns}
                            final_cols = []
                            
                            for col in REQUIRED_COLUMNS:
                                if col.lower() in current_cols_lower:
                                    final_cols.append(df[current_cols_lower[col.lower()]].alias(col))
                                else:
                                    # If a required column is missing, add it as null
                                    print(f"Warning: Column '{col}' not found in {s3_key}. Adding as null.")
                                    final_cols.append(lit(None).alias(col))
                            
                            list_of_dfs.append(df.select(final_cols))
                            
                    else:
                        print(f"No 'accident.csv' found in {s3_key}. Skipping.")
                        
            except Exception as e:
                print(f"Error processing {s3_key}: {e}")

print(f"Successfully processed {len(list_of_dfs)} files. Unioning all DataFrames...")

# --- Combine all 50 DataFrames into one ---
if not list_of_dfs:
    raise Exception("No dataframes were processed. Check S3 path and file contents.")

# Reduce for a memory-efficient union of all dataframes
from functools import reduce
from pyspark.sql import DataFrame
final_df = reduce(DataFrame.unionByName, list_of_dfs)

print(f"Final DataFrame has {final_df.count()} rows.")

# --- Write the final, combined data to S3 as Parquet ---
# Partitioning by YEAR
final_df.write \
    .partitionBy("YEAR") \
    .mode("overwrite") \
    .parquet(f"s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")

print(f"Successfully wrote combined Parquet data to s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")

job.commit()