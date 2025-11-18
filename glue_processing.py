import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
<<<<<<< HEAD
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
=======
from pyspark.sql.functions import input_file_name, regexp_extract, lit, col
import zipfile
from io import BytesIO
import boto3
import pandas as pd  

## --- CONFIGURATION ---
S3_BUCKET_NAME = "crash-risk-radar2025"
RAW_ZIP_PATH = "rawData/NHTSA-zips/"
PROCESSED_PATH = "processedData/"
MIN_YEAR_TO_PROCESS = 2016

# --- SCHEMA MAP, accounts for alternate specifications ---
SCHEMA_MAP = {
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
    'LONGITUD': ['longitud', 'longitude', 'long', 'lon'], 
    'CITYNAME': ['cityname', 'city'], 
    'STATENAME': ['statename', 'state']
}
REQUIRED_COLUMNS = list(SCHEMA_MAP.keys())

# --- Initialize Glue/Spark Context ---
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

<<<<<<< HEAD
s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=RAW_ZIP_PATH)

# All 50 dataframes
list_of_dfs = []

print(f"Starting processing for files in s3://{S3_BUCKET_NAME}/{RAW_ZIP_PATH}")
=======
# --- Boto3 S3 client to list files ---
s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=RAW_ZIP_PATH)
list_of_dfs = []

print(f"Starting processing for files in s3://{S3_BUCKET_NAME}/{RAW_ZIP_PATH}")
print(f"Filtering for years >= {MIN_YEAR_TO_PROCESS}")
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904

for page in pages:
    for obj in page.get('Contents', []):
        s3_key = obj['Key']
        
        if s3_key.endswith('.zip'):
<<<<<<< HEAD
            print(f"Processing {s3_key}...")
            
            try:
                # Get the year from the filename
                year_match = regexp_extract(lit(s3_key), r'FARS(\d{4})NationalCSV\.zip', 1)
                if year_match == "":
                    print(f"Could not extract year from {s3_key}. Skipping.")
                    continue

                # Read the ZIP file from S3 into memory
=======
            
            try:
                year_match_df = spark.createDataFrame([("",)]).select(
                    regexp_extract(lit(s3_key), r'FARS(\d{4})NationalCSV\.zip', 1).alias("YEAR")
                )
                year_str = year_match_df.first().YEAR
                if not year_str:
                    print(f"Could not extract year from {s3_key}. Skipping.")
                    continue
                
                year = int(year_str)
                
                if year < MIN_YEAR_TO_PROCESS: # we will only process 2016 onwards due to huge differences in schema
                    print(f"Skipping {s3_key} (Year {year} < {MIN_YEAR_TO_PROCESS})")
                    continue

                print(f"Processing {s3_key} (Year {year})...")
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
                zip_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                zip_content = zip_obj['Body'].read()
                
                with zipfile.ZipFile(BytesIO(zip_content), 'r') as z:
<<<<<<< HEAD
                    # Find 'accident.csv' (case-insensitive)
=======
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
                    accident_file = next((f for f in z.namelist() if f.lower().endswith('accident.csv')), None)
                    
                    if accident_file:
                        with z.open(accident_file) as f:
<<<<<<< HEAD
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
=======
                            
                            # CSV READING WITH DEBUGGING
                            try:
                                # Try multiple encodings if needed
                                df_pandas = pd.read_csv(f, encoding='latin1', low_memory=False)
                                print(f"  Pandas DataFrame shape: {df_pandas.shape}")
                                print(f"  Pandas columns: {list(df_pandas.columns)[:10]}...")  # Show first 10 columns
                                
                            except Exception as read_err:
                                print(f"Pandas read_csv error on {s3_key}: {read_err}. Trying UTF-8...")
                                # Reset file pointer and try different encoding
                                f.seek(0)
                                try:
                                    df_pandas = pd.read_csv(f, encoding='utf-8', low_memory=False, on_bad_lines='skip')
                                    print(f"  Successfully read with UTF-8. Shape: {df_pandas.shape}")
                                except Exception as utf8_err:
                                    print(f"UTF-8 also failed: {utf8_err}. Skipping file.")
                                    continue
                            
                            # Check if pandas actually read any rows
                            if df_pandas.empty:
                                print(f"Warning: 'accident.csv' in {s3_key} was read as empty by Pandas. Skipping.")
                                continue
                                
                            # --- IMPROVED PANDAS TO SPARK CONVERSION ---
                            # Handle potential data type issues
                            try:
                                # Convert problematic columns to string to avoid type conflicts
                                for col_name in df_pandas.columns:
                                    if df_pandas[col_name].dtype == 'object':
                                        df_pandas[col_name] = df_pandas[col_name].astype(str)
                                
                                df = spark.createDataFrame(df_pandas)
                                print(f"  Spark DataFrame created. Row count: {df.count()}")
                                
                            except Exception as spark_err:
                                print(f"Spark DataFrame creation error: {spark_err}")
                                print("  Trying with sample of data...")
                                # Try with smaller sample if full conversion fails
                                sample_df = df_pandas.head(1000)
                                df = spark.createDataFrame(sample_df)
                                print(f"  Sample Spark DataFrame created. Row count: {df.count()}")
                            
                            # Add the YEAR column
                            df = df.withColumn("YEAR", lit(int(year)))
                            
                            # SCHEMA NORMALIZATION
                            current_cols_lower = {c.lower(): c for c in df.columns}
                            print(f"  Available columns (lowercase): {list(current_cols_lower.keys())[:10]}...")
                            
                            final_cols = []
                            
                            for standard_name in REQUIRED_COLUMNS:
                                found = False
                                possible_names = SCHEMA_MAP.get(standard_name, [])
                                
                                # Check if standard name exists
                                if standard_name.lower() in current_cols_lower:
                                    original_col_name = current_cols_lower[standard_name.lower()]
                                    final_cols.append(col(original_col_name).alias(standard_name))
                                    found = True
                                    print(f"    Found {standard_name} as {original_col_name}")
                                else:
                                    # Check aliases
                                    for alias in possible_names:
                                        if alias.lower() in current_cols_lower:
                                            original_col_name = current_cols_lower[alias.lower()]
                                            final_cols.append(col(original_col_name).alias(standard_name))
                                            found = True
                                            print(f"    Found {standard_name} as {original_col_name} (alias: {alias})")
                                            break 
                                
                                if not found:
                                    print(f"    Warning: Column '{standard_name}' not found. Adding as null.")
                                    final_cols.append(lit(None).alias(standard_name))
                            
                            # Apply column selection and check result
                            normalized_df = df.select(final_cols)
                            row_count_after_select = normalized_df.count()
                            print(f"  After column selection: {row_count_after_select} rows")
                            
                            if row_count_after_select > 0:
                                list_of_dfs.append(normalized_df)
                                print(f"  Successfully added DataFrame with {row_count_after_select} rows")
                            else:
                                print(f"  ERROR: DataFrame became empty after column selection!")
                                # Debug: show what the selection would look like
                                print("  Debugging - showing sample of selected columns:")
                                try:
                                    normalized_df.show(5, truncate=False)
                                except:
                                    print("  Could not show sample - DataFrame is empty")
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
                            
                    else:
                        print(f"No 'accident.csv' found in {s3_key}. Skipping.")
                        
            except Exception as e:
                print(f"Error processing {s3_key}: {e}")
<<<<<<< HEAD

print(f"Successfully processed {len(list_of_dfs)} files. Unioning all DataFrames...")

# --- Combine all 50 DataFrames into one ---
if not list_of_dfs:
    raise Exception("No dataframes were processed. Check S3 path and file contents.")

# Reduce for a memory-efficient union of all dataframes
=======
                import traceback
                traceback.print_exc()

print(f"Successfully processed {len(list_of_dfs)} files. Unioning all DataFrames...")

if not list_of_dfs:
    raise Exception("No dataframes were processed. Check S3 path and file contents.")

# --- Check individual DataFrames before union ---
for i, df in enumerate(list_of_dfs):
    count = df.count()
    print(f"DataFrame {i}: {count} rows")

>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
from functools import reduce
from pyspark.sql import DataFrame
final_df = reduce(DataFrame.unionByName, list_of_dfs)

<<<<<<< HEAD
print(f"Final DataFrame has {final_df.count()} rows.")

# --- Write the final, combined data to S3 as Parquet ---
# Partitioning by YEAR
final_df.write \
    .partitionBy("YEAR") \
    .mode("overwrite") \
    .parquet(f"s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")

print(f"Successfully wrote combined Parquet data to s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")
=======
# Check after union but before dropna
pre_dropna_count = final_df.count()
print(f"After union, before dropna: {pre_dropna_count} rows")

final_df = final_df.dropna(how='all')

row_count = final_df.count()
print(f"Final DataFrame has {row_count} rows after dropna.")

# --- DEBUG: Show sample of final data ---
if row_count > 0:
    print("Sample of final data:")
    final_df.show(5, truncate=False)
    
    final_df.write \
        .partitionBy("YEAR") \
        .mode("overwrite") \
        .parquet(f"s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")

    print(f"Successfully wrote combined Parquet data (2016-2023) to s3://{S3_BUCKET_NAME}/{PROCESSED_PATH}")
else:
    print("Final DataFrame has 0 rows. No data will be written to S3.")
    # Additional debugging
    print("Debugging empty final DataFrame...")
    print("Schema of final DataFrame:")
    final_df.printSchema()
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904

job.commit()