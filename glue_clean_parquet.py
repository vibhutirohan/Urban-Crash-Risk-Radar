import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ---------- Parameters ----------
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'RAW_PREFIX', 'ACC_OUT', 'TEMP_DIR']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_PREFIX = args['RAW_PREFIX']
ACC_OUT = args['ACC_OUT']
TEMP_DIR = args['TEMP_DIR']

print("🚀 Glue Job Started")
print(f"RAW_PREFIX: {RAW_PREFIX}")
print(f"ACC_OUT: {ACC_OUT}")
print(f"TEMP_DIR: {TEMP_DIR}")

# ---------- Read CSV ----------
def read_csv(path):
    print(f"🔍 Reading from: {path}")
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        print(f"✅ Loaded {df.count()} rows from {path}")
        return df
    except Exception as e:
        print(f"⚠️ Failed to read {path}: {str(e)}")
        return None

acc_df = read_csv(f"{RAW_PREFIX}accident/year=2016/ACCIDENT.CSV")

# ---------- Clean ----------
if acc_df:
    acc_df = acc_df.dropna(how="all")
    acc_df = acc_df.dropDuplicates()

# ---------- Write Parquet ----------
if acc_df and acc_df.count() > 0:
    print(f"💾 Writing cleaned data to {ACC_OUT}")
    acc_df.write.mode("overwrite").parquet(ACC_OUT)
    print(f"✅ Successfully wrote Parquet to {ACC_OUT}")
else:
    print("⚠️ No data found, skipping write.")

job.commit()
