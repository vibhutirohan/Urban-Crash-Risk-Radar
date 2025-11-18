import os, re, boto3, zipfile
from io import BytesIO

# -------- CONFIG --------
SRC_BUCKET = "crash-risk-radar2025"
SRC_PREFIX = "rawData/NHTSA-zips/"
DST_BUCKET = "crash-risk-radar2025"
DST_BASE   = "raw/fars"   # we'll write under raw/fars/<table>/year=YYYY/...
YEARS = list(range(2016, 2024))   # keep it tight for the project
KEEP = {
    "accident": "ACCIDENT.CSV",
    "vehicle":  "VEHICLE.CSV",
    "person":   "PERSON.CSV",
}
# ------------------------

s3 = boto3.client("s3")

def list_year_zips():
    # Returns dict {year: key}
    resp = s3.list_objects_v2(Bucket=SRC_BUCKET, Prefix=SRC_PREFIX)
    out = {}
    while True:
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            m = re.search(r"FARS(\d{4})NationalCSV\.zip$", key)
            if m:
                out[int(m.group(1))] = key
        if resp.get("IsTruncated"):
            resp = s3.list_objects_v2(Bucket=SRC_BUCKET, Prefix=SRC_PREFIX,
                                      ContinuationToken=resp["NextContinuationToken"])
        else:
            break
    return out

def put_bytes(bucket, key, data_bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=data_bytes)

def main():
    year_to_key = list_year_zips()
    found = sorted(y for y in YEARS if y in year_to_key)
    print("Years found:", found)

    for y in found:
        zip_key = year_to_key[y]
        print(f"[{y}] reading {zip_key}")
        obj = s3.get_object(Bucket=SRC_BUCKET, Key=zip_key)
        zf = zipfile.ZipFile(BytesIO(obj["Body"].read()))
        names = {n.upper(): n for n in zf.namelist()}

        for table, fname in KEEP.items():
            if fname in names:
                data = zf.read(names[fname])
                out_key = f"{DST_BASE}/{table}/year={y}/{fname}"
                put_bytes(DST_BUCKET, out_key, data)
                print(f"  -> s3://{DST_BUCKET}/{out_key} ({len(data)} bytes)")
            else:
                print(f"  !! {fname} not present for {y}")

if __name__ == "__main__":
    main()
