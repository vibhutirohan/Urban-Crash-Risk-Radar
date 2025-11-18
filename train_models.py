import boto3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.base import clone
from sklearn.metrics import mean_absolute_error
import xgboost as xgb
import json
import traceback


## 1. SETUP AND CONFIGURATION
# ----------------------------------------------------
# S3 bucket and paths
<<<<<<< HEAD
S3_BUCKET_NAME = "CrashRiskRadar2025"
=======
S3_BUCKET_NAME = "crash-risk-radar-2025"
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
# output path from  AWS Glue job
PROCESSED_DATA_PATH = "processedData/"
# final GeoJSON heatmap files will be saved
OUTPUT_GEOJSON_PATH = "output-GeoJSON/"
# final JSON stats files will be saved for model performance
OUTPUT_STATS_PATH = "output-stats/"

# Predefined cities and states to process
CITIES_TO_PROCESS = {
    'los angeles': 'california',
    'houston': 'texas',
    'detroit': 'michigan',
    'dallas': 'texas',
    'memphis': 'tennessee'
}

# Pre-generated Year Options ---
<<<<<<< HEAD
YEAR_OPTIONS = [1, 5, 10, 20, 50] # Will create 4 separate files: 1-year, 5-year, 10-year, 20-year, 50-year
=======
YEAR_OPTIONS = [1, 3, 5, 8] 
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904

# Initialize Boto3 S3 client
s3 = boto3.client('s3')

## 2. CUSTOM TRANSFORMER FOR CYCLICAL FEATURES
# ----------------------------------------------------
class CyclicalEncoder(BaseEstimator, TransformerMixin):
    """Encodes cyclical features into sin/cos components."""
    def __init__(self, max_vals=None):
        self.max_vals = max_vals

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_transformed = pd.DataFrame(index=X.index)
        for col in X.columns:
            if col in self.max_vals:
                max_val = self.max_vals[col]
                col_data = pd.to_numeric(X[col], errors='coerce')
<<<<<<< HEAD
                X_transformed[f'{col}_sin'] = np.sin(2 * np.pi * col_data / max_val)
                X_transformed[f'{col}_cos'] = np.cos(2 * np.pi * col_data / max_val)
=======
                X_transformed[f'{col}_sin'] = np.sin(2 * np.pi * col_data / max_val) #sin transformation for cyclical
                X_transformed[f'{col}_cos'] = np.cos(2 * np.pi * col_data / max_val) #cos transformation for cyclical
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
        return X_transformed.fillna(0)
    
    def get_feature_names_out(self, input_features=None):
        """Generates the output feature names."""
        output_features = []
        for col in input_features:
            output_features.append(f'{col}_sin')
            output_features.append(f'{col}_cos')
        return np.array(output_features, dtype=object)

## 3. GLOBAL CONFIGURATION 
# ----------------------------------------------------
#  columns to keep and feature types.
COLUMNS_TO_KEEP = [
    'YEAR', 'MONTH', 'LGT_COND', 'DAY', 'DAY_WEEK', 'HOUR', 'FUNC_SYS', 
    'RD_OWNER', 'RELJCT2', 'WEATHER', 'ROUTE', 'TWAY_ID', 
    'TYP_INT', 'REL_ROAD', 'LATITUDE', 'LONGITUD', 'CITYNAME', 'STATENAME',
    'is_rush_hour', 'bad_weather_dark'
]

CYCLICAL_FEATURES = ['MONTH', 'DAY', 'DAY_WEEK', 'HOUR']
CATEGORICAL_FEATURES = [
    'LGT_COND', 'FUNC_SYS', 'RD_OWNER', 'RELJCT2', 'WEATHER', 
    'REL_ROAD', 'TWAY_ID', 'TYP_INT', 'ROUTE', 
<<<<<<< HEAD
    'is_rush_hour', 'bad_weather_dark'
]
CYCLICAL_MAX_VALS = {'MONTH': 12, 'DAY': 31, 'DAY_WEEK': 7, 'HOUR': 24}

# Define the preprocessor pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ('cyclical', CyclicalEncoder(max_vals=CYCLICAL_MAX_VALS), CYCLICAL_FEATURES),
        ('categorical', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_FEATURES)
=======
    'is_rush_hour', 'bad_weather_dark' #rush hour and bad weather are engineered features
]
CYCLICAL_MAX_VALS = {'MONTH': 12, 'DAY': 31, 'DAY_WEEK': 7, 'HOUR': 24}

# Preprocessor pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ('cyclical', CyclicalEncoder(max_vals=CYCLICAL_MAX_VALS), CYCLICAL_FEATURES), # cyclical for cyclical
        ('categorical', OneHotEncoder(handle_unknown='ignore', sparse_output=False), CATEGORICAL_FEATURES) # one hot encoding for categorical
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
    ],
    remainder='passthrough' # 'YEAR' will be passed through
)


## 4. FUNCTIONS
# load_and_filter_data: loading and filtering data
# engineer_features: feature engineering
# generate_geojson_output: generate geojsons
# generate_stats_json_output: generate model performance stats
# run_analysis_for_city: run the analysis for the specified city (recursive, calls on above functions)
# ----------------------------------------------------

def load_and_filter_data(bucket, s3_path, years_to_use):
    """
    Loads the full Parquet dataset, filters by year, and pre-cleans.
    Reads from S3 every time to conserve memory.
    """
    print(f"Loading full dataset from s3://{bucket}/{s3_path}...")
    s3_uri = f"s3://{bucket}/{s3_path}"
    full_df = pd.read_parquet(s3_uri)
    print(f"Loaded {len(full_df)} total records.")
    
    # Filter by specified year range
    if 'YEAR' not in full_df.columns:
        raise ValueError("'YEAR' column not found in processed data. Please ensure your Glue script adds it.")
        
    df_copy = full_df.copy() # copy to avoid modifying the original df
    df_copy['YEAR'] = pd.to_numeric(df_copy['YEAR'], errors='coerce')
    max_year = df_copy['YEAR'].max()
    min_year = (max_year - years_to_use) + 1
    
    df_filtered = df_copy[(df_copy['YEAR'] >= min_year) & (df_copy['YEAR'] <= max_year)].copy()
    print(f"Filtered data to {years_to_use} year(s) ({min_year}-{max_year}). {len(df_filtered)} records remaining.")

    # Convert to lowercase for consistent filtering
    object_cols = df_filtered.select_dtypes(include=['object']).columns
<<<<<<< HEAD
    df_filtered[object_cols] = df_filtered[object_cols].apply(lambda x: x.str.lower())
=======
    df_filtered[object_cols] = df_filtered[object_cols].apply(lambda x: x.str.lower()) # to lower
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
    print("Data loaded and cleaned.")
    return df_filtered # Return only the filtered dataframe

def engineer_features(df):
    """Creates new composite features from existing ones."""
    print("Engineering new features...")
    df_eng = df.copy()

    # Feature 1: Rush Hour (Weekdays 7-9am & 4-6pm)
    # DAY_WEEK: Sunday=1, Monday=2, ..., Saturday=7
    is_weekday = df_eng['DAY_WEEK'].between(2, 6)
    is_morning_rush = df_eng['HOUR'].between(7, 9)
    is_evening_rush = df_eng['HOUR'].between(16, 18)
    df_eng['is_rush_hour'] = (is_weekday & (is_morning_rush | is_evening_rush)).astype(int)

    # Feature 2: Bad Weather at Night
    # LGT_COND: Daylight=1, Dark conditions > 1
    # WEATHER: Clear=1, Bad weather (Rain, Snow, Fog, etc.) > 1
    is_dark = df_eng['LGT_COND'] > 1
    is_bad_weather = df_eng['WEATHER'] > 1
    df_eng['bad_weather_dark'] = (is_dark & is_bad_weather).astype(int)
    
    print("Feature engineering complete.")
    return df_eng

def generate_geojson_output(lat_preds, lon_preds, city_name, state_name, years, bucket, s3_prefix):
    """
    Creates a GeoJSON of predicted points and saves it to S3.
    """
    print(f"Generating GeoJSON for {city_name}, {state_name} ({years} years)...")
    
    # Create a GeoDataFrame from the predicted points
    gdf_points = gpd.GeoDataFrame(
<<<<<<< HEAD
        geometry=gpd.points_from_xy(lon_preds, lat_preds),
        crs="EPSG:4326" # Set standard WGS84 coordinate system
=======
        geometry=gpd.points_from_xy(lon_preds, lat_preds), # gets points predicted (latitude and longitude)
        crs="EPSG:4326" #  standard WGS84 coordinate system
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
    )
    
    # Convert the GeoDataFrame to a GeoJSON string
    geojson_output = gdf_points.to_json()

    # Save the GeoJSON file to S3 with the year count in the name
    filename = f"{city_name.lower().replace(' ', '_')}_{state_name.lower().replace(' ', '_')}_{years}_years_heatmap.geojson"
    s3_key = f"{s3_prefix}{filename}"

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=geojson_output,
        ContentType='application/json'
    )
    print(f"✅ GeoJSON of predicted points saved to: s3://{bucket}/{s3_key}")

# Function to save performance statistics 
def generate_stats_json_output(stats_dict, city_name, state_name, years, bucket, s3_prefix):
    """
    Creates a JSON file of model performance stats and saves it to S3.
    """
    print(f"Generating stats JSON for {city_name}, {state_name} ({years} years)...")
    
    # Convert the dict to a JSON string
    stats_json = json.dumps(stats_dict, indent=2)

    # Save the JSON file to S3 with the year count in the name
    filename = f"{city_name.lower().replace(' ', '_')}_{state_name.lower().replace(' ', '_')}_{years}_years_stats.json"
    s3_key = f"{s3_prefix}{filename}"

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=stats_json,
        ContentType='application/json'
    )
    print(f"✅ Stats JSON saved to: s3://{bucket}/{s3_key}")
    
    
def run_analysis_for_city(df, city_name, state_name, years_to_use):
    """
    Filters data for a specific city, trains models, evaluates performance,
    and generates a GeoJSON of predicted points.
    """
    print("-" * 50)
    print(f"Starting analysis for: {city_name.upper()}, {state_name.upper()} ({years_to_use} years)")
    print("-" * 50)

    # Filter and prepare data for the chosen city and state
<<<<<<< HEAD
    city_df = df[(df['CITYNAME'] == city_name.lower()) & (df['STATENAME'] == state_name.lower())].copy()
=======
    city_df = df[(df['CITYNAME'] == city_name.lower()) & (df['STATENAME'] == state_name.lower())].copy() #city, state filtering
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904

    if city_df.empty:
        print(f"Warning: No data found for '{city_name}, {state_name}'. Skipping.")
        return
<<<<<<< HEAD
        
=======
    
    # Run if there are at least 10 data points
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
    if city_df.shape[0] < 10:
        print(f"Warning: Not enough data for '{city_name}, {state_name}' to train a model. Need at least 10 samples. Skipping.")
        return

    # Engineer new features
    city_df_eng = engineer_features(city_df)

    # Keep only the necessary columns and drop rows with missing coordinates
    # Ensure all columns in COLUMNS_TO_KEEP exist
    available_cols = [col for col in COLUMNS_TO_KEEP if col in city_df_eng.columns]
    city_df_final = city_df_eng[available_cols].dropna(subset=['LATITUDE', 'LONGITUD'])

<<<<<<< HEAD
    # Split data into features and targets
    X = city_df_final.drop(columns=['LATITUDE', 'LONGITUD', 'CITYNAME', 'STATENAME'])
=======
    # Split data into features and targets (80% 20%)
    X = city_df_final.drop(columns=['LATITUDE', 'LONGITUD', 'CITYNAME', 'STATENAME']) # DROP ALL IDENTIFYING VARIABLES FROM TRAINING SET!
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
    y_lat = city_df_final['LATITUDE']
    y_lon = city_df_final['LONGITUD']
    
    X_train, X_test, y_lat_train, y_lat_test, y_lon_train, y_lon_test = train_test_split(
        X, y_lat, y_lon, test_size=0.2, random_state=42
    )
    print(f"Data for {city_name} split: {X_train.shape[0]} training samples, {X_test.shape[0]} testing samples.")

<<<<<<< HEAD
    # Define the model and create the full pipeline
=======
    #  XGBoost model pipeline for large number of features
>>>>>>> a0b039543a5248fe551e6cf83e7a01fa772c3904
    model = xgb.XGBRegressor(n_estimators=100, random_state=42, n_jobs=-1, eval_metric='mae')
    base_pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('regressor', model)])

    # Train two separate models (for latitude and longitude)
    print("Training models...")
    pipeline_lat = clone(base_pipeline).fit(X_train, y_lat_train)
    pipeline_lon = clone(base_pipeline).fit(X_train, y_lon_train)
    print("Models trained successfully.")

    # Make predictions and evaluate performance
    lat_predictions = pipeline_lat.predict(X_test)
    lon_predictions = pipeline_lon.predict(X_test)

    lat_mae = mean_absolute_error(y_lat_test, lat_predictions)
    lon_mae = mean_absolute_error(y_lon_test, lon_predictions)

    print("\n--- XGBoost Model Performance (on test data) ---")
    print(f"Latitude Model MAE: {lat_mae:.4f} degrees")
    print(f"Longitude Model MAE: {lon_mae:.4f} degrees\n")

    # Generate the visual GeoJSON
    # We use the test predictions for the output heatmap
    generate_geojson_output(
        lat_predictions, 
        lon_predictions, 
        city_name, 
        state_name,
        years_to_use, # Pass the year count for file naming
        S3_BUCKET_NAME, 
        OUTPUT_GEOJSON_PATH
    )
    
# Generate the statistics JSON 
    stats = {
        'training_samples': int(X_train.shape[0]), 
        'testing_samples': int(X_test.shape[0]),
        'latitude_mae_degrees': round(lat_mae, 6),
        'longitude_mae_degrees': round(lon_mae, 6),
        'total_samples_for_city': int(city_df.shape[0]),
        'data_years_used': years_to_use
    }
    
    generate_stats_json_output(
        stats,
        city_name,
        state_name,
        years_to_use,
        S3_BUCKET_NAME,
        OUTPUT_STATS_PATH
    )

## 5. Main Execution Block
# ----------------------------------------------------

if __name__ == "__main__":
    print(f"--- Starting Model Training Pipeline ---")

    # --- Loop over each city defined in the config ---
    for city, state in CITIES_TO_PROCESS.items():
        
        print("="*60)
        print(f"Processing City: {city.upper()}, {state.upper()}")
        print(f"Generating models for year options: {YEAR_OPTIONS}")
        print("="*60)

        # Loop over each year option to generate
        for years in YEAR_OPTIONS:
            try:
                # Load and filter the data
                # This now reads from S3 every time, preventing memory errors.
                filtered_df = load_and_filter_data(
                    S3_BUCKET_NAME, 
                    PROCESSED_DATA_PATH, 
                    years
                )
                
                # Run the analysis for the current city and year filter
                run_analysis_for_city(
                    filtered_df, 
                    city,  # Use the loop variable
                    state, # Use the loop variable
                    years
                )

                print(f"--- Completed analysis for {years} year(s) ---")

            except Exception as e:
                print(f"An error occurred during the analysis for {city}, {years} years: {e}")
                traceback.print_exc()

    print("="*60)
    print("All analyses for all cities complete.")
    print("="*60)