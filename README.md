<!-- Hero -->
<div align="center">
  <img src="https://user-images.githubusercontent.com/placeholder/urban-crash-radar-hero.gif" alt="Urban Crash Risk Radar" width="820">
  <h1>🚦 Urban Crash Risk Radar — 5 Major US Cities 
    (AWS End-to-End Data Engineering Project)</h1>
  <p><strong>Authors:</strong> Rohan & Ellen</p>

  <!-- Badges -->
  <p>
    <img alt="AWS Only" src="https://img.shields.io/badge/Cloud-AWS%20Only-f7941d?logo=amazonaws&logoColor=white">
    <img alt="S3" src="https://img.shields.io/badge/S3-Lakehouse-569A31?logo=amazons3&logoColor=white">
    <img alt="Glue" src="https://img.shields.io/badge/Glue-ETL%20%26%20Catalog-6B46C1">
    <img alt="Athena" src="https://img.shields.io/badge/Athena-SQL%20&%20CTAS-2563EB">
    <img alt="Lambda" src="https://img.shields.io/badge/Lambda-APIs%20%26%20Ingest-FF9900?logo=awslambda&logoColor=white">
    <img alt="SageMaker" src="https://img.shields.io/badge/SageMaker-Model%20Training-0E9">
  </p>

  <p>
    <em>Memphis (TN) • Detroit (MI) • New York City (NY) • Boston (MA) • Los Angeles (CA)</em>
  </p>

  <h3>“A multi-city lakehouse that ingests crash & weather data, engineers features, trains a predictive model, and serves hourly risk heatmaps on the web — using AWS only.”</h3>
</div>

---

<!-- Animated divider -->
<p align="center">
  <img src="https://user-images.githubusercontent.com/placeholder/pulse-divider.gif" width="640" alt="divider">
</p>

## 🧭 Problem → Why This Matters
Cities pay a steep price for road crashes — in lives, in health, and in billions of dollars. Agencies need **early signals** of **where** and **when** risk is rising so they can target enforcement, redesign streets, and deploy resources before the next collision.

> We transform raw civic data into **hourly, cell-level risk** that leaders can act on.

## 🎯 Goal (One Line)
Continuously ingest crash data for five U.S. cities, enrich with weather & time features, **predict crash risk per grid cell per hour**, and publish a **map-based heatmap** for cross-city comparison.

---

<!-- Subtle animated icons row -->
<p align="center">
  <img src="https://user-images.githubusercontent.com/placeholder/ingest.gif" width="90">
  <img src="https://user-images.githubusercontent.com/placeholder/transform.gif" width="90">
  <img src="https://user-images.githubusercontent.com/placeholder/train.gif" width="90">
  <img src="https://user-images.githubusercontent.com/placeholder/serve.gif" width="90">
  <img src="https://user-images.githubusercontent.com/placeholder/visualize.gif" width="90">
</p>

## ⚙️ Pipeline at a Glance
- **Ingest**: City crash feeds + NOAA weather → **Amazon S3** (`raw/`) via **AWS Lambda** on **EventBridge** schedules  
- **Process**: Clean & grid to 500–800m cells, rolling features → **AWS Glue** (PySpark) → **S3** (`silver/`, `gold/`)  
- **Query/QA**: **Amazon Athena** (CTAS, partition pruning)  
- **Model**: Binary risk model (XGBoost in **Amazon SageMaker** or Glue ML fallback) → JSONL/GeoJSON predictions  
- **Serve**: **Lambda Function URL** `/risk?city=&cell_id=` returns latest risk  
- **Visualize**: Static site on **S3** (Leaflet / Mapbox GL) renders an **animated heatmap** per city

---

## 🗺️ Cities
**Memphis • Detroit • New York City • Boston • Los Angeles**  


---

## ✨ What makes it stand out
- **Multi-city, multi-tenant lakehouse** — `city` as a first-class partition across raw/silver/gold  
- **Spatiotemporal features** — history windows, neighbor context, weather signals  
- **Production-ish flow** — scheduled ingestion, partitioned Parquet, API endpoint, static site  
- **Academy-friendly** — only core AWS; graceful fallbacks where services are limited


<!-- Footer animation -->
<p align="center">
  <img src="https://user-images.githubusercontent.com/placeholder/heatmap-loop.gif" width="760" alt="animated heatmap">
</p>
