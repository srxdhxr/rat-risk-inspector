# Rat Risk Inspector 🐀

A data pipeline for analyzing restaurant and rat inspection data from NYC Open Data to assess health and safety risks.

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)

## 🎯 Overview

The Rat Risk Inspector fetches restaurant and rat inspection data from NYC Open Data, processes it through dbt transformations, and creates risk scores at restaurant, street, and zipcode levels.

### Key Features

- **Data Ingestion**: Automated fetching of restaurant and rat inspection data from NYC Open Data API
- **Data Transformation**: ETL pipeline using dbt for data cleaning and modeling
- **Risk Scoring**: Scoring logic to capture Risk Score at the Restaurant Level, Street Level and the Zipcode Level.

## 🔧 Prerequisites

- Python 3.8+
- NYC Open Data API Token
- MotherDuck account (optional)
- PostgreSQL (optional)

## 📦 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/rat-risk-inspector.git
cd rat-risk-inspector
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## ⚙️ Configuration

Create a `.env` file in the project root:

```env
# NYC Open Data API
APP_TOKEN=your_nyc_open_data_token_here

# MotherDuck (optional)
MD_TOKEN=your_motherduck_token_here

# PostgreSQL (optional)
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DB=your_database_name
PG_USERNAME=your_username
PG_PASSWORD=your_password
```

## 🚀 Usage

### Data Ingestion

#### Fetch Restaurant Inspection Data

```bash
# Fetch data from API and save as parquet
python -m ingestion.restaurant_inspection.restaurant_inspection fetch

# Copy data to MotherDuck
python -m ingestion.restaurant_inspection.restaurant_inspection copy 20250121
```

#### Fetch Rat Inspection Data

```bash
# Fetch data from API and save as parquet
python -m ingestion.rat_inspection.rat_inspection fetch

# Copy data to MotherDuck
python -m ingestion.rat_inspection.rat_inspection copy 20250121
```

### Data Transformation

```bash
cd rri_transformation

# Run dbt models
dbt run

# Run tests
dbt test
```

### Data Export

#### Export to CSV/Parquet

```bash
cd mart_transport
python mart_extractor.py
```

#### Load to PostgreSQL

```bash
cd mart_transport
python load_to_pg.py
```

## 📁 Project Structure

```
rat-risk-inspector/
├── ingestion/                    # Data ingestion modules
│   ├── fetcher.py               # Base DataFetcher class
│   ├── rat_inspection/          # Rat inspection data fetcher
│   └── restaurant_inspection/   # Restaurant inspection data fetcher
├── rri_transformation/          # dbt project
│   ├── models/
│   │   ├── stage/              # Staging models
│   │   ├── clean/              # Clean models
│   │   └── marts/duckdb/       # Mart models
│   └── dbt_project.yml
├── mart_transport/              # Data transport utilities
│   ├── mart_extractor.py       # Export to CSV/Parquet
│   └── load_to_pg.py          # Load to PostgreSQL
├── data/                       # Data storage
│   ├── raw/                   # Raw data
│   └── clean/                 # Clean data
└── requirements.txt
```

## 🗄️ Database Schema

### Raw Tables

The ingestion process creates these raw tables:

- `raw.restaurant_inspection` - Restaurant inspection data from NYC Open Data
- `raw.rat_inspection` - Rat inspection data from NYC Open Data

### Mart Tables

The dbt transformations create these mart tables:

- `main_mart.mart_restaurant_inspection` - Clean restaurant inspection facts
- `main_mart.mart_rat_inspection` - Clean rat inspection facts  
- `main_mart.mart_rat_risk_model` - Combined risk assessment model
- `main_mart.restaurant_risk_analysis` - Restaurant risk scoring
- `main_mart.rat_inspection_analysis` - Rat inspection analysis

### Risk Scoring

The system calculates risk scores at three levels:

1. **Restaurant Level**: Based on violation history, grades, and inspection frequency
2. **Street Level**: Based on rat activity and restaurant density on the street
3. **Zipcode Level**: Based on area-wide risk patterns

Risk categories: LOW, MODERATE, HIGH, VERY HIGH

---

**Built with ❤️ for public health and safety**
