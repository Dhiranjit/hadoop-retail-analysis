# Hadoop Retail Analysis

End-to-end big data pipeline for the **Online Retail II** dataset using **Apache Hadoop (HDFS)** and **Apache Spark (PySpark)**. The pipeline ingests raw transactional CSV data, cleans and transforms it, computes 7 business analytics, exports results as CSV, and generates visualisation plots.

---

## Dataset

| Field         | Description                                       |
| ------------- | ------------------------------------------------- |
| `InvoiceNo`   | 6-digit transaction ID. Prefix `C` = cancellation |
| `StockCode`   | 5-digit product code                              |
| `Description` | Product name                                      |
| `Quantity`    | Units per transaction                             |
| `InvoiceDate` | Date and time of transaction                      |
| `UnitPrice`   | Price per unit in GBP (£)                         |
| `CustomerID`  | 5-digit customer identifier                       |
| `Country`     | Country of the customer                           |

Source: [UCI ML Repository — Online Retail II](https://archive.ics.uci.edu/ml/datasets/Online+Retail+II)

---

## Project Structure

```
hadoop-retail-analysis/
├── configs/
│   └── config.py               # All HDFS paths, local output paths, app names
├── scripts/
│   ├── common.py               # Shared SparkSession factory and schema definition
│   ├── pipeline.py             # Stage 1: Ingest → Clean → Transform → Write Parquet
│   ├── analytics.py            # Stage 2: Aggregations → HDFS Parquet + local CSV
│   └── visualize.py            # Stage 3: Read CSV → Generate PNG plots
└── outputs/                    # Created at runtime (git-ignored)
    ├── csv/                    # CSV exports from analytics.py
    └── plots/                  # PNG plots from visualize.py
```

---

## Prerequisites

- Hadoop cluster running with YARN
- `HADOOP_CONF_DIR` pointing to your Hadoop config (e.g. `/opt/hadoop/etc/hadoop`)
- PySpark installed in your Python environment
- `matplotlib` and `seaborn` installed for visualizations

---

## Step 0 — Set Up HDFS Directory Structure

Create the required directories in HDFS before running anything:

```bash
# Raw data input directory
hdfs dfs -mkdir -p /data/raw/online_retail

# Processed data directory (written by pipeline.py)
hdfs dfs -mkdir -p /data/processed/online_retail

# Analytics output directory (written by analytics.py)
hdfs dfs -mkdir -p /data/output/online_retail
```

Verify the directories were created:

```bash
hdfs dfs -ls /data/
```

---

## Step 1 — Upload the Dataset to HDFS

Put the raw CSV file from your local machine into HDFS:

```bash
hdfs dfs -put /path/to/OnlineRetail.csv /data/raw/online_retail/OnlineRetail.csv
```

Verify the file is in HDFS:

```bash
hdfs dfs -ls /data/raw/online_retail/
```

---

## Step 2 — Run the Data Pipeline (`pipeline.py`)

**What it does:**

1. **Reads** `OnlineRetail.csv` from HDFS with an enforced schema (no type inference).
2. **Cleans** the data with three filters:
   - Drops cancelled invoices (`InvoiceNo` starting with `C`)
   - Drops rows with `Quantity <= 0`
   - Drops rows with a null `CustomerID`
3. **Transforms** the data:
   - Parses `InvoiceDate` from string to proper timestamp (`M/d/yyyy H:mm`)
   - Adds a derived `Revenue` column: `Quantity × UnitPrice`
4. **Writes** the cleaned and enriched DataFrame to HDFS as **Parquet**, partitioned by `Country` (enables partition pruning for downstream queries).

**HDFS output:** `hdfs:///data/processed/online_retail/` (partitioned Parquet)

```bash
spark-submit --master yarn scripts/pipeline.py
```

---

## Step 3 — Run Analytics (`analytics.py`)

**What it does:**

Reads the processed Parquet from HDFS and computes 7 aggregations. Each result is written to:

- **HDFS** as Parquet (under `hdfs:///data/output/online_retail/`)
- **Local disk** as CSV (under `outputs/csv/`)

| #   | Metric                       | Operation                                           | Output File           |
| --- | ---------------------------- | --------------------------------------------------- | --------------------- |
| 1   | Monthly Revenue              | `SUM(Revenue)` grouped by Year + Month              | `monthly_sales.csv`   |
| 2   | Top 10 Products              | `SUM(Revenue)` grouped by Description, top 10       | `top_products.csv`    |
| 3   | Revenue by Country           | `SUM(Revenue)` grouped by Country                   | `country_sales.csv`   |
| 4   | Unique Customers per Country | `COUNT(DISTINCT CustomerID)` by Country             | `customer_count.csv`  |
| 5   | Avg Order Value per Month    | `SUM(Revenue) / COUNT(DISTINCT InvoiceNo)` by Month | `avg_order_value.csv` |
| 6   | Top 10 Customers             | `SUM(Revenue)` grouped by CustomerID, top 10        | `top_customers.csv`   |
| 7   | Month-over-Month Growth %    | `lag()` window function on monthly revenue          | `mom_growth.csv`      |

```bash
spark-submit --master yarn scripts/analytics.py
```

---

## Step 4 — Generate Visualizations (`visualize.py`)

**What it does:**

Reads all 7 local CSV files from `outputs/csv/` and generates 7 PNG plots saved to `outputs/plots/`. This script runs entirely locally — no Spark or HDFS needed.

| #   | Plot                              | Chart Type                                       |
| --- | --------------------------------- | ------------------------------------------------ |
| 1   | Monthly Revenue Over Time         | Line + fill                                      |
| 2   | Average Order Value per Month     | Line + fill                                      |
| 3   | Month-over-Month Revenue Growth % | Diverging bar (green = positive, red = negative) |
| 4   | Top 10 Products by Revenue        | Horizontal bar                                   |
| 5   | Top 10 Countries by Revenue       | Horizontal bar                                   |
| 6   | Unique Customers per Country      | Horizontal bar                                   |
| 7   | Top 10 Customers by Revenue       | Horizontal bar                                   |

```bash
python scripts/visualize.py
```

---

## Data Flow

```
Local Machine
└── OnlineRetail.csv
        │
        │  hdfs dfs -put
        ▼
HDFS: /data/raw/online_retail/OnlineRetail.csv
        │
        │  spark-submit scripts/pipeline.py
        │  - Drop cancellations (InvoiceNo starts with C)
        │  - Drop Quantity <= 0
        │  - Drop null CustomerID
        │  - Parse InvoiceDate to timestamp
        │  - Add Revenue = Quantity × UnitPrice
        ▼
HDFS: /data/processed/online_retail/  (Parquet, partitioned by Country)
        │
        │  spark-submit scripts/analytics.py
        │  - 7 aggregation queries
        │  - Write each result → HDFS Parquet
        │  - Write each result → Local CSV
        ▼
HDFS: /data/output/online_retail/     (Parquet per metric)
Local: outputs/csv/                   (CSV per metric)
        │
        │  python scripts/visualize.py
        │  - Read CSVs with pandas
        │  - Generate 7 matplotlib/seaborn plots
        ▼
Local: outputs/plots/                 (7 PNG files)
```

---

## Configuration

All paths and app names are centralised in [configs/config.py](configs/config.py):

```python
RAW_DATA_PATH       = "hdfs:///data/raw/online_retail/OnlineRetail.csv"
PROCESSED_DATA_PATH = "hdfs:///data/processed/online_retail/"
OUTPUT_PATH         = "hdfs:///data/output/online_retail/"
LOCAL_CSV_OUTPUT_PATH = "outputs/csv/"
LOCAL_PLOTS_PATH      = "outputs/plots/"
```

Change these if your HDFS paths or local output directories differ.

---

## Script Reference

| Script                                       | Purpose                                                                  |
| -------------------------------------------- | ------------------------------------------------------------------------ |
| [configs/config.py](configs/config.py)       | Centralised path and app name constants                                  |
| [scripts/common.py](scripts/common.py)       | `create_spark_session()` and `get_retail_schema()` shared across scripts |
| [scripts/pipeline.py](scripts/pipeline.py)   | Data ingestion, cleaning, transformation and HDFS Parquet write          |
| [scripts/analytics.py](scripts/analytics.py) | 7 business aggregations, writes HDFS Parquet + local CSV                 |
| [scripts/visualize.py](scripts/visualize.py) | Reads local CSVs, generates and saves 7 PNG visualisation plots          |
