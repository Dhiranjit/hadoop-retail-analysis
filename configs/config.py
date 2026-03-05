# configs/config.py
RAW_DATA_PATH = "hdfs:///data/raw/online_retail/OnlineRetail.csv"
PROCESSED_DATA_PATH = "hdfs:///data/processed/online_retail/"
OUTPUT_PATH = "hdfs:///data/output/online_retail/"

# Local directory where CSV exports are written for visualization
LOCAL_CSV_OUTPUT_PATH = "outputs/csv/"

# Local directory where generated plot images are saved
LOCAL_PLOTS_PATH = "outputs/plots/"

APP_NAME_PIPELINE = "OnlineRetailPipeline"
APP_NAME_ANALYTICS = "OnlineRetailAnalytics"