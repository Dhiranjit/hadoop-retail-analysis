# scripts/pipeline.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import col, to_timestamp
from common import create_spark_session, get_retail_schema
from configs.config import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    APP_NAME_PIPELINE
)


def main():
    spark = create_spark_session(APP_NAME_PIPELINE)

    # -------------------------------
    # Read Raw Data
    # -------------------------------
    df = spark.read \
        .option("header", True) \
        .schema(get_retail_schema()) \
        .csv(RAW_DATA_PATH)

    print("Initial Record Count:", df.count())

    # -------------------------------
    # Data Cleaning
    # -------------------------------

    # Remove cancelled invoices
    df = df.filter(~col("InvoiceNo").startswith("C"))

    # Remove invalid quantities
    df = df.filter(col("Quantity") > 0)

    # Remove null customers
    df = df.filter(col("CustomerID").isNotNull())

    print("After Cleaning Record Count:", df.count())

    # -------------------------------
    # Transformations
    # -------------------------------

    # Convert to timestamp
    df = df.withColumn(
        "InvoiceDate",
        to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
    )

    # Add Revenue column
    df = df.withColumn(
        "Revenue",
        col("Quantity") * col("UnitPrice")
    )

    # -------------------------------
    # Save as Partitioned Parquet
    # -------------------------------
    df.write \
        .mode("overwrite") \
        .partitionBy("Country") \
        .parquet(PROCESSED_DATA_PATH)

    print("Pipeline completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()