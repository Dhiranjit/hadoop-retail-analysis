# scripts/analytics.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, year, month,
    sum as _sum,
    countDistinct
)
from common import create_spark_session
from configs.config import (
    PROCESSED_DATA_PATH,
    OUTPUT_PATH,
    APP_NAME_ANALYTICS
)


def main():
    spark = create_spark_session(APP_NAME_ANALYTICS)

    # -------------------------------
    # Read Processed Data
    # -------------------------------
    df = spark.read.parquet(PROCESSED_DATA_PATH)

    # -------------------------------
    # Monthly Revenue
    # -------------------------------
    monthly_sales = df.groupBy(
        year("InvoiceDate").alias("Year"),
        month("InvoiceDate").alias("Month")
    ).agg(
        _sum("Revenue").alias("TotalRevenue")
    ).orderBy("Year", "Month")

    monthly_sales.show()

    monthly_sales.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH + "monthly_sales/")

    # -------------------------------
    # Top 10 Products
    # -------------------------------
    top_products = df.groupBy("Description") \
        .agg(_sum("Revenue").alias("TotalRevenue")) \
        .orderBy(col("TotalRevenue").desc()) \
        .limit(10)

    top_products.show()

    top_products.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH + "top_products/")

    # -------------------------------
    # Revenue by Country
    # -------------------------------
    country_sales = df.groupBy("Country") \
        .agg(_sum("Revenue").alias("TotalRevenue")) \
        .orderBy(col("TotalRevenue").desc())

    country_sales.show()

    country_sales.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH + "country_sales/")

    # -------------------------------
    # Unique Customers per Country
    # -------------------------------
    customer_count = df.groupBy("Country") \
        .agg(countDistinct("CustomerID").alias("UniqueCustomers")) \
        .orderBy(col("UniqueCustomers").desc())

    customer_count.show()

    customer_count.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH + "customer_count/")

    print("Analytics completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()