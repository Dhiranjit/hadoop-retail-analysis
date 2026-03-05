# scripts/analytics.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, year, month,
    sum as _sum,
    countDistinct,
    lag
)
from pyspark.sql.window import Window
from common import create_spark_session
from configs.config import (
    PROCESSED_DATA_PATH,
    OUTPUT_PATH,
    LOCAL_CSV_OUTPUT_PATH,
    APP_NAME_ANALYTICS
)


def save(df, hdfs_path, csv_name, csv_dir):
    """Write a Spark DataFrame to HDFS as Parquet and locally as CSV."""
    df.write.mode("overwrite").parquet(hdfs_path)
    df.toPandas().to_csv(os.path.join(csv_dir, csv_name), index=False)


def main():
    os.makedirs(LOCAL_CSV_OUTPUT_PATH, exist_ok=True)

    spark = create_spark_session(APP_NAME_ANALYTICS)

    # -------------------------------
    # Read Processed Data
    # -------------------------------
    df = spark.read.parquet(PROCESSED_DATA_PATH)

    # ---------------------------------------------------------------
    # 1. Monthly Revenue
    # ---------------------------------------------------------------
    monthly_sales = df.groupBy(
        year("InvoiceDate").alias("Year"),
        month("InvoiceDate").alias("Month")
    ).agg(
        _sum("Revenue").alias("TotalRevenue")
    ).orderBy("Year", "Month")

    monthly_sales.show()
    save(monthly_sales,
         OUTPUT_PATH + "monthly_sales/",
         "monthly_sales.csv",
         LOCAL_CSV_OUTPUT_PATH)

    # ---------------------------------------------------------------
    # 2. Top 10 Products by Revenue
    # ---------------------------------------------------------------
    top_products = df.groupBy("Description") \
        .agg(_sum("Revenue").alias("TotalRevenue")) \
        .orderBy(col("TotalRevenue").desc()) \
        .limit(10)

    top_products.show()
    save(top_products,
         OUTPUT_PATH + "top_products/",
         "top_products.csv",
         LOCAL_CSV_OUTPUT_PATH)

    # ---------------------------------------------------------------
    # 3. Revenue by Country
    # ---------------------------------------------------------------
    country_sales = df.groupBy("Country") \
        .agg(_sum("Revenue").alias("TotalRevenue")) \
        .orderBy(col("TotalRevenue").desc())

    country_sales.show()
    save(country_sales,
         OUTPUT_PATH + "country_sales/",
         "country_sales.csv",
         LOCAL_CSV_OUTPUT_PATH)

    # ---------------------------------------------------------------
    # 4. Unique Customers per Country
    # ---------------------------------------------------------------
    customer_count = df.groupBy("Country") \
        .agg(countDistinct("CustomerID").alias("UniqueCustomers")) \
        .orderBy(col("UniqueCustomers").desc())

    customer_count.show()
    save(customer_count,
         OUTPUT_PATH + "customer_count/",
         "customer_count.csv",
         LOCAL_CSV_OUTPUT_PATH)

    # ---------------------------------------------------------------
    # 5. Average Order Value per Month
    #    (Total Revenue / Distinct Invoices for that month)
    # ---------------------------------------------------------------
    avg_order_value = df.groupBy(
        year("InvoiceDate").alias("Year"),
        month("InvoiceDate").alias("Month")
    ).agg(
        (_sum("Revenue") / countDistinct("InvoiceNo")).alias("AvgOrderValue")
    ).orderBy("Year", "Month")

    avg_order_value.show()
    save(avg_order_value,
         OUTPUT_PATH + "avg_order_value/",
         "avg_order_value.csv",
         LOCAL_CSV_OUTPUT_PATH)

    # ---------------------------------------------------------------
    # 6. Top 10 Customers by Revenue
    # ---------------------------------------------------------------
    top_customers = df.groupBy("CustomerID") \
        .agg(_sum("Revenue").alias("TotalRevenue")) \
        .orderBy(col("TotalRevenue").desc()) \
        .limit(10)

    top_customers.show()
    save(top_customers,
         OUTPUT_PATH + "top_customers/",
         "top_customers.csv",
         LOCAL_CSV_OUTPUT_PATH)

    # ---------------------------------------------------------------
    # 7. Month-over-Month Revenue Growth (%)
    # ---------------------------------------------------------------
    window_spec = Window.orderBy("Year", "Month")

    mom_growth = monthly_sales.withColumn(
        "PrevMonthRevenue",
        lag("TotalRevenue", 1).over(window_spec)
    ).withColumn(
        "MoM_Growth_Pct",
        ((col("TotalRevenue") - col("PrevMonthRevenue")) / col("PrevMonthRevenue") * 100)
    ).drop("PrevMonthRevenue")

    mom_growth.show()
    save(mom_growth,
         OUTPUT_PATH + "mom_growth/",
         "mom_growth.csv",
         LOCAL_CSV_OUTPUT_PATH)

    print("Analytics completed successfully.")
    print(f"CSV files saved to: {LOCAL_CSV_OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()