# scripts/visualize.py

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from configs.config import LOCAL_CSV_OUTPUT_PATH, LOCAL_PLOTS_PATH


# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
os.makedirs(LOCAL_PLOTS_PATH, exist_ok=True)

CSV = LOCAL_CSV_OUTPUT_PATH
PLOTS = LOCAL_PLOTS_PATH


def savefig(name):
    path = os.path.join(PLOTS, name)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Saved: {path}")


# -------------------------------------------------------------------
# 1. Monthly Revenue — Line Chart
# -------------------------------------------------------------------
def plot_monthly_revenue():
    df = pd.read_csv(os.path.join(CSV, "monthly_sales.csv"))
    df["Period"] = df["Year"].astype(str) + "-" + df["Month"].astype(str).str.zfill(2)

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(df["Period"], df["TotalRevenue"], marker="o", linewidth=2, color="#2196F3")
    ax.fill_between(df["Period"], df["TotalRevenue"], alpha=0.15, color="#2196F3")
    ax.set_title("Monthly Revenue Over Time")
    ax.set_xlabel("Period (Year-Month)")
    ax.set_ylabel("Total Revenue (£)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}"))
    plt.xticks(rotation=45, ha="right")
    savefig("1_monthly_revenue.png")


# -------------------------------------------------------------------
# 2. Average Order Value per Month — Line Chart
# -------------------------------------------------------------------
def plot_avg_order_value():
    df = pd.read_csv(os.path.join(CSV, "avg_order_value.csv"))
    df["Period"] = df["Year"].astype(str) + "-" + df["Month"].astype(str).str.zfill(2)

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(df["Period"], df["AvgOrderValue"], marker="s", linewidth=2, color="#FF9800")
    ax.fill_between(df["Period"], df["AvgOrderValue"], alpha=0.15, color="#FF9800")
    ax.set_title("Average Order Value per Month")
    ax.set_xlabel("Period (Year-Month)")
    ax.set_ylabel("Avg Order Value (£)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.2f}"))
    plt.xticks(rotation=45, ha="right")
    savefig("2_avg_order_value.png")


# -------------------------------------------------------------------
# 3. Month-over-Month Revenue Growth — Bar Chart
# -------------------------------------------------------------------
def plot_mom_growth():
    df = pd.read_csv(os.path.join(CSV, "mom_growth.csv")).dropna(subset=["MoM_Growth_Pct"])
    df["Period"] = df["Year"].astype(str) + "-" + df["Month"].astype(str).str.zfill(2)

    colors = ["#4CAF50" if v >= 0 else "#F44336" for v in df["MoM_Growth_Pct"]]

    fig, ax = plt.subplots(figsize=(14, 5))
    bars = ax.bar(df["Period"], df["MoM_Growth_Pct"], color=colors, edgecolor="white")
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_title("Month-over-Month Revenue Growth (%)")
    ax.set_xlabel("Period (Year-Month)")
    ax.set_ylabel("Growth (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    plt.xticks(rotation=45, ha="right")
    savefig("3_mom_growth.png")


# -------------------------------------------------------------------
# 4. Top 10 Products by Revenue — Horizontal Bar Chart
# -------------------------------------------------------------------
def plot_top_products():
    df = pd.read_csv(os.path.join(CSV, "top_products.csv")) \
           .sort_values("TotalRevenue", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["Description"], df["TotalRevenue"], color="#9C27B0", edgecolor="white")
    ax.set_title("Top 10 Products by Revenue")
    ax.set_xlabel("Total Revenue (£)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}"))
    for bar in bars:
        w = bar.get_width()
        ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                f"£{w:,.0f}", va="center", fontsize=8.5)
    savefig("4_top_products.png")


# -------------------------------------------------------------------
# 5. Revenue by Country — Horizontal Bar Chart (Top 10)
# -------------------------------------------------------------------
def plot_country_revenue():
    df = pd.read_csv(os.path.join(CSV, "country_sales.csv")) \
           .head(10) \
           .sort_values("TotalRevenue", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["Country"], df["TotalRevenue"], color="#00BCD4", edgecolor="white")
    ax.set_title("Top 10 Countries by Revenue")
    ax.set_xlabel("Total Revenue (£)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}"))
    for bar in bars:
        w = bar.get_width()
        ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                f"£{w:,.0f}", va="center", fontsize=8.5)
    savefig("5_country_revenue.png")


# -------------------------------------------------------------------
# 6. Unique Customers per Country — Bar Chart (Top 10)
# -------------------------------------------------------------------
def plot_customer_count():
    df = pd.read_csv(os.path.join(CSV, "customer_count.csv")) \
           .head(10) \
           .sort_values("UniqueCustomers", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["Country"], df["UniqueCustomers"], color="#FF5722", edgecolor="white")
    ax.set_title("Unique Customers per Country (Top 10)")
    ax.set_xlabel("Number of Unique Customers")
    for bar in bars:
        w = bar.get_width()
        ax.text(w + 5, bar.get_y() + bar.get_height() / 2,
                str(int(w)), va="center", fontsize=8.5)
    savefig("6_customer_count.png")


# -------------------------------------------------------------------
# 7. Top 10 Customers by Revenue — Bar Chart
# -------------------------------------------------------------------
def plot_top_customers():
    df = pd.read_csv(os.path.join(CSV, "top_customers.csv")) \
           .sort_values("TotalRevenue", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df["CustomerID"].astype(str), df["TotalRevenue"],
                   color="#3F51B5", edgecolor="white")
    ax.set_title("Top 10 Customers by Revenue")
    ax.set_xlabel("Total Revenue (£)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}"))
    for bar in bars:
        w = bar.get_width()
        ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                f"£{w:,.0f}", va="center", fontsize=8.5)
    savefig("7_top_customers.png")


# -------------------------------------------------------------------
# Run all plots
# -------------------------------------------------------------------
if __name__ == "__main__":
    print("Generating visualizations...\n")
    plot_monthly_revenue()
    plot_avg_order_value()
    plot_mom_growth()
    plot_top_products()
    plot_country_revenue()
    plot_customer_count()
    plot_top_customers()
    print(f"\nAll plots saved to: {PLOTS}")
