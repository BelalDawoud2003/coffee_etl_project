# ☕ Coffee ETL Project

**A Bash-based ETL pipeline for a fictional coffee shop chain, BrewTopia.**  
This project consolidates sales and inventory data from multiple sources, transforms it, and generates business insights.

## 📌 Project Overview

This ETL project extracts data from **JSON files**, **CSV files**, and a **MySQL database**.  
It then **transforms** the data (standardizes columns, calculates totals, filters invalid records), **loads** it into a unified dataset, and **generates reports**.

## 🛠 Technologies Used

- **Bash scripting** – main ETL workflow  
- **jq** – JSON processing  
- **MySQL** – data storage & extraction  
- **CSV files** – for raw and processed data  
- **tar** – archiving processed files  
- Logging & reporting – daily summaries

## 📂 Project Structure


coffee_etl_project/
├── config/       # Configuration files
│   └── config.env
├── data/         # Raw data files
│   ├── online_orders.json
│   └── instore_sales.csv
├── sql/          # SQL scripts
│   └── init_coffee_db.sql
├── scripts/      # ETL Bash scripts
│   └── coffee_etl.sh
├── logs/         # Pipeline logs
├── processed/    # Transformed & merged CSVs
└── reports/      # Summary reports


## ⚙️ ETL Pipeline Steps

1. **Extraction**: JSON orders, CSV sales, MySQL inventory  
2. **Transformation**: Standardize columns, calculate totals, filter invalid records, add source identifiers  
3. **Loading & Merging**: Merge all processed CSVs into `final_output.csv` with sequential `record_id`  
4. **Reporting & Archiving**: Generate daily summary reports, archive CSVs, cleanup logs

## 📊 Example Insights

- Total revenue by category  
- Most popular products  
- Sales by store location  
- Inventory items below minimum stock

## 🚀 How to Run

1. Install dependencies: `jq`, `mysql-client`, `tar`  
2. Set database credentials in `config/config.env`  
3. Run ETL:
```bash
cd scripts
./coffee_etl.sh

