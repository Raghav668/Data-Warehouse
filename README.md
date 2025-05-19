
# Data Warehouse Project

This repository contains the complete ETL pipeline and data modeling layers for a Data Warehouse project, structured using the Medallion Architecture: **Bronze**, **Silver**, and **Gold** layers. The final layer supports analytics, reporting, and dashboarding.

## Project Overview

The objective is to build a scalable and maintainable data warehouse using SQL-based transformations. The system follows the **Star Schema** design pattern in the Gold layer, featuring clean and enriched dimension and fact tables for business consumption.

## Layered Architecture

### 1. 🟤 Bronze Layer – Raw Data Ingestion
- **Purpose:** Store raw, untransformed data as ingested from source systems (CRM, ERP).
- **Examples:**
  - `bronze.crm_cust_info`
  - `bronze.erp_prd_info`
  - `bronze.cust_sales_details`
- **Transformations:** None (raw landing tables)


### 2. 🥈 Silver Layer – Cleansed and Modeled Data
- **Purpose:** Clean and join raw data, handle missing values, remove duplicates, and prepare for business logic.
- **Examples:**
  - `silver.crm_cust_info`
  - `silver.erp_cust_az12`
  - `silver.crm_sales_details`
  - `silver.erp_loc_a101`
  - `silver.crm_prd_info`
  - `silver.erp_px_cat_g1v2`
- **Transformations:**
  - Filtering null/invalid records
  - Standardizing formats


### 3. 🥇 Gold Layer – Business-ready Views (Star Schema)
- **Purpose:** Provide enriched and analytics-ready dimension and fact views for dashboards, KPIs, and data analysis.
- **Views Created:**
  - `gold.dim_customers`
  - `gold.dim_products`
  - `gold.fact_sales`
- **Highlights:**
  - Uses surrogate keys via `ROW_NUMBER()` for dimension tables.
  - Cleans and enriches gender, birthdate, and other fields.
  - Builds a fact table by joining dimension views.
  - Filters only active products (`prd_end_dt IS NULL`).

## 🛠️ Usage

These views can be queried directly by BI tools (e.g., Power BI, Tableau) or analysts for reporting and decision-making. They follow the Star Schema structure to optimize performance and query readability.


