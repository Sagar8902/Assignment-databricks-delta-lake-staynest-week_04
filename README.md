# Assignment-databricks-delta-lake-staynest-week_04

# StayNest – Delta Lake & Lakehouse Engineering

**Codebasics | Databricks | Session 7 Assignment**

## 📌 Project Overview

This project is part of the Codebasics Databricks learning program. It focuses on Delta Lake and Lakehouse Engineering using Apache Spark and Databricks.

The assignment uses the StayNest booking dataset to practice Spark joins, Delta Lake table operations, time travel, data optimization, Medallion Architecture, and incremental data loading with MERGE.

## 🎯 Objectives

* Understand Spark join strategies and broadcast joins.
* Create and manage Delta tables.
* Perform UPDATE and DELETE operations.
* Explore Delta Lake history, time travel, and RESTORE.
* Optimize Delta tables using OPTIMIZE and Z-Ordering.
* Implement Bronze, Silver, and Gold data layers.
* Perform incremental data loading using MERGE.

## 🛠️ Tech Stack

| Technology   | Purpose                                |
| ------------ | -------------------------------------- |
| Python       | Data processing                        |
| Apache Spark | Distributed data processing            |
| PySpark      | Spark DataFrame operations             |
| Databricks   | Notebook development and execution     |
| Delta Lake   | ACID transactions and data management  |
| SQL          | Data manipulation and MERGE operations |

## 📂 Assignment Tasks

### Q1. Spark Joins & Broadcast Join

* Perform a standard join between bookings and hotels.
* Analyze the physical execution plan using `.explain()`.
* Apply a broadcast join and compare execution plans.
* Understand how broadcast joins can reduce shuffle for suitable workloads.

### Q2. Delta Table Operations

* Create a managed Delta table using `saveAsTable()`.
* Update pending bookings to completed.
* Delete cancelled bookings.
* Review table changes using `DESCRIBE HISTORY`.

### Q3. Time Travel & RESTORE

* Read the Delta table at Version 0.
* Compare historical and current row counts.
* Restore the table to Version 0.
* Verify the restored data and inspect the Delta history.

### Q4. OPTIMIZE & Z-Ordering

* Run `OPTIMIZE` to compact small files.
* Apply `ZORDER BY (city)`.
* Understand how file compaction and data skipping can improve query performance.

### Q5. Bronze Layer – Raw Data Ingestion

* Load all 12,000 raw booking records.
* Add an `ingested_at` timestamp.
* Store the raw data in the `bronze_bookings` Delta table.

### Q6. Silver Layer – Data Transformation

* Read booking data from the Bronze layer.
* Keep only completed bookings.
* Join hotel details to enrich the booking data.
* Avoid duplicate city columns.
* Store the transformed data in `silver_bookings`.

### Q7. Gold Layer – City Revenue Analysis

* Aggregate bookings by city.
* Calculate total bookings and revenue per city.
* Sort results by revenue in descending order.
* Store the final aggregated data in `gold_city_revenue`.

### Q8. Incremental Load Using MERGE

* Read today's incremental updates from the provided CSV file.
* Update 150 existing bookings using matching `booking_id` values.
* Insert 50 new bookings.
* Perform both operations using a single Delta MERGE statement.
* Compare row counts before and after MERGE.

**Expected result:** The target table grows by 50 rows, assuming the source contains 150 distinct matched IDs and 50 distinct new IDs.

## 🏗️ Medallion Architecture

The assignment uses the Medallion Architecture to organize data into three layers.

```text
                 StayNest Raw Data
                        |
                        v
              +-------------------+
              |   Bronze Layer    |
              |  bronze_bookings  |
              |  Raw booking data |
              +-------------------+
                        |
                        v
              +-------------------+
              |    Silver Layer  |
              |  silver_bookings |
              | Cleaned & enriched|
              +-------------------+
                        |
                        v
              +-------------------+
              |     Gold Layer    |
              | gold_city_revenue |
              | City-level metrics|
              +-------------------+
```

## 📁 Repository Structure

```text
codebasics-databricks-delta-lake-staynest/
│
├── notebooks/
│   └── staynest_delta_lake_assignment
│
├── data/
│   └── README.md
│
└── README.md
```

*Note: Update the structure to match the files and notebooks included in your repository. The source CSV is stored in the Databricks Volume.*

## 📊 Key Learning Outcomes

By completing this assignment, I practiced:

* Understanding Spark physical execution plans.
* Working with managed Delta tables.
* Using Delta Lake time travel and RESTORE.
* Optimizing Delta tables with file compaction and Z-Ordering.
* Building Bronze, Silver, and Gold data pipelines.
* Implementing incremental upserts with Delta MERGE.
* Validating data using row counts and table history.

## ▶️ How to Run

1. Open the notebook in your Databricks workspace.

2. Ensure the required StayNest datasets are available.

3. Verify the source CSV path used in the notebook:

   `/Volumes/workspace/default/staynest_02/bookings_updates.csv`

4. Run the notebook cells in sequence.

5. Review the output, row counts, execution plans, and Delta table history.

## 👨‍💻 Author

**Sagar Soni**
Data Engineer | Data Analytics | Databricks | PySpark | SQL

---

*This repository is created for learning and assignment submission as part of the Codebasics Databricks program.*
