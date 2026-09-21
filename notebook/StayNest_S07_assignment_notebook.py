# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # StayNest - Session 7 Assignment (Delta Lake & Lakehouse)
# MAGIC Work through the 8 tasks in order. Read the Assignment Questions PDF for the full
# MAGIC detail and acceptance criteria. Fill each `# TODO` cell, run it, and keep the output
# MAGIC visible. Runs on Databricks Free Edition (serverless).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 0 - Setup (already done for you)
# MAGIC Upload `bookings.csv`, `hotels.csv`, `bookings_updates.csv` to a Volume, set `BASE`,
# MAGIC `CATALOG`, `SCHEMA`, and run this cell. Expect 12000 / 200 / 200.

# COMMAND ----------

BASE    = "/Volumes/workspace/default/staynest_02"
CATALOG = "workspace"
SCHEMA  = "default"
FQN = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

read_csv = lambda name: (spark.read
    .option("header", True).option("inferSchema", True)
    .csv(f"{BASE}/{name}.csv"))

bookings_df = read_csv("bookings")
hotels_df   = read_csv("hotels")
updates_df  = read_csv("bookings_updates")

print(f"bookings: {bookings_df.count()}, hotels: {hotels_df.count()}, "
      f"updates: {updates_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Read the plan and force a broadcast join
# MAGIC Join bookings to hotels and call `.explain()` to see the plan. Then force a
# MAGIC broadcast join with `broadcast(hotels_df)` and `.explain()` again. In a comment,
# MAGIC say which join each plan used and why broadcast avoids a shuffle.
# MAGIC (Tip: hotels also has a `city` column, so `hotels_df.drop("city")` before joining.)

# COMMAND ----------

# TODO

# Question 1: Read the plan and force a broadcast join

from pyspark.sql.functions import broadcast

# --------------------------------------------------
# 1. Normal join - let Catalyst choose the join strategy
# --------------------------------------------------

normal_join_df = bookings_df.join(
    hotels_df,
    on="hotel_id",
    how="inner"
)

print("Normal Join - Catalyst Optimized Execution Plan")
normal_join_df.explain()

# --------------------------------------------------
# 2. Force a broadcast join on hotels_df
# --------------------------------------------------

broadcast_join_df = bookings_df.join(
    broadcast(hotels_df),
    on="hotel_id",
    how="inner"
)

print("Forced Broadcast Join - Execution Plan")
broadcast_join_df.explain()

# --------------------------------------------------
# Explanation:
# Both the normal and forced joins use PhotonBroadcastHashJoin.
# Catalyst automatically chose broadcast for the normal join,
# likely because hotels_df is small enough to broadcast.
# The forced join confirms the same strategy explicitly.
# Broadcast distributes the small hotels_df to workers, avoiding
# the need to shuffle the large bookings_df by hotel_id.
# --------------------------------------------------)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Create a Delta table, then read its history
# MAGIC Write `bookings_df` as a managed Delta table with `saveAsTable`. Then create some
# MAGIC history: run an `UPDATE` (set pending to completed) and a `DELETE` (remove
# MAGIC cancelled). Show `DESCRIBE HISTORY` and point out the versioned commits.

# COMMAND ----------

# TODO

# Question 2: Create a managed Delta table

# DROP TABLE IF EXISTS staynest_bookings_delta;

table_name = "staynest_bookings_delta"

# Write bookings_df as a managed Delta table
bookings_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

# Verify the table was created
display(spark.table(table_name))



# Update pending bookings to completed

spark.sql("""
    UPDATE staynest_bookings_delta
    SET status = 'completed'
    WHERE status = 'pending'
""")

# Verify the update
display(
    spark.sql("""
        SELECT status, COUNT(*) AS total_bookings
        FROM staynest_bookings_delta
        GROUP BY status
    """)
)



# Delete cancelled bookings

spark.sql("""
    DELETE FROM staynest_bookings_delta
    WHERE status = 'cancelled'
""")

# Verify the cancelled bookings are removed
display(
    spark.sql("""
        SELECT status, COUNT(*) AS total_bookings
        FROM staynest_bookings_delta
        GROUP BY status
    """)
)



# Show Delta table history

display(
    spark.sql("""
        DESCRIBE HISTORY staynest_bookings_delta
    """)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Time travel and RESTORE
# MAGIC Read the table as it was at **version 0** (before your UPDATE and DELETE) and show
# MAGIC its count. Then `RESTORE` the table to version 0 and confirm the count is back.
# MAGIC Show that RESTORE appears as a new commit in the history.

# COMMAND ----------

# TODO

# ============================================================
# Question 3: Delta Lake Time Travel and RESTORE
# ============================================================

table_name = "staynest_bookings_delta"

# ------------------------------------------------------------
# 1. Read the table as it existed at Version 0 (Time Travel)
# ------------------------------------------------------------

version_0_df = spark.sql(f"""
    SELECT *
    FROM {table_name} VERSION AS OF 0
""")

version_0_count = version_0_df.count()

print("Version 0 row count:", version_0_count)


# ------------------------------------------------------------
# 2. Get the current row count before RESTORE
# ------------------------------------------------------------

current_count_before = spark.table(table_name).count()

print("Current row count before RESTORE:", current_count_before)

# Confirm Version 0 and current counts are different
print(
    "Counts are different:",
    version_0_count != current_count_before
)


# ------------------------------------------------------------
# 3. Restore the Delta table to Version 0
# ------------------------------------------------------------

spark.sql(f"""
    RESTORE TABLE {table_name}
    TO VERSION AS OF 0
""")

print("Table restored to Version 0")


# ------------------------------------------------------------
# 4. Verify the row count after RESTORE
# ------------------------------------------------------------

restored_count = spark.table(table_name).count()

print("Version 0 row count:", version_0_count)
print("Row count after RESTORE:", restored_count)

print(
    "RESTORE successful:",
    restored_count == version_0_count
)


# ------------------------------------------------------------
# 5. Display Delta table history
# ------------------------------------------------------------

display(
    spark.sql(f"""
        DESCRIBE HISTORY {table_name}
    """)
)


# ------------------------------------------------------------
# Explanation:
# Time Travel reads an older Delta table version without
# changing the current table. RESTORE makes Version 0 the
# current state and creates a new commit in Delta history.
# ------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - OPTIMIZE and ZORDER
# MAGIC Run `OPTIMIZE` on your Delta table to compact files. Then run
# MAGIC `OPTIMIZE ... ZORDER BY (city)`. In a comment, say what OPTIMIZE does and why
# MAGIC `city` is a good ZORDER column but `status` would not be.

# COMMAND ----------

# TODO


# ============================================================
# Question 4: OPTIMIZE and ZORDER
# ============================================================

table_name = "staynest_bookings_delta"

# ------------------------------------------------------------
# 1. OPTIMIZE - Compact small Delta files
# ------------------------------------------------------------

spark.sql(f"""
    OPTIMIZE {table_name}
""")

print("OPTIMIZE completed successfully.")


# ------------------------------------------------------------
# 2. OPTIMIZE with ZORDER BY city
# ------------------------------------------------------------

spark.sql(f"""
    OPTIMIZE {table_name}
    ZORDER BY (city)
""")

print("OPTIMIZE with ZORDER BY (city) completed successfully.")


# ------------------------------------------------------------
# Explanation:
# OPTIMIZE compacts small Delta files into fewer, larger files.
# ZORDER BY (city) colocates related city data to improve data
# skipping for city-based filters. Status has low cardinality,
# so it is generally less useful for ZORDER in this dataset.
# ------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Bronze: land the raw data
# MAGIC Write the raw bookings to a `bronze_bookings` Delta table, keeping every row and
# MAGIC adding an `ingested_at` timestamp column.

# COMMAND ----------

# TODO


# ============================================================
# Question 5: Bronze Layer - Create bronze_bookings
# ============================================================

from pyspark.sql.functions import current_timestamp, col

# ------------------------------------------------------------
# 1. Add ingestion timestamp and keep all raw booking rows
# ------------------------------------------------------------

bronze_df = bookings_df.withColumn(
    "ingested_at",
    current_timestamp()
)

# ------------------------------------------------------------
# 2. Write data as a managed Delta table
# ------------------------------------------------------------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_bookings")

print("Bronze Delta table created successfully!")


# ------------------------------------------------------------
# 3. Verify row count
# ------------------------------------------------------------

bronze_bookings_df = spark.table("bronze_bookings")

print("Raw bookings count:", bookings_df.count())
print("Bronze bookings count:", bronze_bookings_df.count())


# ------------------------------------------------------------
# 4. Verify ingestion timestamp column and data
# ------------------------------------------------------------

bronze_bookings_df.printSchema()

display(
    bronze_bookings_df.select(
        "booking_id",
        "hotel_id",
        "status",
        "ingested_at"
    ).limit(10)
)


# ------------------------------------------------------------
# Explanation:
# The Bronze layer preserves all raw booking records and adds
# ingested_at to capture the ingestion timestamp. The data is
# stored as a managed Delta table for downstream processing.
# ------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Silver: clean and conform
# MAGIC Build `silver_bookings` from bronze: keep only completed bookings and join the
# MAGIC hotel dimension to add `category`, `star_rating`, and the hotel name. Drop the
# MAGIC duplicate `city` from the hotel side so the join has a single `city`.

# COMMAND ----------

# TODO


# ============================================================
# Question 6: Silver Layer - Create silver_bookings
# ============================================================

from pyspark.sql.functions import col

# ------------------------------------------------------------
# 1. Read Bronze table and keep completed bookings only
# ------------------------------------------------------------

bronze_df = spark.table("bronze_bookings")

completed_df = bronze_df.filter(
    col("status") == "completed"
)


# ------------------------------------------------------------
# 2. Select required hotel attributes
#    Exclude city to avoid duplicate city columns
# ------------------------------------------------------------

hotel_dim_df = hotels_df.select(
    "hotel_id",
    "hotel_name",
    "category",
    "star_rating"
)


# ------------------------------------------------------------
# 3. Join completed bookings with hotel dimension
# ------------------------------------------------------------

silver_df = completed_df.join(
    hotel_dim_df,
    on="hotel_id",
    how="inner"
)


# ------------------------------------------------------------
# 4. Write the Silver Delta table
# ------------------------------------------------------------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_bookings")

print("Silver Delta table created successfully!")


# ------------------------------------------------------------
# 5. Verify the Silver table
# ------------------------------------------------------------

silver_bookings_df = spark.table("silver_bookings")

print("Completed Bronze bookings (status = completed):", completed_df.count())
print("Silver bookings count:", silver_bookings_df.count())

# Verify the schema - should contain only one city column
silver_bookings_df.printSchema()

# Verify sample records
display(silver_bookings_df.limit(10))


# ------------------------------------------------------------
# Explanation:
# The Silver layer contains only completed bookings enriched
# with hotel attributes. The hotel-side city is excluded
# during selection, preserving a single city column.
# ------------------------------------------------------------


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Gold: business-ready aggregate
# MAGIC From silver, build a `gold_city_revenue` Delta table: bookings and total revenue
# MAGIC per city, ordered by revenue.

# COMMAND ----------

# TODO



# ============================================================
# Question 7: Gold Layer - City Revenue Analysis
# ============================================================

from pyspark.sql.functions import count, sum, col

# ------------------------------------------------------------
# 1. Read the Silver Delta table
# ------------------------------------------------------------

silver_df = spark.table("silver_bookings")


# ------------------------------------------------------------
# 2. Group by city and calculate bookings and revenue
# ------------------------------------------------------------

gold_df = silver_df.groupBy("city").agg(
    count("booking_id").alias("bookings"),
    sum("amount").alias("total_revenue")
)


# ------------------------------------------------------------
# 3. Sort by total revenue (highest to lowest)
# ------------------------------------------------------------

gold_df = gold_df.orderBy(
    col("total_revenue").desc()
)


# ------------------------------------------------------------
# 4. Save as a managed Gold Delta table
# ------------------------------------------------------------

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_city_revenue")

print("Gold Delta table created successfully!")


# ------------------------------------------------------------
# 5. Verify the Gold table (sorted by revenue descending)
# ------------------------------------------------------------

gold_city_revenue_df = spark.table("gold_city_revenue")

display(
    gold_city_revenue_df.orderBy(
        col("total_revenue").desc()
    )
)


# ------------------------------------------------------------
# Explanation:
# The Gold layer groups completed Silver bookings by city,
# counts bookings, and sums amount to calculate total revenue.
# Results are sorted by total_revenue in descending order.
# ------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - Incremental load with MERGE
# MAGIC You have today's batch in `updates_df` (150 changed bookings + 50 new ones).
# MAGIC `MERGE` it into your Delta table: update matched booking_ids, insert new ones, in
# MAGIC one command. Report the row count before and after (it should grow by the 50 new).

# COMMAND ----------

# TODO

# 1. Read today's updates
updates_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/staynest_02/bookings_updates.csv")
)

# 2. Create a temporary view for MERGE
updates_df.createOrReplaceTempView("updates_today")

# 3. Count rows before MERGE
before = spark.table("staynest_bookings_delta").count()
print(f"Before MERGE: {before:,}")

# 4. MERGE — update existing bookings, insert new bookings
spark.sql("""
    MERGE INTO staynest_bookings_delta AS target
    USING updates_today AS source
        ON target.booking_id = source.booking_id

    WHEN MATCHED     THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ MERGE complete — 150 updates + 50 inserts")

# 5. Count rows after MERGE
after = spark.table("staynest_bookings_delta").count()

print(f"Before MERGE: {before:,}")
print(f"After MERGE : {after:,}")
print(f"New rows    : {after - before:,}")
