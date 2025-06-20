# Databricks notebook source
# MAGIC %md
# MAGIC # Cafe Rewards Data Pipeline - PySpark Only Solution
# MAGIC
# MAGIC **Data Engineering Task - Databricks Pipeline**
# MAGIC
# MAGIC This notebook implements a complete data pipeline with Bronze, Silver, and Gold layers using PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
import json
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

print("Libraries imported successfully!")

# COMMAND ----------

# Configuration settings
class Config:
    """Configuration settings for the pipeline"""
    
    # Update these paths to match your Databricks environment
    DATA_PATH = "./data/land_zone/"  # Default DBFS path for uploaded files
    CUSTOMERS_FILE = "customers.csv"
    OFFERS_FILE = "offers.csv"
    EVENTS_FILE = "events.csv"
    DATA_DICT_FILE = "data_dictionary.csv"
    
    # Database schemas
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    
    @staticmethod
    def get_file_path(filename):
        return f"{Config.DATA_PATH}/{filename}"

# Create database schemas
for schema in [Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA, Config.GOLD_SCHEMA]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    print(f"Created schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Schema Definitions

# COMMAND ----------

# Define schemas for each layer
class Schemas:
    """Schema definitions for all data layers"""
    
    # Bronze Layer Schemas (Raw)
    BRONZE = {
        "customers": StructType([
            StructField("customer_id", StringType(), False),
            StructField("became_member_on", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("income", DoubleType(), True)
        ]),
        
        "offers": StructType([
            StructField("offer_id", StringType(), False),
            StructField("offer_type", StringType(), False),
            StructField("difficulty", IntegerType(), False),
            StructField("reward", IntegerType(), False),
            StructField("duration", IntegerType(), False),
            StructField("channels", StringType(), False)
        ]),
        
        "events": StructType([
            StructField("customer_id", StringType(), False),
            StructField("event", StringType(), False),
            StructField("value", StringType(), True),
            StructField("time", IntegerType(), False)
        ])
    }
    
    # Silver Layer Schemas (Cleansed and Enriched)
    SILVER = {
        "customers_cleansed": StructType([
            StructField("customer_id", StringType(), False),
            StructField("membership_date", DateType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("age_group", StringType(), True),
            StructField("income", DoubleType(), True),
            StructField("income_bracket", StringType(), True),
            StructField("membership_duration_days", IntegerType(), True),
            StructField("data_quality_flag", StringType(), True)
        ]),
        
        "offers_enriched": StructType([
            StructField("offer_id", StringType(), False),
            StructField("offer_type", StringType(), False),
            StructField("difficulty", IntegerType(), False),
            StructField("reward", IntegerType(), False),
            StructField("duration", IntegerType(), False),
            StructField("duration_hours", IntegerType(), False),
            StructField("channels_list", ArrayType(StringType()), False),
            StructField("num_channels", IntegerType(), False),
            StructField("is_multichannel", BooleanType(), False),
            StructField("roi_ratio", DoubleType(), True)
        ]),
        
        "events_processed": StructType([
            StructField("event_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("offer_id", StringType(), True),
            StructField("transaction_amount", DoubleType(), True),
            StructField("reward_amount", DoubleType(), True),
            StructField("event_time_hours", IntegerType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("event_date", DateType(), False)
        ]),
        
        "offer_interactions": StructType([
            StructField("customer_id", StringType(), False),
            StructField("offer_id", StringType(), False),
            StructField("received_time", IntegerType(), True),
            StructField("viewed_time", IntegerType(), True),
            StructField("completed_time", IntegerType(), True),
            StructField("is_received", BooleanType(), False),
            StructField("is_viewed", BooleanType(), False),
            StructField("is_completed", BooleanType(), False),
            StructField("time_to_view", IntegerType(), True),
            StructField("time_to_complete", IntegerType(), True),
            StructField("completion_within_duration", BooleanType(), True)
        ])
    }
    
    # Gold Layer Schemas (Business Metrics)
    GOLD = {
        "channel_effectiveness": StructType([
            StructField("channel", StringType(), False),
            StructField("total_offers_sent", IntegerType(), False),
            StructField("offers_completed", IntegerType(), False),
            StructField("completion_rate", DoubleType(), False),
            StructField("avg_completion_time_hours", DoubleType(), True),
            StructField("total_revenue_generated", DoubleType(), True),
            StructField("avg_revenue_per_offer", DoubleType(), True),
            StructField("effectiveness_rank", IntegerType(), False)
        ]),
        
        "age_distribution_analysis": StructType([
            StructField("age_group", StringType(), False),
            StructField("completion_status", StringType(), False),
            StructField("customer_count", IntegerType(), False),
            StructField("percentage", DoubleType(), False),
            StructField("avg_income", DoubleType(), True),
            StructField("avg_offers_received", DoubleType(), True)
        ]),
        
        "offer_completion_metrics": StructType([
            StructField("offer_type", StringType(), False),
            StructField("channel", StringType(), False),  # ADDED
            StructField("avg_completion_time_hours", DoubleType(), False),
            StructField("median_completion_time_hours", DoubleType(), False),
            StructField("min_completion_time_hours", DoubleType(), False),
            StructField("max_completion_time_hours", DoubleType(), False),
            StructField("std_dev_hours", DoubleType(), True),
            StructField("total_completions", LongType(), False),  # IntegerType → LongType
            StructField("total_received", LongType(), False),  # ADDED
            StructField("completion_rate", DoubleType(), False),
            StructField("unique_customers", LongType(), False),  # ADDED
            StructField("total_customers_targeted", LongType(), False)  # 
        ])
    }

print("All schemas defined successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer - Raw Data Ingestion

# COMMAND ----------

def load_raw_data():
    """
    Load raw CSV data into Bronze layer Delta tables
    """
    print("Starting Bronze layer data ingestion...")
    
    # Load Customers data
    customers_pdf = pd.read_csv(Config.get_file_path(Config.CUSTOMERS_FILE))
    customers_df = spark.createDataFrame(customers_pdf,Schemas.BRONZE["customers"])
    customers_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.BRONZE_SCHEMA}.customers")
    
    print(f"Loaded {customers_df.count()} customer records")
    
    # Load Offers data
    offers_pdf = pd.read_csv(Config.get_file_path(Config.OFFERS_FILE))
    offers_pdf['difficulty'] = offers_pdf['difficulty'].astype(str) 
    offers_df = spark.createDataFrame(offers_pdf)
    offers_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.BRONZE_SCHEMA}.offers")
    
    print(f"Loaded {offers_df.count()} offer records")
    
    # Load Events data
    events_pdf = pd.read_csv(Config.get_file_path(Config.EVENTS_FILE))
    events_df = spark.createDataFrame(events_pdf)
    events_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.BRONZE_SCHEMA}.events")
    
    print(f"Loaded {events_df.count()} event records")
    
    print("Bronze layer ingestion completed successfully!")
    
    return customers_df, offers_df, events_df

# Execute data loading
customers_raw, offers_raw, events_raw = load_raw_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Layer - Data Cleansing and Enrichment

# COMMAND ----------

def create_silver_customers():
    """
    Clean and enrich customer data for Silver layer
    """
    print("Processing customers for Silver layer...")
    
    # Read from Bronze
    customers_bronze = spark.table(f"{Config.BRONZE_SCHEMA}.customers")
    
    # Data cleansing and enrichment
    customers_silver = customers_bronze \
        .withColumn("membership_date", 
                   to_date(col("became_member_on").cast("string"), "yyyyMMdd")) \
        .withColumn("age_group", 
                   when(col("age") < 25, "18-24")
                   .when(col("age") < 35, "25-34")
                   .when(col("age") < 45, "35-44")
                   .when(col("age") < 55, "45-54")
                   .when(col("age") < 65, "55-64")
                   .otherwise("65+")) \
        .withColumn("income_bracket",
                   when(col("income") < 40000, "Low")
                   .when(col("income") < 80000, "Medium")
                   .when(col("income") < 120000, "High")
                   .otherwise("Very High")) \
        .withColumn("membership_duration_days",
                   datediff(current_date(), col("membership_date"))) \
        .withColumn("data_quality_flag",
                   when(col("age").isNull() | col("income").isNull() | 
                        col("gender").isNull(), "incomplete")
                   .when(col("age") < 18, "invalid_age")
                   .when(col("age") > 101, "invalid_age")
                   .when(col("income") < 0, "invalid_income")
                   .otherwise("valid")) \
        .select(
            "customer_id",
            "membership_date",
            "gender",
            "age",
            "age_group",
            "income",
            "income_bracket",
            "membership_duration_days",
            "data_quality_flag"
        )
    
    # Write to Silver layer
    customers_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.SILVER_SCHEMA}.customers_cleansed")
    
    print(f"Created Silver customers table with {customers_silver.count()} records")
    print(f"Data quality issues found: {customers_silver.filter(col('data_quality_flag') != 'valid').count()} records")
    
    return customers_silver

# Execute customer processing
customers_silver = create_silver_customers()

# COMMAND ----------

def create_silver_offers():
    """
    Clean and enrich offer data for Silver layer
    """
    print("Processing offers for Silver layer...")
    
    # Read from Bronze
    offers_bronze = spark.table(f"{Config.BRONZE_SCHEMA}.offers")
    
    # Debug: Check the channels column format
    print("Sample channels data:")
    offers_bronze.select("offer_id", "channels").show(5, truncate=False)
    
    # Process channels and add enrichments
    # The channels column contains values like ['web', 'email', 'mobile']
    offers_silver = offers_bronze \
        .withColumn("channels_clean", regexp_replace(col("channels"), "[\\[\\]']", "")) \
        .withColumn("channels_clean", regexp_replace(col("channels_clean"), '[""]', "")) \
        .withColumn("channels_list", split(col("channels_clean"), ", ")) \
        .withColumn("channels_list", 
                   expr("transform(channels_list, x -> trim(x))")) \
        .withColumn("num_channels", size(col("channels_list"))) \
        .withColumn("is_multichannel", col("num_channels") > 1) \
        .withColumn("duration_hours", col("duration") * 24) \
        .withColumn("roi_ratio", 
                   when(col("difficulty") > 0, col("reward") / col("difficulty"))
                   .otherwise(lit(None))) \
        .select(
            "offer_id",
            "offer_type",
            "difficulty",
            "reward",
            "duration",
            "duration_hours",
            "channels_list",
            "num_channels",
            "is_multichannel",
            "roi_ratio"
        )
    
    # Debug: Check the processed channels
    print("Sample processed channels:")
    offers_silver.select("offer_id", "channels_list").show(5, truncate=False)
    
    # Write to Silver layer
    offers_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.SILVER_SCHEMA}.offers_enriched")
    
    print(f"Created Silver offers table with {offers_silver.count()} records")
    
    return offers_silver

# Execute offer processing
offers_silver = create_silver_offers()

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, when, regexp_extract, lit, expr, to_date

def create_silver_events():
    """
    Process and parse event data for Silver layer
    """
    print("Processing events for Silver layer...")
    
    # Read from Bronze
    events_bronze = spark.table(f"{Config.BRONZE_SCHEMA}.events")
    
    # Parse the value column and extract relevant fields
    events_silver = events_bronze \
        .withColumn("event_id", concat_ws("_", col("customer_id"), col("event"), col("time"))) \
        .withColumn("event_type", col("event")) \
        .withColumn("offer_id", 
                   when(col("event").isin(["offer received", "offer viewed"]),
                        regexp_extract(col("value"), r"'offer id': '([^']+)'", 1))
                    .when(col("event").isin(["offer completed"]),
                        regexp_extract(col("value"), r"'offer_id': '([^']+)'", 1))
                   .otherwise(lit(None))) \
        .withColumn("transaction_amount",
                   when((col("event") == "transaction") & (regexp_extract(col("value"), r'"amount": ([0-9.]+)', 1) != ''),
                        regexp_extract(col("value"), r'"amount": ([0-9.]+)', 1).cast("double"))
                   .otherwise(lit(None))) \
        .withColumn("reward_amount",
                   when((col("event") == "offer completed") & (regexp_extract(col("value"), r'"amount": ([0-9.]+)', 1) != ''),
                        regexp_extract(col("value"), r'"reward": ([0-9.]+)', 1).cast("double"))
                   .otherwise(lit(None))) \
        .withColumn("event_timestamp", 
                   expr("make_timestamp(2018, 1, 1, 0, 0, 0) + INTERVAL 1 HOUR")) \
        .withColumn("event_date", to_date(col("event_timestamp"))) \
        .select(
            "event_id",
            "customer_id",
            "event_type",
            "offer_id",
            "transaction_amount",
            "reward_amount",
            col("time").alias("event_time_hours"),
            "event_timestamp",
            "event_date"
        )
    
    # Write to Silver layer
    events_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.SILVER_SCHEMA}.events_processed")
    
    print(f"Created Silver events table with {events_silver.count()} records")
    
    return events_silver

# Execute event processing
events_silver = create_silver_events()

# COMMAND ----------

def create_offer_interactions():
    """
    Create offer interactions table in Silver layer
    This table tracks the customer journey for each offer
    """
    print("Creating offer interactions table...")
    
    # Read events from Silver layer
    events = spark.table(f"{Config.SILVER_SCHEMA}.events_processed")
    offers = spark.table(f"{Config.SILVER_SCHEMA}.offers_enriched")
    
    # Filter offer-related events
    offer_events = events.filter(col("offer_id").isNotNull())
    
    # Pivot events to get interaction timeline
    interactions_pivot = offer_events \
        .groupBy("customer_id", "offer_id") \
        .pivot("event_type", ["offer received", "offer viewed", "offer completed"]) \
        .agg(min("event_time_hours"))
    
    # Rename columns and calculate metrics
    interactions = interactions_pivot \
        .withColumnRenamed("offer received", "received_time") \
        .withColumnRenamed("offer viewed", "viewed_time") \
        .withColumnRenamed("offer completed", "completed_time") \
        .withColumn("is_received", col("received_time").isNotNull()) \
        .withColumn("is_viewed", col("viewed_time").isNotNull()) \
        .withColumn("is_completed", col("completed_time").isNotNull()) \
        .withColumn("time_to_view", 
                   when(col("viewed_time").isNotNull() & col("received_time").isNotNull(),
                        col("viewed_time") - col("received_time"))
                   .otherwise(lit(None))) \
        .withColumn("time_to_complete",
                   when(col("completed_time").isNotNull() & col("received_time").isNotNull(),
                        col("completed_time") - col("received_time"))
                   .otherwise(lit(None)))
    
    # Join with offers to check if completion was within duration
    interactions_with_duration = interactions \
        .join(offers.select("offer_id", "duration_hours"), "offer_id", "left") \
        .withColumn("completion_within_duration",
                   when(col("is_completed") & (col("time_to_complete") <= col("duration_hours")), True)
                   .when(col("is_completed"), False)
                   .otherwise(lit(None))) \
        .drop("duration_hours")
    
    # Write to Silver layer
    interactions_with_duration.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.SILVER_SCHEMA}.offer_interactions")
    
    print(f"Created offer interactions table with {interactions_with_duration.count()} records")
    
    return interactions_with_duration

# Execute offer interactions processing
offer_interactions = create_offer_interactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Layer - Business Metrics and Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 1: Which marketing channel is the most effective in terms of offer completion rate?

# COMMAND ----------

def analyze_channel_effectiveness():
    """
    Analyze marketing channel effectiveness based on offer completion rates
    """
    print("Analyzing channel effectiveness...")
    
    # Read required tables
    offers = spark.table(f"{Config.SILVER_SCHEMA}.offers_enriched")
    interactions = spark.table(f"{Config.SILVER_SCHEMA}.offer_interactions")
    events = spark.table(f"{Config.SILVER_SCHEMA}.events_processed")
    
    # Debug: Check channels format
    print("Sample channels from offers_enriched:")
    offers.select("offer_id", "channels_list").show(5, truncate=False)
    
    # Explode channels to analyze each channel separately
    offers_by_channel = offers \
        .select("offer_id", "offer_type", "difficulty", "reward", 
                explode("channels_list").alias("channel")) \
        .filter(col("channel").isNotNull() & (col("channel") != "") & (length(col("channel")) > 0))
    
    # Debug: Check exploded channels
    print("\nSample exploded channels:")
    offers_by_channel.select("offer_id", "channel").distinct().show(10, truncate=False)
    
    # Join with interactions to get completion data
    channel_interactions = offers_by_channel \
        .alias("o") \
        .join(interactions.alias("i"), col("o.offer_id") == col("i.offer_id"), "inner") \
        .filter(col("i.is_received") == True) \
        .select(
            col("o.channel"),
            col("o.offer_id"),
            col("o.reward"),
            col("i.customer_id"),
            col("i.received_time"),
            col("i.completed_time"),
            col("i.is_completed"),
            col("i.time_to_complete")
        )
    
    # Calculate base metrics by channel
    channel_metrics = channel_interactions \
        .groupBy("channel") \
        .agg(
            count("*").alias("total_offers_sent"),
            sum(when(col("is_completed"), 1).otherwise(0)).alias("offers_completed"),
            avg(when(col("is_completed"), col("time_to_complete")).otherwise(None)).alias("avg_completion_time_hours")
        ) \
        .withColumn("completion_rate", 
                   round(col("offers_completed") / col("total_offers_sent") * 100, 2))
    
    # Calculate revenue more accurately
    # Get all completed offers
    completed_offers = channel_interactions \
        .filter(col("is_completed") == True) \
        .select("channel", "customer_id", "offer_id", "reward", "received_time", "completed_time")
    
    # Get all customer transactions
    customer_transactions = events \
        .filter(col("event_type") == "transaction") \
        .select("customer_id", "transaction_amount", "event_time_hours")
    
    # Join to find transactions during offer periods
    offer_period_transactions = completed_offers.alias("co") \
        .join(
            customer_transactions.alias("t"),
            (col("co.customer_id") == col("t.customer_id")) &
            (col("t.event_time_hours") >= col("co.received_time")) &
            (col("t.event_time_hours") <= col("co.completed_time")),
            "inner"
        ) \
        .select(
            col("co.channel"),
            col("co.offer_id"),
            col("co.customer_id"),
            col("t.transaction_amount")
        )
    
    # Aggregate revenue by channel
    channel_revenue = offer_period_transactions \
        .groupBy("channel") \
        .agg(
            sum("transaction_amount").alias("total_revenue_generated"),
            countDistinct("offer_id").alias("offers_with_revenue"),
            countDistinct("customer_id").alias("customers_with_revenue")
        )
    
    # Also calculate reward-based revenue (as fallback if no transactions found)
    reward_revenue = completed_offers \
        .groupBy("channel") \
        .agg(
            sum("reward").alias("total_rewards_earned")
        )
    
    # Combine all metrics
    channel_effectiveness = channel_metrics \
        .join(channel_revenue, "channel", "left") \
        .join(reward_revenue, "channel", "left") \
        .withColumn("total_revenue_generated", 
                   coalesce(col("total_revenue_generated"), col("total_rewards_earned"), lit(0))) \
        .withColumn("avg_revenue_per_offer",
                   when(col("offers_completed") > 0,
                        round(col("total_revenue_generated") / col("offers_completed"), 2))
                   .otherwise(lit(0))) \
        .withColumn("avg_completion_time_hours",
                   when(col("avg_completion_time_hours").isNotNull(),
                        round(col("avg_completion_time_hours"), 2))
                   .otherwise(lit(None))) \
        .withColumn("effectiveness_rank",
                   rank().over(Window.orderBy(col("completion_rate").desc())))
    
    # Final selection with all required columns
    channel_effectiveness_final = channel_effectiveness \
        .select(
            "channel",
            "total_offers_sent",
            "offers_completed",
            "completion_rate",
            "avg_completion_time_hours",
            round(col("total_revenue_generated"), 2).alias("total_revenue_generated"),
            "avg_revenue_per_offer",
            "effectiveness_rank"
        ) \
        .filter(col("channel").isNotNull() & (col("channel") != ""))
    
    # Debug: Show sample results before writing
    print("\nChannel effectiveness results:")
    channel_effectiveness_final.show()
    
    # Write to Gold layer
    channel_effectiveness_final.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.GOLD_SCHEMA}.channel_effectiveness")
    
    # Display results
    print("\n=== Channel Effectiveness Analysis ===")
    display(channel_effectiveness_final.orderBy("effectiveness_rank"))
    
    return channel_effectiveness_final

# Execute channel effectiveness analysis
channel_effectiveness_df = analyze_channel_effectiveness()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2: How is the age distribution of customers who completed offers compared to those who did not?

# COMMAND ----------

def analyze_age_distribution():
    """
    Analyze age distribution of customers based on offer completion status
    """
    print("Analyzing age distribution by completion status...")
    
    # Read required tables
    customers = spark.table(f"{Config.SILVER_SCHEMA}.customers_cleansed")
    interactions = spark.table(f"{Config.SILVER_SCHEMA}.offer_interactions")
    
    # Get customers who received offers
    customers_with_offers = interactions \
        .filter(col("is_received") == True) \
        .groupBy("customer_id") \
        .agg(
            sum(when(col("is_completed"), 1).otherwise(0)).alias("offers_completed"),
            count("*").alias("offers_received"),
            avg(when(col("is_completed"), col("time_to_complete")).otherwise(None)).alias("avg_completion_time")
        ) \
        .withColumn("completion_status", 
                   when(col("offers_completed") > 0, "Completed").otherwise("Not Completed"))
    
    # Join with customer demographics
    age_analysis = customers \
        .join(customers_with_offers, "customer_id", "inner") \
        .filter(col("data_quality_flag") == "valid")
    
    # Calculate distribution by age group and completion status
    age_distribution = age_analysis \
        .groupBy("age_group", "completion_status") \
        .agg(
            count("*").alias("customer_count"),
            avg("income").alias("avg_income"),
            avg("offers_received").alias("avg_offers_received")
        )
    
    # Calculate percentages within each completion status
    window_spec = Window.partitionBy("completion_status")
    age_distribution_with_pct = age_distribution \
        .withColumn("total_in_status", sum("customer_count").over(window_spec)) \
        .withColumn("percentage", round(col("customer_count") / col("total_in_status") * 100, 2)) \
        .drop("total_in_status") \
        .select(
            "age_group",
            "completion_status",
            "customer_count",
            "percentage",
            round("avg_income", 2).alias("avg_income"),
            round("avg_offers_received", 2).alias("avg_offers_received")
        )
    
    # Write to Gold layer
    age_distribution_with_pct.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.GOLD_SCHEMA}.age_distribution_analysis")
    
    # Display results
    print("\n=== Age Distribution Analysis ===")
    display(age_distribution_with_pct.orderBy("completion_status", "age_group"))
    
    return age_distribution_with_pct

# Execute age distribution analysis
age_distribution_df = analyze_age_distribution()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: What is the average time taken by customers to complete an offer after receiving it?

# COMMAND ----------

def analyze_completion_time():
    """
    Analyze average time taken to complete offers by offer type and channel
    """
    print("Analyzing offer completion time...")
    
    # Read required tables
    offers = spark.table(f"{Config.SILVER_SCHEMA}.offers_enriched")
    interactions = spark.table(f"{Config.SILVER_SCHEMA}.offer_interactions")
    
    # Join offers with interactions
    completion_analysis_base = interactions \
        .filter(col("is_completed") == True) \
        .join(offers, "offer_id", "inner")
    
    # Explode channels to create rows for each channel
    completion_analysis = completion_analysis_base \
        .select("offer_id", "offer_type", "time_to_complete", "customer_id", 
                explode("channels_list").alias("channel"))
    
    # Calculate completion time metrics by offer type AND channel
    completion_metrics = completion_analysis \
        .groupBy("offer_type", "channel") \
        .agg(
            avg("time_to_complete").alias("avg_completion_time_hours"),
            expr("percentile_approx(time_to_complete, 0.5)").alias("median_completion_time_hours"),
            min("time_to_complete").alias("min_completion_time_hours"),
            max("time_to_complete").alias("max_completion_time_hours"),
            stddev("time_to_complete").alias("std_dev_hours"),
            count("*").alias("total_completions"),
            countDistinct("customer_id").alias("unique_customers")
        )
    
    # Calculate total offers received by offer type and channel
    # First, get all received offers with exploded channels
    all_offers_received = interactions \
        .filter(col("is_received") == True) \
        .join(offers, "offer_id", "inner") \
        .select("offer_id", "offer_type", "customer_id", 
                explode("channels_list").alias("channel"))
    
    total_offers = all_offers_received \
        .groupBy("offer_type", "channel") \
        .agg(
            count("*").alias("total_received"),
            countDistinct("customer_id").alias("total_customers_targeted")
        )
    
    # Combine metrics
    completion_time_final = completion_metrics \
        .join(total_offers, ["offer_type", "channel"], "inner") \
        .withColumn("completion_rate", 
                   round(col("total_completions") / col("total_received") * 100, 2)) \
        .select(
            "offer_type",
            "channel",
            round("avg_completion_time_hours", 2).alias("avg_completion_time_hours"),
            round("median_completion_time_hours", 2).alias("median_completion_time_hours"),
            round("min_completion_time_hours", 2).alias("min_completion_time_hours"),
            round("max_completion_time_hours", 2).alias("max_completion_time_hours"),
            round("std_dev_hours", 2).alias("std_dev_hours"),
            "total_completions",
            "total_received",
            "completion_rate",
            "unique_customers",
            "total_customers_targeted"
        ) \
        .orderBy("offer_type", "channel")
    
    # Write to Gold layer
    completion_time_final.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{Config.GOLD_SCHEMA}.offer_completion_metrics")
    
    # Display results
    print("\n=== Offer Completion Time Analysis by Type and Channel ===")
    display(completion_time_final.orderBy("offer_type", "avg_completion_time_hours"))
    
    # Calculate overall averages
    print("\n=== Summary Statistics ===")
    
    # Overall average
    overall_avg = completion_analysis.agg(avg("time_to_complete")).collect()[0][0]
    print(f"Overall average completion time: {overall_avg:.2f} hours")
    
    # Average by offer type only
    print("\n=== Average Completion Time by Offer Type ===")
    by_type = completion_analysis \
        .groupBy("offer_type") \
        .agg(
            avg("time_to_complete").alias("avg_hours"),
            count("*").alias("completions")
        ) \
        .orderBy("avg_hours")
    display(by_type)
    
    # Average by channel only
    print("\n=== Average Completion Time by Channel ===")
    by_channel = completion_analysis \
        .groupBy("channel") \
        .agg(
            avg("time_to_complete").alias("avg_hours"),
            count("*").alias("completions")
        ) \
        .orderBy("avg_hours")
    display(by_channel)
    
    return completion_time_final

# Execute completion time analysis
completion_time_df = analyze_completion_time()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Gold Layer Views

# COMMAND ----------


def create_gold_views():
    """
    Create 3 basic views in Gold layer using SQL directly
    """
    print("Creating Gold layer views...")
    
    # Drop existing views if they exist
    for view in ["v_channel_performance", "v_age_summary", "v_offer_summary"]:
        spark.sql(f"DROP VIEW IF EXISTS {Config.GOLD_SCHEMA}.{view}")
    
    # 1. Channel Performance View
    spark.sql(f"""
        CREATE OR REPLACE VIEW {Config.GOLD_SCHEMA}.v_channel_performance AS
        SELECT 
            channel,
            total_offers_sent,
            offers_completed,
            completion_rate,
            total_revenue_generated,
            effectiveness_rank,
            CASE 
                WHEN completion_rate >= 50 THEN 'High'
                WHEN completion_rate >= 30 THEN 'Medium'
                ELSE 'Low'
            END as performance_level
        FROM {Config.GOLD_SCHEMA}.channel_effectiveness
        ORDER BY effectiveness_rank
    """)
    print("✓ Created v_channel_performance")
    
    # 2. Age Completion Summary View
    spark.sql(f"""
        CREATE OR REPLACE VIEW {Config.GOLD_SCHEMA}.v_age_summary AS
        SELECT 
            age_group,
            SUM(CASE WHEN completion_status = 'Completed' THEN customer_count ELSE 0 END) as completed_count,
            SUM(CASE WHEN completion_status = 'Not Completed' THEN customer_count ELSE 0 END) as not_completed_count,
            AVG(avg_income) as avg_income,
            SUM(customer_count) as total_customers,
            ROUND(
                SUM(CASE WHEN completion_status = 'Completed' THEN customer_count ELSE 0 END) * 100.0 / 
                SUM(customer_count), 2
            ) as completion_rate
        FROM {Config.GOLD_SCHEMA}.age_distribution_analysis
        GROUP BY age_group
        ORDER BY age_group
    """)
    print("✓ Created v_age_summary")
    
    # 3. Offer Type Summary View
    spark.sql(f"""
        CREATE OR REPLACE VIEW {Config.GOLD_SCHEMA}.v_offer_summary AS
        SELECT 
            offer_type,
            ROUND(AVG(completion_rate), 2) as avg_completion_rate,
            ROUND(AVG(avg_completion_time_hours), 2) as avg_completion_time,
            SUM(total_completions) as total_completions,
            SUM(total_received) as total_received
        FROM {Config.GOLD_SCHEMA}.offer_completion_metrics
        GROUP BY offer_type
        ORDER BY avg_completion_rate DESC
    """)
    print("✓ Created v_offer_summary")
    
    print("\n=== Gold Layer Views Summary ===")
    views = ["v_channel_performance", "v_age_summary", "v_offer_summary"]
    
    for view in views:
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {Config.GOLD_SCHEMA}.{view}").collect()[0][0]
            print(f"{view}: {count} records")
        except Exception as e:
            print(f"{view}: Error - {str(e)}")

# Execute view creation
create_gold_views()

# COMMAND ----------

# Display the views
print("=== Channel Performance ===")
display(spark.sql(f"SELECT * FROM {Config.GOLD_SCHEMA}.v_channel_performance"))

print("\n=== Age Summary ===")
display(spark.sql(f"SELECT * FROM {Config.GOLD_SCHEMA}.v_age_summary"))

print("\n=== Offer Summary ===")
display(spark.sql(f"SELECT * FROM {Config.GOLD_SCHEMA}.v_offer_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Visualization from Gold Layer

# COMMAND ----------

# Visualize Channel Effectiveness
channel_data = spark.table(f"{Config.GOLD_SCHEMA}.channel_effectiveness").toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# Completion rate by channel
channel_data_sorted = channel_data.sort_values('completion_rate', ascending=True)
ax1.barh(channel_data_sorted['channel'], channel_data_sorted['completion_rate'], color='#2ecc71')
ax1.set_xlabel('Completion Rate (%)')
ax1.set_title('Channel Effectiveness by Completion Rate')

# Add value labels
for i, v in enumerate(channel_data_sorted['completion_rate']):
    ax1.text(v + 0.5, i, f'{v:.1f}%', va='center')

# Revenue per offer by channel
ax2.bar(channel_data['channel'], channel_data['avg_revenue_per_offer'], color='#3498db')
ax2.set_xlabel('Channel')
ax2.set_ylabel('Average Revenue per Offer ($)')
ax2.set_title('Average Revenue per Completed Offer by Channel')
ax2.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# Visualize Age Distribution
age_data = spark.table(f"{Config.GOLD_SCHEMA}.age_distribution_analysis").toPandas()

# Pivot data for visualization
age_pivot = age_data.pivot(index='age_group', columns='completion_status', values='percentage')

# Create visualization
fig, ax = plt.subplots(figsize=(12, 8))

# Create grouped bar chart
x = range(len(age_pivot.index))
width = 0.35

completed_bars = ax.bar([i - width/2 for i in x], age_pivot['Completed'], width, label='Completed', color='#2ecc71')
not_completed_bars = ax.bar([i + width/2 for i in x], age_pivot['Not Completed'], width, label='Not Completed', color='#e74c3c')

# Customize chart
ax.set_xlabel('Age Group')
ax.set_ylabel('Percentage of Customers (%)')
ax.set_title('Age Distribution: Offer Completion Status Comparison')
ax.set_xticks(x)
ax.set_xticklabels(age_pivot.index)
ax.legend()
ax.grid(axis='y', alpha=0.3)

# Add value labels
for bars in [completed_bars, not_completed_bars]:
    for bar in bars:
        height = bar.get_height()
        if height is not None and not pd.isna(height):
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}%', ha='center', va='bottom')

plt.tight_layout()
plt.show()

# COMMAND ----------

# Visualize Completion Time - Clear Version
completion_data = spark.table(f"{Config.GOLD_SCHEMA}.offer_completion_metrics").toPandas()

# Since we have offer_type x channel combinations, let's aggregate by offer_type
offer_summary = completion_data.groupby('offer_type').agg({
    'avg_completion_time_hours': 'mean',
    'completion_rate': 'mean',
    'total_completions': 'sum',
    'total_received': 'sum'
}).round(2)

# Create figure with better layout
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Offer Performance Analysis', fontsize=16, y=0.98)

# 1. Average completion time by offer type (aggregated)
ax1 = axes[0, 0]
offer_types = offer_summary.index
avg_times = offer_summary['avg_completion_time_hours']

colors = ['#3498db', '#e74c3c', '#f39c12']
bars1 = ax1.bar(offer_types, avg_times, color=colors[:len(offer_types)])
ax1.set_xlabel('Offer Type', fontsize=12)
ax1.set_ylabel('Average Completion Time (hours)', fontsize=12)
ax1.set_title('Average Time to Complete by Offer Type', fontsize=14, pad=20)
ax1.grid(axis='y', alpha=0.3)

# Add value labels
for bar, value in zip(bars1, avg_times):
    ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 2,
            f'{value:.1f}h', ha='center', va='bottom', fontsize=11, fontweight='bold')

# 2. Completion rate by offer type (aggregated)
ax2 = axes[0, 1]
completion_rates = offer_summary['completion_rate']
bars2 = ax2.bar(offer_types, completion_rates, color=colors[:len(offer_types)])
ax2.set_xlabel('Offer Type', fontsize=12)
ax2.set_ylabel('Completion Rate (%)', fontsize=12)
ax2.set_title('Average Completion Rate by Offer Type', fontsize=14, pad=20)
ax2.grid(axis='y', alpha=0.3)
ax2.set_ylim(0, float(completion_rates.max()) * 1.2)  # Fixed: Convert to float

# Add value labels
for bar, value in zip(bars2, completion_rates):
    ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
            f'{value:.1f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

# 3. Heatmap: Completion time by offer type and channel
ax3 = axes[1, 0]
pivot_time = completion_data.pivot(index='offer_type', columns='channel', values='avg_completion_time_hours')
sns.heatmap(pivot_time, annot=True, fmt='.0f', cmap='YlOrRd', ax=ax3, 
            cbar_kws={'label': 'Hours'})
ax3.set_title('Completion Time Heatmap (Offer × Channel)', fontsize=14, pad=20)
ax3.set_xlabel('Channel', fontsize=12)
ax3.set_ylabel('Offer Type', fontsize=12)

# 4. Heatmap: Completion rate by offer type and channel
ax4 = axes[1, 1]
pivot_rate = completion_data.pivot(index='offer_type', columns='channel', values='completion_rate')
sns.heatmap(pivot_rate, annot=True, fmt='.1f', cmap='RdYlGn', ax=ax4,
            cbar_kws={'label': 'Completion %'})
ax4.set_title('Completion Rate Heatmap (Offer × Channel)', fontsize=14, pad=20)
ax4.set_xlabel('Channel', fontsize=12)
ax4.set_ylabel('Offer Type', fontsize=12)

plt.tight_layout()
plt.show()

# Additional visualization: Channel comparison for each offer type
unique_offers = completion_data['offer_type'].unique()
fig2, axes2 = plt.subplots(1, len(unique_offers), figsize=(6*len(unique_offers), 6))
if len(unique_offers) == 1:
    axes2 = [axes2]  # Make it a list if only one subplot
fig2.suptitle('Channel Performance by Offer Type', fontsize=16)

for idx, (offer_type, ax) in enumerate(zip(unique_offers, axes2)):
    offer_data = completion_data[completion_data['offer_type'] == offer_type].sort_values('channel')
    
    x = range(len(offer_data))
    width = 0.35
    
    # Create bars for completion time and rate
    ax_twin = ax.twinx()
    
    bars1 = ax.bar([i - width/2 for i in x], offer_data['avg_completion_time_hours'], 
                   width, label='Completion Time', color='#3498db', alpha=0.7)
    bars2 = ax_twin.bar([i + width/2 for i in x], offer_data['completion_rate'], 
                       width, label='Completion Rate', color='#2ecc71', alpha=0.7)
    
    # Formatting
    ax.set_xlabel('Channel', fontsize=12)
    ax.set_ylabel('Avg Completion Time (hours)', fontsize=12, color='#3498db')
    ax_twin.set_ylabel('Completion Rate (%)', fontsize=12, color='#2ecc71')
    ax.set_title(f'{offer_type.title()} Offers', fontsize=14, pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(offer_data['channel'], rotation=45)
    ax.tick_params(axis='y', colors='#3498db')
    ax_twin.tick_params(axis='y', colors='#2ecc71')
    
    # Add value labels
    for bar, value in zip(bars1, offer_data['avg_completion_time_hours']):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
               f'{value:.0f}h', ha='center', va='bottom', fontsize=9)
    
    for bar, value in zip(bars2, offer_data['completion_rate']):
        ax_twin.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
                    f'{value:.0f}%', ha='center', va='bottom', fontsize=9)
    
    # Add legends
    if idx == 0:
        ax.legend(loc='upper left')
        ax_twin.legend(loc='upper right')

plt.tight_layout()
plt.show()

# Summary statistics
print("\n=== OFFER COMPLETION INSIGHTS ===")
print("\nOverall Performance by Offer Type:")
print(offer_summary[['avg_completion_time_hours', 'completion_rate']].round(1))

print("\nBest Performing Combinations:")
best_combos = completion_data.nlargest(5, 'completion_rate')[['offer_type', 'channel', 'completion_rate', 'avg_completion_time_hours']]
print(best_combos.to_string(index=False))

print("\nFastest Completion Combinations:")
fastest_combos = completion_data.nsmallest(5, 'avg_completion_time_hours')[['offer_type', 'channel', 'avg_completion_time_hours', 'completion_rate']]
print(fastest_combos.to_string(index=False))
