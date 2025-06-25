#!/usr/bin/env python
# coding: utf-8

# ## nb_bronze_silver_uber_eats
# 
# New notebook

# # Phase 1: Environment Setup and Data Loading
# *Objective: Load bronze layer tables and verify data availability for transformation pipeline.*

# In[32]:


print("🚀 Phase 1: Environment Setup and Data Loading")
print("=" * 50)
print("Objective: Load bronze tables and verify data availability")
print()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

print(f"Spark Version: {spark.version}")

print("\n Loading Bronze Tables:")

df_drivers = spark.table("lh_uber_eats.dbo.df2_bronze_postgres_drivers")
print(f"✅ Drivers: {df_drivers.count():,} rows")

df_orders = spark.table("lh_uber_eats.dbo.nb_bronze_kafka_orders")
print(f"✅ Orders: {df_orders.count():,} rows")


df_status = spark.table("lh_uber_eats.dbo.pl_bronze_kafka_status") 
print(f"✅ Status: {df_status.count():,} rows")

print("\n✅ Phase 1 Complete - All bronze data loaded successfully")


# # Phase 2: Data Quality Assessment and Schema Analysis
# *Objective: Analyze data schemas, identify quality issues, and understand data patterns before transformation.*

# In[33]:


print("🔍 Phase 2: Data Quality Assessment and Schema Analysis")
print("=" * 60)
print("Objective: Analyze schemas and identify data quality issues")
print()

print("📋 SCHEMA ANALYSIS:")
print("-" * 30)

print("\n🔍 Drivers Schema:")
df_drivers.printSchema()

print("\n🔍 Orders Schema:")
df_orders.printSchema()

print("\n🔍 Status Schema:")
df_status.printSchema()

print("\n🎯 DATA QUALITY ASSESSMENT:")
print("-" * 30)

print("\n📊 Null Count Analysis:")

print("\nDrivers - Key Field Nulls:")
df_drivers.select([
    sum(when(col("driver_id").isNull(), 1).otherwise(0)).alias("driver_id_nulls"),
    sum(when(col("phone_number").isNull(), 1).otherwise(0)).alias("phone_nulls"),
    sum(when(col("first_name").isNull(), 1).otherwise(0)).alias("first_name_nulls")
]).show()

print("Orders - Key Field Nulls:")
df_orders.select([
    sum(when(col("order_id").isNull(), 1).otherwise(0)).alias("order_id_nulls"),
    sum(when(col("driver_key").isNull(), 1).otherwise(0)).alias("driver_key_nulls"),
    sum(when(col("total_amount").isNull(), 1).otherwise(0)).alias("amount_nulls")
]).show()

print("Status - Key Field Nulls:")
df_status.select([
    sum(when(col("order_identifier").isNull(), 1).otherwise(0)).alias("order_id_nulls"),
    sum(when(col("`status.status_name`").isNull(), 1).otherwise(0)).alias("status_nulls")
]).show()

print("\n📊 SAMPLE DATA PREVIEW:")
print("-" * 30)

print("\nDrivers Sample (top 5):")
df_drivers.select("driver_id", "first_name", "last_name", "vehicle_type", "phone_number").show(5)

print("Orders Sample (top 5):")
df_orders.select("order_id", "driver_key", "total_amount", "order_date").show(5)

print("Status Sample (top 5):")
df_status.select("order_identifier", "`status.status_name`", "`status.timestamp`").show(5)

print("\n✅ Phase 2 Complete - Data quality assessment finished")


# # Phase 3: Data Cleansing and Standardization
# *Objective: Clean and standardize each bronze table while maintaining the same granularity (no aggregations).* 

# In[34]:


print("🧹 Phase 3: Data Cleansing and Standardization")
print("=" * 50)
print("Objective: Clean and standardize all bronze data while maintaining granularity")
print()

print("🔧 Cleaning Drivers Data:")
print("-" * 30)

df_drivers_clean = df_drivers.select(
    col("driver_id").cast("integer").alias("driver_id"),
    initcap(trim(col("first_name"))).alias("first_name_clean"),
    initcap(trim(col("last_name"))).alias("last_name_clean"),
    concat(initcap(trim(col("first_name"))), lit(" "), initcap(trim(col("last_name")))).alias("full_name"),
    regexp_replace(col("phone_number"), "[^0-9]", "").alias("phone_digits_only"),
    when(upper(col("vehicle_type")).contains("BIKE"), "Motorcycle")
    .when(upper(col("vehicle_type")).contains("CAR"), "Car") 
    .when(upper(col("vehicle_type")).contains("MOTOR"), "Motorcycle")
    .otherwise(initcap(col("vehicle_type"))).alias("vehicle_type_clean"),
    (year(current_date()) - col("vehicle_year")).alias("vehicle_age_years"),
    initcap(trim(col("city"))).alias("city_clean"),
    upper(trim(col("country"))).alias("country_clean"),
    col("license_number"), col("vehicle_make"), col("vehicle_model"), col("vehicle_year"),
    col("uuid"), col("date_birth"), col("dt_current_timestamp"), col("record_source")
)

print(f"✅ Drivers cleaned: {df_drivers_clean.count():,} rows")

print("\n🔧 Cleaning Orders Data:")
print("-" * 30)

df_orders_clean = df_orders.select(
    trim(col("order_id")).alias("order_id"),
    trim(col("driver_key")).alias("driver_key"), 
    col("driver_key").cast("integer").alias("driver_id"),
    round(col("total_amount").cast("decimal(10,2)"), 2).alias("total_amount"),
    to_timestamp(col("order_date")).alias("order_datetime"),
    to_date(to_timestamp(col("order_date"))).alias("order_date"),  # Using to_date instead of date
    hour(to_timestamp(col("order_date"))).alias("order_hour"),
    when(hour(to_timestamp(col("order_date"))).between(6, 11), "Morning")
    .when(hour(to_timestamp(col("order_date"))).between(12, 17), "Afternoon") 
    .when(hour(to_timestamp(col("order_date"))).between(18, 22), "Evening")
    .otherwise("Night").alias("time_period"),
    trim(col("user_key")).alias("user_key"),
    trim(col("restaurant_key")).alias("restaurant_key"),
    trim(col("payment_key")).alias("payment_key"),
    trim(col("rating_key")).alias("rating_key"),
    col("load_dts"), col("record_source"), col("ingest_method")
)

print(f"✅ Orders cleaned: {df_orders_clean.count():,} rows")

print("\n🔧 Cleaning Status Data:")
print("-" * 30)

df_status_clean = df_status.select(
    trim(col("order_identifier")).alias("order_id"),
    initcap(trim(col("`status.status_name`"))).alias("status_name_clean"),
    (col("`status.timestamp`").cast("bigint") / 1000).cast("timestamp").alias("status_datetime"),
    col("status_id"),
    col("dt_current_timestamp")
)

print(f"✅ Status cleaned: {df_status_clean.count():,} rows")

print("\n📊 Sample cleaned data:")
print("\nDrivers:")
df_drivers_clean.select("driver_id", "full_name", "vehicle_type_clean", "city_clean").show(3)
print("\nOrders:")
df_orders_clean.select("order_id", "driver_id", "total_amount", "time_period").show(3)
print("\nStatus:")
df_status_clean.select("order_id", "status_name_clean", "status_datetime").show(3)

print("\n✅ Phase 3 Complete - All bronze data cleaned and standardized")


# # Phase 4: Data Verification and Validation
# *Objective: Verify data integrity and apply business rule validations while maintaining granularity* 

# In[36]:


print("✅ Phase 4: Data Verification and Validation")
print("=" * 50)
print("Objective: Verify data integrity and apply business rule validations while maintaining granularity")
print()

print("🔍 Data Verification Rules:")
print("-" * 30)

print("\n📋 Drivers Verification:")
drivers_verification = df_drivers_clean.select(
    count("*").alias("total_records"),
    countDistinct("driver_id").alias("unique_drivers"),
    sum(when(col("driver_id").isNull(), 1).otherwise(0)).alias("null_driver_ids"),
    sum(when(col("full_name").isNull() | (col("full_name") == ""), 1).otherwise(0)).alias("missing_names"),
    sum(when(col("phone_digits_only").isNull() | (length(col("phone_digits_only")) < 10), 1).otherwise(0)).alias("invalid_phones"),
    sum(when(col("vehicle_age_years") < 0, 1).otherwise(0)).alias("invalid_vehicle_age"),
    sum(when(col("vehicle_age_years") > 50, 1).otherwise(0)).alias("very_old_vehicles")
)
drivers_verification.show()

print("📋 Orders Verification:")
orders_verification = df_orders_clean.select(
    count("*").alias("total_records"),
    countDistinct("order_id").alias("unique_orders"),
    sum(when(col("order_id").isNull(), 1).otherwise(0)).alias("null_order_ids"),
    sum(when(col("total_amount") <= 0, 1).otherwise(0)).alias("invalid_amounts"),
    sum(when(col("driver_id").isNull(), 1).otherwise(0)).alias("missing_driver_refs"),
    sum(when(col("order_datetime").isNull(), 1).otherwise(0)).alias("missing_timestamps")
)
orders_verification.show()

print("📋 Status Verification:")
status_verification = df_status_clean.select(
    count("*").alias("total_records"),
    countDistinct("order_id").alias("unique_orders_with_status"),
    sum(when(col("order_id").isNull(), 1).otherwise(0)).alias("null_order_refs"),
    sum(when(col("status_name_clean").isNull(), 1).otherwise(0)).alias("missing_status"),
    sum(when(col("status_datetime").isNull(), 1).otherwise(0)).alias("invalid_timestamps")
)
status_verification.show()

print("\n🎯 Business Rule Validations:")
print("-" * 30)

df_drivers_verified = df_drivers_clean.withColumn(
    "is_valid_driver",
    when(
        (col("driver_id").isNotNull()) &
        (col("full_name").isNotNull() & (col("full_name") != "")) &
        (length(col("phone_digits_only")) >= 10) &
        (col("vehicle_age_years") >= 0) &
        (col("vehicle_age_years") <= 50),
        True
    ).otherwise(False)
)

df_orders_verified = df_orders_clean.withColumn(
    "is_valid_order", 
    when(
        (col("order_id").isNotNull()) &
        (col("total_amount") > 0) &
        (col("driver_id").isNotNull()) &
        (col("order_datetime").isNotNull()),
        True
    ).otherwise(False)
)

df_status_verified = df_status_clean.withColumn(
    "is_valid_status",
    when(
        (col("order_id").isNotNull()) &
        (col("status_name_clean").isNotNull()) &
        (col("status_datetime").isNotNull()),
        True
    ).otherwise(False)
)

print("✅ Validation flags applied to all datasets")

print("\n📊 Validation Results:")
print("Drivers valid records:", df_drivers_verified.filter(col("is_valid_driver") == True).count())
print("Orders valid records:", df_orders_verified.filter(col("is_valid_order") == True).count()) 
print("Status valid records:", df_status_verified.filter(col("is_valid_status") == True).count())

print("\n✅ Phase 4 Complete - Data verification and validation finished")


# # Data Conformance and Standardization
# *Objective: Conform data to standard formats and create unified schemas across all tables*

# In[37]:


print("🔄 Phase 5: Data Conformance and Standardization")
print("=" * 50)
print("Objective: Conform data to standard formats and create unified schemas across all tables")
print()

print("🎯 Conforming Data Standards:")
print("-" * 30)

print("\n📅 Standardizing DateTime Formats:")

df_drivers_conformed = df_drivers_verified.withColumn(
    "created_at", coalesce(col("dt_current_timestamp"), current_timestamp())
).withColumn(
    "updated_at", current_timestamp()
).withColumn(
    "data_source", lit("postgres_drivers")
).withColumn(
    "quality_score", 
    when(col("is_valid_driver"), 100)
    .when(col("phone_digits_only").isNull(), 70)
    .when(col("vehicle_age_years") > 30, 80)
    .otherwise(90)
)

df_orders_conformed = df_orders_verified.withColumn(
    "created_at", coalesce(col("load_dts"), current_timestamp())
).withColumn(
    "updated_at", current_timestamp()
).withColumn(
    "data_source", lit("kafka_orders")
).withColumn(
    "order_value_category",
    when(col("total_amount") <= 20, "Low")
    .when(col("total_amount") <= 50, "Medium")  
    .when(col("total_amount") <= 100, "High")
    .otherwise("Premium")
).withColumn(
    "quality_score",
    when(col("is_valid_order"), 100)
    .when(col("driver_id").isNull(), 60)
    .when(col("total_amount") <= 0, 50)
    .otherwise(90)
)

df_status_conformed = df_status_verified.withColumn(
    "created_at", coalesce(col("dt_current_timestamp"), current_timestamp())
).withColumn(
    "updated_at", current_timestamp()
).withColumn(
    "data_source", lit("kafka_status")
).withColumn(
    "status_category",
    when(col("status_name_clean").isin("Order Placed", "In Analysis", "Accepted"), "Initial")
    .when(col("status_name_clean").isin("Preparing", "Ready For Pickup", "Picked Up"), "Preparation")
    .when(col("status_name_clean").isin("Out For Delivery"), "Delivery")
    .when(col("status_name_clean").isin("Delivered", "Completed"), "Final")
    .otherwise("Unknown")
).withColumn(
    "quality_score",
    when(col("is_valid_status"), 100)
    .when(col("status_datetime").isNull(), 70)
    .otherwise(90)
)

print("✅ Data conformed with standard columns and quality scores")

print("\n📊 Conformance Results:")
print("\nDrivers Quality Score Distribution:")
df_drivers_conformed.select("data_source", "quality_score").groupBy("quality_score").count().show()

print("Orders Value Category Distribution:")
df_orders_conformed.select("data_source", "order_value_category").groupBy("order_value_category").count().show()

print("Status Category Distribution:")
df_status_conformed.select("data_source", "status_category").groupBy("status_category").count().show()

print("\n✅ Phase 5 Complete - Data conformance and standardization finished")


# # Phase 6: Data Enrichment and Business Logic
# *Objective: Add calculated fields and business logic to enhance data value without creating relationships*

# In[40]:


print("⚡ Phase 6: Data Enrichment and Business Logic")
print("=" * 50)
print("Objective: Add calculated fields and business logic to enhance data value without creating relationships")
print()

print("🔧 Enriching Drivers Data:")
print("-" * 30)

df_drivers_enriched = df_drivers_conformed.withColumn(
    "vehicle_category",
    when(col("vehicle_type_clean").isin("Car"), "Four Wheeler")
    .when(col("vehicle_type_clean").isin("Motorcycle"), "Two Wheeler")
    .otherwise("Other")
).withColumn(
    "experience_level",
    when(col("vehicle_age_years") <= 3, "New Vehicle")
    .when(col("vehicle_age_years") <= 10, "Experienced")
    .otherwise("Veteran")
).withColumn(
    "phone_formatted",
    when(length(col("phone_digits_only")) == 11,
         concat(lit("("), substring(col("phone_digits_only"), 1, 2), lit(") "),
                substring(col("phone_digits_only"), 3, 5), lit("-"),
                substring(col("phone_digits_only"), 8, 4)))
    .otherwise(col("phone_digits_only"))
).withColumn(
    "driver_profile_completeness",
    when(col("is_valid_driver") & col("phone_digits_only").isNotNull() & 
         col("vehicle_make").isNotNull() & col("city_clean").isNotNull(), "Complete")
    .when(col("is_valid_driver"), "Partial")
    .otherwise("Incomplete")
)

print("✅ Drivers data enriched with business categories")

print("\n🔧 Enriching Orders Data:")
print("-" * 30)

df_orders_enriched = df_orders_conformed.withColumn(
    "order_day_of_week",
    date_format(col("order_date"), "EEEE")
).withColumn(
    "is_weekend",
    when(date_format(col("order_date"), "EEEE").isin("Saturday", "Sunday"), True).otherwise(False)
).withColumn(
    "order_month",
    month(col("order_date"))
).withColumn(
    "order_year",
    year(col("order_date"))
).withColumn(
    "delivery_urgency",
    when(col("time_period") == "Night", "Low")
    .when(col("time_period").isin("Morning", "Afternoon"), "High")
    .otherwise("Medium")
).withColumn(
    "order_size_category",
    when(col("total_amount") <= 30, "Small")
    .when(col("total_amount") <= 80, "Medium")
    .otherwise("Large")
)

print("✅ Orders data enriched with temporal and business attributes")

print("\n🔧 Enriching Status Data:")
print("-" * 30)

df_status_enriched = df_status_conformed.withColumn(
    "status_hour",
    hour(col("status_datetime"))
).withColumn(
    "status_day_of_week",
    date_format(col("status_datetime"), "EEEE")
).withColumn(
    "status_date",
    to_date(col("status_datetime"))
).withColumn(
    "is_business_hours",
    when(hour(col("status_datetime")).between(9, 18), True).otherwise(False)
).withColumn(
    "status_priority",
    when(col("status_name_clean").isin("Order Placed", "Delivered"), "High")
    .when(col("status_name_clean").isin("Preparing", "Out For Delivery"), "Medium")
    .otherwise("Low")
)

print("✅ Status data enriched with temporal and priority attributes")

print("\n📊 Enrichment Results:")
print("-" * 30)

print("Drivers Vehicle Categories:")
df_drivers_enriched.groupBy("vehicle_category").count().show()

print("Orders by Day of Week:")
df_orders_enriched.groupBy("order_day_of_week").count().orderBy("count", ascending=False).show()

print("Status Priority Distribution:")
df_status_enriched.groupBy("status_priority").count().show()

print("\n✅ Phase 6 Complete - Data enrichment and business logic applied")


# # Phase 7: Data Munging and Final Silver Preparation
# *Objective: Apply final transformations and prepare optimized Silver tables with consistent schema*

# In[41]:


print("⚡ Phase 7: Data Munging and Final Silver Preparation")
print("=" * 50)
print("Objective: Apply final transformations and prepare optimized Silver tables with consistent schema")
print()

print("🔧 Final Data Munging:")
print("-" * 30)

# Drivers final silver preparation
df_drivers_silver = df_drivers_enriched.select(
    col("driver_id"),
    col("full_name"),
    col("first_name_clean").alias("first_name"),
    col("last_name_clean").alias("last_name"),
    col("phone_formatted").alias("phone_number"),
    col("vehicle_type_clean").alias("vehicle_type"),
    col("vehicle_category"),
    col("vehicle_make"),
    col("vehicle_model"), 
    col("vehicle_year"),
    col("vehicle_age_years"),
    col("experience_level"),
    col("city_clean").alias("city"),
    col("country_clean").alias("country"),
    col("license_number"),
    col("driver_profile_completeness"),
    col("is_valid_driver").alias("is_valid"),
    col("quality_score"),
    col("data_source"),
    col("created_at"),
    col("updated_at")
).withColumn(
    "silver_load_timestamp", current_timestamp()
).withColumn(
    "record_hash", 
    hash(concat_ws("|", col("driver_id"), col("full_name"), col("phone_number"), col("vehicle_type")))
)

# Orders final silver preparation  
df_orders_silver = df_orders_enriched.select(
    col("order_id"),
    col("driver_id"),
    col("user_key"),
    col("restaurant_key"), 
    col("payment_key"),
    col("rating_key"),
    col("total_amount"),
    col("order_datetime"),
    col("order_date"),
    col("order_hour"),
    col("order_day_of_week"),
    col("order_month"),
    col("order_year"),
    col("is_weekend"),
    col("time_period"),
    col("delivery_urgency"),
    col("order_value_category"),
    col("order_size_category"),
    col("is_valid_order").alias("is_valid"),
    col("quality_score"),
    col("data_source"),
    col("created_at"),
    col("updated_at")
).withColumn(
    "silver_load_timestamp", current_timestamp()
).withColumn(
    "record_hash",
    hash(concat_ws("|", col("order_id"), col("driver_id"), col("total_amount"), col("order_datetime")))
)

# Status final silver preparation
df_status_silver = df_status_enriched.select(
    col("status_id"),
    col("order_id"),
    col("status_name_clean").alias("status_name"),
    col("status_datetime"),
    col("status_date"),
    col("status_hour"),
    col("status_day_of_week"),
    col("status_category"),
    col("status_priority"),
    col("is_business_hours"),
    col("is_valid_status").alias("is_valid"),
    col("quality_score"),
    col("data_source"),
    col("created_at"),
    col("updated_at")
).withColumn(
    "silver_load_timestamp", current_timestamp()
).withColumn(
    "record_hash",
    hash(concat_ws("|", col("status_id"), col("order_id"), col("status_name"), col("status_datetime")))
)

print("✅ Silver tables prepared with final optimized schema")

print("\n📊 Silver Data Quality Summary:")
print("-" * 30)

print(f"\n🚗 Drivers Silver:")
print(f"   Total records: {df_drivers_silver.count():,}")
print(f"   Valid records: {df_drivers_silver.filter(col('is_valid')).count():,}")
print(f"   Average quality score: {df_drivers_silver.agg(avg('quality_score')).collect()[0][0]:.1f}")

print(f"\n📦 Orders Silver:")  
print(f"   Total records: {df_orders_silver.count():,}")
print(f"   Valid records: {df_orders_silver.filter(col('is_valid')).count():,}")
print(f"   Average quality score: {df_orders_silver.agg(avg('quality_score')).collect()[0][0]:.1f}")

print(f"\n📊 Status Silver:")
print(f"   Total records: {df_status_silver.count():,}")
print(f"   Valid records: {df_status_silver.filter(col('is_valid')).count():,}")
print(f"   Average quality score: {df_status_silver.agg(avg('quality_score')).collect()[0][0]:.1f}")

print("\n🔍 Final Schema Validation:")
print("-" * 30)
print("\nDrivers Silver Schema:")
df_drivers_silver.printSchema()

print("\nOrders Silver Schema:")
df_orders_silver.printSchema()

print("\nStatus Silver Schema:")
df_status_silver.printSchema()

print("\n✅ Phase 7 Complete - Data munging and Silver preparation finished")


# # Phase 8: Write Silver Tables to Lakehouse
# *Objective: Persist cleaned Silver tables to the Silver layer with proper Delta Lake optimization*

# In[47]:


print("💾 Phase 8: Write Silver Tables to Lakehouse")
print("=" * 50)
print("Objective: Persist cleaned Silver tables to the Silver layer with proper Delta Lake optimization")
print()

print("📝 Writing Silver Tables:")
print("-" * 30)

print("\n🚗 Writing Drivers Silver table...")
df_drivers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_uber_eats.dbo.silver_drivers")

print(f"✅ Silver Drivers table created with {df_drivers_silver.count():,} records")

print("\n📦 Writing Orders Silver table...")
df_orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_uber_eats.dbo.silver_orders")

print(f"✅ Silver Orders table created with {df_orders_silver.count():,} records")

print("\n📊 Writing Status Silver table...")
df_status_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_uber_eats.dbo.silver_status")

print(f"✅ Silver Status table created with {df_status_silver.count():,} records")

print("\n🎯 Verification - Reading back Silver tables:")
print("-" * 30)

silver_drivers_check = spark.table("lh_uber_eats.dbo.silver_drivers").count()
silver_orders_check = spark.table("lh_uber_eats.dbo.silver_orders").count()  
silver_status_check = spark.table("lh_uber_eats.dbo.silver_status").count()

print(f"✅ silver.drivers: {silver_drivers_check:,} records")
print(f"✅ silver.orders: {silver_orders_check:,} records")
print(f"✅ silver.status: {silver_status_check:,} records")

print("\n📈 Final Silver Layer Statistics:")
print("-" * 30)

total_records = silver_drivers_check + silver_orders_check + silver_status_check
print(f"Total Silver records: {total_records:,}")

print("\n🔍 Silver Tables Sample Data:")
print("\nDrivers Silver Sample:")
spark.table("lh_uber_eats.dbo.silver_drivers").select(
    "driver_id", "full_name", "vehicle_type", "vehicle_category", "quality_score", "is_valid"
).show(3)

print("Orders Silver Sample:")
spark.table("lh_uber_eats.dbo.silver_orders").select(
    "order_id", "driver_id", "total_amount", "order_day_of_week", "delivery_urgency", "is_valid"
).show(3)

print("Status Silver Sample:")
spark.table("lh_uber_eats.dbo.silver_status").select(
    "order_id", "status_name", "status_category", "status_priority", "quality_score"
).show(3)

print("\n🔧 Delta Lake Optimization:")
print("-" * 30)

print("Optimizing silver.drivers table...")
spark.sql("OPTIMIZE lh_uber_eats.dbo.silver_drivers")

print("Optimizing silver.orders table...")
spark.sql("OPTIMIZE lh_uber_eats.dbo.silver_orders")

print("Optimizing silver.status table...")
spark.sql("OPTIMIZE lh_uber_eats.dbo.silver_status")

print("✅ All Silver tables optimized")

print("\n🎉 BRONZE TO SILVER TRANSFORMATION COMPLETE!")
print("=" * 60)
print("✅ All data successfully cleaned, verified, conformed, enriched, and munged")
print("✅ Silver tables created with quality scores and validation flags")
print("✅ Business logic and calculated fields added for enhanced analytics")
print("✅ Data granularity maintained throughout the transformation")
print("✅ Delta Lake tables optimized for query performance")
print("✅ Ready for Gold layer dimensional modeling")


# In[ ]:




