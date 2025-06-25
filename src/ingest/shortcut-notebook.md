# Shortcut + Notebook Bronze Ingestion - Kafka Orders

## Objective

Learn to use OneLake shortcuts for data virtualization combined with Spark notebooks for programmatic data processing. This lab demonstrates how to access external data without copying it, then process JSON files using PySpark to create Delta tables with comprehensive audit columns.

## Prerequisites

- Microsoft Fabric workspace with Data Engineering experience enabled
- Lakehouse created: `lh_uber_eats` with public schema preview enabled
- Completion of DataFlow Gen2 lab for comparison
- Basic familiarity with Python and Spark concepts

## Architecture Overview

```
Azure Data Lake Storage Gen2 → OneLake Shortcut → Spark Notebook → Delta Table
│                              │                 │               │
├── JSON files                 ├── Data Virtual- ├── PySpark     ├── Delta format
├── kafka/orders/              ├── ization       ├── Transform-  ├── nb_bronze_kafka_orders
└── Real-time order data       └── No data copy  ├── ations      └── Bronze layer
                                                 └── Code-based
```

## Configuration Details

- **Shortcut Source**: Azure Data Lake Storage Gen2
- **Connection URL**: `https://pythianstg.dfs.core.windows.net/`
- **Container/Bucket**: `owshq-shadow-traffic-uber-eats/`
- **Notebook Name**: `nb_kafka_orders_ingestion`
- **Destination Table**: `nb_bronze_kafka_orders`
- **Data Format**: JSON files with order transactions

## Step-by-Step Implementation

### Step 1: Create OneLake Shortcut

1. **Navigate to Your Lakehouse**
   - Open your `lh_uber_eats` lakehouse
   - Go to the **Files** section (not Tables)

2. **Create New Shortcut**
   - Right-click in the Files area
   - Select **New shortcut**
   - Choose **Azure Data Lake Storage Gen2**

3. **Configure Shortcut Connection**
   - **Connection settings**: Create new connection
   - **URL**: `https://pythianstg.dfs.core.windows.net/`
   - **Authentication kind**: Anonymous
   - **Connection name**: `uber_eats_external_data`
   - Click **Next**

4. **Select Shortcut Path**
   - **Container/Path**: `owshq-shadow-traffic-uber-eats/`
   - **Shortcut name**: `owshq-shadow-traffic-uber-eats`
   - Click **Create**

5. **Verify Shortcut Creation**
   - You should see the shortcut folder in your Files section
   - Navigate to confirm the `kafka/orders/` path exists
   - Verify JSON files are visible without copying data

### Step 2: Create Spark Notebook

6. **Create New Notebook**
   - In your workspace, click **+ New**
   - Select **Notebook**
   - **Name**: `nb_kafka_orders_ingestion`
   - Click **Create**

7. **Attach to Lakehouse**
   - In the notebook, click **Add** in the left panel
   - Select **Existing lakehouse**
   - Choose your `lh_uber_eats` lakehouse
   - Click **Add**

### Step 3: Implement Spark Transformations

8. **Cell 1: Import Libraries and Read Data**
   ```python
   from pyspark.sql.functions import current_timestamp, lit
   
   # Read JSON files through the shortcut path
   df_orders_json = spark.read.json("Files/owshq-shadow-traffic-uber-eats/kafka/orders/*")
   
   # Display schema and sample data
   print("Schema of the orders data:")
   df_orders_json.printSchema()
   
   print(f"\nTotal records found: {df_orders_json.count()}")
   print("\nSample data:")
   display(df_orders_json.limit(5))
   ```

9. **Cell 2: Add Audit Columns and Transform**
   ```python
   # Add bronze layer audit columns
   df_orders_bronze = df_orders_json.withColumn("load_dts", current_timestamp()) \
                     .withColumn("record_source", lit("kafka.orders")) \
                     .withColumn("ingest_method", lit("shortcut"))
   
   # Display transformation results
   print(f"Records with audit columns: {df_orders_bronze.count()}")
   print("\nFinal schema:")
   df_orders_bronze.printSchema()
   
   print("\nSample of transformed data:")
   display(df_orders_bronze.limit(10))
   ```

10. **Cell 3: Write to Delta Table**
    ```python
    # Write to Delta format in lakehouse
    df_orders_bronze.write.format("delta").mode("overwrite").save("Tables/dbo/nb_bronze_kafka_orders")
    
    print("✅ Data successfully written to Delta table: nb_bronze_kafka_orders")
    ```

11. **Cell 4: Validation and Preview**
    ```python
    # Read back and validate the Delta table
    df_validation = spark.read.format("delta").load("Tables/dbo/nb_bronze_kafka_orders")
    
    print(f"Records in Delta table: {df_validation.count()}")
    print("\nFinal table schema:")
    df_validation.printSchema()
    
    print("\nFinal data preview:")
    display(df_validation)
    ```

### Step 4: Execute and Validate

12. **Run the Notebook**
    - Execute all cells using **Run all** or run cell by cell
    - Monitor execution and verify no errors occur
    - Check output for data validation messages

13. **Verify Results in Lakehouse**
    - Navigate back to your `lh_uber_eats` lakehouse
    - Check the **Tables** section
    - Confirm `nb_bronze_kafka_orders` table exists
    - Preview the data to verify transformations

## Complete Notebook Code

For reference, here's the complete notebook implementation:

```python
# Cell 1: Import libraries and read data
from pyspark.sql.functions import current_timestamp, lit

# Read JSON files from the shortcut path
df_orders_json = spark.read.json("Files/owshq-shadow-traffic-uber-eats/kafka/orders/*")

# Cell 2: Transform and add audit columns  
df_orders_bronze = df_orders_json.withColumn("load_dts", current_timestamp()) \
                  .withColumn("record_source", lit("kafka.orders")) \
                  .withColumn("ingest_method", lit("shortcut"))

# Cell 3: Write to Delta table
df_orders_bronze.write.format("delta").mode("overwrite").save("Tables/dbo/nb_bronze_kafka_orders")

# Cell 4: Validate results
display(spark.read.format("delta").load("Tables/dbo/nb_bronze_kafka_orders"))
```

## Expected Output Schema

Your final Delta table should contain:

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Unique order identifier |
| user_key | string | Reference to customer |
| restaurant_key | string | Reference to restaurant |
| driver_key | string | Reference to delivery driver |
| order_date | timestamp | When order was placed |
| total_amount | decimal | Order total value |
| payment_key | string | Payment reference |
| status | string | Order status |
| delivery_address | string | Customer delivery location |
| **load_dts** | timestamp | **Audit: When data was loaded** |
| **record_source** | string | **Audit: Source system (kafka.orders)** |
| **ingest_method** | string | **Audit: How data was ingested (shortcut)** |

## Key Concepts Demonstrated

### OneLake Shortcuts
- **Data Virtualization**: Access external data without copying or moving
- **Cost Efficiency**: No duplicate storage costs or data transfer fees
- **Real-time Access**: Always reflects the current state of source data
- **Security**: Inherits source system permissions and access controls
- **Performance**: Direct access without intermediate storage layers

### Spark Programming
- **Schema Inference**: Automatic JSON schema detection and validation
- **Functional Transformations**: Using `.withColumn()` for data manipulation
- **Built-in Functions**: `current_timestamp()`, `lit()` for adding metadata
- **Delta Format**: ACID transactions, time travel, and versioning capabilities
- **Wildcard Reading**: Process multiple files with pattern matching (`*`)

### Bronze Layer Best Practices
- **Minimal Transformation**: Preserve raw data structure and content
- **Comprehensive Audit Trail**: load_dts, record_source, ingest_method
- **Batch Processing**: Handle multiple files efficiently
- **Error Handling**: Spark's resilient distributed processing
- **Scalability**: Distributed computing for large datasets

## Comparison with DataFlow Gen2

| Aspect | DataFlow Gen2 (Lab 1) | Shortcut + Notebook (Lab 2) |
|--------|----------------------|----------------------------|
| **Interface** | Visual, low-code | Code-based, programmable |
| **Data Access** | Direct connection | Data virtualization |
| **Flexibility** | Limited to Power Query | Full Spark capabilities |
| **Performance** | Good for small/medium data | Excellent for large datasets |
| **Version Control** | No Git integration | Full Git support |
| **Custom Logic** | M language limitations | Unlimited Python/Spark |
| **Learning Curve** | Low | Medium |
| **Enterprise Features** | Basic | Advanced |
| **Data Movement** | Copies data | Virtual access only |

## Performance Considerations

### Optimization Techniques
```python
# For large datasets, consider:
df_orders_bronze = df_orders_json \
    .repartition(4) \
    .withColumn("load_dts", current_timestamp()) \
    .withColumn("record_source", lit("kafka.orders")) \
    .withColumn("ingest_method", lit("shortcut"))

# Write with optimization options
df_orders_bronze.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .mode("overwrite") \
    .save("Tables/dbo/nb_bronze_kafka_orders")
```

### Advanced Spark Configuration
```python
# Configure Spark session for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## Key Learning Points

### Data Virtualization Benefits
- **Zero Data Movement**: Access data where it lives without copying
- **Always Current**: Real-time view of source data changes
- **Cost Effective**: No storage duplication or transfer costs
- **Simplified Architecture**: Fewer data copies to maintain and secure
- **Faster Implementation**: No ETL processes for data movement

### Spark Programming Advantages
- **Scalability**: Handle datasets from MB to PB efficiently
- **Flexibility**: Custom business logic and complex transformations
- **Performance**: Parallel processing and automatic optimization
- **Integration**: Native support for Delta Lake, MLflow, and other tools
- **Language Support**: Python, Scala, SQL, and R interfaces

### Bronze Layer Evolution
- **From ETL to ELT**: Transform after loading data
- **Schema on Read**: Flexible data structure handling
- **Audit Everything**: Comprehensive data lineage and tracking
- **Git Integration**: Version control for data pipelines and notebooks

## Troubleshooting Common Issues

**Issue**: Shortcut not accessible or visible
- **Solution**: Verify external storage permissions and URL correctness

**Issue**: JSON parsing errors in Spark
- **Solution**: Check file format consistency and schema variations

**Issue**: Delta table write fails
- **Solution**: Verify lakehouse permissions and Spark cluster availability

**Issue**: Performance issues with large files
- **Solution**: Implement partitioning and optimize Spark configuration

**Issue**: Path not found errors
- **Solution**: Confirm shortcut path and file organization structure

## Validation Checklist

- OneLake shortcut created successfully
- Shortcut points to correct external container
- Notebook created and attached to lakehouse
- JSON files accessible through shortcut path
- Spark transformations executed without errors
- Audit columns added properly (load_dts, record_source, ingest_method)
- Delta table created with correct name and location
- Data validation confirms expected record count
- Schema includes all source columns plus audit fields

## Advanced Features

### Incremental Processing
```python
# For production scenarios, consider incremental processing
from delta.tables import DeltaTable

# Check if table exists
if DeltaTable.isDeltaTable(spark, "Tables/dbo/nb_bronze_kafka_orders"):
    # Get last processed timestamp
    last_processed = spark.sql("""
        SELECT MAX(load_dts) as max_timestamp 
        FROM nb_bronze_kafka_orders
    """).collect()[0][0]
    
    # Filter only new data
    df_new_orders = df_orders_json.filter(col("order_date") > last_processed)
else:
    # First run, process all data
    df_new_orders = df_orders_json
```

### Error Handling
```python
try:
    # Read and process data
    df_orders_json = spark.read.json("Files/owshq-shadow-traffic-uber-eats/kafka/orders/*")
    
    # Validate data quality
    if df_orders_json.count() == 0:
        raise ValueError("No data found in source files")
    
    # Continue with transformations
    df_orders_bronze = df_orders_json.withColumn("load_dts", current_timestamp())
    
except Exception as e:
    print(f"Error processing data: {str(e)}")
    # Implement error handling logic
```

## Next Steps

After completing this Shortcut + Notebook implementation:
1. **Compare approaches**: Understand when to use shortcuts vs direct connections
2. **Explore advanced Spark**: Learn about DataFrames, SQL, and optimization
3. **Implement data quality**: Add validation rules and error handling
4. **Version control**: Integrate notebook with Git for collaborative development
5. **Production readiness**: Add monitoring, logging, and error recovery

---

**Duration**: 45-60 minutes  
**Difficulty**: Intermediate  
**Target Persona**: Data Engineers, Technical Developers
