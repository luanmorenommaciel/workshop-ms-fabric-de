# Pipeline Bronze Ingestion - Kafka Status

## Objective

Learn to use Microsoft Fabric Data Factory pipelines for enterprise-grade data orchestration and ingestion. This lab demonstrates how to build production-ready data pipelines using visual design tools, combining the reliability of Azure Data Factory with the power of OneLake storage.

## Prerequisites

- Microsoft Fabric workspace with Data Engineering experience enabled
- Lakehouse created: `lh_uber_eats` with public schema preview enabled
- Completion of DataFlow Gen2 and Shortcut + Notebook labs for comparison
- Understanding of basic ETL/ELT concepts

## Architecture Overview

```
Azure Data Lake Storage Gen2 → Fabric Pipeline → OneLake → Delta Table
│                              │                │        │
├── JSON files                 ├── Copy Activity├── Auto ├── Delta format
├── kafka/status/              ├── Visual Design├── Conv ├── pl_bronze_kafka_status
└── Order status events        └── Enterprise   └── ert   └── Bronze layer
                                  Orchestration
```

## Configuration Details

- **Pipeline Name**: `pl_ingest_uber_eats_files`
- **Connection URL**: `https://pythianstg.dfs.core.windows.net/`
- **Source Path**: `kafka/status/`
- **Destination Table**: `pl_bronze_kafka_status`
- **Activity Name**: `copy_kafka_status_one_lake`
- **Data Format**: JSON files with order status events

## Step-by-Step Implementation

### Step 1: Create Data Factory Pipeline

1. **Navigate to Data Engineering Experience**
   - Click the experience switcher (bottom left)
   - Select **Data Engineering**

2. **Create New Pipeline**
   - Click **+ New** → **Data pipeline**
   - **Name**: `pl_ingest_uber_eats_files`
   - Click **Create**

### Step 2: Configure Source Connection

3. **Add Copy Activity**
   - From the **Activities** pane, drag **Copy data** onto the canvas
   - **Activity name**: `copy_kafka_status_one_lake`
   - Click on the activity to configure

4. **Configure Source Settings**
   - Click on the **Source** tab
   - Click **+ New** to create new dataset
   - Select **Azure Data Lake Storage Gen2**
   - **Connection**: Create new connection
     - **Account URL**: `https://pythianstg.dfs.core.windows.net/`
     - **Authentication**: Anonymous
     - **Name**: `adls_uber_eats_source`
   - **File path**: 
     - **Container**: `owshq-shadow-traffic-uber-eats`
     - **Directory**: `kafka/status`
     - **File**: `*` (wildcard for all files)
   - **File format**: JSON

### Step 3: Configure Destination Settings

5. **Set Up Lakehouse Destination**
   - Click on the **Destination** tab
   - Click **+ New** to create new dataset
   - Select **Lakehouse**
   - Choose your `lh_uber_eats` lakehouse
   - **Table name**: `pl_bronze_kafka_status`
   - **Write behavior**: Overwrite

### Step 4: Configure Copy Activity Settings

6. **Mapping and Schema**
   - Click on the **Mapping** tab
   - Enable **Auto mapping** to automatically map source to destination
   - Review the column mappings

7. **Advanced Settings**
   - Click on the **Settings** tab
   - **File format**: JSON
   - **Compression**: Auto-detect
   - **Error handling**: Skip incompatible rows

### Step 5: Add Data Transformation (Optional)

8. **Add Audit Columns via Script**
   - Add a **Notebook** activity after the Copy activity
   - Create a simple transformation notebook:

   ```python
   # Add audit columns to the copied data
   from pyspark.sql.functions import current_timestamp, lit
   
   # Read the data that was just copied
   df_status = spark.table("pl_bronze_kafka_status")
   
   # Add audit columns
   df_status_enriched = df_status.withColumn("load_dts", current_timestamp()) \
                               .withColumn("record_source", lit("kafka.status")) \
                               .withColumn("ingest_method", lit("pipeline"))
   
   # Overwrite the table with enriched data
   df_status_enriched.write.format("delta").mode("overwrite").saveAsTable("pl_bronze_kafka_status")
   
   print(f"✅ Processed {df_status_enriched.count()} records")
   ```

### Step 6: Configure Pipeline Settings

9. **Add Pipeline Parameters** (Optional)
   - Click on **Parameters** in the pipeline canvas
   - Add parameters for source path and table name for reusability:
     - `source_path`: Default value `kafka/status`
     - `table_name`: Default value `pl_bronze_kafka_status`

10. **Configure Triggers**
    - Click **Add trigger** → **New/Edit**
    - **Trigger type**: Schedule
    - **Recurrence**: Daily at specified time
    - **Start date**: Current date
    - Click **OK**

### Step 7: Validate and Test Pipeline

11. **Validate Pipeline**
    - Click **Validate** in the top toolbar
    - Review and fix any validation errors
    - Ensure all connections and mappings are correct

12. **Debug Pipeline**
    - Click **Debug** to test the pipeline execution
    - Monitor the progress in the **Output** tab
    - Check for any errors or warnings

### Step 8: Publish and Execute

13. **Publish Pipeline**
    - Click **Publish all** to save the pipeline
    - Wait for successful publication

14. **Trigger Manual Run**
    - Click **Add trigger** → **Trigger now**
    - Monitor execution in the **Monitor** tab
    - Verify successful completion

### Step 9: Verify Results

15. **Check Lakehouse Table**
    - Navigate to your `lh_uber_eats` lakehouse
    - Verify `pl_bronze_kafka_status` table exists
    - Preview data to confirm successful ingestion

## Pipeline Components Explained

### Copy Activity Configuration

```json
{
  "name": "copy_kafka_status_one_lake",
  "type": "Copy",
  "source": {
    "type": "JsonSource",
    "storeSettings": {
      "type": "AzureDataLakeStoreReadSettings",
      "recursive": true,
      "wildcardFileName": "*"
    }
  },
  "sink": {
    "type": "LakehouseTableSink",
    "tableOption": "autoCreate"
  },
  "enableStaging": false
}
```

### Complete Pipeline Structure

```
pl_ingest_uber_eats_files
├── copy_kafka_status_one_lake (Copy Activity)
│   ├── Source: ADLS Gen2 (kafka/status/*)
│   ├── Destination: Lakehouse (pl_bronze_kafka_status)
│   └── Mapping: Auto-mapping enabled
├── enrich_audit_columns (Notebook Activity) [Optional]
│   └── Add load_dts, record_source, ingest_method
└── Pipeline Parameters
    ├── source_path: kafka/status
    └── table_name: pl_bronze_kafka_status
```

## Expected Output Schema

Your final Delta table should contain:

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Reference to the order |
| status | string | Current order status |
| timestamp | timestamp | When status was updated |
| driver_id | string | Driver handling the order |
| location_lat | decimal | Current latitude |
| location_lon | decimal | Current longitude |
| estimated_delivery | timestamp | Estimated delivery time |
| status_details | string | Additional status information |
| **load_dts** | timestamp | **Audit: When data was loaded** |
| **record_source** | string | **Audit: Source system (kafka.status)** |
| **ingest_method** | string | **Audit: How data was ingested (pipeline)** |

## Key Concepts Demonstrated

### Enterprise Data Factory Features
- **Visual Pipeline Design**: Drag-and-drop activity composition
- **Built-in Connectors**: 100+ pre-built data source connectors
- **Error Handling**: Comprehensive error handling and retry logi
