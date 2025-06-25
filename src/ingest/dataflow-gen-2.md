# DataFlow Gen2 Bronze Ingestion - PostgreSQL Drivers

## Objective

Learn to ingest JSON data from Azure Data Lake Storage into Delta tables using DataFlow Gen2's visual interface and Power Query transformations. This lab demonstrates how to process JSONL (JSON Lines) format data and create a robust bronze layer table with proper data types and audit columns.

## Prerequisites

- Microsoft Fabric workspace with Data Engineering experience enabled
- Lakehouse created: `lh_uber_eats` with public schema preview enabled
- Contributor or Admin role in the workspace

## Architecture Overview

```
Azure Data Lake Storage Gen2 → DataFlow Gen2 → Lakehouse (Bronze Layer)
│                              │               │
├── JSONL files                ├── Power Query ├── Delta table
├── postgres/drivers/          ├── M Language  ├── df2_bronze_postgres_drivers
└── Raw driver data            └── Visual UI   └── Structured + Audit columns
```

## Data Source Configuration

- **Name**: `df2_bronze_postgres_drivers`
- **Data Source**: Azure Data Lake Storage Gen2
- **Source URL**: `https://pythianstg.dfs.core.windows.net/owshq-shadow-traffic-uber-eats/postgres/drivers/`
- **Destination Table**: `df2_bronze_postgres_drivers`
- **Data Format**: JSON Lines (JSONL) - one JSON object per line

## Step-by-Step Implementation

### Step 1: Create DataFlow Gen2

1. **Navigate to Data Engineering Experience**
   - Click the experience switcher (bottom left)
   - Select **Data Engineering**

2. **Create New DataFlow**
   - Click **+ New** → **Dataflow Gen2**
   - **Name**: `df2_bronze_postgres_drivers`
   - Click **Create**

### Step 2: Configure Data Source

3. **Connect to Azure Data Lake**
   - Click **Get data**
   - Search for and select **Azure Data Lake Storage Gen2**
   - **Data Lake URL**: `https://pythianstg.dfs.core.windows.net/owshq-shadow-traffic-uber-eats/postgres/drivers/`
   - **Authentication**: Anonymous
   - Click **Next**

4. **Select Data Files**
   - Navigate to the drivers folder
   - Select all JSON files in the directory
   - Click **Create** to proceed

### Step 3: Add Custom Transformations

5. **Transform Data Using Custom Columns**

   Add these custom columns in sequence using **Add Column** → **Custom Column**:

   **Column 1: FileText**
   ```m
   Text.FromBinary([Content])
   ```
   *Purpose: Convert binary file content to readable text*

   **Column 2: Lines**
   ```m
   Lines.FromText([FileText])
   ```
   *Purpose: Split text into individual lines for JSONL processing*

   **Column 3: ParsedJson**
   ```m
   Json.Document([Lines])
   ```
   *Purpose: Parse each line as a JSON document*

   **Column 4: load_dts**
   ```m
   DateTimeZone.UtcNow()
   ```
   *Purpose: Add audit timestamp for data lineage*

   **Column 5: record_source**
   ```m
   "postgres.drivers"
   ```
   *Purpose: Add source system identifier for data governance*

6. **Expand Data Structure**
   - Expand the **Lines** column to create separate rows
   - Expand the **ParsedJson** column to create individual columns
   - Select all available JSON fields: country, date_birth, city, vehicle_year, phone_number, license_number, vehicle_make, uuid, vehicle_model, driver_id, last_name, first_name, vehicle_type, dt_current_timestamp

### Step 4: Select Final Columns

7. **Choose Columns for Output**
   - Use **Choose columns** to select only the final business and audit columns:
     - country, date_birth, city, vehicle_year, phone_number
     - license_number, vehicle_make, uuid, vehicle_model, driver_id
     - last_name, first_name, vehicle_type, dt_current_timestamp
     - load_dts, record_source

### Step 5: Fix Data Types

8. **Transform Column Types**
   - Select all columns and use **Transform** → **Detect data types**
   - Or manually set types for key columns:
     - driver_id: Whole Number
     - vehicle_year: Whole Number
     - load_dts: Date/Time/Timezone
     - All text fields: Text

### Step 6: Configure Destination

9. **Set Data Destination**
   - Click **Add data destination**
   - Select **Lakehouse**
   - Choose your `lh_uber_eats` lakehouse
   - **Table name**: `df2_bronze_postgres_drivers`
   - **Update method**: Replace

10. **Save and Publish**
    - Click **Save** in the top ribbon
    - Click **Publish** to make the DataFlow available

### Step 7: Execute and Validate

11. **Run the DataFlow**
    - Click **Refresh now** to execute
    - Monitor execution in the refresh history

12. **Verify Results**
    - Navigate to your `lh_uber_eats` lakehouse
    - Check the **Tables** section
    - Preview `df2_bronze_postgres_drivers` table

## Complete Power Query M Code

For advanced users or troubleshooting, here's the complete M language code:

```m
let
    Source = AzureStorage.DataLake("https://pythianstg.dfs.core.windows.net/owshq-shadow-traffic-uber-eats/postgres/drivers/"),
    #"Added custom" = Table.AddColumn(Source, "FileText", each Text.FromBinary([Content])),
    #"Added custom 1" = Table.AddColumn(#"Added custom", "Lines", each Lines.FromText([FileText])),
    #"Expanded Lines" = Table.ExpandListColumn(#"Added custom 1", "Lines"),
    #"Added custom 2" = Table.AddColumn(#"Expanded Lines", "ParsedJson", each Json.Document([Lines])),
    #"Expanded ParsedJson" = Table.ExpandRecordColumn(#"Added custom 2", "ParsedJson", 
        {"country", "date_birth", "city", "vehicle_year", "phone_number", "license_number", 
         "vehicle_make", "uuid", "vehicle_model", "driver_id", "last_name", "first_name", 
         "vehicle_type", "dt_current_timestamp"}, 
        {"country", "date_birth", "city", "vehicle_year", "phone_number", "license_number", 
         "vehicle_make", "uuid", "vehicle_model", "driver_id", "last_name", "first_name", 
         "vehicle_type", "dt_current_timestamp"}),
    #"Added custom 3" = Table.AddColumn(#"Expanded ParsedJson", "load_dts", each DateTimeZone.UtcNow()),
    #"Added custom 4" = Table.AddColumn(#"Added custom 3", "record_source", each "postgres.drivers"),
    #"Choose columns" = Table.SelectColumns(#"Added custom 4", 
        {"country", "date_birth", "city", "vehicle_year", "phone_number", "license_number", 
         "vehicle_make", "uuid", "vehicle_model", "driver_id", "last_name", "first_name", 
         "vehicle_type", "dt_current_timestamp", "load_dts", "record_source"}),
    
    // Fix data types for Delta table compatibility
    #"Changed Types" = Table.TransformColumnTypes(#"Choose columns",{
        {"driver_id", Int64.Type},
        {"first_name", type text},
        {"last_name", type text},
        {"country", type text},
        {"city", type text},
        {"phone_number", type text},
        {"license_number", type text},
        {"vehicle_make", type text},
        {"vehicle_model", type text},
        {"vehicle_type", type text},
        {"uuid", type text},
        {"date_birth", type text},
        {"vehicle_year", Int64.Type},
        {"dt_current_timestamp", type text},
        {"load_dts", type datetimezone},
        {"record_source", type text}
    })
in
    #"Changed Types"
```

## Expected Output Schema

Your final Delta table should contain:

| Column | Type | Description |
|--------|------|-------------|
| driver_id | integer | Unique driver identifier |
| first_name | string | Driver's first name |
| last_name | string | Driver's last name |
| phone_number | string | Contact number |
| license_number | string | Driver's license number |
| vehicle_type | string | Vehicle category (Car, Motorcycle, etc.) |
| vehicle_make | string | Vehicle manufacturer |
| vehicle_model | string | Vehicle model |
| vehicle_year | integer | Year of manufacture |
| country | string | Driver's country |
| city | string | Driver's city |
| date_birth | string | Date of birth |
| uuid | string | Universal unique identifier |
| dt_current_timestamp | string | Source system timestamp |
| **load_dts** | datetime | **Audit: When data was loaded** |
| **record_source** | string | **Audit: Source system identifier** |

## Key Learning Points

### Power Query Transformation Pattern
1. **Binary to Text**: Convert file content to readable format
2. **Line Splitting**: Handle JSONL format properly
3. **JSON Parsing**: Extract structured data from JSON
4. **Column Expansion**: Create tabular structure from nested data
5. **Data Type Conversion**: Ensure Delta table compatibility

### Bronze Layer Best Practices
- **Preserve Raw Structure**: Minimal transformation, maximum data preservation
- **Add Audit Columns**: load_dts and record_source for data lineage
- **Handle Multiple Files**: Process entire folders with pattern matching
- **Schema Flexibility**: JSON parsing allows for evolving data structures

### DataFlow Gen2 Advantages
- **Visual Development**: Drag-and-drop interface familiar to Power BI users
- **Real-time Preview**: See transformation results immediately
- **Auto-generated M Code**: Learn Power Query language through UI actions
- **Enterprise Integration**: Native Fabric and OneLake connectivity

## Troubleshooting Common Issues

**Issue**: Data type validation errors
- **Solution**: Ensure all columns have explicit types before writing to Delta

**Issue**: JSON parsing failures
- **Solution**: Verify JSONL format (one JSON object per line)

**Issue**: Missing columns after expansion
- **Solution**: Check JSON structure consistency across files

**Issue**: Performance issues with large files
- **Solution**: Consider file size limits and processing patterns

## Validation Checklist

- DataFlow Gen2 created with correct name
- Data source connection established
- All custom columns added successfully
- JSON structure expanded properly
- Data types converted correctly
- Destination configured to lakehouse
- Table created successfully in bronze layer
- Data preview shows expected structure
- Audit columns populated correctly

## Next Steps

After completing this DataFlow Gen2 implementation:
1. **Compare with other ingestion methods** (Shortcut + Notebook, Pipeline)
2. **Understand the trade-offs** between visual and code-based approaches
3. **Learn when to use** DataFlow Gen2 vs other Fabric tools
4. **Explore silver layer transformations** building on this bronze foundation

---

**Duration**: 30-45 minutes  
**Difficulty**: Beginner to Intermediate  
**Target Persona**: Business Analysts, Power BI Developers, Data Engineers
