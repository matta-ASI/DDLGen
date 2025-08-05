This Python script automatically uploads your 50 CSV files to SQL Server as separate tables. This will handle the entire process including schema detection, table creation, and data insertion.This comprehensive script will upload all your CSV files to SQL Server as separate tables. Here's what you need to do:

## Prerequisites:

### 1. **Install Required Packages**
```bash
pip install pandas pyodbc sqlalchemy
```

### 2. **Install SQL Server ODBC Driver**
Download and install "ODBC Driver 17 for SQL Server" from Microsoft's website.

## Configuration:

### 1. **Update Database Settings**
```python
DB_CONFIG = {
    'server': 'your_server_name',      # e.g., 'localhost\SQLEXPRESS'
    'database': 'YourDatabase',        # Your target database
    'trusted_connection': True,        # Windows authentication
}

# OR for SQL Server authentication:
DB_CONFIG = {
    'server': 'your_server_name',
    'database': 'YourDatabase',
    'username': 'your_username',
    'password': 'your_password',
    'trusted_connection': False,
}
```

### 2. **Set Your CSV Folder Path**
```python
CSV_FOLDER = r"C:\path\to\your\50\csv\files"
```

## Key Features:

### **Automatic Processing**
- **Schema Detection**: Automatically detects delimiters and data types
- **Table Creation**: Creates tables with appropriate column types
- **Data Type Inference**: Maps CSV data to SQL Server types (INT, DECIMAL, NVARCHAR, DATE, BIT)
- **Name Cleaning**: Converts file/column names to SQL Server-compliant identifiers

### **Robust Upload Process**
- **Chunked Upload**: Processes large files in batches (configurable chunk size)
- **Error Handling**: Continues processing other files if one fails
- **Data Validation**: Verifies row counts after upload
- **Progress Tracking**: Shows real-time progress for large datasets

### **Smart Data Handling**
- **Type Conversion**: Automatically converts string numbers to numeric types
- **Null Handling**: Properly handles various null representations
- **Duplicate Columns**: Handles duplicate column names automatically
- **Primary Keys**: Adds auto-increment ID column to each table

## Output:

The script creates:
1. **50 SQL Server tables** (one per CSV file)
2. **Upload report** (`csv_upload_report.txt`) with detailed results
3. **Processing log** (`csv_upload.log`) with all operations

## Example Output:
```
Upload completed!
Successful: 48/50 files
Success rate: 96.0%

Tables created:
- sales_data_2023 (15,243 rows, 8 columns)
- customer_info (8,901 rows, 12 columns)
- product_catalog (2,156 rows, 6 columns)
...
```

## Alternative Approaches:

### **Option 1: SQL Server Import Wizard**
If you prefer a GUI approach:
1. Open SQL Server Management Studio (SSMS)
2. Right-click database → Tasks → Import Data
3. Use "Flat File Source" for each CSV

### **Option 2: BULK INSERT (for very large files)**
```sql
BULK INSERT YourTable
FROM 'C:\path\to\file.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);
```

### **Option 3: Azure Data Factory**
For enterprise scenarios with complex transformations.

## Customization Options:

- **Batch Size**: Adjust `chunk_size` for memory optimization
- **Data Types**: Modify `infer_sql_type()` for custom type mapping
- **Table Names**: Customize `clean_table_name()` for naming conventions
- **Error Handling**: Configure retry logic or skip problematic files

The script handles the most common scenarios automatically, but you can customize it based on your specific requirements. Would you like me to modify any part of the script for your particular use case?
