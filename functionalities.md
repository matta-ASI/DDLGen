I'll help you create a Python script that analyzes text files to detect their schema and generates T-SQL DDL statements. This script will handle CSV/delimited files and create appropriate table structures.This Python script provides a comprehensive solution for analyzing text files and generating T-SQL DDL statements. Here's what it does:

## Key Features:

1. **Auto-detects delimiters** - Identifies whether files use commas, tabs, pipes, semicolons, etc.

2. **Infers data types** - Analyzes sample data to determine appropriate SQL Server data types:
   - INT/BIGINT for integers
   - DECIMAL for floating-point numbers
   - DATE/DATETIME2 for date/time values
   - BIT for boolean values
   - NVARCHAR with appropriate lengths for text

3. **Cleans column names** - Makes column names SQL Server compliant by removing special characters

4. **Handles multiple file formats** - Works with .txt, .csv, .tsv, and .dat files

5. **Generates complete DDL** - Creates CREATE TABLE statements with proper data types and nullability

6. **Includes BULK INSERT templates** - Provides commented examples for loading data

## To use the script:

1. **Install required packages** (if not already installed):
   ```bash
   pip install pandas
   ```

2. **Update the configuration** at the bottom of the script:
   ```python
   FOLDER_PATH = r"C:\path\to\your\text\files"  # Your folder path
   OUTPUT_FILE = "generated_table_ddl.sql"      # Output file name
   ```

3. **Run the script** - It will analyze all text files and create a single SQL file with all DDL statements

## The output file will contain:

- CREATE TABLE statements for each file
- Column definitions with appropriate data types
- Comments showing original column names if they were modified
- Sample BULK INSERT statements for each table
- Summary information about delimiters and row counts

The script is robust and handles various edge cases like special characters in column names, different date formats, and encoding issues. You can modify the `sample_rows` parameter in `analyze_file()` to analyze more or fewer rows for type detection.
