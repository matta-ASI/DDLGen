import os
import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime
import pandas as pd

class SchemaDetector:
    def __init__(self, folder_path: str, output_file: str = "table_ddl.sql"):
        self.folder_path = folder_path
        self.output_file = output_file
        self.detected_schemas = {}
        
    def detect_delimiter(self, file_path: str, sample_lines: int = 5) -> str:
        """Detect the delimiter used in the file"""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            sample = ''.join([file.readline() for _ in range(sample_lines)])
        
        # Test common delimiters
        delimiters = [',', '\t', '|', ';', ':']
        delimiter_counts = {}
        
        for delimiter in delimiters:
            delimiter_counts[delimiter] = sample.count(delimiter)
        
        # Return the delimiter with the highest count
        best_delimiter = max(delimiter_counts, key=delimiter_counts.get)
        return best_delimiter if delimiter_counts[best_delimiter] > 0 else ','
    
    def infer_sql_type(self, values: List[str]) -> str:
        """Infer SQL Server data type from sample values"""
        # Remove None/empty values for analysis
        clean_values = [str(v).strip() for v in values if v is not None and str(v).strip()]
        
        if not clean_values:
            return "NVARCHAR(255)"
        
        # Check if all values are integers
        try:
            int_values = [int(v) for v in clean_values]
            max_val = max(int_values)
            min_val = min(int_values)
            
            if min_val >= -2147483648 and max_val <= 2147483647:
                return "INT"
            else:
                return "BIGINT"
        except ValueError:
            pass
        
        # Check if all values are floats
        try:
            [float(v) for v in clean_values]
            return "DECIMAL(18,4)"  # Default precision for decimals
        except ValueError:
            pass
        
        # Check if all values are dates
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
        ]
        
        date_match_count = 0
        for value in clean_values[:10]:  # Check first 10 values
            for pattern in date_patterns:
                if re.match(pattern, value):
                    date_match_count += 1
                    break
        
        if date_match_count / len(clean_values[:10]) > 0.8:  # 80% match rate
            return "DATE"
        
        # Check for datetime patterns
        datetime_patterns = [
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',
            r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',
        ]
        
        datetime_match_count = 0
        for value in clean_values[:10]:
            for pattern in datetime_patterns:
                if re.match(pattern, value):
                    datetime_match_count += 1
                    break
        
        if datetime_match_count / len(clean_values[:10]) > 0.8:
            return "DATETIME2"
        
        # Check for boolean values
        boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}
        if all(v.lower() in boolean_values for v in clean_values):
            return "BIT"
        
        # Default to NVARCHAR with appropriate length
        max_length = max(len(str(v)) for v in clean_values)
        
        if max_length <= 50:
            return "NVARCHAR(50)"
        elif max_length <= 255:
            return "NVARCHAR(255)"
        elif max_length <= 1000:
            return "NVARCHAR(1000)"
        else:
            return "NVARCHAR(MAX)"
    
    def clean_column_name(self, column_name: str) -> str:
        """Clean column name to be SQL Server compliant"""
        # Remove special characters and replace with underscore
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(column_name))
        
        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = f"col_{clean_name}"
        
        # Ensure it's not empty
        if not clean_name:
            clean_name = "unknown_column"
        
        # Limit length to 128 characters (SQL Server limit)
        return clean_name[:128]
    
    def analyze_file(self, file_path: str, sample_rows: int = 100) -> Dict[str, Any]:
        """Analyze a single file and detect its schema"""
        try:
            delimiter = self.detect_delimiter(file_path)
            
            # Read the file with pandas for easier handling
            df = pd.read_csv(file_path, delimiter=delimiter, nrows=sample_rows, 
                           encoding='utf-8', low_memory=False, na_values=[''])
            
            schema = {
                'file_name': Path(file_path).stem,
                'delimiter': delimiter,
                'columns': [],
                'row_count_sample': len(df)
            }
            
            for column in df.columns:
                clean_col_name = self.clean_column_name(column)
                sql_type = self.infer_sql_type(df[column].dropna().astype(str).tolist())
                
                schema['columns'].append({
                    'original_name': column,
                    'clean_name': clean_col_name,
                    'sql_type': sql_type,
                    'nullable': df[column].isnull().any()
                })
            
            return schema
            
        except Exception as e:
            print(f"Error analyzing file {file_path}: {str(e)}")
            return None
    
    def generate_table_ddl(self, schema: Dict[str, Any]) -> str:
        """Generate T-SQL CREATE TABLE statement from schema"""
        table_name = schema['file_name']
        
        ddl = f"-- Table DDL for file: {table_name}\n"
        ddl += f"-- Detected delimiter: '{schema['delimiter']}'\n"
        ddl += f"-- Sample rows analyzed: {schema['row_count_sample']}\n\n"
        
        ddl += f"CREATE TABLE [{table_name}] (\n"
        
        column_definitions = []
        for col in schema['columns']:
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            column_def = f"    [{col['clean_name']}] {col['sql_type']} {nullable}"
            
            # Add comment about original name if different
            if col['original_name'] != col['clean_name']:
                column_def += f" -- Original: {col['original_name']}"
            
            column_definitions.append(column_def)
        
        ddl += ",\n".join(column_definitions)
        ddl += "\n);\n\n"
        
        # Add BULK INSERT statement as comment
        ddl += f"/*\n-- Sample BULK INSERT statement:\nBULK INSERT [{table_name}]\n"
        ddl += f"FROM 'C:\\path\\to\\your\\file\\{table_name}.txt'\n"
        ddl += f"WITH (\n"
        ddl += f"    FIELDTERMINATOR = '{schema['delimiter']}',\n"
        ddl += f"    ROWTERMINATOR = '\\n',\n"
        ddl += f"    FIRSTROW = 2,  -- Skip header row\n"
        ddl += f"    CODEPAGE = '65001'  -- UTF-8\n"
        ddl += f");\n*/\n\n"
        
        return ddl
    
    def process_folder(self):
        """Process all text files in the folder"""
        print(f"Scanning folder: {self.folder_path}")
        
        # Get all text files
        text_extensions = ['.txt', '.csv', '.tsv', '.dat']
        text_files = []
        
        for ext in text_extensions:
            text_files.extend(Path(self.folder_path).glob(f"*{ext}"))
        
        if not text_files:
            print("No text files found in the specified folder.")
            return
        
        print(f"Found {len(text_files)} text files")
        
        all_ddl = []
        all_ddl.append(f"-- Generated DDL Statements\n")
        all_ddl.append(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        all_ddl.append(f"-- Source folder: {self.folder_path}\n")
        all_ddl.append("-- " + "="*60 + "\n\n")
        
        for file_path in text_files:
            print(f"Analyzing: {file_path.name}")
            
            schema = self.analyze_file(str(file_path))
            if schema:
                self.detected_schemas[schema['file_name']] = schema
                ddl = self.generate_table_ddl(schema)
                all_ddl.append(ddl)
        
        # Write all DDL to output file
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.writelines(all_ddl)
        
        print(f"\nDDL statements written to: {self.output_file}")
        print(f"Total tables generated: {len(self.detected_schemas)}")
    
    def print_summary(self):
        """Print summary of detected schemas"""
        print("\n" + "="*60)
        print("SCHEMA DETECTION SUMMARY")
        print("="*60)
        
        for table_name, schema in self.detected_schemas.items():
            print(f"\nTable: {table_name}")
            print(f"Columns: {len(schema['columns'])}")
            print(f"Delimiter: '{schema['delimiter']}'")
            print("Column Details:")
            for col in schema['columns']:
                print(f"  - {col['clean_name']}: {col['sql_type']}")


# Usage example
if __name__ == "__main__":
    # Configuration
    FOLDER_PATH = r"C:\path\to\your\text\files"  # Update this path
    OUTPUT_FILE = "generated_table_ddl.sql"
    
    # Create detector instance
    detector = SchemaDetector(FOLDER_PATH, OUTPUT_FILE)
    
    # Process all files in the folder
    detector.process_folder()
    
    # Print summary
    detector.print_summary()
    
    print(f"\nTo use this script:")
    print(f"1. Update FOLDER_PATH to point to your text files directory")
    print(f"2. Run the script")
    print(f"3. Check {OUTPUT_FILE} for the generated DDL statements")
