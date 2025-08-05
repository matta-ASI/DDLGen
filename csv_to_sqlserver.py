import os
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.types import NVARCHAR, INTEGER, BIGINT, DECIMAL, DATE, DATETIME, BIT
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any
import logging
from datetime import datetime
import numpy as np

class CSVToSQLServerUploader:
    def __init__(self, server: str, database: str, username: str = None, password: str = None, 
                 trusted_connection: bool = True, driver: str = "ODBC Driver 17 for SQL Server"):
        """
        Initialize the CSV to SQL Server uploader
        
        Args:
            server: SQL Server instance name
            database: Database name
            username: Username (if not using Windows auth)
            password: Password (if not using Windows auth)
            trusted_connection: Use Windows authentication
            driver: ODBC driver name
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.driver = driver
        
        # Setup logging
        self.setup_logging()
        
        # Initialize connection
        self.engine = None
        self.connection_string = self.build_connection_string()
        
        # Track upload results
        self.upload_results = {}
        self.failed_uploads = []
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('csv_upload.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def build_connection_string(self) -> str:
        """Build SQL Server connection string"""
        if self.trusted_connection:
            conn_str = f"mssql+pyodbc://{self.server}/{self.database}?driver={self.driver}&trusted_connection=yes"
        else:
            conn_str = f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver={self.driver}"
        
        return conn_str
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            self.engine = create_engine(self.connection_string)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection successful")
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def clean_table_name(self, filename: str) -> str:
        """Clean filename to create valid SQL Server table name"""
        # Remove file extension
        table_name = Path(filename).stem
        
        # Replace special characters with underscore
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        
        # Ensure it doesn't start with a number
        if table_name and table_name[0].isdigit():
            table_name = f"tbl_{table_name}"
        
        # Ensure it's not empty
        if not table_name:
            table_name = "unknown_table"
        
        # Limit length to 128 characters (SQL Server limit)
        table_name = table_name[:128]
        
        # Remove consecutive underscores
        table_name = re.sub(r'_+', '_', table_name)
        table_name = table_name.strip('_')
        
        return table_name
    
    def clean_column_name(self, column_name: str) -> str:
        """Clean column name to be SQL Server compliant"""
        # Convert to string and strip
        clean_name = str(column_name).strip()
        
        # Replace special characters with underscore
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_name)
        
        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = f"col_{clean_name}"
        
        # Ensure it's not empty
        if not clean_name:
            clean_name = "unknown_column"
        
        # Limit length to 128 characters
        clean_name = clean_name[:128]
        
        # Remove consecutive underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        clean_name = clean_name.strip('_')
        
        return clean_name
    
    def detect_delimiter(self, file_path: str) -> str:
        """Detect the delimiter used in the CSV file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                first_line = file.readline()
                second_line = file.readline()
                sample = first_line + second_line
            
            # Test common delimiters
            delimiters = [',', '\t', '|', ';', ':']
            delimiter_counts = {}
            
            for delimiter in delimiters:
                count = sample.count(delimiter)
                if delimiter in first_line and delimiter in second_line:
                    delimiter_counts[delimiter] = count
                else:
                    delimiter_counts[delimiter] = 0
            
            best_delimiter = max(delimiter_counts, key=delimiter_counts.get)
            return best_delimiter if delimiter_counts[best_delimiter] > 0 else ','
            
        except Exception as e:
            self.logger.warning(f"Could not detect delimiter for {file_path}: {e}")
            return ','
    
    def infer_sql_type(self, series: pd.Series, column_name: str) -> sqlalchemy.types.TypeEngine:
        """Infer SQL Server data type from pandas series"""
        # Remove null values for analysis
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return NVARCHAR(255)
        
        # Check pandas dtype first
        dtype_str = str(series.dtype).lower()
        
        if 'int' in dtype_str:
            max_val = non_null_series.max()
            min_val = non_null_series.min()
            if min_val >= -2147483648 and max_val <= 2147483647:
                return INTEGER()
            else:
                return BIGINT()
        
        elif 'float' in dtype_str:
            return DECIMAL(18, 4)
        
        elif 'bool' in dtype_str:
            return BIT()
        
        elif 'datetime' in dtype_str:
            return DATETIME()
        
        else:
            # For object/string types, analyze the content
            sample_values = non_null_series.astype(str).head(100).tolist()
            
            # Check if all values look like integers
            try:
                int_values = [int(val) for val in sample_values if val.strip()]
                max_val = max(int_values)
                min_val = min(int_values)
                if min_val >= -2147483648 and max_val <= 2147483647:
                    return INTEGER()
                else:
                    return BIGINT()
            except (ValueError, TypeError):
                pass
            
            # Check if all values look like floats
            try:
                [float(val) for val in sample_values if val.strip()]
                return DECIMAL(18, 4)
            except (ValueError, TypeError):
                pass
            
            # Check for date patterns
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',
                r'\d{2}/\d{2}/\d{4}',
                r'\d{2}-\d{2}-\d{4}',
                r'\d{4}/\d{2}/\d{2}',
            ]
            
            date_match_count = 0
            for value in sample_values[:20]:
                for pattern in date_patterns:
                    if re.match(pattern, str(value).strip()):
                        date_match_count += 1
                        break
            
            if date_match_count / min(len(sample_values), 20) > 0.7:
                return DATE()
            
            # Check for boolean values
            boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}
            unique_values = {str(val).lower().strip() for val in sample_values}
            if unique_values.issubset(boolean_values) and len(unique_values) <= 2:
                return BIT()
            
            # Default to NVARCHAR with appropriate length
            max_length = max(len(str(val)) for val in sample_values)
            
            if max_length <= 50:
                return NVARCHAR(50)
            elif max_length <= 255:
                return NVARCHAR(255)
            elif max_length <= 1000:
                return NVARCHAR(1000)
            else:
                return NVARCHAR(max(4000, min(max_length + 100, 8000)))
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """Create SQL Server table based on DataFrame structure"""
        try:
            # Clean column names
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Check for duplicate column names
            seen_columns = set()
            unique_columns = []
            for col in df.columns:
                if col in seen_columns:
                    counter = 1
                    new_col = f"{col}_{counter}"
                    while new_col in seen_columns:
                        counter += 1
                        new_col = f"{col}_{counter}"
                    unique_columns.append(new_col)
                    seen_columns.add(new_col)
                else:
                    unique_columns.append(col)
                    seen_columns.add(col)
            
            df.columns = unique_columns
            
            # Create table schema
            metadata = MetaData()
            columns = []
            
            for col_name in df.columns:
                sql_type = self.infer_sql_type(df[col_name], col_name)
                columns.append(Column(col_name, sql_type, nullable=True))
            
            # Add an identity column as primary key
            columns.insert(0, Column('id', INTEGER(), primary_key=True, autoincrement=True))
            
            table = Table(table_name, metadata, *columns)
            
            # Drop table if exists
            with self.engine.connect() as conn:
                conn.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]"))
                conn.commit()
            
            # Create table
            metadata.create_all(self.engine, tables=[table])
            
            self.logger.info(f"Created table '{table_name}' with {len(df.columns)} columns")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating table '{table_name}': {e}")
            return False
    
    def upload_csv_file(self, file_path: str, table_name: str = None, 
                       chunk_size: int = 10000, if_exists: str = 'replace') -> bool:
        """Upload a single CSV file to SQL Server"""
        try:
            self.logger.info(f"Processing file: {os.path.basename(file_path)}")
            
            # Determine table name
            if not table_name:
                table_name = self.clean_table_name(os.path.basename(file_path))
            
            # Detect delimiter
            delimiter = self.detect_delimiter(file_path)
            
            # Read CSV file
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding='utf-8',
                low_memory=False,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'None', 'none'],
                keep_default_na=True
            )
            
            if df.empty:
                self.logger.warning(f"File {file_path} is empty, skipping")
                return False
            
            self.logger.info(f"Read {len(df)} rows and {len(df.columns)} columns")
            
            # Clean data
            df = self.clean_dataframe(df)
            
            # Create table structure if it doesn't exist or if replacing
            if if_exists == 'replace':
                if not self.create_table_from_dataframe(df, table_name):
                    return False
            
            # Upload data in chunks
            total_rows = len(df)
            rows_uploaded = 0
            
            for chunk_start in range(0, total_rows, chunk_size):
                chunk_end = min(chunk_start + chunk_size, total_rows)
                chunk_df = df.iloc[chunk_start:chunk_end].copy()
                
                # Upload chunk
                chunk_df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                rows_uploaded += len(chunk_df)
                self.logger.info(f"Uploaded {rows_uploaded}/{total_rows} rows to {table_name}")
            
            # Verify upload
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM [{table_name}]"))
                db_row_count = result.scalar()
            
            if db_row_count == total_rows:
                self.logger.info(f"Successfully uploaded {total_rows} rows to table '{table_name}'")
                self.upload_results[file_path] = {
                    'table_name': table_name,
                    'rows_uploaded': total_rows,
                    'columns': len(df.columns),
                    'status': 'success'
                }
                return True
            else:
                self.logger.error(f"Row count mismatch for {table_name}: expected {total_rows}, got {db_row_count}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error uploading {file_path}: {e}")
            self.failed_uploads.append((file_path, str(e)))
            return False
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame before uploading"""
        # Replace inf and -inf with NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # Convert object columns that look like numbers
        for col in df.select_dtypes(include=['object']).columns:
            # Try to convert to numeric
            numeric_series = pd.to_numeric(df[col], errors='coerce')
            if not numeric_series.isna().all():  # If at least some values converted
                # If more than 80% of non-null values converted successfully
                non_null_original = df[col].dropna()
                non_null_converted = numeric_series.dropna()
                if len(non_null_converted) / len(non_null_original) > 0.8:
                    df[col] = numeric_series
        
        return df
    
    def upload_multiple_csv_files(self, folder_path: str, file_pattern: str = "*.csv",
                                 chunk_size: int = 10000, max_files: int = None) -> Dict[str, Any]:
        """Upload multiple CSV files from a folder"""
        self.logger.info(f"Starting bulk upload from folder: {folder_path}")
        
        # Get all CSV files
        csv_files = list(Path(folder_path).glob(file_pattern))
        
        if max_files:
            csv_files = csv_files[:max_files]
        
        self.logger.info(f"Found {len(csv_files)} files to upload")
        
        # Upload each file
        successful_uploads = 0
        for i, file_path in enumerate(csv_files, 1):
            self.logger.info(f"Processing file {i}/{len(csv_files)}: {file_path.name}")
            
            if self.upload_csv_file(str(file_path), chunk_size=chunk_size):
                successful_uploads += 1
            
            # Progress update
            if i % 10 == 0:
                self.logger.info(f"Progress: {i}/{len(csv_files)} files processed")
        
        # Generate summary
        summary = {
            'total_files': len(csv_files),
            'successful_uploads': successful_uploads,
            'failed_uploads': len(self.failed_uploads),
            'success_rate': (successful_uploads / len(csv_files)) * 100 if csv_files else 0,
            'upload_results': self.upload_results,
            'failed_files': self.failed_uploads
        }
        
        self.generate_upload_report(summary)
        
        return summary
    
    def generate_upload_report(self, summary: Dict[str, Any]):
        """Generate a detailed upload report"""
        report_path = 'csv_upload_report.txt'
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"CSV to SQL Server Upload Report\n")
            f.write(f"{'='*50}\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database: {self.server}.{self.database}\n\n")
            
            f.write(f"Upload Summary:\n")
            f.write(f"  Total files processed: {summary['total_files']}\n")
            f.write(f"  Successful uploads: {summary['successful_uploads']}\n")
            f.write(f"  Failed uploads: {summary['failed_uploads']}\n")
            f.write(f"  Success rate: {summary['success_rate']:.1f}%\n\n")
            
            if summary['upload_results']:
                f.write(f"Successful Uploads:\n")
                f.write(f"{'-'*50}\n")
                for file_path, result in summary['upload_results'].items():
                    f.write(f"File: {os.path.basename(file_path)}\n")
                    f.write(f"  Table: {result['table_name']}\n")
                    f.write(f"  Rows: {result['rows_uploaded']:,}\n")
                    f.write(f"  Columns: {result['columns']}\n\n")
            
            if summary['failed_files']:
                f.write(f"Failed Uploads:\n")
                f.write(f"{'-'*50}\n")
                for file_path, error in summary['failed_files']:
                    f.write(f"File: {os.path.basename(file_path)}\n")
                    f.write(f"  Error: {error}\n\n")
        
        self.logger.info(f"Upload report generated: {report_path}")


# Usage example
if __name__ == "__main__":
    # Database configuration
    DB_CONFIG = {
        'server': 'localhost\\SQLEXPRESS',  # Update with your server
        'database': 'YourDatabase',         # Update with your database
        'trusted_connection': True,         # Use Windows authentication
        # If not using Windows auth, set these:
        # 'username': 'your_username',
        # 'password': 'your_password',
        # 'trusted_connection': False
    }
    
    # File configuration
    CSV_FOLDER = r"C:\path\to\your\50\csv\files"  # Update this path
    
    # Create uploader instance
    uploader = CSVToSQLServerUploader(**DB_CONFIG)
    
    # Test connection
    if uploader.test_connection():
        print("Database connection successful!")
        
        # Upload all CSV files
        summary = uploader.upload_multiple_csv_files(
            folder_path=CSV_FOLDER,
            file_pattern="*.csv",        # Pattern to match files
            chunk_size=5000,             # Rows per batch
            max_files=None               # None = no limit, or set a number
        )
        
        # Print summary
        print(f"\nUpload completed!")
        print(f"Successful: {summary['successful_uploads']}/{summary['total_files']} files")
        print(f"Success rate: {summary['success_rate']:.1f}%")
        
        if summary['failed_uploads'] > 0:
            print(f"\nFailed files:")
            for file_path, error in summary['failed_files']:
                print(f"  - {os.path.basename(file_path)}: {error}")
        
        print(f"\nCheck 'csv_upload_report.txt' for detailed results")
        print(f"Check 'csv_upload.log' for processing details")
    
    else:
        print("Database connection failed. Please check your configuration.")
