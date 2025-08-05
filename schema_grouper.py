import os
import pandas as pd
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
import re
from datetime import datetime
import logging

class SchemaGrouper:
    def __init__(self, folder_path: str, output_folder: str = "combined_files", 
                 sample_rows: int = 10, file_extensions: List[str] = None):
        """
        Initialize the Schema Grouper
        
        Args:
            folder_path: Path to folder containing the files
            output_folder: Path where combined files will be saved
            sample_rows: Number of rows to sample for schema detection
            file_extensions: List of file extensions to process
        """
        self.folder_path = folder_path
        self.output_folder = output_folder
        self.sample_rows = sample_rows
        self.file_extensions = file_extensions or ['.txt', '.csv', '.tsv', '.dat']
        
        # Create output folder if it doesn't exist
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
        # Storage for schemas and file groupings
        self.schema_groups = defaultdict(list)  # schema_hash -> list of file paths
        self.schema_details = {}  # schema_hash -> schema details
        self.failed_files = []
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = os.path.join(self.output_folder, 'schema_grouping.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def detect_delimiter(self, file_path: str) -> str:
        """Detect the delimiter used in the file"""
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
                # Ensure delimiter appears in both lines (consistency check)
                if delimiter in first_line and delimiter in second_line:
                    delimiter_counts[delimiter] = count
                else:
                    delimiter_counts[delimiter] = 0
            
            # Return the delimiter with the highest count
            best_delimiter = max(delimiter_counts, key=delimiter_counts.get)
            return best_delimiter if delimiter_counts[best_delimiter] > 0 else ','
            
        except Exception as e:
            self.logger.warning(f"Could not detect delimiter for {file_path}: {e}")
            return ','
    
    def normalize_column_name(self, col_name: str) -> str:
        """Normalize column names for comparison"""
        # Convert to lowercase, remove extra spaces, replace special chars
        normalized = str(col_name).lower().strip()
        normalized = re.sub(r'[^\w\s]', '', normalized)  # Remove special chars
        normalized = re.sub(r'\s+', '_', normalized)     # Replace spaces with underscore
        return normalized
    
    def get_file_schema(self, file_path: str) -> Tuple[str, Dict]:
        """
        Extract schema information from a file
        
        Returns:
            Tuple of (schema_hash, schema_details)
        """
        try:
            delimiter = self.detect_delimiter(file_path)
            
            # Read just the header and a few sample rows
            df_sample = pd.read_csv(
                file_path, 
                delimiter=delimiter, 
                nrows=self.sample_rows,
                encoding='utf-8',
                low_memory=False,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a']
            )
            
            if df_sample.empty:
                raise ValueError("File is empty or has no readable data")
            
            # Get column information
            columns = list(df_sample.columns)
            normalized_columns = [self.normalize_column_name(col) for col in columns]
            
            # Create schema signature - order matters for exact matching
            schema_signature = {
                'column_count': len(columns),
                'normalized_columns': tuple(sorted(normalized_columns)),  # Sort for consistency
                'delimiter': delimiter,
                'dtypes': tuple(sorted([str(dtype) for dtype in df_sample.dtypes]))
            }
            
            # Create hash of the schema
            schema_string = f"{schema_signature['column_count']}|{schema_signature['normalized_columns']}|{schema_signature['delimiter']}"
            schema_hash = hashlib.md5(schema_string.encode()).hexdigest()
            
            schema_details = {
                'original_columns': columns,
                'normalized_columns': normalized_columns,
                'delimiter': delimiter,
                'column_count': len(columns),
                'sample_dtypes': dict(df_sample.dtypes.astype(str)),
                'schema_hash': schema_hash
            }
            
            return schema_hash, schema_details
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            self.failed_files.append((file_path, str(e)))
            return None, None
    
    def scan_files(self) -> Dict[str, List[str]]:
        """
        Scan all files in the folder and group them by schema
        
        Returns:
            Dictionary mapping schema_hash to list of file paths
        """
        self.logger.info(f"Scanning folder: {self.folder_path}")
        
        # Get all files with specified extensions
        all_files = []
        for ext in self.file_extensions:
            pattern = f"**/*{ext}"
            files = list(Path(self.folder_path).glob(pattern))
            all_files.extend(files)
        
        self.logger.info(f"Found {len(all_files)} files to process")
        
        # Process each file
        processed_count = 0
        for file_path in all_files:
            if processed_count % 100 == 0:
                self.logger.info(f"Processed {processed_count}/{len(all_files)} files")
            
            schema_hash, schema_details = self.get_file_schema(str(file_path))
            
            if schema_hash:
                self.schema_groups[schema_hash].append(str(file_path))
                
                # Store schema details (use first file's details as representative)
                if schema_hash not in self.schema_details:
                    self.schema_details[schema_hash] = schema_details
            
            processed_count += 1
        
        self.logger.info(f"Completed scanning. Found {len(self.schema_groups)} unique schemas")
        self.logger.info(f"Failed to process {len(self.failed_files)} files")
        
        return dict(self.schema_groups)
    
    def combine_files_by_schema(self, min_files_per_group: int = 2, 
                               max_files_per_group: int = None,
                               output_format: str = 'csv') -> Dict[str, str]:
        """
        Combine files with the same schema into single files
        
        Args:
            min_files_per_group: Minimum number of files required to create a combined file
            max_files_per_group: Maximum number of files to combine (None for no limit)
            output_format: Output format ('csv', 'parquet', 'excel')
        
        Returns:
            Dictionary mapping schema_hash to output file path
        """
        combined_files = {}
        
        for schema_hash, file_list in self.schema_groups.items():
            if len(file_list) < min_files_per_group:
                self.logger.info(f"Skipping schema {schema_hash[:8]}... - only {len(file_list)} files")
                continue
            
            self.logger.info(f"Combining {len(file_list)} files for schema {schema_hash[:8]}...")
            
            # Limit files if specified
            files_to_process = file_list[:max_files_per_group] if max_files_per_group else file_list
            
            try:
                combined_df = self.combine_files(files_to_process, schema_hash)
                
                if combined_df is not None and not combined_df.empty:
                    # Generate output filename
                    schema_details = self.schema_details[schema_hash]
                    file_suffix = f"schema_{schema_hash[:8]}_{len(files_to_process)}files"
                    
                    # Save combined file
                    if output_format.lower() == 'csv':
                        output_path = os.path.join(self.output_folder, f"{file_suffix}.csv")
                        combined_df.to_csv(output_path, index=False)
                    elif output_format.lower() == 'parquet':
                        output_path = os.path.join(self.output_folder, f"{file_suffix}.parquet")
                        combined_df.to_parquet(output_path, index=False)
                    elif output_format.lower() == 'excel':
                        output_path = os.path.join(self.output_folder, f"{file_suffix}.xlsx")
                        combined_df.to_excel(output_path, index=False)
                    else:
                        raise ValueError(f"Unsupported output format: {output_format}")
                    
                    combined_files[schema_hash] = output_path
                    
                    # Create metadata file
                    self.create_metadata_file(schema_hash, files_to_process, output_path)
                    
                    self.logger.info(f"Created combined file: {output_path}")
                    self.logger.info(f"Combined {len(files_to_process)} files into {len(combined_df)} rows")
                
            except Exception as e:
                self.logger.error(f"Error combining files for schema {schema_hash}: {e}")
        
        return combined_files
    
    def combine_files(self, file_list: List[str], schema_hash: str) -> pd.DataFrame:
        """Combine multiple files with the same schema into a single DataFrame"""
        dataframes = []
        schema_details = self.schema_details[schema_hash]
        delimiter = schema_details['delimiter']
        
        for file_path in file_list:
            try:
                # Read the full file
                df = pd.read_csv(
                    file_path,
                    delimiter=delimiter,
                    encoding='utf-8',
                    low_memory=False,
                    na_values=['', 'NULL', 'null', 'N/A', 'n/a']
                )
                
                # Add source file column
                df['_source_file'] = os.path.basename(file_path)
                df['_source_path'] = file_path
                
                dataframes.append(df)
                
            except Exception as e:
                self.logger.warning(f"Could not read file {file_path}: {e}")
                continue
        
        if not dataframes:
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True, sort=False)
        
        return combined_df
    
    def create_metadata_file(self, schema_hash: str, source_files: List[str], output_path: str):
        """Create a metadata file with information about the combined file"""
        metadata_path = output_path.replace('.csv', '_metadata.txt').replace('.parquet', '_metadata.txt').replace('.xlsx', '_metadata.txt')
        
        schema_details = self.schema_details[schema_hash]
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            f.write(f"Combined File Metadata\n")
            f.write(f"{'='*50}\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Schema Hash: {schema_hash}\n")
            f.write(f"Output File: {os.path.basename(output_path)}\n")
            f.write(f"Number of source files: {len(source_files)}\n")
            f.write(f"Delimiter: '{schema_details['delimiter']}'\n")
            f.write(f"Number of columns: {schema_details['column_count']}\n\n")
            
            f.write("Columns:\n")
            for i, col in enumerate(schema_details['original_columns'], 1):
                f.write(f"  {i:2d}. {col}\n")
            
            f.write(f"\nSource Files:\n")
            for i, file_path in enumerate(source_files, 1):
                f.write(f"  {i:3d}. {os.path.basename(file_path)}\n")
    
    def generate_summary_report(self) -> str:
        """Generate a summary report of the schema grouping process"""
        report_path = os.path.join(self.output_folder, 'schema_summary_report.txt')
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"Schema Grouping Summary Report\n")
            f.write(f"{'='*60}\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source folder: {self.folder_path}\n")
            f.write(f"Output folder: {self.output_folder}\n\n")
            
            f.write(f"Processing Summary:\n")
            f.write(f"  Total unique schemas found: {len(self.schema_groups)}\n")
            f.write(f"  Total files processed successfully: {sum(len(files) for files in self.schema_groups.values())}\n")
            f.write(f"  Total files failed: {len(self.failed_files)}\n\n")
            
            f.write(f"Schema Details:\n")
            f.write(f"{'-'*60}\n")
            
            # Sort schemas by number of files (descending)
            sorted_schemas = sorted(self.schema_groups.items(), 
                                  key=lambda x: len(x[1]), reverse=True)
            
            for i, (schema_hash, file_list) in enumerate(sorted_schemas, 1):
                schema_details = self.schema_details[schema_hash]
                f.write(f"\n{i:2d}. Schema {schema_hash[:12]}...\n")
                f.write(f"    Files: {len(file_list)}\n")
                f.write(f"    Columns: {schema_details['column_count']}\n")
                f.write(f"    Delimiter: '{schema_details['delimiter']}'\n")
                f.write(f"    Column names: {', '.join(schema_details['original_columns'][:5])}")
                if len(schema_details['original_columns']) > 5:
                    f.write(f" ... (+{len(schema_details['original_columns'])-5} more)")
                f.write(f"\n")
            
            if self.failed_files:
                f.write(f"\nFailed Files:\n")
                f.write(f"{'-'*30}\n")
                for file_path, error in self.failed_files:
                    f.write(f"  {os.path.basename(file_path)}: {error}\n")
        
        return report_path
    
    def process_all(self, min_files_per_group: int = 2, 
                   max_files_per_group: int = None,
                   output_format: str = 'csv') -> Dict[str, str]:
        """
        Complete process: scan files, group by schema, and combine
        
        Returns:
            Dictionary mapping schema_hash to output file path
        """
        self.logger.info("Starting schema-based file grouping and combination process")
        
        # Step 1: Scan and group files
        self.scan_files()
        
        # Step 2: Combine files by schema
        combined_files = self.combine_files_by_schema(
            min_files_per_group=min_files_per_group,
            max_files_per_group=max_files_per_group,
            output_format=output_format
        )
        
        # Step 3: Generate summary report
        report_path = self.generate_summary_report()
        
        self.logger.info(f"Process completed. Created {len(combined_files)} combined files")
        self.logger.info(f"Summary report: {report_path}")
        
        return combined_files


# Usage example
if __name__ == "__main__":
    # Configuration
    SOURCE_FOLDER = r"C:\path\to\your\thousands\of\files"  # Update this path
    OUTPUT_FOLDER = r"C:\path\to\output\combined_files"    # Update this path
    
    # Create grouper instance
    grouper = SchemaGrouper(
        folder_path=SOURCE_FOLDER,
        output_folder=OUTPUT_FOLDER,
        sample_rows=20,  # Number of rows to sample for schema detection
        file_extensions=['.txt', '.csv', '.tsv', '.dat']  # File types to process
    )
    
    # Process all files
    combined_files = grouper.process_all(
        min_files_per_group=2,      # Only combine if at least 2 files have same schema
        max_files_per_group=1000,   # Limit files per group (None for no limit)
        output_format='csv'         # Output format: 'csv', 'parquet', or 'excel'
    )
    
    # Print results
    print(f"\nProcessing completed!")
    print(f"Created {len(combined_files)} combined files:")
    for schema_hash, output_path in combined_files.items():
        print(f"  - {os.path.basename(output_path)} (Schema: {schema_hash[:12]}...)")
    
    print(f"\nCheck the output folder for:")
    print(f"  - Combined data files")
    print(f"  - Metadata files (_metadata.txt)")
    print(f"  - Processing log (schema_grouping.log)")
    print(f"  - Summary report (schema_summary_report.txt)")
