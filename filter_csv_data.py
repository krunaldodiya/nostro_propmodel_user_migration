#!/usr/bin/env python3
"""
CSV Data Filter Script using Polars
Filters CSV files to keep only records after November 2024
"""

import polars as pl
import os
from pathlib import Path
from datetime import datetime
import argparse


def detect_date_columns(df: pl.DataFrame) -> list:
    """Detect potential date columns in the DataFrame"""
    date_columns = []

    for col in df.columns:
        col_lower = col.lower()
        # Check column names that might contain dates
        if any(
            keyword in col_lower
            for keyword in ["date", "time", "created", "updated", "timestamp"]
        ):
            date_columns.append(col)

    return date_columns


def try_parse_date_column(df: pl.DataFrame, col_name: str) -> bool:
    """Try to parse a column as date to see if it's a valid date column"""
    try:
        # Try to parse a small sample
        sample_df = df.head(1000)
        sample_df.select(pl.col(col_name).str.to_datetime()).collect()
        return True
    except:
        try:
            # Try as date only
            sample_df = df.head(1000)
            sample_df.select(pl.col(col_name).str.to_date()).collect()
            return True
        except:
            return False


def filter_csv_by_date(input_file: str, output_file: str, date_column: str = None):
    """
    Filter CSV file to keep only records after November 1, 2024

    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        date_column: Name of the date column (auto-detected if None)
    """
    print(f"Processing file: {input_file}")

    # Read the CSV file in streaming mode for large files
    try:
        # First, read just the schema to understand the structure
        df_schema = pl.scan_csv(input_file).select(pl.all()).limit(0).collect()
        print(f"Columns found: {df_schema.columns}")

        # Detect date columns if not specified
        if date_column is None:
            potential_date_cols = detect_date_columns(df_schema)
            print(f"Potential date columns: {potential_date_cols}")

            # Try to find a valid date column
            date_column = None
            for col in potential_date_cols:
                if try_parse_date_column(pl.scan_csv(input_file), col):
                    date_column = col
                    print(f"Using date column: {date_column}")
                    break

            if date_column is None:
                print("No valid date column found. Please specify one manually.")
                return False

        # Read and filter the data
        print("Reading and filtering data...")

        # Use streaming for large files with error handling
        try:
            df = (
                pl.scan_csv(input_file)
                .filter(pl.col(date_column).str.to_datetime() >= datetime(2024, 11, 1))
                .collect(streaming=True)
            )
        except Exception as e:
            print(f"Error with default parsing: {e}")
            print("Trying with more flexible parsing...")
            df = (
                pl.scan_csv(input_file, infer_schema_length=10000, ignore_errors=True)
                .filter(pl.col(date_column).str.to_datetime() >= datetime(2024, 11, 1))
                .collect(streaming=True)
            )

        print(f"Filtered data shape: {df.shape}")

        # Write the filtered data
        print(f"Writing filtered data to: {output_file}")
        df.write_csv(output_file)

        print(f"Successfully created filtered file: {output_file}")
        return True

    except Exception as e:
        print(f"Error processing file {input_file}: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Filter CSV files to keep records after November 2024"
    )
    parser.add_argument("input_dir", help="Directory containing CSV files to filter")
    parser.add_argument("output_dir", help="Directory to save filtered CSV files")
    parser.add_argument(
        "--date-column", help="Name of the date column (auto-detected if not specified)"
    )
    parser.add_argument(
        "--files", nargs="+", help="Specific files to process (default: all CSV files)"
    )

    args = parser.parse_args()

    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Find CSV files to process
    if args.files:
        csv_files = [input_path / f for f in args.files if f.endswith(".csv")]
    else:
        csv_files = list(input_path.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in the input directory")
        return

    print(f"Found {len(csv_files)} CSV files to process:")
    for file in csv_files:
        print(f"  - {file.name}")

    # Process each CSV file
    for csv_file in csv_files:
        output_file = output_path / f"filtered_{csv_file.name}"

        print(f"\n{'='*50}")
        print(f"Processing: {csv_file.name}")
        print(f"{'='*50}")

        success = filter_csv_by_date(str(csv_file), str(output_file), args.date_column)

        if success:
            print(f"✅ Successfully processed {csv_file.name}")
        else:
            print(f"❌ Failed to process {csv_file.name}")


if __name__ == "__main__":
    main()
