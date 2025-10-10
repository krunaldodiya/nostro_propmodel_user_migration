#!/usr/bin/env python3
"""
CSV Structure Examination Script
Helps examine the structure of large CSV files to identify date columns
"""

import polars as pl
from pathlib import Path
import argparse


def examine_csv_structure(file_path: str, sample_size: int = 1000):
    """
    Examine the structure of a CSV file

    Args:
        file_path: Path to the CSV file
        sample_size: Number of rows to sample for analysis
    """
    print(f"Examining file: {file_path}")
    print("=" * 60)

    try:
        # Read a sample of the data with error handling
        try:
            df = pl.scan_csv(file_path).head(sample_size).collect()
        except Exception as e:
            print(f"Error reading with default settings: {e}")
            print("Trying with more flexible parsing...")
            # Try with more flexible parsing
            df = (
                pl.scan_csv(file_path, infer_schema_length=10000, ignore_errors=True)
                .head(sample_size)
                .collect()
            )

        print(f"File shape: {df.shape}")
        print(f"Columns: {df.columns}")
        print()

        # Show data types
        print("Data types:")
        for col, dtype in zip(df.columns, df.dtypes):
            print(f"  {col}: {dtype}")
        print()

        # Show first few rows
        print("First 5 rows:")
        print(df.head())
        print()

        # Look for potential date columns
        date_keywords = ["date", "time", "created", "updated", "timestamp"]
        potential_date_cols = []

        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in date_keywords):
                potential_date_cols.append(col)

        if potential_date_cols:
            print(f"Potential date columns: {potential_date_cols}")

            # Try to parse each potential date column
            for col in potential_date_cols:
                print(f"\nAnalyzing column '{col}':")
                try:
                    # Show unique values in the sample
                    unique_values = df.select(pl.col(col)).unique().head(10)
                    print(f"  Sample values: {unique_values.to_series().to_list()}")

                    # Try to parse as datetime
                    try:
                        parsed = df.select(pl.col(col).str.to_datetime()).head(5)
                        print(f"  ✅ Successfully parsed as datetime")
                        print(f"  Sample parsed values: {parsed.to_series().to_list()}")
                    except:
                        print(f"  ❌ Failed to parse as datetime")

                except Exception as e:
                    print(f"  Error analyzing column: {e}")
        else:
            print(
                "No obvious date columns found. You may need to specify the date column manually."
            )

        print("\n" + "=" * 60)

    except Exception as e:
        print(f"Error examining file: {e}")


def main():
    parser = argparse.ArgumentParser(description="Examine CSV file structure")
    parser.add_argument("file_path", help="Path to CSV file to examine")
    parser.add_argument(
        "--sample-size", type=int, default=1000, help="Number of rows to sample"
    )

    args = parser.parse_args()

    if not Path(args.file_path).exists():
        print(f"File not found: {args.file_path}")
        return

    examine_csv_structure(args.file_path, args.sample_size)


if __name__ == "__main__":
    main()
