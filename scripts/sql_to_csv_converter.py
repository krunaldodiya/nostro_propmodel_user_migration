#!/usr/bin/env python3
"""
SQL to CSV Converter

This script converts MySQL dump SQL files to CSV format for each table.
It uses polars and pyarrow for high-performance data processing.

Usage:
    python sql_to_csv_converter.py <sql_file_path> [output_directory]

Example:
    python sql_to_csv_converter.py ~/Downloads/dump-nostro_admin-202510091401.sql ./csv_output/
"""

import re
import sys
import os
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import polars as pl
import pyarrow as pa
import pyarrow.csv as csv


class SQLToCSVConverter:
    """Converts MySQL dump SQL files to CSV format for each table."""

    def __init__(
        self,
        sql_file_path: str,
        output_dir: str = "./csv_output/",
    ):
        self.sql_file_path = Path(sql_file_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Table definitions found in the SQL file
        self.table_definitions: Dict[str, Dict] = {}
        self.table_data: Dict[str, List[List[str]]] = {}

    def parse_sql_file(self) -> None:
        """Parse the SQL file to extract table definitions and data."""
        print(f"Parsing SQL file: {self.sql_file_path}")
        print(f"File size: {self.sql_file_path.stat().st_size / (1024**3):.2f} GB")

        current_table = None
        in_insert_statement = False
        insert_buffer = ""
        insert_count = 0
        total_rows_extracted = 0

        with open(self.sql_file_path, "r", encoding="utf-8") as file:
            for line_num, line in enumerate(file, 1):
                if line_num % 100000 == 0:
                    print(f"Processed {line_num:,} lines...")

                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("--") or line.startswith("/*"):
                    continue

                # Parse CREATE TABLE statements
                if line.startswith("CREATE TABLE"):
                    current_table = self._extract_table_name(line)
                    self.table_definitions[current_table] = {
                        "columns": [],
                        "create_statement": line,
                    }
                    continue

                # Parse column definitions
                if current_table and line.startswith("`") and "`" in line:
                    column_name = self._extract_column_name(line)
                    if column_name:
                        self.table_definitions[current_table]["columns"].append(
                            column_name
                        )
                    continue

                # Parse INSERT statements
                if line.startswith("INSERT INTO"):
                    current_table = self._extract_table_name_from_insert(line)
                    in_insert_statement = True
                    insert_count += 1
                    # Extract the VALUES part from the INSERT line
                    insert_buffer = self._extract_values_part_from_insert(line)
                    print(f"Found INSERT #{insert_count} for table: {current_table}")
                    continue

                # Continue parsing INSERT statement values
                if in_insert_statement and current_table:
                    # Add the line to the buffer
                    insert_buffer += " " + line

                    # Check if INSERT statement ends
                    if line.endswith(";"):
                        # Process the complete INSERT statement
                        values = self._extract_values_from_insert_buffer(insert_buffer)
                        if values:
                            if current_table not in self.table_data:
                                self.table_data[current_table] = []
                            self.table_data[current_table].extend(values)
                            total_rows_extracted += len(values)
                            print(
                                f"INSERT #{insert_count}: Extracted {len(values)} rows for {current_table} (Total: {total_rows_extracted:,})"
                            )
                        else:
                            print(
                                f"INSERT #{insert_count}: No values extracted for {current_table}"
                            )

                        in_insert_statement = False
                        current_table = None
                        insert_buffer = ""

        print(f"Parsing complete. Found {len(self.table_definitions)} tables.")
        print(f"Total INSERT statements processed: {insert_count}")
        print(f"Total rows extracted: {total_rows_extracted:,}")
        for table_name, table_info in self.table_definitions.items():
            row_count = len(self.table_data.get(table_name, []))
            print(
                f"  - {table_name}: {len(table_info['columns'])} columns, {row_count:,} rows"
            )

    def _extract_table_name(self, create_line: str) -> str:
        """Extract table name from CREATE TABLE statement."""
        match = re.search(r"CREATE TABLE `([^`]+)`", create_line)
        return match.group(1) if match else None

    def _extract_table_name_from_insert(self, insert_line: str) -> str:
        """Extract table name from INSERT INTO statement."""
        match = re.search(r"INSERT INTO `([^`]+)`", insert_line)
        return match.group(1) if match else None

    def _extract_column_name(self, column_line: str) -> Optional[str]:
        """Extract column name from column definition."""
        match = re.search(r"`([^`]+)`", column_line)
        return match.group(1) if match else None

    def _extract_values_part_from_insert(self, insert_line: str) -> str:
        """Extract the VALUES part from an INSERT statement."""
        # Look for VALUES keyword and everything after it
        values_match = re.search(r"VALUES\s+(.*)", insert_line, re.IGNORECASE)
        if values_match:
            return values_match.group(1)
        return ""

    def _extract_values_from_insert_buffer(self, insert_buffer: str) -> List[List[str]]:
        """Extract values from a complete INSERT statement buffer using a simple but effective approach."""
        values = []

        # Remove trailing semicolon if present
        insert_buffer = insert_buffer.rstrip(";")

        # Find the VALUES part and extract everything after it
        values_match = re.search(
            r"VALUES\s+(.*)", insert_buffer, re.IGNORECASE | re.DOTALL
        )
        if not values_match:
            return values

        values_content = values_match.group(1)

        # Use a much simpler approach: split by "),(" pattern
        # This is the most reliable way to handle large INSERT statements
        # First, remove the leading and trailing parentheses
        values_content = values_content.strip()
        if values_content.startswith("("):
            values_content = values_content[1:]
        if values_content.endswith(")"):
            values_content = values_content[:-1]

        # Split by "),(" to get individual rows
        # This pattern separates rows in the extended INSERT format
        row_parts = values_content.split("),(")

        for i, row_part in enumerate(row_parts):
            # Add back the parentheses for the first and last rows
            if i == 0:
                row_content = "(" + row_part
            elif i == len(row_parts) - 1:
                row_content = row_part + ")"
            else:
                row_content = "(" + row_part + ")"

            # Extract values from this row
            row_values = self._extract_values_from_single_row(row_content)
            if row_values:
                values.append(row_values)

        return values

    def _extract_values_from_single_row(self, row_content: str) -> List[str]:
        """Extract values from a single row enclosed in parentheses."""
        # Remove the outer parentheses
        if row_content.startswith("(") and row_content.endswith(")"):
            row_content = row_content[1:-1]

        # Split by comma, but be careful with commas inside quotes
        return self._split_values(row_content)

    def _extract_values_from_line(self, line: str) -> List[List[str]]:
        """Extract values from VALUES line (legacy method - kept for compatibility)."""
        values = []

        # Remove VALUES keyword if present
        if line.startswith("VALUES"):
            line = line[6:].strip()

        # Find all value groups in parentheses
        pattern = r"\(([^)]+)\)"
        matches = re.findall(pattern, line)

        for match in matches:
            # Split by comma, but be careful with commas inside quotes
            row_values = self._split_values(match)
            values.append(row_values)

        return values

    def _split_values(self, value_string: str) -> List[str]:
        """Split comma-separated values, handling quoted strings."""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None

        i = 0
        while i < len(value_string):
            char = value_string[i]

            if not in_quotes:
                if char in ["'", '"']:
                    in_quotes = True
                    quote_char = char
                    current_value += char
                elif char == ",":
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            else:
                current_value += char
                if char == quote_char:
                    # Check if it's escaped
                    if i + 1 < len(value_string) and value_string[i + 1] == quote_char:
                        current_value += value_string[i + 1]
                        i += 1
                    else:
                        in_quotes = False
                        quote_char = None

            i += 1

        # Add the last value
        if current_value:
            values.append(current_value.strip())

        return values

    def convert_to_csv(self) -> None:
        """Convert parsed data to CSV files."""
        print(f"\nConverting tables to CSV files in: {self.output_dir}")

        for table_name, data in self.table_data.items():
            if not data:
                print(f"  Skipping {table_name} - no data")
                continue

            # Get column names
            columns = self.table_definitions[table_name]["columns"]

            print(f"  Converting {table_name} ({len(data):,} rows)...")

            # Create DataFrame
            try:
                # Convert data to proper types
                processed_data = []
                for row in data:
                    processed_row = []
                    for i, value in enumerate(row):
                        # Remove quotes and handle NULL values
                        if value.upper() == "NULL":
                            processed_row.append(None)
                        else:
                            # Remove surrounding quotes
                            if value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            elif value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            processed_row.append(value)
                    processed_data.append(processed_row)

                # Create Polars DataFrame
                df = pl.DataFrame(processed_data, schema=columns)

                # Write to CSV
                output_file = self.output_dir / f"{table_name}.csv"
                df.write_csv(output_file)

                print(f"    ✓ Saved to {output_file}")
                print(f"    ✓ Size: {output_file.stat().st_size / (1024**2):.2f} MB")

            except Exception as e:
                print(f"    ✗ Error converting {table_name}: {e}")
                # Try to save as raw CSV without type conversion
                try:
                    output_file = self.output_dir / f"{table_name}_raw.csv"
                    with open(output_file, "w", encoding="utf-8") as f:
                        # Write header
                        f.write(",".join(columns) + "\n")
                        # Write data
                        for row in data:
                            f.write(",".join(row) + "\n")
                    print(f"    ✓ Saved raw data to {output_file}")
                except Exception as e2:
                    print(f"    ✗ Failed to save raw data: {e2}")

    def generate_summary(self) -> None:
        """Generate a summary of the conversion."""
        summary_file = self.output_dir / "conversion_summary.txt"

        with open(summary_file, "w") as f:
            f.write("SQL to CSV Conversion Summary\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Source SQL file: {self.sql_file_path}\n")
            f.write(f"Output directory: {self.output_dir}\n\n")

            f.write("Tables converted:\n")
            f.write("-" * 20 + "\n")

            for table_name, table_info in self.table_definitions.items():
                row_count = len(self.table_data.get(table_name, []))
                f.write(f"{table_name}:\n")
                f.write(f"  Columns: {len(table_info['columns'])}\n")
                f.write(f"  Rows: {row_count:,}\n")
                f.write(f"  Columns: {', '.join(table_info['columns'])}\n\n")

        print(f"\nSummary saved to: {summary_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert MySQL dump SQL files to CSV format"
    )
    parser.add_argument("sql_file", help="Path to the SQL dump file")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="./csv_output/",
        help="Output directory for CSV files (default: ./csv_output/)",
    )

    args = parser.parse_args()

    # Validate input file
    if not os.path.exists(args.sql_file):
        print(f"Error: SQL file '{args.sql_file}' not found")
        sys.exit(1)

    # Create converter and run conversion
    converter = SQLToCSVConverter(args.sql_file, args.output_dir)

    try:
        converter.parse_sql_file()
        converter.convert_to_csv()
        converter.generate_summary()
        print("\n✓ Conversion completed successfully!")

    except Exception as e:
        print(f"\n✗ Error during conversion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
