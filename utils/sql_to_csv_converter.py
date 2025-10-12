#!/usr/bin/env python3
"""
SQL to CSV Converter - Improved Version

This script converts MySQL dump SQL files to CSV format for each table.
It uses polars for high-performance data processing.

Usage:
    python sql_to_csv_converter.py <sql_file_path> [output_directory]
    python sql_to_csv_converter.py --input-dir <directory_path> [output_directory]
"""

import re
import sys
import os
import argparse
from pathlib import Path
from typing import Dict, List, Optional
import polars as pl


class SQLToCSVConverter:
    """Converts MySQL dump SQL files to CSV format for each table."""

    def __init__(self, sql_file_path: str, output_dir: str = "./csv_output/"):
        self.sql_file_path = Path(sql_file_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.table_definitions: Dict[str, Dict] = {}
        self.table_data: Dict[str, List[List[str]]] = {}

    def parse_sql_file(self) -> None:
        """Parse the SQL file to extract table definitions and data."""
        print(f"Parsing SQL file: {self.sql_file_path}")
        print(f"File size: {self.sql_file_path.stat().st_size / (1024**3):.2f} GB")

        current_table = None
        in_create_table = False
        in_insert_statement = False
        statement_buffer = ""
        insert_count = 0
        total_rows_extracted = 0

        with open(self.sql_file_path, "r", encoding="utf-8", errors="replace") as file:
            for line_num, line in enumerate(file, 1):
                if line_num % 100000 == 0:
                    print(f"Processed {line_num:,} lines...")

                # Skip comments
                if line.strip().startswith("--") or line.strip().startswith("/*"):
                    continue

                # Handle CREATE TABLE statements
                if "CREATE TABLE" in line:
                    in_create_table = True
                    statement_buffer = line
                    current_table = self._extract_table_name(line)
                    if current_table:
                        self.table_definitions[current_table] = {
                            "columns": [],
                            "create_statement": line,
                        }
                    continue

                if in_create_table:
                    statement_buffer += " " + line

                    # Extract column names
                    if current_table and line.strip().startswith("`"):
                        column_name = self._extract_column_name(line)
                        if (
                            column_name
                            and column_name
                            not in self.table_definitions[current_table]["columns"]
                        ):
                            self.table_definitions[current_table]["columns"].append(
                                column_name
                            )

                    # End of CREATE TABLE
                    if line.strip().endswith(";"):
                        in_create_table = False
                        statement_buffer = ""
                    continue

                # Handle INSERT statements
                if "INSERT INTO" in line:
                    table_name = self._extract_table_name_from_insert(line)
                    if table_name:
                        current_table = table_name
                        insert_count += 1

                        # Initialize table data if needed
                        if current_table not in self.table_data:
                            self.table_data[current_table] = []

                        # Check if this is a single-line INSERT statement (ends with semicolon)
                        if line.strip().endswith(";"):
                            # Process the complete single-line INSERT statement immediately
                            rows = self._parse_insert_statement(line)
                            if rows:
                                self.table_data[current_table].extend(rows)
                                total_rows_extracted += len(rows)
                                print(
                                    f"INSERT #{insert_count} for {current_table}: {len(rows)} rows (Total: {total_rows_extracted:,})"
                                )
                        else:
                            # Multi-line INSERT statement - start buffering
                            in_insert_statement = True
                            statement_buffer = line
                    continue

                if in_insert_statement:
                    statement_buffer += " " + line

                    # Check if statement is complete
                    if line.strip().endswith(";"):
                        # Process the complete INSERT statement
                        rows = self._parse_insert_statement(statement_buffer)
                        if rows and current_table:
                            self.table_data[current_table].extend(rows)
                            total_rows_extracted += len(rows)
                            print(
                                f"INSERT #{insert_count} for {current_table}: {len(rows)} rows (Total: {total_rows_extracted:,})"
                            )

                        in_insert_statement = False
                        statement_buffer = ""

        print(f"\nParsing complete. Found {len(self.table_definitions)} tables.")
        print(f"Total INSERT statements processed: {insert_count}")
        print(f"Total rows extracted: {total_rows_extracted:,}")

        for table_name, table_info in self.table_definitions.items():
            row_count = len(self.table_data.get(table_name, []))
            print(
                f"  - {table_name}: {len(table_info['columns'])} columns, {row_count:,} rows"
            )

    def _extract_table_name(self, line: str) -> Optional[str]:
        """Extract table name from CREATE TABLE statement."""
        match = re.search(r"CREATE TABLE\s+`?([^`\s(]+)`?\s*\(", line, re.IGNORECASE)
        return match.group(1) if match else None

    def _extract_table_name_from_insert(self, line: str) -> Optional[str]:
        """Extract table name from INSERT INTO statement."""
        match = re.search(r"INSERT INTO\s+`?([^`\s(]+)`?", line, re.IGNORECASE)
        return match.group(1) if match else None

    def _extract_column_name(self, line: str) -> Optional[str]:
        """Extract column name from column definition."""
        match = re.search(r"`([^`]+)`", line)
        return match.group(1) if match else None

    def _parse_insert_statement(self, statement: str) -> List[List[str]]:
        """Parse a complete INSERT statement and extract all rows."""
        # Find the VALUES clause
        values_match = re.search(
            r"VALUES\s+(.+);?\s*$", statement, re.IGNORECASE | re.DOTALL
        )
        if not values_match:
            return []

        values_part = values_match.group(1).strip().rstrip(";")

        # Parse rows using a state machine approach
        rows = []
        current_row = []
        current_value = ""
        depth = 0
        in_string = False
        string_char = None
        i = 0

        while i < len(values_part):
            char = values_part[i]

            if not in_string:
                if char in ("'", '"'):
                    in_string = True
                    string_char = char
                    current_value += char
                elif char == "(":
                    if depth == 0:
                        # Start of new row
                        current_row = []
                        current_value = ""
                    else:
                        current_value += char
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0:
                        # End of row
                        if current_value.strip():
                            current_row.append(current_value.strip())
                        if current_row:
                            rows.append(current_row)
                        current_row = []
                        current_value = ""
                    else:
                        current_value += char
                elif char == "," and depth == 1:
                    # Column separator within a row
                    current_row.append(current_value.strip())
                    current_value = ""
                elif char == "," and depth == 0:
                    # Row separator - do nothing
                    pass
                else:
                    if depth > 0:
                        current_value += char
            else:
                current_value += char
                if char == string_char:
                    # Check if escaped
                    if i + 1 < len(values_part) and values_part[i + 1] == string_char:
                        current_value += values_part[i + 1]
                        i += 1
                    else:
                        in_string = False
                        string_char = None
                elif char == "\\" and i + 1 < len(values_part):
                    # Handle escape sequences
                    current_value += values_part[i + 1]
                    i += 1

            i += 1

        return rows

    def convert_to_csv(self) -> None:
        """Convert parsed data to CSV files."""
        print(f"\nConverting tables to CSV files in: {self.output_dir}")

        for table_name, data in self.table_data.items():
            if not data:
                print(f"  Skipping {table_name} - no data")
                continue

            columns = self.table_definitions[table_name]["columns"]

            if not columns:
                print(f"  Skipping {table_name} - no column definitions found")
                continue

            print(f"  Converting {table_name} ({len(data):,} rows)...")

            try:
                # Process data
                processed_data = []
                for row_idx, row in enumerate(data):
                    # Handle column count mismatch
                    if len(row) != len(columns):
                        print(
                            f"    Warning: Row {row_idx} has {len(row)} values but table has {len(columns)} columns"
                        )
                        # Pad or truncate
                        while len(row) < len(columns):
                            row.append(None)
                        row = row[: len(columns)]

                    processed_row = []
                    for value in row:
                        value = value.strip()

                        # Handle NULL
                        if value.upper() == "NULL":
                            processed_row.append(None)
                        # Remove quotes
                        elif (value.startswith("'") and value.endswith("'")) or (
                            value.startswith('"') and value.endswith('"')
                        ):
                            # Unescape the value
                            unquoted = value[1:-1]
                            unquoted = unquoted.replace("\\'", "'").replace('\\"', '"')
                            unquoted = unquoted.replace("\\\\", "\\")
                            processed_row.append(unquoted)
                        else:
                            processed_row.append(value)

                    processed_data.append(processed_row)

                # Write CSV directly to avoid Polars data type issues
                output_file = self.output_dir / f"{table_name}.csv"

                # Write header
                with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                    import csv

                    writer = csv.writer(csvfile)
                    writer.writerow(columns)

                    # Write data rows
                    for row in processed_data:
                        writer.writerow(row)

                print(f"    ✓ Saved to {output_file}")
                print(f"    ✓ Size: {output_file.stat().st_size / (1024**2):.2f} MB")

            except Exception as e:
                print(f"    ✗ Error converting {table_name}: {e}")
                import traceback

                traceback.print_exc()

    def generate_summary(self) -> None:
        """Generate a summary of the conversion."""
        # Generate individual summary files for each table
        for table_name, table_info in self.table_definitions.items():
            row_count = len(self.table_data.get(table_name, []))
            summary_file = self.output_dir / f"{table_name}_conversion_summary.txt"

            with open(summary_file, "w") as f:
                f.write("SQL to CSV Conversion Summary\n")
                f.write("=" * 40 + "\n\n")
                f.write(f"Source SQL file: {self.sql_file_path}\n")
                f.write(f"Output directory: {self.output_dir}\n\n")
                f.write("Table converted:\n")
                f.write("-" * 20 + "\n")
                f.write(f"{table_name}:\n")
                f.write(f"  Columns: {len(table_info['columns'])}\n")
                f.write(f"  Rows: {row_count:,}\n")
                f.write(f"  Column names: {', '.join(table_info['columns'])}\n\n")

            print(f"Summary for {table_name} saved to: {summary_file}")

        # Also generate a combined summary file
        combined_summary_file = self.output_dir / "conversion_summary.txt"
        with open(combined_summary_file, "w") as f:
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
                f.write(f"  Column names: {', '.join(table_info['columns'])}\n\n")

        print(f"\nCombined summary saved to: {combined_summary_file}")

    def process_multiple_sql_files(self, sql_files: List[Path]) -> None:
        """Process multiple SQL files and combine results."""
        print(f"Processing {len(sql_files)} SQL files...")

        for i, sql_file in enumerate(sql_files, 1):
            print(f"\n--- Processing file {i}/{len(sql_files)}: {sql_file.name} ---")

            temp_converter = SQLToCSVConverter(str(sql_file), str(self.output_dir))

            try:
                temp_converter.parse_sql_file()

                # Merge results
                for table_name, table_info in temp_converter.table_definitions.items():
                    if table_name not in self.table_definitions:
                        self.table_definitions[table_name] = table_info
                        self.table_data[table_name] = []

                    if table_name in temp_converter.table_data:
                        self.table_data[table_name].extend(
                            temp_converter.table_data[table_name]
                        )
                        print(
                            f"  Added {len(temp_converter.table_data[table_name]):,} rows to {table_name}"
                        )

            except Exception as e:
                print(f"  ✗ Error processing {sql_file.name}: {e}")
                continue

        print(f"\nCombined results:")
        for table_name in self.table_definitions:
            row_count = len(self.table_data.get(table_name, []))
            print(f"  - {table_name}: {row_count:,} rows")


def main():
    parser = argparse.ArgumentParser(
        description="Convert MySQL dump SQL files to CSV format"
    )
    parser.add_argument("--input-dir", help="Path to directory containing SQL files")
    parser.add_argument(
        "--table", help="Specific table name to process (e.g., 'equity_data_daily')"
    )
    parser.add_argument("sql_file", nargs="?", help="Path to a single SQL dump file")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Output directory for CSV files (defaults to same as input directory)",
    )

    args = parser.parse_args()

    if args.input_dir:
        input_dir = Path(args.input_dir).expanduser()
        if not input_dir.exists():
            print(f"Error: Input directory '{input_dir}' not found")
            sys.exit(1)

        # Get all SQL files
        all_sql_files = list(input_dir.glob("*.sql"))
        if not all_sql_files:
            print(f"Error: No SQL files found in '{input_dir}'")
            sys.exit(1)

        # Filter by table name if specified
        if args.table:
            sql_files = [f for f in all_sql_files if args.table in f.name]
            if not sql_files:
                print(
                    f"Error: No SQL files found for table '{args.table}' in '{input_dir}'"
                )
                print(f"Available files: {[f.name for f in all_sql_files]}")
                sys.exit(1)
            print(
                f"Processing table '{args.table}' - Found {len(sql_files)} SQL file(s)"
            )
        else:
            sql_files = all_sql_files
            print(f"Found {len(sql_files)} SQL files")

        # Use input directory as output directory if not specified
        output_dir = args.output_dir if args.output_dir else str(input_dir)
        converter = SQLToCSVConverter(str(sql_files[0]), output_dir)

        try:
            converter.process_multiple_sql_files(sql_files)
            converter.convert_to_csv()
            converter.generate_summary()
            print("\n✓ Conversion completed successfully!")
        except Exception as e:
            print(f"\n✗ Error during conversion: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    elif args.sql_file:
        if not os.path.exists(args.sql_file):
            print(f"Error: SQL file '{args.sql_file}' not found")
            sys.exit(1)

        converter = SQLToCSVConverter(args.sql_file, args.output_dir)

        try:
            converter.parse_sql_file()
            converter.convert_to_csv()
            converter.generate_summary()
            print("\n✓ Conversion completed successfully!")
        except Exception as e:
            print(f"\n✗ Error during conversion: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)
    else:
        print("Error: Must provide either a SQL file or --input-dir")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
