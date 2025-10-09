# SQL to CSV Conversion Task

## Project Overview

Convert MySQL dump SQL files to CSV format for each table, with the ability to filter data by date.

## Current Task Status

**Status**: ⚠️ PARTIAL SUCCESS - Major Issue Identified
**Date**: January 2025
**Issue**: Script only extracting ~15k rows from 18M+ total rows

## Task Requirements

### Primary Objective

- Convert MySQL dump SQL files to CSV format
- Extract table definitions and data from SQL dump files
- Generate individual CSV files for each table
- Provide summary of conversion process

### Technical Requirements

- Use Python with `polars` and `pyarrow` for efficient CSV conversion
- Handle multi-row INSERT statements
- Parse table definitions from CREATE TABLE statements
- Extract data from INSERT INTO statements
- Generate conversion summary

### File Structure

- **Input**: SQL dump file (e.g., `dump-nostro_admin-202510091401.sql`)
- **Output**: CSV files in `csv_output/` directory
- **Summary**: `conversion_summary.txt` with conversion details

## Current Implementation

### Script Location

- **File**: `scripts/sql_to_csv_converter.py`
- **Dependencies**: `polars`, `pyarrow` (managed via `uv`)

### Key Components

1. **SQLToCSVConverter Class**

   - Parses SQL dump files
   - Extracts table definitions
   - Converts data to CSV format
   - Generates summary

2. **Main Methods**
   - `parse_sql_file()`: Parses SQL file and extracts table definitions
   - `convert_to_csv()`: Converts parsed data to CSV files
   - `generate_summary()`: Creates conversion summary

## Current Issue Analysis

### Problem Identified

The script successfully parses table definitions and extracts some data, but is missing the vast majority of rows from the SQL file.

### Scale of the Problem

**SQL File Analysis**:

- **Total Rows**: 18,074,521 rows (based on row separators)
- **INSERT Statements**: 1,533 total INSERT statements
  - `periodic_trading_export`: 1,117 INSERT statements
  - `equity_data_daily`: 416 INSERT statements
- **Current Extraction**: Only ~15,361 rows (0.08% of total data)

### Root Cause Analysis

The issue is likely in the regex pattern used to extract values from the large INSERT statements. Each INSERT statement contains thousands of rows, but our current parsing logic is not correctly handling:

1. **Large INSERT Statements**: Each INSERT contains 10,000+ rows
2. **Complex Value Parsing**: The regex pattern may be failing on large statements
3. **Memory/Performance Issues**: Processing 18M+ rows may require different approach

### Technical Investigation Needed

1. **Debug Parsing Logic**: Add diagnostic output to see which INSERT statements are being processed
2. **Test Regex Patterns**: Verify the regex can handle large INSERT statements
3. **Memory Optimization**: Consider streaming approach for very large datasets
4. **Error Handling**: Check for silent failures in value extraction

## Current Conversion Results

### Generated Files (Partial)

⚠️ **CSV Files Created (INCOMPLETE)**:

- `periodic_trading_export.csv` - 6,707 rows, 14 columns (0.73 MB) - **MISSING ~99.9% of data**
- `equity_data_daily.csv` - 8,654 rows, 6 columns (0.51 MB) - **MISSING ~99.9% of data**
- `conversion_summary.txt` - Detailed conversion report

### Data Verification

❌ **Data Quality Issues**:

- ✅ Table definitions correctly parsed
- ❌ INSERT statements only partially processed (0.08% of total data)
- ✅ Data types properly handled for extracted rows
- ❌ CSV files contain only a tiny fraction of the actual data

### Expected vs Actual

- **Expected**: 18,074,521 total rows
- **Actual**: 15,361 rows extracted
- **Missing**: 18,059,160 rows (99.92% of data)

## Next Steps Required

### Immediate Actions Needed

1. **Debug the Parsing Logic**:

   - Add diagnostic output to track which INSERT statements are being processed
   - Count rows extracted from each INSERT statement
   - Identify where the parsing is failing

2. **Fix the Regex Pattern**:

   - Test the current regex on a sample large INSERT statement
   - Implement a more robust parsing approach for 10,000+ row INSERT statements
   - Consider using a different parsing strategy (e.g., state machine approach)

3. **Memory and Performance Optimization**:

   - Handle the scale of 18M+ rows efficiently
   - Consider chunked processing for very large INSERT statements
   - Optimize memory usage for large datasets

4. **Validation and Testing**:
   - Verify row counts match expected totals
   - Test with sample data to ensure accuracy
   - Add comprehensive error reporting

### Technical Challenges

- **Scale**: Processing 18M+ rows requires careful memory management
- **Complex INSERT Statements**: Each INSERT contains thousands of rows with complex data
- **Regex Limitations**: Current regex may not handle the complexity of large statements
- **Performance**: Need to optimize for processing time and memory usage

## Dependencies

- Python 3.11+
- `polars` >= 1.0.0
- `pyarrow` >= 21.0.0
- `uv` for dependency management

## Usage

```bash
# Install dependencies
uv sync

# Run conversion
uv run scripts/sql_to_csv_converter.py <sql_file> [output_dir]
```

## Critical Issue Summary

**MAJOR PROBLEM**: The SQL to CSV conversion script is only extracting 0.08% of the available data (15,361 out of 18,074,521 rows).

**Root Cause**: The regex pattern for parsing large INSERT statements is failing to extract the majority of rows from the extended INSERT format.

**Impact**: The generated CSV files are essentially useless as they contain less than 1% of the actual data.

**Priority**: HIGH - This needs immediate attention to fix the parsing logic and ensure complete data extraction.

## Notes

- The script was originally designed with date filtering capability
- Date filtering was removed per user request
- Focus is now on converting all data to CSV format
- **CRITICAL**: Current implementation is missing 99.92% of the data
- Date filtering will be implemented as a separate script later
