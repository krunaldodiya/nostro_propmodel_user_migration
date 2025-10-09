# SQL to CSV Conversion Task

## Project Overview

Convert MySQL dump SQL files to CSV format for each table, with the ability to filter data by date.

## Current Task Status

**Status**: ✅ SUCCESS - equity_data_daily Complete!
**Date**: January 2025
**Previous Issue**: Script only extracting ~15k rows from 18M+ total rows
**Current Success**: equity_data_daily: 6,478,423 rows (100% extracted) → 390.7 MB CSV
**Next**: Process periodic_trading_export table

### Data Extraction Analysis

| Table                     | Expected Rows  | Extracted Rows | Missing Rows   | % Extracted | Status |
| ------------------------- | -------------- | -------------- | -------------- | ----------- | ------ |
| `equity_data_daily`       | **6,478,423**  | **6,478,423**  | **0**          | **100%**    | ✅ DONE |
| `periodic_trading_export` | **11,599,425** | 8,204          | **11,591,221** | **0.07%**   | 🔄 PENDING |

**SUCCESS**: equity_data_daily table is now 100% extracted!

### Completed Strategy

1. ✅ **Individual Table Processing**: Updated script to accept `--table` argument
2. ✅ **Focus on equity_data_daily first**: Successfully extracted all 6,478,423 rows
3. ✅ **Fixed CSV Writing Issue**: Replaced Polars DataFrame with direct CSV writing
4. 🔄 **Next**: Process periodic_trading_export table

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
- **NEW**: Process multiple separate SQL files instead of one combined file

### File Structure

- **Input**: Separate SQL dump files for each table in `~/Downloads/nostro sql dump/`
  - `equity_data_daily.sql`
  - `periodic_trading_export.sql`
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

## Updated Approach

### New Strategy

Instead of processing one large combined SQL file, we now use separate SQL files for each table:

- **Input Directory**: `~/Downloads/nostro sql dump/`
- **Separate Files**: Each table has its own SQL file
- **Benefits**:
  - Easier to debug and process individual tables
  - Better memory management
  - More reliable parsing for large datasets
  - Can process tables independently

### Previous Issue Analysis

**Problem Identified**: The script was only extracting ~15k rows from 18M+ total rows when processing the combined SQL file.

**Root Cause**: The regex pattern used to extract values from large INSERT statements was failing to handle the complexity of the combined file format.

**Solution**: Using separate SQL files eliminates the complexity of parsing multiple tables from one large file, making the parsing more reliable and efficient.

## Implementation Plan

### Updated Script Requirements

The script needs to be modified to:

1. **Process Multiple SQL Files**: Handle separate SQL files for each table
2. **Batch Processing**: Process all SQL files in the input directory
3. **Improved Parsing**: Fix the regex parsing issues for better data extraction
4. **Better Error Handling**: Handle individual file failures gracefully

### Expected Benefits

- **Reliability**: Separate files are easier to parse and debug
- **Performance**: Better memory management with smaller individual files
- **Maintainability**: Easier to troubleshoot issues with specific tables
- **Scalability**: Can process tables in parallel if needed

## Next Steps Required

### Immediate Actions Needed

1. **Modify the Script**:

   - Update `SQLToCSVConverter` to accept a directory of SQL files
   - Add batch processing capability for multiple files
   - Improve the regex parsing logic for better data extraction

2. **Test with Separate Files**:

   - Process `equity_data_daily.sql` separately
   - Process `periodic_trading_export.sql` separately
   - Verify complete data extraction from each file

3. **Validation and Testing**:
   - Compare row counts with expected totals
   - Test data integrity and completeness
   - Add comprehensive error reporting

### Technical Improvements

- **Better Parsing**: Fix the regex patterns that were failing on large INSERT statements
- **Memory Optimization**: Process files individually to reduce memory usage
- **Error Handling**: Handle individual file failures without stopping the entire process
- **Progress Tracking**: Better progress reporting for multiple file processing

## Dependencies

- Python 3.11+
- `polars` >= 1.0.0
- `pyarrow` >= 21.0.0
- `uv` for dependency management

## Usage

```bash
# Install dependencies
uv sync

# Run conversion with single SQL file (current approach)
uv run scripts/sql_to_csv_converter.py <sql_file> [output_dir]

# NEW: Run conversion with directory of SQL files (updated approach)
uv run scripts/sql_to_csv_converter.py --input-dir "~/Downloads/nostro sql dump/" [output_dir]
```

## Updated Approach Summary

**NEW STRATEGY**: Using separate SQL files for each table instead of one combined file.

**Benefits**:

- Eliminates the parsing complexity that was causing 99.92% data loss
- Better memory management and performance
- Easier debugging and maintenance
- More reliable data extraction

**Implementation Status**:

- ✅ Task documentation updated
- 🔄 Script modification in progress
- ⏳ Testing with separate files pending

## Notes

- The script was originally designed with date filtering capability
- Date filtering was removed per user request
- Focus is now on converting all data to CSV format
- **UPDATED**: Now using separate SQL files to avoid parsing issues
- Date filtering will be implemented as a separate script later
