# CSV Data Filtering Scripts

This directory contains scripts to filter large CSV files and keep only records after November 2024.

## Scripts

### 1. `examine_csv.py` - Examine CSV Structure

Use this script first to understand the structure of your CSV files and identify date columns.

```bash
# Examine a single CSV file
uv run examine_csv.py "/home/krunaldodiya/Downloads/nostro sql dump/csv/equity_data_daily.csv"

# Examine with larger sample size
uv run examine_csv.py "/home/krunaldodiya/Downloads/nostro sql dump/csv/equity_data_daily.csv" --sample-size 5000
```

### 2. `filter_csv_data.py` - Filter CSV Data

Use this script to filter CSV files and keep only records after November 1, 2024.

```bash
# Filter all CSV files in a directory (auto-detect date columns)
uv run filter_csv_data.py "/home/krunaldodiya/Downloads/nostro sql dump/csv" "/home/krunaldodiya/Downloads/nostro sql dump/filtered"

# Filter specific files
uv run filter_csv_data.py "/home/krunaldodiya/Downloads/nostro sql dump/csv" "/home/krunaldodiya/Downloads/nostro sql dump/filtered" --files "equity_data_daily.csv" "periodic_trading_export.csv"

# Specify date column manually
uv run filter_csv_data.py "/home/krunaldodiya/Downloads/nostro sql dump/csv" "/home/krunaldodiya/Downloads/nostro sql dump/filtered" --date-column "created_at"
```

## Usage Steps

1. **First, examine the CSV structure:**

   ```bash
   uv run examine_csv.py "/home/krunaldodiya/Downloads/nostro sql dump/csv/equity_data_daily.csv"
   uv run examine_csv.py "/home/krunaldodiya/Downloads/nostro sql dump/csv/periodic_trading_export.csv"
   ```

2. **Then filter the data:**

   ```bash
   # Create output directory
   mkdir -p "/home/krunaldodiya/Downloads/nostro sql dump/filtered"

   # Filter all CSV files
   uv run filter_csv_data.py "/home/krunaldodiya/Downloads/nostro sql dump/csv" "/home/krunaldodiya/Downloads/nostro sql dump/filtered"
   ```

## Features

- **Memory Efficient**: Uses Polars streaming for large files (>200MB)
- **Auto-Detection**: Automatically detects date columns
- **Flexible**: Can specify date columns manually
- **Batch Processing**: Can process multiple files at once
- **Error Handling**: Continues processing even if one file fails

## Output

Filtered files will be saved with the prefix `filtered_` in the output directory:

- `equity_data_daily.csv` → `filtered_equity_data_daily.csv`
- `periodic_trading_export.csv` → `filtered_periodic_trading_export.csv`

## Date Filter

The script filters records where the date column value is >= November 1, 2024 (2024-11-01).

## Troubleshooting

If the auto-detection doesn't work:

1. Use `examine_csv.py` to see the column names
2. Use `--date-column` parameter to specify the exact column name
3. Check that the date format is recognizable by Polars
