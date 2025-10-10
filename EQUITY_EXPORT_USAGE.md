# Equity Data Daily Export Script

This script processes the filtered equity data daily CSV and adds platform account UUID mapping by joining with the platform accounts data.

## Script: `scripts/equity_data_daily_export.py`

### Purpose

- Maps `trading_account` from equity data to `platform_login_id` from platform accounts
- Adds `platform_account_uuid` column to the equity data
- Handles mixed data types in platform_login_id (numeric and string values)

### Usage

#### Preview Mode (Recommended First)

```bash
uv run scripts/equity_data_daily_export.py \
  --equity-file "/path/to/filtered_equity_data_daily.csv" \
  --platform-accounts "csv/output/new_platform_accounts.csv" \
  --output "csv/output/new_equity_data_daily.csv" \
  --preview
```

#### Full Export

```bash
uv run scripts/equity_data_daily_export.py \
  --equity-file "/path/to/filtered_equity_data_daily.csv" \
  --platform-accounts "csv/output/new_platform_accounts.csv" \
  --output "csv/output/new_equity_data_daily.csv"
```

### Arguments

- `--equity-file`: Path to the filtered equity data daily CSV file
- `--platform-accounts`: Path to the new_platform_accounts.csv file
- `--output`: Path for the output CSV file
- `--preview`: Preview mode - shows sample data without writing file

### Output Structure

The output CSV contains the following columns:

1. `trading_account` (Int64) - Original trading account ID
2. `platform_account_uuid` (String) - UUID from platform accounts (null if no match)
3. `day` (String) - Date of the equity record
4. `created_date` (String) - Timestamp when record was created
5. `equity` (Float64) - Equity value
6. `balance` (Float64) - Balance value
7. `equity_eod_mt5` (Float64) - End of day equity from MT5

### Mapping Results

**From the latest run:**

- **Total records**: 3,892,814
- **Matched records**: 3,357,014 (86.2% match rate)
- **Unmatched records**: 535,848 (13.8% unmatched)

**Unmatched trading accounts** include values like:

- 124022, 10945, 130459, 234948874, 5404, 105134, 118407, 124683, 5477993, 234946001

### Data Quality Notes

1. **Mixed Platform Login IDs**: The platform accounts file contains both numeric and string values (e.g., "D#1244582")
2. **Partial Matching**: Some trading accounts don't have corresponding platform accounts
3. **Null Values**: Unmatched records will have `null` values in the `platform_account_uuid` column

### File Sizes

- **Input**: ~228MB (filtered equity data)
- **Output**: ~348MB (enriched equity data with UUIDs)

### Error Handling

The script handles:

- Mixed data types in platform_login_id column
- CSV parsing errors with flexible schema inference
- Type conversion between Int64 and String for joining
- Missing platform account mappings (left join)

### Example Output

```csv
trading_account,platform_account_uuid,day,created_date,equity,balance,equity_eod_mt5
1003,,2024-11-10,2024-11-10 21:43:22,5000.0,5000.0,
1004,8e44a5f8-6b55-4bcc-8750-de138d...,2024-11-10,2024-11-10 21:43:22,10000.0,10000.0,
1005,,2024-11-10,2024-11-10 21:43:22,5000.0,5000.0,
```

### Integration with Main Export System

This script can be integrated into the main export system by:

1. Adding it to the main.py script
2. Creating a configuration entry in the config files
3. Adding it to the export workflow

The output file `new_equity_data_daily.csv` is ready for use in the migration system.
