# Export Process Summary

This document provides a high-level overview of all export processes and their data filtering results.

## Overview

All export processes follow the same pattern:

1. **Source**: Original CSV files in `csv/input/`
2. **Processing**: Filter, transform, and map data according to business rules
3. **Output**: New CSV files in `csv/output/` ready for PostgreSQL import

## Export Summary Table

| Export Name                     | Source File                   | Original Records | Final Records | Records Removed | Retention Rate | File Size |
| ------------------------------- | ----------------------------- | ---------------- | ------------- | --------------- | -------------- | --------- |
| **Platform Accounts**           | `mt5_users.csv`               | 89,881           | 67,930        | 21,951          | 75.58%         | 22M       |
| **Discount Codes**              | `discount_codes.csv`          | 54,000           | 53,892        | 108             | 99.80%         | 1M        |
| **Purchases**                   | `purchases.csv`               | 258,662          | 258,662       | 0               | 100.0%         | 15M       |
| **Equity Data Daily**           | `equity_data_daily.csv`       | 3,892,814        | 3,321,328     | 571,486         | 85.32%         | 8M        |
| **Periodic Trading Export**     | `periodic_trading_export.csv` | 8,705,821        | 8,705,821     | 0               | 100.0%         | 127M      |
| **Advanced Challenge Settings** | Generated                     | 0                | 67,962        | 0               | N/A            | 25M       |

## Key Filtering Reasons

### Platform Accounts (21,951 records removed)

- **1,239 records**: TradeLocker entries (D# prefixes) - not MT5 accounts
- **658 records**: Duplicate purchase_ids - database constraint violation
- **1 record**: Duplicate login - database constraint violation
- **3 records**: Missing group information - cannot be categorized
- **20,050 records**: No matching platform group combinations - invalid configurations

### Discount Codes (108 records removed)

- **108 records**: Duplicate discount codes - database constraint violation

### Equity Data Daily (571,486 records removed)

- **571,486 records**: No matching platform account - foreign key constraint violation

### Periodic Trading Export (0 records removed)

- **0 records**: All trading records have valid platform account mappings

### Purchases (0 records removed)

- **0 records**: All purchases have valid user mappings
- **115,997 purchases**: Have discount codes
- **142,665 purchases**: No discount codes (nullable field)

### Advanced Challenge Settings (Generated Data)

- **32 platform groups**: Each gets default challenge settings
- **67,930 platform accounts**: Each gets default challenge settings
- **Total**: 67,962 records generated

## Data Quality Insights

### High Retention Rates

- **Purchases**: 100% retention - all data is valid
- **Periodic Trading**: 100% retention - all data is valid
- **Discount Codes**: 99.80% retention - minimal duplicates

### Moderate Filtering

- **Equity Data Daily**: 85.32% retention - significant filtering due to platform account mapping
- **Platform Accounts**: 75.58% retention - substantial filtering for data quality

### Generated Data

- **Advanced Challenge Settings**: 100% generation success - all platform entities covered

## Database Import Readiness

All exported CSV files are ready for PostgreSQL import with:

- ✅ Unique UUIDs for all primary keys
- ✅ Proper foreign key relationships
- ✅ Correct data types matching PostgreSQL schema
- ✅ All constraints satisfied (unique, not null, etc.)
- ✅ No duplicate primary keys
- ✅ Valid foreign key references

## File Sizes

Total output size: **~198MB**

- Largest: Periodic Trading Export (127MB)
- Medium: Platform Accounts (22MB), Advanced Challenge Settings (25M)
- Smaller: Equity Data Daily (8MB), Purchases (15MB), Discount Codes (1MB)

## Documentation Files

Each export has detailed documentation:

- `{export_name}_export_documentation.md` - Human-readable markdown
- `{export_name}_export_stats.json` - Machine-readable statistics

## Next Steps

1. **Import to PostgreSQL**: All files are ready for database import
2. **Verify Relationships**: Test foreign key constraints after import
3. **Monitor Performance**: Large files may need batch processing
4. **Data Validation**: Run queries to verify data integrity

---

_Generated on: 2025-10-11T13:11:47_
_Total processing time: ~2 minutes_
_All exports completed successfully_
