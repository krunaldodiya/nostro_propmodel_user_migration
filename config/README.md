# Configuration Files

This directory contains all column configuration files for the data migration scripts.

## Files

### 1. `users_column_config.json`

**Purpose**: Controls which columns are exported in `new_users.csv`  
**Used by**: `users_export.py`  
**Format**: JSON array of column names

**Example**:

```json
[
  "uuid",
  "email",
  "first_name",
  "last_name",
  ...
]
```

### 2. `purchases_column_config.json`

**Purpose**: Controls which columns are exported in `new_purchases.csv`  
**Used by**: `purchases_export.py`  
**Format**: JSON array of 20 PostgreSQL schema columns  
**Total Columns**: 20

**Example**:

```json
[
  "uuid",
  "user_uuid",
  "discount_code_uuid",
  "stripe_payment_id",
  ...
]
```

### 3. `discount_codes_column_config.json`

**Purpose**: Controls which columns are exported in `new_discount_codes.csv`  
**Used by**: `discount_codes_export.py`  
**Format**: JSON array of 18 PostgreSQL schema columns  
**Total Columns**: 18

**Example**:

```json
[
  "uuid",
  "code",
  "discount_percentage",
  "commission_percentage",
  ...
]
```

### 4. `platform_accounts_column_config.json`

**Purpose**: Controls which columns are exported in `new_platform_accounts.csv`  
**Used by**: `platform_accounts_export.py`  
**Format**: JSON array of 30 PostgreSQL schema columns  
**Total Columns**: 30

**Example**:

```json
[
  "uuid",
  "user_uuid",
  "purchase_uuid",
  "platform_login_id",
  ...
]
```

## Usage

These configuration files define the exact columns to export, ensuring the output CSV files match the PostgreSQL database schema.

### Adding a New Column

1. Add the column to the PostgreSQL schema
2. Update the corresponding `*_column_config.json` file
3. Add the column generation logic to the export script (if needed)
4. Test the export with preview mode first

### Removing a Column

1. Remove the column from the `*_column_config.json` file
2. The export script will automatically exclude it

## Notes

- All column names must exactly match the DataFrame column names in the export scripts
- Order matters! Columns are exported in the order they appear in these files
- Invalid column names will be reported during export with a warning
- If a config file is missing, the script will export all columns as a fallback
