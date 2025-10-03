# Discount Codes Schema Comparison

## MySQL CSV vs PostgreSQL Table Structure

### Overview

This document compares the structure of `discount_codes.csv` (MySQL export) with the PostgreSQL `discount_codes` table schema.

---

## Column Mapping & Differences

| MySQL CSV Column         | PostgreSQL Column       | Status              | Notes                                                                                                   |
| ------------------------ | ----------------------- | ------------------- | ------------------------------------------------------------------------------------------------------- |
| `id`                     | `uuid`                  | ⚠️ **CHANGED**      | Changed from INT to UUID, now PRIMARY KEY with `gen_random_uuid()`                                      |
| -                        | `name`                  | ➕ **NEW**          | New column in PostgreSQL - **Added in export using `code` as value**                                    |
| `code`                   | `code`                  | ✅ **SAME**         | Both exist, but PostgreSQL has NOT NULL constraint                                                      |
| `status`                 | `status`                | ⚠️ **NORMALIZED**   | CSV: varchar (ACTIVE/INACTIVE), PostgreSQL: varchar - **Converted to lowercase ('active'/'inactive')**  |
| `max_usages_count`       | `max_usage_count`       | ⚠️ **RENAMED**      | Renamed from plural to singular, PostgreSQL has DEFAULT 0 - **Both columns included in export**         |
| `current_usages_count`   | `current_usage_count`   | ⚠️ **RENAMED**      | Renamed from plural to singular, PostgreSQL has DEFAULT 0 - **Both columns included in export**         |
| `discount`               | `discount`              | ✅ **SAME**         | Both exist, PostgreSQL has DEFAULT 0                                                                    |
| -                        | `start_date`            | ➕ **NEW**          | New column in PostgreSQL - **Added in export using `created_at` value**                                 |
| `discount_code_end_date` | `end_date`              | ⚠️ **RENAMED**      | Renamed to simpler name, type changed to timestamptz - **Both columns included in export**              |
| `group_name`             | -                       | ❌ **REMOVED**      | Does not exist in PostgreSQL                                                                            |
| `account_balance`        | -                       | ❌ **REMOVED**      | Does not exist in PostgreSQL                                                                            |
| `challenge_amount`       | `challenge_amount`      | ⚠️ **TYPE CHANGED** | CSV: text/varchar, PostgreSQL: jsonb                                                                    |
| `email`                  | `email`                 | ⚠️ **TYPE CHANGED** | CSV: text/varchar, PostgreSQL: jsonb                                                                    |
| `challenge_step`         | `challenge_step`        | ⚠️ **TYPE CHANGED** | CSV: text/varchar, PostgreSQL: jsonb                                                                    |
| -                        | `created_by`            | ➕ **NEW**          | New column in PostgreSQL (uuid, nullable) - **Set to 'f965141e-43f0-4992-a742-7899edbe1ca5' in export** |
| `created_date`           | `created_at`            | ⚠️ **RENAMED**      | Renamed, PostgreSQL has NOT NULL + DEFAULT CURRENT_TIMESTAMP                                            |
| `last_modified_date`     | `updated_at`            | ⚠️ **RENAMED**      | Renamed, PostgreSQL has NOT NULL + DEFAULT CURRENT_TIMESTAMP                                            |
| -                        | `type`                  | ➕ **NEW**          | New column in PostgreSQL - **Added in export with default 'admin'**                                     |
| -                        | `commission_percentage` | ➕ **NEW**          | New column in PostgreSQL - **Added in export using `discount` value**                                   |
| -                        | `deleted_at`            | ➕ **NEW**          | New column in PostgreSQL (timestamp, nullable) - for soft deletes                                       |

---

## Detailed Changes

### 1. Primary Key Change

- **MySQL**: `id` (INT, AUTO_INCREMENT)
- **PostgreSQL**: `uuid` (UUID, DEFAULT gen_random_uuid())
- **Impact**: Complete redesign of primary key from integer to UUID

### 2. Column Renames

#### Plural to Singular

- `max_usages_count` → `max_usage_count`
- `current_usages_count` → `current_usage_count`

#### Simplified Names

- `discount_code_end_date` → `end_date`
- `created_date` → `created_at`
- `last_modified_date` → `updated_at`

### 3. New Columns in PostgreSQL

| Column                  | Type        | Default | Purpose                                    |
| ----------------------- | ----------- | ------- | ------------------------------------------ |
| `name`                  | varchar     | NULL    | Descriptive name for the discount code     |
| `start_date`            | timestamptz | NULL    | When discount becomes active               |
| `created_by`            | uuid        | NULL    | Reference to user who created the discount |
| `type`                  | varchar     | 'admin' | Type of discount (admin/affiliate/etc.)    |
| `commission_percentage` | float8      | 0       | Commission percentage for affiliates       |
| `deleted_at`            | timestamp   | NULL    | Soft delete timestamp                      |

### 4. Removed Columns from MySQL

| Column            | Type         | Notes                                                       |
| ----------------- | ------------ | ----------------------------------------------------------- |
| `group_name`      | text/varchar | Removed - possibly replaced by different grouping mechanism |
| `account_balance` | text/varchar | Removed - likely moved to different table or not needed     |

### 5. Type Changes

#### From Text/Varchar to JSONB

The following columns changed from text/varchar to jsonb to support structured data:

- **`challenge_amount`**: Now stores JSON objects instead of plain text
- **`email`**: Now stores JSON (possibly for multiple emails or structured email data)
- **`challenge_step`**: Now stores JSON objects for structured step data

**Example transformation needed:**

```
CSV: "10000" or "value"
PostgreSQL: {"amount": 10000} or structured JSON
```

### 6. Constraint Changes

#### NOT NULL Constraints Added

- `code`: Now has NOT NULL constraint in PostgreSQL
- `created_at`: Now has NOT NULL constraint
- `updated_at`: Now has NOT NULL constraint

#### DEFAULT Values Added

- `max_usage_count`: DEFAULT 0
- `current_usage_count`: DEFAULT 0
- `discount`: DEFAULT 0
- `created_at`: DEFAULT CURRENT_TIMESTAMP
- `updated_at`: DEFAULT CURRENT_TIMESTAMP
- `type`: DEFAULT 'admin'
- `commission_percentage`: DEFAULT 0
- `status`: DEFAULT 'active'

---

## Data Type Comparison

| Column            | MySQL/CSV Type            | PostgreSQL Type | Notes                                     |
| ----------------- | ------------------------- | --------------- | ----------------------------------------- |
| Primary Key       | INT                       | UUID            | Complete redesign                         |
| `discount`        | numeric/float             | float8          | Compatible                                |
| `*_date` columns  | datetime/varchar          | timestamptz     | PostgreSQL uses timezone-aware timestamps |
| `*_count` columns | INT                       | int4            | Compatible                                |
| JSON columns      | text/varchar              | jsonb           | Requires transformation                   |
| `status`          | varchar (ACTIVE/INACTIVE) | varchar         | Converted to integer (1/0) in export      |
| `code`            | varchar                   | varchar         | Compatible                                |

---

## Migration Considerations

### 1. UUID Generation

- All existing `id` values need to be mapped to newly generated UUIDs
- Foreign key relationships in other tables must be updated

### 2. Status Normalization

✅ **HANDLED** - Status column normalized to lowercase:

- `ACTIVE` → `'active'` (53,777 records)
- `INACTIVE` → `'inactive'` (117 records)
- Any other value → `'inactive'` (treated as inactive)

### 3. JSON Transformation

Columns that need JSON transformation:

- `challenge_amount`: Convert text to structured JSON
- `email`: Convert text to structured JSON
- `challenge_step`: Convert text to structured JSON

### 4. Column Renaming Strategy

✅ **HANDLED** - Export includes both MySQL and PostgreSQL column names for smooth migration:

- `max_usages_count` → `max_usage_count` (both columns present with same value)
- `current_usages_count` → `current_usage_count` (both columns present with same value)
- `discount_code_end_date` → `end_date` (both columns present with same value)
- `created_date` → `created_at` (both columns present with same value)
- `last_modified_date` → `updated_at` (both columns present with same value)

### 5. Missing Data Handling

New columns need default values:

- `name`: ✅ **HANDLED** - Using `code` value in export
- `start_date`: ✅ **HANDLED** - Using `created_at` value in export
- `created_by`: ✅ **HANDLED** - Set to UUID `f965141e-43f0-4992-a742-7899edbe1ca5` in export
- `type`: ✅ **HANDLED** - Set to 'admin' in export
- `commission_percentage`: ✅ **HANDLED** - Using `discount` value in export
- `deleted_at`: NULL

### 6. Removed Columns Data Preservation

Consider if data from removed columns needs to be:

- Moved to another table
- Stored as metadata
- Archived separately
- Discarded

Columns to consider:

- `group_name`
- `account_balance`

---

## Summary Statistics

### Column Count

- **MySQL CSV**: 14 columns
- **PostgreSQL**: 18 columns
- **New Columns**: 6
- **Removed Columns**: 2
- **Renamed Columns**: 5
- **Type Changed**: 3

### Breaking Changes

1. ⚠️ Primary key type change (INT → UUID)
2. ⚠️ Three columns changed to JSONB (requires data transformation)
3. ⚠️ Two columns removed completely
4. ⚠️ NOT NULL constraints added to critical columns

---

## Recommendations

1. **Before Migration:**

   - Backup `group_name` and `account_balance` data if needed
   - Analyze JSON column data structure requirements
   - Plan UUID mapping strategy

2. **During Migration:**

   - Generate UUID mapping table (old_id → new_uuid)
   - Transform text columns to proper JSON format
   - Set appropriate defaults for new columns
   - Validate NOT NULL constraints are satisfied

3. **After Migration:**
   - Verify all foreign key relationships updated
   - Test JSON queries work as expected
   - Ensure soft delete functionality works
   - Update application code to use new column names

---

**Generated**: For MySQL to PostgreSQL migration analysis
**Status**: Documentation only - no data modifications made
