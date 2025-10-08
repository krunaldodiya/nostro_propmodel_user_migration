# Purchase UUID Import Issue - Solutions

## Problem Summary

- **548 platform accounts** have NULL `purchase_uuid`
- **Root Cause**: 526 `purchase_id` values in `mt5_users.csv` don't exist in `purchases.csv`
- **PostgreSQL Error**: `purchase_uuid` column is defined as `NOT NULL` in the schema

## Data Analysis

```
Total platform accounts: 89,878
Accounts with NULL purchase_uuid: 548 (0.61%)
Missing purchase_ids: 526
```

Sample missing purchase_ids: 258595, 258596, 258598, 258599, 258601, etc.
These are recent purchases (created 2025-10-06)

## Solution Options

### Option 1: Filter Out Orphaned Accounts (Recommended for Quick Fix)

**Action**: Exclude the 548 accounts from the export

**Pros:**

- Simple and fast
- Ensures data integrity
- No schema changes needed

**Cons:**

- Loses 548 accounts (0.61% of data)
- May lose important recent data

**Implementation**:

```python
# In platform_accounts_export.py, add before column filtering:
accounts_df = accounts_df.filter(pl.col("purchase_uuid").is_not_null())
```

### Option 2: Update purchases.csv (Recommended for Complete Data)

**Action**: Get updated `purchases.csv` with the missing 526 purchases

**Pros:**

- Preserves all data
- Maintains referential integrity
- No data loss

**Cons:**

- Requires access to source database
- Need to regenerate purchases export

**Steps:**

1. Export purchases with `id >= 258074` from source database
2. Regenerate `new_purchases.csv` with complete data
3. Regenerate `new_platform_accounts.csv`

### Option 3: Allow NULL in PostgreSQL Schema

**Action**: Change PostgreSQL schema to allow NULL for `purchase_uuid`

**Pros:**

- Can import all accounts
- No data loss

**Cons:**

- Breaks referential integrity
- May cause issues with application logic
- Not recommended for production

**Implementation**:

```sql
ALTER TABLE platform_accounts
ALTER COLUMN purchase_uuid DROP NOT NULL;
```

### Option 4: Create Placeholder Purchases

**Action**: Create placeholder purchase records for the missing purchase_ids

**Pros:**

- Maintains referential integrity
- Preserves all accounts

**Cons:**

- Creates fake data
- May cause issues with business logic
- Not recommended for production

## Recommended Approach

**Best Solution**: **Option 2** (Update purchases.csv)

- Get the complete purchases data
- This ensures data integrity and completeness

**Quick Fix**: **Option 1** (Filter out orphaned accounts)

- If you need to import immediately
- Accept the loss of 548 accounts (0.61%)

## Files Generated for Investigation

1. `accounts_with_null_purchase_uuid.csv` - All 548 affected accounts
2. `missing_purchase_ids.txt` - List of 526 missing purchase IDs (coming next)

## Next Steps

1. Check if you have access to complete purchases data
2. If yes, regenerate purchases.csv with ids >= 258074
3. If no, decide between Options 1 or 3
