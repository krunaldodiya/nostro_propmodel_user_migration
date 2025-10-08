# Purchase Mapping Explained

## How the Mapping Works

### Current Logic in `platform_accounts_export.py` (lines 298-336)

```python
# Step 1: Load original purchases (with IDs)
original_purchases_df = pl.read_csv("csv/purchases.csv")

# Step 2: Load exported purchases (with UUIDs)
new_purchases_df = pl.read_csv("new_purchases.csv")

# Step 3: Create mapping dictionary: purchase_id → purchase_uuid
purchase_id_to_uuid = dict(
    zip(original_purchases_df["id"], new_purchases_df["uuid"])
)

# Step 4: For each mt5_user account, map purchase_id to purchase_uuid
accounts_df = accounts_df.with_columns(
    pl.col("purchase_id").map_elements(
        lambda x: purchase_id_to_uuid.get(x) if x in purchase_id_to_uuid else None
    ).alias("purchase_uuid")
)
```

### The Mapping Process

```
csv/purchases.csv          new_purchases.csv           mt5_users.csv
┌──────────────┐          ┌────────────────┐          ┌──────────────┐
│ id  │ data   │    →     │ uuid  │ data   │    ←     │ purchase_id  │
├──────────────┤          ├────────────────┤          ├──────────────┤
│ 54  │ ...    │          │ uuid1 │ ...    │          │ 54    ✓      │
│ 100 │ ...    │          │ uuid2 │ ...    │          │ 100   ✓      │
│ ... │ ...    │          │ ...   │ ...    │          │ 124782 ✗     │  ← Missing!
│ 258523│ ...  │          │ uuid3 │ ...    │          │ 258601 ✗     │  ← Not in CSV!
└──────────────┘          └────────────────┘          └──────────────┘
     89,135 records            89,135 UUIDs              87,984 unique IDs
                                                         (526 missing)
```

## The Problem

### Issue: Missing Purchase Records

**526 purchase IDs** referenced in `mt5_users.csv` **don't exist** in `csv/purchases.csv`:

```
Missing IDs: 124782, 124850, 124851, ... 258601
```

These fall into two categories:

### Category 1: Gaps in ID Sequence (Old Records)

```
IDs like: 124782, 124850, 125002, etc.
These are within the ID range (54-258523) but missing from csv/purchases.csv
Likely: Deleted purchases or data corruption
```

### Category 2: IDs Beyond Max (New Records)

```
Max ID in csv/purchases.csv: 258523
Missing IDs: 258524-258601 (78 records)
These are recent purchases created after the csv was exported
```

### Impact

```
Total mt5_user accounts: 89,881
Accounts with purchase_id: 89,878
Accounts that can be mapped: 89,330 ✓
Accounts that cannot be mapped: 548 ✗ (0.61%)
```

## Why the Mapping is Correct

The mapping logic itself is **CORRECT** and working as designed:

1. ✅ It properly zips purchase IDs with UUIDs
2. ✅ It handles missing IDs by setting them to NULL
3. ✅ All purchase_uuids that ARE generated are valid

**The issue is with the source data**, not the mapping logic.

## Root Cause

```
csv/purchases.csv is INCOMPLETE or OUTDATED
- Missing 526 purchase records that mt5_users references
- Needs to be re-exported from the source database
```

## Solutions

### Solution 1: Re-export purchases.csv (Recommended)

```bash
# Export complete purchases from database with ALL records
# Then regenerate:
uv run main.py --generate --purchases
uv run main.py --generate --platform-accounts
```

### Solution 2: Filter orphaned accounts

```python
# Add to platform_accounts_export.py before final export:
accounts_df = accounts_df.filter(pl.col("purchase_uuid").is_not_null())
print(f"Filtered out {548} accounts with missing purchase records")
```

### Solution 3: Allow NULL in PostgreSQL

```sql
-- Change schema to allow NULL (not recommended)
ALTER TABLE platform_accounts
ALTER COLUMN purchase_uuid DROP NOT NULL;
```

## Verification

Run the verification script:

```bash
uv run python check_purchase_mapping.py
```

This shows:

- How many purchases are in the mapping
- How many purchase_ids are needed
- Which purchase_ids are missing
- How many accounts are affected
