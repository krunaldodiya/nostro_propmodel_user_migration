#!/usr/bin/env python3
"""
Verify that all purchase_uuid in new_platform_accounts.csv exist in new_purchases.csv
This checks the GENERATED CSV files, not the source files.
"""
import polars as pl
import sys

print("=" * 80)
print("VERIFYING PURCHASE UUIDs IN EXPORTED FILES")
print("=" * 80)

# Check if files exist
try:
    print("\n1. Loading new_platform_accounts.csv...")
    platform_accounts_df = pl.read_csv(
        "new_platform_accounts.csv", infer_schema_length=100000
    )
    print(f"   ✓ Loaded {len(platform_accounts_df)} platform accounts")
except FileNotFoundError:
    print("   ✗ new_platform_accounts.csv not found!")
    print("   Please run: uv run main.py --generate --platform-accounts")
    sys.exit(1)

try:
    print("\n2. Loading new_purchases.csv...")
    purchases_df = pl.read_csv("new_purchases.csv", infer_schema_length=100000)
    print(f"   ✓ Loaded {len(purchases_df)} purchases")
except FileNotFoundError:
    print("   ✗ new_purchases.csv not found!")
    print("   Please run: uv run main.py --generate --purchases")
    sys.exit(1)

print("\n" + "=" * 80)
print("ANALYSIS")
print("=" * 80)

# Count platform accounts with non-null purchase_uuid
accounts_with_purchase = platform_accounts_df.filter(
    pl.col("purchase_uuid").is_not_null()
)
accounts_without_purchase = platform_accounts_df.filter(
    pl.col("purchase_uuid").is_null()
)

print(f"\n📊 Platform Accounts:")
print(f"   Total accounts: {len(platform_accounts_df):,}")
print(f"   With purchase_uuid: {len(accounts_with_purchase):,}")
print(f"   Without purchase_uuid (NULL): {len(accounts_without_purchase):,}")

if len(accounts_without_purchase) > 0:
    print(
        f"\n   ⚠️  Warning: {len(accounts_without_purchase)} accounts have NULL purchase_uuid"
    )
    print(
        f"   These accounts cannot be imported to PostgreSQL if purchase_uuid is NOT NULL"
    )

# Get unique purchase_uuids from platform accounts
unique_purchase_uuids_in_accounts = set(
    accounts_with_purchase["purchase_uuid"].unique().to_list()
)
print(
    f"\n📊 Unique purchase_uuid in platform accounts: {len(unique_purchase_uuids_in_accounts):,}"
)

# Get unique purchase_uuids from purchases
unique_purchase_uuids_in_purchases = set(purchases_df["uuid"].unique().to_list())
print(f"📊 Unique uuid in purchases: {len(unique_purchase_uuids_in_purchases):,}")

# Find missing purchase_uuids
missing_purchase_uuids = (
    unique_purchase_uuids_in_accounts - unique_purchase_uuids_in_purchases
)

print("\n" + "=" * 80)
print("VERIFICATION RESULTS")
print("=" * 80)

if len(missing_purchase_uuids) == 0 and len(accounts_without_purchase) == 0:
    print("\n✅ ✅ ✅ PERFECT! ✅ ✅ ✅")
    print("\n   All purchase_uuid values exist in purchases!")
    print("   No NULL purchase_uuid values found!")
    print("\n   ✓ Safe to import to PostgreSQL")

elif len(missing_purchase_uuids) == 0 and len(accounts_without_purchase) > 0:
    print(f"\n✅ All non-NULL purchase_uuid values exist in purchases!")
    print(
        f"\n⚠️  BUT: {len(accounts_without_purchase)} accounts have NULL purchase_uuid"
    )
    print(f"\n   Options:")
    print(f"   1. Filter out accounts with NULL purchase_uuid before import")
    print(f"   2. Make purchase_uuid nullable in PostgreSQL schema")
    print(f"   3. Investigate why these accounts don't have purchase mappings")

    # Show sample accounts with NULL purchase_uuid
    print(f"\n   Sample accounts with NULL purchase_uuid (first 10):")
    sample = accounts_without_purchase.select(
        ["uuid", "platform_login_id", "user_uuid", "purchase_uuid"]
    ).head(10)
    print(sample)

else:
    print(
        f"\n❌ PROBLEM: {len(missing_purchase_uuids)} purchase_uuid values are MISSING from purchases!"
    )

    # Count how many accounts are affected
    affected_accounts = accounts_with_purchase.filter(
        pl.col("purchase_uuid").is_in(list(missing_purchase_uuids))
    )

    print(f"\n   Affected accounts: {len(affected_accounts):,}")
    print(f"   Missing purchase_uuids: {len(missing_purchase_uuids):,}")

    # Show sample of missing purchase_uuids
    print(f"\n   Sample missing purchase_uuids (first 10):")
    for i, uuid in enumerate(list(missing_purchase_uuids)[:10], 1):
        print(f"     {i}. {uuid}")

    # Show sample affected accounts
    print(f"\n   Sample affected accounts (first 10):")
    print(
        affected_accounts.select(
            ["uuid", "platform_login_id", "purchase_uuid", "user_uuid"]
        ).head(10)
    )

    # Export missing purchase_uuids to file
    with open("missing_purchase_uuids_in_exports.txt", "w") as f:
        f.write("# Missing purchase_uuid values in EXPORTED files\n")
        f.write(
            f"# These {len(missing_purchase_uuids)} UUIDs exist in new_platform_accounts.csv\n"
        )
        f.write(f"# but NOT in new_purchases.csv\n")
        f.write(f"# Affected accounts: {len(affected_accounts)}\n\n")
        for uuid in sorted(missing_purchase_uuids):
            f.write(f"{uuid}\n")

    print(f"\n   ✓ Exported missing UUIDs to: missing_purchase_uuids_in_exports.txt")

    # Export affected accounts details
    affected_accounts.select(
        [
            "uuid",
            "platform_login_id",
            "purchase_uuid",
            "user_uuid",
            "initial_balance",
            "funded_at",
            "created_at",
        ]
    ).write_csv("affected_platform_accounts_exports.csv")

    print(f"   ✓ Exported affected accounts to: affected_platform_accounts_exports.csv")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Platform accounts: {len(platform_accounts_df):,}")
print(f"  - With purchase_uuid: {len(accounts_with_purchase):,}")
print(f"  - With NULL purchase_uuid: {len(accounts_without_purchase):,}")
print(f"Purchases: {len(purchases_df):,}")
print(f"Missing purchase_uuids: {len(missing_purchase_uuids):,}")

if len(missing_purchase_uuids) == 0 and len(accounts_without_purchase) == 0:
    print(f"\nStatus: ✅ READY FOR IMPORT")
elif len(missing_purchase_uuids) == 0:
    print(f"\nStatus: ⚠️  NEEDS REVIEW (NULL purchase_uuids)")
else:
    print(f"\nStatus: ❌ NOT READY (Missing purchase_uuids)")

print("=" * 80)
