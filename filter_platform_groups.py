import polars as pl

# Define the groups to keep (all start with 'demo\Nostro\')
GROUPS_TO_KEEP = [
    "demo\\Nostro\\U-SST-1-B",
    "demo\\Nostro\\U-SAG-1-B",
    "demo\\Nostro\\U-DST-1-B",
    "demo\\Nostro\\U-DST-2-B",
    "demo\\Nostro\\U-DAG-1-B",
    "demo\\Nostro\\U-DAG-2-B",
    "demo\\Nostro\\U-TPS-1-B",
    "demo\\Nostro\\U-TPS-2-B",
    "demo\\Nostro\\U-TPS-3-B",
    "demo\\Nostro\\U-TPA-1-B",
    "demo\\Nostro\\U-TPA-2-B",
    "demo\\Nostro\\U-TPA-3-B",
    "demo\\Nostro\\U-FTE-1-B",
    "demo\\Nostro\\U-FTE-2-B",
    "demo\\Nostro\\U-FTE-3-B",
    "demo\\Nostro\\U-FTF-1-B",
    "demo\\Nostro\\U-FTF-1-A",
    "demo\\Nostro\\U-COF-1-B",
    "demo\\Nostro\\U-COP-1-B",
    "demo\\Nostro\\U-SSF-1-B",
    "demo\\Nostro\\U-SSF-1-A",
    "demo\\Nostro\\U-SAF-1-B",
    "demo\\Nostro\\U-SAF-1-A",
    "demo\\Nostro\\U-DSF-1-B",
    "demo\\Nostro\\U-DSF-1-A",
    "demo\\Nostro\\U-DAF-1-B",
    "demo\\Nostro\\U-DAF-1-A",
    "demo\\Nostro\\U-TSF-1-B",
    "demo\\Nostro\\U-TSF-1-A",
    "demo\\Nostro\\U-TAF-1-B",
    "demo\\Nostro\\U-TAF-1-A",
]

print("Loading platform_groups.csv...")
df = pl.read_csv("platform_groups.csv")
print(f"Total rows in original file: {len(df)}")

# Show unique group names that start with 'demo\'
demo_groups = (
    df.filter(pl.col("name").str.starts_with("demo\\")).select("name").unique()
)
print(f"\nFound {len(demo_groups)} unique groups starting with 'demo\\' in the file:")
print(demo_groups)

# Filter to only include rows where 'name' is in our GROUPS_TO_KEEP list
filtered_df = df.filter(pl.col("name").is_in(GROUPS_TO_KEEP))
print(f"\nFiltered rows matching our groups list: {len(filtered_df)}")

# Show the groups that were found
found_groups = filtered_df.select("name").unique().sort("name")
print(f"\nMatched groups ({len(found_groups)}):")
print(found_groups)

# Check which groups from our list are missing in the file
found_group_set = set(found_groups.to_series().to_list())
missing_groups = [g for g in GROUPS_TO_KEEP if g not in found_group_set]
if missing_groups:
    print(f"\n⚠️  Groups in list but NOT found in CSV ({len(missing_groups)}):")
    for g in missing_groups:
        print(f"  - {g}")
else:
    print("\n✅ All groups from the list were found in the CSV!")

# Show summary statistics
print(f"\n{'='*70}")
print(f"{'SUMMARY':^70}")
print(f"{'='*70}")
print(f"  Original rows: {len(df)}")
print(f"  Filtered rows: {len(filtered_df)}")
print(f"  Removed rows: {len(df) - len(filtered_df)}")
print(f"  Groups to keep: {len(GROUPS_TO_KEEP)}")
print(f"  Groups found: {len(found_groups)}")
print(f"  Groups missing: {len(missing_groups)}")
print(f"{'='*70}\n")

# Preview filtered data
print("Preview of filtered data (first 10 rows):")
print(
    filtered_df.select(
        [
            "uuid",
            "name",
            "description",
            "initial_balance",
            "account_stage",
            "account_type",
        ]
    ).head(10)
)

# Save filtered data
output_file = "new_platform_groups.csv"
filtered_df.write_csv(output_file)
print(f"\n✅ Filtered data saved to: {output_file}")
print(f"   Rows: {len(filtered_df)}")
print(f"   Columns: {len(filtered_df.columns)}")
