import polars as pl
import json


def export_platform_groups(generate=False):
    """
    Export platform groups from platform_groups.csv to new_platform_groups.csv

    Args:
        generate (bool): If True, generates new_platform_groups.csv file. If False, only previews.
    """

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

    print("Loading csv/platform_groups.csv...")
    groups_df = pl.read_csv("csv/platform_groups.csv")
    print(f"Loaded {len(groups_df)} platform groups")

    # Show unique group names that start with 'demo\'
    demo_groups = (
        groups_df.filter(pl.col("name").str.starts_with("demo\\"))
        .select("name")
        .unique()
    )
    print(f"Found {len(demo_groups)} unique groups starting with 'demo\\' in the file")

    # Filter to only include rows where 'name' is in our GROUPS_TO_KEEP list
    filtered_df = groups_df.filter(pl.col("name").is_in(GROUPS_TO_KEEP))
    print(f"Filtered to {len(filtered_df)} groups matching our required list")

    # Show the groups that were found
    found_groups = filtered_df.select("name").unique().sort("name")
    print(f"Matched groups: {len(found_groups)}")

    # Check which groups from our list are missing in the file
    found_group_set = set(found_groups.to_series().to_list())
    missing_groups = [g for g in GROUPS_TO_KEEP if g not in found_group_set]

    if missing_groups:
        print(f"\n⚠️  Warning: {len(missing_groups)} groups in list NOT found in CSV:")
        for g in missing_groups:
            print(f"  - {g}")
    else:
        print("✅ All groups from the required list were found in CSV!")

    # Load column configuration from JSON file if it exists
    try:
        with open("config/platform_groups_column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include
        columns_to_include = config

        # Filter DataFrame to only include selected columns
        available_columns = [
            col for col in columns_to_include if col in filtered_df.columns
        ]
        missing_columns = [
            col for col in columns_to_include if col not in filtered_df.columns
        ]

        if missing_columns:
            print(
                f"\nWarning: The following columns are not available: {missing_columns}"
            )

        filtered_df = filtered_df.select(available_columns)

        print(f"\nPlatform Groups DataFrame shape: {filtered_df.shape}")
        print(f"Selected columns ({len(available_columns)}): {available_columns}")

    except FileNotFoundError:
        print("\nNote: config/platform_groups_column_config.json not found.")
        print("Using all columns from CSV.")
        print(f"\nPlatform Groups DataFrame shape: {filtered_df.shape}")
        print(f"All columns ({len(filtered_df.columns)}): {filtered_df.columns}")

    except json.JSONDecodeError as e:
        print(
            f"\nError: Invalid JSON in config/platform_groups_column_config.json: {e}"
        )
        print("Using all columns instead.")

    # Show preview
    print("\nFirst few rows:")
    print(filtered_df.head())

    # Save the processed data to new_platform_groups.csv only if generate flag is True
    if generate:
        filtered_df.write_csv("new_platform_groups.csv")
        print(
            f"\nSuccessfully generated new_platform_groups.csv with {len(filtered_df)} rows and {len(filtered_df.columns)} columns"
        )
    else:
        print("\nTo generate new_platform_groups.csv, run with --generate flag:")
        print("  uv run main.py --generate --platform-groups")
