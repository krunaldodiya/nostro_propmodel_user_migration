import polars as pl
import json


def export_platform_groups(generate=False):
    """
    Export platform groups from platform_groups.csv to new_platform_groups.csv

    Args:
        generate (bool): If True, generates new_platform_groups.csv file. If False, only previews.
    """

    # Filter to only keep challenge groups (group_type = 'challenge')
    # This will automatically include all challenge groups from the CSV
    print("Filtering to only keep challenge groups...")

    print("Loading csv/input/platform_groups.csv...")
    groups_df = pl.read_csv("csv/input/platform_groups.csv")
    print(f"Loaded {len(groups_df)} platform groups")

    # Show unique group names that start with 'demo\'
    demo_groups = (
        groups_df.filter(pl.col("name").str.starts_with("demo\\"))
        .select("name")
        .unique()
    )
    print(f"Found {len(demo_groups)} unique groups starting with 'demo\\' in the file")

    # Filter to only include challenge groups (group_type = 'challenge')
    # AND groups that start with 'demo\Nostro' (handles both single and double backslashes)
    filtered_df = groups_df.filter(
        (pl.col("group_type") == "challenge")
        & (
            pl.col("name").str.starts_with("demo\\Nostro")
            | pl.col("name").str.starts_with("demo\\\\Nostro")
        )
    )
    print(
        f"Filtered to {len(filtered_df)} challenge groups starting with 'demo\\Nostro' (handles backslashes)"
    )

    # Ensure platform_name is always uppercase "MT5"
    if "platform_name" in filtered_df.columns:
        filtered_df = filtered_df.with_columns(
            pl.col("platform_name").str.to_uppercase().alias("platform_name")
        )
        print("Updated platform_name to uppercase: MT5")

    # Show the groups that were found
    found_groups = filtered_df.select("name").unique().sort("name")
    print(f"Challenge groups found: {len(found_groups)}")
    print("Challenge group names:")
    print(found_groups)

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

    # Save the processed data to csv/output/new_platform_groups.csv only if generate flag is True
    if generate:
        filtered_df.write_csv("csv/output/new_platform_groups.csv")
        print(
            f"\nSuccessfully generated csv/output/new_platform_groups.csv with {len(filtered_df)} rows and {len(filtered_df.columns)} columns"
        )
    else:
        print(
            "\nTo generate csv/output/new_platform_groups.csv, run with --generate flag:"
        )
        print("  uv run main.py --generate --platform-groups")
