import polars as pl
import uuid
import json


def export_platform_events(generate=False):
    """
    Export platform events from mt5_events.csv to new_platform_events.csv

    Args:
        generate (bool): If True, generates new_platform_events.csv file. If False, only previews.
    """

    # Load the platform events CSV file
    print("Loading csv/input/mt5_events.csv...")
    events_df = pl.read_csv(
        "csv/input/mt5_events.csv",
        infer_schema_length=100000,
        schema_overrides={
            "login": pl.Utf8
        },  # Read login as string to handle non-numeric values
    )
    print(f"Loaded {len(events_df)} platform event records")

    # Generate UUID for each record
    print("\nGenerating UUIDs for platform events...")
    events_df = events_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(events_df))])
    )

    # Load platform accounts to create login to platform_account_uuid and user_uuid mapping
    print("\nLoading platform accounts for UUID mapping...")
    try:
        platform_accounts_df = pl.read_csv(
            "csv/output/new_platform_accounts.csv", infer_schema_length=100000
        )
        print(f"Loaded {len(platform_accounts_df)} platform accounts")

        # Create mapping from platform_login_id to platform_account_uuid and user_uuid
        login_to_platform_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )
        login_to_user_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["user_uuid"],
            )
        )
        print(
            f"Created platform account UUID mapping for {len(login_to_platform_uuid)} accounts"
        )
        print(f"Created user UUID mapping for {len(login_to_user_uuid)} accounts")

        # Map login to platform_account_uuid
        events_df = events_df.with_columns(
            pl.col("login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_platform_uuid.get(x)
                    if x is not None and x in login_to_platform_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("platform_account_uuid")
        )

        # Map login to user_uuid
        events_df = events_df.with_columns(
            pl.col("login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_user_uuid.get(x)
                    if x is not None and x in login_to_user_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("user_uuid")
        )

        valid_platform_mappings = events_df.filter(
            pl.col("platform_account_uuid").is_not_null()
        ).height
        invalid_platform_mappings = events_df.filter(
            pl.col("platform_account_uuid").is_null()
        ).height
        valid_user_mappings = events_df.filter(pl.col("user_uuid").is_not_null()).height
        invalid_user_mappings = events_df.filter(pl.col("user_uuid").is_null()).height

        print(f"  Valid platform account UUID mappings: {valid_platform_mappings}")
        print(
            f"  Invalid/missing platform account UUID mappings: {invalid_platform_mappings}"
        )
        print(f"  Valid user UUID mappings: {valid_user_mappings}")
        print(f"  Invalid/missing user UUID mappings: {invalid_user_mappings}")

        # Filter out records without valid platform account UUID mappings
        if invalid_platform_mappings > 0:
            print(
                f"\nRemoving {invalid_platform_mappings} records without valid platform account UUID mappings..."
            )
            events_df = events_df.filter(pl.col("platform_account_uuid").is_not_null())
            print(f"Remaining records after filtering: {len(events_df)}")

    except FileNotFoundError:
        print(
            "\n⚠️  Warning: csv/output/new_platform_accounts.csv not found. Skipping UUID mapping."
        )
        print("Please run: uv run main.py --generate --platform-accounts")
        events_df = events_df.with_columns(
            pl.lit(None).alias("platform_account_uuid"), pl.lit(None).alias("user_uuid")
        )

    # Map event types: hardBreach -> hardBreached
    print("\nMapping event types...")
    events_df = events_df.with_columns(
        pl.when(pl.col("event") == "hardBreach")
        .then(pl.lit("hardBreached"))
        .otherwise(pl.col("event"))
        .alias("event")
    )

    # Add reason column (same as event)
    print("Adding reason column (same as event)...")
    events_df = events_df.with_columns(pl.col("event").alias("reason"))

    # Add updated_at column (copy from created_at)
    print("Adding updated_at column...")
    events_df = events_df.with_columns(pl.col("created_at").alias("updated_at"))

    # Convert created_at to proper timestamp format
    if "created_at" in events_df.columns:
        events_df = events_df.with_columns(
            pl.col("created_at")
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            .alias("created_at")
        )

    # Convert updated_at to proper timestamp format
    if "updated_at" in events_df.columns:
        events_df = events_df.with_columns(
            pl.col("updated_at")
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            .alias("updated_at")
        )

    # Remove columns not needed in new schema
    columns_to_remove = ["id", "user_id", "login", "hourl_gap_status"]
    for col in columns_to_remove:
        if col in events_df.columns:
            events_df = events_df.drop(col)

    # Show event type statistics
    print("\nEvent type statistics:")
    event_stats = (
        events_df.group_by("event")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
    )
    print(event_stats)

    # Load column configuration from JSON file
    try:
        with open("config/platform_events_column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include
        columns_to_include = config

        # Filter columns
        # Only keep columns that exist in the dataframe
        existing_columns = [
            col for col in columns_to_include if col in events_df.columns
        ]
        missing_columns = [
            col for col in columns_to_include if col not in events_df.columns
        ]

        if missing_columns:
            print(
                f"\n⚠️  Warning: The following columns in config were not found in data:"
            )
            for col in missing_columns:
                print(f"  - {col}")

        events_df_filtered = events_df.select(existing_columns)

        print(
            f"\nPlatform Events DataFrame shape: ({len(events_df_filtered)}, {len(events_df_filtered.columns)})"
        )
        print(f"Selected columns ({len(existing_columns)}): {existing_columns}")

        print(f"\nFirst few rows:")
        print(events_df_filtered.head())

        # Save the processed data to csv/output/new_platform_events.csv only if generate flag is True
        if generate:
            events_df_filtered.write_csv("csv/output/new_platform_events.csv")
            print(
                f"\nSuccessfully generated csv/output/new_platform_events.csv with {len(events_df_filtered)} rows and {len(events_df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(existing_columns)}")
        else:
            print(
                "\nTo generate csv/output/new_platform_events.csv, run with --generate flag:"
            )
            print("  uv run main.py --generate --platform-events")

    except FileNotFoundError:
        print("\n⚠️  config/platform_events_column_config.json not found!")
        print("Exporting all columns...")

        print(
            f"\nPlatform Events DataFrame shape: ({len(events_df)}, {len(events_df.columns)})"
        )
        print(f"All columns ({len(events_df.columns)}): {events_df.columns}")

        print(f"\nFirst few rows:")
        print(events_df.head())

        if generate:
            events_df.write_csv("csv/output/new_platform_events.csv")
            print(
                f"\nSuccessfully generated csv/output/new_platform_events.csv with {len(events_df)} rows and {len(events_df.columns)} columns"
            )
        else:
            print(
                "\nTo generate csv/output/new_platform_events.csv, run with --generate flag:"
            )
            print("  uv run main.py --generate --platform-events")

    except json.JSONDecodeError as e:
        print(f"\n❌ Error parsing config/platform_events_column_config.json: {e}")
        print("Please check the JSON syntax.")
        events_df.write_csv("csv/output/new_platform_events.csv")
        print(
            f"\nSuccessfully generated csv/output/new_platform_events.csv with {len(events_df)} rows and {len(events_df.columns)} columns"
        )
