import polars as pl
import uuid
import json


def export_breach_account_activities(generate=False):
    """
    Export breach account activities from BreachAccountActivity.csv to new_breach_account_activities.csv

    Args:
        generate (bool): If True, generates new_breach_account_activities.csv file. If False, only previews.
    """

    # Load the breach account activities CSV file
    print("Loading csv/input/BreachAccountActivity.csv...")
    breach_df = pl.read_csv(
        "csv/input/BreachAccountActivity.csv", infer_schema_length=100000
    )
    print(f"Loaded {len(breach_df)} breach account activity records")

    # Generate UUID for each record
    print("\nGenerating UUIDs for breach account activities...")
    breach_df = breach_df.with_columns(
        pl.Series("id", [str(uuid.uuid4()) for _ in range(len(breach_df))])
    )

    # Load platform accounts to create account_login to platform_account_uuid mapping
    print("\nLoading platform accounts for UUID mapping...")
    try:
        platform_accounts_df = pl.read_csv(
            "csv/output/new_platform_accounts.csv", infer_schema_length=100000
        )
        print(f"Loaded {len(platform_accounts_df)} platform accounts")

        # Create mapping from platform_login_id to platform_account_uuid
        login_to_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )
        print(
            f"Created platform account UUID mapping for {len(login_to_uuid)} accounts"
        )

        # Map account_login to platform_account_uuid
        breach_df = breach_df.with_columns(
            pl.col("account_login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_uuid.get(x)
                    if x is not None and x in login_to_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("platform_account_uuid")
        )

        valid_mappings = breach_df.filter(
            pl.col("platform_account_uuid").is_not_null()
        ).height
        invalid_mappings = breach_df.filter(
            pl.col("platform_account_uuid").is_null()
        ).height
        print(f"  Valid platform account UUID mappings: {valid_mappings}")
        print(f"  Invalid/missing platform account UUID mappings: {invalid_mappings}")

        # Filter out records without valid platform account UUID mappings
        if invalid_mappings > 0:
            print(
                f"\nRemoving {invalid_mappings} records without valid platform account UUID mappings..."
            )
            breach_df = breach_df.filter(pl.col("platform_account_uuid").is_not_null())
            print(f"Remaining records after filtering: {len(breach_df)}")

    except FileNotFoundError:
        print(
            "\n⚠️  Warning: csv/output/new_platform_accounts.csv not found. Skipping platform account UUID mapping."
        )
        print("Please run: uv run main.py --generate --platform-accounts")
        breach_df = breach_df.with_columns(pl.lit(None).alias("platform_account_uuid"))

    # Rename columns to match the database schema
    print("\nRenaming columns to match database schema...")

    # Rename BreachCount to breach_count
    if "BreachCount" in breach_df.columns:
        breach_df = breach_df.rename({"BreachCount": "breach_count"})

    # Rename LastBreachDate to last_breach_date
    if "LastBreachDate" in breach_df.columns:
        breach_df = breach_df.rename({"LastBreachDate": "last_breach_date"})

    # Rename IsBreached to is_breached
    if "IsBreached" in breach_df.columns:
        breach_df = breach_df.rename({"IsBreached": "is_breached"})

    # Convert is_breached from int to bool
    if "is_breached" in breach_df.columns:
        breach_df = breach_df.with_columns(
            pl.col("is_breached").cast(pl.Boolean).alias("is_breached")
        )

    # Convert last_breach_date to proper timestamp format
    if "last_breach_date" in breach_df.columns:
        breach_df = breach_df.with_columns(
            pl.col("last_breach_date")
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            .alias("last_breach_date")
        )

    # Remove the original Id column as we're using our generated UUID
    if "Id" in breach_df.columns:
        breach_df = breach_df.drop("Id")

    # Remove account_login column as we've mapped it to platform_account_uuid
    if "account_login" in breach_df.columns:
        breach_df = breach_df.drop("account_login")

    # Load column configuration from JSON file
    try:
        with open("config/breach_account_activities_column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include
        columns_to_include = config

        # Filter columns
        # Only keep columns that exist in the dataframe
        existing_columns = [
            col for col in columns_to_include if col in breach_df.columns
        ]
        missing_columns = [
            col for col in columns_to_include if col not in breach_df.columns
        ]

        if missing_columns:
            print(
                f"\n⚠️  Warning: The following columns in config were not found in data:"
            )
            for col in missing_columns:
                print(f"  - {col}")

        breach_df_filtered = breach_df.select(existing_columns)

        print(
            f"\nBreach Account Activities DataFrame shape: ({len(breach_df_filtered)}, {len(breach_df_filtered.columns)})"
        )
        print(f"Selected columns ({len(existing_columns)}): {existing_columns}")

        print(f"\nFirst few rows:")
        print(breach_df_filtered.head())

        # Save the processed data to csv/output/new_breach_account_activities.csv only if generate flag is True
        if generate:
            breach_df_filtered.write_csv("csv/output/new_breach_account_activities.csv")
            print(
                f"\nSuccessfully generated csv/output/new_breach_account_activities.csv with {len(breach_df_filtered)} rows and {len(breach_df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(existing_columns)}")
        else:
            print(
                "\nTo generate csv/output/new_breach_account_activities.csv, run with --generate flag:"
            )
            print("  uv run main.py --generate --breach-account-activities")

    except FileNotFoundError:
        print("\n⚠️  config/breach_account_activities_column_config.json not found!")
        print("Exporting all columns...")

        print(
            f"\nBreach Account Activities DataFrame shape: ({len(breach_df)}, {len(breach_df.columns)})"
        )
        print(f"All columns ({len(breach_df.columns)}): {breach_df.columns}")

        print(f"\nFirst few rows:")
        print(breach_df.head())

        if generate:
            breach_df.write_csv("csv/output/new_breach_account_activities.csv")
            print(
                f"\nSuccessfully generated csv/output/new_breach_account_activities.csv with {len(breach_df)} rows and {len(breach_df.columns)} columns"
            )
        else:
            print(
                "\nTo generate csv/output/new_breach_account_activities.csv, run with --generate flag:"
            )
            print("  uv run main.py --generate --breach-account-activities")

    except json.JSONDecodeError as e:
        print(
            f"\n❌ Error parsing config/breach_account_activities_column_config.json: {e}"
        )
        print("Please check the JSON syntax.")
        breach_df.write_csv("csv/output/new_breach_account_activities.csv")
        print(
            f"\nSuccessfully generated csv/output/new_breach_account_activities.csv with {len(breach_df)} rows and {len(breach_df.columns)} columns"
        )
