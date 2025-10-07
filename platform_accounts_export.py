import polars as pl
import uuid
import json


def export_platform_accounts(generate=False):
    """
    Export platform accounts from mt5_users.csv to new_platform_accounts.csv

    Args:
        generate (bool): If True, generates new_platform_accounts.csv file. If False, only previews.
    """

    # Load the platform accounts CSV file
    print("Loading csv/mt5_users.csv...")
    accounts_df = pl.read_csv("csv/mt5_users.csv", infer_schema_length=100000)
    print(f"Loaded {len(accounts_df)} platform accounts")

    # Generate UUID for each account
    print("\nGenerating UUIDs for platform accounts...")
    accounts_df = accounts_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(accounts_df))])
    )

    # Load the original users.csv and new_users.csv to create user_id-to-user_uuid mapping
    print("Loading user files for user mapping...")
    try:
        original_users_df = pl.read_csv("csv/users.csv")
        new_users_df = pl.read_csv("new_users.csv", infer_schema_length=100000)
        user_id_to_uuid = dict(zip(original_users_df["id"], new_users_df["uuid"]))
        print(f"Created user mapping for {len(user_id_to_uuid)} users")

        # Map user_id to user_uuid
        accounts_df = accounts_df.with_columns(
            pl.col("user_id")
            .map_elements(
                lambda x: (
                    user_id_to_uuid.get(x)
                    if x is not None and x in user_id_to_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("user_uuid")
        )

        valid_user_mappings = accounts_df.filter(
            pl.col("user_uuid").is_not_null()
        ).height
        invalid_user_mappings = accounts_df.filter(pl.col("user_uuid").is_null()).height
        print(f"  Valid user mappings: {valid_user_mappings}")
        print(f"  Invalid/missing user mappings: {invalid_user_mappings}")

        # Map identity_status to is_kyc
        print("\nMapping identity_status to is_kyc...")
        user_id_to_identity_status = dict(
            zip(original_users_df["id"], original_users_df["identity_status"])
        )

        # Map identity_status to accounts
        accounts_df = accounts_df.with_columns(
            pl.col("user_id")
            .map_elements(
                lambda x: (
                    user_id_to_identity_status.get(x)
                    if x is not None and x in user_id_to_identity_status
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("identity_status")
        )

        # Create is_kyc column: 1 if identity_status == "completed", else 0
        accounts_df = accounts_df.with_columns(
            pl.when(pl.col("identity_status") == "completed")
            .then(1)
            .otherwise(0)
            .alias("is_kyc")
        )

        kyc_completed = (accounts_df["is_kyc"] == 1).sum()
        kyc_not_completed = (accounts_df["is_kyc"] == 0).sum()
        print(f"  KYC completed (is_kyc=1): {kyc_completed}")
        print(f"  KYC not completed (is_kyc=0): {kyc_not_completed}")

    except FileNotFoundError:
        print("\nWarning: User files not found. Skipping user UUID mapping.")
        print("Please run: uv run main.py --generate --users")
        accounts_df = accounts_df.with_columns(
            pl.lit(None).alias("user_uuid"),
            pl.lit(None).alias("identity_status"),
            pl.lit(0).alias("is_kyc"),
        )

    # Load purchases to create purchase_id-to-purchase_uuid mapping
    print("\nLoading purchase files for purchase mapping...")
    try:
        original_purchases_df = pl.read_csv(
            "csv/purchases.csv", infer_schema_length=100000
        )
        new_purchases_df = pl.read_csv("new_purchases.csv", infer_schema_length=100000)
        purchase_id_to_uuid = dict(
            zip(original_purchases_df["id"], new_purchases_df["uuid"])
        )
        print(f"Created purchase mapping for {len(purchase_id_to_uuid)} purchases")

        # Map purchase_id to purchase_uuid
        accounts_df = accounts_df.with_columns(
            pl.col("purchase_id")
            .map_elements(
                lambda x: (
                    purchase_id_to_uuid.get(x)
                    if x is not None and x in purchase_id_to_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("purchase_uuid")
        )

        valid_purchase_mappings = accounts_df.filter(
            pl.col("purchase_uuid").is_not_null()
        ).height
        invalid_purchase_mappings = accounts_df.filter(
            pl.col("purchase_uuid").is_null()
        ).height
        print(f"  Valid purchase mappings: {valid_purchase_mappings}")
        print(f"  Invalid/missing purchase mappings: {invalid_purchase_mappings}")

    except FileNotFoundError:
        print("\nWarning: Purchase files not found. Skipping purchase UUID mapping.")
        print("Please run: uv run main.py --generate --purchases")
        accounts_df = accounts_df.with_columns(pl.lit(None).alias("purchase_uuid"))

    # Load platform groups to create group name-to-uuid mapping
    print("\nLoading platform groups for group mapping...")
    try:
        platform_groups_df = pl.read_csv("new_platform_groups.csv")
        group_name_to_uuid = dict(
            zip(platform_groups_df["name"], platform_groups_df["uuid"])
        )
        print(f"Created group mapping for {len(group_name_to_uuid)} platform groups")

        # Map group to platform_group_uuid
        accounts_df = accounts_df.with_columns(
            pl.col("group")
            .map_elements(
                lambda x: (
                    group_name_to_uuid.get(x)
                    if x is not None and x in group_name_to_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("platform_group_uuid")
        )

        valid_group_mappings = accounts_df.filter(
            pl.col("platform_group_uuid").is_not_null()
        ).height
        invalid_group_mappings = accounts_df.filter(
            pl.col("platform_group_uuid").is_null()
        ).height
        print(f"  Valid platform group mappings: {valid_group_mappings}")
        print(f"  Invalid/missing group mappings: {invalid_group_mappings}")

    except FileNotFoundError:
        print(
            "\nWarning: new_platform_groups.csv not found. Skipping platform group UUID mapping."
        )
        print("Please run: uv run python filter_platform_groups.py")
        accounts_df = accounts_df.with_columns(
            pl.lit(None).alias("platform_group_uuid")
        )

    # Add updated_at column (copy from created_at)
    if "updated_at" not in accounts_df.columns:
        if "created_at" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("created_at").alias("updated_at")
            )

    # Add deleted_at column (NULL by default)
    if "deleted_at" not in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit(None).alias("deleted_at"))

    # Add platform_login_id column (copy from login)
    if "platform_login_id" not in accounts_df.columns:
        if "login" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("login").alias("platform_login_id")
            )
        else:
            accounts_df = accounts_df.with_columns(
                pl.lit(None).alias("platform_login_id")
            )

    # Add platform_name column (default to 'mt5')
    if "platform_name" not in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit("mt5").alias("platform_name"))

    # Add remote_group_name column (default to 'demo\PropModel\common')
    if "remote_group_name" not in accounts_df.columns:
        accounts_df = accounts_df.with_columns(
            pl.lit("demo\\PropModel\\common").alias("remote_group_name")
        )

    # Add profit_target column (copy from target)
    if "profit_target" not in accounts_df.columns:
        if "target" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("target").alias("profit_target")
            )
        else:
            accounts_df = accounts_df.with_columns(pl.lit(None).alias("profit_target"))

    # Add status column (from is_active: true/1 -> 1, false/0 -> 0)
    if "status" not in accounts_df.columns:
        if "is_active" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("is_active").cast(pl.Int8).alias("status")
            )
        else:
            accounts_df = accounts_df.with_columns(
                pl.lit(0).cast(pl.Int8).alias("status")
            )

    # Add is_trades_check column (from trades_check: 1 -> 1, 0 -> 0, null -> 0)
    if "is_trades_check" not in accounts_df.columns:
        if "trades_check" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("trades_check")
                .fill_null(0)
                .cast(pl.Int8)
                .alias("is_trades_check")
            )
        else:
            accounts_df = accounts_df.with_columns(
                pl.lit(0).cast(pl.Int8).alias("is_trades_check")
            )

    # Add is_trade_agreement column (from contract_sign_staus: 1 -> 1, 0 -> 0, null -> 0)
    if "is_trade_agreement" not in accounts_df.columns:
        if "contract_sign_staus" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("contract_sign_staus")
                .fill_null(0)
                .cast(pl.Int8)
                .alias("is_trade_agreement")
            )
        else:
            accounts_df = accounts_df.with_columns(
                pl.lit(0).cast(pl.Int8).alias("is_trade_agreement")
            )

    # Rename account_type to account_type_old and create new account_type (0 -> 'standard', 1 -> 'aggressive')
    if (
        "account_type" in accounts_df.columns
        and "account_type_old" not in accounts_df.columns
    ):
        accounts_df = accounts_df.rename({"account_type": "account_type_old"})
        accounts_df = accounts_df.with_columns(
            pl.when(pl.col("account_type_old") == 0)
            .then(pl.lit("standard"))
            .when(pl.col("account_type_old") == 1)
            .then(pl.lit("aggressive"))
            .otherwise(pl.lit("standard"))
            .alias("account_type")
        )

    # Add account_stage column mapping from account_stages (0->trial, 1->single, 2->double, 3->triple, 4->instant)
    if "account_stage" not in accounts_df.columns:
        if "account_stages" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.when(pl.col("account_stages") == 0)
                .then(pl.lit("trial"))
                .when(pl.col("account_stages") == 1)
                .then(pl.lit("single"))
                .when(pl.col("account_stages") == 2)
                .then(pl.lit("double"))
                .when(pl.col("account_stages") == 3)
                .then(pl.lit("triple"))
                .when(pl.col("account_stages") == 4)
                .then(pl.lit("instant"))
                .otherwise(pl.lit("trial"))
                .alias("account_stage")
            )
        else:
            accounts_df = accounts_df.with_columns(
                pl.lit("trial").alias("account_stage")
            )

    # Add platform_user_id column (null for now)
    if "platform_user_id" not in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit(None).alias("platform_user_id"))

    # Set action_type to 'challenge'
    if "action_type" in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit("challenge").alias("action_type"))

    # Load column configuration from JSON file
    try:
        with open("platform_accounts_column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include
        columns_to_include = config

        # Filter columns
        # Only keep columns that exist in the dataframe
        existing_columns = [
            col for col in columns_to_include if col in accounts_df.columns
        ]
        missing_columns = [
            col for col in columns_to_include if col not in accounts_df.columns
        ]

        if missing_columns:
            print(
                f"\n⚠️  Warning: The following columns in config were not found in data:"
            )
            for col in missing_columns:
                print(f"  - {col}")

        accounts_df_filtered = accounts_df.select(existing_columns)

        print(
            f"\nPlatform Accounts DataFrame shape: ({len(accounts_df_filtered)}, {len(accounts_df_filtered.columns)})"
        )
        print(f"Selected columns ({len(existing_columns)}): {existing_columns}")

        print(f"\nFirst few rows:")
        print(accounts_df_filtered.head())

        # Save the processed data to new_platform_accounts.csv only if generate flag is True
        if generate:
            accounts_df_filtered.write_csv("new_platform_accounts.csv")
            print(
                f"\nSuccessfully generated new_platform_accounts.csv with {len(accounts_df_filtered)} rows and {len(accounts_df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(existing_columns)}")
        else:
            print("\nTo generate new_platform_accounts.csv, run with --generate flag:")
            print("  uv run main.py --generate --platform-accounts")

    except FileNotFoundError:
        print("\n⚠️  platform_accounts_column_config.json not found!")
        print("Exporting all columns...")

        print(
            f"\nPlatform Accounts DataFrame shape: ({len(accounts_df)}, {len(accounts_df.columns)})"
        )
        print(f"All columns ({len(accounts_df.columns)}): {accounts_df.columns}")

        print(f"\nFirst few rows:")
        print(accounts_df.head())

        if generate:
            accounts_df.write_csv("new_platform_accounts.csv")
            print(
                f"\nSuccessfully generated new_platform_accounts.csv with {len(accounts_df)} rows and {len(accounts_df.columns)} columns"
            )
        else:
            print("\nTo generate new_platform_accounts.csv, run with --generate flag:")
            print("  uv run main.py --generate --platform-accounts")

    except json.JSONDecodeError as e:
        print(f"\n❌ Error parsing platform_accounts_column_config.json: {e}")
        print("Please check the JSON syntax.")
        accounts_df.write_csv("new_platform_accounts.csv")
        print(
            f"\nSuccessfully generated new_platform_accounts.csv with {len(accounts_df)} rows and {len(accounts_df.columns)} columns"
        )
