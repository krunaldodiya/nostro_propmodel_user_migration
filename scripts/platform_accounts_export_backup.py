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
    print("Loading csv/input/mt5_users.csv...")
    accounts_df = pl.read_csv("csv/input/mt5_users.csv", infer_schema_length=100000)
    print(f"Loaded {len(accounts_df)} platform accounts")

    # Filter out TradeLocker entries (login IDs starting with "D#")
    print("\nFiltering out TradeLocker entries...")
    tradelocker_count = accounts_df.filter(pl.col("login").str.starts_with("D#")).height
    print(f"Found {tradelocker_count} TradeLocker entries to remove")

    accounts_df = accounts_df.filter(~pl.col("login").str.starts_with("D#"))
    print(f"Remaining MT5 accounts after filtering: {len(accounts_df)}")

    # Check for and handle duplicate purchase_ids and logins
    print("\nChecking for duplicate purchase_ids and logins...")

    # Check purchase_id duplicates
    unique_purchase_ids = accounts_df["purchase_id"].n_unique()
    total_purchase_ids = len(accounts_df)
    purchase_duplicates = total_purchase_ids - unique_purchase_ids

    if purchase_duplicates > 0:
        print(
            f"Found {purchase_duplicates} duplicate purchase_ids out of {total_purchase_ids} total records"
        )
        print(
            "Removing duplicates by keeping the first occurrence of each purchase_id..."
        )
        accounts_df = accounts_df.unique(subset=["purchase_id"], keep="first")
        print(f"After removing purchase_id duplicates: {len(accounts_df)} records")
        print(
            f"Removed {total_purchase_ids - len(accounts_df)} duplicate purchase_id records"
        )
    else:
        print("✅ No duplicate purchase_ids found")

    # Check login duplicates
    unique_logins = accounts_df["login"].n_unique()
    total_logins = len(accounts_df)
    login_duplicates = total_logins - unique_logins

    if login_duplicates > 0:
        print(
            f"Found {login_duplicates} duplicate logins out of {total_logins} total records"
        )
        print("Removing duplicates by keeping the first occurrence of each login...")
        accounts_df = accounts_df.unique(subset=["login"], keep="first")
        print(f"After removing login duplicates: {len(accounts_df)} records")
        print(f"Removed {total_logins - len(accounts_df)} duplicate login records")
    else:
        print("✅ No duplicate logins found")

    # Load account_stats.csv for group mapping (primary source)
    print("\nLoading csv/input/account_stats.csv for group mapping...")
    try:
        account_stats_df = pl.read_csv(
            "csv/input/account_stats.csv", infer_schema_length=100000
        )
        print(f"Loaded {len(account_stats_df)} account stats records")

        # Create mapping from account_login to group
        # Filter out empty/null groups from account_stats
        account_stats_with_group = account_stats_df.filter(
            pl.col("group").is_not_null() & (pl.col("group") != "")
        )

        login_to_group_stats = dict(
            zip(
                account_stats_with_group["account_login"].cast(pl.Utf8),
                account_stats_with_group["group"],
            )
        )
        print(
            f"Created group mapping from account_stats for {len(login_to_group_stats)} accounts"
        )

        # Store original group from mt5_users as backup
        accounts_df = accounts_df.with_columns(pl.col("group").alias("group_backup"))

        # Map group from account_stats (primary), fallback to mt5_users group (backup)
        accounts_df = accounts_df.with_columns(
            pl.col("login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_group_stats.get(x)
                    if x is not None and x in login_to_group_stats
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("group_from_stats")
        )

        # Keep original group for funded status logic, use account_stats group for reference only
        # The 'group' column will remain as the original mt5_users group for funded status logic
        # The 'group_from_stats' will be used as 'remote_group_name' for reference
        # DO NOT override the original group - it's needed for funded status logic

        # Count the sources
        from_stats = accounts_df.filter(pl.col("group_from_stats").is_not_null()).height
        from_backup = accounts_df.filter(
            pl.col("group_from_stats").is_null() & pl.col("group_backup").is_not_null()
        ).height
        no_group = accounts_df.filter(
            pl.col("group_from_stats").is_null() & pl.col("group_backup").is_null()
        ).height

        print(f"  Group from account_stats: {from_stats}")
        print(f"  Group from mt5_users (backup): {from_backup}")
        print(f"  No group information: {no_group}")

        # Filter out accounts without group information BEFORE dropping temporary columns
        # An account is considered to have group information if it has a group in EITHER mt5_users OR account_stats
        if no_group > 0:
            print(f"\nRemoving {no_group} accounts without group information...")
            accounts_df = accounts_df.filter(
                (pl.col("group").is_not_null() & (pl.col("group") != ""))
                | (
                    pl.col("group_from_stats").is_not_null()
                    & (pl.col("group_from_stats") != "")
                )
            )
            print(f"Remaining accounts after filtering: {len(accounts_df)}")

        # Create remote_group_name - always set to demo\PropModel\common for reference only
        accounts_df = accounts_df.with_columns(
            pl.lit("demo\\PropModel\\common").alias("remote_group_name")
        )

        # Apply funded status logic BEFORE dropping group columns
        print("\nApplying funded status logic...")
        
        # Step 1: Determine current_phase based on group pattern
        # 1-A or 1-B → current_phase = 1
        # 2-A or 2-B → current_phase = 2
        # 3-A or 3-B → current_phase = 3
        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("group").str.contains("1-A") | pl.col("group").str.contains("1-B")
            )
            .then(pl.lit(1))
            .when(pl.col("group").str.contains("2-A") | pl.col("group").str.contains("2-B"))
            .then(pl.lit(2))
            .when(pl.col("group").str.contains("3-A") | pl.col("group").str.contains("3-B"))
            .then(pl.lit(3))
            .otherwise(pl.lit(1))  # Default to phase 1
            .cast(pl.Int8)
            .alias("current_phase")
        )

        # Step 2: Set funded_status = 0 for Evolution Phase (funded_at is null)
        accounts_df = accounts_df.with_columns(
            pl.when(pl.col("funded_at").is_null())
            .then(pl.lit(0))  # Evolution Phase: funded_status = 0
            .otherwise(pl.lit(None))  # Will be set in Scenario 1 later
            .cast(pl.Int8)
            .alias("funded_status")
        )

        # Step 3: Set status = is_active for Evolution Phase (funded_at is null)
        if "is_active" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.when(pl.col("funded_at").is_null())
                .then(
                    pl.col("is_active").cast(pl.Int8)
                )  # Evolution Phase: status = is_active
                .otherwise(pl.lit(None))  # Will be set in Scenario 1 later
                .cast(pl.Int8)
                .alias("status")
            )
        else:
            accounts_df = accounts_df.with_columns(
                pl.lit(None).cast(pl.Int8).alias("status")
            )

        # Load funded groups for Scenario 1 logic
        complete_funded_groups = [
            "demo\\Nostro\\U-FTF-1-A",
            "demo\\Nostro\\U-FTF-1-B",
            "demo\\Nostro\\U-COF-1-A",
            "demo\\Nostro\\U-COF-1-B",
            "demo\\Nostro\\U-SSF-1-A",
            "demo\\Nostro\\U-SSF-1-B",
            "demo\\Nostro\\U-SAF-1-A",
            "demo\\Nostro\\U-SAF-1-B",
            "demo\\Nostro\\U-DSF-1-A",
            "demo\\Nostro\\U-DSF-1-B",
            "demo\\Nostro\\U-DAF-1-A",
            "demo\\Nostro\\U-DAF-1-B",
            "demo\\Nostro\\U-TSF-1-A",
            "demo\\Nostro\\U-TSF-1-B",
            "demo\\Nostro\\U-TAF-1-A",
            "demo\\Nostro\\U-TAF-1-B",
        ]

        try:
            platform_groups_df = pl.read_csv("csv/output/new_platform_groups.csv")
            dynamic_funded_groups = (
                platform_groups_df["funded_group_name"].unique().sort().to_list()
            )
            # Merge dynamic groups with complete list (remove duplicates)
            funded_groups = list(set(complete_funded_groups + dynamic_funded_groups))
            funded_groups.sort()
        except FileNotFoundError:
            funded_groups = complete_funded_groups

        # Scenario 1.1: Pending Funded Phase
        # Conditions: funded_at is not null AND group is NOT in funded groups
        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null() & ~pl.col("group").is_in(funded_groups)
            )
            .then(pl.lit(2))  # Pending Funded Phase: status = 2
            .otherwise(pl.col("status"))  # Keep existing value from Evolution Phase
            .alias("status")
        )

        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null() & ~pl.col("group").is_in(funded_groups)
            )
            .then(pl.lit(0))  # Pending Funded Phase: funded_status = 0
            .otherwise(pl.col("funded_status"))  # Keep existing value from Evolution Phase
            .alias("funded_status")
        )

        # Scenario 1.2: Approved Funded Phase
        # Conditions: funded_at is not null AND group IS in funded groups AND is_active = 1
        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null()
                & pl.col("group").is_in(funded_groups)
                & (pl.col("is_active") == 1)
            )
            .then(pl.lit(0))  # Approved Funded Phase: current_phase = 0
            .otherwise(pl.col("current_phase"))  # Keep existing value
            .alias("current_phase")
        )

        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null()
                & pl.col("group").is_in(funded_groups)
                & (pl.col("is_active") == 1)
            )
            .then(pl.lit(1))  # Approved Funded Phase: status = 1
            .otherwise(pl.col("status"))  # Keep existing value
            .alias("status")
        )

        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null()
                & pl.col("group").is_in(funded_groups)
                & (pl.col("is_active") == 1)
            )
            .then(pl.lit(1))  # Approved Funded Phase: funded_status = 1
            .otherwise(pl.col("funded_status"))  # Keep existing value
            .alias("funded_status")
        )

        # Scenario 1.3: Rejected Funded Phase
        # Conditions: funded_at is not null AND group is NOT in funded groups AND is_active = 0
        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null() 
                & ~pl.col("group").is_in(funded_groups)
                & (pl.col("is_active") == 0)
            )
            .then(pl.lit(0))  # Rejected Funded Phase: status = 0
            .otherwise(pl.col("status"))  # Keep existing value
            .alias("status")
        )

        accounts_df = accounts_df.with_columns(
            pl.when(
                pl.col("funded_at").is_not_null() 
                & ~pl.col("group").is_in(funded_groups)
                & (pl.col("is_active") == 0)
            )
            .then(pl.lit(2))  # Rejected Funded Phase: funded_status = 2
            .otherwise(pl.col("funded_status"))  # Keep existing value
            .alias("funded_status")
        )

        # Drop temporary columns AFTER funded status logic
        accounts_df = accounts_df.drop(["group_backup", "group_from_stats"])

    except FileNotFoundError:
        print(
            "\n⚠️  Warning: csv/input/account_stats.csv not found. Using mt5_users.csv group only."
        )
        # Filter out accounts without group information from mt5_users
        accounts_without_group = accounts_df.filter(
            pl.col("group").is_null() | (pl.col("group") == "")
        ).height
        if accounts_without_group > 0:
            print(
                f"Removing {accounts_without_group} accounts without group information..."
            )
            accounts_df = accounts_df.filter(
                pl.col("group").is_not_null() & (pl.col("group") != "")
            )
            print(f"Remaining accounts after filtering: {len(accounts_df)}")

    # Platform group UUID mapping will be done later after all columns are added

    # Load account_data.csv for password mapping
    print("\nLoading csv/input/account_data.csv for password mapping...")
    try:
        account_data_df = pl.read_csv(
            "csv/input/account_data.csv", infer_schema_length=100000
        )
        print(f"Loaded {len(account_data_df)} account data records")

        # Create mapping from account_login to passwords
        # Convert account_login to string to ensure proper matching with login column
        login_to_passwords = dict(
            zip(
                account_data_df["account_login"].cast(pl.Utf8),
                zip(
                    account_data_df["main_password"],
                    account_data_df["investor_password"],
                ),
            )
        )
        print(f"Created password mapping for {len(login_to_passwords)} accounts")

        # Map passwords based on login (convert login to string for matching)
        accounts_df = accounts_df.with_columns(
            pl.col("login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_passwords.get(x)[0]
                    if x is not None and x in login_to_passwords
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("main_password")
        )

        accounts_df = accounts_df.with_columns(
            pl.col("login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_passwords.get(x)[1]
                    if x is not None and x in login_to_passwords
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("investor_password")
        )

        valid_password_mappings = accounts_df.filter(
            pl.col("main_password").is_not_null()
        ).height
        invalid_password_mappings = accounts_df.filter(
            pl.col("main_password").is_null()
        ).height
        print(f"  Valid password mappings: {valid_password_mappings}")
        print(f"  Invalid/missing password mappings: {invalid_password_mappings}")

    except FileNotFoundError:
        print(
            "\n⚠️  Warning: csv/input/account_data.csv not found. Skipping password mapping."
        )
        accounts_df = accounts_df.with_columns(
            pl.lit(None).alias("main_password"), pl.lit(None).alias("investor_password")
        )

    # Generate UUID for each account
    print("\nGenerating UUIDs for platform accounts...")
    accounts_df = accounts_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(accounts_df))])
    )

    # Load the original users.csv and new_users.csv to create user_id-to-user_uuid mapping
    print("Loading user files for user mapping...")
    try:
        original_users_df = pl.read_csv("csv/input/users.csv")
        new_users_df = pl.read_csv(
            "csv/output/new_users.csv", infer_schema_length=100000
        )
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
            "csv/input/purchases.csv",
            infer_schema_length=100000,
            schema_overrides={"discount": pl.Float64},
        )
        new_purchases_df = pl.read_csv(
            "csv/output/new_purchases.csv", infer_schema_length=100000
        )
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

    # Add updated_at column (use funded_at if available, otherwise created_at)
    if "updated_at" not in accounts_df.columns:
        if "created_at" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.when(pl.col("funded_at").is_not_null())
                .then(pl.col("funded_at"))
                .otherwise(pl.col("created_at"))
                .alias("updated_at")
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

    # Add platform_name column (default to 'MT5' - uppercase to match platform groups)
    if "platform_name" not in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit("MT5").alias("platform_name"))

    # remote_group_name column is already created earlier in the script

    # Add profit_target column (copy from target)
    if "profit_target" not in accounts_df.columns:
        if "target" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(
                pl.col("target").alias("profit_target")
            )
        else:
            accounts_df = accounts_df.with_columns(pl.lit(None).alias("profit_target"))

    # Note: status, current_phase, and funded_status will be set based on funded_status_validation.md logic

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

    # Load platform groups to create combination-based UUID mapping
    # Map based on: platform_name + account_stage + account_type + initial_balance
    print("\nLoading platform groups for combination-based UUID mapping...")
    try:
        platform_groups_df = pl.read_csv("csv/output/new_platform_groups.csv")
        print(f"Loaded {len(platform_groups_df)} platform groups")

        # Create a mapping based on the combination of fields
        # We'll use a join operation to map based on the combination
        mapping_df = platform_groups_df.select(
            [
                "platform_name",
                "account_stage",
                "account_type",
                "initial_balance",
                "uuid",
            ]
        ).rename({"uuid": "platform_group_uuid"})

        # Convert initial_balance to match data types (f64 to i64)
        mapping_df = mapping_df.with_columns(pl.col("initial_balance").cast(pl.Int64))

        print(f"Created mapping for {len(mapping_df)} platform group combinations")
        print("Available combinations:")
        print(
            mapping_df.select(
                ["platform_name", "account_stage", "account_type", "initial_balance"]
            ).unique()
        )

        # Join accounts with platform groups based on the combination
        accounts_df = accounts_df.join(
            mapping_df,
            on=["platform_name", "account_stage", "account_type", "initial_balance"],
            how="inner",  # Use INNER JOIN to only keep accounts that match platform groups
        )

        valid_group_mappings = accounts_df.filter(
            pl.col("platform_group_uuid").is_not_null()
        ).height
        print(
            f"  Accounts with valid platform group UUID mappings: {valid_group_mappings}"
        )
        print(f"  Total accounts after filtering: {len(accounts_df)}")

        # Show which combinations are now included
        print("\nIncluded combinations:")
        included_combinations = (
            accounts_df.select(
                ["platform_name", "account_stage", "account_type", "initial_balance"]
            )
            .unique()
            .sort(["platform_name", "account_stage", "account_type", "initial_balance"])
        )
        print(f"Total included combinations: {len(included_combinations)}")
        print(included_combinations)

    except FileNotFoundError:
        print(
            "\n⚠️  Warning: csv/output/new_platform_groups.csv not found. Skipping platform group UUID mapping."
        )
        print("Please run: uv run main.py --generate --platform-groups")
        accounts_df = accounts_df.with_columns(
            pl.lit(None).alias("platform_group_uuid")
        )

    # Scenario 2: Evolution Phase - For accounts where funded_at is null
    # Funded status logic has been moved to occur before group columns are dropped

    # Step 1: Determine current_phase based on group pattern
    # 1-A or 1-B → current_phase = 1
    # 2-A or 2-B → current_phase = 2
    # 3-A or 3-B → current_phase = 3
    accounts_df = accounts_df.with_columns(
        pl.when(
            pl.col("group").str.contains("1-A") | pl.col("group").str.contains("1-B")
        )
        .then(pl.lit(1))
        .when(pl.col("group").str.contains("2-A") | pl.col("group").str.contains("2-B"))
        .then(pl.lit(2))
        .when(pl.col("group").str.contains("3-A") | pl.col("group").str.contains("3-B"))
        .then(pl.lit(3))
        .otherwise(pl.lit(1))  # Default to phase 1
        .cast(pl.Int8)
        .alias("current_phase")
    )

    # Step 2: Set funded_status = 0 for Evolution Phase (funded_at is null)
    accounts_df = accounts_df.with_columns(
        pl.when(pl.col("funded_at").is_null())
        .then(pl.lit(0))  # Evolution Phase: funded_status = 0
        .otherwise(pl.lit(None))  # Will be set in Scenario 1 later
        .cast(pl.Int8)
        .alias("funded_status")
    )

    # Step 3: Set status = is_active for Evolution Phase (funded_at is null)
    if "is_active" in accounts_df.columns:
        accounts_df = accounts_df.with_columns(
            pl.when(pl.col("funded_at").is_null())
            .then(
                pl.col("is_active").cast(pl.Int8)
            )  # Evolution Phase: status = is_active
            .otherwise(pl.lit(None))  # Will be set in Scenario 1 later
            .cast(pl.Int8)
            .alias("status")
        )
    else:
        accounts_df = accounts_df.with_columns(
            pl.lit(None).cast(pl.Int8).alias("status")
        )

    # Count and display Evolution Phase statistics
    evolution_phase_accounts = accounts_df.filter(pl.col("funded_at").is_null()).height
    print(f"  Total Evolution Phase accounts: {evolution_phase_accounts}")

    if evolution_phase_accounts > 0:
        # Breakdown by current_phase
        phase_1_count = accounts_df.filter(
            pl.col("funded_at").is_null() & (pl.col("current_phase") == 1)
        ).height
        phase_2_count = accounts_df.filter(
            pl.col("funded_at").is_null() & (pl.col("current_phase") == 2)
        ).height
        phase_3_count = accounts_df.filter(
            pl.col("funded_at").is_null() & (pl.col("current_phase") == 3)
        ).height

        print(f"    Phase 1 (1-A/1-B): {phase_1_count}")
        print(f"    Phase 2 (2-A/2-B): {phase_2_count}")
        print(f"    Phase 3 (3-A/3-B): {phase_3_count}")

        # Breakdown by status (active vs inactive)
        active_count = accounts_df.filter(
            pl.col("funded_at").is_null() & (pl.col("status") == 1)
        ).height
        inactive_count = accounts_df.filter(
            pl.col("funded_at").is_null() & (pl.col("status") == 0)
        ).height

        print(f"    Active (status=1): {active_count}")
        print(f"    Inactive (status=0): {inactive_count}")

    # Scenario 1: Funded Phase - For accounts where funded_at is not null
    print("\nApplying Scenario 1: Funded Phase (funded_at is not null)...")

    # Load funded groups dynamically from platform_groups.csv and merge with complete list
    print("Loading funded groups from platform_groups.csv...")

    # Complete hardcoded list (16 groups)
    complete_funded_groups = [
        "demo\\Nostro\\U-FTF-1-A",
        "demo\\Nostro\\U-FTF-1-B",
        "demo\\Nostro\\U-COF-1-A",
        "demo\\Nostro\\U-COF-1-B",
        "demo\\Nostro\\U-SSF-1-A",
        "demo\\Nostro\\U-SSF-1-B",
        "demo\\Nostro\\U-SAF-1-A",
        "demo\\Nostro\\U-SAF-1-B",
        "demo\\Nostro\\U-DSF-1-A",
        "demo\\Nostro\\U-DSF-1-B",
        "demo\\Nostro\\U-DAF-1-A",
        "demo\\Nostro\\U-DAF-1-B",
        "demo\\Nostro\\U-TSF-1-A",
        "demo\\Nostro\\U-TSF-1-B",
        "demo\\Nostro\\U-TAF-1-A",
        "demo\\Nostro\\U-TAF-1-B",
    ]

    try:
        platform_groups_df = pl.read_csv("csv/output/new_platform_groups.csv")
        dynamic_funded_groups = (
            platform_groups_df["funded_group_name"].unique().sort().to_list()
        )
        print(
            f"Loaded {len(dynamic_funded_groups)} funded groups from platform_groups.csv:"
        )
        for i, group in enumerate(dynamic_funded_groups, 1):
            print(f"  {i}. {group}")

        # Merge dynamic groups with complete list (remove duplicates)
        funded_groups = list(set(complete_funded_groups + dynamic_funded_groups))
        funded_groups.sort()

        print(f"\nMerged with complete list: {len(funded_groups)} total funded groups")
        print("Final merged funded groups:")
        for i, group in enumerate(funded_groups, 1):
            print(f"  {i}. {group}")

    except FileNotFoundError:
        print(
            "Warning: platform_groups.csv not found. Using complete funded groups list."
        )
        funded_groups = complete_funded_groups
        print(f"Using complete list with {len(funded_groups)} funded groups")

    # Scenario 1.1: Pending Funded Phase
    # Conditions: funded_at is not null AND group is NOT in funded groups
    # Set: current_phase = preserve original phase, status = 2, funded_status = 0
    print("\n  Applying Pending Funded Phase logic...")

    accounts_df = accounts_df.with_columns(
        # Update current_phase for Pending Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null() & ~pl.col("group").is_in(funded_groups)
        )
        .then(
            pl.col("current_phase")
        )  # Pending Funded Phase: preserve original phase from group pattern
        .otherwise(pl.col("current_phase"))  # Keep existing value from Scenario 2
        .alias("current_phase")
    )

    accounts_df = accounts_df.with_columns(
        # Update status for Pending Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null() & ~pl.col("group").is_in(funded_groups)
        )
        .then(pl.lit(2))  # Pending Funded Phase: status = 2
        .otherwise(pl.col("status"))  # Keep existing value from Scenario 2
        .alias("status")
    )

    accounts_df = accounts_df.with_columns(
        # Update funded_status for Pending Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null() & ~pl.col("group").is_in(funded_groups)
        )
        .then(pl.lit(0))  # Pending Funded Phase: funded_status = 0
        .otherwise(pl.col("funded_status"))  # Keep existing value from Scenario 2
        .alias("funded_status")
    )

    # Count Pending Funded Phase accounts
    pending_funded_accounts = accounts_df.filter(
        pl.col("funded_at").is_not_null() & ~pl.col("group").is_in(funded_groups)
    ).height
    print(f"    Total Pending Funded Phase accounts: {pending_funded_accounts}")

    if pending_funded_accounts > 0:
        print(f"      current_phase = preserve original phase (1/2/3)")
        print(f"      status = 2 (pending)")
        print(f"      funded_status = 0")

    # Scenario 1.2: Approved Funded Phase
    # Conditions: funded_at is not null AND group IS in funded groups AND is_active = 1
    # Set: current_phase = 0, status = 1, funded_status = 1
    print("\n  Applying Approved Funded Phase logic...")

    accounts_df = accounts_df.with_columns(
        # Update current_phase for Approved Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null()
            & pl.col("group").is_in(funded_groups)
            & (pl.col("is_active") == 1)
        )
        .then(pl.lit(0))  # Approved Funded Phase: current_phase = 0
        .otherwise(pl.col("current_phase"))  # Keep existing value
        .alias("current_phase")
    )

    accounts_df = accounts_df.with_columns(
        # Update status for Approved Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null()
            & pl.col("group").is_in(funded_groups)
            & (pl.col("is_active") == 1)
        )
        .then(pl.lit(1))  # Approved Funded Phase: status = 1
        .otherwise(pl.col("status"))  # Keep existing value
        .alias("status")
    )

    accounts_df = accounts_df.with_columns(
        # Update funded_status for Approved Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null()
            & pl.col("group").is_in(funded_groups)
            & (pl.col("is_active") == 1)
        )
        .then(pl.lit(1))  # Approved Funded Phase: funded_status = 1
        .otherwise(pl.col("funded_status"))  # Keep existing value
        .alias("funded_status")
    )

    # Count Approved Funded Phase accounts
    approved_funded_accounts = accounts_df.filter(
        pl.col("funded_at").is_not_null()
        & pl.col("group").is_in(funded_groups)
        & (pl.col("is_active") == 1)
    ).height
    print(f"    Total Approved Funded Phase accounts: {approved_funded_accounts}")

    if approved_funded_accounts > 0:
        print(f"      current_phase = 0")
        print(f"      status = 1 (active)")
        print(f"      funded_status = 1 (approved)")

    # Scenario 1.3: Rejected Funded Phase
    # Conditions: funded_at is not null AND group is NOT in funded groups AND is_active = 0
    # Set: current_phase = preserve original phase, status = 0, funded_status = 2
    print("\n  Applying Rejected Funded Phase logic...")

    accounts_df = accounts_df.with_columns(
        # Update current_phase for Rejected Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null()
            & ~pl.col("group").is_in(funded_groups)
            & (pl.col("is_active") == 0)
        )
        .then(
            pl.col("current_phase")
        )  # Rejected Funded Phase: preserve original phase from group pattern
        .otherwise(pl.col("current_phase"))  # Keep existing value
        .alias("current_phase")
    )

    accounts_df = accounts_df.with_columns(
        # Update status for Rejected Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null()
            & ~pl.col("group").is_in(funded_groups)
            & (pl.col("is_active") == 0)
        )
        .then(pl.lit(0))  # Rejected Funded Phase: status = 0
        .otherwise(pl.col("status"))  # Keep existing value
        .alias("status")
    )

    accounts_df = accounts_df.with_columns(
        # Update funded_status for Rejected Funded Phase
        pl.when(
            pl.col("funded_at").is_not_null()
            & ~pl.col("group").is_in(funded_groups)
            & (pl.col("is_active") == 0)
        )
        .then(pl.lit(2))  # Rejected Funded Phase: funded_status = 2
        .otherwise(pl.col("funded_status"))  # Keep existing value
        .alias("funded_status")
    )

    # Count Rejected Funded Phase accounts
    rejected_funded_accounts = accounts_df.filter(
        pl.col("funded_at").is_not_null()
        & ~pl.col("group").is_in(funded_groups)
        & (pl.col("is_active") == 0)
    ).height
    print(f"    Total Rejected Funded Phase accounts: {rejected_funded_accounts}")

    if rejected_funded_accounts > 0:
        print(f"      current_phase = preserve original phase (1/2/3)")
        print(f"      status = 0 (inactive)")
        print(f"      funded_status = 2 (rejected)")

    # Summary of all scenarios
    total_funded = (
        pending_funded_accounts + approved_funded_accounts + rejected_funded_accounts
    )
    print(f"\n  Summary of Funded Phase (funded_at is not null):")
    print(f"    Total: {total_funded}")
    print(f"    Pending: {pending_funded_accounts}")
    print(f"    Approved: {approved_funded_accounts}")
    print(f"    Rejected: {rejected_funded_accounts}")

    # Add platform_user_id column (null for now)
    if "platform_user_id" not in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit(None).alias("platform_user_id"))

    # Set action_type to 'challenge'
    if "action_type" in accounts_df.columns:
        accounts_df = accounts_df.with_columns(pl.lit("challenge").alias("action_type"))

    # Load column configuration from JSON file
    try:
        with open("config/platform_accounts_column_config.json", "r") as f:
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

        # Save the processed data to csv/output/new_platform_accounts.csv only if generate flag is True
        if generate:
            accounts_df_filtered.write_csv("csv/output/new_platform_accounts.csv")
            print(
                f"\nSuccessfully generated csv/output/new_platform_accounts.csv with {len(accounts_df_filtered)} rows and {len(accounts_df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(existing_columns)}")
        else:
            print(
                "\nTo generate csv/output/new_platform_accounts.csv, run with --generate flag:"
            )
            print("  uv run main.py --generate --platform-accounts")

    except FileNotFoundError:
        print("\n⚠️  config/platform_accounts_column_config.json not found!")
        print("Exporting all columns...")

        print(
            f"\nPlatform Accounts DataFrame shape: ({len(accounts_df)}, {len(accounts_df.columns)})"
        )
        print(f"All columns ({len(accounts_df.columns)}): {accounts_df.columns}")

        print(f"\nFirst few rows:")
        print(accounts_df.head())

        if generate:
            accounts_df.write_csv("csv/output/new_platform_accounts.csv")
            print(
                f"\nSuccessfully generated csv/output/new_platform_accounts.csv with {len(accounts_df)} rows and {len(accounts_df.columns)} columns"
            )
        else:
            print(
                "\nTo generate csv/output/new_platform_accounts.csv, run with --generate flag:"
            )
            print("  uv run main.py --generate --platform-accounts")

    except json.JSONDecodeError as e:
        print(f"\n❌ Error parsing config/platform_accounts_column_config.json: {e}")
        print("Please check the JSON syntax.")
        accounts_df.write_csv("csv/output/new_platform_accounts.csv")
        print(
            f"\nSuccessfully generated csv/output/new_platform_accounts.csv with {len(accounts_df)} rows and {len(accounts_df.columns)} columns"
        )
