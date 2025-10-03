import polars as pl
import uuid
import json


def export_purchases(generate=False):
    """
    Export purchases from purchases.csv to new_purchases.csv

    Args:
        generate (bool): If True, generates new_purchases.csv file. If False, only previews.
    """

    # Load the purchases CSV file
    print("Loading purchases.csv...")
    # Use infer_schema_length to properly detect column types
    purchases_df = pl.read_csv("purchases.csv", infer_schema_length=100000)
    print(f"Loaded {len(purchases_df)} purchases")

    # Load the new_users.csv file to get the id-to-uuid mapping
    print("Loading new_users.csv for user mapping...")
    users_df = pl.read_csv("new_users.csv")
    print(f"Loaded {len(users_df)} users")

    # Create a mapping from old user id to new uuid
    # We only need the id and uuid columns
    id_to_uuid = dict(zip(users_df["id"], users_df["uuid"]))
    print(f"Created mapping for {len(id_to_uuid)} users")

    # Add uuid column for purchases
    purchases_df = purchases_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(purchases_df))])
    )

    # Map user_id to user_uuid using the id_to_uuid mapping
    purchases_df = purchases_df.with_columns(
        pl.col("user_id")
        .map_elements(
            lambda x: id_to_uuid.get(x) if x is not None and x in id_to_uuid else None,
            return_dtype=pl.Utf8,
        )
        .alias("user_uuid")
    )

    # Count how many purchases have valid user mappings
    valid_user_mappings = purchases_df.filter(pl.col("user_uuid").is_not_null()).height
    invalid_user_mappings = purchases_df.filter(pl.col("user_uuid").is_null()).height
    print(f"\nUser mapping results:")
    print(f"  Valid user mappings: {valid_user_mappings}")
    print(f"  Invalid/missing user mappings: {invalid_user_mappings}")

    # Load the new_discounts.csv file to get the discount id-to-uuid mapping
    try:
        print("\nLoading new_discounts.csv for discount mapping...")
        discounts_df = pl.read_csv("new_discounts.csv")
        print(f"Loaded {len(discounts_df)} discount codes")

        # Create a mapping from old discount id to new uuid
        discount_id_to_uuid = dict(zip(discounts_df["id"], discounts_df["uuid"]))
        print(f"Created mapping for {len(discount_id_to_uuid)} discount codes")

        # Map discount_id to discount_uuid using the discount_id_to_uuid mapping
        purchases_df = purchases_df.with_columns(
            pl.col("discount_id")
            .map_elements(
                lambda x: (
                    discount_id_to_uuid.get(x)
                    if x is not None and x in discount_id_to_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("discount_uuid")
        )

        # Count how many purchases have valid discount mappings
        valid_discount_mappings = purchases_df.filter(
            pl.col("discount_uuid").is_not_null()
        ).height
        invalid_discount_mappings = purchases_df.filter(
            pl.col("discount_uuid").is_null()
        ).height
        print(f"\nDiscount mapping results:")
        print(f"  Valid discount mappings: {valid_discount_mappings}")
        print(f"  Invalid/missing discount mappings: {invalid_discount_mappings}")

    except FileNotFoundError:
        print("\nWarning: new_discounts.csv not found. Skipping discount UUID mapping.")
        print("Please run: uv run main.py --generate --discounts")
        # Add discount_uuid column with null values
        purchases_df = purchases_df.with_columns(pl.lit(None).alias("discount_uuid"))

    # Add created_at as updated_at if updated_at doesn't exist
    if "updated_at" not in purchases_df.columns:
        purchases_df = purchases_df.with_columns(
            pl.col("created_at").alias("updated_at")
        )

    # Add deleted_at column set to null as default
    if "deleted_at" not in purchases_df.columns:
        purchases_df = purchases_df.with_columns(pl.lit(None).alias("deleted_at"))

    # Load column configuration from JSON file
    try:
        with open("purchases_column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include
        columns_to_include = config

        # Filter DataFrame to only include selected columns
        available_columns = [
            col for col in columns_to_include if col in purchases_df.columns
        ]
        missing_columns = [
            col for col in columns_to_include if col not in purchases_df.columns
        ]

        if missing_columns:
            print(
                f"Warning: The following columns are not available: {missing_columns}"
            )

        purchases_df_filtered = purchases_df.select(available_columns)

        print(f"\nPurchases DataFrame shape: {purchases_df_filtered.shape}")
        print(f"Selected columns ({len(available_columns)}): {available_columns}")
        print("\nFirst few rows:")
        print(purchases_df_filtered.head())

        # Save the processed data to new_purchases.csv only if generate flag is True
        if generate:
            purchases_df_filtered.write_csv("new_purchases.csv")
            print(
                f"\nSuccessfully generated new_purchases.csv with {len(purchases_df_filtered)} rows and {len(purchases_df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(available_columns)}")
        else:
            print("\nTo generate new_purchases.csv, run with --generate flag:")
            print("  uv run main.py --generate --purchases")

    except FileNotFoundError:
        print("Error: purchases_column_config.json not found. Using all columns.")
        print(f"\nPurchases DataFrame shape: {purchases_df.shape}")
        print(f"Columns: {purchases_df.columns}")
        print("\nFirst few rows:")
        print(purchases_df.head())

        if generate:
            purchases_df.write_csv("new_purchases.csv")
            print(
                f"\nSuccessfully generated new_purchases.csv with {len(purchases_df)} rows and {len(purchases_df.columns)} columns"
            )
        else:
            print("\nTo generate new_purchases.csv, run with --generate flag:")
            print("  uv run main.py --generate --purchases")

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in purchases_column_config.json: {e}")
        print("Using all columns instead.")

        if generate:
            purchases_df.write_csv("new_purchases.csv")
            print(
                f"\nSuccessfully generated new_purchases.csv with {len(purchases_df)} rows and {len(purchases_df.columns)} columns"
            )
