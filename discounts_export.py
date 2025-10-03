import polars as pl
import uuid
import json


def export_discounts(generate=False):
    """
    Export discounts from discount_codes.csv to new_discounts.csv

    Args:
        generate (bool): If True, generates new_discounts.csv file. If False, only previews.
    """
    print("Loading discount_codes.csv...")
    # Load the CSV file
    discounts_df = pl.read_csv("discount_codes.csv")
    print(f"Loaded {len(discounts_df)} discount codes")

    # Add uuid column with random UUIDs (insert right after id)
    discounts_df = discounts_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(discounts_df))])
    )

    # Add updated_at column (copy from last_modified_date or created_date)
    if "last_modified_date" in discounts_df.columns:
        discounts_df = discounts_df.with_columns(
            pl.when(pl.col("last_modified_date").is_not_null())
            .then(pl.col("last_modified_date"))
            .otherwise(
                pl.col("created_date")
                if "created_date" in discounts_df.columns
                else None
            )
            .alias("updated_at")
        )
    elif "created_date" in discounts_df.columns:
        discounts_df = discounts_df.with_columns(
            pl.col("created_date").alias("updated_at")
        )

    # Add created_at column (rename from created_date if exists)
    if "created_date" in discounts_df.columns:
        discounts_df = discounts_df.with_columns(
            pl.col("created_date").alias("created_at")
        )

    # Add deleted_at column set to null as default
    if "deleted_at" not in discounts_df.columns:
        discounts_df = discounts_df.with_columns(pl.lit(None).alias("deleted_at"))

    # Load column configuration from JSON file
    try:
        with open("discounts_column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include
        columns_to_include = config

        # Filter DataFrame to only include selected columns
        available_columns = [
            col for col in columns_to_include if col in discounts_df.columns
        ]
        missing_columns = [
            col for col in columns_to_include if col not in discounts_df.columns
        ]

        if missing_columns:
            print(
                f"Warning: The following columns are not available: {missing_columns}"
            )

        discounts_df_filtered = discounts_df.select(available_columns)

        print(f"\nDiscounts DataFrame shape: {discounts_df_filtered.shape}")
        print(f"Selected columns ({len(available_columns)}): {available_columns}")
        print("\nFirst few rows:")
        print(discounts_df_filtered.head())

        # Save the processed data to new_discounts.csv only if generate flag is True
        if generate:
            discounts_df_filtered.write_csv("new_discounts.csv")
            print(
                f"\nSuccessfully generated new_discounts.csv with {len(discounts_df_filtered)} rows and {len(discounts_df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(available_columns)}")
        else:
            print("\nTo generate new_discounts.csv, run with --generate flag:")
            print("  uv run main.py --generate --discounts")

    except FileNotFoundError:
        print("Error: discounts_column_config.json not found. Using all columns.")
        print(f"\nDiscounts DataFrame shape: {discounts_df.shape}")
        print(f"Columns: {discounts_df.columns}")
        print("\nFirst few rows:")
        print(discounts_df.head())

        if generate:
            discounts_df.write_csv("new_discounts.csv")
            print(
                f"\nSuccessfully generated new_discounts.csv with {len(discounts_df)} rows and {len(discounts_df.columns)} columns"
            )
        else:
            print("\nTo generate new_discounts.csv, run with --generate flag:")
            print("  uv run main.py --generate --discounts")

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in discounts_column_config.json: {e}")
        print("Using all columns instead.")

        if generate:
            discounts_df.write_csv("new_discounts.csv")
            print(
                f"\nSuccessfully generated new_discounts.csv with {len(discounts_df)} rows and {len(discounts_df.columns)} columns"
            )
