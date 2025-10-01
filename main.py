import polars as pl
import uuid
import argparse
import json
import random
import string


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="User migration tool")
    parser.add_argument(
        "--generate", action="store_true", help="Generate new_users.csv file"
    )
    args = parser.parse_args()

    user_role_id = "7d240ffe-f3f3-4015-9aa7-18a3acc854f7"
    admin_role_id = "4498cf39-7fe2-4059-9571-6e65632eb283"

    # Load the CSV file
    df = pl.read_csv("users.csv")

    # Add uuid column with random UUIDs
    df = df.with_columns(pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(df))]))

    # Add role_id column based on role value
    df = df.with_columns(
        pl.when(pl.col("role") == 2)
        .then(pl.lit(user_role_id))
        .otherwise(pl.lit(admin_role_id))
        .alias("role_id")
    )

    # Create ref_by_user_uuid column by mapping ref_by_user_id to uuid
    # Create a mapping from id to uuid
    id_to_uuid = dict(zip(df["id"], df["uuid"]))

    # Map ref_by_user_id to ref_by_user_uuid - only include valid UUIDs
    df = df.with_columns(
        pl.col("ref_by_user_id")
        .map_elements(
            lambda x: id_to_uuid.get(x) if x is not None and x in id_to_uuid else None,
            return_dtype=pl.Utf8,
        )
        .alias("ref_by_user_uuid")
    )

    # Fix ref_by_user_id column to use NULL instead of empty strings
    df = df.with_columns(
        pl.col("ref_by_user_id")
        .map_elements(
            lambda x: id_to_uuid.get(x) if x is not None and x in id_to_uuid else None,
            return_dtype=pl.Utf8,
        )
        .alias("ref_by_user_id")
    )

    # Rename columns
    df = df.rename(
        {"username": "email", "firstname": "first_name", "lastname": "last_name"}
    )

    # Blank out password column for security
    if "password" in df.columns:
        df = df.with_columns(pl.lit(None).alias("password"))

    # Ensure all new columns exist with proper default values
    # Add ref_link_count if it doesn't exist (should already exist from CSV modifications)
    if "ref_link_count" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("ref_link_count"))

    # Add used_free_count if it doesn't exist (should already exist from CSV modifications)
    if "used_free_count" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("used_free_count"))

    # Add available_count if it doesn't exist (should already exist from CSV modifications)
    if "available_count" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("available_count"))

    # Add google_app_secret if it doesn't exist (should already exist from CSV modifications)
    if "google_app_secret" not in df.columns:
        df = df.with_columns(pl.lit(None).alias("google_app_secret"))

    # Add is_google_app_verify if it doesn't exist (should already exist from CSV modifications)
    if "is_google_app_verify" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("is_google_app_verify"))

    # Add trail_verification_status if it doesn't exist
    if "trail_verification_status" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("trail_verification_status"))

    # Add ref_code column with 8-character random alphanumeric strings
    if "ref_code" not in df.columns:

        def generate_ref_code():
            return "".join(random.choices(string.ascii_letters + string.digits, k=8))

        ref_codes = [generate_ref_code() for _ in range(len(df))]
        df = df.with_columns(pl.Series("ref_code", ref_codes))

    # Add status column that maps to active column (1=active, 0=inactive)
    df = df.with_columns(
        pl.when(pl.col("active") == 1)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("status")
    )

    # Add last_login_at column that maps to last_login
    df = df.with_columns(pl.col("last_login").alias("last_login_at"))

    # Add updated_at column same as created_at
    df = df.with_columns(pl.col("created_at").alias("updated_at"))

    # Add deleted_at column set to null as default
    df = df.with_columns(pl.lit(None).alias("deleted_at"))

    # Add affiliate_terms column that maps from accept_affiliate_terms
    if "accept_affiliate_terms" in df.columns:
        df = df.with_columns(pl.col("accept_affiliate_terms").alias("affiliate_terms"))
    else:
        df = df.with_columns(pl.lit(0).alias("affiliate_terms"))

    # Add dob column set to null as default
    df = df.with_columns(pl.lit(None).alias("dob"))

    # Ensure all string columns use None instead of empty strings
    string_columns = [
        "reset_pass_hash",
        "address",
        "country",
        "state",
        "city",
        "zip",
        "timezone",
        "identity_status",
        "identity_verified_at",
    ]

    for col in string_columns:
        if col in df.columns:
            # Replace empty strings with None for these columns
            df = df.with_columns(
                pl.when(pl.col(col) == "")
                .then(pl.lit(None))
                .otherwise(pl.col(col))
                .alias(col)
            )

    # Load column configuration from JSON file
    try:
        with open("column_config.json", "r") as f:
            config = json.load(f)

        # Get columns to include (now it's a simple list)
        columns_to_include = config

        # Filter DataFrame to only include selected columns
        available_columns = [col for col in columns_to_include if col in df.columns]
        missing_columns = [col for col in columns_to_include if col not in df.columns]

        if missing_columns:
            print(
                f"Warning: The following columns are not available: {missing_columns}"
            )

        df_filtered = df.select(available_columns)

        print(f"DataFrame shape: {df_filtered.shape}")
        print(f"Selected columns ({len(available_columns)}): {available_columns}")
        print("\nFirst few rows:")
        print(df_filtered.head())

        # Save the processed data to new_users.csv only if --generate flag is passed
        if args.generate:
            df_filtered.write_csv("new_users.csv")

            print(
                f"\nSuccessfully generated new_users.csv with {len(df_filtered)} rows and {len(df_filtered.columns)} columns"
            )
            print(f"Included columns: {', '.join(available_columns)}")

    except FileNotFoundError:
        print("Error: column_config.json not found. Using all columns.")
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {df.columns}")
        print("\nFirst few rows:")
        print(df.head())

        if args.generate:
            df.write_csv("new_users.csv")
            print(
                f"\nSuccessfully generated new_users.csv with {len(df)} rows and {len(df.columns)} columns"
            )

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in column_config.json: {e}")
        print("Using all columns instead.")

        if args.generate:
            df.write_csv("new_users.csv")
            print(
                f"\nSuccessfully generated new_users.csv with {len(df)} rows and {len(df.columns)} columns"
            )


if __name__ == "__main__":
    main()
