import polars as pl
import uuid
import argparse


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

    # Add user_uuid column with random UUIDs
    df = df.with_columns(
        pl.Series("user_uuid", [str(uuid.uuid4()) for _ in range(len(df))])
    )

    # Add role_id column based on role value
    df = df.with_columns(
        pl.when(pl.col("role") == 2)
        .then(pl.lit(user_role_id))
        .otherwise(pl.lit(admin_role_id))
        .alias("role_id")
    )

    # Create ref_by_user_uuid column by mapping ref_by_user_id to user_uuid
    # Create a mapping from id to user_uuid
    id_to_uuid = dict(zip(df["id"], df["user_uuid"]))

    # Map ref_by_user_id to ref_by_user_uuid
    df = df.with_columns(
        pl.col("ref_by_user_id")
        .map_elements(
            lambda x: id_to_uuid.get(x) if x is not None else None, return_dtype=pl.Utf8
        )
        .alias("ref_by_user_uuid")
    )

    # Rename username column to email
    df = df.rename({"username": "email"})

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
        df = df.with_columns(pl.lit("").alias("google_app_secret"))

    # Add is_google_app_verify if it doesn't exist (should already exist from CSV modifications)
    if "is_google_app_verify" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("is_google_app_verify"))

    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print("\nFirst few rows:")
    print(df.head())

    # Save the processed data to new_users.csv only if --generate flag is passed
    if args.generate:
        df.write_csv("new_users.csv")
        print(
            f"\nSuccessfully generated new_users.csv with {len(df)} rows and {len(df.columns)} columns"
        )


if __name__ == "__main__":
    main()
