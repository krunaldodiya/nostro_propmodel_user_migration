import polars as pl
import uuid


def main():
    user_role_id = "7d240ffe-f3f3-4015-9aa7-18a3acc854f7"
    admin_role_id = "4498cf39-7fe2-4059-9571-6e65632eb283"

    print("Loading users CSV file...")

    # Load the CSV file with semicolon separator
    df = pl.read_csv("users.csv")

    print(f"Loaded {len(df)} users")
    print(f"Original columns: {df.columns}")

    # Add user_uuid column with random UUIDs
    print("Adding user_uuid column...")
    df = df.with_columns(
        pl.Series("user_uuid", [str(uuid.uuid4()) for _ in range(len(df))])
    )

    # Add role_id column based on role value
    print("Adding role_id column...")
    df = df.with_columns(
        pl.when(pl.col("role") == 2)
        .then(pl.lit(user_role_id))
        .otherwise(pl.lit(admin_role_id))
        .alias("role_id")
    )

    # Create ref_by_user_uuid column by mapping ref_by_user_id to user_uuid
    print("Adding ref_by_user_uuid column...")

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

    print(f"Updated columns: {df.columns}")
    print("\nFirst 5 rows:")
    print(df.head())


if __name__ == "__main__":
    main()
