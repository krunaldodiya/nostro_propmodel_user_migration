import polars as pl
from pathlib import Path

DEFAULT_SETTINGS_INPUT_PATH = Path("csv/input/default_challenge_settings.csv")
USERS_OUTPUT_PATH = Path("csv/output/new_users.csv")
DEFAULT_SETTINGS_OUTPUT_PATH = Path("csv/output/new_default_challenge_settings.csv")
TARGET_USER_EMAIL = "scott@nostro.co"


def export_default_challenge_settings(generate: bool = False) -> None:
    """
    Export default challenge settings ensuring created_by and updated_by
    reference Scott's user UUID.

    Args:
        generate (bool): When True, writes csv/output/new_default_challenge_settings.csv.
                         When False, only previews the processed dataframe.
    """
    print("Loading default challenge settings...")
    if not DEFAULT_SETTINGS_INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Source file not found: {DEFAULT_SETTINGS_INPUT_PATH}. "
            "Ensure the default challenge settings CSV is available in csv/input/."
        )

    settings_df = pl.read_csv(DEFAULT_SETTINGS_INPUT_PATH)
    print(f"Loaded {len(settings_df)} default challenge settings row(s)")

    print("Resolving Scott's UUID from generated users export...")
    if not USERS_OUTPUT_PATH.exists():
        raise FileNotFoundError(
            "csv/output/new_users.csv not found. Generate users export first:\n"
            "  uv run main.py --generate --users"
        )

    users_df = pl.read_csv(USERS_OUTPUT_PATH)
    scott_df = users_df.filter(pl.col("email") == TARGET_USER_EMAIL)

    if scott_df.is_empty():
        raise ValueError(
            f"User with email {TARGET_USER_EMAIL} not found in {USERS_OUTPUT_PATH}. "
            "Verify the users export contains this user."
        )

    if scott_df.height > 1:
        print(
            f"Warning: Multiple users found with email {TARGET_USER_EMAIL}; "
            "using the first occurrence."
        )

    scott_uuid = scott_df.select("uuid").item()
    print(f"Resolved Scott's UUID: {scott_uuid}")

    # Inject created_by and updated_by with Scott's UUID (overwrite existing values if present)
    columns_to_add = [pl.lit(scott_uuid).alias("created_by")]
    if "updated_by" in settings_df.columns:
        columns_to_add.append(pl.lit(scott_uuid).alias("updated_by"))
    settings_df = settings_df.with_columns(columns_to_add)

    # Reorder columns to keep created_by before updated_by when possible
    column_order = list(settings_df.columns)
    if "created_by" in column_order and "updated_by" in column_order:
        column_order.remove("created_by")
        updated_index = column_order.index("updated_by")
        column_order.insert(updated_index, "created_by")
        settings_df = settings_df.select(column_order)

    print(f"\nDefault Challenge Settings DataFrame shape: {settings_df.shape}")
    print(f"Columns ({len(settings_df.columns)}): {settings_df.columns}")
    print("\nFirst few rows:")
    print(settings_df.head())

    if generate:
        settings_df.write_csv(DEFAULT_SETTINGS_OUTPUT_PATH)
        print(
            f"\nSuccessfully generated {DEFAULT_SETTINGS_OUTPUT_PATH} with "
            f"{len(settings_df)} row(s) and {len(settings_df.columns)} column(s)"
        )
    else:
        print(
            "\nTo generate csv/output/new_default_challenge_settings.csv, run with --generate flag:"
        )
        print("  uv run main.py --generate --default-challenge-settings")


if __name__ == "__main__":
    export_default_challenge_settings(generate=True)
