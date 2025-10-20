import polars as pl
import uuid
import json
from datetime import datetime, timedelta


def export_periodic_trading_export_last_6_months(generate=False):
    """
    Export periodic trading data from periodic_trading_export.csv to new_periodic_trading_export_last_6_months.csv,
    including only records from the last 6 months (based on deal_time).

    Args:
        generate (bool): If True, generates new_periodic_trading_export_last_6_months.csv file. If False, only previews.
    """

    # Load the periodic trading export CSV file
    print("Loading csv/input/periodic_trading_export.csv...")
    trading_df = pl.read_csv(
        "csv/input/periodic_trading_export.csv",
        infer_schema_length=100000,
        ignore_errors=True,  # Handle parsing errors
    )
    print(f"Loaded {len(trading_df)} periodic trading records")

    # Generate UUID for each record (id column)
    print("\nGenerating UUIDs for periodic trading records...")
    trading_df = trading_df.with_columns(
        pl.Series("id", [str(uuid.uuid4()) for _ in range(len(trading_df))])
    )

    # Load platform accounts for mapping
    print("\nLoading platform accounts for UUID mapping...")
    try:
        platform_accounts_df = pl.read_csv(
            "csv/output/new_platform_accounts.csv", infer_schema_length=100000
        )
        print(f"Loaded {len(platform_accounts_df)} platform accounts")

        trading_account_to_platform_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )

        print(
            f"Created platform account mapping for {len(trading_account_to_platform_uuid)} accounts"
        )

        trading_df = trading_df.with_columns(
            pl.col("trading_account")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    trading_account_to_platform_uuid.get(x)
                    if x is not None and x in trading_account_to_platform_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("platform_account_uuid")
        )

        mapped_count = trading_df.filter(
            pl.col("platform_account_uuid").is_not_null()
        ).height
        unmapped_count = trading_df.filter(
            pl.col("platform_account_uuid").is_null()
        ).height

        print(f"  Mapped records: {mapped_count}")
        print(f"  Unmapped records: {unmapped_count}")
        print(f"  Mapping rate: {mapped_count/len(trading_df)*100:.1f}%")

        if unmapped_count > 0:
            print(
                "  Warning: Some trading_account values don't have matching platform accounts"
            )
            unmapped_examples = (
                trading_df.filter(pl.col("platform_account_uuid").is_null())
                .select("trading_account")
                .unique()
                .head(10)
            )
            print(
                f"  Examples of unmapped trading_account values: {unmapped_examples.to_series().to_list()}"
            )

    except FileNotFoundError:
        print(
            "\n⚠️  Warning: csv/output/new_platform_accounts.csv not found. Skipping platform account mapping."
        )
        trading_df = trading_df.with_columns(
            pl.lit(None).alias("platform_account_uuid")
        )

    # Convert and filter by last 6 months
    print("\nConverting deal_time and filtering last 6 months of data...")
    trading_df = trading_df.with_columns(
        pl.col("deal_time")
        .str.to_datetime(strict=False, format=None)
        .alias("deal_time")
    )

    cutoff_date = datetime.now() - timedelta(days=180)  # Last 6 months
    print(f"Keeping records with deal_time >= {cutoff_date.isoformat()}")
    trading_df = trading_df.filter(pl.col("deal_time") >= pl.lit(cutoff_date))
    print(f"Remaining records after 6-month filter: {len(trading_df)}")

    # Filter out null platform_account_uuid
    print("\nFiltering out records with null platform_account_uuid...")
    initial_count = len(trading_df)
    trading_df = trading_df.filter(pl.col("platform_account_uuid").is_not_null())
    filtered_count = len(trading_df)
    removed_count = initial_count - filtered_count
    print(f"Removed {removed_count} records with null platform_account_uuid")
    print(f"Remaining records: {filtered_count}")

    # Convert data types to match PostgreSQL schema
    print("\nConverting data types to match PostgreSQL schema...")
    trading_df = trading_df.with_columns(
        [
            pl.col("deal_volume").cast(pl.Float64),
            pl.col("dupe_detected").cast(pl.Boolean),
            pl.col("deal_swap").cast(pl.Float64),
        ]
    )

    # Load column configuration
    print("\nLoading column configuration...")
    try:
        with open("config/periodic_trading_export_column_config.json", "r") as f:
            column_config = json.load(f)
        print(f"Loaded column configuration with {len(column_config)} columns")
    except FileNotFoundError:
        print("⚠️  Warning: Column configuration not found. Using default column order.")
        column_config = [
            "id",
            "deal_id",
            "position_id",
            "deal_type",
            "profit",
            "deal_time",
            "deal_entry",
            "deal_price",
            "deal_symbol",
            "deal_stoploss",
            "deal_volume",
            "deal_commission",
            "dupe_detected",
            "deal_swap",
            "platform_account_uuid",
        ]

    # Select and reorder columns
    print("\nSelecting and reordering columns...")
    available_columns = trading_df.columns
    selected_columns = [col for col in column_config if col in available_columns]
    missing_columns = [col for col in column_config if col not in available_columns]
    if missing_columns:
        print(f"  Warning: Missing columns: {missing_columns}")

    trading_df = trading_df.select(selected_columns)
    print(f"Selected {len(selected_columns)} columns: {selected_columns}")

    # Show preview
    print(f"\nPeriodic Trading Export DataFrame shape: {trading_df.shape}")
    print(trading_df.head())

    # Show data types
    print(f"\nData types:")
    for col, dtype in zip(trading_df.columns, trading_df.dtypes):
        print(f"  {col}: {dtype}")

    if generate:
        output_file = "csv/output/new_periodic_trading_export_last_6_months.csv"
        print(f"\nGenerating {output_file}...")
        trading_df.write_csv(output_file)
        print(
            f"✅ Successfully generated {output_file} with {len(trading_df)} rows and {len(trading_df.columns)} columns"
        )
        print(f"Included columns: {', '.join(trading_df.columns)}")
    else:
        print(
            f"\nTo generate csv/output/new_periodic_trading_export_last_6_months.csv, run with --generate flag:"
        )
        print(f"  uv run main.py --generate --periodic-trading-export")


if __name__ == "__main__":
    export_periodic_trading_export_last_6_months(generate=True)
