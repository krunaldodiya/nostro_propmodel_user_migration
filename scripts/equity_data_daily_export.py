#!/usr/bin/env python3
"""
Equity Data Daily Export Script

This script exports equity_data_daily data from csv/input/equity_data_daily.csv
and generates a new CSV file with the following transformations:
- Generates UUID for id column
- Maps trading_account to platform_account_uuid using new_platform_accounts.csv
- Converts data types to match PostgreSQL schema
- Filters out records with null platform_account_uuid
- Reorders columns according to configuration

Output: csv/output/new_equity_data_daily.csv
"""

import polars as pl
import uuid
import json
import os


def export_equity_data_daily(generate=False):
    """Export equity data daily with platform account mapping."""
    print("=" * 70)
    print("                  EXPORTING EQUITY_DATA_DAILY                    ")
    print("=" * 70)

    # Load equity data
    print("\nLoading csv/input/equity_data_daily.csv...")
    equity_df = pl.read_csv("csv/input/equity_data_daily.csv")
    print(f"Loaded {len(equity_df)} equity data records")

    # Generate UUIDs
    print("\nGenerating UUIDs for equity data records...")
    equity_df = equity_df.with_columns(
        pl.lit("")
        .map_elements(lambda x: str(uuid.uuid4()), return_dtype=pl.Utf8)
        .alias("id")
    )

    # Load platform accounts for UUID mapping
    print("\nLoading platform accounts for UUID mapping...")
    try:
        platform_accounts_df = pl.read_csv("csv/output/new_platform_accounts.csv")
        print(f"Loaded {len(platform_accounts_df)} platform accounts")

        # Create mapping from platform_login_id to platform_account_uuid
        trading_account_to_platform_uuid = {}
        for row in platform_accounts_df.iter_rows(named=True):
            trading_account_to_platform_uuid[str(row["platform_login_id"])] = row[
                "uuid"
            ]

        print(
            f"Created platform account mapping for {len(trading_account_to_platform_uuid)} accounts"
        )

        # Map trading_account to platform_account_uuid
        print("\nMapping trading_account to platform_account_uuid...")
        equity_df = equity_df.with_columns(
            pl.col("trading_account")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: trading_account_to_platform_uuid.get(x), return_dtype=pl.Utf8
            )
            .alias("platform_account_uuid")
        )

        # Check mapping results
        mapped_count = equity_df.filter(
            pl.col("platform_account_uuid").is_not_null()
        ).height
        unmapped_count = equity_df.filter(
            pl.col("platform_account_uuid").is_null()
        ).height

        print(f"  Mapped records: {mapped_count}")
        print(f"  Unmapped records: {unmapped_count}")
        print(f"  Mapping rate: {mapped_count/(mapped_count+unmapped_count)*100:.1f}%")

        if unmapped_count > 0:
            print(
                "  Warning: Some trading_account values don't have matching platform accounts"
            )
            # Show some examples of unmapped records
            unmapped_examples = (
                equity_df.filter(pl.col("platform_account_uuid").is_null())
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
        # Add null platform_account_uuid column
        equity_df = equity_df.with_columns(pl.lit(None).alias("platform_account_uuid"))

    # Filter out records with null platform_account_uuid
    print("\nFiltering out records with null platform_account_uuid...")
    initial_count = len(equity_df)
    equity_df = equity_df.filter(pl.col("platform_account_uuid").is_not_null())
    filtered_count = len(equity_df)
    removed_count = initial_count - filtered_count
    print(f"Removed {removed_count} records with null platform_account_uuid")
    print(f"Remaining records: {filtered_count}")

    # Convert data types to match PostgreSQL schema
    print("\nConverting data types to match PostgreSQL schema...")
    equity_df = equity_df.with_columns(
        [
            # Convert day to date format
            pl.col("day").str.to_date().alias("day"),
            # Convert created_date to proper datetime format
            pl.col("created_date").str.to_datetime().alias("created_date"),
            # Convert equity_eod_mt5 to Float64 (handle nulls)
            pl.col("equity_eod_mt5").cast(pl.Float64),
        ]
    )

    # Load column configuration
    print("\nLoading column configuration...")
    try:
        with open("config/equity_data_daily_column_config.json", "r") as f:
            column_config = json.load(f)
        print(f"Loaded column configuration with {len(column_config)} columns")
    except FileNotFoundError:
        print("⚠️  Warning: Column configuration not found. Using default order.")
        column_config = [
            "id",
            "day",
            "created_date",
            "equity",
            "balance",
            "equity_eod_mt5",
            "platform_account_uuid",
        ]

    # Select and reorder columns
    print("\nSelecting and reordering columns...")
    equity_df = equity_df.select(column_config)
    print(f"Selected {len(equity_df.columns)} columns: {equity_df.columns}")

    # Display sample data
    print(f"\nEquity Data Daily DataFrame shape: {equity_df.shape}")
    print("First few rows:")
    print(equity_df.head())

    print("\nData types:")
    for col in equity_df.columns:
        dtype = equity_df[col].dtype
        print(f"  {col}: {dtype}")

    # Generate output file
    output_file = "csv/output/new_equity_data_daily.csv"
    if generate:
        print(f"\nGenerating {output_file}...")
        equity_df.write_csv(output_file)
        print(
            f"Successfully generated {output_file} with {len(equity_df)} rows and {len(equity_df.columns)} columns"
        )
        print(f"Included columns: {', '.join(equity_df.columns)}")
    else:
        print(
            f"\nTo generate csv/output/new_equity_data_daily.csv, run with --generate flag:"
        )
        print(f"  uv run main.py --generate --equity-data-daily")


if __name__ == "__main__":
    export_equity_data_daily(generate=True)
