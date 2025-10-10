#!/usr/bin/env python3
"""
Equity Data Daily Export Script
Maps trading_account to platform_login_id and adds platform_account_uuid column
"""

import polars as pl
import argparse
from pathlib import Path
import sys


def load_platform_accounts_mapping(platform_accounts_file: str) -> pl.DataFrame:
    """
    Load platform accounts and create a mapping from platform_login_id to uuid

    Args:
        platform_accounts_file: Path to new_platform_accounts.csv

    Returns:
        DataFrame with platform_login_id and uuid columns
    """
    print(f"Loading platform accounts from: {platform_accounts_file}")

    try:
        # Load platform accounts with flexible parsing
        platform_accounts = pl.read_csv(
            platform_accounts_file, infer_schema_length=10000, ignore_errors=True
        )

        # Create mapping: platform_login_id -> uuid
        # Convert platform_login_id to string for consistent joining
        mapping = platform_accounts.select(
            [
                pl.col("platform_login_id").cast(
                    pl.Utf8
                ),  # Convert to string for joining
                pl.col("uuid").alias("platform_account_uuid"),
            ]
        )

        print(f"Loaded {len(mapping)} platform account mappings")
        return mapping

    except Exception as e:
        print(f"Error loading platform accounts: {e}")
        sys.exit(1)


def process_equity_data_daily(
    equity_file: str, platform_mapping: pl.DataFrame, output_file: str
) -> bool:
    """
    Process equity data daily and add platform_account_uuid mapping

    Args:
        equity_file: Path to filtered equity data daily CSV
        platform_mapping: DataFrame with platform_login_id to uuid mapping
        output_file: Path for output CSV file

    Returns:
        True if successful, False otherwise
    """
    print(f"Processing equity data from: {equity_file}")

    try:
        # Read equity data
        equity_data = pl.read_csv(equity_file)
        print(f"Loaded {len(equity_data)} equity records")

        # Join with platform mapping
        print("Joining with platform account mapping...")
        # Convert trading_account to string for joining
        enriched_data = (
            equity_data.with_columns(
                pl.col("trading_account").cast(pl.Utf8).alias("trading_account_str")
            )
            .join(
                platform_mapping,
                left_on="trading_account_str",
                right_on="platform_login_id",
                how="left",
            )
            .drop("trading_account_str")
        )  # Remove temporary column

        # Check how many records were matched
        matched_count = enriched_data.filter(
            pl.col("platform_account_uuid").is_not_null()
        ).height
        unmatched_count = enriched_data.filter(
            pl.col("platform_account_uuid").is_null()
        ).height

        print(f"Matched records: {matched_count}")
        print(f"Unmatched records: {unmatched_count}")

        if unmatched_count > 0:
            print(
                "Warning: Some trading_account values don't have matching platform_login_id"
            )
            # Show some examples of unmatched records
            unmatched_examples = (
                enriched_data.filter(pl.col("platform_account_uuid").is_null())
                .select("trading_account")
                .unique()
                .head(10)
            )
            print(
                f"Examples of unmatched trading_account values: {unmatched_examples.to_series().to_list()}"
            )

        # Reorder columns to put platform_account_uuid after trading_account
        column_order = [
            "trading_account",
            "platform_account_uuid",
            "day",
            "created_date",
            "equity",
            "balance",
            "equity_eod_mt5",
        ]

        final_data = enriched_data.select(column_order)

        # Write output
        print(f"Writing enriched data to: {output_file}")
        final_data.write_csv(output_file)

        print(f"Successfully created {output_file}")
        print(f"Final record count: {len(final_data)}")

        return True

    except Exception as e:
        print(f"Error processing equity data: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Export equity data daily with platform account UUID mapping"
    )
    parser.add_argument(
        "--equity-file",
        required=True,
        help="Path to filtered equity data daily CSV file",
    )
    parser.add_argument(
        "--platform-accounts",
        required=True,
        help="Path to new_platform_accounts.csv file",
    )
    parser.add_argument("--output", required=True, help="Path for output CSV file")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview mode - show sample data without writing file",
    )

    args = parser.parse_args()

    # Validate input files
    if not Path(args.equity_file).exists():
        print(f"Error: Equity file not found: {args.equity_file}")
        sys.exit(1)

    if not Path(args.platform_accounts).exists():
        print(f"Error: Platform accounts file not found: {args.platform_accounts}")
        sys.exit(1)

    # Load platform accounts mapping
    platform_mapping = load_platform_accounts_mapping(args.platform_accounts)

    if args.preview:
        print("\n" + "=" * 60)
        print("PREVIEW MODE - Sample of enriched data:")
        print("=" * 60)

        # Load and process a small sample for preview
        equity_sample = pl.read_csv(args.equity_file).head(10)
        enriched_sample = (
            equity_sample.with_columns(
                pl.col("trading_account").cast(pl.Utf8).alias("trading_account_str")
            )
            .join(
                platform_mapping,
                left_on="trading_account_str",
                right_on="platform_login_id",
                how="left",
            )
            .drop("trading_account_str")
        )

        # Reorder columns
        column_order = [
            "trading_account",
            "platform_account_uuid",
            "day",
            "created_date",
            "equity",
            "balance",
            "equity_eod_mt5",
        ]

        preview_data = enriched_sample.select(column_order)
        print(preview_data)

        # Show statistics
        matched = preview_data.filter(
            pl.col("platform_account_uuid").is_not_null()
        ).height
        total = len(preview_data)
        print(f"\nSample statistics:")
        print(f"Total records: {total}")
        print(f"Matched records: {matched}")
        print(f"Match rate: {matched/total*100:.1f}%")

    else:
        # Process full dataset
        success = process_equity_data_daily(
            args.equity_file, platform_mapping, args.output
        )

        if success:
            print("✅ Export completed successfully!")
        else:
            print("❌ Export failed!")
            sys.exit(1)


if __name__ == "__main__":
    main()
