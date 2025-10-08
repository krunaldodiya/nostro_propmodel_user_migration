#!/usr/bin/env python3
"""
Account Stats Export Script

This script exports account statistics from the MySQL CSV format to PostgreSQL format.
It maps account_login to platform_account_uuid and group to platform_group_uuid,
and adds new fields required by the PostgreSQL schema.

Usage:
    python scripts/account_stats_export.py [--generate]
"""

import polars as pl
import json
import uuid
from datetime import datetime


def export_account_stats(generate: bool = False):
    """
    Export account statistics from MySQL CSV to PostgreSQL format.

    Args:
        generate (bool): If True, generates new_account_stats.csv file. If False, only previews.
    """
    print("=" * 70)
    print("                     EXPORTING ACCOUNT_STATS                     ")
    print("=" * 70)

    # Load account stats CSV
    print("Loading csv/input/account_stats.csv...")
    account_stats_df = pl.read_csv(
        "csv/input/account_stats.csv", infer_schema_length=100000
    )
    print(f"Loaded {len(account_stats_df)} account stats records")

    # Load platform accounts to get both platform_account_uuid and platform_group_uuid
    print("\nLoading platform accounts for UUID mapping...")
    try:
        platform_accounts_df = pl.read_csv(
            "csv/output/new_platform_accounts.csv", infer_schema_length=100000
        )

        # Create mapping from platform_login_id to both platform_account_uuid and platform_group_uuid
        login_to_account_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )
        login_to_group_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["platform_group_uuid"],
            )
        )
        print(
            f"Created platform account UUID mapping for {len(login_to_account_uuid)} accounts"
        )

    except FileNotFoundError:
        print("⚠️  Warning: csv/output/new_platform_accounts.csv not found.")
        print("Please run platform accounts export first.")
        return

    # Map account_login to platform_account_uuid and platform_group_uuid
    print("\nMapping account_login to platform_account_uuid and platform_group_uuid...")
    account_stats_df = account_stats_df.with_columns(
        [
            pl.col("account_login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_account_uuid.get(x)
                    if x is not None and x in login_to_account_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("platform_account_uuid"),
            pl.col("account_login")
            .cast(pl.Utf8)
            .map_elements(
                lambda x: (
                    login_to_group_uuid.get(x)
                    if x is not None and x in login_to_group_uuid
                    else None
                ),
                return_dtype=pl.Utf8,
            )
            .alias("platform_group_uuid"),
        ]
    )

    # Filter out records without valid UUID mappings
    print("\nFiltering records with valid UUID mappings...")
    original_count = len(account_stats_df)
    account_stats_df = account_stats_df.filter(
        pl.col("platform_account_uuid").is_not_null()
        & pl.col("platform_group_uuid").is_not_null()
    )
    filtered_count = len(account_stats_df)
    removed_count = original_count - filtered_count

    print(f"  Original records: {original_count}")
    print(f"  Records with valid mappings: {filtered_count}")
    print(f"  Records removed: {removed_count}")

    # Add new fields required by PostgreSQL schema
    print("\nAdding new fields for PostgreSQL schema...")

    # Generate UUIDs for primary key
    account_stats_df = account_stats_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(account_stats_df))])
    )

    # Add calculated fields
    account_stats_df = account_stats_df.with_columns(
        [
            # current_balance = current_equity
            pl.col("current_equity").alias("current_balance"),
            # current_profit = current_equity - yesterday_equity
            (pl.col("current_equity") - pl.col("yesterday_equity")).alias(
                "current_profit"
            ),
            # lowest_watermark = 0.0 (default)
            pl.lit(0.0).alias("lowest_watermark"),
            # highest_watermark = current_equity
            pl.col("current_equity").alias("highest_watermark"),
            # trading_days_count = 0 (default)
            pl.lit(0).alias("trading_days_count"),
            # first_trade_date = NULL (default)
            pl.lit(None).alias("first_trade_date"),
            # total_trade_count = 0 (default)
            pl.lit(0).alias("total_trade_count"),
        ]
    )

    # Remove old fields that are not in PostgreSQL schema
    print("Removing old fields not in PostgreSQL schema...")
    fields_to_remove = ["account_login", "group"]
    account_stats_df = account_stats_df.drop(fields_to_remove)
    print(f"Removed fields: {fields_to_remove}")

    # Reorder columns to match PostgreSQL schema order (uuid first)
    print("Reordering columns to match PostgreSQL schema...")
    postgresql_column_order = [
        "uuid",  # Primary key first
        "status",
        "current_equity",
        "yesterday_equity",
        "performance_percent",
        "current_overall_drawdown",
        "current_daily_drawdown",
        "average_win",
        "average_loss",
        "hit_ratio",
        "best_trade",
        "worst_trade",
        "max_consecutive_wins",
        "max_consecutive_losses",
        "trades_without_stoploss",
        "most_traded_asset",
        "win_coefficient",
        "avg_win_loss_coefficient",
        "best_worst_coefficient",
        "maximum_daily_drawdown",
        "maximum_overall_drawdown",
        "consistency_score",
        "lowest_watermark",
        "highest_watermark",
        "current_balance",
        "current_profit",
        "platform_group_uuid",
        "platform_account_uuid",
        "trading_days_count",
        "first_trade_date",
        "total_trade_count",
    ]

    # Only include columns that exist in the DataFrame
    available_columns = [
        col for col in postgresql_column_order if col in account_stats_df.columns
    ]
    account_stats_df = account_stats_df.select(available_columns)

    print(f"\nAccount Stats DataFrame shape: {account_stats_df.shape}")
    print(
        f"Selected columns ({len(available_columns)}): {', '.join(available_columns)}"
    )

    # Show sample data
    print("\nFirst few rows:")
    print(account_stats_df.head(3))

    if generate:
        print(f"\nGenerating csv/output/new_account_stats.csv...")
        account_stats_df.write_csv("csv/output/new_account_stats.csv")
        print(
            f"✅ Successfully generated csv/output/new_account_stats.csv with {len(account_stats_df)} rows and {len(available_columns)} columns"
        )
    else:
        print(
            f"\nTo generate csv/output/new_account_stats.csv, run with --generate flag:"
        )
        print(f"  uv run main.py --generate --account-stats")

    print("=" * 70)
    print("                          MIGRATION COMPLETE                         ")
    print("=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export account statistics")
    parser.add_argument(
        "--generate", action="store_true", help="Generate the output CSV file"
    )
    args = parser.parse_args()

    export_account_stats(generate=args.generate)
