#!/usr/bin/env python3
"""
Comprehensive Export Documentation Generator

This script generates detailed documentation for all export processes
without regenerating any data - it only analyzes existing files.

READ-ONLY: This script does not modify or regenerate any CSV files.
"""

import polars as pl
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from export_documentation import ExportDocumentation, analyze_platform_accounts_export


def analyze_discount_codes_export():
    """Analyze discount codes export process"""
    print("Analyzing Discount Codes Export Process (READ-ONLY)...")

    # Load original data
    original_df = pl.read_csv(
        "csv/input/discount_codes.csv", infer_schema_length=100000
    )
    original_count = len(original_df)

    # Create documentation object
    doc = ExportDocumentation("Discount Codes", "csv/input/discount_codes.csv")

    # Step 1: Duplicate code removal
    unique_codes = original_df["code"].n_unique()
    duplicate_codes = original_count - unique_codes

    if duplicate_codes > 0:
        after_dedup = unique_codes
        doc.add_step(
            "Code Deduplication",
            "Remove duplicate discount codes, keeping first occurrence",
            original_count,
            after_dedup,
            reason="Database constraint violation - code must be unique",
            details={"duplicate_codes": duplicate_codes, "unique_codes": unique_codes},
        )
    else:
        after_dedup = original_count
        doc.add_step(
            "Code Deduplication",
            "Check for duplicate discount codes",
            original_count,
            after_dedup,
            reason="No duplicates found",
            details={"duplicate_codes": 0},
        )

    # Load final output to get accurate count
    try:
        final_output_df = pl.read_csv("csv/output/new_discount_codes.csv")
        final_count = len(final_output_df)
        final_columns = len(final_output_df.columns)
        print(f"Found existing output file with {final_count:,} records")
    except FileNotFoundError:
        print("No existing output file found")
        final_count = after_dedup
        final_columns = 8

    # Set final statistics
    doc.set_final_stats(
        total_records=final_count, total_columns=final_columns, file_size="1M"
    )

    # Set filtering summary
    doc.set_filtering_summary(
        original_count=original_count,
        final_count=final_count,
        total_removed=original_count - final_count,
    )

    # Save documentation
    doc.save_documentation()
    return doc


def analyze_purchases_export():
    """Analyze purchases export process"""
    print("Analyzing Purchases Export Process (READ-ONLY)...")

    # Load original data
    original_df = pl.read_csv(
        "csv/input/purchases.csv",
        infer_schema_length=100000,
        ignore_errors=True,
        schema_overrides={"amount_total": pl.Float64},
    )
    original_count = len(original_df)

    # Create documentation object
    doc = ExportDocumentation("Purchases", "csv/input/purchases.csv")

    # Step 1: User mapping filtering
    # Load users to check mapping
    try:
        # Load original users to get the id column
        original_users_df = pl.read_csv("csv/input/users.csv", infer_schema_length=1000)
        new_users_df = pl.read_csv("csv/output/new_users.csv")

        # Create mapping from original id to new uuid
        user_mapping = dict(zip(original_users_df["id"], new_users_df["uuid"]))

        # Check how many purchases have valid user mappings
        purchases_with_users = 0
        purchases_without_users = 0

        for row in original_df.iter_rows(named=True):
            if row["user_id"] in user_mapping:
                purchases_with_users += 1
            else:
                purchases_without_users += 1

        if purchases_without_users > 0:
            after_user_filter = purchases_with_users
            doc.add_step(
                "User Mapping Filtering",
                "Remove purchases without valid user mappings",
                original_count,
                after_user_filter,
                reason="Foreign key constraint - user_id must exist in users table",
                details={
                    "purchases_with_users": purchases_with_users,
                    "purchases_without_users": purchases_without_users,
                },
            )
        else:
            after_user_filter = original_count
            doc.add_step(
                "User Mapping Filtering",
                "Check for purchases without valid user mappings",
                original_count,
                after_user_filter,
                reason="All purchases have valid user mappings",
                details={"purchases_without_users": 0},
            )
    except FileNotFoundError:
        print("Users file not found - skipping user mapping analysis")
        after_user_filter = original_count
        doc.add_step(
            "User Mapping Filtering",
            "Check for purchases without valid user mappings",
            original_count,
            after_user_filter,
            reason="Users file not available for analysis",
            details={"status": "skipped"},
        )

    # Step 2: Discount code mapping filtering
    try:
        # Load original discount codes to get the id column
        original_discount_codes_df = pl.read_csv(
            "csv/input/discount_codes.csv", infer_schema_length=1000
        )
        new_discount_codes_df = pl.read_csv("csv/output/new_discount_codes.csv")

        # Create mapping from original id to new uuid
        discount_mapping = dict(
            zip(original_discount_codes_df["id"], new_discount_codes_df["uuid"])
        )

        # Check how many purchases have valid discount mappings
        purchases_with_discounts = 0
        purchases_without_discounts = 0

        for row in original_df.iter_rows(named=True):
            if row.get("discount_id") and row["discount_id"] in discount_mapping:
                purchases_with_discounts += 1
            else:
                purchases_without_discounts += 1

        # Note: purchases without discount_id are valid (nullable field)
        after_discount_filter = after_user_filter
        doc.add_step(
            "Discount Code Mapping",
            "Map discount codes to new UUIDs",
            after_user_filter,
            after_discount_filter,
            reason="Discount codes are optional - null values are preserved",
            details={
                "purchases_with_discounts": purchases_with_discounts,
                "purchases_without_discounts": purchases_without_discounts,
            },
        )
    except FileNotFoundError:
        print("Discount codes file not found - skipping discount mapping analysis")
        after_discount_filter = after_user_filter
        doc.add_step(
            "Discount Code Mapping",
            "Map discount codes to new UUIDs",
            after_user_filter,
            after_discount_filter,
            reason="Discount codes file not available for analysis",
            details={"status": "skipped"},
        )

    # Load final output to get accurate count
    try:
        final_output_df = pl.read_csv("csv/output/new_purchases.csv")
        final_count = len(final_output_df)
        final_columns = len(final_output_df.columns)
        print(f"Found existing output file with {final_count:,} records")
    except FileNotFoundError:
        print("No existing output file found")
        final_count = after_discount_filter
        final_columns = 12

    # Set final statistics
    doc.set_final_stats(
        total_records=final_count, total_columns=final_columns, file_size="15M"
    )

    # Set filtering summary
    doc.set_filtering_summary(
        original_count=original_count,
        final_count=final_count,
        total_removed=original_count - final_count,
    )

    # Save documentation
    doc.save_documentation()
    return doc


def analyze_equity_data_daily_export():
    """Analyze equity data daily export process"""
    print("Analyzing Equity Data Daily Export Process (READ-ONLY)...")

    # Load original data
    original_df = pl.read_csv(
        "csv/input/equity_data_daily.csv", infer_schema_length=100000
    )
    original_count = len(original_df)

    # Create documentation object
    doc = ExportDocumentation("Equity Data Daily", "csv/input/equity_data_daily.csv")

    # Step 1: Platform account mapping filtering
    try:
        platform_accounts_df = pl.read_csv("csv/output/new_platform_accounts.csv")
        account_mapping = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )

        # Check how many equity records have valid platform account mappings
        equity_with_accounts = 0
        equity_without_accounts = 0

        for row in original_df.iter_rows(named=True):
            trading_account_str = str(row["trading_account"])
            if trading_account_str in account_mapping:
                equity_with_accounts += 1
            else:
                equity_without_accounts += 1

        if equity_without_accounts > 0:
            after_account_filter = equity_with_accounts
            doc.add_step(
                "Platform Account Mapping Filtering",
                "Remove equity records without valid platform account mappings",
                original_count,
                after_account_filter,
                reason="Foreign key constraint - platform_account_uuid must exist in platform_accounts table",
                details={
                    "equity_with_accounts": equity_with_accounts,
                    "equity_without_accounts": equity_without_accounts,
                },
            )
        else:
            after_account_filter = original_count
            doc.add_step(
                "Platform Account Mapping Filtering",
                "Check for equity records without valid platform account mappings",
                original_count,
                after_account_filter,
                reason="All equity records have valid platform account mappings",
                details={"equity_without_accounts": 0},
            )
    except FileNotFoundError:
        print("Platform accounts file not found - skipping account mapping analysis")
        after_account_filter = original_count
        doc.add_step(
            "Platform Account Mapping Filtering",
            "Check for equity records without valid platform account mappings",
            original_count,
            after_account_filter,
            reason="Platform accounts file not available for analysis",
            details={"status": "skipped"},
        )

    # Load final output to get accurate count
    try:
        final_output_df = pl.read_csv("csv/output/new_equity_data_daily.csv")
        final_count = len(final_output_df)
        final_columns = len(final_output_df.columns)
        print(f"Found existing output file with {final_count:,} records")
    except FileNotFoundError:
        print("No existing output file found")
        final_count = after_account_filter
        final_columns = 7

    # Set final statistics
    doc.set_final_stats(
        total_records=final_count, total_columns=final_columns, file_size="8M"
    )

    # Set filtering summary
    doc.set_filtering_summary(
        original_count=original_count,
        final_count=final_count,
        total_removed=original_count - final_count,
    )

    # Save documentation
    doc.save_documentation()
    return doc


def analyze_periodic_trading_export():
    """Analyze periodic trading export process"""
    print("Analyzing Periodic Trading Export Process (READ-ONLY)...")

    # Load original data
    original_df = pl.read_csv(
        "csv/input/periodic_trading_export.csv", infer_schema_length=100000
    )
    original_count = len(original_df)

    # Create documentation object
    doc = ExportDocumentation(
        "Periodic Trading Export", "csv/input/periodic_trading_export.csv"
    )

    # Step 1: Platform account mapping filtering
    try:
        platform_accounts_df = pl.read_csv("csv/output/new_platform_accounts.csv")
        account_mapping = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )

        # Check how many trading records have valid platform account mappings
        trading_with_accounts = 0
        trading_without_accounts = 0

        for row in original_df.iter_rows(named=True):
            trading_account_str = str(row["trading_account"])
            if trading_account_str in account_mapping:
                trading_with_accounts += 1
            else:
                trading_without_accounts += 1

        if trading_without_accounts > 0:
            after_account_filter = trading_with_accounts
            doc.add_step(
                "Platform Account Mapping Filtering",
                "Remove trading records without valid platform account mappings",
                original_count,
                after_account_filter,
                reason="Foreign key constraint - platform_account_uuid must exist in platform_accounts table",
                details={
                    "trading_with_accounts": trading_with_accounts,
                    "trading_without_accounts": trading_without_accounts,
                },
            )
        else:
            after_account_filter = original_count
            doc.add_step(
                "Platform Account Mapping Filtering",
                "Check for trading records without valid platform account mappings",
                original_count,
                after_account_filter,
                reason="All trading records have valid platform account mappings",
                details={"trading_without_accounts": 0},
            )
    except FileNotFoundError:
        print("Platform accounts file not found - skipping account mapping analysis")
        after_account_filter = original_count
        doc.add_step(
            "Platform Account Mapping Filtering",
            "Check for trading records without valid platform account mappings",
            original_count,
            after_account_filter,
            reason="Platform accounts file not available for analysis",
            details={"status": "skipped"},
        )

    # Load final output to get accurate count
    try:
        final_output_df = pl.read_csv("csv/output/new_periodic_trading_export.csv")
        final_count = len(final_output_df)
        final_columns = len(final_output_df.columns)
        print(f"Found existing output file with {final_count:,} records")
    except FileNotFoundError:
        print("No existing output file found")
        final_count = after_account_filter
        final_columns = 15

    # Set final statistics
    doc.set_final_stats(
        total_records=final_count, total_columns=final_columns, file_size="127M"
    )

    # Set filtering summary
    doc.set_filtering_summary(
        original_count=original_count,
        final_count=final_count,
        total_removed=original_count - final_count,
    )

    # Save documentation
    doc.save_documentation()
    return doc


def analyze_advanced_challenge_settings_export():
    """Analyze advanced challenge settings export process"""
    print("Analyzing Advanced Challenge Settings Export Process (READ-ONLY)...")

    # Create documentation object
    doc = ExportDocumentation(
        "Advanced Challenge Settings", "Generated from platform data"
    )

    # Load source data
    try:
        platform_groups_df = pl.read_csv("csv/output/new_platform_groups.csv")
        platform_accounts_df = pl.read_csv("csv/output/new_platform_accounts.csv")

        group_count = len(platform_groups_df)
        account_count = len(platform_accounts_df)
        total_records = group_count + account_count

        # Step 1: Platform groups processing
        doc.add_step(
            "Platform Groups Processing",
            "Generate challenge settings for each platform group",
            0,
            group_count,
            reason="Each platform group needs default challenge settings",
            details={
                "platform_groups_processed": group_count,
                "platform_account_uuid_set_to_null": True,
            },
        )

        # Step 2: Platform accounts processing
        doc.add_step(
            "Platform Accounts Processing",
            "Generate challenge settings for each platform account",
            group_count,
            total_records,
            reason="Each platform account needs default challenge settings",
            details={
                "platform_accounts_processed": account_count,
                "platform_group_uuid_set_to_null": True,
            },
        )

        # Load final output to get accurate count
        try:
            final_output_df = pl.read_csv(
                "csv/output/new_advanced_challenge_settings.csv"
            )
            final_count = len(final_output_df)
            final_columns = len(final_output_df.columns)
            print(f"Found existing output file with {final_count:,} records")
        except FileNotFoundError:
            print("No existing output file found")
            final_count = total_records
            final_columns = 41

        # Set final statistics
        doc.set_final_stats(
            total_records=final_count, total_columns=final_columns, file_size="25M"
        )

        # Set filtering summary
        doc.set_filtering_summary(
            original_count=0, final_count=final_count, total_removed=0  # Generated data
        )

    except FileNotFoundError as e:
        print(f"Source files not found: {e}")
        doc.add_step(
            "Data Generation",
            "Generate challenge settings from platform data",
            0,
            0,
            reason="Source files not available",
            details={"status": "failed", "error": str(e)},
        )

        # Set final statistics
        doc.set_final_stats(total_records=0, total_columns=41, file_size="0M")

        # Set filtering summary
        doc.set_filtering_summary(original_count=0, final_count=0, total_removed=0)

    # Save documentation
    doc.save_documentation()
    return doc


def generate_all_documentation():
    """Generate documentation for all export processes"""
    print("=" * 60)
    print("GENERATING COMPREHENSIVE EXPORT DOCUMENTATION")
    print("=" * 60)
    print("READ-ONLY: This process only analyzes existing data")
    print("No CSV files will be regenerated or modified")
    print("=" * 60)

    # Create docs directory
    os.makedirs("docs/exports", exist_ok=True)

    # Generate documentation for each export
    exports = [
        ("Platform Accounts", analyze_platform_accounts_export),
        ("Discount Codes", analyze_discount_codes_export),
        ("Purchases", analyze_purchases_export),
        ("Equity Data Daily", analyze_equity_data_daily_export),
        ("Periodic Trading Export", analyze_periodic_trading_export),
        ("Advanced Challenge Settings", analyze_advanced_challenge_settings_export),
    ]

    results = {}

    for export_name, analyze_func in exports:
        print(f"\n{'='*20} {export_name} {'='*20}")
        try:
            doc = analyze_func()
            results[export_name] = "SUCCESS"
            print(f"✅ {export_name} documentation generated successfully")
        except Exception as e:
            results[export_name] = f"ERROR: {str(e)}"
            print(f"❌ {export_name} documentation failed: {str(e)}")

    # Generate summary
    print(f"\n{'='*60}")
    print("DOCUMENTATION GENERATION SUMMARY")
    print(f"{'='*60}")

    for export_name, status in results.items():
        status_icon = "✅" if status == "SUCCESS" else "❌"
        print(f"{status_icon} {export_name}: {status}")

    print(f"\nDocumentation files saved in: docs/exports/")
    print("Each export has both .md (markdown) and .json (statistics) files")

    return results


if __name__ == "__main__":
    generate_all_documentation()
