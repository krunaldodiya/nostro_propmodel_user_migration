#!/usr/bin/env python3
"""
Payout Requests Export Script

This script exports payout requests from the MySQL CSV format to PostgreSQL format.
It maps user_id to user_uuid, program_login to platform_account_uuid, and handles
special logic for payout_id generation and note mapping.

Usage:
    python scripts/payout_requests_export.py [--generate]
"""

import polars as pl
import json
import uuid
import random
import string
from datetime import datetime


def generate_affiliate_payout_id():
    """Generate a random payout ID in format AFF-XXXXXXXX"""
    random_part = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    return f"AFF-{random_part}"


def export_payout_requests(generate: bool = False):
    """
    Export payout requests from MySQL CSV to PostgreSQL format.

    Args:
        generate (bool): If True, generates new_payout_requests.csv file. If False, only previews.
    """
    print("=" * 70)
    print("                   EXPORTING PAYOUT_REQUESTS                   ")
    print("=" * 70)

    # Load payout requests CSV
    print("Loading csv/input/payout_requests.csv...")
    payout_requests_df = pl.read_csv(
        "csv/input/payout_requests.csv", infer_schema_length=100000
    )
    print(f"Loaded {len(payout_requests_df)} payout request records")

    # Load users to get user_uuid mapping
    print("\nLoading users for UUID mapping...")
    try:
        # Load original users CSV for id mapping
        original_users_df = pl.read_csv(
            "csv/input/users.csv", infer_schema_length=100000
        )
        # Load generated users CSV for uuid mapping
        generated_users_df = pl.read_csv(
            "csv/output/new_users.csv", infer_schema_length=100000
        )

        # Create mapping from user_id to user_uuid
        user_id_to_uuid = dict(
            zip(original_users_df["id"].cast(pl.Utf8), generated_users_df["uuid"])
        )
        print(f"Created user UUID mapping for {len(user_id_to_uuid)} users")

    except FileNotFoundError:
        print("⚠️  Warning: csv/input/users.csv or csv/output/new_users.csv not found.")
        print("Please run users export first.")
        return

    # Load platform accounts to get platform_account_uuid mapping
    print("\nLoading platform accounts for UUID mapping...")
    try:
        platform_accounts_df = pl.read_csv(
            "csv/output/new_platform_accounts.csv", infer_schema_length=100000
        )

        # Create mapping from platform_login_id to platform_account_uuid
        login_to_uuid = dict(
            zip(
                platform_accounts_df["platform_login_id"].cast(pl.Utf8),
                platform_accounts_df["uuid"],
            )
        )
        print(
            f"Created platform account UUID mapping for {len(login_to_uuid)} accounts"
        )

    except FileNotFoundError:
        print("⚠️  Warning: csv/output/new_platform_accounts.csv not found.")
        print("Please run platform accounts export first.")
        return

    # Get Scott's UUID for note_created_by
    print("\nFinding Scott's UUID for note_created_by...")
    try:
        scott_user = generated_users_df.filter(pl.col("email") == "scott@nostro.co")
        if len(scott_user) > 0:
            scott_uuid = scott_user["uuid"][0]
            print(f"Found Scott's UUID: {scott_uuid}")
        else:
            print("⚠️  Warning: scott@nostro.co not found in users.")
            scott_uuid = None
    except Exception as e:
        print(f"⚠️  Warning: Error finding Scott's UUID: {e}")
        scott_uuid = None

    # Map user_id to user_uuid
    print("\nMapping user_id to user_uuid...")
    payout_requests_df = payout_requests_df.with_columns(
        pl.col("user_id")
        .cast(pl.Utf8)
        .map_elements(
            lambda x: (
                user_id_to_uuid.get(x)
                if x is not None and x in user_id_to_uuid
                else None
            ),
            return_dtype=pl.Utf8,
        )
        .alias("user_uuid")
    )

    # Map program_login to platform_account_uuid
    print("Mapping program_login to platform_account_uuid...")
    payout_requests_df = payout_requests_df.with_columns(
        pl.col("program_login")
        .cast(pl.Utf8)
        .map_elements(
            lambda x: (
                login_to_uuid.get(x) if x is not None and x in login_to_uuid else None
            ),
            return_dtype=pl.Utf8,
        )
        .alias("platform_account_uuid")
    )

    # Filter out records without valid UUID mappings
    print("\nFiltering records with valid UUID mappings...")
    original_count = len(payout_requests_df)
    payout_requests_df = payout_requests_df.filter(pl.col("user_uuid").is_not_null())
    filtered_count = len(payout_requests_df)
    removed_count = original_count - filtered_count

    print(f"  Original records: {original_count}")
    print(f"  Records with valid mappings: {filtered_count}")
    print(f"  Records removed: {removed_count}")

    # Add new fields required by PostgreSQL schema
    print("\nAdding new fields for PostgreSQL schema...")

    # Generate UUIDs for primary key
    payout_requests_df = payout_requests_df.with_columns(
        pl.Series("uuid", [str(uuid.uuid4()) for _ in range(len(payout_requests_df))])
    )

    # Generate payout_id based on type
    print("Generating payout_id based on type...")
    payout_requests_df = payout_requests_df.with_columns(
        pl.when(pl.col("type") == "affiliate")
        .then(
            pl.Series(
                [generate_affiliate_payout_id() for _ in range(len(payout_requests_df))]
            )
        )
        .otherwise(pl.lit(None))
        .alias("payout_id")
    )

    # Map compliance_note to note
    print("Mapping compliance_note to note...")
    payout_requests_df = payout_requests_df.with_columns(
        pl.col("compliance_note").alias("note")
    )

    # Set note_created_by to Scott's UUID
    print("Setting note_created_by to Scott's UUID...")
    payout_requests_df = payout_requests_df.with_columns(
        pl.lit(scott_uuid).alias("note_created_by")
    )

    # Map status 3 to status 4 to avoid conflicts
    print("Mapping status 3 to status 4...")
    payout_requests_df = payout_requests_df.with_columns(
        pl.when(pl.col("status") == 3)
        .then(pl.lit(4))
        .otherwise(pl.col("status"))
        .alias("status")
    )

    # Count how many records were affected
    status_3_count = len(payout_requests_df.filter(pl.col("status") == 4))
    if status_3_count > 0:
        print(f"  Mapped {status_3_count} records from status 3 to status 4")
    else:
        print("  No records with status 3 found")

    # Remove old fields that are not in PostgreSQL schema
    print("Removing old fields not in PostgreSQL schema...")
    fields_to_remove = [
        "id",
        "user_id",
        "program_login",
        "compliance_note",
        "approved_by",
    ]
    payout_requests_df = payout_requests_df.drop(fields_to_remove)
    print(f"Removed fields: {fields_to_remove}")

    # Reorder columns to match PostgreSQL schema order (uuid first)
    print("Reordering columns to match PostgreSQL schema...")
    postgresql_column_order = [
        "uuid",  # Primary key first
        "user_uuid",
        "type",
        "amount",
        "method",
        "status",
        "data",
        "created_at",
        "updated_at",
        "platform_account_uuid",
        "payout_id",
        "note",
        "note_created_by",
    ]

    # Only include columns that exist in the DataFrame
    available_columns = [
        col for col in postgresql_column_order if col in payout_requests_df.columns
    ]
    payout_requests_df = payout_requests_df.select(available_columns)

    print(f"\nPayout Requests DataFrame shape: {payout_requests_df.shape}")
    print(
        f"Selected columns ({len(available_columns)}): {', '.join(available_columns)}"
    )

    # Show sample data
    print("\nFirst few rows:")
    print(payout_requests_df.head(3))

    # Show statistics
    print("\n=== PAYOUT REQUESTS STATISTICS ===")
    print(f"Total records: {len(payout_requests_df)}")

    # Type distribution
    type_dist = (
        payout_requests_df.group_by("type")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print("\nType distribution:")
    print(type_dist)

    # Status distribution
    status_dist = (
        payout_requests_df.group_by("status")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print("\nStatus distribution:")
    print(status_dist)

    # Method distribution
    method_dist = (
        payout_requests_df.group_by("method")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print("\nMethod distribution:")
    print(method_dist)

    if generate:
        print(f"\nGenerating csv/output/new_payout_requests.csv...")
        payout_requests_df.write_csv("csv/output/new_payout_requests.csv")
        print(
            f"✅ Successfully generated csv/output/new_payout_requests.csv with {len(payout_requests_df)} rows and {len(available_columns)} columns"
        )
    else:
        print(
            f"\nTo generate csv/output/new_payout_requests.csv, run with --generate flag:"
        )
        print(f"  uv run main.py --generate --payout-requests")

    print("=" * 70)
    print("                          MIGRATION COMPLETE                         ")
    print("=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export payout requests")
    parser.add_argument(
        "--generate", action="store_true", help="Generate the output CSV file"
    )
    args = parser.parse_args()

    export_payout_requests(generate=args.generate)
