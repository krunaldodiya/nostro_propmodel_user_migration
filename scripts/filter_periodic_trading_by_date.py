import polars as pl
from pathlib import Path
import os
import argparse


def filter_by_uuid(
    input_file: str = None,
    output_file: str = None,
    platform_account_uuid: str = None,
):
    """
    Filter pre-filtered periodic trading export CSV by platform_account_uuid.
    The input CSV should already be filtered by date.

    Args:
        input_file (str): Path to input CSV file. Defaults to parent directory/filtered_periodic_trading_export.csv
        output_file (str): Path to output CSV file. Defaults to {uuid}/filtered.csv
        platform_account_uuid (str): Platform account UUID to filter by. Required parameter.
    """
    # Get workspace root directory and parent directory
    workspace_root = Path(__file__).parent.parent
    parent_dir = workspace_root.parent  # /home/krunaldodiya/WorkSpace/Code/

    # Set default input file path - use the pre-filtered CSV
    if input_file is None:
        input_file = str(parent_dir / "filtered_periodic_trading_export.csv")

    # Check if UUID is provided
    if not platform_account_uuid:
        print("❌ Error: platform_account_uuid is required")
        return

    # Set default output file path
    if output_file is None:
        # Save to {uuid}/filtered.csv in parent directory
        output_file = str(parent_dir / platform_account_uuid / "filtered.csv")

    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"\n❌ Error: Input file not found: {input_file}")
        print(f"Or specify the path manually when calling the function.")
        return

    print(f"Loading {input_file}...")
    try:
        trading_df = pl.read_csv(
            input_file,
            infer_schema_length=100000,
            ignore_errors=True,
        )
        print(f"✅ Loaded {len(trading_df)} periodic trading records")
    except Exception as e:
        print(f"❌ Error loading CSV file: {e}")
        return

    # Check if platform_account_uuid column exists
    if "platform_account_uuid" not in trading_df.columns:
        print("❌ Error: 'platform_account_uuid' column not found in CSV file")
        print(f"Available columns: {', '.join(trading_df.columns)}")
        return

    # Filter by platform_account_uuid
    print(f"\nFiltering by platform_account_uuid: {platform_account_uuid}...")
    initial_count = len(trading_df)
    uuid_str = str(platform_account_uuid)

    trading_df = trading_df.filter(
        pl.col("platform_account_uuid").cast(pl.Utf8) == uuid_str
    )

    filtered_count = len(trading_df)
    removed_count = initial_count - filtered_count

    print(f"  Initial records: {initial_count:,}")
    print(f"  Records matching UUID: {filtered_count:,}")
    print(f"  Records removed: {removed_count:,}")
    print(f"  Filter rate: {filtered_count/initial_count*100:.1f}%")

    if filtered_count == 0:
        print(f"⚠️  Warning: No records found for UUID '{platform_account_uuid}'")
        return

    # Show preview
    print(f"\nFiltered DataFrame shape: {trading_df.shape}")
    print("First few rows:")
    print(trading_df.head())

    # Generate output file
    print(f"\nGenerating {output_file}...")
    try:
        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        trading_df.write_csv(output_file)
        print(
            f"✅ Successfully generated {output_file} with {len(trading_df):,} rows and {len(trading_df.columns)} columns"
        )
        print(f"Included columns: {', '.join(trading_df.columns)}")

        # Show file size
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        print(f"Output file size: {file_size:.2f} MB")
    except Exception as e:
        print(f"❌ Error writing output file: {e}")


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Filter pre-filtered periodic trading export CSV by platform_account_uuid"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to input CSV file (default: parent directory/filtered_periodic_trading_export.csv)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to output CSV file (default: {uuid}/filtered.csv)",
    )
    parser.add_argument(
        "--uuid",
        type=str,
        required=True,
        dest="platform_account_uuid",
        help="Platform account UUID to filter by. Required. Output will be saved to {uuid}/filtered.csv",
    )

    args = parser.parse_args()

    # Run with parsed arguments
    filter_by_uuid(
        input_file=args.input,
        output_file=args.output,
        platform_account_uuid=args.platform_account_uuid,
    )
