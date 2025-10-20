import polars as pl


def count_csv_records(file_path: str) -> int:
    """
    Count the number of records (rows) in a CSV file, excluding the header.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        int: Number of data rows (excluding header).
    """
    try:
        # Read CSV using Polars
        df = pl.read_csv(file_path, infer_schema_length=1000, ignore_errors=True)
        count = len(df)
        print(f"✅ '{file_path}' has {count} records (excluding header).")
        return count
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return 0
    except Exception as e:
        print(f"⚠️ Error reading file: {e}")
        return 0


# Example usage
if __name__ == "__main__":
    count_csv_records("csv/output/new_periodic_trading_export_last_6_months.csv")
