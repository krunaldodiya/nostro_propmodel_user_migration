import polars as pl
import uuid
import json
from datetime import datetime


def export_advanced_challenge_settings(generate=False):
    """
    Export advanced challenge settings from platform groups and platform accounts to new_advanced_challenge_settings.csv

    Args:
        generate (bool): If True, generates new_advanced_challenge_settings.csv file. If False, only previews.
    """

    print("Loading default challenge settings...")

    # Load default challenge settings
    default_settings_df = pl.read_csv("csv/input/default_challenge_settings.csv")
    print(f"Loaded {len(default_settings_df)} default settings records")

    # Get the first (and only) default settings record
    default_settings = default_settings_df.row(0, named=True)
    print(f"Using default settings from: {default_settings['uuid']}")

    print("\nLoading platform groups and platform accounts...")

    # Load platform groups
    platform_groups_df = pl.read_csv("csv/output/new_platform_groups.csv")
    print(f"Loaded {len(platform_groups_df)} platform groups")

    # Load platform accounts
    platform_accounts_df = pl.read_csv("csv/output/new_platform_accounts.csv")
    print(f"Loaded {len(platform_accounts_df)} platform accounts")

    # Create records for platform groups
    print("\nCreating advanced challenge settings for platform groups...")
    group_records = []

    for row in platform_groups_df.iter_rows(named=True):
        group_record = {
            "uuid": str(uuid.uuid4()),
            "100_profit_split": default_settings.get("100_profit_split", False),
            "2_percent_lower_target": default_settings.get(
                "2_percent_lower_target", False
            ),
            "2_percent_more_daily_drawdown": default_settings.get(
                "2_percent_more_daily_drawdown", False
            ),
            "2_percent_more_max_drawdown": default_settings.get(
                "2_percent_more_max_drawdown", False
            ),
            "allow_expert_advisors": default_settings.get(
                "allow_expert_advisors", False
            ),
            "breach_type": default_settings.get("breach_type"),
            "close_all_positions_on_friday": default_settings.get(
                "close_all_positions_on_friday", False
            ),
            "delete_account_after_failure": default_settings.get(
                "delete_account_after_failure"
            ),
            "delete_account_after_failure_unit": default_settings.get(
                "delete_account_after_failure_unit"
            ),
            "double_leverage": default_settings.get("double_leverage", False),
            "held_over_the_weekend": default_settings.get(
                "held_over_the_weekend", False
            ),
            "inactivity_breach_trigger": default_settings.get(
                "inactivity_breach_trigger"
            ),
            "inactivity_breach_trigger_unit": default_settings.get(
                "inactivity_breach_trigger_unit"
            ),
            "max_open_lots": default_settings.get("max_open_lots"),
            "max_risk_per_symbol": default_settings.get("max_risk_per_symbol"),
            "max_time_per_evaluation_phase": default_settings.get(
                "max_time_per_evaluation_phase"
            ),
            "max_time_per_evaluation_phase_unit": default_settings.get(
                "max_time_per_evaluation_phase_unit"
            ),
            "max_time_per_funded_phase": default_settings.get(
                "max_time_per_funded_phase"
            ),
            "max_time_per_funded_phase_unit": default_settings.get(
                "max_time_per_funded_phase_unit"
            ),
            "max_trading_days": default_settings.get("max_trading_days"),
            "min_time_per_phase": default_settings.get("min_time_per_phase"),
            "min_time_per_phase_unit": default_settings.get("min_time_per_phase_unit"),
            "no_sl_required": default_settings.get("no_sl_required", False),
            "requires_stop_loss": default_settings.get("requires_stop_loss", True),
            "requires_take_profit": default_settings.get("requires_take_profit", False),
            "time_between_withdrawals": default_settings.get(
                "time_between_withdrawals"
            ),
            "time_between_withdrawals_unit": default_settings.get(
                "time_between_withdrawals_unit"
            ),
            "visible_on_leaderboard": default_settings.get(
                "visible_on_leaderboard", True
            ),
            "withdraw_within": default_settings.get("withdraw_within"),
            "withdraw_within_unit": default_settings.get("withdraw_within_unit"),
            "platform_group_uuid": row["uuid"],
            "platform_account_uuid": None,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "account_leverage": default_settings.get("account_leverage", 0),
            "profit_split": default_settings.get("profit_split", 0),
            "profit_target": str(default_settings.get("profit_target", "")),
            "max_drawdown": default_settings.get("max_drawdown", 0),
            "max_daily_drawdown": default_settings.get("max_daily_drawdown", 0),
            "min_trading_days": default_settings.get("min_trading_days"),
        }
        group_records.append(group_record)

    print(f"Created {len(group_records)} group records")

    # Create records for platform accounts
    print("\nCreating advanced challenge settings for platform accounts...")
    account_records = []

    for row in platform_accounts_df.iter_rows(named=True):
        account_record = {
            "uuid": str(uuid.uuid4()),
            "100_profit_split": default_settings.get("100_profit_split", False),
            "2_percent_lower_target": default_settings.get(
                "2_percent_lower_target", False
            ),
            "2_percent_more_daily_drawdown": default_settings.get(
                "2_percent_more_daily_drawdown", False
            ),
            "2_percent_more_max_drawdown": default_settings.get(
                "2_percent_more_max_drawdown", False
            ),
            "allow_expert_advisors": default_settings.get(
                "allow_expert_advisors", False
            ),
            "breach_type": default_settings.get("breach_type"),
            "close_all_positions_on_friday": default_settings.get(
                "close_all_positions_on_friday", False
            ),
            "delete_account_after_failure": default_settings.get(
                "delete_account_after_failure"
            ),
            "delete_account_after_failure_unit": default_settings.get(
                "delete_account_after_failure_unit"
            ),
            "double_leverage": default_settings.get("double_leverage", False),
            "held_over_the_weekend": default_settings.get(
                "held_over_the_weekend", False
            ),
            "inactivity_breach_trigger": default_settings.get(
                "inactivity_breach_trigger"
            ),
            "inactivity_breach_trigger_unit": default_settings.get(
                "inactivity_breach_trigger_unit"
            ),
            "max_open_lots": default_settings.get("max_open_lots"),
            "max_risk_per_symbol": default_settings.get("max_risk_per_symbol"),
            "max_time_per_evaluation_phase": default_settings.get(
                "max_time_per_evaluation_phase"
            ),
            "max_time_per_evaluation_phase_unit": default_settings.get(
                "max_time_per_evaluation_phase_unit"
            ),
            "max_time_per_funded_phase": default_settings.get(
                "max_time_per_funded_phase"
            ),
            "max_time_per_funded_phase_unit": default_settings.get(
                "max_time_per_funded_phase_unit"
            ),
            "max_trading_days": default_settings.get("max_trading_days"),
            "min_time_per_phase": default_settings.get("min_time_per_phase"),
            "min_time_per_phase_unit": default_settings.get("min_time_per_phase_unit"),
            "no_sl_required": default_settings.get("no_sl_required", False),
            "requires_stop_loss": default_settings.get("requires_stop_loss", True),
            "requires_take_profit": default_settings.get("requires_take_profit", False),
            "time_between_withdrawals": default_settings.get(
                "time_between_withdrawals"
            ),
            "time_between_withdrawals_unit": default_settings.get(
                "time_between_withdrawals_unit"
            ),
            "visible_on_leaderboard": default_settings.get(
                "visible_on_leaderboard", True
            ),
            "withdraw_within": default_settings.get("withdraw_within"),
            "withdraw_within_unit": default_settings.get("withdraw_within_unit"),
            "platform_group_uuid": None,
            "platform_account_uuid": row["uuid"],
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "account_leverage": default_settings.get("account_leverage", 0),
            "profit_split": default_settings.get("profit_split", 0),
            "profit_target": str(default_settings.get("profit_target", "")),
            "max_drawdown": default_settings.get("max_drawdown", 0),
            "max_daily_drawdown": default_settings.get("max_daily_drawdown", 0),
            "min_trading_days": default_settings.get("min_trading_days"),
        }
        account_records.append(account_record)

    print(f"Created {len(account_records)} account records")

    # Combine all records
    all_records = group_records + account_records
    print(f"\nTotal advanced challenge settings records: {len(all_records)}")

    # Create DataFrame
    settings_df = pl.DataFrame(all_records)

    # Define column order to match PostgreSQL schema
    column_order = [
        "uuid",
        "100_profit_split",
        "2_percent_lower_target",
        "2_percent_more_daily_drawdown",
        "2_percent_more_max_drawdown",
        "allow_expert_advisors",
        "breach_type",
        "close_all_positions_on_friday",
        "delete_account_after_failure",
        "delete_account_after_failure_unit",
        "double_leverage",
        "held_over_the_weekend",
        "inactivity_breach_trigger",
        "inactivity_breach_trigger_unit",
        "max_open_lots",
        "max_risk_per_symbol",
        "max_time_per_evaluation_phase",
        "max_time_per_evaluation_phase_unit",
        "max_time_per_funded_phase",
        "max_time_per_funded_phase_unit",
        "max_trading_days",
        "min_time_per_phase",
        "min_time_per_phase_unit",
        "no_sl_required",
        "requires_stop_loss",
        "requires_take_profit",
        "time_between_withdrawals",
        "time_between_withdrawals_unit",
        "visible_on_leaderboard",
        "withdraw_within",
        "withdraw_within_unit",
        "platform_group_uuid",
        "platform_account_uuid",
        "created_at",
        "updated_at",
        "account_leverage",
        "profit_split",
        "profit_target",
        "max_drawdown",
        "max_daily_drawdown",
        "min_trading_days",
    ]

    # Reorder columns
    settings_df = settings_df.select(column_order)

    print(f"\nAdvanced Challenge Settings DataFrame shape: {settings_df.shape}")
    print(f"Selected columns ({len(column_order)}): {column_order}")
    print("\nFirst few rows:")
    print(settings_df.head())

    # Show statistics
    print(f"\n=== ADVANCED CHALLENGE SETTINGS STATISTICS ===")
    print(f"Total records: {len(settings_df):,}")

    # Count records by type
    group_count = settings_df.filter(pl.col("platform_group_uuid").is_not_null()).height
    account_count = settings_df.filter(
        pl.col("platform_account_uuid").is_not_null()
    ).height

    print(f"\nRecord distribution:")
    print(f"  Platform group records: {group_count:,}")
    print(f"  Platform account records: {account_count:,}")
    print(f"  Total: {group_count + account_count:,}")

    # Show some boolean field statistics
    print(f"\nBoolean field statistics:")
    for col in [
        "100_profit_split",
        "2_percent_lower_target",
        "requires_stop_loss",
        "visible_on_leaderboard",
    ]:
        if col in settings_df.columns:
            true_count = settings_df.filter(pl.col(col) == True).height
            print(
                f"  {col}: {true_count:,} true, {len(settings_df) - true_count:,} false"
            )

    # Save the processed data to csv/output/new_advanced_challenge_settings.csv only if generate flag is True
    if generate:
        settings_df.write_csv("csv/output/new_advanced_challenge_settings.csv")
        print(
            f"\nSuccessfully generated csv/output/new_advanced_challenge_settings.csv with {len(settings_df)} rows and {len(settings_df.columns)} columns"
        )
        print(f"Included columns: {', '.join(column_order)}")
    else:
        print(
            f"\nTo generate csv/output/new_advanced_challenge_settings.csv, run with --generate flag:"
        )
        print("  uv run main.py --generate --advanced-challenge-settings")


if __name__ == "__main__":
    export_advanced_challenge_settings(generate=True)
