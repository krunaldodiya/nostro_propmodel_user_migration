import argparse
import sys


def main():
    """
    Entry point for migration tools.
    Dispatches to specific export functions based on command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Data migration tool for MySQL to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview users export
  uv run main.py --users

  # Generate users export
  uv run main.py --generate --users

  # Preview purchases export
  uv run main.py --purchases

  # Generate purchases export
  uv run main.py --generate --purchases

  # Preview discount codes export
  uv run main.py --discount-codes

  # Generate discount codes export
  uv run main.py --generate --discount-codes

  # Preview platform accounts export
  uv run main.py --platform-accounts

  # Generate platform accounts export
  uv run main.py --generate --platform-accounts

  # Preview platform groups export
  uv run main.py --platform-groups

  # Generate platform groups export
  uv run main.py --generate --platform-groups

  # Preview account stats export
  uv run main.py --account-stats

  # Generate account stats export
  uv run main.py --generate --account-stats

  # Preview payout requests export
  uv run main.py --payout-requests

  # Generate payout requests export
  uv run main.py --generate --payout-requests

  # Preview breach account activities export
  uv run main.py --breach-account-activities

  # Generate breach account activities export
  uv run main.py --generate --breach-account-activities

  # Preview platform events export
  uv run main.py --platform-events

  # Generate platform events export
  uv run main.py --generate --platform-events

  # Preview periodic trading export
  uv run main.py --periodic-trading-export

  # Generate periodic trading export
  uv run main.py --generate --periodic-trading-export

  # Preview equity data daily export
  uv run main.py --equity-data-daily

  # Generate equity data daily export
  uv run main.py --generate --equity-data-daily

  # Preview advanced challenge settings export
  uv run main.py --advanced-challenge-settings

  # Generate advanced challenge settings export
  uv run main.py --generate --advanced-challenge-settings

  # Generate all exports (in dependency order)
  uv run main.py --generate --all
        """,
    )

    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate output files. Without this flag, only preview is shown.",
    )

    parser.add_argument("--users", action="store_true", help="Export users table")

    parser.add_argument(
        "--purchases", action="store_true", help="Export purchases table"
    )

    parser.add_argument(
        "--discount-codes", action="store_true", help="Export discount codes table"
    )

    parser.add_argument(
        "--platform-accounts",
        action="store_true",
        help="Export platform accounts table",
    )

    parser.add_argument(
        "--platform-groups",
        action="store_true",
        help="Export platform groups table",
    )

    parser.add_argument(
        "--account-stats",
        action="store_true",
        help="Export account stats table",
    )

    parser.add_argument(
        "--payout-requests",
        action="store_true",
        help="Export payout requests table",
    )

    parser.add_argument(
        "--breach-account-activities",
        action="store_true",
        help="Export breach account activities table",
    )

    parser.add_argument(
        "--platform-events",
        action="store_true",
        help="Export platform events table",
    )

    parser.add_argument(
        "--periodic-trading-export",
        action="store_true",
        help="Export periodic trading export table",
    )

    parser.add_argument(
        "--equity-data-daily",
        action="store_true",
        help="Export equity data daily table",
    )

    parser.add_argument(
        "--advanced-challenge-settings",
        action="store_true",
        help="Export advanced challenge settings table",
    )

    parser.add_argument("--all", action="store_true", help="Export all tables")

    args = parser.parse_args()

    # If no specific table is selected, show help
    if not (
        args.users
        or args.purchases
        or args.discount_codes
        or args.platform_accounts
        or args.platform_groups
        or args.account_stats
        or args.payout_requests
        or args.breach_account_activities
        or args.platform_events
        or args.periodic_trading_export
        or args.equity_data_daily
        or args.advanced_challenge_settings
        or args.all
    ):
        parser.print_help()
        sys.exit(1)

    # Track which exports were requested
    exports_to_run = []

    if args.all:
        # Generate CSVs in dependency order to ensure foreign key integrity
        exports_to_run = [
            # Phase 1: Independent tables (no dependencies)
            "users",
            "discount_codes",
            "platform_groups",
            # Phase 2: First-level dependencies
            "purchases",  # depends on: users, discount_codes
            # Phase 3: Second-level dependencies
            "platform_accounts",  # depends on: users, purchases, platform_groups
            # Phase 4: Third-level dependencies (all require platform_accounts output)
            "account_stats",  # also needs platform_groups mapping
            "platform_events",
            "breach_account_activities",
            "payout_requests",  # also needs users export
            "equity_data_daily",
            "periodic_trading_export",
            # Phase 5: Remaining exports
            "advanced_challenge_settings",  # depends on: platform_groups, platform_accounts
        ]
    else:
        if args.users:
            exports_to_run.append("users")
        if args.discount_codes:
            exports_to_run.append("discount_codes")
        if args.platform_groups:
            exports_to_run.append("platform_groups")
        if args.purchases:
            exports_to_run.append("purchases")
        if args.platform_accounts:
            exports_to_run.append("platform_accounts")
        if args.account_stats:
            exports_to_run.append("account_stats")
        if args.payout_requests:
            exports_to_run.append("payout_requests")
        if args.breach_account_activities:
            exports_to_run.append("breach_account_activities")
        if args.platform_events:
            exports_to_run.append("platform_events")
        if args.periodic_trading_export:
            exports_to_run.append("periodic_trading_export")
        if args.equity_data_daily:
            exports_to_run.append("equity_data_daily")
        if args.advanced_challenge_settings:
            exports_to_run.append("advanced_challenge_settings")

    # Run exports in dependency order
    for export_type in exports_to_run:
        print(f"\n{'='*70}")
        print(f"{'EXPORTING ' + export_type.upper():^70}")
        print(f"{'='*70}\n")

        try:
            if export_type == "users":
                from scripts.users_export import export_users

                export_users(generate=args.generate)
            elif export_type == "purchases":
                from scripts.purchases_export import export_purchases

                export_purchases(generate=args.generate)
            elif export_type == "discount_codes":
                from scripts.discount_codes_export import export_discounts

                export_discounts(generate=args.generate)
            elif export_type == "platform_groups":
                from scripts.platform_groups_export import export_platform_groups

                export_platform_groups(generate=args.generate)
            elif export_type == "platform_accounts":
                from scripts.platform_accounts_export import export_platform_accounts

                export_platform_accounts(generate=args.generate)
            elif export_type == "account_stats":
                from scripts.account_stats_export import export_account_stats

                export_account_stats(generate=args.generate)
            elif export_type == "payout_requests":
                from scripts.payout_requests_export import export_payout_requests

                export_payout_requests(generate=args.generate)
            elif export_type == "breach_account_activities":
                from scripts.breach_account_activities_export import (
                    export_breach_account_activities,
                )

                export_breach_account_activities(generate=args.generate)
            elif export_type == "platform_events":
                from scripts.platform_events_export import export_platform_events

                export_platform_events(generate=args.generate)
            elif export_type == "periodic_trading_export":
                from scripts.periodic_trading_export import (
                    export_periodic_trading_export,
                )

                export_periodic_trading_export(generate=args.generate)
            elif export_type == "equity_data_daily":
                from scripts.equity_data_daily_export import export_equity_data_daily

                export_equity_data_daily(generate=args.generate)
            elif export_type == "advanced_challenge_settings":
                from scripts.advanced_challenge_settings_export import (
                    export_advanced_challenge_settings,
                )

                export_advanced_challenge_settings(generate=args.generate)
        except Exception as e:
            print(f"\n❌ Error during {export_type} export: {e}")
            import traceback

            traceback.print_exc()
            continue

    print(f"\n{'='*70}")
    print(f"{'MIGRATION COMPLETE':^70}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
