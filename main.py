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

  # Generate all exports
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

    parser.add_argument("--all", action="store_true", help="Export all tables")

    args = parser.parse_args()

    # If no specific table is selected, show help
    if not (
        args.users
        or args.purchases
        or args.discount_codes
        or args.platform_accounts
        or args.platform_groups
        or args.all
    ):
        parser.print_help()
        sys.exit(1)

    # Track which exports were requested
    exports_to_run = []

    if args.all:
        exports_to_run = [
            "users",
            "discount_codes",
            "platform_groups",
            "purchases",
            "platform_accounts",
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

    # Run exports
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
