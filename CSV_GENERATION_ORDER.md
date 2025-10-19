# CSV Generation Order

This document defines the correct order for generating CSV files to ensure all dependencies are met.

## Import Order (Database Import Sequence)

Based on the successful database import results, the correct order is:

| Order | Table                           | Records Imported | Status          | Dependencies                       |
| ----- | ------------------------------- | ---------------- | --------------- | ---------------------------------- |
| 1     | **Roles**                       | 5                | ✅ 100% Success | None                               |
| 2     | **Permissions**                 | 80               | ✅ 100% Success | None                               |
| 3     | **Users**                       | 90,839           | ✅ 100% Success | None                               |
| 4     | **Discount Codes**              | 53,892           | ✅ 100% Success | None                               |
| 5     | **Purchases**                   | 258,662          | ✅ 100% Success | Users, Discount Codes              |
| 6     | **Payout Requests**             | 3,031            | ✅ 100% Success | Users                              |
| 7     | **Platform Groups**             | 32               | ✅ 100% Success | None                               |
| 8     | **Platform Accounts**           | 67,930           | ✅ 100% Success | Users, Purchases, Platform Groups  |
| 9     | **Account Stats**               | 67,523           | ✅ 100% Success | Platform Accounts                  |
| 10    | **Breach Account Activities**   | 9,683            | ✅ 100% Success | Platform Accounts                  |
| 11    | **Platform Events**             | 149,925          | ✅ 100% Success | Platform Accounts                  |
| 12    | **Default Challenge Settings**  | 1                | ✅ 100% Success | None                               |
| 13    | **Advanced Challenge Settings** | 67,962           | ✅ 100% Success | Platform Groups, Platform Accounts |

## CSV Generation Order

For CSV generation, we need to follow the dependency chain:

### Phase 1: Independent Tables (No Dependencies)

1. **Users** - No dependencies
2. **Discount Codes** - No dependencies
3. **Platform Groups** - No dependencies
4. **Default Challenge Settings** - No dependencies (if exists)

### Phase 2: First-Level Dependencies

5. **Purchases** - Depends on: Users, Discount Codes
6. **Payout Requests** - Depends on: Users (if exists)

### Phase 3: Second-Level Dependencies

7. **Platform Accounts** - Depends on: Users, Purchases, Platform Groups
8. **Equity Data Daily** - Depends on: Platform Accounts (if exists)
9. **Periodic Trading Export** - Depends on: Platform Accounts (if exists)

### Phase 4: Third-Level Dependencies

10. **Account Stats** - Depends on: Platform Accounts (if exists)
11. **Breach Account Activities** - Depends on: Platform Accounts (if exists)
12. **Platform Events** - Depends on: Platform Accounts (if exists)
13. **Advanced Challenge Settings** - Depends on: Platform Groups, Platform Accounts

## Available Export Scripts

✅ **Implemented:**

- `users_export.py`
- `discount_codes_export.py`
- `purchases_export.py`
- `platform_groups_export.py`
- `platform_accounts_export.py`
- `equity_data_daily_export.py`
- `periodic_trading_export.py`
- `advanced_challenge_settings_export.py`
- `default_challenge_settings_export.py`

❌ **Not Implemented (Missing Scripts):**

- `roles_export.py`
- `permissions_export.py`
- `payout_requests_export.py`
- `account_stats_export.py`
- `breach_account_activities_export.py`
- `platform_events_export.py`

## Usage

### Generate All Available CSVs

```bash
uv run main.py --generate --all
```

### Generate Individual CSVs (in dependency order)

```bash
# Phase 1: Independent tables
uv run main.py --generate --users
uv run main.py --generate --discount-codes
uv run main.py --generate --platform-groups

# Phase 2: First-level dependencies
uv run main.py --generate --purchases

# Phase 3: Second-level dependencies
uv run main.py --generate --platform-accounts
uv run main.py --generate --equity-data-daily
uv run main.py --generate --periodic-trading-export

# Phase 4: Third-level dependencies
uv run main.py --generate --advanced-challenge-settings
```

## Important Notes

1. **Dependency Order**: Always generate dependencies before dependents
2. **Foreign Key Integrity**: Ensure all referenced UUIDs exist before generating dependent tables
3. **Error Handling**: If a dependency fails, dependent tables will also fail
4. **Validation**: After generation, verify foreign key relationships are intact

## File Locations

- **Source Files**: `csv/input/`
- **Generated Files**: `csv/output/`
- **Export Scripts**: `scripts/`
- **Configuration**: `config/`

---

_Last Updated: 2025-10-11_
_Based on successful database import results_
