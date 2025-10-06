# User Migration: MySQL → PostgreSQL

This document compares the **old MySQL tables** with the **new PostgreSQL tables** and provides migration scripts.

## 📜 Migration Scripts

### Main Entry Point (`main.py`)

The unified entry point for all migration tasks. Provides a clean CLI interface to run individual or all exports.

**Usage:**

```bash
# Show help
uv run main.py --help

# Preview users export
uv run main.py --users

# Generate users export
uv run main.py --generate --users

# Preview purchases export
uv run main.py --purchases

# Generate purchases export
uv run main.py --generate --purchases

# Preview discounts export
uv run main.py --discounts

# Generate discounts export
uv run main.py --generate --discounts

# Generate all exports at once
uv run main.py --generate --all
```

### 1. Users Export (`users_export.py`)

Exports users from `users.csv` to `new_users.csv` with the following transformations:

- Generates new UUID for each user
- Maps old `ref_by_user_id` (INT) to new `ref_by_user_uuid` (UUID)
- Renames columns (username → email, firstname → first_name, etc.)
- Adds new columns (role_id, status, updated_at, deleted_at, etc.)
- Uses `users_column_config.json` to select which columns to export

**Configuration:** Edit `users_column_config.json` to control which columns are exported.

### 2. Purchases Export (`purchases_export.py`)

Exports purchases from `purchases.csv` to `new_purchases.csv` with the following transformations:

- Generates new UUID for each purchase (replaces `id` as primary key)
- Maps old `user_id` (INT) to new `user_uuid` (UUID) using `new_users.csv` mapping
- Maps old `discount_id` (INT) to new `discount_uuid` (UUID) using `discount_codes.csv` and `new_discount_codes.csv` alignment
- Adds new PostgreSQL-only columns:
  - `updated_at` (from `created_at`)
  - `webhook_response` (NULL - for future webhook data)
  - `purchase_type` (NULL - categorizes purchase)
  - `competition_uuid` (NULL - links to competitions)
  - `ip` (NULL - stores user IP address)
- Removes MySQL-only columns: `id`, `user_id`, `discount_id`, `discount`, `discount_code`, `deleted_at`
- Exports only PostgreSQL schema columns (20 total)
- Uses `purchases_column_config.json` to define the exact PostgreSQL schema

**Configuration:** Edit `purchases_column_config.json` to control which columns are exported.

**Statistics:**

- Total purchases: 89,135
- Total columns exported: 20 (PostgreSQL schema only)
- File size: 121 MB
- Valid user mappings: 87,903 (98.6%)
- Invalid/missing user mappings: 1,232 (1.4%)
- Valid discount mappings: 45,751 (51.3%)
- Invalid/missing discount mappings: 43,384 (48.7%)
- Removed columns: `id`, `user_id`, `discount_id`, `discount`, `discount_code`, `deleted_at`
- New columns: `webhook_response`, `purchase_type`, `competition_uuid`, `ip`

### 3. Discounts Export (`discounts_export.py`)

Exports discount codes from `discount_codes.csv` to `new_discount_codes.csv` with the following transformations:

- Generates new UUID for each discount code (replaces `id` as primary key)
- Converts `status` to lowercase (`ACTIVE` → 'active', `INACTIVE` → 'inactive')
- Maps MySQL column names to PostgreSQL names:
  - `max_usages_count` → `max_usage_count` (singular form)
  - `current_usages_count` → `current_usage_count` (singular form)
  - `discount_code_end_date` → `end_date` (simplified name)
  - `created_date` → `created_at`
  - `last_modified_date` → `updated_at`
- Adds new PostgreSQL-only columns:
  - `name` (from `code`)
  - `start_date` (from `created_at`)
  - `commission_percentage` (from `discount`)
  - `type` (default: `'admin'`)
  - `created_by` (UUID: `f965141e-43f0-4992-a742-7899edbe1ca5`)
  - `deleted_at` (NULL for soft deletes)
- Removes MySQL-only columns: `id`, `group_name`, `account_balance`
- Exports only PostgreSQL schema columns (18 total)
- Uses `discounts_column_config.json` to define the exact PostgreSQL schema

**Configuration:** Edit `discounts_column_config.json` to control which columns are exported.

**Statistics:**

- Total discount codes: 53,900
- Total columns exported: 18 (PostgreSQL schema only)
- Status conversion: 53,777 'active' and 117 'inactive'
- File size: 13 MB
- All records have `type='admin'` by default
- All records have `created_by='f965141e-43f0-4992-a742-7899edbe1ca5'`
- `commission_percentage` mirrors `discount` value
- Removed columns: `id`, `max_usages_count`, `current_usages_count`, `discount_code_end_date`, `group_name`, `account_balance`

---

## ⚙️ Configuration Files

### `users_column_config.json`

Controls which columns are exported in `new_users.csv`. Contains a JSON array of column names to include in the export.

### `purchases_column_config.json`

Controls which columns are exported in `new_purchases.csv`. Contains a JSON array of the 20 PostgreSQL schema columns in the exact order matching the database schema.

### `discounts_column_config.json`

Controls which columns are exported in `new_discount_codes.csv`. Contains a JSON array of the 18 PostgreSQL schema columns in the exact order matching the database schema.

---

## 📁 Project Structure

```
user_migration/
├── main.py                          # Entry point - dispatches to export modules
├── users_export.py                  # Users export logic
├── purchases_export.py              # Purchases export logic
├── discounts_export.py              # Discounts export logic
├── users_column_config.json         # Users column configuration
├── purchases_column_config.json     # Purchases column configuration
├── discounts_column_config.json     # Discounts column configuration
├── users.csv                        # Source: Original users data (READ-ONLY)
├── purchases.csv                    # Source: Original purchases data
├── discount_codes.csv               # Source: Original discount codes data
├── new_users.csv                    # Output: Transformed users
├── new_purchases.csv                # Output: Transformed purchases
└── new_discount_codes.csv                # Output: Transformed discount codes
```

---

# 📊 Users Table Migration

---

## 🔑 Primary Key

- **Postgres:** `uuid` (UUID, default `gen_random_uuid()`)
- **MySQL:** `id` (INT AUTO_INCREMENT)

➡️ Changed from auto-increment INT → UUID.

---

## 🧩 References

- **Postgres:** `ref_by_user_id` (UUID)
- **MySQL:** `ref_by_user_id` (INT, FK → users.id)

---

## 📊 Counters

- **Postgres:** `ref_link_count`, `used_free_count`, `available_count`
- **MySQL:** `ref_link_clicks`, `available_challange`, `used_free_account`, `total_free_account`, `softbraech_count`

---

## 👤 Identity & Auth

- **Postgres:** `email`, `password`, `first_name`, `last_name`, `google_app_secret`, `is_google_app_verify`, `2fa_sms_enabled`, `dob`, `role_id`
- **MySQL:** `username`, `password`, `firstname`, `lastname`, `2fa_app_secret`, `2fa_sms_enabled`, `role`

➡️ MySQL used `username`; Postgres switched to `email`.  
➡️ MySQL used numeric `role`; Postgres uses UUID `role_id`.

---

## 📱 Contact & Location

- **Postgres:** `phone`, `phone_verified`, `address`, `country`, `state`, `city`, `zip`, `timezone`
- **MySQL:** Similar but with different types (`phone_verified` = tinyint, `address` = TEXT).

---

## 📩 Activation / Status

- **Postgres:** `status`, `sent_activation_mail_count`, `reset_pass_hash`
- **MySQL:** `active`, `sent_activation_mail_count`, `reset_pass_hash`

---

## 🕒 Timestamps

- **Postgres:** `last_login_at`, `created_at`, `updated_at`, `deleted_at`
- **MySQL:** `last_login`, `created_at`

➡️ Postgres adds `updated_at` + `deleted_at` for soft deletes.

---

## 🔐 2FA & Security

- **Postgres:** `google_app_secret`, `is_google_app_verify`, `2fa_sms_enabled`
- **MySQL:** `2fa_app_secret`, `2fa_sms_enabled`

---

## 🪪 KYC / Identity

- **Postgres:** `identity_status`, `identity_verified_at`, `trail_verification_status`
- **MySQL:** `identity_status`, `identity_verified_at`, `free_trail_verification`, `kyc_approved_by`, `free_trial_approve`

---

## 🪙 Affiliate

- **Postgres:** `affiliate_terms`, `accept_affiliate_terms`, `ref_code`
- **MySQL:** `accept_affiliate_terms`

---

## 🎮 Discord

- **Postgres:** `discord_connected`
- **MySQL:** `discord_connected`, `discord_email`

---

## 🗑️ Other Flags

- **Postgres only:** `dashboard_popup`, `trail_verification_status`, `is_google_app_verify`
- **MySQL only:** `isold_user`, `available_challange`, `softbraech_count`, `note`, `temp_cpde`

---

# ✅ Summary of Key Differences

1. Primary Key: INT → UUID
2. Identifier: username → email
3. Roles: role (int) → role_id (uuid)
4. Counters: renamed/simplified
5. Timestamps: Postgres adds `updated_at` + `deleted_at`
6. 2FA: expanded in Postgres
7. Affiliate: expanded in Postgres
8. Discord: email removed
9. KYC: simplified in Postgres

---

# 📋 Migration Mapping (Old → New)

| MySQL Column            | PostgreSQL Column         | Notes                      |
| ----------------------- | ------------------------- | -------------------------- |
| id (PK)                 | uuid (PK)                 | Changed from INT → UUID    |
| username                | email                     | Identifier changed         |
| firstname               | first_name                | Renamed                    |
| lastname                | last_name                 | Renamed                    |
| role (SMALLINT)         | role_id (UUID)            | Changed from int code → FK |
| active                  | status                    | Renamed                    |
| ref_link_clicks         | ref_link_count            | Renamed                    |
| last_login              | last_login_at             | Renamed                    |
| created_at              | created_at                | Same                       |
| —                       | updated_at                | New                        |
| —                       | deleted_at                | New                        |
| 2fa_app_secret          | google_app_secret         | Renamed                    |
| —                       | is_google_app_verify      | New                        |
| free_trail_verification | trail_verification_status | Renamed                    |
| available_challange     | available_count           | Renamed                    |
| used_free_account       | used_free_count           | Renamed                    |
| total_free_account      | —                         | Removed                    |
| softbraech_count        | —                         | Removed                    |
| discord_email           | —                         | Removed                    |
| note                    | —                         | Removed                    |
| temp_cpde               | —                         | Removed                    |

---
