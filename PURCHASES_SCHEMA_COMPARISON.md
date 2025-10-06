# Purchases Schema Comparison: MySQL vs PostgreSQL

This document compares the `purchases` table schema between the MySQL CSV export and the new PostgreSQL database schema.

---

## Column Mapping & Differences

| MySQL CSV Column         | PostgreSQL Column        | Status              | Notes                                                                                     |
| ------------------------ | ------------------------ | ------------------- | ----------------------------------------------------------------------------------------- |
| `id`                     | `uuid`                   | ⚠️ **CHANGED**      | Changed from INT to UUID, now PRIMARY KEY with `gen_random_uuid()` - **Mapped in export** |
| `user_id`                | `user_uuid`              | ⚠️ **CHANGED**      | Changed from INT to UUID - **Mapped in export using `new_users.csv`**                     |
| `amount_total`           | `amount_total`           | ✅ **SAME**         | Both exist, PostgreSQL has NOT NULL constraint                                            |
| `currency`               | `currency`               | ✅ **SAME**         | Both exist, PostgreSQL has NOT NULL + DEFAULT 'USD'                                       |
| `payment_method`         | `payment_method`         | ✅ **SAME**         | Both exist, nullable in both                                                              |
| `payment_status`         | `payment_status`         | ⚠️ **TYPE CHANGED** | CSV: varchar/int, PostgreSQL: int2 (smallint) NOT NULL DEFAULT 0                          |
| `is_paid_aff_commission` | `is_paid_aff_commission` | ⚠️ **TYPE CHANGED** | CSV: varchar/int, PostgreSQL: int2 (smallint)                                             |
| `user_data`              | `user_data`              | ⚠️ **TYPE CHANGED** | CSV: text/varchar, PostgreSQL: jsonb                                                      |
| `created_at`             | `created_at`             | ✅ **SAME**         | Both exist, PostgreSQL has NOT NULL + DEFAULT CURRENT_TIMESTAMP                           |
| `original_amount`        | `original_amount`        | ✅ **SAME**         | Both exist, type is numeric in PostgreSQL                                                 |
| `discount`               | -                        | ❌ **REMOVED**      | Does not exist in PostgreSQL - stored in discount_codes table                             |
| `discount_code`          | -                        | ❌ **REMOVED**      | Does not exist in PostgreSQL - stored in discount_codes table                             |
| `discount_id`            | `discount_uuid`          | ⚠️ **CHANGED**      | Changed from INT to UUID - **Mapped in export using `new_discount_codes.csv`**            |
| `already_paid`           | `already_paid`           | ⚠️ **TYPE CHANGED** | CSV: int/varchar, PostgreSQL: bool NOT NULL DEFAULT false                                 |
| `payment_transaction_id` | `payment_transaction_id` | ⚠️ **TYPE CHANGED** | CSV: varchar, PostgreSQL: text                                                            |
| `payment_response`       | `payment_response`       | ⚠️ **TYPE CHANGED** | CSV: text/varchar, PostgreSQL: text                                                       |
| `payment_attempt_count`  | `payment_attempt_count`  | ✅ **SAME**         | Both exist, PostgreSQL has DEFAULT 0                                                      |
| -                        | `updated_at`             | ➕ **NEW**          | New column in PostgreSQL - **Added in export (copied from `created_at`)**                 |
| -                        | `webhook_response`       | ➕ **NEW**          | New column in PostgreSQL (text, nullable) - for storing webhook data                      |
| -                        | `purchase_type`          | ➕ **NEW**          | New column in PostgreSQL (varchar, DEFAULT 'challenge') - categorizes purchase            |
| -                        | `competition_uuid`       | ➕ **NEW**          | New column in PostgreSQL (uuid, nullable) - links to competitions                         |
| -                        | `ip`                     | ➕ **NEW**          | New column in PostgreSQL (varchar, nullable) - stores user IP address                     |

---

## Summary Statistics

### Columns in MySQL CSV: 17

- `id`, `user_id`, `amount_total`, `currency`, `payment_method`, `payment_status`, `is_paid_aff_commission`, `user_data`, `created_at`, `original_amount`, `discount`, `discount_code`, `discount_id`, `already_paid`, `payment_transaction_id`, `payment_response`, `payment_attempt_count`

### Columns in PostgreSQL: 20

- `uuid`, `user_uuid`, `amount_total`, `currency`, `payment_method`, `payment_status`, `is_paid_aff_commission`, `user_data`, `original_amount`, `discount_uuid`, `already_paid`, `payment_transaction_id`, `payment_response`, `payment_attempt_count`, `created_at`, `updated_at`, `webhook_response`, `purchase_type`, `competition_uuid`, `ip`

### Changes Breakdown

- **New Columns**: 5 (`updated_at`, `webhook_response`, `purchase_type`, `competition_uuid`, `ip`)
- **Removed Columns**: 2 (`discount`, `discount_code`)
- **Changed Type**: 6 (`payment_status`, `is_paid_aff_commission`, `user_data`, `already_paid`, `payment_transaction_id`, `payment_response`)
- **Changed Primary Key**: 1 (`id` → `uuid`)
- **Changed Foreign Key**: 2 (`user_id` → `user_uuid`, `discount_id` → `discount_uuid`)

---

## Data Type Comparison

| Column                   | MySQL/CSV Type | PostgreSQL Type | Notes                                     |
| ------------------------ | -------------- | --------------- | ----------------------------------------- |
| Primary Key              | INT            | UUID            | Complete redesign                         |
| Foreign Keys (user)      | INT            | UUID            | Changed to UUID references                |
| Foreign Keys (discount)  | INT            | UUID            | Changed to UUID references                |
| `amount_total`           | decimal/float  | numeric         | Compatible                                |
| `original_amount`        | decimal/float  | numeric         | Compatible                                |
| `payment_status`         | varchar/int    | int2 (smallint) | Requires integer values                   |
| `is_paid_aff_commission` | varchar/int    | int2 (smallint) | Requires integer values                   |
| `user_data`              | text/varchar   | jsonb           | Requires JSON transformation              |
| `already_paid`           | int/varchar    | bool            | Requires boolean conversion               |
| `payment_response`       | text/varchar   | text            | Compatible                                |
| `payment_transaction_id` | varchar        | text            | Compatible                                |
| `timestamp` columns      | datetime       | timestamp       | PostgreSQL uses timezone-aware timestamps |

---

## Migration Considerations

### 1. UUID Generation

- All existing `id` values need to be mapped to newly generated UUIDs
- Foreign key relationships must be updated:
  - `user_id` → `user_uuid` (mapped from `new_users.csv`)
  - `discount_id` → `discount_uuid` (mapped from `new_discount_codes.csv`)

### 2. Removed Discount Columns

✅ **HANDLED** - The following columns are removed from purchases but available in discount_codes table:

- `discount` - Discount percentage/amount (stored in discount_codes)
- `discount_code` - Discount code string (stored in discount_codes)
- Keep `discount_uuid` to maintain the relationship

### 3. Missing Data Handling

New columns need default values or can be NULL:

- `updated_at`: ✅ **HANDLED** - Set to `created_at` value in export
- `webhook_response`: NULL (historical purchases won't have this)
- `purchase_type`: NULL (or could default to 'challenge' based on business logic)
- `competition_uuid`: NULL (historical purchases won't have this)
- `ip`: NULL (historical purchases won't have this)

### 4. Data Type Conversions

#### Boolean Conversion

- `already_paid`: Convert from int/varchar to boolean
  - `1`, `'1'`, `'true'` → `true`
  - `0`, `'0'`, `'false'`, `NULL` → `false`

#### Integer Conversion

- `payment_status`: Ensure values are valid smallint (0-32767)
- `is_paid_aff_commission`: Ensure values are valid smallint

#### JSON Conversion

- `user_data`: Convert text/varchar to valid JSONB format
  - Empty strings → `NULL` or `{}`
  - Invalid JSON → needs handling/cleaning

### 5. Validation Requirements

Before importing to PostgreSQL:

- ✅ All `user_uuid` values must exist in the users table
- ✅ All `discount_uuid` values must exist in the discount_codes table (if not NULL)
- Ensure `amount_total` is NOT NULL
- Ensure `currency` is NOT NULL (default to 'USD' if missing)
- Ensure `payment_status` is NOT NULL (default to 0 if missing)
- Ensure `created_at` is NOT NULL
- Ensure `updated_at` is NOT NULL

---

## Export Statistics

Based on the current export:

- **Total purchases**: 89,135
- **Export columns**: 22 (includes both old and new column names for compatibility)
- **User mappings**: 87,903 valid, 1,232 invalid/missing
- **Discount mappings**: 45,751 valid, 43,384 invalid/missing
- **File size**: ~123 MB

### Columns in Export CSV

The export includes transitional columns for migration:

1. `id` (old MySQL ID)
2. `uuid` (new PostgreSQL UUID)
3. `user_id` (old MySQL user ID)
4. `user_uuid` (new PostgreSQL user UUID)
5. `amount_total`
6. `currency`
7. `payment_method`
8. `payment_status`
9. `is_paid_aff_commission`
10. `user_data`
11. `created_at`
12. `original_amount`
13. `discount` (old, will be removed post-migration)
14. `discount_code` (old, will be removed post-migration)
15. `discount_id` (old MySQL discount ID)
16. `discount_uuid` (new PostgreSQL discount UUID)
17. `already_paid`
18. `payment_transaction_id`
19. `payment_response`
20. `payment_attempt_count`
21. `updated_at` (new)
22. `deleted_at` (new, for soft deletes)

---

## Recommendations

1. **Phase 1: Export with Dual Columns**

   - Keep both old (id, user_id, discount_id) and new (uuid, user_uuid, discount_uuid) columns
   - This allows validation and rollback if needed

2. **Phase 2: Validate Mappings**

   - Verify all user_uuid mappings are valid
   - Verify all discount_uuid mappings are valid
   - Check for NULL foreign keys and decide handling

3. **Phase 3: Clean Import to PostgreSQL**

   - Import only PostgreSQL columns
   - Add new columns (webhook_response, purchase_type, competition_uuid, ip) as NULL
   - Ensure all constraints are met

4. **Phase 4: Post-Migration**
   - Remove transitional columns (id, user_id, discount_id, discount, discount_code)
   - Add indices on foreign keys (user_uuid, discount_uuid)
   - Add triggers for updated_at timestamp
   - Verify data integrity with queries

---

## Notes

- The export script maintains both old and new column names for smooth migration
- Invalid user/discount mappings should be reviewed before final import
- Consider adding validation queries to check data quality
- PostgreSQL enforces NOT NULL constraints - ensure all required fields have values
