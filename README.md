# Users Table Migration: MySQL → PostgreSQL

This document compares the **old MySQL `users` table** with the **new PostgreSQL `users` table**.

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
