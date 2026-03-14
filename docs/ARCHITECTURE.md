# Architecture

## Overview

This project is a polling-based Telegram bot for Business message archiving and paid access management. It is a single-process Python app with SQLite storage and filesystem media/archive storage.

Core runtime flow:

1. `business_bot.py` starts the PTB application.
2. Runtime config is validated (`BOT_TOKEN` from env).
3. Incoming updates are routed to command/callback/business handlers.
4. Data operations are executed via `MessageDB` in `database.py`.
5. Media files are saved under `MEDIA_PATH`; exports/archives under `ARCHIVE_PATH`.

## Main Entrypoint

- File: `business_bot.py`
- Entrypoint: `main()`
- Start guard: `if __name__ == "__main__": raise SystemExit(main())`

Startup includes:

- `.env` load (`load_runtime_env_file`) with fail-fast token validation.
- PTB request hardening (`build_telegram_requests`).
- Polling/network log noise filter (`configure_telegram_network_logging`).
- Handler registration and `run_polling(...)`.

## Main Components

## `business_bot.py`

- Telegram handlers:
  - Public commands/callbacks (`/start`, `/help`, cabinet/plans/subscription/payment UX).
  - Admin command/callback panel (`/admin`, `admin_callback`).
  - Business update handlers (connection, message, edit, delete).
- RBAC + scopes:
  - Role resolution (`get_admin_role`, `is_admin`, `is_superadmin`).
  - Scope checks (`can_view_owner`, `can_view_chat`).
  - Action-level access (`can_use_admin_action`, `guard_admin_output_access`).
- Payment flow:
  - `pre_checkout_handler`
  - `successful_payment_handler`
  - Delegates idempotent atomic processing to DB methods.
- Media and archive/export logic:
  - Safe media path checks.
  - Export helpers (JSON/CSV/TXT/HTML/Telegram HTML).
  - Archive creation and scope-filtered media collection.

## `database.py`

- Class: `MessageDB`
- Responsibilities:
  - Schema creation and lightweight auto-migration (`create_tables`).
  - Message, edit-history, user stats, chat/user read models.
  - RBAC storage (`admin_roles`, `admin_scopes`, team RBAC v2 tables).
  - Billing/subscription/referral/promo/gift persistence.
  - Diagnostics snapshot and audit log.
  - Safe cleanup methods:
    - `cleanup_old_messages` (transactional, cleans linked edit history, rebuilds `user_stats`)
    - `delete_chat_messages` (owner-scoped delete + safe shared media unlinking)
  - Hard delete:
    - preview + apply flow
    - owner-scoped message/media targeting
    - shared file skip
    - admin audit anonymization.

## `migrate_db.py`

- Safe legacy DB migration helper.
- Dry-run by default.
- `--apply` mode writes changes; backup is created unless `--skip-backup`.
- Idempotent table/column checks before changes.

## `migrate_legacy_access.py`

- Grants legacy access to existing business users via `MessageDB`.
- Dry-run by default.
- `--apply` requires explicit `--confirm APPLY_LEGACY_MIGRATION`.
- Optional automatic DB backup before apply.

## `scripts/verify_prod_readiness.py`

- Lightweight production-readiness checks:
  - `py_compile` checks for key files.
  - hardcoded token scan.
  - DB integrity counters.
  - media path sanity (`outside MEDIA_PATH`, missing files).

## `scripts/fix_db_integrity.py`

- Integrity fixer for selected DB issues.
- Dry-run by default.
- `--apply` removes orphans, clears broken media refs, rebuilds `user_stats`.

## Data Storage

- SQLite DB path: `business_bot_config.DB_PATH` (default `business_messages.db`).
- Media root: `business_bot_config.MEDIA_PATH` (default `./business_media`).
- Archive/export root: `business_bot_config.ARCHIVE_PATH` (default `./archives`).

## Admin Logic Location

- Main admin UI + callback branching: `business_bot.py` (`admin_command`, `admin_callback`, helper guards).
- Team RBAC + scopes persistence: `database.py` (`team_*_v2`, `admin_roles`, `admin_scopes`).

## Payment Logic Location

- Runtime handlers: `business_bot.py` (`pre_checkout_handler`, `successful_payment_handler`).
- Atomic/idempotent DB operations:
  - `process_star_payment_success`
  - `process_gift_payment_success`
  - referral bonus processing.

## Media Logic Location

- Save/download naming and write: `business_bot.py` business message handler.
- Safe file checks: `is_safe_media_path`, `is_safe_archive_path`.
- Delete behavior:
  - chat cleanup in DB method `delete_chat_messages`.
  - hard-delete file cleanup in `hard_delete_user`.

## Cleanup / Integrity / Readiness

- Runtime cleanup action: `cleanup_old_messages` from admin panel.
- Integrity verification: `scripts/verify_prod_readiness.py`.
- Integrity fixes: `scripts/fix_db_integrity.py`.
- Legacy schema migration: `migrate_db.py`.
- Legacy access migration: `migrate_legacy_access.py`.

