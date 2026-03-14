# Business Bot

## License

This project is licensed under the RATNIK1337 ATTRIBUTION LICENSE v1.0.

Any use, modification, redistribution, or public deployment of this project or derivative works must include attribution to the original author:

**Original author: @ratnik1337**

Telegram Business archive bot with admin panel, RBAC, scoped data access, payments (Telegram Stars), referrals, promo codes, gifts, media storage, and export/archive tooling.

## What This Project Does

- Stores incoming Business messages (text + media) in SQLite.
- Tracks edits/deletes and keeps searchable history.
- Provides admin panel in Telegram (`/admin`) with role/scopes checks.
- Supports subscriptions, trials, referrals, promo codes, gifts, blacklist, anti-spam.
- Supports exports (JSON/CSV/TXT/HTML/Telegram HTML) and media archive bundles.
- Includes maintenance scripts for DB migration, integrity fix, and readiness checks.

## Stack

- Python 3
- `python-telegram-bot==22.5`
- SQLite (single DB file)
- Local filesystem storage for media and archives

## Entry Point

- Main app entry: `business_bot.py` (`main()` + `if __name__ == "__main__": raise SystemExit(main())`)

## Project Structure

```text
business_bot.py                 # Runtime app, handlers, admin panel, payments, RBAC checks
database.py                     # MessageDB, schema, business logic, cleanup/hard-delete, integrity-safe ops
business_bot_config.py          # Non-secret static config (paths/admin IDs/feature toggles)
migrate_db.py                   # Safe/idempotent legacy schema migration helper (dry-run by default)
migrate_legacy_access.py        # Legacy access grant migration helper (dry-run by default)
scripts/verify_prod_readiness.py# Compile/secret/DB/media-path readiness checks
scripts/fix_db_integrity.py     # Integrity fixer (dry-run by default)
business_messages.db            # SQLite DB (runtime)
business_media/                 # Downloaded media files (runtime)
archives/                       # Export/archive artifacts (runtime)
backups/                        # Backups created by maintenance scripts
```

## Requirements

- Python 3.10+ recommended
- Telegram bot token from BotFather
- Optional for archive packing:
  - WinRAR/7-Zip executable configured via `RAR_PATH` / `USE_7ZIP` in `business_bot_config.py`

## Install

```bash
python -m pip install -r requirements.txt
```

For local development/check scripts (same dependency set right now):

```bash
python -m pip install -r requirements-dev.txt
```

## Configure `.env`

Only one runtime env var is required by code:

- `BOT_TOKEN` (required)

Create `.env` in project root (or current working directory) from `.env.example`:

```env
BOT_TOKEN=1234567890:YOUR_REAL_BOT_TOKEN
```

Important:

- Bot loads `.env` from current directory first, then from project directory.
- Existing OS environment variables take precedence over `.env`.
- If `BOT_TOKEN` is missing, app exits fail-fast with config error.

## Run Locally

```bash
python business_bot.py
```

## Main Maintenance Commands

```bash
# Compile sanity for key files
python -m py_compile business_bot.py database.py migrate_db.py migrate_legacy_access.py business_bot_config.py

# Readiness checks (compile, secret scan, DB/media integrity checks)
python scripts/verify_prod_readiness.py

# Integrity fixer (preview only)
python scripts/fix_db_integrity.py

# Integrity fixer apply mode
python scripts/fix_db_integrity.py --apply

# Legacy schema migration helper (dry-run default)
python migrate_db.py

# Apply legacy schema migration with backup
python migrate_db.py --apply
```

## Where Data Is Stored

- DB: `business_messages.db` (path from `business_bot_config.DB_PATH`)
- Media files: `business_media/` (path from `business_bot_config.MEDIA_PATH`)
- Export/archive artifacts: `archives/` (path from `business_bot_config.ARCHIVE_PATH`)

## Admin Panel (Short)

- Open with `/admin`.
- Roles include `superadmin`, `admin`, `admin_lite`, and team RBAC v2 roles (`manager`, `support`, `analyst`, `viewer`, `custom`).
- Scoped roles are restricted by owner/chat scopes and per-action permissions.
- Sensitive output paths (exports, downloads, full text, metadata, archive actions) are guarded by access checks before data/file delivery.

## Security Warning

- Do not commit real tokens or secrets.
- Keep `.env` private and rotate `BOT_TOKEN` if leaked.
- Run only one polling instance per bot token to avoid Telegram `Conflict`.

## Additional Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Admin Panel](docs/ADMIN_PANEL.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Environment Variables](docs/ENVIRONMENT.md)
- [Security](docs/SECURITY.md)
- [Maintenance](docs/MAINTENANCE.md)
