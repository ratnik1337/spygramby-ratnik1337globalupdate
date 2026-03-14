# Maintenance

## Environment Setup

Install dependencies before running maintenance checks:

```bash
python -m pip install -r requirements-dev.txt
```

## Key Maintenance Scripts

## Verify production readiness

Script:

- `scripts/verify_prod_readiness.py`

Purpose:

- compile checks (`py_compile`) for key project files
- hardcoded token/config checks
- DB integrity counters
- media path sanity checks (`outside media root`, missing files)

Run:

```bash
python scripts/verify_prod_readiness.py
```

Optional args:

```bash
python scripts/verify_prod_readiness.py --root . --db-path ./business_messages.db --media-root ./business_media
```

## Fix DB integrity (safe mode first)

Script:

- `scripts/fix_db_integrity.py`

Default mode:

- dry-run (no writes)

Run dry-run:

```bash
python scripts/fix_db_integrity.py
```

Apply fixes:

```bash
python scripts/fix_db_integrity.py --apply
```

Optional args:

```bash
python scripts/fix_db_integrity.py --db-path ./business_messages.db --media-root ./business_media --apply
```

## Legacy schema migration

Script:

- `migrate_db.py`

When to use:

- old DB schema detected
- controlled migration before/after rollout

Behavior:

- dry-run by default
- `--apply` writes changes
- backup created automatically in apply mode unless `--skip-backup`

Commands:

```bash
# Dry-run
python migrate_db.py

# Apply with backup
python migrate_db.py --apply

# Custom DB path
python migrate_db.py --db-path ./business_messages.db --apply
```

## Legacy access migration

Script:

- `migrate_legacy_access.py`

When to use:

- one-time migration of existing business users to legacy access plans

Safety:

- dry-run by default
- apply requires explicit token confirmation

Commands:

```bash
# Dry-run
python migrate_legacy_access.py

# Apply
python migrate_legacy_access.py --apply --confirm APPLY_LEGACY_MIGRATION
```

## DB Backup

## Windows PowerShell

```powershell
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
Copy-Item .\business_messages.db ".\\backups\\business_messages_$ts.db.bak"
```

## macOS/Linux Bash

```bash
ts=$(date +%Y%m%d_%H%M%S)
cp ./business_messages.db "./backups/business_messages_${ts}.db.bak"
```

## py_compile Sanity

Run:

```bash
python -m py_compile business_bot.py database.py migrate_db.py migrate_legacy_access.py business_bot_config.py scripts/verify_prod_readiness.py scripts/fix_db_integrity.py
```

## Safe Operational Notes

- Prefer dry-run first for migration/fix scripts.
- Back up DB before any apply mode.
- Keep only one polling instance for a token.
- Validate permissions after role/scope changes.
- Review archive/media cleanup actions before confirmation.
