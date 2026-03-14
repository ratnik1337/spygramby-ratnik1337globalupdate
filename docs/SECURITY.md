# Security

## Token and Secret Handling

- `BOT_TOKEN` must come from environment (`.env`/OS env), not from source code.
- Never commit real tokens to git.
- `.env` must remain private and host-protected.
- Runtime logs should not print token value.

## Secret-in-Code Policy

- `business_bot_config.py` must not define `BOT_TOKEN`.
- Secret scans are part of `scripts/verify_prod_readiness.py`.

## Admin-Only Access Model

Admin access is enforced in runtime code:

- `/admin` denies non-admin users.
- Callback flow denies non-admin users before any sensitive operation.
- Action-level gate: `can_use_admin_action(...)`.
- Fail-closed output gate: `guard_admin_output_access(...)`.

## Scoped Role Restrictions

For scope-limited roles (`admin_lite` and team RBAC roles):

- data visibility is constrained by owner/chat scopes
- checks use `can_view_owner(...)` and `can_view_chat(...)`
- search/media/user/chat/archive output is filtered by scope

## Exports / Archive / Media / Fulltext Protections

Current protections include:

- callback-level permission/scope checks before output
- archive and media collection restricted by requester scope
- safe file-path checks:
  - media under `MEDIA_PATH` only
  - export/archive files under `ARCHIVE_PATH` only
- admin-only document sending helper validates recipient admin IDs

## Safe File Deletion Rules

Chat delete and hard-delete cleanup use safety checks:

- only paths inside configured media root are physically deleted
- shared media paths with remaining DB references are skipped
- unsafe paths are logged and skipped

## Hard Delete Isolation

Hard delete is designed to avoid cross-user data loss:

- message deletion is owner-scoped (`owner_id = target_user`)
- media collection is owner-scoped
- shared media files are skipped
- audit history rows are anonymized where needed instead of blind deletion
- action is exposed only to superadmin flow with preview + confirmation

## Pre-Production Security Checklist

1. Verify no tokens/secrets in repository files.
2. Confirm `BOT_TOKEN` is provided only via env.
3. Validate `ADMIN_IDS` and role assignments.
4. Run `scripts/verify_prod_readiness.py`.
5. Run `scripts/fix_db_integrity.py` in dry-run.
6. Confirm single polling instance only.
7. Review filesystem permissions for `.env`, DB, media, archives.

## BOT_TOKEN Rotation Procedure

1. Revoke old token in BotFather and issue a new one.
2. Update host secret (`.env` or OS env variable).
3. Restart bot process.
4. Confirm bot starts with new token.
5. Invalidate/remove old token copies from local notes or shared channels.

