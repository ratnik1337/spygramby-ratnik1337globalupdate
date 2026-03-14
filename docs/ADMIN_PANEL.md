# Admin Panel

## Entry

- Telegram command: `/admin`
- Access gate: only users recognized by `is_admin(...)`

## Role Model

Role resolution is handled in `business_bot.py` (`get_admin_role`, `is_admin`).

## Roles

- `superadmin`
  - First ID from `ADMIN_IDS` in `business_bot_config.py`.
  - Full access, including sensitive operations (for example user hard delete).
- `admin`
  - Full access to admin actions, with role/scope management restrictions applied where relevant.
- `admin_lite`
  - Legacy limited admin role.
  - Read-oriented access and limited subscription operations.
  - Requires explicit owner/chat scopes in `admin_scopes`.
- Team RBAC v2 roles: `manager`, `support`, `analyst`, `viewer`, `custom`
  - Permission-based access via `team_member_roles_v2`, `team_member_permissions_v2`.
  - Scope-based data visibility via `team_scopes_v2`.

## Scope Model

Two scope styles are used:

- Owner scope: `owner_id` -> all chats for that owner.
- Chat scope: `owner_id + chat_id` -> one owner chat only.

Checks are enforced by:

- `can_view_owner(admin_user_id, owner_id)`
- `can_view_chat(admin_user_id, owner_id, chat_id)`

## Sensitive Actions

Sensitive admin output includes, among others:

- exports (`export_chat_*`)
- archive/download operations (`archive_*`, `download_*`)
- message metadata/full text (`metadata_*`, `fulltext_*`)
- user/chat/admin lists and diagnostics
- cleanup/delete operations

These are guarded through:

- `can_use_admin_action(...)` (action-level permission)
- `guard_admin_output_access(...)` (fail-closed gate before output)
- owner/chat scope checks extracted from callback payload (`extract_owner_chat_from_action`)

## Security Behavior For Callback Data

`callback_data` is not trusted:

- role check runs on every callback
- action permission check runs on every callback
- owner/chat scope checks run before data delivery
- blocked requests return "access denied" style responses

## Main Admin Sections

Depending on role and permissions:

- Stats/monitoring
- User/chat browsing
- Search (text/media/deleted/edited)
- Media menu and archive tools
- Subscriptions
- Referrals
- Promo codes
- Blacklist/anti-spam
- Roles/scopes management
- Diagnostics
- Settings/cleanup
- Hard delete (superadmin only)

## Exports, Fulltext, Metadata, Archive, Media Download

Behavior in current code:

- Only admins can enter admin callback flow.
- Sensitive outputs are re-checked near output/send points.
- Scoped roles see only owner/chat data they are allowed to view.
- File sends use safe-path checks for media/archive roots.
- Non-admin recipients are blocked for admin-only notifications/documents.

## Hard Delete In Admin Panel

- Start action available only to `superadmin`.
- Flow is preview -> explicit confirmation.
- Prevents deleting superadmin.
- DB hard-delete logic uses owner-scoped message/media targeting and audit anonymization.

