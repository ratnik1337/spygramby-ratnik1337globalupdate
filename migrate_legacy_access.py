import argparse
import os
import sqlite3
import sys
from datetime import datetime
from typing import Any, Dict, List

from business_bot_config import DB_PATH
from database import MessageDB


CONFIRM_TOKEN = "APPLY_LEGACY_MIGRATION"


def format_preview(preview: Dict[str, Any], preview_limit: int = 20) -> str:
    lines: List[str] = []
    lines.append("=== LEGACY MIGRATION PREVIEW ===")
    lines.append(f"generated_at: {preview.get('generated_at')}")
    lines.append(f"dry_run: {preview.get('dry_run')}")
    lines.append(f"source_selector: {preview.get('source_selector')}")
    lines.append(f"plan_code: {preview.get('plan_code')}")
    lines.append(f"source: {preview.get('source')}")
    lines.append(f"duration_days: {preview.get('duration_days')}")
    lines.append(f"candidate_total: {preview.get('candidate_total')}")
    lines.append(f"already_active_total: {preview.get('already_active_total')}")
    lines.append(f"to_grant_total: {preview.get('to_grant_total')}")

    candidate_ids = preview.get("candidate_user_ids") or []
    if candidate_ids:
        shown = candidate_ids[:preview_limit]
        lines.append(f"candidate_user_ids (first {len(shown)}): {shown}")
        if len(candidate_ids) > preview_limit:
            lines.append(f"... +{len(candidate_ids) - preview_limit} more")

    already_active = preview.get("already_active") or []
    if already_active:
        lines.append(f"already_active (first {min(len(already_active), preview_limit)}):")
        for row in already_active[:preview_limit]:
            lines.append(
                f"  - user_id={row.get('user_id')} plan={row.get('plan_code')} "
                f"source={row.get('source')} expires_at={row.get('expires_at')}"
            )
        if len(already_active) > preview_limit:
            lines.append(f"  ... +{len(already_active) - preview_limit} more")

    to_grant = preview.get("to_grant") or []
    if to_grant:
        lines.append(f"to_grant (first {min(len(to_grant), preview_limit)}):")
        for row in to_grant[:preview_limit]:
            lines.append(f"  - user_id={row.get('user_id')}")
        if len(to_grant) > preview_limit:
            lines.append(f"  ... +{len(to_grant) - preview_limit} more")

    if not preview.get("dry_run"):
        granted = preview.get("granted") or []
        lines.append(f"granted_total: {preview.get('granted_total', len(granted))}")
        if granted:
            lines.append(f"granted (first {min(len(granted), preview_limit)}):")
            for row in granted[:preview_limit]:
                lines.append(
                    f"  - user_id={row.get('user_id')} starts_at={row.get('starts_at')} "
                    f"expires_at={row.get('expires_at')}"
                )
            if len(granted) > preview_limit:
                lines.append(f"  ... +{len(granted) - preview_limit} more")
    return "\n".join(lines)


def backup_db_file(db_path: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.legacy_backup_{ts}"
    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(backup_path)
        try:
            src.backup(dst)
            dst.commit()
        finally:
            dst.close()
    finally:
        src.close()
    return backup_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Grandfather existing active business users into legacy access.\n"
            "Default mode is dry-run (no DB writes)."
        )
    )
    parser.add_argument("--db-path", default=DB_PATH, help="SQLite DB path (default from business_bot_config.DB_PATH)")
    parser.add_argument("--apply", action="store_true", help="Apply migration writes (without this flag only dry-run).")
    parser.add_argument(
        "--confirm",
        default="",
        help=f"Safety token required with --apply. Expected: {CONFIRM_TOKEN}",
    )
    parser.add_argument("--actor-user-id", type=int, default=None, help="Actor id for audit log.")
    parser.add_argument("--duration-days", type=int, default=3650, help="Legacy duration in days (default: 3650).")
    parser.add_argument("--plan-code", default="legacy_grandfathered", help="Legacy plan_code.")
    parser.add_argument("--source", default="legacy_migration", help="Legacy source field.")
    parser.add_argument(
        "--comment",
        default="Grandfathered existing production user during migration",
        help="Grant comment for subscription_grants.",
    )
    parser.add_argument("--preview-limit", type=int, default=20, help="How many rows to print in preview sections.")
    parser.add_argument(
        "--target-user-id",
        action="append",
        type=int,
        default=None,
        help="Optional specific user_id for targeted testing (can be provided multiple times).",
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="Skip automatic DB backup before --apply (not recommended).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = os.path.abspath(args.db_path)

    if not os.path.exists(db_path):
        print(f"[ERROR] DB file not found: {db_path}")
        return 2

    if args.apply and args.confirm != CONFIRM_TOKEN:
        print(
            "[ERROR] Refusing to apply without valid --confirm token.\n"
            f"Use: --apply --confirm {CONFIRM_TOKEN}"
        )
        return 2

    db = MessageDB(db_path)

    preview = db.migrate_existing_business_users_to_legacy_access(
        dry_run=True,
        duration_days=args.duration_days,
        plan_code=args.plan_code,
        source=args.source,
        grant_comment=args.comment,
        actor_user_id=args.actor_user_id,
        target_user_ids=args.target_user_id,
    )
    print(format_preview(preview, preview_limit=max(1, int(args.preview_limit))))

    if not args.apply:
        print("\n[OK] Dry-run complete. No changes were written.")
        return 0

    backup_path = None
    if not args.skip_backup:
        backup_path = backup_db_file(db_path)
        print(f"\n[OK] Backup created: {backup_path}")

    result = db.migrate_existing_business_users_to_legacy_access(
        dry_run=False,
        duration_days=args.duration_days,
        plan_code=args.plan_code,
        source=args.source,
        grant_comment=args.comment,
        actor_user_id=args.actor_user_id,
        target_user_ids=args.target_user_id,
    )
    print("\n" + format_preview(result, preview_limit=max(1, int(args.preview_limit))))
    print("\n[OK] Legacy migration applied.")
    if backup_path:
        print(f"[INFO] Rollback source DB backup: {backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
