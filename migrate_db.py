#!/usr/bin/env python3
"""Safe legacy DB migration helper.

Default mode is dry-run. Use --apply to write changes.
"""

import argparse
import os
import sqlite3
from datetime import datetime
from typing import Dict, List

try:
    from business_bot_config import DB_PATH as DEFAULT_DB_PATH
except Exception:
    DEFAULT_DB_PATH = "business_messages.db"


def table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None


def get_columns(cursor: sqlite3.Cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def backup_db(db_path: str) -> str:
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


def run_migration(conn: sqlite3.Connection, apply: bool) -> Dict[str, int]:
    cursor = conn.cursor()
    summary: Dict[str, int] = {
        "messages_owner_migrated": 0,
        "messages_reply_added": 0,
        "edit_history_owner_added": 0,
        "edit_history_owner_filled": 0,
        "edit_history_owner_unresolved": 0,
        "users_table_created": 0,
        "user_stats_table_created": 0,
    }

    messages_present = table_exists(cursor, "messages")
    if messages_present:
        msg_cols = get_columns(cursor, "messages")

        if "owner_id" not in msg_cols:
            summary["messages_owner_migrated"] = 1
            if apply:
                has_reply = "reply_to_message_id" in msg_cols
                reply_select = "reply_to_message_id" if has_reply else "NULL"
                cursor.execute("""
                CREATE TABLE messages_new (
                    message_id INTEGER,
                    chat_id INTEGER,
                    owner_id INTEGER,
                    user_id INTEGER,
                    username TEXT,
                    text TEXT,
                    media_type TEXT,
                    media_path TEXT,
                    date TIMESTAMP,
                    is_deleted INTEGER DEFAULT 0,
                    is_edited INTEGER DEFAULT 0,
                    original_text TEXT,
                    reply_to_message_id INTEGER DEFAULT NULL,
                    PRIMARY KEY (message_id, chat_id, owner_id)
                )
                """)
                cursor.execute(f"""
                INSERT INTO messages_new
                    (message_id, chat_id, owner_id, user_id, username, text, media_type, media_path,
                     date, is_deleted, is_edited, original_text, reply_to_message_id)
                SELECT
                    message_id,
                    chat_id,
                    user_id AS owner_id,
                    user_id,
                    username,
                    text,
                    media_type,
                    media_path,
                    date,
                    COALESCE(is_deleted, 0),
                    COALESCE(is_edited, 0),
                    original_text,
                    {reply_select}
                FROM messages
                """)
                cursor.execute("DROP TABLE messages")
                cursor.execute("ALTER TABLE messages_new RENAME TO messages")
                msg_cols = get_columns(cursor, "messages")

        if "reply_to_message_id" not in msg_cols:
            summary["messages_reply_added"] = 1
            if apply:
                cursor.execute("ALTER TABLE messages ADD COLUMN reply_to_message_id INTEGER DEFAULT NULL")

    if not table_exists(cursor, "users"):
        summary["users_table_created"] = 1
        if apply:
            cursor.execute("""
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
            """)

    if not table_exists(cursor, "user_stats"):
        summary["user_stats_table_created"] = 1
        if apply:
            cursor.execute("""
            CREATE TABLE user_stats (
                user_id INTEGER PRIMARY KEY,
                total_messages INTEGER DEFAULT 0,
                total_deleted INTEGER DEFAULT 0,
                total_edited INTEGER DEFAULT 0,
                total_saved_media INTEGER DEFAULT 0,
                last_activity TIMESTAMP
            )
            """)

    if table_exists(cursor, "edit_history"):
        edit_cols = get_columns(cursor, "edit_history")

        if "owner_id" not in edit_cols:
            summary["edit_history_owner_added"] = 1
            if apply:
                cursor.execute("ALTER TABLE edit_history ADD COLUMN owner_id INTEGER")

        # Try to recover owner_id from messages; keep NULL if unknown.
        if apply and messages_present:
            cursor.execute("""
            UPDATE edit_history
            SET owner_id = (
                SELECT m.owner_id
                FROM messages m
                WHERE m.message_id = edit_history.message_id
                  AND m.chat_id = edit_history.chat_id
                ORDER BY m.date DESC
                LIMIT 1
            )
            WHERE owner_id IS NULL
            """)
            summary["edit_history_owner_filled"] = int(cursor.rowcount or 0)

            cursor.execute("SELECT COUNT(*) FROM edit_history WHERE owner_id IS NULL")
            summary["edit_history_owner_unresolved"] = int((cursor.fetchone() or [0])[0] or 0)
        elif apply and not messages_present:
            cursor.execute("SELECT COUNT(*) FROM edit_history WHERE owner_id IS NULL")
            summary["edit_history_owner_unresolved"] = int((cursor.fetchone() or [0])[0] or 0)
        else:
            if messages_present:
                cursor.execute("""
                SELECT COUNT(*)
                FROM edit_history eh
                WHERE (
                    SELECT m.owner_id
                    FROM messages m
                    WHERE m.message_id = eh.message_id
                      AND m.chat_id = eh.chat_id
                    ORDER BY m.date DESC
                    LIMIT 1
                ) IS NULL
                """)
                summary["edit_history_owner_unresolved"] = int((cursor.fetchone() or [0])[0] or 0)
            else:
                cursor.execute("SELECT COUNT(*) FROM edit_history")
                summary["edit_history_owner_unresolved"] = int((cursor.fetchone() or [0])[0] or 0)

    if apply:
        conn.commit()

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe/Idempotent migration helper for legacy DB schema")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to SQLite DB")
    parser.add_argument("--apply", action="store_true", help="Apply migration changes")
    parser.add_argument("--skip-backup", action="store_true", help="Skip backup before apply")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = os.path.abspath(args.db_path)

    if not os.path.exists(db_path):
        print(f"[ERROR] DB file not found: {db_path}")
        return 2

    print(f"[INFO] DB: {db_path}")
    print(f"[INFO] Mode: {'APPLY' if args.apply else 'DRY-RUN'}")

    if args.apply and not args.skip_backup:
        backup_path = backup_db(db_path)
        print(f"[OK] Backup created: {backup_path}")

    conn = sqlite3.connect(db_path)
    try:
        summary = run_migration(conn, apply=args.apply)
    finally:
        conn.close()

    print("\n=== MIGRATION SUMMARY ===")
    for key in sorted(summary.keys()):
        print(f"- {key}: {summary[key]}")

    if summary.get("edit_history_owner_unresolved", 0) > 0:
        print("[WARN] Some edit_history rows still have owner_id=NULL (cannot be safely restored).")

    if not args.apply:
        print("\n[OK] Dry-run complete. No DB changes were written.")
    else:
        print("\n[OK] Migration applied successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
