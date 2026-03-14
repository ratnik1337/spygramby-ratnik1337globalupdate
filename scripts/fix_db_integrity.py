#!/usr/bin/env python3
"""Safe integrity fixer (dry-run by default)."""

import argparse
import os
import sqlite3
from typing import Dict, List

try:
    from business_bot_config import DB_PATH as DEFAULT_DB_PATH, MEDIA_PATH as DEFAULT_MEDIA_PATH
except Exception:
    DEFAULT_DB_PATH = "business_messages.db"
    DEFAULT_MEDIA_PATH = "./business_media"


def find_orphans(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("""
    SELECT COUNT(*)
    FROM edit_history eh
    WHERE NOT EXISTS (
        SELECT 1 FROM messages m
        WHERE m.message_id = eh.message_id
          AND m.chat_id = eh.chat_id
          AND ((m.owner_id = eh.owner_id) OR (m.owner_id IS NULL AND eh.owner_id IS NULL))
    )
    """)
    return int((cur.fetchone() or [0])[0] or 0)


def list_missing_media(conn: sqlite3.Connection, media_root: str) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT media_path FROM messages WHERE media_path IS NOT NULL AND TRIM(media_path) != ''")
    rows = [str(row[0]) for row in cur.fetchall() if row and row[0]]

    root_abs = os.path.abspath(media_root)
    missing: List[str] = []
    for raw_path in rows:
        abs_path = os.path.abspath(raw_path)
        try:
            inside = os.path.commonpath([root_abs, abs_path]) == root_abs
        except Exception:
            inside = False
        if not inside:
            continue
        if not os.path.isfile(abs_path):
            missing.append(raw_path)
    return missing


def rebuild_user_stats(cur: sqlite3.Cursor) -> None:
    cur.execute("DELETE FROM user_stats")
    cur.execute("""
    INSERT INTO user_stats (user_id, total_messages, total_deleted, total_edited, total_saved_media, last_activity)
    SELECT
        owner_id,
        COUNT(*),
        COALESCE(SUM(CASE WHEN is_deleted = 1 THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN is_edited = 1 THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN media_type IS NOT NULL THEN 1 ELSE 0 END), 0),
        MAX(date)
    FROM messages
    WHERE owner_id IS NOT NULL
    GROUP BY owner_id
    """)
    cur.execute("""
    INSERT OR IGNORE INTO user_stats (user_id, total_messages, total_deleted, total_edited, total_saved_media, last_activity)
    SELECT user_id, 0, 0, 0, 0, NULL FROM users
    """)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fix DB integrity issues safely")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--media-root", default=DEFAULT_MEDIA_PATH)
    parser.add_argument("--apply", action="store_true", help="Apply fixes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = os.path.abspath(args.db_path)
    media_root = os.path.abspath(args.media_root)

    if not os.path.exists(db_path):
        print(f"[ERROR] DB not found: {db_path}")
        return 2

    conn = sqlite3.connect(db_path)
    try:
        orphan_count = find_orphans(conn)
        missing_paths = list_missing_media(conn, media_root)

        print("=== INTEGRITY PREVIEW ===")
        print(f"- edit_history_orphans: {orphan_count}")
        print(f"- missing_media_paths_inside_root: {len(missing_paths)}")

        if not args.apply:
            print("\n[OK] Dry-run only. No changes were written.")
            return 0

        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("""
        DELETE FROM edit_history
        WHERE NOT EXISTS (
            SELECT 1 FROM messages m
            WHERE m.message_id = edit_history.message_id
              AND m.chat_id = edit_history.chat_id
              AND ((m.owner_id = edit_history.owner_id) OR (m.owner_id IS NULL AND edit_history.owner_id IS NULL))
        )
        """)
        deleted_orphans = int(cur.rowcount or 0)

        if missing_paths:
            placeholders = ",".join(["?"] * len(missing_paths))
            cur.execute(
                f"UPDATE messages SET media_path = NULL WHERE media_path IN ({placeholders})",
                tuple(missing_paths),
            )
            cleared_media_refs = int(cur.rowcount or 0)
        else:
            cleared_media_refs = 0

        rebuild_user_stats(cur)
        conn.commit()

        print("\n=== APPLIED ===")
        print(f"- deleted_orphan_edit_history: {deleted_orphans}")
        print(f"- cleared_missing_media_refs: {cleared_media_refs}")
        print("- user_stats: rebuilt")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] Integrity fix failed: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
