#!/usr/bin/env python3
"""Lightweight production-readiness checks for the Telegram bot project."""

import argparse
import os
import py_compile
import re
import sqlite3
from typing import Dict, List, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB = os.path.join(ROOT, "business_messages.db")
FILES_TO_COMPILE = [
    "business_bot.py",
    "database.py",
    "migrate_db.py",
    "migrate_legacy_access.py",
    "business_bot_config.py",
]
TOKEN_RE = re.compile(r"\b\d{8,12}:[A-Za-z0-9_-]{30,}\b")


def compile_checks(root: str) -> List[str]:
    errors: List[str] = []
    for rel_path in FILES_TO_COMPILE:
        abs_path = os.path.join(root, rel_path)
        try:
            py_compile.compile(abs_path, doraise=True)
        except Exception as exc:
            errors.append(f"py_compile failed for {rel_path}: {exc}")
    return errors


def secret_checks(root: str) -> List[str]:
    issues: List[str] = []
    for rel_path in FILES_TO_COMPILE + [".env.example"]:
        abs_path = os.path.join(root, rel_path)
        if not os.path.exists(abs_path):
            continue
        try:
            with open(abs_path, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
        except Exception as exc:
            issues.append(f"unable to read {rel_path}: {exc}")
            continue

        if TOKEN_RE.search(content):
            issues.append(f"possible hardcoded Telegram token found in {rel_path}")

    config_path = os.path.join(root, "business_bot_config.py")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8", errors="ignore") as fh:
            config_content = fh.read()
        if re.search(r"(?m)^\s*BOT_TOKEN\s*=", config_content):
            issues.append("business_bot_config.py still defines BOT_TOKEN")

    return issues


def db_checks(db_path: str, media_root: str) -> Tuple[List[str], Dict[str, int]]:
    warnings: List[str] = []
    counters: Dict[str, int] = {}

    if not os.path.exists(db_path):
        warnings.append(f"DB file not found: {db_path}")
        return warnings, counters

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        sql_checks = {
            "messages_owner_null": "SELECT COUNT(*) FROM messages WHERE owner_id IS NULL",
            "edit_history_owner_null": "SELECT COUNT(*) FROM edit_history WHERE owner_id IS NULL",
            "edit_history_orphans": """
                SELECT COUNT(*)
                FROM edit_history eh
                WHERE NOT EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.message_id = eh.message_id
                      AND m.chat_id = eh.chat_id
                      AND ((m.owner_id = eh.owner_id) OR (m.owner_id IS NULL AND eh.owner_id IS NULL))
                )
            """,
            "admin_scopes_orphan_owner": """
                SELECT COUNT(*)
                FROM admin_scopes s
                LEFT JOIN users u ON u.user_id = s.owner_id
                WHERE s.owner_id IS NOT NULL AND u.user_id IS NULL
            """,
            "team_scopes_orphan_owner": """
                SELECT COUNT(*)
                FROM team_scopes_v2 s
                LEFT JOIN users u ON u.user_id = s.owner_id
                WHERE s.owner_id IS NOT NULL AND u.user_id IS NULL
            """,
        }
        for name, sql in sql_checks.items():
            cur.execute(sql)
            counters[name] = int((cur.fetchone() or [0])[0] or 0)

        cur.execute("SELECT media_path FROM messages WHERE media_path IS NOT NULL AND TRIM(media_path) != ''")
        media_rows = [row[0] for row in cur.fetchall()]
        root_abs = os.path.abspath(media_root)
        outside = 0
        missing = 0
        for media_path in media_rows:
            abs_path = os.path.abspath(str(media_path))
            try:
                inside = os.path.commonpath([root_abs, abs_path]) == root_abs
            except Exception:
                inside = False
            if not inside:
                outside += 1
                continue
            if not os.path.isfile(abs_path):
                missing += 1
        counters["media_paths_total"] = len(media_rows)
        counters["media_paths_outside_root"] = outside
        counters["media_paths_missing_on_disk"] = missing
    finally:
        conn.close()

    return warnings, counters


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify project production-readiness basics")
    parser.add_argument("--root", default=ROOT, help="Project root")
    parser.add_argument("--db-path", default=DEFAULT_DB, help="Path to sqlite DB")
    parser.add_argument("--media-root", default=os.path.join(ROOT, "business_media"), help="MEDIA_PATH root")
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    db_path = os.path.abspath(args.db_path)
    media_root = os.path.abspath(args.media_root)

    compile_errors = compile_checks(root)
    secret_issues = secret_checks(root)
    db_warnings, db_counters = db_checks(db_path, media_root)

    print("=== VERIFY PROD READINESS ===")

    if compile_errors:
        print("\n[FAIL] Compile checks:")
        for item in compile_errors:
            print(f"- {item}")
    else:
        print("\n[OK] Compile checks passed")

    if secret_issues:
        print("\n[FAIL] Secret/config checks:")
        for item in secret_issues:
            print(f"- {item}")
    else:
        print("\n[OK] Secret/config checks passed")

    if db_warnings:
        print("\n[WARN] DB warnings:")
        for item in db_warnings:
            print(f"- {item}")

    if db_counters:
        print("\n[INFO] DB counters:")
        for key in sorted(db_counters.keys()):
            print(f"- {key}: {db_counters[key]}")

    bad_db = any(
        db_counters.get(key, 0) > 0
        for key in (
            "messages_owner_null",
            "edit_history_orphans",
            "admin_scopes_orphan_owner",
            "team_scopes_orphan_owner",
            "media_paths_outside_root",
        )
    )

    if compile_errors or secret_issues or bad_db:
        print("\n[RESULT] FAIL")
        return 1

    print("\n[RESULT] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
