# Environment Variables

This file documents env vars that are actually consumed by runtime code.

## Runtime Variables

## `BOT_TOKEN` (required)

- Used in: `business_bot.py` (`get_runtime_token()`)
- Purpose: Telegram bot token for PTB `Application.builder().token(...)`
- Example:

```env
BOT_TOKEN=1234567890:YOUR_REAL_BOT_TOKEN
```

Behavior if missing:

- `validate_runtime_config(...)` returns config error
- application exits fail-fast before polling starts

Behavior if value is placeholder (`YOUR_BOT_TOKEN_HERE`):

- treated as invalid/missing
- startup exits with config error

## `.env` Loading Behavior

Implemented in `load_runtime_env_file()` (`business_bot.py`):

- Reads `.env` from current working directory first.
- If different, then tries project directory `.env`.
- Supports lines like `KEY=value` and `export KEY=value`.
- Ignores comments and empty lines.
- Strips surrounding single/double quotes.
- Does not overwrite an already set non-empty OS env variable.

## Variables Not Read From Env At Runtime

The following are configured in `business_bot_config.py` (not env-driven in current code):

- `DB_PATH`
- `MEDIA_PATH`
- `DOWNLOAD_MEDIA`
- `ADMIN_IDS`
- `REPLY_SAVE_TRIGGER`
- `ARCHIVE_PATH`
- `RAR_PATH`
- `USE_7ZIP`

If you set these keys in `.env`, current code will load them into process env, but runtime paths/flags still come from `business_bot_config.py`.

## Recommended `.env` Template

Use `.env.example` as baseline:

```env
BOT_TOKEN=1234567890:YOUR_REAL_BOT_TOKEN
```

Do not store real secrets in VCS.

