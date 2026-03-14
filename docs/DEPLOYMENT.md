# Deployment

## Prerequisites

- Python 3.10+ recommended
- Telegram bot token from BotFather
- Network access to Telegram API
- Optional archive tool:
  - WinRAR/7-Zip executable path configured in `business_bot_config.py`

## 1. Prepare Project

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Optional (development/check workflow):

```bash
python -m pip install -r requirements-dev.txt
```

## 2. Prepare `.env`

Create `.env` in project root:

```env
BOT_TOKEN=1234567890:YOUR_REAL_BOT_TOKEN
```

Runtime behavior:

- Bot reads `.env` from current working directory first, then project directory.
- Existing OS env var values are not overwritten by `.env`.
- If token is missing/placeholder, startup exits with config error.

## 3. Run Locally

## Windows PowerShell

```powershell
Set-Location "C:\path\to\Business Bot"
python -m pip install -r requirements.txt
python business_bot.py
```

## macOS/Linux Bash

```bash
cd /path/to/Business\ Bot
python -m pip install -r requirements.txt
python business_bot.py
```

## 4. Verify Startup

Expected behavior on healthy startup:

- startup logs show bot started and handlers registered
- token source is logged as `env BOT_TOKEN` (without printing token)
- polling begins (`run_polling(...)`)
- admins get startup notification

## Failure Case: `BOT_TOKEN` Missing

Symptoms:

- startup exits with config errors including `BOT_TOKEN not set`.

Fix:

1. Add valid token to `.env` or OS env.
2. Restart bot.

## Failure Case: Network/Telegram API Issues

Transient polling network errors may happen (`ReadError`/`NetworkError` style):

- current code uses tuned PTB timeouts/pool settings
- transient polling traceback noise is filtered
- bot usually continues automatically

If updates stop:

1. Check internet reachability from host.
2. Ensure only one polling instance is running with the same token.
3. Restart process and inspect logs.

## Failure Case: Telegram `Conflict`

Cause:

- more than one process uses polling with same bot token.

Fix:

1. Stop duplicate instances.
2. Keep single active polling worker per token.

## Production Recommendations (Current Architecture)

- Keep one polling instance per token.
- Keep DB and media directories on persistent storage.
- Back up DB regularly before migrations or bulk cleanup.
- Run readiness/integrity scripts before production rollout.
- Restrict OS-level access to `.env`, DB, and media directories.

No webhook deployment steps are included because current architecture is polling-based.
