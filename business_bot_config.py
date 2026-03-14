# business_bot_config.py
# IMPORTANT:
# - BOT_TOKEN must be provided via environment variable BOT_TOKEN.
# - Do not store real bot tokens in this file.


DB_PATH = "business_messages.db"
MEDIA_PATH = "./business_media"
DOWNLOAD_MEDIA = True

# Telegram admin user IDs (replace with your own values before production use).
ADMIN_IDS = [111111111, 222222222]

# Trigger words/symbols for reply-to-save flow.
REPLY_SAVE_TRIGGER = [".", "save", "сохрани", "💾"]

ADMIN_LOG_ENABLED = True
ARCHIVE_PATH = "./archives"
RAR_PATH = r"C:\Program Files\WinRAR\Rar.exe"
USE_7ZIP = False
