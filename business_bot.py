# business_bot.py
import os
import re
import sys
import time
import traceback
import logging
from datetime import datetime, date, timedelta, UTC
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.request import HTTPXRequest
from telegram.ext import (Application, MessageHandler, filters, ContextTypes, 
                          TypeHandler, CommandHandler, CallbackQueryHandler, ConversationHandler,
                          PreCheckoutQueryHandler)
from business_bot_config import DB_PATH, MEDIA_PATH, DOWNLOAD_MEDIA, ADMIN_IDS, REPLY_SAVE_TRIGGER
from database import MessageDB

db: MessageDB = MessageDB(DB_PATH)
os.makedirs(MEDIA_PATH, exist_ok=True)
_runtime_env_loaded = False


def configure_stdio_utf8():
    """Best-effort UTF-8 console output for Windows; no-op on unsupported runtimes."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            # Keep default stream settings if runtime does not allow reconfigure.
            pass


def _strip_env_value(raw_value):
    value = (raw_value or "").strip()
    if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")):
        value = value[1:-1]
    return value.strip()


def load_runtime_env_file():
    """Load BOT_TOKEN and other runtime vars from local .env (cwd first, then project dir)."""
    global _runtime_env_loaded
    if _runtime_env_loaded:
        return

    candidate_paths = []
    cwd_env = os.path.join(os.getcwd(), ".env")
    candidate_paths.append(cwd_env)
    project_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.abspath(project_env) != os.path.abspath(cwd_env):
        candidate_paths.append(project_env)

    for env_path in candidate_paths:
        if not os.path.isfile(env_path):
            continue
        try:
            with open(env_path, "r", encoding="utf-8") as fh:
                for raw_line in fh:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("export "):
                        line = line[len("export "):].strip()
                    if "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    if not key:
                        continue
                    existing = os.environ.get(key)
                    if existing is not None and str(existing).strip() != "":
                        continue
                    os.environ[key] = _strip_env_value(value)
            _runtime_env_loaded = True
            return
        except Exception as e:
            print(f"[WARNING] Не удалось прочитать .env файл {env_path}: {e}")

    _runtime_env_loaded = True


def utcnow_naive() -> datetime:
    """UTC now as naive datetime for backward-compatible SQLite timestamps."""
    return datetime.now(UTC).replace(tzinfo=None)

# ==================== КОНСТАНТЫ ====================
ITEMS_PER_PAGE = 15
SUBSCRIPTION_PLANS = {
    "plan_30": {"days": 30, "stars": 30, "title": "30 days access"},
    "plan_90": {"days": 90, "stars": 80, "title": "90 days access"},
    "plan_180": {"days": 180, "stars": 150, "title": "180 days access"},
}
TRIAL_DAYS = 3
REMINDER_DAYS_BEFORE = (3, 1, 0)
DEFAULT_ACTIVITY_PAGE_SIZE = 12
HISTORY_PAGE_SIZE = 10
DEFAULT_PROMO_MAX_ACTIVATIONS = 100
DEFAULT_PROMO_PER_USER_LIMIT = 1
ANTI_SPAM_LIMITS = {
    "trial_activate": {"limit": 3, "window": 3600},
    "promo_apply": {"limit": 5, "window": 3600},
    "gift_start": {"limit": 5, "window": 3600},
    "gift_confirm": {"limit": 5, "window": 3600},
    "payment_start": {"limit": 8, "window": 3600},
    "referral_recovery": {"limit": 30, "window": 3600},
}
REFERRAL_INVITED_BONUS_DAYS = 7
REFERRAL_REFERRER_BONUS_DAYS = 15
INVOICE_PAYLOAD_MAX_LENGTH = 128
_PLAN_CODE_PATTERN = "|".join(re.escape(code) for code in SUBSCRIPTION_PLANS.keys())
INVOICE_PAYLOAD_ALLOWED_RE = re.compile(r"^[A-Za-z0-9:_-]+$")
INVOICE_PAYLOAD_EXPECTED_RE = re.compile(rf"^(?:{_PLAN_CODE_PATTERN}):\d+:\d+$")
GIFTPAYLOAD_EXPECTED_RE = re.compile(rf"^gift:\d+:\d+:(?:{_PLAN_CODE_PATTERN}):\d+$")
PROMOCODE_ALLOWED_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")
TEAM_PERMISSION_KEYS = [
    "view_users", "view_chats", "view_messages", "search_messages", "view_media", "export_data",
    "manage_subscriptions", "manage_trials", "manage_promocodes", "manage_gifts", "manage_referrals",
    "manage_blacklist", "view_diagnostics", "manage_roles", "manage_scopes", "retry_referrals",
    "cleanup_media", "archive_media", "manual_grants",
]
PROMO_TYPES = {
    "bonus_days": "Бонусные дни",
    "free_access": "Бесплатный доступ",
    "discount_percent": "Скидка (%)",
    "fixed_price_override": "Фикс-цена (Stars)",
}
ADMIN_ACTIONS_READ_ONLY = {
    "hide_msg",
    "admin_users", "view_user", "view_chat", "chat_media_menu", "chat_media_type",
    "download", "metadata", "fulltext",
    "admin_search_menu", "search_text", "search_media", "search_deleted", "search_edited",
    "search_page", "search_refresh", "chat_search_page", "search_in_chat",
    "admin_back", "admin_settings", "admin_media_menu", "media_photos", "media_videos",
    "media_voices", "media_videonotes", "media_saved", "media_all",
}
SUBSCRIPTION_BLOCK_TTL_SECONDS = 6 * 60 * 60
EXPIRY_NOTICE_TTL_SECONDS = 24 * 60 * 60
_subscription_block_notice_ts = {}
_expiry_notice_ts = {}
BOT_STARTED_AT = utcnow_naive()
BOT_REQUEST_CONNECT_TIMEOUT = 10.0
BOT_REQUEST_READ_TIMEOUT = 30.0
BOT_REQUEST_WRITE_TIMEOUT = 30.0
BOT_REQUEST_POOL_TIMEOUT = 5.0
BOT_REQUEST_POOL_SIZE = 64
BOT_REQUEST_MEDIA_WRITE_TIMEOUT = 60.0
UPDATES_REQUEST_CONNECT_TIMEOUT = 10.0
UPDATES_REQUEST_READ_TIMEOUT = 45.0
UPDATES_REQUEST_WRITE_TIMEOUT = 10.0
UPDATES_REQUEST_POOL_TIMEOUT = 5.0
UPDATES_REQUEST_POOL_SIZE = 16
POLLING_TIMEOUT_SECONDS = 30
POLLING_BOOTSTRAP_RETRIES = 3
_TRANSIENT_POLLING_LOG_TTL_SECONDS = 120
_last_transient_polling_log_ts = 0.0
_network_logging_configured = False


def build_telegram_requests():
    """Build separate PTB requests for regular API calls and getUpdates polling."""
    bot_request = HTTPXRequest(
        connection_pool_size=BOT_REQUEST_POOL_SIZE,
        connect_timeout=BOT_REQUEST_CONNECT_TIMEOUT,
        read_timeout=BOT_REQUEST_READ_TIMEOUT,
        write_timeout=BOT_REQUEST_WRITE_TIMEOUT,
        pool_timeout=BOT_REQUEST_POOL_TIMEOUT,
        media_write_timeout=BOT_REQUEST_MEDIA_WRITE_TIMEOUT,
    )
    updates_request = HTTPXRequest(
        connection_pool_size=UPDATES_REQUEST_POOL_SIZE,
        connect_timeout=UPDATES_REQUEST_CONNECT_TIMEOUT,
        read_timeout=UPDATES_REQUEST_READ_TIMEOUT,
        write_timeout=UPDATES_REQUEST_WRITE_TIMEOUT,
        pool_timeout=UPDATES_REQUEST_POOL_TIMEOUT,
    )
    return bot_request, updates_request


def _record_contains_transient_polling_error(record):
    """Return True only for noisy transient getUpdates ReadError/NetworkError logs."""
    message = (record.getMessage() or "").lower()
    is_polling_record = ("get updates" in message) or ("polling for updates" in message)
    if not is_polling_record:
        return False

    if "readerror" in message or "networkerror" in message:
        return True

    if not record.exc_info or len(record.exc_info) < 2:
        return False

    exc = record.exc_info[1]
    seen = set()
    depth = 0
    while exc is not None and depth < 8:
        exc_id = id(exc)
        if exc_id in seen:
            break
        seen.add(exc_id)

        exc_name = type(exc).__name__.lower()
        exc_text = str(exc).lower()
        if "readerror" in exc_name or "readerror" in exc_text:
            return True
        if "networkerror" in exc_name or "networkerror" in exc_text:
            return True

        exc = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
        depth += 1

    return False


class TransientPollingNetworkFilter(logging.Filter):
    """Suppress traceback spam for transient polling network errors."""

    def filter(self, record):
        global _last_transient_polling_log_ts

        if not _record_contains_transient_polling_error(record):
            return True

        now_ts = time.time()
        if now_ts - _last_transient_polling_log_ts >= _TRANSIENT_POLLING_LOG_TTL_SECONDS:
            _last_transient_polling_log_ts = now_ts
            print("[WARNING] Временная network ошибка в polling (getUpdates). Бот продолжит работу автоматически.")
        return False


def configure_telegram_network_logging():
    """Reduce noisy transient polling tracebacks without hiding app-level errors."""
    global _network_logging_configured

    if _network_logging_configured:
        return

    transient_filter = TransientPollingNetworkFilter()
    for logger_name in ("telegram.ext.Updater", "telegram.ext._updater", "telegram.ext.Application"):
        logging.getLogger(logger_name).addFilter(transient_filter)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    _network_logging_configured = True

# ==================== ОБРАБОТКА ОШИБОК ====================

async def safe_edit_message(query, text, reply_markup=None):
    """Безопасное редактирование сообщения (игнорирует ошибку 'не изменилось')"""
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as e:
        error_msg = str(e)
        if "There is no text in the message to edit" in error_msg or "is not a text message" in error_msg:
            try:
                await query.edit_message_caption(caption=text, reply_markup=reply_markup)
                return
            except Exception as caption_error:
                error_msg = str(caption_error)
        if "Message is not modified" in error_msg:
            await query.answer("✅ Данные актуальны", show_alert=False)
        elif "Message to edit not found" in error_msg:
            await query.answer("⚠️ Сообщение устарело", show_alert=True)
        elif "Message can't be edited" in error_msg:
            await query.answer("⚠️ Сообщение нельзя редактировать", show_alert=True)
        else:
            print(f"[ERROR] {error_msg}")
            await query.answer("❌ Ошибка", show_alert=False)

# ==================== УТИЛИТЫ ====================


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global PTB error handler to avoid unhandled-error spam in logs."""
    update_kind = type(update).__name__
    callback_data = None
    if isinstance(update, Update):
        if update.callback_query:
            update_kind = "callback_query"
            callback_data = update.callback_query.data
        elif update.message:
            update_kind = "message"
        elif update.business_message:
            update_kind = "business_message"
        elif update.edited_business_message:
            update_kind = "edited_business_message"
        elif update.deleted_business_messages:
            update_kind = "deleted_business_messages"
        elif update.business_connection:
            update_kind = "business_connection"

    details = f", callback_data={callback_data}" if callback_data else ""
    print(f"[ERROR] Unhandled exception (update_type={update_kind}{details}): {context.error}")
    if context.error:
        traceback.print_exception(type(context.error), context.error, context.error.__traceback__)


_unknown_connection_warning_ts = {}


def get_configured_admin_ids():
    """Return only valid admin chat ids from config."""
    if not isinstance(ADMIN_IDS, list):
        return []
    return [admin_id for admin_id in ADMIN_IDS if isinstance(admin_id, int) and admin_id > 0]


def get_admin_chat_id(candidate_id):
    """Validate that recipient is a configured admin."""
    admin_ids = get_configured_admin_ids()
    if not admin_ids:
        print("[ERROR] ADMIN_IDS пуст или некорректен, отправка админ-уведомления отменена")
        return candidate_id if is_admin(candidate_id) else None
    if candidate_id in admin_ids or is_admin(candidate_id):
        return candidate_id
    print(f"[WARNING] Заблокирована отправка admin-only уведомления не-админу: {candidate_id}")
    return None


async def send_admin_notification(bot, text, admin_id=None, **kwargs):
    """Safely send text notification to one admin or all configured admins."""
    if admin_id is None:
        targets = get_configured_admin_ids()
    else:
        validated = get_admin_chat_id(admin_id)
        targets = [validated] if validated is not None else []

    if not targets:
        print("[ERROR] Нет валидных admin_id для отправки текстового уведомления")
        return 0

    sent_count = 0
    for target_id in targets:
        try:
            await bot.send_message(target_id, text, **kwargs)
            sent_count += 1
        except Exception as e:
            print(f"[ERROR] Не удалось отправить уведомление админу {target_id}: {e}")
    return sent_count


async def send_admin_document(bot, document, admin_id=None, **kwargs):
    """Safely send document to one admin or all configured admins."""
    if admin_id is None:
        targets = get_configured_admin_ids()
    else:
        validated = get_admin_chat_id(admin_id)
        targets = [validated] if validated is not None else []

    if not targets:
        print("[ERROR] Нет валидных admin_id для отправки документа")
        return 0

    sent_count = 0
    for target_id in targets:
        try:
            if hasattr(document, "seek"):
                document.seek(0)
            await bot.send_document(target_id, document=document, **kwargs)
            sent_count += 1
        except Exception as e:
            print(f"[ERROR] Не удалось отправить документ админу {target_id}: {e}")
    return sent_count


def get_runtime_token():
    """Read bot token only from environment."""
    load_runtime_env_file()
    return (os.getenv("BOT_TOKEN") or "").strip()


def validate_runtime_config(bot_token):
    """Validate mandatory runtime settings before app startup."""
    errors = []
    if not bot_token or bot_token == "YOUR_BOT_TOKEN_HERE":
        errors.append("BOT_TOKEN не задан")

    if not isinstance(ADMIN_IDS, list) or not ADMIN_IDS:
        errors.append("ADMIN_IDS пустой или некорректный")
    else:
        valid_admin_ids = get_configured_admin_ids()
        if len(valid_admin_ids) != len(ADMIN_IDS):
            errors.append("ADMIN_IDS должен содержать только положительные int Telegram ID")

    if not DB_PATH:
        errors.append("DB_PATH не задан")
    if not MEDIA_PATH:
        errors.append("MEDIA_PATH не задан")

    return errors


def should_log_connection_warning(connection_id, ttl_seconds=300):
    """Rate-limit repeated warnings for unknown business connections."""
    now_ts = time.time()
    last_ts = _unknown_connection_warning_ts.get(connection_id, 0)
    if now_ts - last_ts < ttl_seconds:
        return False

    _unknown_connection_warning_ts[connection_id] = now_ts
    if len(_unknown_connection_warning_ts) > 5000:
        stale_before = now_ts - ttl_seconds
        for key, value in list(_unknown_connection_warning_ts.items()):
            if value < stale_before:
                _unknown_connection_warning_ts.pop(key, None)
    return True


def is_safe_media_path(media_path):
    """Allow only media files inside MEDIA_PATH directory."""
    if not media_path:
        return False

    try:
        media_root = os.path.abspath(MEDIA_PATH)
        candidate = os.path.abspath(media_path)
        return os.path.commonpath([media_root, candidate]) == media_root
    except Exception:
        return False


def is_safe_archive_path(file_path):
    """Allow only archive/export files inside ARCHIVE_PATH directory."""
    if not file_path:
        return False

    try:
        archive_root = os.path.abspath(ARCHIVE_PATH)
        candidate = os.path.abspath(file_path)
        return os.path.commonpath([archive_root, candidate]) == archive_root
    except Exception:
        return False


def safe_file_extension(file_path, default_ext="bin"):
    """Extract file extension safely for local filename generation."""
    if not file_path:
        return default_ext

    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else default_ext
    ext = "".join(ch for ch in ext if ch.isalnum())[:10]
    return ext or default_ext


def build_media_filename(media_type, owner_id, user_id, chat_id, message_id, extension):
    """Build stable media filename with owner isolation to avoid cross-owner collisions."""
    safe_ext = "".join(ch for ch in str(extension or "").lower() if ch.isalnum())[:10] or "bin"
    return f"{media_type}_{int(owner_id)}_{int(user_id)}_{int(chat_id)}_{int(message_id)}.{safe_ext}"


def format_datetime_msk(date_str):
    """Конвертация UTC -> MSK (+3 часа) и форматирование DD.MM HH:MM"""
    if not date_str:
        return "--:--"
    
    try:
        # ✅ ИСПРАВЛЕНИЕ: Поддержка datetime объектов с timezone
        if isinstance(date_str, datetime):
            dt_utc = date_str
            # Если есть timezone, делаем naive (убираем timezone)
            if dt_utc.tzinfo is not None:
                dt_utc = dt_utc.replace(tzinfo=None)
        elif isinstance(date_str, str):
            if len(date_str) < 16:
                return "--:--"
            
            # Убираем timezone из строки если есть
            if '+' in date_str:
                date_str = date_str.split('+')[0]
            elif 'Z' in date_str:
                date_str = date_str.replace('Z', '')
            
            # Убираем микросекунды если есть
            if '.' in date_str:
                date_str = date_str.split('.')[0]
            
            dt_utc = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        else:
            return "--:--"
        
        # Добавляем 3 часа для MSK
        dt_msk = dt_utc + timedelta(hours=3)
        # Форматируем как DD.MM HH:MM
        return dt_msk.strftime('%d.%m %H:%M')
    except Exception as e:
        print(f"[ERROR] format_datetime_msk: {e}, date_str={date_str}")
        return "--:--"

def get_superadmin_id():
    admin_ids = get_configured_admin_ids()
    return admin_ids[0] if admin_ids else None


def get_actor_role(user_id):
    """Alias for role checks in permission helpers."""
    return get_admin_role(user_id)


def get_admin_role(user_id):
    if not isinstance(user_id, int) or user_id <= 0:
        return None

    if user_id == get_superadmin_id():
        return "superadmin"

    db_role = db.get_admin_role(user_id)
    if db_role in ("admin", "admin_lite"):
        return db_role

    configured_admins = get_configured_admin_ids()
    if user_id in configured_admins[1:]:
        return "admin"

    team_role = db.get_team_role_v2(user_id)
    if team_role and int(team_role[3]) == 1:
        return team_role[1]
    return None


def is_superadmin(user_id):
    return get_actor_role(user_id) == "superadmin"


def is_admin_lite(user_id):
    return get_actor_role(user_id) == "admin_lite"


def is_admin(user_id):
    return get_actor_role(user_id) in ("superadmin", "admin", "admin_lite", "manager", "support", "analyst", "viewer", "custom")


def get_team_permissions(user_id):
    if not isinstance(user_id, int) or user_id <= 0:
        return set()
    role = get_actor_role(user_id)
    if role == "superadmin":
        return set(TEAM_PERMISSION_KEYS)
    if role in ("admin", "admin_lite"):
        # legacy permissions remain via old checks
        return set()
    return set(db.get_team_permissions_v2(user_id))


def has_team_permission(user_id, permission_key):
    role = get_actor_role(user_id)
    if role == "superadmin":
        return True
    if role == "admin":
        return True
    if role == "admin_lite":
        # legacy lite behavior is handled separately in can_use_admin_action/can_view_*
        return False
    return permission_key in get_team_permissions(user_id)


def can_manage_roles(actor_id, target_role=None, target_user_id=None):
    """Role management policy.
    superadmin: can manage admin/admin_lite roles.
    admin: can manage only admin_lite roles/scenarios, never superadmin/admin accounts.
    admin_lite: cannot manage roles.
    """
    actor_role = get_actor_role(actor_id)
    if actor_role not in ("superadmin", "admin"):
        return False

    if target_role is not None and target_role not in ("admin", "admin_lite"):
        return False

    if target_user_id is None:
        if actor_role == "superadmin":
            return target_role in ("admin", "admin_lite", None)
        return target_role in ("admin_lite", None)

    if target_user_id == get_superadmin_id():
        return False

    target_current_role = get_actor_role(target_user_id)

    if actor_role == "superadmin":
        if target_role is None:
            return target_current_role in ("admin", "admin_lite")
        return target_role in ("admin", "admin_lite")

    # admin
    if target_user_id == actor_id:
        return False
    if target_current_role in ("superadmin", "admin"):
        return False
    if target_role is None:
        return target_current_role == "admin_lite"
    return target_role == "admin_lite"


def can_manage_scopes(actor_id, target_admin_id=None):
    """Scope management is allowed to superadmin and admin, only for admin_lite targets."""
    actor_role = get_actor_role(actor_id)
    if actor_role not in ("superadmin", "admin"):
        return False

    if target_admin_id is None:
        return True

    if target_admin_id == get_superadmin_id():
        return False
    if get_actor_role(target_admin_id) != "admin_lite":
        return False
    if actor_role == "admin" and target_admin_id == actor_id:
        return False
    return True


def can_grant_subscriptions(actor_id):
    if get_actor_role(actor_id) in ("superadmin", "admin", "admin_lite"):
        return True
    return has_team_permission(actor_id, "manage_subscriptions")


def can_cancel_subscriptions(actor_id):
    if get_actor_role(actor_id) in ("superadmin", "admin"):
        return True
    return has_team_permission(actor_id, "manage_subscriptions")


def is_subscription_exempt(user_id):
    return is_admin(user_id)


def get_active_subscription(user_id):
    return db.get_active_subscription(user_id)


def get_active_trial(user_id):
    return db.get_active_trial(user_id)


def has_active_subscription(user_id):
    if is_subscription_exempt(user_id):
        return True
    return get_active_subscription(user_id) is not None or get_active_trial(user_id) is not None


def get_subscription_days_left(subscription_row):
    if not subscription_row:
        return 0
    try:
        expires_at = db._parse_dt(subscription_row[4])
        if not expires_at:
            return 0
        delta = expires_at - utcnow_naive()
        return max(delta.days + (1 if delta.seconds > 0 else 0), 0)
    except Exception:
        return 0


def format_subscription_summary(user_id):
    if is_subscription_exempt(user_id):
        role = get_admin_role(user_id) or "admin"
        return f"Role access: {role} (subscription not required)"

    row = db.get_subscription(user_id)
    if not row:
        return "Subscription: inactive"

    is_active = int(row[5]) == 1
    expires_at = db._parse_dt(row[4])
    active_now = is_active and expires_at and expires_at > utcnow_naive()
    if not active_now:
        trial = get_active_trial(user_id)
        if trial:
            trial_days = get_subscription_days_left((None, None, None, None, trial[3], 1))
            return f"Subscription: trial active ({trial_days} days left)"
        return "Subscription: inactive"

    days_left = get_subscription_days_left(row)
    return f"Subscription: active ({row[1] or 'custom'}, {days_left} days left)"


def can_view_owner(admin_user_id, owner_id):
    role = get_actor_role(admin_user_id)
    if role in ("superadmin", "admin"):
        return True
    if role != "admin_lite":
        if not has_team_permission(admin_user_id, "view_users"):
            return False
        scopes = db.get_team_scopes_v2(admin_user_id)
        if not scopes:
            return False
        for scope in scopes:
            scope_type = scope[2]
            scope_owner = scope[3]
            if scope_type == "global":
                return True
            if scope_owner is not None and int(scope_owner) == int(owner_id):
                return True
        return False

    scopes = db.get_admin_scopes(admin_user_id)
    if not scopes:
        return False
    return any(int(scope[2]) == int(owner_id) for scope in scopes)


def can_view_chat(admin_user_id, owner_id, chat_id):
    role = get_actor_role(admin_user_id)
    if role in ("superadmin", "admin"):
        return True
    if role != "admin_lite":
        if not has_team_permission(admin_user_id, "view_chats"):
            return False
        scopes = db.get_team_scopes_v2(admin_user_id)
        if not scopes:
            return False
        for scope in scopes:
            scope_type = scope[2]
            scope_owner = scope[3]
            scope_chat = scope[4]
            if scope_type == "global":
                return True
            if scope_owner is None or int(scope_owner) != int(owner_id):
                continue
            if scope_chat is None:
                return True
            if int(scope_chat) == int(chat_id):
                return True
        return False

    scopes = db.get_admin_scopes(admin_user_id)
    if not scopes:
        return False

    for scope in scopes:
        scope_owner = int(scope[2])
        scope_chat = scope[3]
        if scope_owner != int(owner_id):
            continue
        if scope_chat is None:
            return True
        if int(scope_chat) == int(chat_id):
            return True
    return False


def is_scope_limited_admin(admin_user_id):
    """True for roles that must be constrained by explicit owner/chat scope."""
    return get_actor_role(admin_user_id) not in ("superadmin", "admin")


def is_sensitive_admin_output_action(action_name):
    normalized = _normalize_admin_action(action_name or "")
    if not normalized:
        return False

    sensitive_exact = {
        "admin_users",
        "view_user",
        "view_chat",
        "admin_today",
        "admin_dates",
        "admin_activity",
        "admin_deleted",
        "admin_stats_menu",
        "admin_search_menu",
        "admin_media_menu",
        "admin_archive_menu",
        "admin_settings",
        "admin_diagnostics_menu",
        "admin_diagnostics_refresh",
        "download",
        "metadata",
        "fulltext",
    }
    if normalized in sensitive_exact:
        return True

    sensitive_prefixes = (
        "search_",
        "chat_search_page",
        "time_",
        "view_date_",
        "media_",
        "chat_media_",
        "export_chat_",
        "archive_",
        "cleanup_archives",
        "cleanup_media",
        "delete_chat_",
        "media_cleanup_",
    )
    return any(normalized.startswith(prefix) for prefix in sensitive_prefixes)


async def guard_admin_output_access(query, requester_id, action_name, owner_id=None, chat_id=None):
    """Fail-closed guard for every sensitive admin output branch."""
    if not is_admin(requester_id):
        await query.answer("❌ Доступ запрещён", show_alert=True)
        return False

    if action_name and not can_use_admin_action(requester_id, action_name):
        await query.answer("❌ Недостаточно прав", show_alert=True)
        return False

    if owner_id is not None and not can_view_owner(requester_id, owner_id):
        await query.answer("❌ Нет доступа к данным пользователя", show_alert=True)
        return False

    if chat_id is not None:
        if owner_id is None or not can_view_chat(requester_id, owner_id, chat_id):
            await query.answer("❌ Нет доступа к данным чата", show_alert=True)
            return False

    return True


def _normalize_admin_action(action):
    if not action:
        return ""
    if re.match(r"^search_page_\d+$", action):
        return "search_page"
    if re.match(r"^chat_search_page_\d+$", action):
        return "chat_search_page"
    if re.match(r"^search_media_page_\d+$", action):
        return "search_media"
    if re.match(r"^search_deleted_page_\d+$", action):
        return "search_deleted"
    if re.match(r"^search_edited_page_\d+$", action):
        return "search_edited"
    if "_page_" in action:
        action = action.split("_page_", 1)[0]
    for prefix in (
        "view_user_", "view_chat_", "chat_media_menu_", "chat_media_type_", "download_",
        "metadata_", "fulltext_", "search_in_chat_", "search_page_", "search_media_page_",
        "search_deleted_page_", "search_edited_page_", "chat_search_page_", "view_date_",
        "delete_chat_confirm_", "delete_chat_execute_", "export_chat_menu_", "export_chat_json_",
        "export_chat_csv_", "export_chat_txt_", "export_chat_html_", "export_chat_tghtml_",
        "time_24h_page_", "time_7d_page_", "time_30d_page_", "admin_users_page_",
        "admin_activity_page_", "admin_deleted_page_", "media_photos_page_", "media_videos_page_",
        "media_voices_page_", "media_videonotes_page_", "media_saved_page_", "media_all_page_",
        "archive_list_page_", "archive_view_", "role_set_", "role_scope_",
        "role_assign_", "role_revoke_", "role_view_", "scope_add_", "scope_remove_",
        "sub_grant_", "sub_action_", "sub_status_", "sub_history_", "sub_cancel_",
        "promo_create_type_", "promo_toggle_apply_", "promo_edit_apply_",
        "team_assign_pick_", "team_scope_owner_", "team_scope_chat_", "team_scope_clear_",
        "team_custom_perm_", "team_remove_",
        "media_cleanup_prepare_", "media_cleanup_preview_",
        "media_cleanup_confirm_",
    ):
        if action.startswith(prefix):
            return prefix.rstrip("_")
    return action


def _required_permission_for_action(action_name):
    normalized = _normalize_admin_action(action_name)
    if normalized in ("admin_users", "view_user", "view_chat"):
        return "view_users"
    if normalized in ("search_text", "search_media", "search_deleted", "search_edited", "search_page", "search_refresh", "chat_search_page", "search_in_chat"):
        return "search_messages"
    if normalized in ("metadata", "fulltext"):
        return "view_messages"
    if normalized in ("admin_media_menu", "media_photos", "media_videos", "media_voices", "media_videonotes", "media_saved", "media_all", "download", "chat_media_menu", "chat_media_type"):
        return "view_media"
    if normalized.startswith("sub_") or normalized == "admin_subscriptions_menu":
        return "manage_subscriptions"
    if normalized in ("admin_referrals_menu", "ref_admin_user_start"):
        return "manage_referrals"
    if normalized.startswith("ref_admin_retry"):
        return "retry_referrals"
    if normalized in ("admin_promocodes_menu", "promo_create_start", "promo_list", "promo_toggle_start", "promo_stats_start"):
        return "manage_promocodes"
    if normalized in ("admin_blacklist_menu", "blacklist_add_start", "blacklist_remove_start", "blacklist_list"):
        return "manage_blacklist"
    if normalized in ("admin_diagnostics_menu",):
        return "view_diagnostics"
    if normalized in ("admin_roles_menu", "team_roles_menu"):
        return "manage_roles"
    if normalized.startswith("team_assign") or normalized.startswith("team_member") or normalized.startswith("team_template") or normalized.startswith("team_custom") or normalized.startswith("team_remove"):
        return "manage_roles"
    if normalized.startswith("team_scope_"):
        return "manage_scopes"
    if normalized.startswith("media_cleanup_") or normalized in ("cleanup_media_confirm", "cleanup_media_confirmed"):
        return "cleanup_media"
    if normalized.startswith("archive_") or normalized.startswith("cleanup_archives"):
        return "archive_media"
    return None


def can_use_admin_action(admin_user_id, action_name):
    role = get_actor_role(admin_user_id)
    if role == "superadmin":
        return True
    if role == "admin":
        # fine-grained role/scope checks are enforced in role callbacks
        return True
    if role == "admin_lite":
        if action_name == "admin_subscriptions_menu":
            return True
        if action_name.startswith("sub_grant_"):
            return True
        if action_name in ("sub_status", "sub_history"):
            return True
        if action_name.startswith("sub_status_") or action_name.startswith("sub_history_"):
            return True
        if action_name.startswith("sub_grant_confirm_"):
            return True
        if action_name.startswith("sub_grant_back_"):
            return True
        if action_name == "sub_cancel" or action_name.startswith("sub_cancel_"):
            return False
        normalized = _normalize_admin_action(action_name)
        if normalized in ADMIN_ACTIONS_READ_ONLY:
            return True
        if normalized.startswith("view_chat") or normalized.startswith("view_user"):
            return True
        if normalized.startswith("metadata") or normalized.startswith("fulltext"):
            return True
        return False
    if role in ("manager", "support", "analyst", "viewer", "custom"):
        required = _required_permission_for_action(action_name)
        if required is None:
            # allow common navigation
            normalized = _normalize_admin_action(action_name)
            return normalized in ("admin_back", "admin_settings", "hide_msg")
        return has_team_permission(admin_user_id, required)
    return False


def extract_owner_chat_from_action(action):
    patterns = [
        (r"^view_user_(-?\d+)$", lambda m: (int(m.group(1)), None)),
        (r"^view_chat_(-?\d+)_(-?\d+)(?:_page_\d+)?$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^chat_media_menu_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^chat_media_type_(-?\d+)_(-?\d+)_", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^search_in_chat_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^archive_chat_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^delete_chat_(?:confirm|execute)_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^export_chat_(?:menu|json|csv|txt|html|tghtml)_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^download_\d+_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(2)), int(m.group(1)))),
        (r"^metadata_\d+_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(2)), int(m.group(1)))),
        (r"^fulltext_\d+_(-?\d+)_(-?\d+)$", lambda m: (int(m.group(2)), int(m.group(1)))),
    ]

    for pattern, resolver in patterns:
        match = re.match(pattern, action or "")
        if match:
            return resolver(match)
    return (None, None)


def parse_telegram_id(value):
    try:
        parsed = int(str(value).strip())
    except Exception:
        return None
    if parsed <= 0:
        return None
    return parsed


def parse_chat_id(value):
    try:
        parsed = int(str(value).strip())
    except Exception:
        return None
    if parsed == 0:
        return None
    return parsed


def validate_invoice_payload(raw_payload):
    payload = (raw_payload or "").strip()
    if not payload:
        return None, "❌ invoice_payload не может быть пустым"
    if len(payload) > INVOICE_PAYLOAD_MAX_LENGTH:
        return None, f"❌ invoice_payload слишком длинный (макс {INVOICE_PAYLOAD_MAX_LENGTH})"
    if not INVOICE_PAYLOAD_ALLOWED_RE.fullmatch(payload):
        return None, "❌ invoice_payload содержит недопустимые символы"
    if not INVOICE_PAYLOAD_EXPECTED_RE.fullmatch(payload):
        return None, "❌ invoice_payload не соответствует формату plan_xxx:<user_id>:<timestamp>"
    return payload, None


ROLE_FLOW_KEYS = (
    "awaiting_role_action",
    "role_flow_target_id",
    "role_flow_target_role",
    "role_flow_owner_id",
    "role_flow_chat_id",
    "role_flow_remove_mode",
)

SUBSCRIPTION_FLOW_KEYS = (
    "awaiting_subscription_action",
    "sub_flow_target_id",
    "sub_flow_comment",
)

REFERRAL_FLOW_KEYS = (
    "awaiting_referral_action",
    "ref_retry_payload",
)

PUBLIC_FLOW_KEYS = (
    "awaiting_public_action",
    "gift_recipient_id",
    "gift_recipient_label",
    "gift_plan_code",
    "gift_confirm_plan_code",
    "gift_confirm_days",
    "gift_confirm_stars",
    "history_page",
    "promo_pending_code",
)

ADMIN_INPUT_FLOW_KEYS = (
    "awaiting_admin_action",
    "admin_flow_target_id",
    "admin_flow_owner_id",
    "admin_flow_chat_id",
    "admin_flow_role",
    "admin_flow_code",
    "admin_flow_type",
    "admin_flow_value",
    "admin_flow_comment",
    "admin_flow_settings",
    "admin_flow_reason",
    "admin_flow_hours",
    "admin_flow_preview",
)


def clear_role_flow(context):
    for key in ROLE_FLOW_KEYS:
        context.user_data.pop(key, None)


def clear_subscription_flow(context):
    for key in SUBSCRIPTION_FLOW_KEYS:
        context.user_data.pop(key, None)


def clear_referral_flow(context):
    for key in REFERRAL_FLOW_KEYS:
        context.user_data.pop(key, None)


def clear_public_flow(context):
    for key in PUBLIC_FLOW_KEYS:
        context.user_data.pop(key, None)


def clear_admin_flow(context):
    for key in ADMIN_INPUT_FLOW_KEYS:
        context.user_data.pop(key, None)


def get_role_manage_permissions_text(actor_role):
    if actor_role == "superadmin":
        return (
            "Вы можете назначать роли `admin` и `admin_lite`, "
            "управлять scope и снимать роли (кроме superadmin)."
        )
    if actor_role == "admin":
        return (
            "Вы можете назначать только `admin_lite`, "
            "управлять scope для `admin_lite` и снимать роль `admin_lite`."
        )
    return "У вас нет прав на управление ролями."


def format_admin_access_report(target_id):
    role = get_actor_role(target_id)
    lines = [f"👤 Пользователь: `{target_id}`"]

    if role is None:
        lines.append("Роль: не назначена")
        return "\n".join(lines)

    lines.append(f"Роль: `{role}`")
    if role in ("superadmin", "admin"):
        lines.append("Доступ: полный (без scope-ограничений)")
        return "\n".join(lines)

    scopes = db.get_admin_scopes(target_id)
    if not scopes:
        lines.append("Scope: пустой (данные недоступны)")
        return "\n".join(lines)

    owner_scopes = [scope for scope in scopes if scope[3] is None]
    chat_scopes = [scope for scope in scopes if scope[3] is not None]

    lines.append("Scope к owner (все чаты):")
    if owner_scopes:
        for scope in owner_scopes[:50]:
            lines.append(f"• owner `{scope[2]}`")
    else:
        lines.append("• нет")

    lines.append("Scope к конкретным чатам:")
    if chat_scopes:
        for scope in chat_scopes[:100]:
            lines.append(f"• owner `{scope[2]}` + chat `{scope[3]}`")
    else:
        lines.append("• нет")

    return "\n".join(lines)


def format_actor_identity(actor_user):
    if actor_user is None:
        return "Администратор"

    full_name = (actor_user.full_name or actor_user.first_name or "").strip() or "Администратор"
    if actor_user.username:
        return f"{full_name} (@{actor_user.username}, ID: {actor_user.id})"
    return f"{full_name} (ID: {actor_user.id})"


async def notify_manual_subscription_grant(context, target_user_id, duration_days, expires_at, actor_user, comment, was_extension):
    action_word = "продлена" if was_extension else "выдана"
    lines = [
        f"✅ Вам {action_word} подписка на {duration_days} дней.",
        f"👤 Выдал: {format_actor_identity(actor_user)}",
    ]
    if comment and comment.strip():
        lines.append(f"📝 Комментарий: {comment.strip()}")
    lines.append(f"📅 Действует до: {expires_at.strftime('%d.%m.%Y %H:%M:%S')} UTC")

    try:
        await context.bot.send_message(target_user_id, "\n".join(lines))
    except Exception as e:
        print(f"[WARNING] Не удалось отправить уведомление о ручной подписке user_id={target_user_id}: {e}")


def filter_messages_by_scope(messages, admin_user_id):
    if not is_scope_limited_admin(admin_user_id):
        return messages

    filtered = []
    for msg in messages or []:
        try:
            owner_id = msg[2]
            chat_id = msg[1]
        except Exception:
            continue
        if can_view_chat(admin_user_id, owner_id, chat_id):
            filtered.append(msg)
    return filtered


def build_stats_from_messages(messages):
    if not messages:
        return (0, 0, 0, 0, 0, 0)
    total = len(messages)
    deleted = sum(1 for msg in messages if msg[9])
    edited = sum(1 for msg in messages if msg[10])
    media = sum(1 for msg in messages if msg[6] is not None)
    users = len({msg[2] for msg in messages})
    chats = len({(msg[2], msg[1]) for msg in messages})
    return (total, deleted, edited, media, users, chats)


def build_public_start_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Как работает бот", callback_data="public_how_it_works")],
        [InlineKeyboardButton("👤 Личный кабинет", callback_data="public_cabinet")],
        [InlineKeyboardButton("🎁 Реферальная программа", callback_data="public_referral")]
    ])


def build_public_how_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💫 Купить подписку", callback_data="public_plans")],
        [InlineKeyboardButton("◀️ Назад", callback_data="public_back_start")]
    ])


def build_public_plans_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("30 дней — 30 ⭐", callback_data="public_buy_plan_30")],
        [InlineKeyboardButton("90 дней — 80 ⭐", callback_data="public_buy_plan_90")],
        [InlineKeyboardButton("180 дней — 150 ⭐", callback_data="public_buy_plan_180")],
        [InlineKeyboardButton("◀️ Назад", callback_data="public_how_it_works")]
    ])


def parse_referrer_id_from_payload(payload):
    if not payload or not isinstance(payload, str):
        return None
    if not payload.startswith("ref_"):
        return None
    candidate = payload[4:].strip()
    return parse_telegram_id(candidate)


async def get_bot_username_cached(context: ContextTypes.DEFAULT_TYPE):
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", None)

    if isinstance(bot_data, dict):
        cached = bot_data.get("bot_username")
        if cached:
            return cached

    username = getattr(context.bot, "username", None)
    if not username:
        try:
            me = await context.bot.get_me()
            username = me.username if me else None
        except Exception as e:
            print(f"[WARNING] Не удалось получить username бота для referral-ссылки: {e}")
            username = None

    if username and isinstance(bot_data, dict):
        bot_data["bot_username"] = username
    return username


def build_referral_link(bot_username, user_id):
    if not bot_username or user_id is None:
        return None
    return f"https://t.me/{bot_username}?start=ref_{user_id}"


def build_public_referral_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Моя ссылка", callback_data="public_ref_link")],
        [InlineKeyboardButton("📊 Моя реферальная статистика", callback_data="public_ref_stats")],
        [InlineKeyboardButton("📨 Текст для друга", callback_data="public_ref_share")],
        [InlineKeyboardButton("◀️ Назад", callback_data="public_back_start")]
    ])


def format_public_referral_main_text(user_id, referral_link):
    link_text = referral_link or "Ссылка недоступна, попробуйте позже."
    return (
        "🎁 **Реферальная программа**\n\n"
        f"Пригласите друга по ссылке:\n{link_text}\n\n"
        "Как начисляется бонус:\n"
        "1. Друг открывает бота по вашей ссылке.\n"
        "2. Друг оплачивает подписку первый раз.\n"
        f"3. Друг получает +{REFERRAL_INVITED_BONUS_DAYS} дней, а вы +{REFERRAL_REFERRER_BONUS_DAYS} дней.\n\n"
        "Бонус за каждого приглашённого начисляется только один раз."
    )


def format_public_referral_stats_text(user_id):
    stats = db.get_user_referral_stats(user_id, recent_limit=5)
    lines = [
        "📊 **Моя реферальная статистика**",
        "",
        f"👥 Приглашено: {stats['invited_total']}",
        f"💳 Оплатили: {stats['paid_total']}",
        f"🎁 Начислено бонусных дней: {stats['bonus_days_total']}",
        "",
        "Последние приглашённые:"
    ]
    if stats["recent_referrals"]:
        for invited_user_id, created_at, first_paid_at, username, first_name in stats["recent_referrals"]:
            label = f"{first_name or 'User'}"
            if username:
                label += f" (@{username})"
            paid_mark = "оплатил" if first_paid_at else "без оплаты"
            lines.append(f"• `{invited_user_id}` — {label}, {paid_mark}")
    else:
        lines.append("• Пока никого нет")
    return "\n".join(lines)


def build_referral_share_text(referral_link):
    return (
        "Подключай этого бота для контроля удалённых и изменённых сообщений.\n"
        "Вот моя ссылка:\n"
        f"{referral_link}\n\n"
        f"После первой оплаты ты получишь +{REFERRAL_INVITED_BONUS_DAYS} дней, "
        f"а я получу +{REFERRAL_REFERRER_BONUS_DAYS} дней."
    )


def format_admin_referral_overview_text(overview):
    lines = [
        "🎁 **РЕФЕРАЛЬНАЯ СИСТЕМА**",
        "",
        f"Всего реферальных связей: {overview['total_links']}",
        f"Оплативших рефералов: {overview['paid_links']}",
        f"Начислений бонусов: {overview['rewards_count']}",
        f"Суммарно бонусных дней: {overview['rewards_days_total']}",
        "",
        "Топ пригласивших:"
    ]
    if overview["top_referrers"]:
        for referrer_user_id, invited_total, paid_total, bonus_days in overview["top_referrers"][:10]:
            lines.append(
                f"• `{referrer_user_id}` — приглашено {invited_total}, оплатили {paid_total}, бонус {bonus_days} дн."
            )
    else:
        lines.append("• данных пока нет")
    return "\n".join(lines)


def format_admin_referrer_details_text(target_user_id):
    stats = db.get_user_referral_stats(target_user_id, recent_limit=20)
    referrals = db.get_referrals_for_referrer(target_user_id, limit=20)
    lines = [
        f"👤 **Рефералы пользователя `{target_user_id}`**",
        "",
        f"Приглашено: {stats['invited_total']}",
        f"Оплатили: {stats['paid_total']}",
        f"Начислено бонусных дней: {stats['bonus_days_total']}",
        "",
        "Последние связи:"
    ]
    if referrals:
        for row in referrals:
            invited_user_id, created_at, first_paid_at, _, referrer_bonus_granted_at, status, username, first_name = row
            label = first_name or "User"
            if username:
                label += f" (@{username})"
            lines.append(
                f"• invited `{invited_user_id}` — {label}, status={status}, first_paid={first_paid_at or '-'}, "
                f"bonus_at={referrer_bonus_granted_at or '-'}"
            )
    else:
        lines.append("• нет записей")
    return "\n".join(lines)


def format_referral_retry_result_text(invoice_payload, star_payment_row, referral_result):
    invited_user_id = int(star_payment_row[1]) if star_payment_row else 0
    referral_found = bool(referral_result.get("referral_found"))

    invited_status = "не выполнялся"
    if referral_result.get("invited_bonus_granted"):
        invited_status = "✅ начислен"
    elif referral_result.get("invited_bonus_already_granted"):
        invited_status = "ℹ️ уже был начислен"
    elif referral_found:
        invited_status = "ℹ️ не начислен"

    referrer_status = "не выполнялся"
    if referral_result.get("referrer_bonus_granted"):
        referrer_status = "✅ начислен"
    elif referral_result.get("referrer_bonus_already_granted"):
        referrer_status = "ℹ️ уже был начислен"
    elif referral_found:
        referrer_status = "ℹ️ не начислен"

    invited_expires = referral_result.get("invited_expires_at")
    referrer_expires = referral_result.get("referrer_expires_at")
    invited_expires_str = invited_expires.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if invited_expires else "-"
    referrer_expires_str = referrer_expires.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if referrer_expires else "-"

    lines = [
        "🔁 **Retry referral by payload**",
        "",
        f"Payload: `{invoice_payload}`",
        f"Star payment status: `{star_payment_row[7]}`",
        f"Invited user: `{invited_user_id}`",
        f"Referral: {'✅ найден' if referral_found else '❌ не найден'}",
        f"Invited bonus: {invited_status}",
        f"Referrer bonus: {referrer_status}",
        f"Referrer user: `{referral_result.get('referrer_user_id') or 0}`",
        f"Invited продлен до: {invited_expires_str}",
        f"Referrer продлен до: {referrer_expires_str}",
        f"Result reason: `{referral_result.get('reason', 'unknown')}`",
    ]
    return "\n".join(lines)


def format_referral_retry_confirm_text(invoice_payload, star_payment_row, referral_row):
    payment_user_id = int(star_payment_row[1])
    plan_code = star_payment_row[2] or "-"
    status = star_payment_row[7] or "-"
    purchased_at = star_payment_row[8] or "-"

    lines = [
        "⚠️ **Подтверждение retry referral**",
        "",
        f"Payload: `{invoice_payload}`",
        f"Payment user_id: `{payment_user_id}`",
        f"Plan code: `{plan_code}`",
        f"Status: `{status}`",
        f"Purchased at: `{purchased_at}`",
        f"Referral: {'✅ найден' if referral_row else '❌ не найден'}",
    ]
    if status != "paid":
        lines.append("")
        lines.append("⚠️ Внимание: статус платежа не `paid`, retry обычно не требуется.")
    return "\n".join(lines)

def format_stats(stats):
    return f"""📊 **ОБЩАЯ СТАТИСТИКА**

👥 Пользователей: {stats[0] or 0}
💬 Всего сообщений: {stats[1] or 0}
🗑 Удалено: {stats[2] or 0}
✏️ Изменено: {stats[3] or 0}
📎 Медиа: {stats[4] or 0}
"""

def format_date_stats(stats, date_str):
    return f"""📅 **{date_str}**

💬 Сообщений: {stats[0] or 0}
🗑 Удалено: {stats[1] or 0}
✏️ Изменено: {stats[2] or 0}
📎 Медиа: {stats[3] or 0}
👥 Пользователей: {stats[4] or 0}
💭 Чатов: {stats[5] or 0}
"""

def format_time_range_stats(stats, title):
    """Форматирование статистики за временной диапазон"""
    return f"""⏱ **{title}**

💬 Всего: {stats[0] or 0}
🗑 Удалено: {stats[1] or 0}
✏️ Изменено: {stats[2] or 0}
👥 Пользователей: {stats[3] or 0}
💭 Чатов: {stats[4] or 0}
📎 Медиа: {stats[5] or 0}
"""

def truncate_text(text, max_length=30):
    if not text:
        return "(нет текста)"
    return text[:max_length] + "..." if len(text) > max_length else text

def format_message_preview(msg, add_full_button=False):
    """Форматирование превью сообщения с поддержкой reply_to и кнопки полного текста"""
    msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, reply_to = msg
    
    status = ""
    if is_deleted:
        status = "🗑"
    elif is_edited:
        status = "✏️"
    
    reply_icon = "↩️" if reply_to else ""
    
    # ✅ НОВЫЙ ФОРМАТ: DD.MM HH:MM (MSK)
    time_str = format_datetime_msk(msg_date)
    
    # Проверка на длину текста
    if text_msg and len(text_msg) > 100:
        display_text = truncate_text(text_msg, 100)
        has_full_text = True
    else:
        display_text = text_msg if text_msg else f"[{media_type}]" if media_type else "(пусто)"
        has_full_text = False
    
    preview = f"{status}{reply_icon} `{time_str}` **{username}**: {display_text}"
    
    return {
        'text': preview,
        'has_full_text': has_full_text,
        'full_text_data': f"fulltext_{msg_id}_{chat_id}_{owner_id}" if has_full_text else None
    }

def format_message_preview_with_download(msg):
    """✅ КОМПАКТНЫЙ формат для медиа: username DD.MM HH:MM"""
    msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, reply_to = msg
    
    # ✅ НОВЫЙ ФОРМАТ: username DD.MM HH:MM
    time_str = format_datetime_msk(msg_date)
    
    status = ""
    if is_deleted:
        status = "🗑 "
    
    # ✅ КОМПАКТНЫЙ ВЫВОД
    preview_text = f"{status}**{username}** `{time_str}`"
    
    return {
        'text': preview_text,
        'file': media_path.split('/')[-1] if media_path else 'N/A',
        'download_data': f"download_{msg_id}_{chat_id}_{owner_id}",
        'metadata_data': f"metadata_{msg_id}_{chat_id}_{owner_id}"
    }

def format_search_results(messages, search_text, page, total_pages, total_results):
    """Форматирование результатов поиска по тексту"""
    if not messages:
        return f"❌ По запросу '{search_text}' ничего не найдено."
    
    text = f"🔍 **РЕЗУЛЬТАТЫ ПОИСКА: '{search_text}'**\n\n"
    text += f"📊 Найдено сообщений: {total_results}\n"  # ✅ Теперь показывает общее количество
    text += f"📄 Страница {page + 1}/{total_pages}\n\n"
    
    for msg in messages:
        preview = format_message_preview(msg, add_full_button=True)
        text += preview['text'] + "\n"
    
    return text

# ==================== АРХИВАЦИЯ И ЭКСПОРТ ====================

import subprocess
import json
import csv
import shutil
from pathlib import Path
from business_bot_config import ARCHIVE_PATH, RAR_PATH, USE_7ZIP

os.makedirs(ARCHIVE_PATH, exist_ok=True)

def create_rar_archive(source_path, archive_name):
    """Создать .rar архив"""
    archive_path = os.path.join(ARCHIVE_PATH, archive_name)
    
    try:
        if USE_7ZIP:
            # Используем 7-Zip
            cmd = [
                RAR_PATH, 'a', '-tzip',  # создать zip архив
                archive_path, source_path
            ]
        else:
            # Используем WinRAR
            cmd = [
                RAR_PATH, 'a', '-m5', '-ep1',  # максимальное сжатие, без путей
                archive_path, source_path
            ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            return archive_path
        else:
            print(f"[ERROR] Ошибка архивации: {result.stderr}")
            return None
    except Exception as e:
        print(f"[ERROR] Ошибка создания архива: {e}")
        return None

def export_messages_json(messages, filename):
    """Экспорт в JSON"""
    export_path = os.path.join(ARCHIVE_PATH, filename)
    
    data = []
    for msg in messages:
        msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, original_text, reply_to = msg
        
        data.append({
            'message_id': msg_id,
            'chat_id': chat_id,
            'user_id': user_id_msg,
            'username': username,
            'text': text_msg,
            'media_type': media_type,
            'media_path': media_path,
            'date': str(msg_date),
            'is_deleted': bool(is_deleted),
            'is_edited': bool(is_edited),
            'original_text': original_text,
            'reply_to': reply_to
        })
    
    with open(export_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return export_path

def export_messages_csv(messages, filename):
    """Экспорт в CSV"""
    export_path = os.path.join(ARCHIVE_PATH, filename)
    
    with open(export_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'Chat ID', 'Username', 'Text', 'Media', 'Date', 'Deleted', 'Edited'])
        
        for msg in messages:
            msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, _ = msg
            writer.writerow([
                msg_id, chat_id, username, text_msg or '', 
                media_type or '', format_datetime_msk(msg_date), 
                'Да' if is_deleted else 'Нет', 
                'Да' if is_edited else 'Нет'
            ])
    
    return export_path

def export_messages_txt(messages, filename):
    """Экспорт в TXT"""
    export_path = os.path.join(ARCHIVE_PATH, filename)
    
    with open(export_path, 'w', encoding='utf-8') as f:
        for msg in messages:
            msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, _ = msg
            
            f.write(f"{'='*60}\n")
            f.write(f"[{format_datetime_msk(msg_date)}] {username}\n")
            f.write(f"Chat ID: {chat_id} | Message ID: {msg_id}\n")
            
            if is_deleted:
                f.write("🗑 УДАЛЕНО\n")
            if is_edited:
                f.write("✏️ ИЗМЕНЕНО\n")
            
            f.write(f"\n{text_msg or '(нет текста)'}\n")
            
            if media_type:
                f.write(f"\n📎 Медиа: {media_type}\n")
            
            f.write(f"\n")
    
    return export_path

def export_messages_html(messages, filename, chat_name="Чат"):
    """Экспорт в HTML"""
    export_path = os.path.join(ARCHIVE_PATH, filename)
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Экспорт: {chat_name}</title>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            background: #0e1621; 
            color: #e4e6eb; 
            padding: 20px;
            max-width: 900px;
            margin: 0 auto;
        }}
        .message {{ 
            background: #1c1e21; 
            padding: 15px; 
            margin: 10px 0; 
            border-radius: 8px;
            border-left: 3px solid #0088cc;
        }}
        .message.deleted {{ border-left-color: #e53935; }}
        .message.edited {{ border-left-color: #ffa726; }}
        .header {{ 
            color: #0088cc; 
            font-weight: bold; 
            margin-bottom: 8px;
        }}
        .meta {{ 
            color: #8696a0; 
            font-size: 0.85em; 
            margin-bottom: 10px;
        }}
        .text {{ 
            line-height: 1.5; 
            white-space: pre-wrap;
        }}
        .badge {{ 
            background: #e53935; 
            color: white; 
            padding: 2px 8px; 
            border-radius: 4px; 
            font-size: 0.8em;
            margin-left: 10px;
        }}
        .badge.edited {{ background: #ffa726; }}
        .media {{ 
            color: #0088cc; 
            margin-top: 10px;
        }}
        h1 {{ 
            color: #0088cc; 
            text-align: center; 
            border-bottom: 2px solid #0088cc;
            padding-bottom: 10px;
        }}
    </style>
</head>
<body>
    <h1>📋 Экспорт: {chat_name}</h1>
    <p style="text-align: center; color: #8696a0;">Всего сообщений: {len(messages)}</p>
"""
    
    for msg in messages:
        msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, _ = msg
        
        css_class = "message"
        badges = ""
        
        if is_deleted:
            css_class += " deleted"
            badges += '<span class="badge">🗑 УДАЛЕНО</span>'
        if is_edited:
            css_class += " edited"
            badges += '<span class="badge edited">✏️ ИЗМЕНЕНО</span>'
        
        html += f"""
    <div class="{css_class}">
        <div class="header">{username} {badges}</div>
        <div class="meta">{format_datetime_msk(msg_date)} | Chat: {chat_id} | ID: {msg_id}</div>
        <div class="text">{text_msg or '(нет текста)'}</div>
        {f'<div class="media">📎 {media_type}</div>' if media_type else ''}
    </div>
"""
    
    html += """
</body>
</html>
"""
    
    with open(export_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    return export_path


def export_telegram_html(messages, filename, chat_name="Чат"):
    """Экспорт в формате Telegram Desktop"""
    export_path = os.path.join(ARCHIVE_PATH, filename)
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{chat_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Roboto, sans-serif;
            background: #0d1117;
            color: #c9d1d9;
            padding: 0;
            margin: 0;
        }}
        .page {{
            max-width: 900px;
            margin: 0 auto;
            background: #161b22;
            min-height: 100vh;
        }}
        .header {{
            background: #21262d;
            padding: 20px;
            border-bottom: 1px solid #30363d;
        }}
        .header h1 {{
            margin: 0;
            color: #58a6ff;
            font-size: 24px;
        }}
        .header .info {{
            color: #8b949e;
            margin-top: 5px;
            font-size: 14px;
        }}
        .messages {{
            padding: 20px;
        }}
        .message {{
            margin-bottom: 15px;
            display: flex;
            align-items: flex-start;
        }}
        .message.deleted {{
            opacity: 0.6;
        }}
        .avatar {{
            width: 40px;
            height: 40px;
            border-radius: 50%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            margin-right: 12px;
            flex-shrink: 0;
        }}
        .content {{
            flex: 1;
        }}
        .username {{
            color: #58a6ff;
            font-weight: 600;
            margin-bottom: 4px;
        }}
        .time {{
            color: #8b949e;
            font-size: 12px;
            margin-left: 8px;
        }}
        .text {{
            background: #21262d;
            padding: 10px 14px;
            border-radius: 12px;
            line-height: 1.5;
            word-wrap: break-word;
        }}
        .text.deleted {{
            background: #2d1a1f;
            border-left: 3px solid #da3633;
        }}
        .text.edited {{
            background: #2d271a;
            border-left: 3px solid #d29922;
        }}
        .media {{
            background: #0d1117;
            padding: 8px 12px;
            border-radius: 8px;
            margin-top: 8px;
            color: #58a6ff;
            font-size: 14px;
        }}
        .badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
            margin-left: 8px;
            font-weight: 600;
        }}
        .badge.deleted {{
            background: #da3633;
            color: white;
        }}
        .badge.edited {{
            background: #d29922;
            color: white;
        }}
        .reply-indicator {{
            border-left: 3px solid #58a6ff;
            padding-left: 8px;
            margin-bottom: 6px;
            color: #8b949e;
            font-size: 13px;
        }}
    </style>
</head>
<body>
    <div class="page">
        <div class="header">
            <h1>{chat_name}</h1>
            <div class="info">Экспортировано {datetime.now().strftime('%d.%m.%Y %H:%M')} • {len(messages)} сообщений</div>
        </div>
        <div class="messages">
"""
    
    for msg in messages:
        msg_id, chat_id, owner_id, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, original_text, reply_to = msg
        
        css_class = "message"
        if is_deleted:
            css_class += " deleted"
        
        badges = ""
        text_class = "text"
        
        if is_deleted:
            badges += '<span class="badge deleted">УДАЛЕНО</span>'
            text_class += " deleted"
        if is_edited:
            badges += '<span class="badge edited">ИЗМЕНЕНО</span>'
            text_class += " edited"
        
        avatar_letter = username[0].upper() if username else "?"
        
        reply_html = ""
        if reply_to:
            reply_html = f'<div class="reply-indicator">↩️ Ответ на сообщение #{reply_to}</div>'
        
        media_html = ""
        if media_type:
            media_html = f'<div class="media">📎 {media_type}</div>'
        
        html += f"""
            <div class="{css_class}">
                <div class="avatar">{avatar_letter}</div>
                <div class="content">
                    <div>
                        <span class="username">{username}</span>
                        <span class="time">{format_datetime_msk(msg_date)}</span>
                        {badges}
                    </div>
                    {reply_html}
                    <div class="{text_class}">{text_msg or '(нет текста)'}</div>
                    {media_html}
                </div>
            </div>
"""
    
    html += """
        </div>
    </div>
</body>
</html>
"""
    
    with open(export_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    return export_path


# ==================== УПРАВЛЕНИЕ АРХИВАМИ ====================

def get_archives_info():
    """Получить информацию о папке archives"""
    if not os.path.exists(ARCHIVE_PATH):
        return {'count': 0, 'size_mb': 0}
    
    total_size = 0
    file_count = 0
    
    for filename in os.listdir(ARCHIVE_PATH):
        file_path = os.path.join(ARCHIVE_PATH, filename)
        if os.path.isfile(file_path):
            total_size += os.path.getsize(file_path)
            file_count += 1
    
    return {
        'count': file_count,
        'size_mb': round(total_size / (1024 * 1024), 2)
    }

def cleanup_archives():
    """Удалить все файлы из папки archives"""
    if not os.path.exists(ARCHIVE_PATH):
        return 0
    
    deleted_count = 0
    
    for filename in os.listdir(ARCHIVE_PATH):
        file_path = os.path.join(ARCHIVE_PATH, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                deleted_count += 1
        except Exception as e:
            print(f"[ERROR] Ошибка удаления {file_path}: {e}")
    
    return deleted_count


def get_scoped_media_messages(requester_id, where_sql="media_type IS NOT NULL", params=()):
    """Load media messages and enforce scope filtering for non-full-access roles."""
    cursor = db.conn.cursor()
    cursor.execute(
        f"""
        SELECT * FROM messages
        WHERE {where_sql}
        ORDER BY date DESC
        """,
        tuple(params or ()),
    )
    rows = cursor.fetchall()
    return filter_messages_by_scope(rows, requester_id)


def build_scoped_media_stats(requester_id):
    media_rows = get_scoped_media_messages(requester_id, "media_type IS NOT NULL")
    counts = {}
    for row in media_rows:
        media_type = row[6]
        if not media_type:
            continue
        counts[media_type] = counts.get(media_type, 0) + 1

    ordered_types = [
        "photo", "video", "voice", "video_note", "audio", "document",
        "saved_photo", "saved_video", "saved_voice", "saved_video_note", "saved_audio", "saved_document",
    ]
    result = []
    for media_type in ordered_types:
        if media_type in counts:
            result.append((media_type, counts.pop(media_type)))
    for media_type in sorted(counts.keys()):
        result.append((media_type, counts[media_type]))
    return result


def build_scoped_overall_stats(requester_id):
    """Scoped replacement for db.get_all_stats() for limited roles."""
    if not is_scope_limited_admin(requester_id):
        return db.get_all_stats()

    cursor = db.conn.cursor()
    cursor.execute("SELECT owner_id, chat_id, is_deleted, is_edited, media_type FROM messages")
    rows = cursor.fetchall()

    total = 0
    deleted = 0
    edited = 0
    media = 0
    owners = set()

    for owner_id, chat_id, is_deleted, is_edited, media_type in rows:
        if not can_view_chat(requester_id, owner_id, chat_id):
            continue
        total += 1
        deleted += 1 if int(is_deleted or 0) == 1 else 0
        edited += 1 if int(is_edited or 0) == 1 else 0
        media += 1 if media_type else 0
        owners.add(owner_id)

    return (len(owners), total, deleted, edited, media)


def build_scoped_time_stats(requester_id, *, hours=None, days=None):
    """Scoped replacement for db.get_stats_by_time_range() for limited roles."""
    if not is_scope_limited_admin(requester_id):
        return db.get_stats_by_time_range(hours=hours, days=days)

    if hours is not None:
        threshold_dt = utcnow_naive() - timedelta(hours=int(hours))
    elif days is not None:
        threshold_dt = utcnow_naive() - timedelta(days=int(days))
    else:
        threshold_dt = None

    cursor = db.conn.cursor()
    if threshold_dt is None:
        cursor.execute("SELECT owner_id, chat_id, is_deleted, is_edited, media_type FROM messages")
    else:
        cursor.execute(
            """
            SELECT owner_id, chat_id, is_deleted, is_edited, media_type
            FROM messages
            WHERE date >= ?
            """,
            (threshold_dt.strftime("%Y-%m-%d %H:%M:%S"),),
        )

    rows = cursor.fetchall()
    total = 0
    deleted = 0
    edited = 0
    media = 0
    users = set()
    chats = set()

    for owner_id, chat_id, is_deleted, is_edited, media_type in rows:
        if not can_view_chat(requester_id, owner_id, chat_id):
            continue
        total += 1
        deleted += 1 if int(is_deleted or 0) == 1 else 0
        edited += 1 if int(is_edited or 0) == 1 else 0
        media += 1 if media_type else 0
        users.add(owner_id)
        chats.add((owner_id, chat_id))

    return (total, deleted, edited, len(users), len(chats), media)


def get_archive_dates_for_requester(requester_id, limit=10):
    cursor = db.conn.cursor()
    cursor.execute("""
    SELECT DATE(date) AS date_key, owner_id, chat_id
    FROM messages
    WHERE media_type IS NOT NULL AND media_path IS NOT NULL
    ORDER BY date DESC
    """)
    rows = cursor.fetchall()

    counts = {}
    for date_key, owner_id, chat_id in rows:
        if not date_key:
            continue
        if is_scope_limited_admin(requester_id) and not can_view_chat(requester_id, owner_id, chat_id):
            continue
        counts[date_key] = counts.get(date_key, 0) + 1

    return sorted(counts.items(), key=lambda item: item[0], reverse=True)[:max(1, int(limit))]


def get_archive_chats_for_requester(requester_id, limit=20):
    cursor = db.conn.cursor()
    cursor.execute("""
    SELECT owner_id, chat_id, COUNT(*) AS media_count
    FROM messages
    WHERE media_type IS NOT NULL AND media_path IS NOT NULL
    GROUP BY owner_id, chat_id
    ORDER BY media_count DESC
    """)
    rows = cursor.fetchall()

    visible = []
    for owner_id, chat_id, media_count in rows:
        if is_scope_limited_admin(requester_id) and not can_view_chat(requester_id, owner_id, chat_id):
            continue
        visible.append((owner_id, chat_id, int(media_count or 0)))
        if len(visible) >= max(1, int(limit)):
            break
    return visible


def collect_archive_media_paths(requester_id, where_sql="", params=()):
    base_where = "media_type IS NOT NULL AND media_path IS NOT NULL AND TRIM(media_path) != ''"
    final_where = base_where if not where_sql else f"{base_where} AND ({where_sql})"

    cursor = db.conn.cursor()
    cursor.execute(
        f"""
        SELECT owner_id, chat_id, media_path
        FROM messages
        WHERE {final_where}
        ORDER BY date DESC
        """,
        tuple(params or ()),
    )
    rows = cursor.fetchall()

    seen_abs = set()
    copy_paths = []
    skipped_scope = 0
    skipped_unsafe = 0
    missing_files = 0

    for owner_id, chat_id, media_path in rows:
        if is_scope_limited_admin(requester_id) and not can_view_chat(requester_id, owner_id, chat_id):
            skipped_scope += 1
            continue
        if not is_safe_media_path(media_path):
            skipped_unsafe += 1
            print(f"[WARNING] archive skip unsafe media_path: {media_path}")
            continue
        abs_path = os.path.abspath(media_path)
        if abs_path in seen_abs:
            continue
        seen_abs.add(abs_path)
        if not os.path.isfile(abs_path):
            missing_files += 1
            continue
        copy_paths.append(abs_path)

    return {
        "paths": copy_paths,
        "skipped_scope": skipped_scope,
        "skipped_unsafe": skipped_unsafe,
        "missing_files": missing_files,
    }


def copy_media_files_for_archive(paths, temp_dir):
    copied = 0
    for src_path in paths:
        if not is_safe_media_path(src_path):
            print(f"[WARNING] archive copy skipped unsafe source path: {src_path}")
            continue
        src = Path(src_path)
        target_name = src.name
        target_path = Path(temp_dir) / target_name
        suffix = 1
        while target_path.exists():
            target_name = f"{src.stem}_{suffix}{src.suffix}"
            target_path = Path(temp_dir) / target_name
            suffix += 1
        try:
            shutil.copy2(src, target_path)
            copied += 1
        except Exception as exc:
            print(f"[WARNING] archive copy failed for {src_path}: {exc}")
    return copied

# ==================== ОБРАБОТКА ПОИСКА ПО ТЕКСТУ ====================

async def send_search_page(message_or_query, context, page=0, edit=False):
    """Универсальная отправка/редактирование страницы результатов поиска"""
    requester = getattr(message_or_query, "from_user", None)
    requester_id = getattr(requester, "id", None)
    if requester_id is not None:
        if not is_admin(requester_id):
            return
    search_text = context.user_data.get('search_text', '')
    messages = context.user_data.get('search_results', [])
    if requester_id is not None:
        messages = filter_messages_by_scope(messages, requester_id)
        context.user_data['search_results'] = messages
    
    if not messages:
        text = f"❌ По запросу '{search_text}' ничего не найдено."
        keyboard = [
            [InlineKeyboardButton("🔍 Новый поиск", callback_data="search_text")],
            [InlineKeyboardButton("◀️ В меню", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if edit:
            await safe_edit_message(message_or_query, text, reply_markup)
        else:
            await message_or_query.reply_text(text, reply_markup=reply_markup)
        return
    
    # ✅ Пагинация
    ITEMS_PER_PAGE = 10
    total_results = len(messages)
    total_pages = max((total_results + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
    
    # ✅ Защита от выхода за границы
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1
    
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, total_results)
    
    # ✅ Формируем текст
    text = format_search_results(
        messages[start_idx:end_idx], 
        search_text, 
        page, 
        total_pages,
        total_results  # ✅ Передаем общее количество
    )
    
    # ✅ Кнопки для полного текста и метаданных
    keyboard = []
    for msg in messages[start_idx:end_idx]:
        msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
        time_str = format_datetime_msk(msg[8])
        
        keyboard.append([
            InlineKeyboardButton(f"📄 {time_str}", callback_data=f"fulltext_{msg_id}_{chat_id}_{owner_id}"),
            InlineKeyboardButton(f"📋 Метаданные", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
        ])
    
    # ✅ Навигация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"search_page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="search_refresh"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_page_{page+1}"))
    
    keyboard.append(nav_buttons)
    
    # ✅ Доп. кнопки
    keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="search_text")])
    keyboard.append([InlineKeyboardButton("◀️ В меню", callback_data="admin_back")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # ✅ Сохраняем текущую страницу
    context.user_data['search_page'] = page
    
    # ✅ Отправка или редактирование
    if edit:
        await safe_edit_message(message_or_query, text, reply_markup)
    else:
        await message_or_query.reply_text(text, reply_markup=reply_markup)

def get_chat_display_name(chat_id, owner_id, chat_info):
    """Вспомогательная функция для определения имени чата"""
    if chat_id == owner_id:
        return "💾 Избранное"
    elif chat_id < 0:
        if chat_info and chat_info.get('username'):
            return f"👥 @{chat_info['username']}"
        else:
            return f"👥 Группа"
    else:
        if chat_info and chat_info.get('username'):
            return f"👤 @{chat_info['username']}"
        else:
            return f"👤 User"


def get_chat_display_name(chat_id, owner_id, chat_info):
    """Вспомогательная функция для определения имени чата"""
    if chat_id == owner_id:
        return "💾 Избранное"
    elif chat_id < 0:
        if chat_info and chat_info.get('username'):
            return f"👥 @{chat_info['username']}"
        else:
            return f"👥 Группа"
    else:
        if chat_info and chat_info.get('username'):
            return f"👤 @{chat_info['username']}"
        else:
            return f"👤 User"


def get_media_folder_size():
    """Получить размер папки медиа"""
    total_size = 0
    file_count = 0
    
    if not os.path.exists(MEDIA_PATH):
        return {'size_mb': 0, 'files': 0}
    
    for root, dirs, files in os.walk(MEDIA_PATH):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                total_size += os.path.getsize(file_path)
                file_count += 1
            except:
                pass
    
    return {
        'size_mb': round(total_size / (1024 * 1024), 2),
        'files': file_count
    }


def format_size_mb(size_bytes):
    """Convert bytes to MB with 2 decimals."""
    return round(size_bytes / (1024 * 1024), 2)


def build_media_path_variants(file_path):
    """Build path variants to match media_path values stored in DB."""
    variants = set()
    if not file_path:
        return variants

    try:
        abs_path = os.path.abspath(file_path)
        rel_path = os.path.relpath(abs_path, os.getcwd())
    except Exception:
        abs_path = file_path
        rel_path = None

    base_values = [file_path, abs_path]
    if rel_path:
        base_values.extend([
            rel_path,
            f".{os.sep}{rel_path}",
            f"./{rel_path.replace('\\', '/')}",
            f".\\{rel_path.replace('/', '\\')}",
        ])

    for value in base_values:
        if not value:
            continue
        variants.add(value)
        variants.add(value.replace("\\", "/"))
        variants.add(value.replace("/", "\\"))

    return variants


def scan_old_media_files(days, sample_limit=5):
    """Scan MEDIA_PATH for files older than N days and build preview stats."""
    media_root = os.path.abspath(MEDIA_PATH)
    cutoff_ts = time.time() - days * 24 * 60 * 60

    result = {
        'days': days,
        'count': 0,
        'total_size': 0,
        'oldest_mtime': None,
        'oldest_path': None,
        'samples': [],
        'candidates': [],
        'unsafe_skipped': 0,
        'stat_errors': 0,
    }

    if not os.path.isdir(media_root):
        return result

    for root, _, files in os.walk(media_root):
        for filename in files:
            file_path = os.path.abspath(os.path.join(root, filename))

            if not is_safe_media_path(file_path):
                result['unsafe_skipped'] += 1
                print(f"[WARNING] scan_old_media_files: пропущен небезопасный путь {file_path}")
                continue

            try:
                file_stat = os.stat(file_path)
                mtime = file_stat.st_mtime
                size = file_stat.st_size
            except Exception as e:
                result['stat_errors'] += 1
                print(f"[ERROR] scan_old_media_files: ошибка stat для {file_path}: {e}")
                continue

            if mtime >= cutoff_ts:
                continue

            result['count'] += 1
            result['total_size'] += size
            result['candidates'].append({'path': file_path, 'size': size, 'mtime': mtime})

            if result['oldest_mtime'] is None or mtime < result['oldest_mtime']:
                result['oldest_mtime'] = mtime
                result['oldest_path'] = file_path

    result['candidates'].sort(key=lambda item: item['mtime'])

    for item in result['candidates'][:sample_limit]:
        try:
            relative_path = os.path.relpath(item['path'], media_root).replace("\\", "/")
        except Exception:
            relative_path = item['path']
        result['samples'].append(relative_path)

    return result


def cleanup_old_media_files(days):
    """Delete old files from MEDIA_PATH and nullify corresponding media_path values in DB."""
    scan_result = scan_old_media_files(days, sample_limit=5)

    deleted_count = 0
    skipped_count = 0
    error_count = 0
    freed_size = 0
    db_paths_cleared = 0

    cursor = db.conn.cursor()

    for item in scan_result['candidates']:
        file_path = item['path']
        file_size = item['size']

        if not is_safe_media_path(file_path):
            skipped_count += 1
            print(f"[WARNING] cleanup_old_media_files: пропущен небезопасный путь {file_path}")
            continue

        if not os.path.exists(file_path):
            skipped_count += 1
            continue

        try:
            os.remove(file_path)
            deleted_count += 1
            freed_size += file_size
        except FileNotFoundError:
            skipped_count += 1
            continue
        except Exception as e:
            error_count += 1
            print(f"[ERROR] cleanup_old_media_files: не удалось удалить {file_path}: {e}")
            continue

        try:
            for variant in build_media_path_variants(file_path):
                cursor.execute("UPDATE messages SET media_path = NULL WHERE media_path = ?", (variant,))
                db_paths_cleared += cursor.rowcount
        except Exception as e:
            error_count += 1
            print(f"[ERROR] cleanup_old_media_files: ошибка обновления БД для {file_path}: {e}")

    try:
        db.conn.commit()
    except Exception as e:
        error_count += 1
        print(f"[ERROR] cleanup_old_media_files: commit БД завершился с ошибкой: {e}")

    return {
        'days': days,
        'deleted_count': deleted_count,
        'skipped_count': skipped_count,
        'error_count': error_count,
        'freed_size': freed_size,
        'db_paths_cleared': db_paths_cleared,
        'scan_count': scan_result['count'],
        'scan_total_size': scan_result['total_size'],
        'oldest_mtime': scan_result['oldest_mtime'],
        'oldest_path': scan_result['oldest_path'],
        'samples': scan_result['samples'],
        'unsafe_skipped': scan_result['unsafe_skipped'],
        'stat_errors': scan_result['stat_errors'],
    }


def get_welcome_text():
    return (
        "Добро пожаловать!\n"
        "🕵️‍♂️ Этот бот создан, чтобы помогать вам в переписке.\n\n"
        "Возможности бота:\n"
        "• Моментально пришлёт уведомление, если ваш собеседник изменит или удалит сообщение 🔔\n"
        "• Умеет скачивать файлы с таймером, такие как: фото/видео/голосовые/кружки ⏳\n\n"
        "Как подключить бота — смотрите на картинке 👆\n"
        "@your_primary_bot || @your_backup_bot"
    )


def get_how_it_works_text():
    return (
        "Как работает бот:\n\n"
        "1. Подключите Telegram Business в настройках Telegram.\n"
        "2. Выберите этого бота в разделе Chatbot.\n"
        "3. После подключения бот отслеживает изменения и удаления сообщений.\n"
        "4. Для одноразовых медиа ответьте на сообщение триггером (например, `.`), и бот сохранит файл.\n\n"
        "Важно: бот платный. Для полноценной работы нужна активная подписка.\n"
        "Купить подписку можно кнопкой ниже.\n\n"
        "Есть реферальная программа: пригласите друга и получите бонусные дни после его первой оплаты."
    )


def get_plans_text():
    return (
        "💫 Тарифы (разовый доступ, без авто-продления):\n\n"
        "• 30 дней — 30 Stars\n"
        "• 90 дней — 80 Stars\n"
        "• 180 дней — 150 Stars\n\n"
        "После оплаты доступ активируется автоматически."
    )


def get_paysupport_text():
    return (
        "Поддержка по оплатам:\n"
        "Если оплата прошла, но доступ не активировался, отправьте:\n"
        "1) Ваш Telegram ID\n"
        "2) План\n"
        "3) Время оплаты\n"
        "4) `telegram_payment_charge_id` (если есть)\n\n"
        "Администратор проверит платёж и поможет восстановить доступ."
    )


def get_user_access_snapshot(user_id):
    if is_subscription_exempt(user_id):
        return {
            "status": "admin",
            "source": "admin",
            "plan_code": get_admin_role(user_id) or "admin",
            "expires_at": None,
            "days_left": 0,
            "is_active": True,
        }

    sub = db.get_active_subscription(user_id)
    if sub:
        expires_at = db._parse_dt(sub[4])
        return {
            "status": "active",
            "source": sub[6],
            "plan_code": sub[1] or "custom",
            "expires_at": expires_at,
            "days_left": get_subscription_days_left(sub),
            "is_active": True,
        }

    trial = db.get_active_trial(user_id)
    if trial:
        fake_row = (None, "trial", trial[1], trial[2], trial[3], 1, "trial", None, None, None)
        return {
            "status": "trial",
            "source": "trial",
            "plan_code": "trial",
            "expires_at": db._parse_dt(trial[3]),
            "days_left": get_subscription_days_left(fake_row),
            "is_active": True,
        }

    last_sub = db.get_subscription(user_id)
    trial_any = db.get_trial(user_id)
    expires_at = None
    if last_sub:
        expires_at = db._parse_dt(last_sub[4])
    elif trial_any:
        expires_at = db._parse_dt(trial_any[3])

    return {
        "status": "inactive",
        "source": (last_sub[6] if last_sub else ("trial" if trial_any else None)),
        "plan_code": (last_sub[1] if last_sub else ("trial" if trial_any else None)),
        "expires_at": expires_at,
        "days_left": 0,
        "is_active": False,
    }


def check_rate_limit(user_id, action_key):
    cfg = ANTI_SPAM_LIMITS.get(action_key)
    if not cfg:
        return True, 0
    used = db.count_anti_spam_events(user_id, action_key, cfg["window"])
    if used >= cfg["limit"]:
        return False, cfg["window"]
    db.add_anti_spam_event(user_id, action_key)
    return True, 0


def get_blacklist_message(entry):
    reason = (entry or {}).get("reason") or "без указания причины"
    blocked_until = (entry or {}).get("blocked_until")
    if blocked_until:
        return (
            "⛔ Доступ к боту ограничен.\n"
            f"Причина: {reason}\n"
            f"До: {blocked_until} UTC"
        )
    return (
        "⛔ Доступ к боту ограничен.\n"
        f"Причина: {reason}"
    )


async def ensure_not_blacklisted(user_id, target, context):
    blocked, entry = db.is_blacklisted(user_id)
    if not blocked:
        return True

    text = get_blacklist_message(entry)
    try:
        if hasattr(target, "reply_text"):
            await target.reply_text(text)
        elif hasattr(target, "answer"):
            await target.answer(text, show_alert=True)
        else:
            await context.bot.send_message(user_id, text)
    except Exception as e:
        print(f"[WARNING] Failed to send blacklist message user_id={user_id}: {e}")
    return False


def build_cabinet_keyboard(can_use_trial):
    rows = [
        [InlineKeyboardButton("💫 Продлить", callback_data="public_plans")],
    ]
    if can_use_trial:
        rows.append([InlineKeyboardButton("🎁 Пробный период", callback_data="public_trial_info")])
    rows.extend([
        [InlineKeyboardButton("🎟 Ввести промокод", callback_data="public_promo_enter")],
        [InlineKeyboardButton("🎁 Подарить подписку", callback_data="public_gift_start")],
        [InlineKeyboardButton("👥 Реферальная программа", callback_data="public_referral")],
        [InlineKeyboardButton("📜 История действий", callback_data="public_history")],
        [InlineKeyboardButton("◀️ Назад", callback_data="public_back_start")],
    ])
    return InlineKeyboardMarkup(rows)


async def build_cabinet_text(user_id, context):
    access = get_user_access_snapshot(user_id)
    expires_at = access["expires_at"].strftime("%Y-%m-%d %H:%M:%S") + " UTC" if access["expires_at"] else "—"
    trial_row = db.get_trial(user_id)
    trial_used = "да" if trial_row else "нет"

    bot_username = await get_bot_username_cached(context)
    ref_link = build_referral_link(bot_username, user_id) or "недоступна"
    ref_stats = db.get_user_referral_stats(user_id, recent_limit=3)
    user_stats = db.get_user_stats(user_id)
    total_messages = user_stats[1] if user_stats else 0
    deleted = user_stats[2] if user_stats else 0
    edited = user_stats[3] if user_stats else 0

    status_map = {
        "admin": "админ-доступ",
        "active": "активна",
        "trial": "пробный период",
        "inactive": "неактивна",
    }
    status_text = status_map.get(access["status"], access["status"])

    return (
        "👤 **Личный кабинет**\n\n"
        f"Статус доступа: **{status_text}**\n"
        f"Тариф/источник: `{access['plan_code'] or '-'} / {access['source'] or '-'}`\n"
        f"Действует до: {expires_at}\n"
        f"Осталось дней: {access['days_left']}\n"
        f"Trial уже использован: {trial_used}\n\n"
        f"👥 Рефералы: приглашено {ref_stats['invited_total']}, оплатили {ref_stats['paid_total']}\n"
        f"🎁 Реф. бонусов дней: {ref_stats['bonus_days_total']}\n"
        f"🔗 Ваша ссылка: {ref_link}\n\n"
        f"📊 Активность: сообщений {total_messages}, удалено {deleted}, изменено {edited}"
    )


def format_promo_apply_result(result):
    reason_map = {
        "not_found": "промокод не найден",
        "inactive": "промокод отключён",
        "not_started": "промокод ещё не активен",
        "expired": "срок действия промокода истёк",
        "global_limit_reached": "лимит активаций исчерпан",
        "user_limit_reached": "вы уже использовали этот промокод",
        "only_new_users": "промокод доступен только новым пользователям",
        "first_payment_only": "промокод доступен только до первой оплаты",
        "trial_conflict": "промокод нельзя применить при активном trial",
        "unsupported_type": "тип промокода не поддерживается",
    }
    if not result.get("ok"):
        return f"❌ Не удалось применить промокод: {reason_map.get(result.get('reason'), result.get('reason', 'неизвестная ошибка'))}"

    promo_type = result.get("promo_type")
    if promo_type == "bonus_days":
        expires_at = result.get("expires_at")
        until_text = expires_at.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if isinstance(expires_at, datetime) else "обновлено"
        return f"✅ Промокод применён. Подписка продлена.\nДействует до: {until_text}"
    if promo_type == "free_access":
        expires_at = result.get("expires_at")
        until_text = expires_at.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if isinstance(expires_at, datetime) else "обновлено"
        return f"✅ Промокод применён. Бесплатный доступ активирован.\nДействует до: {until_text}"
    if promo_type == "discount_percent":
        return "✅ Промокод применён. Скидка будет использована при следующей покупке тарифа."
    if promo_type == "fixed_price_override":
        return "✅ Промокод применён. Специальная цена будет использована при следующей покупке тарифа."
    if promo_type == "plan_override":
        return "✅ Промокод применён. Специальный тариф будет использован при следующей покупке."
    return "✅ Промокод применён."


def build_public_gift_plans_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("30 дней — 30 ⭐", callback_data="public_gift_plan_plan_30")],
        [InlineKeyboardButton("90 дней — 80 ⭐", callback_data="public_gift_plan_plan_90")],
        [InlineKeyboardButton("180 дней — 150 ⭐", callback_data="public_gift_plan_plan_180")],
        [InlineKeyboardButton("❌ Отмена", callback_data="public_gift_cancel")],
    ])


def build_public_gift_confirm_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить и оплатить", callback_data="public_gift_confirm")],
        [InlineKeyboardButton("◀️ Назад к тарифам", callback_data="public_gift_back_plans")],
        [InlineKeyboardButton("❌ Отмена", callback_data="public_gift_cancel")],
    ])


def build_public_history_keyboard(page, total_pages):
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("◀️", callback_data=f"public_history_page_{page - 1}"))
    buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"public_history_page_{page}"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("▶️", callback_data=f"public_history_page_{page + 1}"))
    rows = [buttons] if buttons else []
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")])
    return InlineKeyboardMarkup(rows)


def format_public_history_text(events, page, page_size):
    if not events:
        return "📜 История действий пока пуста."

    total = len(events)
    total_pages = max((total + page_size - 1) // page_size, 1)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = min(start + page_size, total)
    page_items = events[start:end]

    lines = [
        "📜 **История действий**",
        "",
        f"Событий: {total}",
        f"Страница: {page + 1}/{total_pages}",
        "",
    ]
    for dt_value, _event_type, text in page_items:
        when = dt_value.strftime("%d.%m %H:%M") if isinstance(dt_value, datetime) else str(dt_value)
        lines.append(f"• `{when}` — {text}")
    return "\n".join(lines)


def parse_bool_flag(value, default=False):
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in ("1", "true", "yes", "y", "да", "on"):
        return True
    if normalized in ("0", "false", "no", "n", "нет", "off"):
        return False
    return default


def parse_iso_datetime_or_none(value):
    raw = (value or "").strip()
    if not raw:
        return None
    candidate = raw.replace("T", " ")
    if len(candidate) == 10:
        candidate = f"{candidate} 00:00:00"
    if len(candidate) == 16:
        candidate = f"{candidate}:00"
    try:
        dt_value = datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return dt_value.strftime("%Y-%m-%d %H:%M:%S")


def parse_promo_settings(text):
    settings = {}
    cleaned = (text or "").strip()
    if not cleaned or cleaned == "-":
        return settings
    for chunk in cleaned.split():
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        settings[key.strip().lower()] = value.strip()
    return settings


def format_diagnostics_text(snapshot):
    uptime_seconds = int((utcnow_naive() - BOT_STARTED_AT).total_seconds())
    uptime_hours = uptime_seconds // 3600
    uptime_days = uptime_hours // 24
    uptime_remainder = uptime_hours % 24

    db_size = db.get_database_size()
    media_info = get_media_folder_size()
    media_writable = os.access(MEDIA_PATH, os.W_OK)
    db_exists = os.path.exists(DB_PATH)

    return (
        "🩺 **Диагностика бота**\n\n"
        f"Uptime: {uptime_days} д {uptime_remainder} ч\n"
        f"Пользователей: {snapshot['users_total']}\n"
        f"Активных подписок: {snapshot['subscriptions_active']}\n"
        f"Активных trial: {snapshot['trials_active']}\n"
        f"Скоро истекают (<=3д): {snapshot['expiring_soon']}\n"
        f"Истекли: {snapshot['expired']}\n"
        f"Активных business connections: {snapshot['business_connections_active']}\n"
        f"Оплат сегодня: {snapshot['paid_today']}\n"
        f"Реферальных связей: {snapshot['referrals_total']}\n"
        f"Активных промокодов: {snapshot['promo_active']}\n"
        f"В blacklist: {snapshot['blacklist_active']}\n\n"
        f"База данных: {db_size['size_mb']} MB\n"
        f"Media: {media_info['files']} файлов ({media_info['size_mb']} MB)\n"
        f"DB доступна: {'да' if db_exists else 'нет'}\n"
        f"Media writable: {'да' if media_writable else 'нет'}"
    )


def format_size_bytes(size_bytes):
    size = max(int(size_bytes or 0), 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    value = float(size)
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def format_user_hard_delete_preview(preview):
    user_id = preview.get("user_id")
    user_row = preview.get("user_row")
    delete_counts = preview.get("delete_counts", {}) or {}
    anonymize_counts = preview.get("anonymize_counts", {}) or {}
    subscription_row = preview.get("subscription_row")

    username = "-"
    first_name = "-"
    if user_row:
        username = f"@{user_row[1]}" if user_row[1] else "-"
        first_name = user_row[2] or "-"

    if subscription_row:
        sub_plan = subscription_row[0] or "-"
        sub_expires = subscription_row[1] or "-"
        sub_active = "да" if int(subscription_row[2] or 0) == 1 else "нет"
    else:
        sub_plan = "-"
        sub_expires = "-"
        sub_active = "нет"

    table_labels = {
        "users": "users",
        "messages": "messages",
        "edit_history": "edit_history",
        "user_stats": "user_stats",
        "business_connections": "business_connections",
        "subscriptions": "subscriptions",
        "subscription_grants": "subscription_grants",
        "star_payments": "star_payments",
        "trials": "trials",
        "subscription_reminders": "subscription_reminders",
        "activity_history": "activity_history",
        "referrals": "referrals",
        "referral_rewards": "referral_rewards",
        "referral_retry_audit": "referral_retry_audit",
        "gift_payments": "gift_payments",
        "promo_code_usages": "promo_code_usages",
        "promo_user_benefits": "promo_user_benefits",
        "blacklist": "blacklist",
        "anti_spam_events": "anti_spam_events",
        "team_member_permissions_v2": "team_member_permissions_v2",
        "team_scopes_v2": "team_scopes_v2",
        "team_member_roles_v2": "team_member_roles_v2",
        "admin_scopes": "admin_scopes",
        "admin_roles": "admin_roles",
        "admin_audit_log": "admin_audit_log",
    }

    non_zero_delete = [(table_labels.get(k, k), v) for k, v in delete_counts.items() if int(v or 0) > 0]
    non_zero_anonymize = [(k, v) for k, v in anonymize_counts.items() if int(v or 0) > 0]

    lines = [
        "🧨 **ПОЛНОЕ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ (PREVIEW)**",
        "",
        "⚠️ Действие необратимо. Будет выполнен hard delete связанных данных.",
        "",
        f"🆔 Telegram ID: `{user_id}`",
        f"👤 Username: {username}",
        f"📛 Имя: {first_name}",
        f"📦 Найден в users: {'да' if preview.get('user_found') else 'нет'}",
        "",
        f"💬 Сообщений владельца: {preview.get('owned_messages_count', 0)}",
        f"💬 Чатов владельца: {preview.get('owned_chats_count', 0)}",
        f"📁 Media кандидатов: {preview.get('media_total', 0)}",
        f"✅ Safe media: {preview.get('media_safe', 0)}",
        f"🔁 Shared media (будет пропущено): {preview.get('media_shared_skipped', 0)}",
        f"💾 Потенциально освободится: {format_size_bytes(preview.get('media_deletable_bytes', preview.get('media_bytes', 0)))}",
        "",
        f"💳 Подписка: {'есть' if subscription_row else 'нет'}",
        f"• active: {sub_active}",
        f"• plan: `{sub_plan}`",
        f"• expires_at: {sub_expires}",
        "",
        f"🗑 Всего записей к удалению: {preview.get('delete_total', 0)}",
    ]

    if non_zero_delete:
        lines.append("")
        lines.append("Удаляемые таблицы (ненулевые):")
        for table_name, count in non_zero_delete:
            lines.append(f"• `{table_name}`: {count}")
    else:
        lines.append("")
        lines.append("Удаляемые таблицы: нет данных для удаления.")

    if non_zero_anonymize:
        lines.append("")
        lines.append("Обезличивание shared-записей (без удаления):")
        for field_name, count in non_zero_anonymize:
            lines.append(f"• `{field_name}`: {count}")

    lines.extend([
        "",
        "Подтвердить полное удаление пользователя?",
    ])
    return "\n".join(lines)


def format_user_hard_delete_result(result):
    user_id = result.get("user_id")
    deleted_counts = result.get("deleted_counts", {}) or {}
    anonymized_counts = result.get("anonymized_counts", {}) or {}
    files = result.get("files", {}) or {}

    non_zero_delete = [(k, v) for k, v in deleted_counts.items() if int(v or 0) > 0]
    non_zero_anonymize = [(k, v) for k, v in anonymized_counts.items() if int(v or 0) > 0]

    lines = [
        "✅ **ПОЛНОЕ УДАЛЕНИЕ ЗАВЕРШЕНО**",
        "",
        f"🆔 Пользователь: `{user_id}`",
        f"🗑 Удалено записей: {result.get('deleted_total', 0)}",
        "",
        "📁 Media cleanup:",
        f"• кандидатов: {files.get('total_candidates', 0)}",
        f"• удалено: {files.get('deleted', 0)}",
        f"• пропущено (нет файла): {files.get('missing', 0)}",
        f"• пропущено (unsafe): {files.get('unsafe', 0)}",
        f"• пропущено (shared): {files.get('shared_skipped', 0)}",
        f"• ошибок: {files.get('errors_count', 0)}",
        f"• освобождено: {format_size_bytes(files.get('freed_bytes', 0))}",
    ]

    if non_zero_delete:
        lines.append("")
        lines.append("Удалено по таблицам:")
        for table_name, count in non_zero_delete:
            lines.append(f"• `{table_name}`: {count}")

    if non_zero_anonymize:
        lines.append("")
        lines.append("Обезличено в shared-таблицах:")
        for field_name, count in non_zero_anonymize:
            lines.append(f"• `{field_name}`: {count}")

    if files.get("errors_count", 0):
        lines.append("")
        lines.append("⚠️ Ошибки удаления файлов (первые 5):")
        for item in (files.get("errors") or [])[:5]:
            lines.append(f"• `{item.get('path', '-')}` -> {item.get('error', '-')}")

    if files.get("shared_skipped", 0):
        lines.append("")
        lines.append("ℹ️ Shared media (первые 5):")
        for item in (files.get("shared") or [])[:5]:
            lines.append(f"• `{item.get('path', '-')}` | refs={item.get('remaining_refs', 0)}")

    return "\n".join(lines)


def format_team_member_report(target_id):
    role = db.get_team_role_v2(target_id)
    perms = db.get_team_permissions_v2(target_id)
    scopes = db.get_team_scopes_v2(target_id)

    if not role or int(role[3]) != 1:
        return f"👤 `{target_id}`\nГибкая роль не назначена."

    lines = [
        f"👤 Пользователь: `{target_id}`",
        f"Роль-шаблон: `{role[1]}`",
        f"Custom permissions: {'да' if int(role[2]) == 1 else 'нет'}",
        "",
        "Permissions:",
    ]
    if perms:
        for perm in perms:
            lines.append(f"• `{perm}`")
    else:
        lines.append("• нет")

    lines.append("")
    lines.append("Scopes:")
    if scopes:
        for scope in scopes:
            scope_type = scope[2]
            owner_id = scope[3]
            chat_id = scope[4]
            if scope_type == "global":
                lines.append("• global")
            elif owner_id is not None and chat_id is None:
                lines.append(f"• owner `{owner_id}` (все чаты)")
            else:
                lines.append(f"• owner `{owner_id}` + chat `{chat_id}`")
    else:
        lines.append("• пусто")
    return "\n".join(lines)


def _can_send_subscription_notice(user_id):
    now_ts = time.time()
    last_ts = _subscription_block_notice_ts.get(user_id, 0)
    if now_ts - last_ts < SUBSCRIPTION_BLOCK_TTL_SECONDS:
        return False
    _subscription_block_notice_ts[user_id] = now_ts
    return True


async def send_subscription_block_message(target, context):
    text = (
        "Доступ к этой функции недоступен.\n"
        "Нужна активная подписка.\n\n"
        "Вы можете:\n"
        "• посмотреть, как работает бот\n"
        "• купить или продлить подписку"
    )
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Как работает бот", callback_data="public_how_it_works")],
        [InlineKeyboardButton("💫 Купить подписку", callback_data="public_plans")]
    ])
    try:
        if hasattr(target, "reply_text"):
            await target.reply_text(text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(target, text, reply_markup=reply_markup)
    except Exception as e:
        print(f"[WARNING] Failed to send subscription block message: {e}")


async def maybe_send_expiry_notice(user_id, context):
    if is_subscription_exempt(user_id):
        return
    access = get_user_access_snapshot(user_id)
    if not access["is_active"] or access["status"] not in ("active", "trial"):
        return
    days_left = access["days_left"]
    if days_left not in REMINDER_DAYS_BEFORE:
        return

    expires_at = access["expires_at"]
    if not expires_at:
        return

    reminder_kind = f"{access['status']}_{days_left}"
    if db.was_reminder_sent(user_id, reminder_kind, expires_at):
        return

    text = (
        f"Напоминание: ваш {'пробный период' if access['status'] == 'trial' else 'доступ'} истекает через {days_left} дн."
        if days_left > 0
        else f"Ваш {'пробный период' if access['status'] == 'trial' else 'доступ'} истекает сегодня. Продлите доступ, чтобы продолжить работу."
    )
    try:
        await context.bot.send_message(
            user_id,
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💫 Продлить подписку", callback_data="public_plans")]
            ])
        )
        db.mark_reminder_sent(user_id, reminder_kind, expires_at)
    except Exception as e:
        print(f"[WARNING] Failed to send expiry notice to {user_id}: {e}")


async def process_expiry_reminders(context):
    candidates = db.get_expiring_access_candidates()
    now = utcnow_naive()
    for user_id, _plan_code, expires_at_raw, source, access_kind in candidates:
        expires_at = db._parse_dt(expires_at_raw)
        if not expires_at:
            continue
        days_left = max((expires_at - now).days, 0)
        if days_left not in REMINDER_DAYS_BEFORE:
            continue
        reminder_kind = f"{access_kind}_{days_left}"
        if db.was_reminder_sent(user_id, reminder_kind, expires_at):
            continue
        text = (
            f"Напоминание: ваш {'пробный период' if access_kind == 'trial' else 'доступ'} истекает через {days_left} дн."
            if days_left > 0
            else f"Ваш {'пробный период' if access_kind == 'trial' else 'доступ'} истекает сегодня. Продлите доступ."
        )
        try:
            await context.bot.send_message(
                user_id,
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💫 Продлить", callback_data="public_plans")]])
            )
            db.mark_reminder_sent(user_id, reminder_kind, expires_at)
        except Exception as e:
            print(f"[WARNING] reminder send failed user_id={user_id}: {e}")


async def require_paid_access(update, context, user_id, notify=False, target_message=None):
    if not await ensure_not_blacklisted(user_id, target_message or user_id, context):
        return False
    if has_active_subscription(user_id):
        await maybe_send_expiry_notice(user_id, context)
        return True

    if notify and _can_send_subscription_notice(user_id):
        if target_message is not None:
            await send_subscription_block_message(target_message, context)
        else:
            await send_subscription_block_message(user_id, context)
    return False


def get_admin_main_keyboard(role=None):
    role = role or "admin"
    keyboard = [
        [
            InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
            InlineKeyboardButton("📊 Статистика", callback_data="admin_stats_menu")
        ],
        [
            InlineKeyboardButton("🗓 Сегодня", callback_data="admin_today"),
            InlineKeyboardButton("📆 Выбрать дату", callback_data="admin_dates")
        ],
        [
            InlineKeyboardButton("⏱ По времени", callback_data="admin_time_filters"),
            InlineKeyboardButton("🗑 Удалённые", callback_data="admin_deleted")
        ],
        [
            InlineKeyboardButton("📁 Медиа", callback_data="admin_media_menu"),
            InlineKeyboardButton("⚡ Активность", callback_data="admin_activity")
        ],
        [
            InlineKeyboardButton("📦 Архивация", callback_data="admin_archive_menu"),
            InlineKeyboardButton("🔍 Поиск", callback_data="admin_search_menu")
        ],
        [
            InlineKeyboardButton("💳 Подписки", callback_data="admin_subscriptions_menu"),
            InlineKeyboardButton("🎁 Рефералы", callback_data="admin_referrals_menu")
        ],
        [
            InlineKeyboardButton("🎟 Промокоды", callback_data="admin_promocodes_menu"),
            InlineKeyboardButton("⛔ ЧС/Антиспам", callback_data="admin_blacklist_menu")
        ],
        [
            InlineKeyboardButton("👮 Роли и доступы", callback_data="admin_roles_menu"),
            InlineKeyboardButton("🩺 Диагностика", callback_data="admin_diagnostics_menu")
        ],
    ]
    if role == "superadmin":
        keyboard.append([InlineKeyboardButton("🧨 Полное удаление пользователя", callback_data="admin_user_hard_delete_start")])
    keyboard.append([InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")])
    if role == "admin_lite":
        # hide dangerous sections from lite UI, keep subscription operations available
        keyboard = [
            keyboard[0],
            [InlineKeyboardButton("📁 Медиа", callback_data="admin_media_menu")],
            [InlineKeyboardButton("🔍 Поиск", callback_data="admin_search_menu")],
            [InlineKeyboardButton("💳 Подписки", callback_data="admin_subscriptions_menu")],
            [InlineKeyboardButton("🩺 Диагностика", callback_data="admin_diagnostics_menu")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")]
        ]
    return InlineKeyboardMarkup(keyboard)


async def notify_admins(context, text):
    """Отправить уведомление всем админам"""
    await send_admin_notification(context.bot, text)


async def on_startup(application):
    """Уведомление при запуске бота"""
    startup_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    text = f"""🟢 **БОТ ЗАПУЩЕН**

⏰ Время: {startup_time}
📊 База: {DB_PATH}
📁 Медиа: {MEDIA_PATH}
👥 Админы: {len(get_configured_admin_ids())}
"""
    await send_admin_notification(application.bot, text)

    job_queue = getattr(application, "job_queue", None)
    if job_queue is not None:
        try:
            job_queue.run_repeating(
                process_expiry_reminders,
                interval=60 * 60,
                first=30,
                name="expiry_reminders",
            )
            print("[INFO] Expiry reminders job registered (interval=1h)")
        except Exception as e:
            print(f"[WARNING] Failed to register expiry reminders job: {e}")
    else:
        print("[WARNING] JobQueue недоступен, periodic reminders отключены")


async def on_shutdown(application):
    """Уведомление при остановке бота"""
    shutdown_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')

    stats = db.get_all_stats()
    db_info = db.get_database_size()
    media_info = get_media_folder_size()
    total_messages = stats[1] if stats and len(stats) > 1 else 0
    total_deleted = stats[2] if stats and len(stats) > 2 else 0
    total_edited = stats[3] if stats and len(stats) > 3 else 0
    total_media = stats[4] if stats and len(stats) > 4 else 0

    text = f"""🔴 **БОТ ОСТАНОВЛЕН**

⏰ Время: {shutdown_time}

📊 **Финальная статистика:**
💬 Всего сообщений: {total_messages}
🗑 Удалено: {total_deleted}
✏️ Изменено: {total_edited}
📎 С медиа: {total_media}

🗄 **Хранилище:**
💾 База: {db_info['size_mb']} MB
📁 Медиа: {media_info['files']} файлов ({media_info['size_mb']} MB)
"""

    await send_admin_notification(application.bot, text)

async def handle_search_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений для поиска"""
    message = update.effective_message
    if message is None or message.text is None:
        return

    search_text = message.text.strip()

    user = update.effective_user
    if user is None:
        return
    user_id = user.id

    # Public text-input flows (promo/gift) for non-admin users
    public_action = context.user_data.get("awaiting_public_action")
    if public_action and not is_admin(user_id):
        if not await ensure_not_blacklisted(user_id, message, context):
            return

        if public_action == "promo_code_input":
            code = search_text.upper()
            if not PROMOCODE_ALLOWED_RE.fullmatch(code):
                await message.reply_text("❌ Неверный формат промокода. Используйте только буквы/цифры/`_`/`-` (3-32).")
                return

            promo_result = db.apply_promo_code(user_id=user_id, code=code, actor_id=user_id)
            await message.reply_text(
                format_promo_apply_result(promo_result),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 В кабинет", callback_data="public_cabinet")]]),
            )
            if promo_result.get("ok"):
                db.log_activity(user_id, "promo_applied", f"Применён промокод {code}", {"code": code, "promo_type": promo_result.get("promo_type")})
                clear_public_flow(context)
            return

        if public_action == "gift_recipient_input":
            recipient_id = parse_telegram_id(search_text)
            if recipient_id is None:
                await message.reply_text("❌ Неверный Telegram ID получателя.")
                return
            if recipient_id == user_id:
                await message.reply_text("❌ Дарить подписку самому себе нельзя. Введите другой Telegram ID.")
                return
            context.user_data["gift_recipient_id"] = recipient_id
            context.user_data["gift_recipient_label"] = str(recipient_id)
            context.user_data["awaiting_public_action"] = "gift_plan_select"
            await message.reply_text(
                f"🎁 Получатель: `{recipient_id}`\nВыберите тариф подарка:",
                reply_markup=build_public_gift_plans_keyboard(),
            )
            return

        clear_public_flow(context)
        await message.reply_text("⚠️ Сессия действия устарела. Откройте нужный раздел заново.")
        return

    if not is_admin(user_id):
        return

    admin_input_action = context.user_data.get("awaiting_admin_action")
    if admin_input_action:
        if admin_input_action == "hard_delete_user_id":
            if not is_superadmin(user_id):
                clear_admin_flow(context)
                await message.reply_text("❌ Полное удаление пользователя доступно только superadmin.")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID. Введите положительный числовой ID.")
                return
            if target_id == get_superadmin_id():
                await message.reply_text("❌ Нельзя удалить superadmin из базы.")
                return

            preview = db.get_user_hard_delete_preview(target_id, media_root=MEDIA_PATH)
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["admin_flow_preview"] = preview
            context.user_data["awaiting_admin_action"] = "hard_delete_confirm"
            await message.reply_text(
                format_user_hard_delete_preview(preview),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить удаление", callback_data="hard_delete_user_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="hard_delete_user_cancel")],
                ]),
            )
            return

        if admin_input_action == "promo_create_code":
            code = search_text.upper()
            if not PROMOCODE_ALLOWED_RE.fullmatch(code):
                await message.reply_text("❌ Неверный код. Формат: буквы/цифры/`_`/`-`, длина 3-32.")
                return
            context.user_data["admin_flow_code"] = code
            context.user_data["awaiting_admin_action"] = "promo_create_type"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("bonus_days", callback_data="promo_create_type_bonus_days")],
                [InlineKeyboardButton("free_access", callback_data="promo_create_type_free_access")],
                [InlineKeyboardButton("discount_percent", callback_data="promo_create_type_discount_percent")],
                [InlineKeyboardButton("fixed_price_override", callback_data="promo_create_type_fixed_price_override")],
                [InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")],
            ])
            await message.reply_text(f"Код: `{code}`\nВыберите тип промокода:", reply_markup=keyboard)
            return

        if admin_input_action == "promo_create_value":
            promo_type = context.user_data.get("admin_flow_type")
            if promo_type in ("bonus_days", "free_access"):
                try:
                    value = int(search_text)
                except ValueError:
                    await message.reply_text("❌ Введите целое число дней.")
                    return
                if value <= 0 or value > 3650:
                    await message.reply_text("❌ Дни должны быть в диапазоне 1..3650.")
                    return
                context.user_data["admin_flow_value"] = value
            elif promo_type == "discount_percent":
                try:
                    value = int(search_text)
                except ValueError:
                    await message.reply_text("❌ Введите целый процент.")
                    return
                if value <= 0 or value >= 100:
                    await message.reply_text("❌ Скидка должна быть в диапазоне 1..99.")
                    return
                context.user_data["admin_flow_value"] = value
            elif promo_type == "fixed_price_override":
                parts = search_text.split()
                if len(parts) != 3:
                    await message.reply_text("❌ Введите три значения через пробел: `price30 price90 price180`.")
                    return
                try:
                    values = [int(x) for x in parts]
                except ValueError:
                    await message.reply_text("❌ Все цены должны быть целыми числами.")
                    return
                if any(v <= 0 for v in values):
                    await message.reply_text("❌ Цены должны быть больше 0.")
                    return
                context.user_data["admin_flow_value"] = values
            else:
                clear_admin_flow(context)
                await message.reply_text("⚠️ Неизвестный тип промокода. Начните заново.")
                return

            context.user_data["awaiting_admin_action"] = "promo_create_comment"
            await message.reply_text("Введите комментарий (или `-`):")
            return

        if admin_input_action == "promo_create_comment":
            comment = search_text if search_text not in ("", "-") else ""
            context.user_data["admin_flow_comment"] = comment
            context.user_data["awaiting_admin_action"] = "promo_create_settings"
            await message.reply_text(
                "Введите доп.настройки через пробел в формате `key=value` или `-`.\n"
                "Ключи: starts, expires, max, per_user, only_new, first_payment, with_trial, with_bonus"
            )
            return

        if admin_input_action == "promo_create_settings":
            settings = parse_promo_settings(search_text)
            context.user_data["admin_flow_settings"] = settings
            context.user_data["awaiting_admin_action"] = "promo_create_confirm"
            code = context.user_data.get("admin_flow_code")
            promo_type = context.user_data.get("admin_flow_type")
            value = context.user_data.get("admin_flow_value")
            await message.reply_text(
                (
                    "Подтвердите создание промокода:\n"
                    f"• code: `{code}`\n"
                    f"• type: `{promo_type}`\n"
                    f"• value: `{value}`\n"
                    f"• settings: `{settings}`"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Создать", callback_data="promo_create_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")],
                ]),
            )
            return

        if admin_input_action == "promo_toggle_code":
            code = search_text.upper()
            promo = db.get_promo_code(code)
            if not promo:
                await message.reply_text("❌ Промокод не найден.")
                return
            context.user_data["admin_flow_code"] = code
            context.user_data["awaiting_admin_action"] = "promo_toggle_confirm"
            current_active = int(promo[18]) == 1
            await message.reply_text(
                f"Промокод `{code}` сейчас {'активен' if current_active else 'выключен'}.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Включить", callback_data="promo_toggle_apply_on")],
                    [InlineKeyboardButton("⛔ Выключить", callback_data="promo_toggle_apply_off")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")],
                ]),
            )
            return

        if admin_input_action == "promo_stats_code":
            code = search_text.upper()
            promo = db.get_promo_code(code)
            if not promo:
                await message.reply_text("❌ Промокод не найден.")
                return
            stats = db.get_promo_usage_stats(code)
            clear_admin_flow(context)
            await message.reply_text(
                (
                    f"🎟 Промокод `{code}`\n"
                    f"Тип: {promo[2]}\n"
                    f"Активен: {'да' if int(promo[18]) == 1 else 'нет'}\n"
                    f"Использований: {stats['total_uses'] if stats else 0}\n"
                    f"Пользователей: {stats['users'] if stats else 0}\n"
                    f"Лимит: {promo[12] or 0} / per user: {promo[13] or 1}"
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_promocodes_menu")]]),
            )
            return

        if admin_input_action == "promo_edit_code":
            code = search_text.upper()
            promo = db.get_promo_code(code)
            if not promo:
                await message.reply_text("❌ Промокод не найден.")
                return
            context.user_data["admin_flow_code"] = code
            context.user_data["awaiting_admin_action"] = "promo_edit_settings"
            await message.reply_text(
                "Введите поля для изменения в формате `key=value` через пробел.\n"
                "Ключи: starts, expires, max, per_user, only_new, first_payment, with_trial, with_bonus, comment",
            )
            return

        if admin_input_action == "promo_edit_settings":
            settings = parse_promo_settings(search_text)
            if not settings:
                await message.reply_text("❌ Нет валидных настроек для обновления.")
                return
            code = context.user_data.get("admin_flow_code")
            update_fields = {}
            if "starts" in settings:
                starts = parse_iso_datetime_or_none(settings["starts"])
                if starts is None:
                    await message.reply_text("❌ Неверный формат starts (ожидается YYYY-MM-DD или YYYY-MM-DDTHH:MM).")
                    return
                update_fields["starts_at"] = starts
            if "expires" in settings:
                expires = parse_iso_datetime_or_none(settings["expires"])
                if expires is None:
                    await message.reply_text("❌ Неверный формат expires (ожидается YYYY-MM-DD или YYYY-MM-DDTHH:MM).")
                    return
                update_fields["expires_at"] = expires
            if "max" in settings:
                update_fields["max_activations"] = max(0, int(settings["max"]))
            if "per_user" in settings:
                update_fields["per_user_limit"] = max(1, int(settings["per_user"]))
            if "only_new" in settings:
                update_fields["only_new_users"] = 1 if parse_bool_flag(settings["only_new"]) else 0
            if "first_payment" in settings:
                update_fields["first_payment_only"] = 1 if parse_bool_flag(settings["first_payment"]) else 0
            if "with_trial" in settings:
                update_fields["allow_with_trial"] = 1 if parse_bool_flag(settings["with_trial"], default=True) else 0
            if "with_bonus" in settings:
                update_fields["allow_with_other_bonus"] = 1 if parse_bool_flag(settings["with_bonus"], default=True) else 0
            if "comment" in settings:
                update_fields["comment"] = settings["comment"]
            if not update_fields:
                await message.reply_text("❌ Нет валидных полей для обновления.")
                return
            changed = db.update_promo_code(code, **update_fields)
            clear_admin_flow(context)
            await message.reply_text(
                f"{'✅ Промокод обновлён.' if changed else '❌ Не удалось обновить промокод.'}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_promocodes_menu")]]),
            )
            return

        if admin_input_action == "blacklist_add_user":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "blacklist_add_reason"
            await message.reply_text("Введите причину блокировки (или `-`):")
            return

        if admin_input_action == "blacklist_add_reason":
            context.user_data["admin_flow_reason"] = search_text if search_text not in ("", "-") else "без причины"
            context.user_data["awaiting_admin_action"] = "blacklist_add_hours"
            await message.reply_text("Введите срок блокировки в часах (0 = бессрочно):")
            return

        if admin_input_action == "blacklist_add_hours":
            try:
                hours = int(search_text)
            except ValueError:
                await message.reply_text("❌ Введите целое число часов.")
                return
            if hours < 0:
                await message.reply_text("❌ Срок не может быть отрицательным.")
                return
            context.user_data["admin_flow_hours"] = hours
            context.user_data["awaiting_admin_action"] = "blacklist_add_confirm"
            target_id = context.user_data.get("admin_flow_target_id")
            reason = context.user_data.get("admin_flow_reason", "без причины")
            until_text = "бессрочно" if hours == 0 else f"{hours} ч"
            await message.reply_text(
                f"Подтвердите блокировку:\n• user_id: `{target_id}`\n• причина: {reason}\n• срок: {until_text}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="blacklist_add_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_blacklist_menu")],
                ]),
            )
            return

        if admin_input_action == "blacklist_remove_user":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "blacklist_remove_confirm"
            await message.reply_text(
                f"Снять блокировку с `{target_id}`?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Снять", callback_data="blacklist_remove_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_blacklist_menu")],
                ]),
            )
            return

        if admin_input_action == "team_assign_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "team_assign_pick"
            rows = [
                [InlineKeyboardButton("manager", callback_data="team_assign_pick_manager"), InlineKeyboardButton("support", callback_data="team_assign_pick_support")],
                [InlineKeyboardButton("analyst", callback_data="team_assign_pick_analyst"), InlineKeyboardButton("viewer", callback_data="team_assign_pick_viewer")],
                [InlineKeyboardButton("custom", callback_data="team_assign_pick_custom"), InlineKeyboardButton("admin", callback_data="team_assign_pick_admin")],
                [InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")],
            ]
            await message.reply_text(f"Выберите роль для `{target_id}`:", reply_markup=InlineKeyboardMarkup(rows))
            return

        if admin_input_action == "team_member_view_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            clear_admin_flow(context)
            await message.reply_text(
                format_team_member_report(target_id),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="team_roles_menu")]]),
            )
            return

        if admin_input_action == "team_scope_owner_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "team_scope_owner_owner"
            await message.reply_text("Введите owner_id для доступа ко всем чатам владельца:")
            return

        if admin_input_action == "team_scope_owner_owner":
            owner_id = parse_telegram_id(search_text)
            if owner_id is None:
                await message.reply_text("❌ Неверный owner_id.")
                return
            context.user_data["admin_flow_owner_id"] = owner_id
            context.user_data["awaiting_admin_action"] = "team_scope_owner_confirm"
            await message.reply_text(
                (
                    f"Подтвердите scope owner-wide:\n"
                    f"• сотрудник: `{context.user_data.get('admin_flow_target_id')}`\n"
                    f"• owner_id: `{owner_id}`"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="team_scope_owner_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")],
                ]),
            )
            return

        if admin_input_action == "team_scope_chat_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "team_scope_chat_owner"
            await message.reply_text("Введите owner_id:")
            return

        if admin_input_action == "team_scope_chat_owner":
            owner_id = parse_telegram_id(search_text)
            if owner_id is None:
                await message.reply_text("❌ Неверный owner_id.")
                return
            context.user_data["admin_flow_owner_id"] = owner_id
            context.user_data["awaiting_admin_action"] = "team_scope_chat_chat"
            await message.reply_text("Введите chat_id:")
            return

        if admin_input_action == "team_scope_chat_chat":
            chat_id = parse_chat_id(search_text)
            if chat_id is None:
                await message.reply_text("❌ Неверный chat_id.")
                return
            context.user_data["admin_flow_chat_id"] = chat_id
            context.user_data["awaiting_admin_action"] = "team_scope_chat_confirm"
            await message.reply_text(
                (
                    f"Подтвердите scope owner+chat:\n"
                    f"• сотрудник: `{context.user_data.get('admin_flow_target_id')}`\n"
                    f"• owner_id: `{context.user_data.get('admin_flow_owner_id')}`\n"
                    f"• chat_id: `{chat_id}`"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="team_scope_chat_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")],
                ]),
            )
            return

        if admin_input_action == "team_scope_clear_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "team_scope_clear_confirm"
            await message.reply_text(
                f"Удалить все scopes у `{target_id}`?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Удалить", callback_data="team_scope_clear_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")],
                ]),
            )
            return

        if admin_input_action == "team_custom_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "team_custom_permissions"
            await message.reply_text(
                "Введите permissions через запятую.\n"
                f"Доступные ключи: {', '.join(TEAM_PERMISSION_KEYS)}\n"
                "Пример: `view_users,view_chats,manage_subscriptions`",
            )
            return

        if admin_input_action == "team_custom_permissions":
            target_id = context.user_data.get("admin_flow_target_id")
            if target_id is None:
                clear_admin_flow(context)
                await message.reply_text("⚠️ Сессия устарела. Повторите.")
                return
            perms = [p.strip() for p in search_text.split(",") if p.strip()]
            invalid = [p for p in perms if p not in TEAM_PERMISSION_KEYS]
            if invalid:
                await message.reply_text(f"❌ Неизвестные permissions: {', '.join(invalid)}")
                return
            context.user_data["admin_flow_value"] = perms
            context.user_data["awaiting_admin_action"] = "team_custom_confirm"
            await message.reply_text(
                f"Подтвердите custom permissions для `{target_id}`:\n{', '.join(perms) if perms else '(пусто)'}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="team_custom_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")],
                ]),
            )
            return

        if admin_input_action == "team_remove_target":
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID.")
                return
            context.user_data["admin_flow_target_id"] = target_id
            context.user_data["awaiting_admin_action"] = "team_remove_confirm"
            await message.reply_text(
                f"Снять гибкую роль и scopes у `{target_id}`?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Снять роль", callback_data="team_remove_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")],
                ]),
            )
            return

        if admin_input_action in (
            "promo_create_type",
            "promo_create_confirm",
            "promo_toggle_confirm",
            "blacklist_add_confirm",
            "blacklist_remove_confirm",
            "team_assign_pick",
            "team_scope_owner_confirm",
            "team_scope_chat_confirm",
            "team_scope_clear_confirm",
            "team_custom_confirm",
            "team_remove_confirm",
            "hard_delete_confirm",
        ):
            await message.reply_text("Используйте кнопки подтверждения или нажмите «Отмена».")
            return

        clear_admin_flow(context)
        await message.reply_text("⚠️ Сессия действия устарела. Откройте раздел заново.")
        return

    role_action = context.user_data.get("awaiting_role_action")
    if role_action:
        if role_action == "role_assign_target":
            if not can_manage_roles(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ У вас нет прав на назначение ролей")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            if target_id == get_superadmin_id():
                await message.reply_text("❌ Нельзя менять роль superadmin")
                return
            context.user_data["role_flow_target_id"] = target_id
            context.user_data["awaiting_role_action"] = "role_assign_pick"
            actor_role = get_actor_role(user_id)
            role_buttons = []
            if actor_role == "superadmin":
                role_buttons.append(InlineKeyboardButton("admin", callback_data="role_assign_pick_admin"))
            role_buttons.append(InlineKeyboardButton("admin_lite", callback_data="role_assign_pick_admin_lite"))
            await message.reply_text(
                f"Пользователь: `{target_id}`\nВыберите роль:",
                reply_markup=InlineKeyboardMarkup([
                    role_buttons,
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action == "role_view_target":
            if not can_manage_roles(user_id) and not can_manage_scopes(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ Недостаточно прав для просмотра ролей и scope")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            report = format_admin_access_report(target_id)
            clear_role_flow(context)
            await message.reply_text(
                report,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]
                ]),
            )
            return

        if role_action == "role_revoke_target":
            if not can_manage_roles(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ У вас нет прав на снятие ролей")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            if not can_manage_roles(user_id, target_role=None, target_user_id=target_id):
                clear_role_flow(context)
                await message.reply_text("❌ Нельзя снять роль у этого пользователя")
                return
            context.user_data["role_flow_target_id"] = target_id
            context.user_data["awaiting_role_action"] = "role_revoke_confirm"
            await message.reply_text(
                f"Снять роль и удалить scope у `{target_id}`?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="role_revoke_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action == "scope_add_owner_target":
            if not can_manage_scopes(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ У вас нет прав на управление scope")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            if not can_manage_scopes(user_id, target_id):
                clear_role_flow(context)
                await message.reply_text("❌ Scope можно назначать только пользователю с ролью admin_lite")
                return
            context.user_data["role_flow_target_id"] = target_id
            context.user_data["awaiting_role_action"] = "scope_add_owner_owner"
            await message.reply_text("Введите owner_id, к которому нужен доступ ко всем чатам:")
            return

        if role_action == "scope_add_owner_owner":
            owner_id = parse_telegram_id(search_text)
            if owner_id is None:
                await message.reply_text("❌ Неверный owner_id")
                return
            context.user_data["role_flow_owner_id"] = owner_id
            context.user_data["awaiting_role_action"] = "scope_add_owner_confirm"
            await message.reply_text(
                (
                    "Подтвердите выдачу доступа:\n"
                    f"• сотрудник: `{context.user_data.get('role_flow_target_id')}`\n"
                    f"• owner_id: `{owner_id}`\n"
                    "• доступ: все чаты пользователя"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="scope_add_owner_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action == "scope_add_chat_target":
            if not can_manage_scopes(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ У вас нет прав на управление scope")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            if not can_manage_scopes(user_id, target_id):
                clear_role_flow(context)
                await message.reply_text("❌ Scope можно назначать только пользователю с ролью admin_lite")
                return
            context.user_data["role_flow_target_id"] = target_id
            context.user_data["awaiting_role_action"] = "scope_add_chat_owner"
            await message.reply_text("Введите owner_id пользователя:")
            return

        if role_action == "scope_add_chat_owner":
            owner_id = parse_telegram_id(search_text)
            if owner_id is None:
                await message.reply_text("❌ Неверный owner_id")
                return
            context.user_data["role_flow_owner_id"] = owner_id
            context.user_data["awaiting_role_action"] = "scope_add_chat_chat"
            await message.reply_text("Введите chat_id, к которому нужен доступ:")
            return

        if role_action == "scope_add_chat_chat":
            chat_id = parse_chat_id(search_text)
            if chat_id is None:
                await message.reply_text("❌ Неверный chat_id")
                return
            context.user_data["role_flow_chat_id"] = chat_id
            context.user_data["awaiting_role_action"] = "scope_add_chat_confirm"
            await message.reply_text(
                (
                    "Подтвердите выдачу доступа:\n"
                    f"• сотрудник: `{context.user_data.get('role_flow_target_id')}`\n"
                    f"• owner_id: `{context.user_data.get('role_flow_owner_id')}`\n"
                    f"• chat_id: `{chat_id}`\n"
                    "• доступ: только этот чат"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="scope_add_chat_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action == "scope_remove_one_target":
            if not can_manage_scopes(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ У вас нет прав на управление scope")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            if not can_manage_scopes(user_id, target_id):
                clear_role_flow(context)
                await message.reply_text("❌ Scope можно удалять только у пользователя с ролью admin_lite")
                return
            context.user_data["role_flow_target_id"] = target_id
            context.user_data["awaiting_role_action"] = "scope_remove_one_owner"
            await message.reply_text("Введите owner_id, из которого нужно удалить доступ:")
            return

        if role_action == "scope_remove_one_owner":
            owner_id = parse_telegram_id(search_text)
            if owner_id is None:
                await message.reply_text("❌ Неверный owner_id")
                return
            context.user_data["role_flow_owner_id"] = owner_id
            context.user_data["awaiting_role_action"] = "scope_remove_one_mode"
            await message.reply_text(
                "Что удалить?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Все чаты owner", callback_data="scope_remove_one_mode_owner")],
                    [InlineKeyboardButton("Один chat_id", callback_data="scope_remove_one_mode_chat")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action == "scope_remove_one_chat":
            chat_id = parse_chat_id(search_text)
            if chat_id is None:
                await message.reply_text("❌ Неверный chat_id")
                return
            context.user_data["role_flow_chat_id"] = chat_id
            context.user_data["awaiting_role_action"] = "scope_remove_one_confirm"
            await message.reply_text(
                (
                    "Подтвердите удаление scope:\n"
                    f"• сотрудник: `{context.user_data.get('role_flow_target_id')}`\n"
                    f"• owner_id: `{context.user_data.get('role_flow_owner_id')}`\n"
                    f"• chat_id: `{chat_id}`"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="scope_remove_one_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action == "scope_remove_all_target":
            if not can_manage_scopes(user_id):
                clear_role_flow(context)
                await message.reply_text("❌ У вас нет прав на управление scope")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            if not can_manage_scopes(user_id, target_id):
                clear_role_flow(context)
                await message.reply_text("❌ Scope можно удалять только у пользователя с ролью admin_lite")
                return
            context.user_data["role_flow_target_id"] = target_id
            context.user_data["awaiting_role_action"] = "scope_remove_all_confirm"
            await message.reply_text(
                f"Удалить все scope у `{target_id}`?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="scope_remove_all_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
                ]),
            )
            return

        if role_action in (
            "role_assign_pick",
            "role_assign_confirm",
            "role_revoke_confirm",
            "scope_add_owner_confirm",
            "scope_add_chat_confirm",
            "scope_remove_one_mode",
            "scope_remove_one_confirm",
            "scope_remove_all_confirm",
        ):
            await message.reply_text("Используйте кнопки подтверждения или нажмите «Отмена».")
            return

        clear_role_flow(context)
        await message.reply_text("⚠️ Сессия управления ролями устарела. Откройте раздел заново.")
        return

    subscription_action = context.user_data.get("awaiting_subscription_action")
    if subscription_action:
        if subscription_action.startswith("sub_grant_target_"):
            if not can_grant_subscriptions(user_id):
                clear_subscription_flow(context)
                await message.reply_text("❌ Недостаточно прав для выдачи подписки")
                return
            days = int(subscription_action.split("_")[-1])
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            context.user_data["sub_flow_target_id"] = target_id
            context.user_data["awaiting_subscription_action"] = f"sub_grant_comment_{days}"
            await message.reply_text(
                "Введите комментарий для пользователя (или '-' без комментария):"
            )
            return

        if subscription_action.startswith("sub_grant_comment_"):
            if not can_grant_subscriptions(user_id):
                clear_subscription_flow(context)
                await message.reply_text("❌ Недостаточно прав для выдачи подписки")
                return
            days = int(subscription_action.split("_")[-1])
            comment = search_text.strip()
            if comment in ("", "-"):
                comment = "manual admin grant"
            context.user_data["sub_flow_comment"] = comment
            context.user_data["awaiting_subscription_action"] = f"sub_grant_confirm_{days}"
            target_id = context.user_data.get("sub_flow_target_id")
            await message.reply_text(
                (
                    "Подтвердите выдачу подписки:\n"
                    f"• user_id: `{target_id}`\n"
                    f"• срок: {days} дней\n"
                    f"• комментарий: {comment}"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data=f"sub_grant_confirm_{days}")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="admin_subscriptions_menu")],
                ]),
            )
            return

        if subscription_action.startswith("sub_grant_confirm_"):
            await message.reply_text("Используйте кнопку «Подтвердить» или «Отмена».")
            return

        # Legacy path kept for backward compatibility with old in-memory state.
        if subscription_action.startswith("grant_"):
            if not can_grant_subscriptions(user_id):
                clear_subscription_flow(context)
                await message.reply_text("❌ Недостаточно прав для выдачи подписки")
                return
            days = int(subscription_action.split("_")[1])
            parts = search_text.split(maxsplit=1)
            target_id = parse_telegram_id(parts[0]) if parts else None
            comment = parts[1] if len(parts) > 1 else "manual admin grant"
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            had_active_before = db.get_active_subscription(target_id) is not None
            plan_code = f"plan_{days}" if f"plan_{days}" in SUBSCRIPTION_PLANS else "manual"
            grant = db.grant_subscription(
                user_id=target_id,
                plan_code=plan_code,
                duration_days=days,
                source="manual",
                granted_by=user_id,
                grant_comment=comment,
            )
            await notify_manual_subscription_grant(
                context=context,
                target_user_id=target_id,
                duration_days=days,
                expires_at=grant["expires_at"],
                actor_user=user,
                comment=comment,
                was_extension=had_active_before,
            )
            clear_subscription_flow(context)
            await message.reply_text(
                f"✅ Подписка выдана пользователю `{target_id}` на {days} дней.\n"
                f"До: {grant['expires_at'].strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
            return

        if subscription_action == "sub_status":
            if not can_grant_subscriptions(user_id):
                clear_subscription_flow(context)
                await message.reply_text("❌ Недостаточно прав")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            sub = db.get_subscription(target_id)
            clear_subscription_flow(context)
            if not sub:
                await message.reply_text("📅 Подписка не найдена")
                return
            expires_at = db._parse_dt(sub[4])
            is_active = int(sub[5]) == 1 and expires_at and expires_at > utcnow_naive()
            await message.reply_text(
                "📅 Статус подписки\n"
                f"user_id: `{target_id}`\n"
                f"plan: {sub[1]}\n"
                f"active: {'yes' if is_active else 'no'}\n"
                f"starts_at: {sub[3]}\n"
                f"expires_at: {sub[4]}\n"
                f"source: {sub[6]}"
            )
            return

        if subscription_action == "sub_cancel":
            if not can_cancel_subscriptions(user_id):
                clear_subscription_flow(context)
                await message.reply_text("❌ У вас нет прав на деактивацию подписок")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            changed = db.cancel_subscription(
                target_id,
                granted_by=user_id,
                grant_comment="manual cancel from admin panel",
            )
            clear_subscription_flow(context)
            if changed:
                await message.reply_text(f"✅ Подписка деактивирована для `{target_id}`")
            else:
                await message.reply_text("ℹ️ Подписка не найдена или уже отключена")
            return

        if subscription_action == "sub_history":
            if not can_grant_subscriptions(user_id):
                clear_subscription_flow(context)
                await message.reply_text("❌ Недостаточно прав")
                return
            target_id = parse_telegram_id(search_text)
            if target_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            history = db.list_subscription_grants(user_id=target_id, limit=10)
            clear_subscription_flow(context)
            if not history:
                await message.reply_text("История выдач пуста")
                return
            lines = [f"🧾 История подписок `{target_id}`:"]
            for row in history:
                lines.append(
                    f"- {row[12]} | {row[2]} {row[3]}d | {row[11]} | by `{row[7] or 0}`"
                )
            await message.reply_text("\n".join(lines))
            return

        clear_subscription_flow(context)
        await message.reply_text("⚠️ Сессия работы с подписками устарела. Откройте раздел заново.")
        return

    referral_action = context.user_data.get("awaiting_referral_action")
    if referral_action:
        if get_actor_role(user_id) not in ("superadmin", "admin"):
            clear_referral_flow(context)
            await message.reply_text("❌ Недостаточно прав")
            return

        if referral_action == "ref_admin_user_lookup":
            target_user_id = parse_telegram_id(search_text)
            if target_user_id is None:
                await message.reply_text("❌ Неверный Telegram ID")
                return
            clear_referral_flow(context)
            await message.reply_text(
                format_admin_referrer_details_text(target_user_id),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]
                ])
            )
            return

        if referral_action == "ref_admin_retry_payload_input":
            invoice_payload, error_text = validate_invoice_payload(search_text)
            if error_text:
                await message.reply_text(error_text)
                return

            payment = db.get_star_payment_by_payload(invoice_payload)
            if not payment:
                await message.reply_text(
                    "❌ Star payment не найден по этому payload.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]
                    ])
                )
                return

            payment_user_id = int(payment[1])
            referral_row = db.get_referral_by_invited(payment_user_id)

            context.user_data["ref_retry_payload"] = invoice_payload
            context.user_data["awaiting_referral_action"] = "ref_admin_retry_confirm"

            await message.reply_text(
                format_referral_retry_confirm_text(invoice_payload, payment, referral_row),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить retry", callback_data="ref_admin_retry_confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="ref_admin_retry_cancel")],
                ])
            )
            return

        if referral_action == "ref_admin_retry_confirm":
            await message.reply_text("Используйте кнопки подтверждения или отмены.")
            return

        clear_referral_flow(context)
        await message.reply_text("⚠️ Сессия работы с рефералами устарела. Откройте раздел заново.")
        return
    
    # ✅ ПРОВЕРКА: Поиск в конкретном чате
    if context.user_data.get('awaiting_chat_search', False):
        if not search_text or len(search_text) < 2:
            await message.reply_text("❌ Запрос должен содержать минимум 2 символа")
            return
        
        owner_id = context.user_data.get('chat_search_owner_id')
        chat_id = context.user_data.get('chat_search_chat_id')
        
        if not owner_id or chat_id is None:
            await message.reply_text("❌ Ошибка: данные чата не найдены")
            context.user_data['awaiting_chat_search'] = False
            return

        if not can_view_chat(user_id, owner_id, chat_id):
            await message.reply_text("❌ Нет доступа к этому чату")
            context.user_data['awaiting_chat_search'] = False
            return
        
        context.user_data['awaiting_chat_search'] = False
        
        print(f"[CHAT_SEARCH] Поиск в чате {chat_id}: '{search_text}'")
        messages = db.search_messages_by_text(search_text, owner_id=owner_id, chat_id=chat_id, limit=500)
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        chat_display = get_chat_display_name(chat_id, owner_id, chat_info)
        
        if not messages:
            await message.reply_text(
                f"❌ По запросу **'{search_text}'** ничего не найдено в чате {chat_display}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 Новый поиск", callback_data=f"search_in_chat_{owner_id}_{chat_id}")
                ], [
                    InlineKeyboardButton("◀️ К чату", callback_data=f"view_chat_{owner_id}_{chat_id}")
                ]])
            )
            return
        
        context.user_data['chat_search_text'] = search_text
        context.user_data['chat_search_results'] = messages
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        
        text = f"""🔍 **ПОИСК В ЧАТЕ: '{search_text}'**

📁 Чат: {chat_display}
📊 Найдено: {len(messages)}
📄 Страница 1/{total_pages}

"""
        
        keyboard = []
        
        for msg in messages[:ITEMS_PER_PAGE]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            msg_id, chat_id_msg, owner_id_msg = msg[0], msg[1], msg[2]
            keyboard.append([
                InlineKeyboardButton("📄 Текст", callback_data=f"fulltext_{msg_id}_{chat_id_msg}_{owner_id_msg}"),
                InlineKeyboardButton("📋 Метаданные", callback_data=f"metadata_{msg_id}_{chat_id_msg}_{owner_id_msg}")
            ])
        
        nav_buttons = [InlineKeyboardButton("🔄", callback_data="chat_search_page_0")]
        nav_buttons.append(InlineKeyboardButton(f"1/{total_pages}", callback_data="chat_search_page_0"))
        if total_pages > 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data="chat_search_page_1"))
        
        keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data=f"search_in_chat_{owner_id}_{chat_id}")])
        keyboard.append([InlineKeyboardButton("◀️ К чату", callback_data=f"view_chat_{owner_id}_{chat_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.reply_text(text, reply_markup=reply_markup)
        return
    
    # ✅ Проверка флага ожидания ГЛОБАЛЬНОГО поиска
    if not context.user_data.get('awaiting_search', False):
        return
    
    if not search_text or len(search_text) < 2:
        await message.reply_text("❌ Запрос должен содержать минимум 2 символа")
        return
    
    context.user_data['awaiting_search'] = False
    
    print(f"[SEARCH] Поиск: '{search_text}'")
    messages = db.search_messages_by_text(search_text, limit=1000)
    messages = filter_messages_by_scope(messages, user_id)
    
    if not messages:
        await message.reply_text(
            f"❌ По запросу **'{search_text}'** ничего не найдено.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 Новый поиск", callback_data="search_text")
            ], [
                InlineKeyboardButton("◀️ В меню", callback_data="admin_back")
            ]])
        )
        return
    
    context.user_data['search_text'] = search_text
    context.user_data['search_results'] = messages
    context.user_data['search_page'] = 0
    
    await send_search_page(message, context, page=0)


# ==================== КОМАНДЫ ====================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return
    if not is_admin(user.id):
        if not await ensure_not_blacklisted(user.id, message, context):
            return

    is_new = db.register_user(user.id, user.username, user.first_name)
    start_payload = ""
    if getattr(context, "args", None):
        try:
            start_payload = (context.args[0] or "").strip()
        except Exception:
            start_payload = ""

    referral_bind_result = None
    referrer_user_id = parse_referrer_id_from_payload(start_payload)
    if referrer_user_id:
        referral_bind_result = db.bind_referrer(
            invited_user_id=user.id,
            referrer_user_id=referrer_user_id,
            source_payload=start_payload,
        )

    if is_new:
        await send_admin_notification(
            context.bot,
            f"""🆕 **НОВЫЙ ПОЛЬЗОВАТЕЛЬ**

👤 Имя: {user.first_name}
🆔 ID: `{user.id}`
📱 Username: @{user.username or 'нет'}
🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        )

    welcome_text = get_welcome_text()
    welcome_markup = build_public_start_keyboard()
    welcome_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "welcome.jpg")

    if os.path.isfile(welcome_path):
        try:
            with open(welcome_path, "rb") as photo:
                await context.bot.send_photo(
                    chat_id=message.chat_id,
                    photo=photo,
                    caption=welcome_text,
                    reply_markup=welcome_markup
                )
        except Exception as e:
            print(f"[WARNING] Failed to send welcome.jpg, fallback to text: {e}")
            await message.reply_text(welcome_text, reply_markup=welcome_markup)
    else:
        print(f"[WARNING] welcome.jpg not found at {welcome_path}, fallback to text")
        await message.reply_text(welcome_text, reply_markup=welcome_markup)

    if referral_bind_result and referral_bind_result.get("linked"):
        await message.reply_text(
            "🎁 Вы подключены по реферальной ссылке.\n"
            f"После вашей первой оплаты вы получите +{REFERRAL_INVITED_BONUS_DAYS} дней к подписке."
        )

    if is_admin(user.id):
        await message.reply_text(
            f"Админ-доступ активен.\n{format_subscription_summary(user.id)}\n\n/admin - админ-панель"
        )
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if message is None:
        return
    user = update.effective_user
    if user and not is_admin(user.id):
        if not await ensure_not_blacklisted(user.id, message, context):
            return

    help_text = (
        "Справка:\n\n"
        "/start - стартовый экран\n"
        "/help - справка\n"
        "/cabinet - личный кабинет\n"
        "/subscription - статус подписки\n"
        "/plans - тарифы\n"
        "/paysupport - поддержка по оплатам\n"
        "/stats - статистика (доступно при активной подписке)\n"
    )

    if is_admin(update.effective_user.id if update.effective_user else 0):
        help_text += "\n/admin - админ-панель\n"

    await message.reply_text(help_text)
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    if not await require_paid_access(update, context, user.id, notify=True, target_message=message):
        return

    stats = db.get_user_stats(user.id)
    if not stats:
        await message.reply_text("У тебя пока нет статистики. Подключи бота к Business.")
        return

    last_msg = db.get_user_last_activity(user.id)
    last_activity = format_datetime_msk(last_msg[8]) if last_msg else "нет данных"

    stats_text = f"""📊 **ТВОЯ СТАТИСТИКА**

💬 Всего сообщений: **{stats[1]}**
🗑 Удалено: **{stats[2]}**
✏️ Изменено: **{stats[3]}**
📎 Сохранено медиа: **{stats[4]}**
🕐 Последняя активность: {last_activity}
"""
    await message.reply_text(stats_text)
async def plans_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if not is_admin(user.id):
        if not await ensure_not_blacklisted(user.id, message, context):
            return
    if not check_rate_limit(user.id, "payment_start")[0]:
        await message.reply_text("Слишком много попыток. Повторите позже.")
        return
    await message.reply_text(get_plans_text(), reply_markup=build_public_plans_keyboard())


async def paysupport_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(get_paysupport_text())


async def subscription_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if not is_admin(user.id):
        if not await ensure_not_blacklisted(user.id, message, context):
            return

    if is_subscription_exempt(user.id):
        role = get_admin_role(user.id) or "admin"
        await message.reply_text(
            f"Роль: {role}\nПодписка не требуется для администраторов."
        )
        return

    sub = db.get_subscription(user.id)
    if not sub:
        await message.reply_text(
            "📅 Подписка: неактивна",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💫 Купить подписку", callback_data="public_plans")]])
        )
        return

    starts_at = db._parse_dt(sub[3])
    expires_at = db._parse_dt(sub[4])
    is_active = int(sub[5]) == 1 and expires_at and expires_at > utcnow_naive()
    days_left = get_subscription_days_left(sub) if is_active else 0

    text = (
        "📅 **Моя подписка**\n\n"
        f"Статус: {'активна' if is_active else 'неактивна'}\n"
        f"Тариф: {sub[1] or 'custom'}\n"
        f"Начало: {starts_at.strftime('%Y-%m-%d %H:%M:%S') if starts_at else '-'} UTC\n"
        f"Окончание: {expires_at.strftime('%Y-%m-%d %H:%M:%S') if expires_at else '-'} UTC\n"
        f"Осталось дней: {days_left}"
    )
    await message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💫 Купить подписку", callback_data="public_plans")]
        ])
    )


async def cabinet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    if not is_admin(user.id):
        if not await ensure_not_blacklisted(user.id, message, context):
            return

    can_trial, _reason = db.can_activate_trial(user.id)
    text = await build_cabinet_text(user.id, context)
    await message.reply_text(text, reply_markup=build_cabinet_keyboard(can_trial))


async def send_plan_invoice(context: ContextTypes.DEFAULT_TYPE, user_id: int, plan_code: str):
    plan = SUBSCRIPTION_PLANS.get(plan_code)
    if not plan:
        return False

    discounted_stars, discount_mode = db.get_discounted_stars_for_plan(user_id, plan_code, int(plan["stars"]))
    invoice_payload = f"{plan_code}:{user_id}:{int(time.time())}"
    db.create_star_payment(
        user_id=user_id,
        plan_code=plan_code,
        amount_stars=discounted_stars,
        duration_days=plan["days"],
        invoice_payload=invoice_payload,
    )

    price_label = plan["title"]
    description = f"Разовый доступ к функциям бота на {plan['days']} дней."
    if discount_mode == "promo_discount":
        description += "\nПрименена скидка по промокоду."
    elif discount_mode == "promo_fixed":
        description += "\nПрименена фиксированная цена по промокоду."

    await context.bot.send_invoice(
        chat_id=user_id,
        title=f"Доступ на {plan['days']} дней",
        description=description,
        payload=invoice_payload,
        currency="XTR",
        prices=[LabeledPrice(label=price_label, amount=discounted_stars)],
        provider_token="",
        start_parameter=f"{plan_code}_{user_id}",
    )
    return True


async def send_gift_invoice(
    context: ContextTypes.DEFAULT_TYPE,
    payer_user_id: int,
    recipient_user_id: int,
    plan_code: str,
):
    plan = SUBSCRIPTION_PLANS.get(plan_code)
    if not plan:
        return None

    gift_payload = f"gift:{payer_user_id}:{recipient_user_id}:{plan_code}:{int(time.time())}"
    db.create_gift_payment(
        gift_payload=gift_payload,
        payer_user_id=payer_user_id,
        recipient_user_id=recipient_user_id,
        plan_code=plan_code,
        duration_days=int(plan["days"]),
        amount_stars=int(plan["stars"]),
    )

    await context.bot.send_invoice(
        chat_id=payer_user_id,
        title=f"Подарочная подписка {plan['days']} дней",
        description=(
            "Подарок подписки через Telegram Stars.\n"
            f"Получатель: {recipient_user_id}\n"
            f"Срок: {plan['days']} дней."
        ),
        payload=gift_payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"Gift {plan['days']} days", amount=int(plan["stars"]))],
        provider_token="",
        start_parameter=f"gift_{payer_user_id}_{recipient_user_id}_{plan_code}",
    )
    return gift_payload


async def public_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    user = query.from_user
    if user is None:
        await query.answer()
        return

    if not is_admin(user.id):
        if not await ensure_not_blacklisted(user.id, query, context):
            return

    action = query.data or ""
    await query.answer()

    if action == "public_how_it_works":
        await safe_edit_message(query, get_how_it_works_text(), build_public_how_keyboard())
        return

    if action == "public_back_start":
        clear_public_flow(context)
        await safe_edit_message(query, get_welcome_text(), build_public_start_keyboard())
        return

    if action == "public_cabinet":
        clear_public_flow(context)
        can_trial, _ = db.can_activate_trial(user.id)
        text = await build_cabinet_text(user.id, context)
        await safe_edit_message(query, text, build_cabinet_keyboard(can_trial))
        return

    if action == "public_trial_info":
        can_trial, reason = db.can_activate_trial(user.id)
        if not can_trial:
            reason_map = {
                "trial_already_used": "Пробный период уже использован.",
                "subscription_already_used": "Trial доступен только до первой активации подписки.",
            }
            await safe_edit_message(
                query,
                f"🎁 Пробный период недоступен.\n\n{reason_map.get(reason, 'Ограничение аккаунта.')}",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return
        await safe_edit_message(
            query,
            (
                "🎁 **Пробный период**\n\n"
                f"Длительность: {TRIAL_DAYS} дня.\n"
                "Можно активировать только один раз.\n"
                "После окончания нужно купить подписку."
            ),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Активировать trial", callback_data="public_trial_confirm")],
                [InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")],
            ]),
        )
        return

    if action == "public_trial_confirm":
        allowed, _ = check_rate_limit(user.id, "trial_activate")
        if not allowed:
            await safe_edit_message(
                query,
                "❌ Слишком много попыток активации trial. Повторите позже.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return
        trial_result = db.activate_trial(user.id, TRIAL_DAYS, activated_by=user.id, source="self")
        if not trial_result.get("ok"):
            reason_map = {
                "trial_already_used": "Пробный период уже использован.",
                "subscription_already_used": "Trial доступен только до первой активации подписки.",
            }
            await safe_edit_message(
                query,
                f"❌ Не удалось активировать trial.\n{reason_map.get(trial_result.get('reason'), trial_result.get('reason', 'unknown'))}",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return

        expires_at = trial_result.get("expires_at")
        expires_text = expires_at.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if isinstance(expires_at, datetime) else "-"
        db.log_activity(user.id, "trial_activated", f"Активирован пробный период на {TRIAL_DAYS} дн.", {
            "expires_at": expires_text,
        })
        await safe_edit_message(
            query,
            f"✅ Trial активирован на {TRIAL_DAYS} дня.\nДействует до: {expires_text}",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Личный кабинет", callback_data="public_cabinet")],
                [InlineKeyboardButton("💫 Тарифы", callback_data="public_plans")],
            ]),
        )
        return

    if action == "public_promo_enter":
        allowed, _ = check_rate_limit(user.id, "promo_apply")
        if not allowed:
            await safe_edit_message(
                query,
                "❌ Слишком много попыток ввода промокода. Повторите позже.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return
        clear_public_flow(context)
        context.user_data["awaiting_public_action"] = "promo_code_input"
        await safe_edit_message(
            query,
            "🎟 Введите промокод одним сообщением:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="public_cabinet")]]),
        )
        return

    if action == "public_gift_start":
        allowed, _ = check_rate_limit(user.id, "gift_start")
        if not allowed:
            await safe_edit_message(
                query,
                "❌ Слишком много попыток запуска подарка. Повторите позже.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return
        clear_public_flow(context)
        context.user_data["awaiting_public_action"] = "gift_recipient_input"
        await safe_edit_message(
            query,
            "🎁 Введите Telegram ID получателя подарочной подписки:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="public_cabinet")]]),
        )
        return

    if action == "public_gift_back_plans":
        recipient_id = context.user_data.get("gift_recipient_id")
        if not recipient_id:
            clear_public_flow(context)
            await safe_edit_message(query, "⚠️ Получатель не выбран. Начните заново.", InlineKeyboardMarkup([
                [InlineKeyboardButton("🎁 Подарить подписку", callback_data="public_gift_start")],
                [InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")],
            ]))
            return
        await safe_edit_message(
            query,
            f"🎁 Получатель: `{recipient_id}`\nВыберите тариф подарка:",
            build_public_gift_plans_keyboard(),
        )
        return

    if action.startswith("public_gift_plan_"):
        recipient_id = context.user_data.get("gift_recipient_id")
        if not recipient_id:
            clear_public_flow(context)
            await safe_edit_message(
                query,
                "⚠️ Сессия подарка устарела. Начните заново.",
                InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Подарить подписку", callback_data="public_gift_start")]]),
            )
            return
        plan_code = action.replace("public_gift_plan_", "", 1)
        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            await safe_edit_message(query, "❌ Неверный тариф.", build_public_gift_plans_keyboard())
            return
        context.user_data["gift_confirm_plan_code"] = plan_code
        context.user_data["gift_confirm_days"] = int(plan["days"])
        context.user_data["gift_confirm_stars"] = int(plan["stars"])
        await safe_edit_message(
            query,
            (
                "🎁 **Подтверждение подарка**\n\n"
                f"Получатель: `{recipient_id}`\n"
                f"Тариф: {plan['days']} дней\n"
                f"Цена: {plan['stars']} Stars\n\n"
                "После подтверждения откроется оплата через Telegram Stars."
            ),
            build_public_gift_confirm_keyboard(),
        )
        return

    if action == "public_gift_confirm":
        allowed, _ = check_rate_limit(user.id, "gift_confirm")
        if not allowed:
            await safe_edit_message(
                query,
                "❌ Слишком много попыток подтверждения. Повторите позже.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return
        recipient_id = context.user_data.get("gift_recipient_id")
        plan_code = context.user_data.get("gift_confirm_plan_code")
        if not recipient_id or not plan_code:
            clear_public_flow(context)
            await safe_edit_message(
                query,
                "⚠️ Сессия подарка устарела. Начните заново.",
                InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Подарить подписку", callback_data="public_gift_start")]]),
            )
            return
        if recipient_id == user.id:
            await safe_edit_message(
                query,
                "❌ Дарить подписку самому себе нельзя.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
            return
        try:
            await send_gift_invoice(context, user.id, int(recipient_id), plan_code)
            clear_public_flow(context)
            await safe_edit_message(
                query,
                "✅ Счёт на подарочную подписку отправлен. Подтвердите оплату в Telegram.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В кабинет", callback_data="public_cabinet")]]),
            )
        except Exception as e:
            print(f"[ERROR] Failed to send gift invoice: {e}")
            await safe_edit_message(
                query,
                "❌ Не удалось создать счёт на подарок. Попробуйте позже.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="public_cabinet")]]),
            )
        return

    if action == "public_gift_cancel":
        clear_public_flow(context)
        await safe_edit_message(
            query,
            "❌ Оформление подарка отменено.",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В кабинет", callback_data="public_cabinet")]]),
        )
        return

    if action == "public_history":
        events = db.get_user_action_history(user.id, limit=200)
        context.user_data["history_page"] = 0
        text = format_public_history_text(events, 0, HISTORY_PAGE_SIZE)
        total_pages = max((len(events) + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE, 1)
        await safe_edit_message(query, text, build_public_history_keyboard(0, total_pages))
        return

    if action.startswith("public_history_page_"):
        try:
            page = int(action.replace("public_history_page_", "", 1))
        except Exception:
            page = 0
        events = db.get_user_action_history(user.id, limit=200)
        total_pages = max((len(events) + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE, 1)
        page = max(0, min(page, total_pages - 1))
        context.user_data["history_page"] = page
        text = format_public_history_text(events, page, HISTORY_PAGE_SIZE)
        await safe_edit_message(query, text, build_public_history_keyboard(page, total_pages))
        return

    if action == "public_referral":
        bot_username = await get_bot_username_cached(context)
        referral_link = build_referral_link(bot_username, user.id)
        await safe_edit_message(
            query,
            format_public_referral_main_text(user.id, referral_link),
            build_public_referral_keyboard(),
        )
        return

    if action == "public_ref_link":
        bot_username = await get_bot_username_cached(context)
        referral_link = build_referral_link(bot_username, user.id)
        if not referral_link:
            text = "⚠️ Не удалось сформировать ссылку. Попробуйте позже."
        else:
            text = f"📋 **Ваша реферальная ссылка:**\n{referral_link}"
        await safe_edit_message(query, text, build_public_referral_keyboard())
        return

    if action == "public_ref_stats":
        await safe_edit_message(query, format_public_referral_stats_text(user.id), build_public_referral_keyboard())
        return

    if action == "public_ref_share":
        bot_username = await get_bot_username_cached(context)
        referral_link = build_referral_link(bot_username, user.id)
        if not referral_link:
            share_text = "⚠️ Не удалось сформировать текст для пересылки. Попробуйте позже."
        else:
            share_text = build_referral_share_text(referral_link)
        await safe_edit_message(query, share_text, build_public_referral_keyboard())
        return

    if action == "public_plans":
        await safe_edit_message(query, get_plans_text(), build_public_plans_keyboard())
        return

    if action == "public_subscription_status":
        access = get_user_access_snapshot(user.id)
        expires = access["expires_at"].strftime("%Y-%m-%d %H:%M:%S") + " UTC" if access["expires_at"] else "-"
        text = (
            "📅 Мой доступ\n\n"
            f"Статус: {access['status']}\n"
            f"Тариф: {access['plan_code'] or '-'}\n"
            f"Источник: {access['source'] or '-'}\n"
            f"Окончание: {expires}\n"
            f"Осталось дней: {access['days_left']}"
        )
        await safe_edit_message(
            query,
            text,
            InlineKeyboardMarkup([
                [InlineKeyboardButton("💫 Купить/продлить", callback_data="public_plans")],
                [InlineKeyboardButton("◀️ Назад", callback_data="public_back_start")]
            ])
        )
        return

    if action.startswith("public_buy_"):
        if not check_rate_limit(user.id, "payment_start")[0]:
            await context.bot.send_message(user.id, "Слишком много попыток. Повторите позже.")
            return
        plan_code = action.replace("public_buy_", "", 1)
        if plan_code not in SUBSCRIPTION_PLANS:
            await context.bot.send_message(user.id, "Неверный тариф")
            return
        try:
            await send_plan_invoice(context, user.id, plan_code)
            await context.bot.send_message(user.id, "Счёт отправлен. Подтвердите оплату в Telegram.")
        except Exception as e:
            print(f"[ERROR] Failed to send Stars invoice: {e}")
            await context.bot.send_message(user.id, "Не удалось создать счёт. Попробуйте позже.")
        return


async def pre_checkout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    if query is None:
        return

    payload = query.invoice_payload or ""
    if query.currency != "XTR":
        await query.answer(ok=False, error_message="Unsupported currency")
        return

    if GIFTPAYLOAD_EXPECTED_RE.fullmatch(payload):
        gift_payment = db.get_gift_payment_by_payload(payload)
        if not gift_payment:
            await query.answer(ok=False, error_message="Подарочный платеж не найден. Попробуйте снова.")
            return
        if int(gift_payment[2]) != int(query.from_user.id):
            await query.answer(ok=False, error_message="Плательщик подарка не совпадает.")
            return
        db.mark_gift_payment_precheckout(payload)
        await query.answer(ok=True)
        return

    payment = db.get_star_payment_by_payload(payload)
    if not payment:
        await query.answer(ok=False, error_message="Платеж не найден. Попробуйте снова.")
        return

    if int(payment[1]) != int(query.from_user.id):
        await query.answer(ok=False, error_message="Пользователь платежа не совпадает.")
        return

    db.mark_star_payment_precheckout(payload)
    await query.answer(ok=True)


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or not message.successful_payment:
        return

    payment_data = message.successful_payment
    if payment_data.currency != "XTR":
        await message.reply_text("Получен платеж в неподдерживаемой валюте. Напишите /paysupport")
        return

    payload = payment_data.invoice_payload or ""

    # Gift payment flow (separate from regular subscription and referral logic)
    if GIFTPAYLOAD_EXPECTED_RE.fullmatch(payload):
        paid_at = utcnow_naive()
        gift_result = db.process_gift_payment_success(
            payer_user_id=user.id,
            gift_payload=payload,
            telegram_payment_charge_id=payment_data.telegram_payment_charge_id,
            purchased_at=paid_at,
        )
        gift_status = gift_result.get("status")
        if gift_status == "not_found":
            await message.reply_text("Подарочный платеж получен, но не найден в системе. Напишите /paysupport")
            return
        if gift_status == "payer_mismatch":
            await message.reply_text("Плательщик подарка не совпадает. Напишите /paysupport")
            return
        if gift_status == "already_paid":
            expires_dt = gift_result.get("expires_at")
            expires_text = expires_dt.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if expires_dt else "unknown"
            await message.reply_text(f"✅ Подарочный платеж уже обработан ранее. Действует до: {expires_text}")
            return
        if gift_status != "paid":
            await message.reply_text("Подарочный платеж временно недоступен. Напишите /paysupport")
            return

        payer_user_id = int(gift_result["payer_user_id"])
        recipient_user_id = int(gift_result["recipient_user_id"])
        plan_code = gift_result["plan_code"]
        duration_days = int(gift_result["duration_days"])
        grant = gift_result.get("grant")
        if not grant or "expires_at" not in grant:
            await message.reply_text("Подарочный платеж получен, но тариф не распознан. Напишите /paysupport")
            return

        db.log_activity(
            payer_user_id,
            "gift_purchase",
            f"Оплачен подарок на {duration_days} дн. для {recipient_user_id}",
            {
                "recipient_user_id": recipient_user_id,
                "plan_code": plan_code,
                "expires_at": grant["expires_at"].strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        db.log_activity(
            recipient_user_id,
            "gift_received",
            f"Получена подарочная подписка на {duration_days} дн. от {payer_user_id}",
            {
                "payer_user_id": payer_user_id,
                "plan_code": plan_code,
                "expires_at": grant["expires_at"].strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        expires_text = grant["expires_at"].strftime("%Y-%m-%d %H:%M:%S") + " UTC"
        try:
            payer_name = user.full_name or user.first_name or str(user.id)
            payer_line = f"{payer_name}"
            if user.username:
                payer_line += f" (@{user.username})"
            payer_line += f" [ID: {payer_user_id}]"
            await context.bot.send_message(
                recipient_user_id,
                (
                    "🎁 Вам подарили подписку!\n"
                    f"Срок: {duration_days} дней\n"
                    f"Выдал: {payer_line}\n"
                    f"Действует до: {expires_text}"
                ),
            )
            db.mark_gift_payment_notified(payload)
        except Exception as e:
            print(f"[WARNING] Failed to notify gift recipient user_id={recipient_user_id}: {e}")

        await message.reply_text(
            (
                "✅ Подарок успешно оплачен.\n"
                f"Получатель: `{recipient_user_id}`\n"
                f"Срок: {duration_days} дней\n"
                f"Подписка активна до: {expires_text}"
            )
        )

        await send_admin_notification(
            context.bot,
            (
                "🎁 Подарочная оплата Stars\n"
                f"payer={payer_user_id}\n"
                f"recipient={recipient_user_id}\n"
                f"plan={plan_code}\n"
                f"amount={payment_data.total_amount} XTR\n"
                f"charge={payment_data.telegram_payment_charge_id}"
            ),
        )
        return

    payment_result = db.process_star_payment_success(
        user_id=user.id,
        invoice_payload=payload,
        telegram_payment_charge_id=payment_data.telegram_payment_charge_id,
        purchased_at=utcnow_naive(),
    )
    payment_status = payment_result.get("status")

    if payment_status == "not_found":
        await message.reply_text("Платеж получен, но не найден в системе. Напишите /paysupport")
        return
    if payment_status == "payer_mismatch":
        await message.reply_text("Плательщик не совпадает с пользователем платежа. Напишите /paysupport")
        return

    if payment_status == "already_paid":
        paid_expires = payment_result.get("expires_at")
        paid_until = paid_expires.strftime('%Y-%m-%d %H:%M:%S') if paid_expires else "unknown"
        paid_at = payment_result.get("purchased_at") or utcnow_naive()

        try:
            referral_result = db.process_referral_bonus_for_successful_payment(
                invited_user_id=user.id,
                invoice_payload=payload,
                purchased_at=paid_at,
                invited_bonus_days=REFERRAL_INVITED_BONUS_DAYS,
                referrer_bonus_days=REFERRAL_REFERRER_BONUS_DAYS,
            )
        except Exception as e:
            print(f"[ERROR] Ошибка recovery начисления реферальных бонусов user_id={user.id}: {e}")
            referral_result = {"invited_bonus_granted": False, "referrer_bonus_granted": False}

        extra_lines = []
        if referral_result.get("invited_bonus_granted"):
            invited_bonus_expires = referral_result.get("invited_expires_at")
            bonus_until = invited_bonus_expires.strftime('%Y-%m-%d %H:%M:%S') if invited_bonus_expires else "unknown"
            extra_lines.append(
                f"🎃 Реферальный бонус начислен: +{REFERRAL_INVITED_BONUS_DAYS} дней.\n"
                f"Новый срок: {bonus_until} UTC."
            )

        if referral_result.get("referrer_bonus_granted"):
            referrer_user_id = referral_result.get("referrer_user_id")
            referrer_expires = referral_result.get("referrer_expires_at")
            referrer_until = referrer_expires.strftime('%Y-%m-%d %H:%M:%S') if referrer_expires else "unknown"
            try:
                await context.bot.send_message(
                    referrer_user_id,
                    "🎉 Ваш друг оплатил подписку по вашей реферальной ссылке.\n"
                    f"Вам начислено +{REFERRAL_REFERRER_BONUS_DAYS} дней.\n"
                    f"Подписка активна до {referrer_until} UTC."
                )
            except Exception as e:
                print(f"[WARNING] Не удалось отправить recovery-уведомление referrer={referrer_user_id}: {e}")

        await message.reply_text(
            "✅ Этот платеж уже был обработан ранее.\n"
            f"Подписка активна до {paid_until} UTC."
            + ("\n\n" + "\n".join(extra_lines) if extra_lines else "")
        )
        return

    if payment_status != "paid":
        await message.reply_text("Платеж не удалось обработать. Напишите /paysupport")
        return

    plan_code = payment_result.get("plan_code")
    granted = payment_result.get("grant")
    if not plan_code or not granted or "expires_at" not in granted:
        await message.reply_text("Платеж получен, но тариф не распознан. Напишите /paysupport")
        return
    plan = SUBSCRIPTION_PLANS.get(plan_code) or {"days": int(payment_result.get("duration_days") or 0)}
    paid_at = payment_result.get("purchased_at") or utcnow_naive()

    db.log_activity(
        user.id,
        "payment_success",
        f"Оплачена подписка {plan_code} на {plan['days']} дн.",
        {
            "plan_code": plan_code,
            "stars": payment_data.total_amount,
            "expires_at": granted["expires_at"].strftime("%Y-%m-%d %H:%M:%S"),
            "payload": payload,
        },
    )

    try:
        referral_result = db.process_referral_bonus_for_successful_payment(
            invited_user_id=user.id,
            invoice_payload=payload,
            purchased_at=paid_at,
            invited_bonus_days=REFERRAL_INVITED_BONUS_DAYS,
            referrer_bonus_days=REFERRAL_REFERRER_BONUS_DAYS,
        )
    except Exception as e:
        print(f"[ERROR] Ошибка начисления реферальных бонусов user_id={user.id}: {e}")
        referral_result = {"invited_bonus_granted": False, "referrer_bonus_granted": False}

    extra_lines = []
    if referral_result.get("invited_bonus_granted"):
        invited_bonus_expires = referral_result.get("invited_expires_at")
        bonus_until = invited_bonus_expires.strftime('%Y-%m-%d %H:%M:%S') if invited_bonus_expires else "unknown"
        db.log_activity(
            user.id,
            "referral_bonus_invited",
            f"Начислен реферальный бонус +{REFERRAL_INVITED_BONUS_DAYS} дн.",
            {"expires_at": bonus_until},
        )
        extra_lines.append(
            f"🎃 Реферальный бонус начислен: +{REFERRAL_INVITED_BONUS_DAYS} дней.\n"
            f"Новый срок: {bonus_until} UTC."
        )

    if referral_result.get("referrer_bonus_granted"):
        referrer_user_id = referral_result.get("referrer_user_id")
        referrer_expires = referral_result.get("referrer_expires_at")
        referrer_until = referrer_expires.strftime('%Y-%m-%d %H:%M:%S') if referrer_expires else "unknown"
        if referrer_user_id:
            db.log_activity(
                int(referrer_user_id),
                "referral_bonus_referrer",
                f"Начислен реферальный бонус +{REFERRAL_REFERRER_BONUS_DAYS} дн. за оплату реферала {user.id}",
                {"expires_at": referrer_until, "invited_user_id": user.id},
            )
            try:
                await context.bot.send_message(
                    referrer_user_id,
                    "🎉 Ваш друг оплатил подписку по вашей реферальной ссылке.\n"
                    f"Вам начислено +{REFERRAL_REFERRER_BONUS_DAYS} дней.\n"
                    f"Подписка активна до {referrer_until} UTC."
                )
            except Exception as e:
                print(f"[WARNING] Не удалось отправить реферальное уведомление referrer={referrer_user_id}: {e}")

    await message.reply_text(
        "✅ Оплата прошла успешно.\n"
        f"Подписка активна до {granted['expires_at'].strftime('%Y-%m-%d %H:%M:%S')} UTC."
        + ("\n\n" + "\n".join(extra_lines) if extra_lines else ""),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Моя подписка", callback_data="public_subscription_status")],
            [InlineKeyboardButton("💫 Продлить", callback_data="public_plans")]
        ])
    )

    await send_admin_notification(
        context.bot,
        f"💳 Оплата Stars\nuser_id={user.id}\nplan={plan_code}\namount={payment_data.total_amount} XTR\ncharge={payment_data.telegram_payment_charge_id}"
    )


def build_admin_promocodes_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Создать", callback_data="promo_create_start")],
        [InlineKeyboardButton("🧾 Список", callback_data="promo_list")],
        [InlineKeyboardButton("✏️ Редактировать", callback_data="promo_edit_start")],
        [InlineKeyboardButton("🔁 Вкл/выкл", callback_data="promo_toggle_start")],
        [InlineKeyboardButton("📊 Статистика", callback_data="promo_stats_start")],
        [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
    ])


def build_admin_blacklist_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⛔ Добавить в ЧС", callback_data="blacklist_add_start")],
        [InlineKeyboardButton("✅ Снять блокировку", callback_data="blacklist_remove_start")],
        [InlineKeyboardButton("📋 Список блокировок", callback_data="blacklist_list")],
        [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
    ])


def build_team_roles_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Сотрудники", callback_data="team_members_list"), InlineKeyboardButton("🧩 Шаблоны", callback_data="team_templates_list")],
        [InlineKeyboardButton("➕ Назначить роль", callback_data="team_assign_start"), InlineKeyboardButton("👁 Просмотр по ID", callback_data="team_member_view_start")],
        [InlineKeyboardButton("➕ Scope owner", callback_data="team_scope_owner_start"), InlineKeyboardButton("➕ Scope owner+chat", callback_data="team_scope_chat_start")],
        [InlineKeyboardButton("🧹 Очистить scopes", callback_data="team_scope_clear_start"), InlineKeyboardButton("🛠 Custom perms", callback_data="team_custom_start")],
        [InlineKeyboardButton("➖ Снять роль", callback_data="team_remove_start")],
        [InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")],
    ])


async def handle_admin_extensions(query, context, requester_id, action):
    admin_role = get_actor_role(requester_id)
    owner_id_ctx, chat_id_ctx = extract_owner_chat_from_action(action)
    if not await guard_admin_output_access(
        query,
        requester_id,
        action_name=action,
        owner_id=owner_id_ctx,
        chat_id=chat_id_ctx,
    ):
        return True

    if action == "admin_promocodes_menu":
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            "🎟 **Промокоды**\n\nСоздание, редактирование, включение/выключение и статистика использования.",
            build_admin_promocodes_keyboard(),
        )
        return True

    if action == "promo_create_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "promo_create_code"
        await safe_edit_message(
            query,
            "Введите код нового промокода (3-32 символа, A-Z/0-9/_/-):",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")]]),
        )
        return True

    if action.startswith("promo_create_type_"):
        if context.user_data.get("awaiting_admin_action") != "promo_create_type":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия создания промокода устарела.", build_admin_promocodes_keyboard())
            return True
        promo_type = action.replace("promo_create_type_", "", 1)
        if promo_type not in PROMO_TYPES:
            await safe_edit_message(query, "❌ Неизвестный тип промокода.", build_admin_promocodes_keyboard())
            return True
        context.user_data["admin_flow_type"] = promo_type
        context.user_data["awaiting_admin_action"] = "promo_create_value"
        if promo_type in ("bonus_days", "free_access"):
            hint = "Введите количество дней (целое число)."
        elif promo_type == "discount_percent":
            hint = "Введите скидку в процентах (1..99)."
        else:
            hint = "Введите три цены через пробел: `price30 price90 price180`."
        await safe_edit_message(
            query,
            f"Тип: `{promo_type}`\n{hint}",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")]]),
        )
        return True

    if action == "promo_create_confirm":
        if context.user_data.get("awaiting_admin_action") != "promo_create_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия создания промокода устарела.", build_admin_promocodes_keyboard())
            return True

        code = context.user_data.get("admin_flow_code")
        promo_type = context.user_data.get("admin_flow_type")
        value = context.user_data.get("admin_flow_value")
        comment = context.user_data.get("admin_flow_comment", "")
        settings = context.user_data.get("admin_flow_settings", {}) or {}
        kwargs = {
            "comment": comment,
            "max_activations": DEFAULT_PROMO_MAX_ACTIVATIONS,
            "per_user_limit": DEFAULT_PROMO_PER_USER_LIMIT,
            "allow_with_trial": True,
            "allow_with_other_bonus": True,
            "is_active": True,
        }

        if promo_type == "bonus_days":
            kwargs["bonus_days"] = int(value or 0)
        elif promo_type == "free_access":
            kwargs["free_days"] = int(value or 0)
        elif promo_type == "discount_percent":
            kwargs["discount_percent"] = int(value or 0)
        elif promo_type == "fixed_price_override":
            prices = value or [None, None, None]
            kwargs["fixed_stars_30"] = int(prices[0])
            kwargs["fixed_stars_90"] = int(prices[1])
            kwargs["fixed_stars_180"] = int(prices[2])
        else:
            await safe_edit_message(query, "❌ Неподдерживаемый тип промокода.", build_admin_promocodes_keyboard())
            clear_admin_flow(context)
            return True

        if "starts" in settings:
            starts = parse_iso_datetime_or_none(settings.get("starts"))
            if starts is None:
                await safe_edit_message(query, "❌ Неверный формат starts.", build_admin_promocodes_keyboard())
                return True
            kwargs["starts_at"] = starts
        if "expires" in settings:
            expires = parse_iso_datetime_or_none(settings.get("expires"))
            if expires is None:
                await safe_edit_message(query, "❌ Неверный формат expires.", build_admin_promocodes_keyboard())
                return True
            kwargs["expires_at"] = expires
        if "max" in settings:
            kwargs["max_activations"] = max(0, int(settings["max"]))
        if "per_user" in settings:
            kwargs["per_user_limit"] = max(1, int(settings["per_user"]))
        if "only_new" in settings:
            kwargs["only_new_users"] = parse_bool_flag(settings["only_new"])
        if "first_payment" in settings:
            kwargs["first_payment_only"] = parse_bool_flag(settings["first_payment"])
        if "with_trial" in settings:
            kwargs["allow_with_trial"] = parse_bool_flag(settings["with_trial"], default=True)
        if "with_bonus" in settings:
            kwargs["allow_with_other_bonus"] = parse_bool_flag(settings["with_bonus"], default=True)

        result = db.create_promo_code(code=code, promo_type=promo_type, created_by=requester_id, **kwargs)
        clear_admin_flow(context)
        if not result.get("ok"):
            await safe_edit_message(
                query,
                f"❌ Не удалось создать промокод: {result.get('reason', 'unknown')}",
                build_admin_promocodes_keyboard(),
            )
            return True

        db.log_admin_audit(requester_id, "promo_create", target_user_id=None, details=f"code={code};type={promo_type}")
        await safe_edit_message(query, f"✅ Промокод `{code}` создан.", build_admin_promocodes_keyboard())
        return True

    if action == "promo_list":
        rows = db.list_promo_codes(limit=30)
        lines = ["🎟 **Промокоды**", ""]
        if not rows:
            lines.append("Промокодов пока нет.")
        else:
            for row in rows:
                code = row[1]
                promo_type = row[2]
                is_active = "✅" if int(row[3]) == 1 else "⛔"
                max_uses = row[6] or 0
                per_user = row[7] or 1
                lines.append(f"{is_active} `{code}` | {promo_type} | max={max_uses} | per_user={per_user}")
        await safe_edit_message(query, "\n".join(lines), build_admin_promocodes_keyboard())
        return True

    if action == "promo_toggle_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "promo_toggle_code"
        await safe_edit_message(
            query,
            "Введите код промокода для включения/выключения:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")]]),
        )
        return True

    if action in ("promo_toggle_apply_on", "promo_toggle_apply_off"):
        if context.user_data.get("awaiting_admin_action") != "promo_toggle_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия переключения устарела.", build_admin_promocodes_keyboard())
            return True
        code = context.user_data.get("admin_flow_code")
        active_state = action.endswith("_on")
        changed = db.set_promo_active(code, active_state)
        clear_admin_flow(context)
        if changed:
            db.log_admin_audit(requester_id, "promo_toggle", details=f"code={code};active={int(active_state)}")
        await safe_edit_message(
            query,
            f"{'✅' if changed else '❌'} {'Включено' if active_state else 'Выключено'}: `{code}`",
            build_admin_promocodes_keyboard(),
        )
        return True

    if action == "promo_stats_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "promo_stats_code"
        await safe_edit_message(
            query,
            "Введите код промокода для просмотра статистики:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")]]),
        )
        return True

    if action == "promo_edit_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "promo_edit_code"
        await safe_edit_message(
            query,
            "Введите код промокода для редактирования:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_promocodes_menu")]]),
        )
        return True

    if action == "admin_blacklist_menu":
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            "⛔ **Чёрный список и антиспам**\n\nБлокировка отключает /start, trial, платежные сценарии и premium-функции.",
            build_admin_blacklist_keyboard(),
        )
        return True

    if action == "blacklist_add_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "blacklist_add_user"
        await safe_edit_message(
            query,
            "Введите Telegram ID пользователя для блокировки:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_blacklist_menu")]]),
        )
        return True

    if action == "blacklist_add_confirm":
        if context.user_data.get("awaiting_admin_action") != "blacklist_add_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия блокировки устарела.", build_admin_blacklist_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        reason = context.user_data.get("admin_flow_reason", "без причины")
        hours = int(context.user_data.get("admin_flow_hours") or 0)
        blocked_until = None
        if hours > 0:
            blocked_until = utcnow_naive() + timedelta(hours=hours)
        db.set_blacklist(target_id, reason=reason, blocked_until=blocked_until, blocked_by=requester_id)
        db.log_admin_audit(requester_id, "blacklist_add", target_user_id=target_id, details=f"reason={reason};hours={hours}")
        clear_admin_flow(context)
        until_text = blocked_until.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if blocked_until else "бессрочно"
        await safe_edit_message(
            query,
            f"✅ Пользователь `{target_id}` добавлен в blacklist.\nПричина: {reason}\nСрок: {until_text}",
            build_admin_blacklist_keyboard(),
        )
        return True

    if action == "blacklist_remove_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "blacklist_remove_user"
        await safe_edit_message(
            query,
            "Введите Telegram ID пользователя для снятия блокировки:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_blacklist_menu")]]),
        )
        return True

    if action == "blacklist_remove_confirm":
        if context.user_data.get("awaiting_admin_action") != "blacklist_remove_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия разблокировки устарела.", build_admin_blacklist_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        removed = db.remove_blacklist(target_id)
        if removed:
            db.log_admin_audit(requester_id, "blacklist_remove", target_user_id=target_id, details="removed")
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            f"{'✅' if removed else 'ℹ️'} Блокировка {'снята' if removed else 'не найдена'} для `{target_id}`",
            build_admin_blacklist_keyboard(),
        )
        return True

    if action == "blacklist_list":
        rows = db.list_blacklist(limit=100)
        lines = ["⛔ **Активные блокировки**", ""]
        if not rows:
            lines.append("Список пуст.")
        else:
            for row in rows:
                user_id, reason, blocked_until, blocked_by, _, created_at = row
                until = blocked_until or "бессрочно"
                lines.append(f"• `{user_id}` | до: {until} | reason: {reason or '-'} | by: `{blocked_by or 0}`")
        await safe_edit_message(query, "\n".join(lines), build_admin_blacklist_keyboard())
        return True

    if action in ("admin_diagnostics_menu", "admin_diagnostics_refresh"):
        if is_scope_limited_admin(requester_id):
            await safe_edit_message(
                query,
                "❌ Диагностика доступна только full-admin ролям.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
            )
            return True
        snapshot = db.get_diagnostics_snapshot()
        await safe_edit_message(
            query,
            format_diagnostics_text(snapshot),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin_diagnostics_refresh")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
            ]),
        )
        return True

    if action == "admin_user_hard_delete_start":
        if not is_superadmin(requester_id):
            await safe_edit_message(query, "❌ Полное удаление пользователя доступно только superadmin.")
            return True
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "hard_delete_user_id"
        await safe_edit_message(
            query,
            (
                "🧨 **ПОЛНОЕ УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЯ**\n\n"
                "Введите Telegram ID пользователя для hard delete.\n"
                "Сначала будет показан preview.\n\n"
                "⚠️ Действие необратимо."
            ),
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="hard_delete_user_cancel")]]),
        )
        return True

    if action == "hard_delete_user_cancel":
        if not is_superadmin(requester_id):
            await safe_edit_message(query, "❌ Недостаточно прав.")
            return True
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            "❌ Операция полного удаления отменена.",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
        )
        return True

    if action == "hard_delete_user_confirm":
        if not is_superadmin(requester_id):
            await safe_edit_message(query, "❌ Полное удаление пользователя доступно только superadmin.")
            return True
        if context.user_data.get("awaiting_admin_action") != "hard_delete_confirm":
            clear_admin_flow(context)
            await safe_edit_message(
                query,
                "⚠️ Сессия удаления устарела. Запустите процесс заново.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
            )
            return True

        target_id = context.user_data.get("admin_flow_target_id")
        if target_id is None:
            clear_admin_flow(context)
            await safe_edit_message(
                query,
                "⚠️ Не найден целевой user_id. Запустите процесс заново.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
            )
            return True
        if int(target_id) == get_superadmin_id():
            clear_admin_flow(context)
            await safe_edit_message(
                query,
                "❌ Нельзя удалить superadmin из базы.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
            )
            return True

        try:
            result = db.hard_delete_user(int(target_id), media_root=MEDIA_PATH)
        except Exception as e:
            clear_admin_flow(context)
            await safe_edit_message(
                query,
                f"❌ Ошибка hard delete: {e}",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
            )
            return True

        files = result.get("files", {}) or {}
        db.log_admin_audit(
            requester_id,
            "hard_delete_user",
            target_user_id=int(target_id),
            details=(
                f"deleted_total={result.get('deleted_total', 0)};"
                f"files_deleted={files.get('deleted', 0)};"
                f"files_errors={files.get('errors_count', 0)}"
            ),
        )
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            format_user_hard_delete_result(result),
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]),
        )
        return True

    if action == "team_roles_menu":
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            "👮 **Гибкие роли команды (RBAC v2)**\n\nРоли-шаблоны + permissions + scopes.",
            build_team_roles_keyboard(),
        )
        return True

    if action == "team_templates_list":
        templates = db.list_role_templates_v2()
        lines = ["🧩 **Шаблоны ролей**", ""]
        for row in templates:
            name = row[1]
            perms_count = len(db.get_role_template_permissions_v2(name))
            lines.append(f"• `{name}` — permissions: {perms_count}")
        await safe_edit_message(query, "\n".join(lines), build_team_roles_keyboard())
        return True

    if action == "team_members_list":
        members = db.list_team_members_v2(limit=200)
        lines = ["📋 **Сотрудники (v2 роли)**", ""]
        if not members:
            lines.append("Пока нет назначенных сотрудников.")
        else:
            for row in members:
                user_id, template_name, is_custom, is_active, assigned_by, _, updated_at = row
                lines.append(
                    f"• `{user_id}` — {template_name} | custom={int(is_custom)} | active={int(is_active)} | by `{assigned_by or 0}` | {updated_at}"
                )
        await safe_edit_message(query, "\n".join(lines), build_team_roles_keyboard())
        return True

    if action == "team_assign_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_assign_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для назначения роли:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action.startswith("team_assign_pick_"):
        if context.user_data.get("awaiting_admin_action") != "team_assign_pick":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия назначения роли устарела.", build_team_roles_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        template = action.replace("team_assign_pick_", "", 1)
        allowed_templates = {"admin", "manager", "support", "analyst", "viewer", "custom"}
        if template not in allowed_templates:
            await safe_edit_message(query, "❌ Неизвестный шаблон роли.", build_team_roles_keyboard())
            return True
        if template == "admin" and not is_superadmin(requester_id):
            await safe_edit_message(query, "❌ Только superadmin может назначить роль-шаблон admin.", build_team_roles_keyboard())
            return True
        if target_id == get_superadmin_id():
            await safe_edit_message(query, "❌ Нельзя менять superadmin.", build_team_roles_keyboard())
            return True
        db.assign_team_role_v2(target_id, template, assigned_by=requester_id, is_custom=(template == "custom"))
        if template != "custom":
            db.set_team_custom_permissions_v2(target_id, [])
        db.log_admin_audit(requester_id, "team_assign_role", target_user_id=target_id, details=f"template={template}")
        clear_admin_flow(context)
        await safe_edit_message(query, f"✅ Назначена роль `{template}` для `{target_id}`.", build_team_roles_keyboard())
        return True

    if action == "team_member_view_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_member_view_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для просмотра роли/permissions/scopes:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action == "team_scope_owner_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_scope_owner_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для выдачи owner-wide scope:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action == "team_scope_owner_confirm":
        if context.user_data.get("awaiting_admin_action") != "team_scope_owner_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия scope устарела.", build_team_roles_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        owner_id = context.user_data.get("admin_flow_owner_id")
        added = db.add_team_scope_v2(target_id, "owner", owner_id=owner_id, chat_id=None, created_by=requester_id)
        db.log_admin_audit(requester_id, "team_scope_owner", target_user_id=target_id, details=f"owner={owner_id};added={int(added)}")
        clear_admin_flow(context)
        await safe_edit_message(query, f"{'✅' if added else 'ℹ️'} Scope owner `{owner_id}` для `{target_id}`", build_team_roles_keyboard())
        return True

    if action == "team_scope_chat_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_scope_chat_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для выдачи owner+chat scope:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action == "team_scope_chat_confirm":
        if context.user_data.get("awaiting_admin_action") != "team_scope_chat_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия scope устарела.", build_team_roles_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        owner_id = context.user_data.get("admin_flow_owner_id")
        chat_id = context.user_data.get("admin_flow_chat_id")
        added = db.add_team_scope_v2(target_id, "chat", owner_id=owner_id, chat_id=chat_id, created_by=requester_id)
        db.log_admin_audit(requester_id, "team_scope_chat", target_user_id=target_id, details=f"owner={owner_id};chat={chat_id};added={int(added)}")
        clear_admin_flow(context)
        await safe_edit_message(query, f"{'✅' if added else 'ℹ️'} Scope owner `{owner_id}` + chat `{chat_id}` для `{target_id}`", build_team_roles_keyboard())
        return True

    if action == "team_scope_clear_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_scope_clear_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для удаления всех scopes:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action == "team_scope_clear_confirm":
        if context.user_data.get("awaiting_admin_action") != "team_scope_clear_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия очистки scope устарела.", build_team_roles_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        deleted = db.remove_team_scope_v2(target_id)
        db.log_admin_audit(requester_id, "team_scope_clear", target_user_id=target_id, details=f"deleted={deleted}")
        clear_admin_flow(context)
        await safe_edit_message(query, f"✅ Удалено scopes: {deleted} у `{target_id}`.", build_team_roles_keyboard())
        return True

    if action == "team_custom_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_custom_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для задания custom permissions:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action == "team_custom_confirm":
        if context.user_data.get("awaiting_admin_action") != "team_custom_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия custom permissions устарела.", build_team_roles_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        permissions = context.user_data.get("admin_flow_value") or []
        db.assign_team_role_v2(target_id, "custom", assigned_by=requester_id, is_custom=True)
        db.set_team_custom_permissions_v2(target_id, permissions)
        db.log_admin_audit(requester_id, "team_custom_permissions", target_user_id=target_id, details=",".join(permissions))
        clear_admin_flow(context)
        await safe_edit_message(query, f"✅ Custom permissions применены для `{target_id}`.", build_team_roles_keyboard())
        return True

    if action == "team_remove_start":
        clear_admin_flow(context)
        context.user_data["awaiting_admin_action"] = "team_remove_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника для снятия гибкой роли:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="team_roles_menu")]]),
        )
        return True

    if action == "team_remove_confirm":
        if context.user_data.get("awaiting_admin_action") != "team_remove_confirm":
            clear_admin_flow(context)
            await safe_edit_message(query, "⚠️ Сессия снятия роли устарела.", build_team_roles_keyboard())
            return True
        target_id = context.user_data.get("admin_flow_target_id")
        removed_role = db.remove_team_role_v2(target_id)
        deleted_scopes = db.remove_team_scope_v2(target_id)
        db.set_team_custom_permissions_v2(target_id, [])
        db.log_admin_audit(requester_id, "team_remove_role", target_user_id=target_id, details=f"removed={int(removed_role)};scopes={deleted_scopes}")
        clear_admin_flow(context)
        await safe_edit_message(
            query,
            f"✅ Гибкая роль снята у `{target_id}`. Удалено scopes: {deleted_scopes}.",
            build_team_roles_keyboard(),
        )
        return True

    return False


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню админ-панели."""
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return

    user_id = user.id
    if not is_admin(user_id):
        await message.reply_text("❌ Доступ запрещён")
        return

    role = get_admin_role(user_id) or "admin"
    stats = build_scoped_overall_stats(user_id)

    if is_scope_limited_admin(user_id):
        storage_text = (
            "🗄 **Хранилище:**\n"
            "🔒 Для scoped-ролей общесистемная статистика хранилища скрыта"
        )
    else:
        db_info = db.get_database_size()
        media_info = get_media_folder_size()
        storage_text = (
            "🗄 **Хранилище:**\n"
            f"💾 База данных: {db_info['size_mb']} MB\n"
            f"📝 Записей редактирования: {db_info['edits']}\n"
            f"📁 Медиа файлы: {media_info['files']} шт. ({media_info['size_mb']} MB)"
        )

    text = f"""🔒 **АДМИН-ПАНЕЛЬ**

👮 Роль: **{role}**

{format_stats(stats)}

{storage_text}
"""

    await message.reply_text(text, reply_markup=get_admin_main_keyboard(role))

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок админ-панели"""
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    
    requester_id = query.from_user.id
    
    if not is_admin(requester_id):
        await safe_edit_message(query, "❌ Доступ запрещён")
        return

    admin_chat_id = requester_id
    action = query.data or ""
    legacy_action_aliases = {
        "role_revoke": "role_revoke_start",
        "role_scope_add_owner": "scope_add_owner_start",
        "role_scope_add_chat": "scope_add_chat_start",
        "role_scope_remove": "scope_remove_one_start",
        "role_scope_view": "scope_view_start",
        "role_set_admin": "role_assign_start",
        "role_set_admin_lite": "role_assign_start",
    }
    action = legacy_action_aliases.get(action, action)
    if action == "noop":
        return

    # Public flow should be handled by dedicated callback handler
    if action.startswith("public_"):
        return

    if not can_use_admin_action(requester_id, action):
        await safe_edit_message(query, "❌ Недостаточно прав для этого действия")
        return

    owner_id_ctx, chat_id_ctx = extract_owner_chat_from_action(action)
    if owner_id_ctx is not None and not can_view_owner(requester_id, owner_id_ctx):
        await safe_edit_message(query, "❌ У вас нет доступа к этому пользователю")
        return
    if chat_id_ctx is not None and owner_id_ctx is not None and not can_view_chat(requester_id, owner_id_ctx, chat_id_ctx):
        await safe_edit_message(query, "❌ У вас нет доступа к этому чату")
        return

    if action.startswith("chat_search_page_"):
        owner_id_ctx = context.user_data.get('chat_search_owner_id')
        chat_id_ctx = context.user_data.get('chat_search_chat_id')
        if owner_id_ctx is not None and chat_id_ctx is not None:
            if not can_view_chat(requester_id, owner_id_ctx, chat_id_ctx):
                await safe_edit_message(query, "❌ У вас нет доступа к результатам этого поиска")
                return

    if await handle_admin_extensions(query, context, requester_id, action):
        return

    # ==================== ROLES & ACCESS ====================
    if action == "admin_roles_menu":
        if not can_manage_roles(requester_id) and not can_manage_scopes(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на управление ролями и доступами")
            return

        clear_role_flow(context)
        role = get_actor_role(requester_id) or "admin"
        text = (
            "👮 **РОЛИ И ДОСТУПЫ**\n\n"
            f"Ваша роль: `{role}`\n"
            f"{get_role_manage_permissions_text(role)}\n\n"
            "Модель scope:\n"
            "• owner_id -> доступ ко всем чатам пользователя\n"
            "• owner_id + chat_id -> доступ к одному чату"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Список админов", callback_data="role_list")],
            [InlineKeyboardButton("🧩 Роли команды (RBAC v2)", callback_data="team_roles_menu")],
            [InlineKeyboardButton("➕ Назначить роль", callback_data="role_assign_start")],
            [InlineKeyboardButton("👁 Посмотреть доступы по ID", callback_data="role_view_start")],
            [InlineKeyboardButton("➕ Доступ к owner (все чаты)", callback_data="scope_add_owner_start")],
            [InlineKeyboardButton("➕ Доступ к owner + chat", callback_data="scope_add_chat_start")],
            [InlineKeyboardButton("➖ Удалить конкретный scope", callback_data="scope_remove_one_start")],
            [InlineKeyboardButton("🧹 Удалить все scope пользователя", callback_data="scope_remove_all_start")],
            [InlineKeyboardButton("➖ Снять роль", callback_data="role_revoke_start")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
        ])
        await safe_edit_message(query, text, keyboard)
        return

    elif action == "role_list":
        rows = db.list_admin_roles()
        configured = get_configured_admin_ids()
        text_lines = ["👮 **АДМИНЫ И РОЛИ**", ""]
        superadmin_id = get_superadmin_id()
        if superadmin_id:
            text_lines.append(f"• `{superadmin_id}` — superadmin (config)")
        for cfg_admin in configured[1:]:
            text_lines.append(f"• `{cfg_admin}` — admin (config)")
        for user_id, role, assigned_by, assigned_at, _ in rows:
            text_lines.append(
                f"• `{user_id}` — {role} (by `{assigned_by or 0}` at {assigned_at})"
            )
        if len(text_lines) <= 2:
            text_lines.append("Пока нет назначенных ролей в БД.")
        await safe_edit_message(
            query,
            "\n".join(text_lines),
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "role_assign_start":
        if not can_manage_roles(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на назначение ролей")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "role_assign_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника, которому нужно назначить роль:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action in ("role_assign_pick_admin", "role_assign_pick_admin_lite"):
        target_id = context.user_data.get("role_flow_target_id")
        target_role = "admin" if action.endswith("_admin") else "admin_lite"
        if target_id is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните назначение заново.")
            return
        if not can_manage_roles(requester_id, target_role=target_role, target_user_id=target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя назначить эту роль выбранному пользователю")
            return
        context.user_data["role_flow_target_role"] = target_role
        context.user_data["awaiting_role_action"] = "role_assign_confirm"
        await safe_edit_message(
            query,
            (
                "Подтвердите назначение роли:\n"
                f"• пользователь: `{target_id}`\n"
                f"• роль: `{target_role}`"
            ),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить", callback_data="role_assign_confirm")],
                [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
            ]),
        )
        return

    elif action == "role_assign_confirm":
        target_id = context.user_data.get("role_flow_target_id")
        target_role = context.user_data.get("role_flow_target_role")
        if target_id is None or target_role is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните назначение заново.")
            return
        if not can_manage_roles(requester_id, target_role=target_role, target_user_id=target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя назначить роль этому пользователю")
            return
        db.set_admin_role(target_id, target_role, assigned_by=requester_id)
        if target_role == "admin":
            db.remove_admin_scope(target_id)
        clear_role_flow(context)
        await safe_edit_message(
            query,
            f"✅ Роль `{target_role}` назначена пользователю `{target_id}`",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "role_view_start" or action == "scope_view_start":
        if not can_manage_roles(requester_id) and not can_manage_scopes(requester_id):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "role_view_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID для просмотра роли и scope:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "role_revoke_start":
        if not can_manage_roles(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на снятие ролей")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "role_revoke_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника, у которого нужно снять роль:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "role_revoke_confirm":
        target_id = context.user_data.get("role_flow_target_id")
        if target_id is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        if not can_manage_roles(requester_id, target_role=None, target_user_id=target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя снять роль у этого пользователя")
            return
        removed = db.remove_admin_role(target_id)
        clear_role_flow(context)
        if removed:
            await safe_edit_message(
                query,
                f"✅ Роль снята у `{target_id}`. Scope очищен.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
            )
        else:
            await safe_edit_message(
                query,
                "ℹ️ У пользователя не было роли в БД.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
            )
        return

    elif action == "scope_add_owner_start":
        if not can_manage_scopes(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на выдачу scope")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "scope_add_owner_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника (admin_lite), которому выдаём доступ:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_add_owner_confirm":
        target_id = context.user_data.get("role_flow_target_id")
        owner_id = context.user_data.get("role_flow_owner_id")
        if target_id is None or owner_id is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        if not can_manage_scopes(requester_id, target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя выдать scope этому пользователю")
            return
        added = db.add_admin_scope(target_id, owner_id, None, created_by=requester_id)
        clear_role_flow(context)
        await safe_edit_message(
            query,
            (
                f"{'✅ Scope добавлен' if added else 'ℹ️ Такой scope уже есть'}\n"
                f"• сотрудник: `{target_id}`\n"
                f"• owner_id: `{owner_id}`\n"
                "• доступ: все чаты"
            ),
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_add_chat_start":
        if not can_manage_scopes(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на выдачу scope")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "scope_add_chat_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника (admin_lite), которому выдаём доступ:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_add_chat_confirm":
        target_id = context.user_data.get("role_flow_target_id")
        owner_id = context.user_data.get("role_flow_owner_id")
        chat_id = context.user_data.get("role_flow_chat_id")
        if target_id is None or owner_id is None or chat_id is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        if not can_manage_scopes(requester_id, target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя выдать scope этому пользователю")
            return
        added = db.add_admin_scope(target_id, owner_id, chat_id, created_by=requester_id)
        clear_role_flow(context)
        await safe_edit_message(
            query,
            (
                f"{'✅ Scope добавлен' if added else 'ℹ️ Такой scope уже есть'}\n"
                f"• сотрудник: `{target_id}`\n"
                f"• owner_id: `{owner_id}`\n"
                f"• chat_id: `{chat_id}`"
            ),
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_remove_one_start":
        if not can_manage_scopes(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на удаление scope")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "scope_remove_one_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника (admin_lite), у которого удаляем scope:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_remove_one_mode_owner":
        if context.user_data.get("awaiting_role_action") != "scope_remove_one_mode":
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        context.user_data["role_flow_remove_mode"] = "owner"
        context.user_data["role_flow_chat_id"] = None
        context.user_data["awaiting_role_action"] = "scope_remove_one_confirm"
        await safe_edit_message(
            query,
            (
                "Подтвердите удаление scope:\n"
                f"• сотрудник: `{context.user_data.get('role_flow_target_id')}`\n"
                f"• owner_id: `{context.user_data.get('role_flow_owner_id')}`\n"
                "• chat_id: все чаты owner"
            ),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить", callback_data="scope_remove_one_confirm")],
                [InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")],
            ]),
        )
        return

    elif action == "scope_remove_one_mode_chat":
        if context.user_data.get("awaiting_role_action") != "scope_remove_one_mode":
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        context.user_data["role_flow_remove_mode"] = "chat"
        context.user_data["awaiting_role_action"] = "scope_remove_one_chat"
        await safe_edit_message(
            query,
            "Введите chat_id, который нужно удалить из scope:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_remove_one_confirm":
        target_id = context.user_data.get("role_flow_target_id")
        owner_id = context.user_data.get("role_flow_owner_id")
        remove_mode = context.user_data.get("role_flow_remove_mode")
        chat_id = context.user_data.get("role_flow_chat_id")
        if target_id is None or owner_id is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        if not can_manage_scopes(requester_id, target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя удалять scope у этого пользователя")
            return

        if remove_mode == "owner":
            deleted = db.remove_admin_scope(target_id, owner_id, None)
        else:
            deleted = db.remove_admin_scope(target_id, owner_id, chat_id)
        clear_role_flow(context)
        await safe_edit_message(
            query,
            f"✅ Удалено scope: {deleted}",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_remove_all_start":
        if not can_manage_scopes(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на удаление scope")
            return
        clear_role_flow(context)
        context.user_data["awaiting_role_action"] = "scope_remove_all_target"
        await safe_edit_message(
            query,
            "Введите Telegram ID сотрудника (admin_lite), у которого нужно удалить все scope:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_roles_menu")]])
        )
        return

    elif action == "scope_remove_all_confirm":
        target_id = context.user_data.get("role_flow_target_id")
        if target_id is None:
            clear_role_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните заново.")
            return
        if not can_manage_scopes(requester_id, target_id):
            clear_role_flow(context)
            await safe_edit_message(query, "❌ Нельзя удалять scope у этого пользователя")
            return
        deleted = db.remove_admin_scope(target_id)
        clear_role_flow(context)
        await safe_edit_message(
            query,
            f"✅ Удалено scope записей: {deleted}",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_roles_menu")]])
        )
        return

    # ==================== REFERRALS (ADMIN) ====================
    elif action == "admin_referrals_menu":
        if get_actor_role(requester_id) not in ("superadmin", "admin"):
            await safe_edit_message(query, "❌ У вас нет прав на просмотр реферальной статистики")
            return
        clear_referral_flow(context)
        overview = db.get_admin_referral_overview(top_limit=10)
        await safe_edit_message(
            query,
            format_admin_referral_overview_text(overview),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔎 Поиск по user_id", callback_data="ref_admin_user_start")],
                [InlineKeyboardButton("🔁 Retry referral by payload", callback_data="ref_admin_retry_payload_start")],
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin_referrals_menu")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
            ])
        )
        return

    elif action == "ref_admin_user_start":
        if get_actor_role(requester_id) not in ("superadmin", "admin"):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        clear_referral_flow(context)
        context.user_data["awaiting_referral_action"] = "ref_admin_user_lookup"
        await safe_edit_message(
            query,
            "Введите Telegram ID пригласившего пользователя, чтобы посмотреть его рефералов:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_referrals_menu")]])
        )
        return

    elif action == "ref_admin_retry_payload_start":
        if get_actor_role(requester_id) not in ("superadmin", "admin"):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        clear_referral_flow(context)
        context.user_data["awaiting_referral_action"] = "ref_admin_retry_payload_input"
        await safe_edit_message(
            query,
            "Введите invoice_payload для retry referral bonus:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_referrals_menu")]])
        )
        return

    elif action == "ref_admin_retry_cancel":
        if get_actor_role(requester_id) not in ("superadmin", "admin"):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        clear_referral_flow(context)
        await safe_edit_message(
            query,
            "❌ Retry referral отменен.",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]])
        )
        return

    elif action == "ref_admin_retry_confirm":
        if get_actor_role(requester_id) not in ("superadmin", "admin"):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return

        invoice_payload = context.user_data.get("ref_retry_payload")
        valid_payload, error_text = validate_invoice_payload(invoice_payload)
        if error_text:
            clear_referral_flow(context)
            await safe_edit_message(
                query,
                f"{error_text}\nСессия retry сброшена.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]])
            )
            return

        payment = db.get_star_payment_by_payload(valid_payload)
        if not payment:
            db.log_referral_retry_audit(
                actor_user_id=requester_id,
                invoice_payload=valid_payload,
                payment_user_id=0,
                result_status="payment_not_found",
                invited_bonus_granted=False,
                referrer_bonus_granted=False,
            )
            clear_referral_flow(context)
            await safe_edit_message(
                query,
                "❌ Star payment не найден по этому payload.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]])
            )
            return

        payment_user_id = int(payment[1])
        if (payment[7] or "") != "paid":
            db.log_referral_retry_audit(
                actor_user_id=requester_id,
                invoice_payload=valid_payload,
                payment_user_id=payment_user_id,
                result_status=f"skipped_not_paid:{payment[7]}",
                invited_bonus_granted=False,
                referrer_bonus_granted=False,
            )
            clear_referral_flow(context)
            await safe_edit_message(
                query,
                f"ℹ️ Recovery пропущен: статус платежа `{payment[7]}`.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]])
            )
            return

        purchased_at = db._parse_dt(payment[8]) or utcnow_naive()
        try:
            referral_result = db.process_referral_bonus_for_successful_payment(
                invited_user_id=payment_user_id,
                invoice_payload=valid_payload,
                purchased_at=purchased_at,
                invited_bonus_days=REFERRAL_INVITED_BONUS_DAYS,
                referrer_bonus_days=REFERRAL_REFERRER_BONUS_DAYS,
            )
        except Exception as e:
            db.log_referral_retry_audit(
                actor_user_id=requester_id,
                invoice_payload=valid_payload,
                payment_user_id=payment_user_id,
                result_status=f"error:{type(e).__name__}",
                invited_bonus_granted=False,
                referrer_bonus_granted=False,
            )
            clear_referral_flow(context)
            await safe_edit_message(
                query,
                f"❌ Ошибка retry referral: {e}",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]])
            )
            return

        if referral_result.get("invited_bonus_granted"):
            invited_expires = referral_result.get("invited_expires_at")
            invited_until = invited_expires.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if invited_expires else "unknown"
            try:
                await context.bot.send_message(
                    payment_user_id,
                    "🎉 Вам начислен реферальный бонус.\n"
                    f"+{REFERRAL_INVITED_BONUS_DAYS} дней к подписке.\n"
                    f"Действует до: {invited_until}"
                )
            except Exception as e:
                print(f"[WARNING] Не удалось отправить retry-уведомление invited={payment_user_id}: {e}")

        if referral_result.get("referrer_bonus_granted"):
            referrer_user_id = referral_result.get("referrer_user_id")
            referrer_expires = referral_result.get("referrer_expires_at")
            referrer_until = referrer_expires.strftime("%Y-%m-%d %H:%M:%S") + " UTC" if referrer_expires else "unknown"
            try:
                await context.bot.send_message(
                    int(referrer_user_id),
                    "🎉 Ваш реферал успешно оплатил подписку.\n"
                    f"Вам начислено +{REFERRAL_REFERRER_BONUS_DAYS} дней.\n"
                    f"Действует до: {referrer_until}"
                )
            except Exception as e:
                print(f"[WARNING] Не удалось отправить retry-уведомление referrer={referrer_user_id}: {e}")

        if not referral_result.get("referral_found"):
            result_status = "no_referral"
        elif referral_result.get("invited_bonus_granted") or referral_result.get("referrer_bonus_granted"):
            result_status = "applied"
        elif referral_result.get("invited_bonus_already_granted") and referral_result.get("referrer_bonus_already_granted"):
            result_status = "already_granted"
        else:
            result_status = referral_result.get("reason", "ok")

        db.log_referral_retry_audit(
            actor_user_id=requester_id,
            invoice_payload=valid_payload,
            payment_user_id=payment_user_id,
            result_status=result_status,
            invited_bonus_granted=bool(referral_result.get("invited_bonus_granted")),
            referrer_bonus_granted=bool(referral_result.get("referrer_bonus_granted")),
        )

        clear_referral_flow(context)
        await safe_edit_message(
            query,
            format_referral_retry_result_text(valid_payload, payment, referral_result),
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_referrals_menu")]])
        )
        return

    # ==================== SUBSCRIPTIONS ====================
    elif action == "admin_subscriptions_menu":
        if not can_grant_subscriptions(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на управление подписками")
            return
        clear_subscription_flow(context)

        cancel_row = []
        if can_cancel_subscriptions(requester_id):
            cancel_row = [InlineKeyboardButton("⛔ Деактивировать", callback_data="sub_cancel")]
        text = (
            "💳 **ПОДПИСКИ**\n\n"
            "Выберите действие:\n"
            "• выдать/продлить\n"
            "• проверить статус\n"
            "• посмотреть историю"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ Выдать 30 дней", callback_data="sub_grant_30"),
                InlineKeyboardButton("➕ Выдать 90 дней", callback_data="sub_grant_90"),
            ],
            [InlineKeyboardButton("➕ Выдать 180 дней", callback_data="sub_grant_180")],
            [
                InlineKeyboardButton("🔎 Статус по ID", callback_data="sub_status"),
                InlineKeyboardButton("🧾 История по ID", callback_data="sub_history"),
            ],
            cancel_row if cancel_row else [InlineKeyboardButton("ℹ️ Деактивация недоступна для вашей роли", callback_data="noop")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")],
        ])
        await safe_edit_message(query, text, keyboard)
        return

    elif action.startswith("sub_grant_confirm_"):
        if not can_grant_subscriptions(requester_id):
            clear_subscription_flow(context)
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        days = int(action.split("_")[-1])
        target_id = context.user_data.get("sub_flow_target_id")
        comment = context.user_data.get("sub_flow_comment", "manual admin grant")
        if target_id is None:
            clear_subscription_flow(context)
            await safe_edit_message(query, "⚠️ Сессия истекла. Начните выдачу заново.")
            return
        had_active_before = db.get_active_subscription(target_id) is not None
        plan_code = f"plan_{days}" if f"plan_{days}" in SUBSCRIPTION_PLANS else "manual"
        grant = db.grant_subscription(
            user_id=target_id,
            plan_code=plan_code,
            duration_days=days,
            source="manual",
            granted_by=requester_id,
            grant_comment=comment,
        )
        await notify_manual_subscription_grant(
            context=context,
            target_user_id=target_id,
            duration_days=days,
            expires_at=grant["expires_at"],
            actor_user=query.from_user,
            comment=comment,
            was_extension=had_active_before,
        )
        clear_subscription_flow(context)
        await safe_edit_message(
            query,
            (
                f"✅ Подписка выдана пользователю `{target_id}` на {days} дней.\n"
                f"До: {grant['expires_at'].strftime('%Y-%m-%d %H:%M:%S')} UTC"
            ),
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_subscriptions_menu")]])
        )
        return

    elif action.startswith("sub_grant_"):
        if not can_grant_subscriptions(requester_id):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        days = int(action.split("_")[-1])
        clear_subscription_flow(context)
        context.user_data["awaiting_subscription_action"] = f"sub_grant_target_{days}"
        await safe_edit_message(
            query,
            f"Введите Telegram ID пользователя.\nБудет выдано {days} дней.",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_subscriptions_menu")]])
        )
        return

    elif action in ("sub_status", "sub_cancel", "sub_history"):
        if action == "sub_cancel" and not can_cancel_subscriptions(requester_id):
            await safe_edit_message(query, "❌ У вас нет прав на деактивацию подписок")
            return
        if action != "sub_cancel" and not can_grant_subscriptions(requester_id):
            await safe_edit_message(query, "❌ Недостаточно прав")
            return
        clear_subscription_flow(context)
        context.user_data["awaiting_subscription_action"] = action
        await safe_edit_message(
            query,
            "Введите Telegram ID пользователя:",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_subscriptions_menu")]])
        )
        return
    
    # ==================== СКРЫТЬ СООБЩЕНИЕ ====================
    if action == "hide_msg":
        try:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id
            )
        except Exception as e:
            print(f"[ERROR] Ошибка скрытия сообщения: {e}")
        return
    
    # ==================== ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ ЧАТА ====================
    elif action.startswith("delete_chat_confirm_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        
        if chat_id == owner_id:
            chat_display = "💾 Избранное"
        elif chat_id < 0:
            if chat_info and chat_info['username']:
                chat_display = f"👥 @{chat_info['username']}"
            else:
                chat_display = f"👥 Группа `{chat_id}`"
        else:
            if chat_info and chat_info['username']:
                chat_display = f"👤 @{chat_info['username']}"
            else:
                chat_display = f"👤 User `{chat_id}`"
        
        messages = db.get_chat_messages(chat_id, owner_id)
        msg_count = len(messages)
        
        text = f"""⚠️ **ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ**

📁 Чат: {chat_display}
🆔 ID: `{chat_id}`
📊 Сообщений: {msg_count}

❗️ Будут удалены:
• Все сообщения из базы
• История редактирований
• Медиа файлы с диска

⚠️ **Это действие необратимо!**

Подтвердить удаление?
"""
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_chat_execute_{owner_id}_{chat_id}"),
                InlineKeyboardButton("❌ Отмена", callback_data=f"view_chat_{owner_id}_{chat_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ВЫПОЛНЕНИЕ УДАЛЕНИЯ ЧАТА ====================
    elif action.startswith("delete_chat_execute_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        result = db.delete_chat_messages(chat_id, owner_id, media_root=MEDIA_PATH)
        
        text = f"""✅ **ЧАТ УДАЛЁН**

🗑 Удалено сообщений: {result['messages']}
📁 Удалено файлов: {result['files']}
"""
        if result.get("files_skipped_shared"):
            text += f"\n⏭ Пропущено shared файлов: {result['files_skipped_shared']}"
        if result.get("files_skipped_unsafe"):
            text += f"\n⚠️ Пропущено небезопасных путей: {result['files_skipped_unsafe']}"
        
        keyboard = [[InlineKeyboardButton("◀️ К пользователю", callback_data=f"view_user_{owner_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== МЕНЮ ЭКСПОРТА ЧАТА ====================
    elif action.startswith("export_chat_menu_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        
        if chat_id == owner_id:
            chat_display = "💾 Избранное"
        elif chat_id < 0:
            if chat_info and chat_info['username']:
                chat_display = f"👥 @{chat_info['username']}"
            else:
                chat_display = f"👥 Группа"
        else:
            if chat_info and chat_info['username']:
                chat_display = f"👤 @{chat_info['username']}"
            else:
                chat_display = f"👤 User"
        
        messages = db.get_chat_messages(chat_id, owner_id)
        
        text = f"""💾 **ЭКСПОРТ ЧАТА**

📁 Чат: {chat_display}
📊 Сообщений: {len(messages)}

Выберите формат экспорта:
"""
        
        keyboard = [
            [
                InlineKeyboardButton("📄 JSON", callback_data=f"export_chat_json_{owner_id}_{chat_id}"),
                InlineKeyboardButton("📋 CSV", callback_data=f"export_chat_csv_{owner_id}_{chat_id}")
            ],
            [
                InlineKeyboardButton("📝 TXT", callback_data=f"export_chat_txt_{owner_id}_{chat_id}"),
                InlineKeyboardButton("🌐 HTML", callback_data=f"export_chat_html_{owner_id}_{chat_id}")
            ],
            [
                InlineKeyboardButton("💬 Telegram HTML", callback_data=f"export_chat_tghtml_{owner_id}_{chat_id}")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data=f"view_chat_{owner_id}_{chat_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ЭКСПОРТ ЧАТА JSON ====================
    elif action.startswith("export_chat_json_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        messages = db.get_chat_messages(chat_id, owner_id)
        
        if not messages:
            await query.answer("❌ Нет сообщений для экспорта", show_alert=True)
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        chat_name = chat_info['username'] if chat_info else f"chat_{chat_id}"
        
        filename = f"chat_{chat_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            export_path = export_messages_json(messages, filename)
            if not is_safe_archive_path(export_path) or not os.path.isfile(export_path):
                print(f"[WARNING] Unsafe/missing export path blocked: {export_path}")
                await query.answer("❌ Не удалось подготовить файл", show_alert=True)
                return
            
            with open(export_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    filename=filename,
                    caption=f"💾 Экспорт чата: {chat_name}\n📊 {len(messages)} сообщений"
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить файл", show_alert=True)
                return
            
            await query.answer("✅ Файл отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Экспорт JSON: {e}")
            await query.answer("❌ Ошибка экспорта", show_alert=True)
    
    # ==================== ЭКСПОРТ ЧАТА CSV ====================
    elif action.startswith("export_chat_csv_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        messages = db.get_chat_messages(chat_id, owner_id)
        
        if not messages:
            await query.answer("❌ Нет сообщений для экспорта", show_alert=True)
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        chat_name = chat_info['username'] if chat_info else f"chat_{chat_id}"
        
        filename = f"chat_{chat_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            export_path = export_messages_csv(messages, filename)
            if not is_safe_archive_path(export_path) or not os.path.isfile(export_path):
                print(f"[WARNING] Unsafe/missing export path blocked: {export_path}")
                await query.answer("❌ Не удалось подготовить файл", show_alert=True)
                return
            
            with open(export_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    filename=filename,
                    caption=f"💾 Экспорт чата: {chat_name}\n📊 {len(messages)} сообщений"
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить файл", show_alert=True)
                return
            
            await query.answer("✅ Файл отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Экспорт CSV: {e}")
            await query.answer("❌ Ошибка экспорта", show_alert=True)
    
    # ==================== ЭКСПОРТ ЧАТА TXT ====================
    elif action.startswith("export_chat_txt_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        messages = db.get_chat_messages(chat_id, owner_id)
        
        if not messages:
            await query.answer("❌ Нет сообщений для экспорта", show_alert=True)
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        chat_name = chat_info['username'] if chat_info else f"chat_{chat_id}"
        
        filename = f"chat_{chat_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        try:
            export_path = export_messages_txt(messages, filename)
            if not is_safe_archive_path(export_path) or not os.path.isfile(export_path):
                print(f"[WARNING] Unsafe/missing export path blocked: {export_path}")
                await query.answer("❌ Не удалось подготовить файл", show_alert=True)
                return
            
            with open(export_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    filename=filename,
                    caption=f"💾 Экспорт чата: {chat_name}\n📊 {len(messages)} сообщений"
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить файл", show_alert=True)
                return
            
            await query.answer("✅ Файл отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Экспорт TXT: {e}")
            await query.answer("❌ Ошибка экспорта", show_alert=True)
    
    # ==================== ЭКСПОРТ ЧАТА HTML ====================
    elif action.startswith("export_chat_html_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        messages = db.get_chat_messages(chat_id, owner_id)
        
        if not messages:
            await query.answer("❌ Нет сообщений для экспорта", show_alert=True)
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        chat_name = chat_info['username'] if chat_info else f"Чат {chat_id}"
        
        filename = f"chat_{chat_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        try:
            export_path = export_messages_html(messages, filename, chat_name)
            if not is_safe_archive_path(export_path) or not os.path.isfile(export_path):
                print(f"[WARNING] Unsafe/missing export path blocked: {export_path}")
                await query.answer("❌ Не удалось подготовить файл", show_alert=True)
                return
            
            with open(export_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    filename=filename,
                    caption=f"💾 Экспорт чата: {chat_name}\n📊 {len(messages)} сообщений"
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить файл", show_alert=True)
                return
            
            await query.answer("✅ Файл отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Экспорт HTML: {e}")
            await query.answer("❌ Ошибка экспорта", show_alert=True)
    
    # ==================== ЭКСПОРТ ЧАТА TELEGRAM HTML ====================
    elif action.startswith("export_chat_tghtml_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        messages = db.get_chat_messages(chat_id, owner_id)
        
        if not messages:
            await query.answer("❌ Нет сообщений для экспорта", show_alert=True)
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        chat_name = chat_info['username'] if chat_info else f"Чат {chat_id}"
        
        filename = f"chat_{chat_id}_telegram_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        try:
            export_path = export_telegram_html(messages, filename, chat_name)
            if not is_safe_archive_path(export_path) or not os.path.isfile(export_path):
                print(f"[WARNING] Unsafe/missing export path blocked: {export_path}")
                await query.answer("❌ Не удалось подготовить файл", show_alert=True)
                return
            
            with open(export_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    filename=filename,
                    caption=f"💬 Экспорт в Telegram формате\n📁 {chat_name}\n📊 {len(messages)} сообщений"
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить файл", show_alert=True)
                return
            
            await query.answer("✅ Файл отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Экспорт Telegram HTML: {e}")
            await query.answer("❌ Ошибка экспорта", show_alert=True)
    
    # ==================== ПОИСК В КОНКРЕТНОМ ЧАТЕ ====================

    elif action.startswith("search_in_chat_"):
        parts = action.split("_")
        try:
            owner_id = int(parts[3])
            chat_id = int(parts[4])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        
        if chat_id == owner_id:
            chat_display = "💾 Избранное"
        elif chat_id < 0:
            if chat_info and chat_info['username']:
                chat_display = f"👥 @{chat_info['username']}"
            else:
                chat_display = f"👥 Группа"
        else:
            if chat_info and chat_info['username']:
                chat_display = f"👤 @{chat_info['username']}"
            else:
                chat_display = f"👤 User"
        
        text = f"""🔍 **ПОИСК В ЧАТЕ**

📁 Чат: {chat_display}

Отправьте текст для поиска в этом чате:
"""
        
        # Устанавливаем флаг ожидания
        context.user_data['awaiting_chat_search'] = True
        context.user_data['chat_search_owner_id'] = owner_id
        context.user_data['chat_search_chat_id'] = chat_id
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data=f"view_chat_{owner_id}_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== РЕЗУЛЬТАТЫ ПОИСКА В ЧАТЕ - ПАГИНАЦИЯ ====================
    elif action.startswith("chat_search_page_"):
        parts = action.split("_")
        try:
            page = int(parts[3])
        except Exception:
            await query.answer("❌ Некорректная страница", show_alert=True)
            return
        
        owner_id = context.user_data.get('chat_search_owner_id')
        chat_id = context.user_data.get('chat_search_chat_id')
        search_text = context.user_data.get('chat_search_text', '')
        messages = context.user_data.get('chat_search_results', [])
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return

        if not messages:
            await query.answer("❌ Результаты поиска устарели", show_alert=True)
            return
        messages = [msg for msg in messages if can_view_chat(requester_id, msg[2], msg[1])]
        context.user_data['chat_search_results'] = messages
        if not messages:
            await query.answer("❌ Нет данных в доступном scope", show_alert=True)
            return
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        
        if page < 0:
            page = 0
        if page >= total_pages:
            page = total_pages - 1
        
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(messages))
        
        chat_info = db.get_chat_info(chat_id, owner_id)
        
        if chat_id == owner_id:
            chat_display = "💾 Избранное"
        elif chat_id < 0:
            if chat_info and chat_info['username']:
                chat_display = f"👥 @{chat_info['username']}"
            else:
                chat_display = f"👥 Группа"
        else:
            if chat_info and chat_info['username']:
                chat_display = f"👤 @{chat_info['username']}"
            else:
                chat_display = f"👤 User"
        
        text = f"""🔍 **ПОИСК В ЧАТЕ: '{search_text}'**

📁 Чат: {chat_display}
📊 Найдено: {len(messages)}
📄 Страница {page + 1}/{total_pages}

"""
        
        keyboard = []
        
        for msg in messages[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            msg_id, chat_id_msg, owner_id_msg = msg[0], msg[1], msg[2]
            keyboard.append([
                InlineKeyboardButton("📄 Текст", callback_data=f"fulltext_{msg_id}_{chat_id_msg}_{owner_id_msg}"),
                InlineKeyboardButton("📋 Метаданные", callback_data=f"metadata_{msg_id}_{chat_id_msg}_{owner_id_msg}")
            ])
        
        # Навигация
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"chat_search_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"chat_search_page_{page}"))
        nav_buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"chat_search_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"chat_search_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data=f"search_in_chat_{owner_id}_{chat_id}")])
        keyboard.append([InlineKeyboardButton("◀️ К чату", callback_data=f"view_chat_{owner_id}_{chat_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ЗАГРУЗКА ПОЛНОГО ТЕКСТА ====================
    if action.startswith("fulltext_"):
        parts = action.split("_")
        try:
            msg_id = int(parts[1])
            chat_id = int(parts[2])
            owner_id = int(parts[3])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        msg_data = db.get_message(msg_id, chat_id, owner_id)
        
        if not msg_data:
            await query.answer("❌ Сообщение не найдено", show_alert=True)
            return
        
        msg_id_db, chat_id_db, owner_id_db, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, reply_to = msg_data
        
        # ✅ ДАТА+ВРЕМЯ MSK
        full_datetime = format_datetime_msk(msg_date)
        
        full_text = f"""📄 **ПОЛНЫЙ ТЕКСТ СООБЩЕНИЯ**

👤 От: **{username}**
🆔 ID: `{user_id_msg}`
💬 Chat: `{chat_id}`
📅 Дата: {full_datetime}

{'🗑 **УДАЛЕНО**' if is_deleted else ''}
{'✏️ **ИЗМЕНЕНО**' if is_edited else ''}

━━━━━━━━━━━━━━━━

{text_msg or '(нет текста)'}
"""
        
        try:
            await send_admin_notification(context.bot, full_text, admin_id=admin_chat_id)
            await query.answer("✅ Полный текст отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] fulltext send failed: {e}")
            await query.answer("❌ Ошибка отправки", show_alert=True)
        
        return
    
    # ==================== МЕТАДАННЫЕ СООБЩЕНИЯ ====================

    if action.startswith("metadata_"):
        parts = action.split("_")
        try:
            msg_id = int(parts[1])
            chat_id = int(parts[2])
            owner_id = int(parts[3])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        msg_data = db.get_message(msg_id, chat_id, owner_id)
        
        if not msg_data:
            await query.answer("❌ Сообщение не найдено", show_alert=True)
            return
        
        msg_id_db, chat_id_db, owner_id_db, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, original_text, reply_to = msg_data
        
        # ✅ ДАТА+ВРЕМЯ MSK
        full_datetime = format_datetime_msk(msg_date)
        
        # Получение цепочки replies
        reply_chain = db.get_reply_chain(msg_id, chat_id, owner_id, limit=5)
        
        # Получение истории изменений
        edit_history = db.get_edit_history(msg_id, chat_id, owner_id)
        
        metadata_text = f"""📋 **МЕТАДАННЫЕ СООБЩЕНИЯ**

🆔 Message ID: `{msg_id}`
💬 Chat ID: `{chat_id}`
👤 User ID: `{user_id_msg}`
📱 Username: @{username or 'нет'}
🕐 Дата: {full_datetime}

📝 **Текст:**
{truncate_text(text_msg, 200) if text_msg else '(нет текста)'}

📎 **Медиа:** {media_type or 'нет'}
📁 **Файл:** {media_path.split('/')[-1] if media_path else 'нет'}

🗑 **Удалено:** {'Да' if is_deleted else 'Нет'}
✏️ **Изменено:** {'Да' if is_edited else 'Нет'}
↩️ **Reply на:** {reply_to or 'нет'}
"""
        
        # История изменений
        if edit_history:
            metadata_text += f"\n📝 **История изменений ({len(edit_history)}):**\n"
            for idx, (old, new, edited_at) in enumerate(edit_history[:3], 1):
                edit_time = format_datetime_msk(edited_at)
                metadata_text += f"\n{idx}. `{edit_time}`"
                metadata_text += f"\n   Было: {truncate_text(old, 30)}"
                metadata_text += f"\n   Стало: {truncate_text(new, 30)}\n"
        
        # Цепочка replies
        if len(reply_chain) > 1:
            metadata_text += f"\n↩️ **Цепочка ответов ({len(reply_chain)}):**\n"
            for idx, rmsg in enumerate(reply_chain[:3], 1):
                r_username = rmsg[4]
                r_text = rmsg[5]
                metadata_text += f"\n{idx}. **{r_username}**: {truncate_text(r_text, 25)}"
        
        keyboard = []
        
        # Кнопка полного текста
        if text_msg and len(text_msg) > 200:
            keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=f"fulltext_{msg_id}_{chat_id}_{owner_id}")])
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, metadata_text, reply_markup)
        return
    
        # ==================== ЗАГРУЗКА МЕДИА ====================
    if action.startswith("download_"):
        parts = action.split("_")
        try:
            msg_id = int(parts[1])
            chat_id = int(parts[2])
            owner_id = int(parts[3])
        except Exception:
            await query.answer("❌ Некорректные параметры", show_alert=True)
            return
        if not await guard_admin_output_access(
            query, requester_id, action_name=action, owner_id=owner_id, chat_id=chat_id
        ):
            return
        
        msg_data = db.get_message(msg_id, chat_id, owner_id)
        
        if not msg_data:
            await query.answer("❌ Сообщение не найдено", show_alert=True)
            return
        
        _, _, _, user_id_msg, username, text_msg, media_type, media_path, msg_date, is_deleted, is_edited, _, _ = msg_data
        
        if not media_path or not is_safe_media_path(media_path) or not os.path.exists(media_path):
            await query.answer("❌ Файл не найден или недоступен", show_alert=True)
            return
        
        try:
            full_datetime = format_datetime_msk(msg_date)
            
            caption = f"""📥 **ЗАГРУЖЕНО МЕДИА**

👤 От: {username}
📱 Чат ID: `{chat_id}`
📅 {full_datetime}
📎 Тип: {media_type}
"""
            if text_msg:
                caption += f"\n💬 Текст: {truncate_text(text_msg, 100)}"
            
            if is_deleted:
                caption += "\n🗑 **УДАЛЕНО**"
            
            print(f"[DEBUG] media_type = '{media_type}' (len={len(media_type) if media_type else 0})")
            
            with open(media_path, 'rb') as f:
                media_type_normalized = media_type.strip().lower() if media_type else ""
                
                if media_type_normalized in ["video_note", "videonote", "saved_video_note", "saved_videonote"]:
                    print(f"[DEBUG] Отправляю как video_note")
                    await context.bot.send_video_note(admin_chat_id, video_note=f)
                    await send_admin_notification(context.bot, caption, admin_id=admin_chat_id)
                elif "photo" in media_type_normalized:
                    await context.bot.send_photo(admin_chat_id, photo=f, caption=caption)
                elif "voice" in media_type_normalized:
                    await context.bot.send_voice(admin_chat_id, voice=f, caption=caption)
                elif "audio" in media_type_normalized:
                    await context.bot.send_audio(admin_chat_id, audio=f, caption=caption)
                elif "video" in media_type_normalized:
                    print(f"[DEBUG] Отправляю как video")
                    await context.bot.send_video(admin_chat_id, video=f, caption=caption)
                else:
                    await context.bot.send_document(admin_chat_id, document=f, caption=caption)
            
            if text_msg and len(text_msg) > 100:
                await send_admin_notification(context.bot, f"📄 **Полный текст:**\n\n{text_msg}", admin_id=admin_chat_id)
            
            await query.answer("✅ Медиа отправлено в чат!", show_alert=True)
        
        except Exception as e:
            print(f"[ERROR] Ошибка загрузки медиа: {e}")
            await query.answer("❌ Ошибка загрузки", show_alert=True)
        
        return
    
    # Парсинг пагинации
    page = 0
    if "_page_" in action:
        parts = action.split("_page_")
        action_base = parts[0]
        page_info = parts[1].split("_")
        try:
            page = int(page_info[0])
        except Exception:
            await query.answer("❌ Некорректная страница", show_alert=True)
            return
        
        if not (action_base.startswith("view_date_") 
                or action_base.startswith("view_chat_") 
                or action_base.startswith("chat_media_")
                or action_base == "search"):
            action = action_base

    if is_sensitive_admin_output_action(action):
        guard_owner_id, guard_chat_id = extract_owner_chat_from_action(action)
        if action.startswith("chat_search_page_"):
            guard_owner_id = context.user_data.get('chat_search_owner_id')
            guard_chat_id = context.user_data.get('chat_search_chat_id')
        if not await guard_admin_output_access(
            query,
            requester_id,
            action_name=action,
            owner_id=guard_owner_id,
            chat_id=guard_chat_id,
        ):
            return
    
    # ==================== ГЛАВНОЕ МЕНЮ ====================
    if action == "admin_back":
        clear_role_flow(context)
        clear_subscription_flow(context)
        clear_referral_flow(context)
        clear_public_flow(context)
        stats = build_scoped_overall_stats(requester_id)

        if is_scope_limited_admin(requester_id):
            storage_text = (
                "🗄 **Хранилище:**\n"
                "🔒 Для scoped-ролей общесистемная статистика хранилища скрыта"
            )
        else:
            db_info = db.get_database_size()
            media_info = get_media_folder_size()
            storage_text = (
                "🗄 **Хранилище:**\n"
                f"💾 База данных: {db_info['size_mb']} MB\n"
                f"📝 Записей редактирования: {db_info['edits']}\n"
                f"📁 Медиа файлы: {media_info['files']} шт. ({media_info['size_mb']} MB)"
            )
        
        text = f"""🔐 **АДМИН-ПАНЕЛЬ**

{format_stats(stats)}

{storage_text}
"""
        
        reply_markup = get_admin_main_keyboard(get_admin_role(requester_id) or "admin")
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ФИЛЬТРЫ ПО ВРЕМЕНИ ====================
    elif action == "admin_time_filters":
        text = "⏱ **ФИЛЬТРЫ ПО ВРЕМЕНИ**\n\nВыбери диапазон:"
        
        keyboard = [
            [
                InlineKeyboardButton("🕐 Последние 24 часа", callback_data="time_24h"),
                InlineKeyboardButton("📅 Последние 7 дней", callback_data="time_7d")
            ],
            [
                InlineKeyboardButton("📆 Последние 30 дней", callback_data="time_30d"),
                InlineKeyboardButton("📊 Сравнение", callback_data="time_compare")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "time_24h":
        stats = build_scoped_time_stats(requester_id, hours=24)
        messages = db.get_messages_last_hours(hours=24, limit=200)  # Увеличено количество сообщений
        messages = filter_messages_by_scope(messages, requester_id)
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1) if messages else 1
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = format_time_range_stats(stats, "ПОСЛЕДНИЕ 24 ЧАСА") + "\n\n"
        
        if messages:
            text += f"**📋 СООБЩЕНИЯ:** {len(messages)}\n"
            text += f"📄 Страница {page + 1}/{total_pages}\n\n"
            
            keyboard = []
            
            for msg in messages[start_idx:end_idx]:
                preview = format_message_preview(msg, add_full_button=True)
                text += preview['text'] + "\n"
                
                if preview['has_full_text']:
                    keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        else:
            text += "Нет сообщений"
            keyboard = []
        
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"time_24h_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"time_24h_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"time_24h_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_time_filters")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "time_7d":
        stats = build_scoped_time_stats(requester_id, days=7)
        messages = db.get_messages_last_days(days=7, limit=300)  # Увеличено количество сообщений
        messages = filter_messages_by_scope(messages, requester_id)
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1) if messages else 1
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = format_time_range_stats(stats, "ПОСЛЕДНИЕ 7 ДНЕЙ") + "\n\n"
        
        if messages:
            text += f"**📋 СООБЩЕНИЯ:** {len(messages)}\n"
            text += f"📄 Страница {page + 1}/{total_pages}\n\n"
            
            keyboard = []
            
            for msg in messages[start_idx:end_idx]:
                preview = format_message_preview(msg, add_full_button=True)
                text += preview['text'] + "\n"
                
                if preview['has_full_text']:
                    keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        else:
            text += "Нет сообщений"
            keyboard = []
        
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"time_7d_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"time_7d_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"time_7d_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_time_filters")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "time_30d":
        stats = build_scoped_time_stats(requester_id, days=30)
        messages = db.get_messages_last_days(days=30, limit=500)  # Увеличено количество сообщений
        messages = filter_messages_by_scope(messages, requester_id)
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1) if messages else 1
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = format_time_range_stats(stats, "ПОСЛЕДНИЕ 30 ДНЕЙ") + "\n\n"
        
        if messages:
            text += f"**📋 СООБЩЕНИЯ:** {len(messages)}\n"
            text += f"📄 Страница {page + 1}/{total_pages}\n\n"
            
            keyboard = []
            
            for msg in messages[start_idx:end_idx]:
                preview = format_message_preview(msg, add_full_button=True)
                text += preview['text'] + "\n"
                
                if preview['has_full_text']:
                    keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        else:
            text += "Нет сообщений"
            keyboard = []
        
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"time_30d_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"time_30d_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"time_30d_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_time_filters")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "time_compare":
        stats_24h = build_scoped_time_stats(requester_id, hours=24)
        stats_7d = build_scoped_time_stats(requester_id, days=7)
        stats_30d = build_scoped_time_stats(requester_id, days=30)
        
        text = f"""📊 **СРАВНЕНИЕ ПЕРИОДОВ**

🕐 **Последние 24 часа:**
💬 Сообщений: {stats_24h[0]}
🗑 Удалено: {stats_24h[1]}
✏️ Изменено: {stats_24h[2]}

📅 **Последние 7 дней:**
💬 Сообщений: {stats_7d[0]}
🗑 Удалено: {stats_7d[1]}
✏️ Изменено: {stats_7d[2]}

📆 **Последние 30 дней:**
💬 Сообщений: {stats_30d[0]}
🗑 Удалено: {stats_30d[1]}
✏️ Изменено: {stats_30d[2]}

📈 **Средние показатели:**
• В день: {stats_30d[0] // 30 if stats_30d[0] else 0} сообщений
• В неделю: {stats_7d[0]} сообщений
• В час: {stats_24h[0] // 24 if stats_24h[0] else 0} сообщений
"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="time_compare")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_time_filters")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== СТАТИСТИКА ====================
    elif action == "admin_stats_menu":
        stats = build_scoped_overall_stats(requester_id)
        media_stats = build_scoped_media_stats(requester_id) if is_scope_limited_admin(requester_id) else db.get_media_stats()
        
        text = f"""{format_stats(stats)}
📎 **МЕДИА ПО ТИПАМ:**
"""
        
        for media_type, count in media_stats:
            emoji = {
                'photo': '📸',
                'video': '📹',
                'voice': '🎤',
                'video_note': '⭕️',
                'audio': '🎵',
                'document': '📄',
                'saved_photo': '💾📸',
                'saved_video': '💾📹',
                'saved_voice': '💾🎤',
                'saved_video_note': '💾⭕️'
            }.get(media_type, '📎')
            
            text += f"\n{emoji} {media_type}: **{count}**"
        
        keyboard = [
            [
                InlineKeyboardButton("🏆 Топ по сообщениям", callback_data="admin_top_messages"),
                InlineKeyboardButton("🗑 Топ по удалениям", callback_data="admin_top_deleted")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "admin_top_messages":
        if is_scope_limited_admin(requester_id):
            await safe_edit_message(
                query,
                "❌ Для scoped-ролей глобальный TOP недоступен.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_stats_menu")]]),
            )
            return
        top_users = db.get_top_users_by_messages(10)
        
        text = "🏆 **ТОП-10 ПО СООБЩЕНИЯМ**\n\n"
        
        medals = ['🥇', '🥈', '🥉'] + ['👤'] * 7
        
        for idx, user in enumerate(top_users, 1):
            user_id, username, first_name, total_msg, total_del, total_edit = user
            text += f"{medals[idx-1]} **{idx}.** {first_name} (@{username or 'нет'})\n"
            text += f"   💬 {total_msg} | 🗑 {total_del} | ✏️ {total_edit}\n\n"
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_stats_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "admin_top_deleted":
        if is_scope_limited_admin(requester_id):
            await safe_edit_message(
                query,
                "❌ Для scoped-ролей глобальный TOP недоступен.",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_stats_menu")]]),
            )
            return
        top_users = db.get_top_users_by_deleted(10)
        
        text = "🗑 **ТОП-10 ПО УДАЛЕНИЯМ**\n\n"
        
        for idx, user in enumerate(top_users, 1):
            user_id, username, first_name, total_del, total_msg = user
            percentage = round((total_del / total_msg * 100) if total_msg > 0 else 0, 1)
            text += f"**{idx}.** {first_name} (@{username or 'нет'})\n"
            text += f"   🗑 {total_del} из {total_msg} ({percentage}%)\n\n"
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_stats_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    # ==================== ПОСЛЕДНЯЯ АКТИВНОСТЬ ====================
    elif action == "admin_activity":
        recent = db.get_recent_activity(200)  # Увеличено количество сообщений
        recent = filter_messages_by_scope(recent, requester_id)
        
        total_pages = max((len(recent) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"⚡️ **ПОСЛЕДНЯЯ АКТИВНОСТЬ**\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in recent[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            if preview['has_full_text']:
                keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        
        if not recent:
            text += "Нет активности"
        
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"admin_activity_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton("🔄 Обновить", callback_data=f"admin_activity_page_{page}"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"admin_activity_page_{page+1}"))
        
        keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ Главное меню", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== УДАЛЁННЫЕ СООБЩЕНИЯ ====================
    elif action == "admin_deleted":
        deleted = db.get_deleted_recent(200)  # Увеличено количество сообщений
        deleted = filter_messages_by_scope(deleted, requester_id)
        
        total_pages = max((len(deleted) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"🗑 **ПОСЛЕДНИЕ УДАЛЁННЫЕ**\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in deleted[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            if preview['has_full_text']:
                keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        
        if not deleted:
            text += "Нет удалённых сообщений"
        
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"admin_deleted_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton("🔄 Обновить", callback_data=f"admin_deleted_page_{page}"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"admin_deleted_page_{page+1}"))
        
        keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ Главное меню", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
        # ==================== ПОИСК ====================
    elif action == "admin_search_menu":
        text = "🔍 **ПОИСК И ФИЛЬТРЫ**\n\nВыбери тип поиска:"
        
        keyboard = [
            [
                InlineKeyboardButton("📝 По тексту", callback_data="search_text"),
                InlineKeyboardButton("📸 Только медиа", callback_data="search_media")
            ],
            [
                InlineKeyboardButton("🗑 Только удалённые", callback_data="search_deleted"),
                InlineKeyboardButton("✏️ Только изменённые", callback_data="search_edited")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ПОИСК ПО ТЕКСТУ ====================
    elif action == "search_text":
        # ✅ Устанавливаем флаг ожидания текста
        context.user_data['awaiting_search'] = True
        
        text = """🔍 **ПОИСК ПО ТЕКСТУ**

Отправь текст для поиска (минимум 2 символа).

**Примеры:**
• `привет` - найдет все сообщения с "привет"
• `важный документ` - найдет фразу полностью
• `@username` - найдет упоминания

💡 Поиск ищет в:
  • Текущем тексте сообщений
  • Оригинальных текстах (до редактирования)
  • Истории всех изменений

❗️ Поиск регистронезависимый"""
        
        keyboard = [
            [InlineKeyboardButton("❌ Отмена", callback_data="search_cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
        await query.answer("✍️ Жду текст для поиска...", show_alert=False)
    
    elif action == "search_cancel":
        # ✅ Сброс флагов поиска
        context.user_data['awaiting_search'] = False
        context.user_data.pop('search_results', None)
        context.user_data.pop('search_text', None)
        context.user_data.pop('search_page', None)
        
        await query.answer("❌ Поиск отменён", show_alert=True)
        
        # Возврат в меню поиска
        text = "🔍 **ПОИСК И ФИЛЬТРЫ**\n\nВыбери тип поиска:"
        
        keyboard = [
            [
                InlineKeyboardButton("📝 По тексту", callback_data="search_text"),
                InlineKeyboardButton("📸 Только медиа", callback_data="search_media")
            ],
            [
                InlineKeyboardButton("🗑 Только удалённые", callback_data="search_deleted"),
                InlineKeyboardButton("✏️ Только изменённые", callback_data="search_edited")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action.startswith("search_page_"):
        # ✅ Пагинация результатов текстового поиска
        page_num = int(action.split("_")[-1])
        await send_search_page(query, context, page=page_num, edit=True)
    
    elif action == "search_refresh":
        # ✅ Обновление текущей страницы
        current_page = context.user_data.get('search_page', 0)
        await send_search_page(query, context, page=current_page, edit=True)
    
    # ==================== ФИЛЬТРОВАННЫЕ ПОИСКИ ====================
    elif action == "search_media":
        results = db.search_advanced(media_only=True, limit=500)
        results = filter_messages_by_scope(results, requester_id)
        
        if not results:
            text = "📸 **МЕДИА**\n\n❌ Медиа не найдено"
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await safe_edit_message(query, text, reply_markup)
            return
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
        
        text = f"📸 **МЕДИА** (найдено: {len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
            time_str = format_datetime_msk(msg[8])
            username = msg[4]
            media_type = msg[6]
            
            # Эмодзи для типа медиа
            media_emoji = {
                'photo': '📸',
                'video': '📹',
                'voice': '🎤',
                'video_note': '⭕️',
                'audio': '🎵',
                'document': '📄'
            }.get(media_type, '📎')
            
            text += f"{media_emoji} `{time_str}` **{username}**\n"
            
            # Кнопки для каждого медиа
            keyboard.append([
                InlineKeyboardButton(f"📥 {time_str}", callback_data=f"download_{msg_id}_{chat_id}_{owner_id}"),
                InlineKeyboardButton("📋", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
            ])
        
        # Навигация
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"search_media_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"search_media_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "search_deleted":
        results = db.search_advanced(deleted_only=True, limit=500)
        results = filter_messages_by_scope(results, requester_id)
        
        if not results:
            text = "🗑 **УДАЛЁННЫЕ**\n\n❌ Удалённых сообщений нет"
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await safe_edit_message(query, text, reply_markup)
            return
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
        
        text = f"🗑 **УДАЛЁННЫЕ** (найдено: {len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
            time_str = format_datetime_msk(msg[8])
            
            keyboard.append([
                InlineKeyboardButton(f"📄 {time_str}", callback_data=f"fulltext_{msg_id}_{chat_id}_{owner_id}"),
                InlineKeyboardButton("📋", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
            ])
        
        # Навигация
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"search_deleted_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"search_deleted_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "search_edited":
        results = db.search_advanced(edited_only=True, limit=500)
        results = filter_messages_by_scope(results, requester_id)
        
        if not results:
            text = "✏️ **ИЗМЕНЁННЫЕ**\n\n❌ Изменённых сообщений нет"
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await safe_edit_message(query, text, reply_markup)
            return
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
        
        text = f"✏️ **ИЗМЕНЁННЫЕ** (найдено: {len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
            time_str = format_datetime_msk(msg[8])
            
            keyboard.append([
                InlineKeyboardButton(f"📄 {time_str}", callback_data=f"fulltext_{msg_id}_{chat_id}_{owner_id}"),
                InlineKeyboardButton("📋", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
            ])
        
        # Навигация
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"search_edited_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"search_edited_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
        # ==================== ПАГИНАЦИЯ ПОИСКА ПО ТЕКСТУ ====================
    elif action.startswith("search_page_"):
        
        if 'search_results' not in context.user_data:

            await query.answer("❌ Нет активного поиска", show_alert=True)
            return
        
        # ✅ ИСПОЛЬЗУЕМ query.data (оригинальный action ДО изменений)
        original_action = query.data
        
        try:
            page = int(original_action.split('_')[2])
        except:
            page = 0
        
        search_results = context.user_data['search_results']
        search_text = context.user_data['search_text']
        
        total_pages = max((len(search_results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        
        if page >= total_pages:
            await query.answer("ℹ️ Это последняя страница", show_alert=True)
            return
        
        context.user_data['search_page'] = page
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(search_results))
        
        text = format_search_results(search_results[start_idx:end_idx], search_text, page, total_pages)
        
        keyboard = []
        
        # Кнопки для полного текста
        for msg in search_results[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            if preview['has_full_text']:
                keyboard.append([InlineKeyboardButton(
                    f"📄 Полный текст {format_datetime_msk(msg[8])}",
                    callback_data=preview['full_text_data']
                )])
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"search_page_{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"search_page_{page}"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️ Далее", callback_data=f"search_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="search_text")])
        keyboard.append([InlineKeyboardButton("◀️ В меню", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message(query, text, reply_markup)
    # ==================== ПАГИНАЦИЯ search_media ====================
    elif action.startswith("search_media_page_"):
        # ✅ ИСПОЛЬЗУЕМ query.data (оригинальный action)
        original_action = query.data
        page = int(original_action.split('_')[-1])
        
        results = db.search_advanced(media_only=True, limit=500)
        results = filter_messages_by_scope(results, requester_id)
        
        if not results:
            await query.answer("❌ Медиа не найдено", show_alert=True)
            return
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
        
        text = f"📸 **МЕДИА** (найдено: {len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
            time_str = format_datetime_msk(msg[8])
            username = msg[4]
            media_type = msg[6]
            
            media_emoji = {
                'photo': '📸',
                'video': '📹',
                'voice': '🎤',
                'video_note': '⭕️',
                'audio': '🎵',
                'document': '📄'
            }.get(media_type, '📎')
            
            text += f"{media_emoji} `{time_str}` **{username}**\n"
            
            keyboard.append([
                InlineKeyboardButton(f"📥 {time_str}", callback_data=f"download_{msg_id}_{chat_id}_{owner_id}"),
                InlineKeyboardButton("📋", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
            ])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"search_media_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data=f"search_media_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"search_media_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ПАГИНАЦИЯ search_deleted ====================
    elif action.startswith("search_deleted_page_"):
        # ✅ ИСПОЛЬЗУЕМ query.data (оригинальный action)
        original_action = query.data
        page = int(original_action.split('_')[-1])
        
        results = db.search_advanced(deleted_only=True, limit=500)
        results = filter_messages_by_scope(results, requester_id)
        
        if not results:
            await query.answer("❌ Удалённых сообщений нет", show_alert=True)
            return
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
        
        text = f"🗑 **УДАЛЁННЫЕ** (найдено: {len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
            time_str = format_datetime_msk(msg[8])
            
            keyboard.append([
                InlineKeyboardButton(f"📄 {time_str}", callback_data=f"fulltext_{msg_id}_{chat_id}_{owner_id}"),
                InlineKeyboardButton("📋", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
            ])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"search_deleted_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data=f"search_deleted_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"search_deleted_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ПАГИНАЦИЯ search_edited ====================
    elif action.startswith("search_edited_page_"):
        # ✅ ИСПОЛЬЗУЕМ query.data (оригинальный action)
        original_action = query.data
        page = int(original_action.split('_')[-1])
        
        results = db.search_advanced(edited_only=True, limit=500)
        results = filter_messages_by_scope(results, requester_id)
        
        if not results:
            await query.answer("❌ Изменённых сообщений нет", show_alert=True)
            return
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(results))
        
        text = f"✏️ **ИЗМЕНЁННЫЕ** (найдено: {len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            msg_id, chat_id, owner_id = msg[0], msg[1], msg[2]
            time_str = format_datetime_msk(msg[8])
            
            keyboard.append([
                InlineKeyboardButton(f"📄 {time_str}", callback_data=f"fulltext_{msg_id}_{chat_id}_{owner_id}"),
                InlineKeyboardButton("📋", callback_data=f"metadata_{msg_id}_{chat_id}_{owner_id}")
            ])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"search_edited_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data=f"search_edited_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"search_edited_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_search_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "search_next_page":

        if 'search_results' not in context.user_data:
            await query.answer("❌ Нет активного поиска", show_alert=True)
            return
        
        current_page = context.user_data.get('search_page', 0)
        next_page = current_page + 1
        
        # Вызываем ту же функцию с новым action
        new_action = f"search_page_{next_page}"
        query.data = new_action
        await admin_callback(update, context)
        return
    
    elif action == "search_prev_page":
        if 'search_results' not in context.user_data:
            await query.answer("❌ Нет активного поиска", show_alert=True)
            return
        
        current_page = context.user_data.get('search_page', 0)
        prev_page = max(current_page - 1, 0)
        
        # Вызываем ту же функцию с новым action
        new_action = f"search_page_{prev_page}"
        query.data = new_action
        await admin_callback(update, context)
        return

        

    
    # ==================== МЕДИА МЕНЮ ====================
    elif action == "admin_media_menu":
        media_stats = build_scoped_media_stats(requester_id)
        
        text = "📁 **ГАЛЕРЕЯ МЕДИА**\n\n"
        
        total_media = sum([count for _, count in media_stats])
        text += f"📊 Всего медиа: **{total_media}**\n\n"
        
        for media_type, count in media_stats:
            emoji = {
                'photo': '📸',
                'video': '📹',
                'voice': '🎤',
                'video_note': '⭕️',
                'audio': '🎵',
                'document': '📄',
                'saved_photo': '💾📸',
                'saved_video': '💾📹',
                'saved_voice': '💾🎤',
                'saved_video_note': '💾⭕️'
            }.get(media_type, '📎')
            
            text += f"{emoji} {media_type}: **{count}**\n"
        
        keyboard = [
            [
                InlineKeyboardButton("📸 Все фото", callback_data="media_photos"),
                InlineKeyboardButton("📹 Все видео", callback_data="media_videos")
            ],
            [
                InlineKeyboardButton("🎤 Голосовые", callback_data="media_voices"),
                InlineKeyboardButton("⭕️ Кружочки", callback_data="media_videonotes")
            ],
            [
                InlineKeyboardButton("💾 Сохранённые", callback_data="media_saved"),
                InlineKeyboardButton("📎 Все медиа", callback_data="media_all")
            ],
            [InlineKeyboardButton("🧹 Очистка media", callback_data="media_cleanup_menu")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)

    # ==================== ОЧИСТКА MEDIA ПО ВОЗРАСТУ ====================
    elif action == "media_cleanup_menu":
        text = """🧹 **ОЧИСТКА MEDIA ПО ВОЗРАСТУ**

Удаляются только файлы из папки media.
Перед удалением доступен preview (без удаления).

Выберите действие:
"""

        keyboard = [
            [
                InlineKeyboardButton("🗑 Удалить старше 30 дней", callback_data="media_cleanup_prepare_30"),
                InlineKeyboardButton("🗑 Удалить старше 60 дней", callback_data="media_cleanup_prepare_60")
            ],
            [InlineKeyboardButton("🗑 Удалить старше 90 дней", callback_data="media_cleanup_prepare_90")],
            [InlineKeyboardButton("📊 Показать, сколько будет удалено", callback_data="media_cleanup_preview_select")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_media_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await safe_edit_message(query, text, reply_markup)

    elif action == "media_cleanup_preview_select":
        text = """📊 **PREVIEW ОЧИСТКИ MEDIA**

Выберите период для предварительного анализа:
"""

        keyboard = [
            [
                InlineKeyboardButton("30 дней", callback_data="media_cleanup_preview_30"),
                InlineKeyboardButton("60 дней", callback_data="media_cleanup_preview_60"),
                InlineKeyboardButton("90 дней", callback_data="media_cleanup_preview_90")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="media_cleanup_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await safe_edit_message(query, text, reply_markup)

    elif action.startswith("media_cleanup_preview_"):
        days = int(action.split("_")[-1])
        preview = scan_old_media_files(days)

        oldest_info = "нет"
        if preview['oldest_mtime'] and preview['oldest_path']:
            try:
                oldest_dt = datetime.fromtimestamp(preview['oldest_mtime']).strftime('%d.%m.%Y %H:%M')
            except Exception:
                oldest_dt = "unknown"
            try:
                oldest_rel = os.path.relpath(preview['oldest_path'], os.path.abspath(MEDIA_PATH)).replace("\\", "/")
            except Exception:
                oldest_rel = preview['oldest_path']
            oldest_info = f"`{oldest_rel}` ({oldest_dt})"

        samples_text = "\n".join([f"• `{sample}`" for sample in preview['samples']]) if preview['samples'] else "• нет"

        text = f"""📊 **PREVIEW ОЧИСТКИ {days}+ ДНЕЙ**

📁 Папка: `{MEDIA_PATH}`
🧾 Найдено файлов: **{preview['count']}**
💾 Потенциально освободится: **{format_size_mb(preview['total_size'])} MB**
🕰 Самый старый файл: {oldest_info}
⚠️ Пропущено небезопасных путей: {preview['unsafe_skipped']}
❌ Ошибок чтения: {preview['stat_errors']}

Примеры файлов:
{samples_text}
"""

        keyboard = [
            [InlineKeyboardButton(f"🗑 Удалить старше {days} дней", callback_data=f"media_cleanup_prepare_{days}")],
            [InlineKeyboardButton("◀️ Назад", callback_data="media_cleanup_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await safe_edit_message(query, text, reply_markup)

    elif action.startswith("media_cleanup_prepare_"):
        days = int(action.split("_")[-1])
        preview = scan_old_media_files(days)

        oldest_info = "нет"
        if preview['oldest_mtime'] and preview['oldest_path']:
            try:
                oldest_dt = datetime.fromtimestamp(preview['oldest_mtime']).strftime('%d.%m.%Y %H:%M')
            except Exception:
                oldest_dt = "unknown"
            try:
                oldest_rel = os.path.relpath(preview['oldest_path'], os.path.abspath(MEDIA_PATH)).replace("\\", "/")
            except Exception:
                oldest_rel = preview['oldest_path']
            oldest_info = f"`{oldest_rel}` ({oldest_dt})"

        text = f"""⚠️ **ПОДТВЕРЖДЕНИЕ ОЧИСТКИ MEDIA {days}+ ДНЕЙ**

📁 Папка: `{MEDIA_PATH}`
🧾 Найдено файлов к удалению: **{preview['count']}**
💾 Освободится примерно: **{format_size_mb(preview['total_size'])} MB**
🕰 Самый старый файл: {oldest_info}
⚠️ Небезопасные пути будут пропущены автоматически.

Без подтверждения ничего не удаляется.
"""

        keyboard = [
            [InlineKeyboardButton("✅ Подтвердить удаление", callback_data=f"media_cleanup_confirm_{days}")],
            [InlineKeyboardButton("❌ Отмена", callback_data="media_cleanup_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await safe_edit_message(query, text, reply_markup)

    elif action.startswith("media_cleanup_confirm_"):
        days = int(action.split("_")[-1])

        await safe_edit_message(query, f"🔄 **Идет очистка media {days}+ дней...**\n\nПожалуйста, подождите.")

        result = cleanup_old_media_files(days)

        text = f"""✅ **ОТЧЕТ ОЧИСТКИ MEDIA ({days}+ ДНЕЙ)**

🗑 Удалено файлов: **{result['deleted_count']}**
⏭ Пропущено: **{result['skipped_count']}**
❌ Ошибок: **{result['error_count']}**
💾 Освобождено: **{format_size_mb(result['freed_size'])} MB**
🗄 Обновлено ссылок media_path в БД: **{result['db_paths_cleared']}**

📊 Предварительно найдено: {result['scan_count']} файлов
📦 Потенциал по preview: {format_size_mb(result['scan_total_size'])} MB
⚠️ Пропущено небезопасных путей: {result['unsafe_skipped']}
❌ Ошибок чтения при сканировании: {result['stat_errors']}
"""

        keyboard = [
            [InlineKeyboardButton("🔁 Повторить очистку", callback_data="media_cleanup_menu")],
            [InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ВСЕ ФОТО ====================
    elif action == "media_photos":
        results = get_scoped_media_messages(
            requester_id,
            "media_type IN ('photo', 'saved_photo')",
        )
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"📸 **ВСЕ ФОТО** ({len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not results:
            text += "Нет фото"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"media_photos_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"media_photos_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ВСЕ ВИДЕО ====================
    elif action == "media_videos":
        results = get_scoped_media_messages(
            requester_id,
            "media_type IN ('video', 'saved_video')",
        )
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"📹 **ВСЕ ВИДЕО** ({len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not results:
            text += "Нет видео"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"media_videos_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"media_videos_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ГОЛОСОВЫЕ ====================
    elif action == "media_voices":
        results = get_scoped_media_messages(
            requester_id,
            "media_type IN ('voice', 'saved_voice')",
        )
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"🎤 **ГОЛОСОВЫЕ** ({len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not results:
            text += "Нет голосовых"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"media_voices_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"media_voices_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== КРУЖОЧКИ ====================
    elif action == "media_videonotes":
        results = get_scoped_media_messages(
            requester_id,
            "media_type IN ('video_note', 'saved_video_note')",
        )
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"⭕️ **КРУЖОЧКИ** ({len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not results:
            text += "Нет кружочков"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"media_videonotes_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"media_videonotes_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== СОХРАНЁННЫЕ МЕДИА ====================
    elif action == "media_saved":
        results = get_scoped_media_messages(
            requester_id,
            "media_type LIKE 'saved_%'",
        )
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"💾 **СОХРАНЁННЫЕ МЕДИА** ({len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not results:
            text += "Нет сохранённых медиа"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"media_saved_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"media_saved_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ВСЕ МЕДИА ====================
    elif action == "media_all":
        results = get_scoped_media_messages(requester_id, "media_type IS NOT NULL")
        
        total_pages = max((len(results) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"📎 **ВСЕ МЕДИА** ({len(results)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in results[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not results:
            text += "Нет медиа"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"media_all_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"media_all_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("◀️ К медиа", callback_data="admin_media_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ПОЛЬЗОВАТЕЛИ ====================
    elif action == "admin_users":
        users = db.get_all_users()
        if is_scope_limited_admin(requester_id):
            users = [user_row for user_row in users if can_view_owner(requester_id, user_row[0])]
        
        total_pages = max((len(users) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"👥 **ПОЛЬЗОВАТЕЛИ** ({len(users)})\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for user in users[start_idx:end_idx]:
            user_id, username, first_name, registered_at, is_active = user
            stats = db.get_user_stats(user_id)
            
            # ✅ ДАТА+ВРЕМЯ MSK
            reg_time = format_datetime_msk(registered_at)
            
            status = "✅" if is_active else "❌"
            
            user_line = f"{status} **{first_name}** (@{username or 'нет'})\n"
            user_line += f"   🆔 `{user_id}` | 📅 {reg_time}\n"
            
            if stats:
                user_line += f"   💬 {stats[1]} | 🗑 {stats[2]} | ✏️ {stats[3]}\n"
            
            text += user_line + "\n"
            
            keyboard.append([InlineKeyboardButton(
                f"👤 {first_name}",
                callback_data=f"view_user_{user_id}"
            )])
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"admin_users_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"admin_users_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)

               # ==================== ПРОСМОТР ПОЛЬЗОВАТЕЛЯ ====================
    elif action.startswith("view_user_"):
        # ✅ ТОЛЬКО user_id, БЕЗ пагинации чатов
        user_id = int(action.split("_")[2])
        
        user_info = db.get_user_info(user_id)
        
        if not user_info:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        
        user_id_db, username, first_name, registered_at, is_active = user_info
        stats = db.get_user_stats(user_id)
        
        reg_time = format_datetime_msk(registered_at)
        
        text = f"""👤 **ПОЛЬЗОВАТЕЛЬ**

📱 Имя: {first_name}
🆔 ID: `{user_id}`
👤 Username: @{username or 'нет'}
📅 Регистрация: {reg_time}
{'✅ Активен' if is_active else '❌ Неактивен'}

📊 **Статистика:**
"""
        
        if stats:
            text += f"""💬 Сообщений: **{stats[1]}**
🗑 Удалено: **{stats[2]}**
✏️ Изменено: **{stats[3]}**
📎 Медиа: **{stats[4]}**
"""
        else:
            text += "Нет статистики"
        
        # ✅ ПОЛУЧЕНИЕ ВСЕХ ЧАТОВ БЕЗ ПАГИНАЦИИ
        chats = db.get_user_chats(user_id)
        if is_scope_limited_admin(requester_id):
            chats = [chat_row for chat_row in chats if can_view_chat(requester_id, user_id, chat_row[0])]
        
        keyboard = []
        
        if chats:
            text += f"\n\n💭 **ЧАТЫ ({len(chats)}):**\n\n"
            
            for idx, (chat_id, msg_count, last_username) in enumerate(chats, 1):
                # ✅ Определяем отображаемое имя чата
                if chat_id == user_id:
                    # Личные сообщения (Saved Messages)
                    chat_display = "💾 Избранное"
                    icon = "💾"
                elif chat_id < 0:
                    # Группа или канал (отрицательный ID)
                    if last_username:
                        chat_display = f"@{last_username}"
                        icon = "👥"
                    else:
                        chat_display = f"Группа `{chat_id}`"
                        icon = "👥"
                else:
                    # Личный чат (положительный ID)
                    if last_username:
                        chat_display = f"@{last_username}"
                        icon = "👤"
                    else:
                        chat_display = f"User `{chat_id}`"
                        icon = "👤"
                
                text += f"{idx}. {icon} {chat_display} — **{msg_count}** сообщений\n"
                
                # Кнопка для просмотра чата
                keyboard.append([
                    InlineKeyboardButton(
                        f"{icon} {chat_display} ({msg_count})",
                        callback_data=f"view_chat_{user_id}_{chat_id}"
                    )
                ])
        else:
            text += "\n\n💭 **Чаты:** нет"
        
        keyboard.append([InlineKeyboardButton("◀️ К пользователям", callback_data="admin_users")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)

    
                    # ==================== ПРОСМОТР ЧАТА ====================
    elif action.startswith("view_chat_"):
        # ✅ ИСПОЛЬЗУЕМ query.data (оригинальный action ДО изменений)
        original_action = query.data
        parts = original_action.split("_")
        
        # Ищем индекс "page" в списке parts
        if "page" in parts:
            page_idx = parts.index("page")
            user_id = int(parts[2])
            chat_id = int(parts[3])
            page = int(parts[page_idx + 1])
        else:
            user_id = int(parts[2])
            chat_id = int(parts[3])
            page = 0
        
        # ✅ БЕЗ ОГРАНИЧЕНИЙ - все сообщения
        messages = db.get_chat_messages(chat_id, user_id)
        
        # ✅ Получаем информацию о чате
        chat_info = db.get_chat_info(chat_id, user_id)
        
        # ✅ Определяем отображаемое имя чата
        if chat_id == user_id:
            chat_display = "💾 Избранное"
        elif chat_id < 0:
            # Группа или канал
            if chat_info and chat_info['username']:
                chat_display = f"👥 @{chat_info['username']}"
            else:
                chat_display = f"👥 Группа `{chat_id}`"
        else:
            # Личный чат
            if chat_info and chat_info['username']:
                chat_display = f"👤 @{chat_info['username']}"
            else:
                chat_display = f"👤 User `{chat_id}`"
        
        # ✅ ПАГИНАЦИЯ
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"💬 **ЧАТ:** {chat_display}\n"
        text += f"📊 Всего: {len(messages)}\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in messages[start_idx:end_idx]:
            preview = format_message_preview(msg, add_full_button=True)
            text += preview['text'] + "\n"
            
            # Кнопки для медиа
            if msg[6]:  # media_type
                msg_id, chat_id_msg, owner_id = msg[0], msg[1], msg[2]
                keyboard.append([
                    InlineKeyboardButton("📥 Загрузить", callback_data=f"download_{msg_id}_{chat_id_msg}_{owner_id}"),
                    InlineKeyboardButton("ℹ️ Инфо", callback_data=f"metadata_{msg_id}_{chat_id_msg}_{owner_id}")
                ])
            elif preview['has_full_text']:
                keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        
        # ✅ НАВИГАЦИЯ
        nav_buttons = []
        
        # Кнопка ◀️ (предыдущая страница)
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"view_chat_{user_id}_{chat_id}_page_{page-1}"))
        
        # Кнопка 🔄 (обновление текущей страницы)
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"view_chat_{user_id}_{chat_id}_page_{page}"))
        
        # Кнопка с номером страницы
        nav_buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"view_chat_{user_id}_{chat_id}_page_{page}"))
        
        # Кнопка ▶️ (следующая страница)
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"view_chat_{user_id}_{chat_id}_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([
            InlineKeyboardButton("📁 Медиа", callback_data=f"chat_media_menu_{user_id}_{chat_id}"),
            InlineKeyboardButton("💾 Экспорт", callback_data=f"export_chat_menu_{user_id}_{chat_id}")
        ])
        keyboard.append([
            InlineKeyboardButton("🔍 Поиск", callback_data=f"search_in_chat_{user_id}_{chat_id}"),
            InlineKeyboardButton("🗑 Удалить чат", callback_data=f"delete_chat_confirm_{user_id}_{chat_id}")
        ])
        keyboard.append([InlineKeyboardButton("◀️ К пользователю", callback_data=f"view_user_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)





    
    # ==================== МЕНЮ ВЫБОРА ТИПА МЕДИА ДЛЯ ЧАТА ====================
    elif action.startswith("chat_media_menu_"):
        parts = action.split("_")
        user_id = int(parts[3])
        chat_id = int(parts[4])
        
        text = f"📁 **ВЫБОР ТИПА МЕДИА ДЛЯ ЧАТА** `{chat_id}`\n\nВыберите категорию медиа:"
        
        keyboard = [
            [
                InlineKeyboardButton("📸 Фото", callback_data=f"chat_media_type_{user_id}_{chat_id}_photo"),
                InlineKeyboardButton("📹 Видео", callback_data=f"chat_media_type_{user_id}_{chat_id}_video")
            ],
            [
                InlineKeyboardButton("🎤 Голосовые", callback_data=f"chat_media_type_{user_id}_{chat_id}_voice"),
                InlineKeyboardButton("⭕️ Кружочки", callback_data=f"chat_media_type_{user_id}_{chat_id}_video_note")
            ],
            [
                InlineKeyboardButton("💾 Сохранённые", callback_data=f"chat_media_type_{user_id}_{chat_id}_saved"),
                InlineKeyboardButton("📎 Все медиа", callback_data=f"chat_media_type_{user_id}_{chat_id}_all")
            ],
            [InlineKeyboardButton("◀️ К чату", callback_data=f"view_chat_{user_id}_{chat_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== МЕДИА ЧАТА ПО ТИПУ ====================
    elif action.startswith("chat_media_type_"):
        parts = action.split("_")
        user_id = int(parts[3])
        chat_id = int(parts[4])
        media_type = parts[5]
        
        # Определяем SQL-запрос в зависимости от типа медиа
        if media_type == 'all':
            condition = "media_type IS NOT NULL"
            params = (chat_id, user_id)
            type_name = "Все медиа"
        elif media_type == 'saved':
            condition = "media_type LIKE 'saved_%'"
            params = (chat_id, user_id)
            type_name = "Сохранённые медиа"
        else:
            condition = "media_type = ?"
            params = (chat_id, user_id, media_type)
            type_name = {
                'photo': 'Фото',
                'video': 'Видео',
                'voice': 'Голосовые',
                'video_note': 'Кружочки'
            }.get(media_type, media_type)
        
        cursor = db.conn.cursor()
        cursor.execute(f"""
        SELECT * FROM messages
        WHERE chat_id = ? AND owner_id = ? AND {condition}
        ORDER BY date DESC
        """, params)
        media_list = cursor.fetchall()
        
        total_pages = max((len(media_list) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = f"📁 **{type_name} чата** `{chat_id}`\n"
        text += f"Всего: {len(media_list)}\n"
        text += f"📄 Страница {page + 1}/{total_pages}\n\n"
        
        keyboard = []
        
        for msg in media_list[start_idx:end_idx]:
            preview = format_message_preview_with_download(msg)
            text += f"{preview['text']}\n\n"
            
            keyboard.append([
                InlineKeyboardButton("📥 Загрузить", callback_data=preview['download_data']),
                InlineKeyboardButton("ℹ️ Инфо", callback_data=preview['metadata_data'])
            ])
        
        if not media_list:
            text += f"Нет медиа типа '{type_name}'"
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"chat_media_type_{user_id}_{chat_id}_{media_type}_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"chat_media_type_{user_id}_{chat_id}_{media_type}_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ К выбору типа", callback_data=f"chat_media_menu_{user_id}_{chat_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== СЕГОДНЯ ====================
    elif action == "admin_today":
        today = date.today().strftime('%Y-%m-%d')
        stats = db.get_stats_by_date(today)
        messages = db.get_messages_by_date(today, limit=200)  # Увеличено количество сообщений
        messages = filter_messages_by_scope(messages, requester_id)
        if is_scope_limited_admin(requester_id):
            stats = build_stats_from_messages(messages)
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        text = format_date_stats(stats, "СЕГОДНЯ") + "\n\n"
        
        if messages:
            text += f"**📋 СООБЩЕНИЯ:** {len(messages)}\n"
            text += f"📄 Страница {page + 1}/{total_pages}\n\n"
            
            keyboard = []
            
            for msg in messages[start_idx:end_idx]:
                preview = format_message_preview(msg, add_full_button=True)
                text += preview['text'] + "\n"
                
                if preview['has_full_text']:
                    keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        else:
            text += "Нет сообщений"
            keyboard = []
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"admin_today_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"admin_today_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"admin_today_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ВЫБОР ДАТЫ ====================
    elif action == "admin_dates":
        dates = db.get_available_dates(30)
        
        text = "📆 **ВЫБЕРИ ДАТУ**\n\nПоследние 30 дней:\n\n"
        
        keyboard = []
        
        for date_str, count in dates[:15]:
            # Конвертируем дату в читаемый формат
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                display_date = dt.strftime('%d.%m.%Y')
            except:
                display_date = date_str
            
            keyboard.append([
                InlineKeyboardButton(
                    f"📅 {display_date} ({count} сообщений)",
                    callback_data=f"view_date_{date_str}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ПРОСМОТР ДАТЫ ====================
    elif action.startswith("view_date_"):
        date_str = action.split("_")[2]
        
        stats = db.get_stats_by_date(date_str)
        messages = db.get_messages_by_date(date_str, limit=200)  # Увеличено количество сообщений
        messages = filter_messages_by_scope(messages, requester_id)
        if is_scope_limited_admin(requester_id):
            stats = build_stats_from_messages(messages)
        
        total_pages = max((len(messages) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE, 1)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        
        # Форматируем дату для отображения
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            display_date = dt.strftime('%d.%m.%Y')
        except:
            display_date = date_str
        
        text = format_date_stats(stats, display_date) + "\n\n"
        
        if messages:
            text += f"**📋 СООБЩЕНИЯ:** {len(messages)}\n"
            text += f"📄 Страница {page + 1}/{total_pages}\n\n"
            
            keyboard = []
            
            for msg in messages[start_idx:end_idx]:
                preview = format_message_preview(msg, add_full_button=True)
                text += preview['text'] + "\n"
                
                if preview['has_full_text']:
                    keyboard.append([InlineKeyboardButton("📄 Полный текст", callback_data=preview['full_text_data'])])
        else:
            text += "Нет сообщений"
            keyboard = []
        
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"view_date_{date_str}_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton("🔄", callback_data=f"view_date_{date_str}_page_{page}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"view_date_{date_str}_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("◀️ К датам", callback_data="admin_dates")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
        
                # ==================== ОЧИСТКА АРХИВОВ ====================
    elif action == "cleanup_archives_confirm":
        archives_info = get_archives_info()
        
        text = f"""⚠️ **ПОДТВЕРДИ УДАЛЕНИЕ АРХИВОВ**

Будет удалено:
📦 Архивов: {archives_info['count']}
💾 Освободится: {archives_info['size_mb']} MB

**Это действие необратимо!**
Убедись, что архивы уже отправлены в Telegram.
"""
        
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data="cleanup_archives_confirmed")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_archive_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    elif action == "cleanup_archives_confirmed":
        try:
            deleted_count = cleanup_archives()
            
            await query.answer(f"✅ Удалено архивов: {deleted_count}", show_alert=True)
            
            text = f"""✅ **ОЧИСТКА ЗАВЕРШЕНА**

🗑 Удалено архивов: {deleted_count}
💾 Место освобождено
📁 Папка archives очищена
"""
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_archive_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await safe_edit_message(query, text, reply_markup)
        
        except Exception as e:
            print(f"[ERROR] Ошибка очистки archives: {e}")
            await query.answer("❌ Ошибка очистки архивов", show_alert=True)
    # ==================== АРХИВАЦИЯ МЕДИА ====================
    elif action == "admin_archive_menu":
        scan = collect_archive_media_paths(requester_id)
        total_size_bytes = 0
        for media_path in scan["paths"]:
            try:
                total_size_bytes += os.path.getsize(media_path)
            except Exception:
                pass
        total_size_mb = total_size_bytes / (1024 * 1024)
        archives_info = get_archives_info()

        text = f"""📦 **АРХИВАЦИЯ МЕДИА**

📊 Доступно медиа: {len(scan['paths'])}
💾 Размер доступного медиа: ~{total_size_mb:.2f} MB
⛔️ Пропущено по scope: {scan['skipped_scope']}
⚠️ Пропущено небезопасных путей: {scan['skipped_unsafe']}
📭 Отсутствует на диске: {scan['missing_files']}

📁 **Папка archives:**
📦 Архивов: {archives_info['count']}
💾 Размер: {archives_info['size_mb']} MB

Выбери действие:
"""

        keyboard = [
            [InlineKeyboardButton("📦 Архивировать всё", callback_data="archive_all")],
            [
                InlineKeyboardButton("📅 По дате", callback_data="archive_by_date"),
                InlineKeyboardButton("💬 По чату", callback_data="archive_by_chat")
            ],
            [InlineKeyboardButton("🗑 Очистить медиа", callback_data="cleanup_media_confirm")]
        ]
        if archives_info['count'] > 0:
            keyboard.append([InlineKeyboardButton("🗑 Очистить папку archives", callback_data="cleanup_archives_confirm")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        await safe_edit_message(query, text, InlineKeyboardMarkup(keyboard))

    elif action == "archive_all":
        await query.answer("🔄 Создаю архив...", show_alert=False)
        scan = collect_archive_media_paths(requester_id)
        if not scan["paths"]:
            await query.answer("❌ Нет доступных медиа для архивации", show_alert=True)
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_dir = os.path.join(ARCHIVE_PATH, f"temp_all_{requester_id}_{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        try:
            copied = copy_media_files_for_archive(scan["paths"], temp_dir)
            if copied <= 0:
                await query.answer("❌ Не удалось подготовить файлы для архива", show_alert=True)
                return

            archive_name = f"media_all_{requester_id}_{timestamp}.rar"
            archive_path = create_rar_archive(temp_dir, archive_name)
            if not archive_path or not os.path.exists(archive_path) or not is_safe_archive_path(archive_path):
                if archive_path and not is_safe_archive_path(archive_path):
                    print(f"[WARNING] Unsafe archive path blocked: {archive_path}")
                await query.answer("❌ Ошибка создания архива", show_alert=True)
                return

            file_size = os.path.getsize(archive_path) / (1024 * 1024)
            with open(archive_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    caption=(
                        f"📦 **АРХИВ МЕДИА (scope-safe)**\n\n"
                        f"📊 Файлов: {copied}\n"
                        f"⛔️ Scope skip: {scan['skipped_scope']}\n"
                        f"⚠️ Unsafe skip: {scan['skipped_unsafe']}\n"
                        f"📭 Missing: {scan['missing_files']}\n"
                        f"💾 Размер: {file_size:.2f} MB"
                    )
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить архив", show_alert=True)
                return
            await query.answer("✅ Архив создан и отправлен!", show_alert=True)
            await safe_edit_message(
                query,
                (
                    "✅ **АРХИВ СОЗДАН**\n\n"
                    f"📦 Файл: `{archive_name}`\n"
                    f"📊 В архиве: {copied} файлов\n"
                    f"💾 Размер: {file_size:.2f} MB\n"
                    f"⛔️ Пропущено по scope: {scan['skipped_scope']}\n"
                    f"⚠️ Небезопасных путей: {scan['skipped_unsafe']}\n"
                    f"📭 Отсутствует на диске: {scan['missing_files']}\n\n"
                    "Хочешь удалить исходные файлы?"
                ),
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑 Удалить медиа", callback_data="cleanup_media_after_archive")],
                    [InlineKeyboardButton("◀️ Назад", callback_data="admin_archive_menu")]
                ])
            )
        except Exception as e:
            print(f"[ERROR] Ошибка архивации: {e}")
            await query.answer("❌ Ошибка архивации", show_alert=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    elif action == "archive_by_date":
        dates = get_archive_dates_for_requester(requester_id, limit=10)
        text = "📅 **ВЫБЕРИ ДАТУ ДЛЯ АРХИВАЦИИ**\n\n"
        keyboard = []
        for date_str, count in dates:
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                display_date = dt.strftime('%d.%m.%Y')
            except Exception:
                display_date = date_str
            keyboard.append([
                InlineKeyboardButton(
                    f"📅 {display_date} ({count} медиа)",
                    callback_data=f"archive_date_{date_str}"
                )
            ])
        if not dates:
            text += "Нет доступных медиа для архивации."
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_archive_menu")])
        await safe_edit_message(query, text, InlineKeyboardMarkup(keyboard))

    elif action.startswith("archive_date_"):
        date_str = action.split("_", 2)[2]
        await query.answer("🔄 Создаю архив за дату...", show_alert=False)
        scan = collect_archive_media_paths(requester_id, "DATE(date) = ?", (date_str,))
        if not scan["paths"]:
            await query.answer("❌ Нет доступных медиа за эту дату", show_alert=True)
            return

        timestamp = datetime.now().strftime('%H%M%S')
        temp_dir = os.path.join(ARCHIVE_PATH, f"temp_date_{requester_id}_{date_str}_{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        try:
            copied = copy_media_files_for_archive(scan["paths"], temp_dir)
            if copied <= 0:
                await query.answer("❌ Не удалось подготовить файлы для архива", show_alert=True)
                return

            archive_name = f"media_{date_str}_{requester_id}_{timestamp}.rar"
            archive_path = create_rar_archive(temp_dir, archive_name)
            if not archive_path or not os.path.exists(archive_path) or not is_safe_archive_path(archive_path):
                if archive_path and not is_safe_archive_path(archive_path):
                    print(f"[WARNING] Unsafe archive path blocked: {archive_path}")
                await query.answer("❌ Ошибка создания архива", show_alert=True)
                return

            file_size = os.path.getsize(archive_path) / (1024 * 1024)
            with open(archive_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    caption=(
                        f"📦 **АРХИВ ЗА {date_str}**\n\n"
                        f"📊 Файлов: {copied}\n"
                        f"⛔️ Scope skip: {scan['skipped_scope']}\n"
                        f"⚠️ Unsafe skip: {scan['skipped_unsafe']}\n"
                        f"📭 Missing: {scan['missing_files']}\n"
                        f"💾 Размер: {file_size:.2f} MB"
                    )
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить архив", show_alert=True)
                return
            await query.answer("✅ Архив отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Ошибка архивации по дате: {e}")
            await query.answer("❌ Ошибка архивации", show_alert=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    elif action == "archive_by_chat":
        chats = get_archive_chats_for_requester(requester_id, limit=20)
        text = "💬 **ВЫБЕРИ ЧАТ ДЛЯ АРХИВАЦИИ**\n\n"
        keyboard = []
        for owner_id_db, chat_id_db, count in chats:
            chat_info = db.get_chat_info(chat_id_db, owner_id_db)
            chat_display = get_chat_display_name(chat_id_db, owner_id_db, chat_info)
            keyboard.append([
                InlineKeyboardButton(
                    f"{chat_display} ({count} медиа)",
                    callback_data=f"archive_chat_{owner_id_db}_{chat_id_db}"
                )
            ])
        if not chats:
            text += "Нет доступных чатов с медиа."
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_archive_menu")])
        await safe_edit_message(query, text, InlineKeyboardMarkup(keyboard))

    elif action.startswith("archive_chat_"):
        parts = action.split("_")
        owner_id_target = None
        chat_id_target = None
        try:
            if len(parts) >= 4:
                owner_id_target = int(parts[2])
                chat_id_target = int(parts[3])
            elif len(parts) >= 3:
                chat_id_target = int(parts[2])
            else:
                raise ValueError("invalid archive_chat action")
        except Exception:
            await query.answer("❌ Некорректные параметры чата", show_alert=True)
            return

        if owner_id_target is None and is_scope_limited_admin(requester_id):
            await query.answer("❌ Для scoped-ролей требуется owner_id в callback", show_alert=True)
            return

        if owner_id_target is not None and not can_view_chat(requester_id, owner_id_target, chat_id_target):
            await query.answer("❌ Нет доступа к этому чату", show_alert=True)
            return

        await query.answer("🔄 Создаю архив чата...", show_alert=False)
        if owner_id_target is not None:
            scan = collect_archive_media_paths(
                requester_id,
                "owner_id = ? AND chat_id = ?",
                (owner_id_target, chat_id_target),
            )
        else:
            scan = collect_archive_media_paths(
                requester_id,
                "chat_id = ?",
                (chat_id_target,),
            )
        if not scan["paths"]:
            await query.answer("❌ Нет доступных медиа в этом чате", show_alert=True)
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_dir = os.path.join(ARCHIVE_PATH, f"temp_chat_{requester_id}_{chat_id_target}_{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        try:
            copied = copy_media_files_for_archive(scan["paths"], temp_dir)
            if copied <= 0:
                await query.answer("❌ Не удалось подготовить файлы для архива", show_alert=True)
                return

            archive_name = (
                f"media_chat_{owner_id_target}_{chat_id_target}_{timestamp}.rar"
                if owner_id_target is not None
                else f"media_chat_{chat_id_target}_{timestamp}.rar"
            )
            archive_path = create_rar_archive(temp_dir, archive_name)
            if not archive_path or not os.path.exists(archive_path) or not is_safe_archive_path(archive_path):
                if archive_path and not is_safe_archive_path(archive_path):
                    print(f"[WARNING] Unsafe archive path blocked: {archive_path}")
                await query.answer("❌ Ошибка создания архива", show_alert=True)
                return

            file_size = os.path.getsize(archive_path) / (1024 * 1024)
            with open(archive_path, 'rb') as f:
                sent_count = await send_admin_document(
                    context.bot,
                    f,
                    admin_id=admin_chat_id,
                    caption=(
                        f"📦 **АРХИВ ЧАТА {chat_id_target}**\n\n"
                        f"📊 Файлов: {copied}\n"
                        f"⛔️ Scope skip: {scan['skipped_scope']}\n"
                        f"⚠️ Unsafe skip: {scan['skipped_unsafe']}\n"
                        f"📭 Missing: {scan['missing_files']}\n"
                        f"💾 Размер: {file_size:.2f} MB"
                    )
                )
            if sent_count <= 0:
                await query.answer("❌ Не удалось отправить архив", show_alert=True)
                return
            await query.answer("✅ Архив отправлен!", show_alert=True)
        except Exception as e:
            print(f"[ERROR] Ошибка архивации чата: {e}")
            await query.answer("❌ Ошибка архивации", show_alert=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    elif action == "cleanup_media_confirm":
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM messages WHERE media_type IS NOT NULL AND media_path IS NOT NULL")
        count = cursor.fetchone()[0]

        text = f"""⚠️ **ПОДТВЕРДИ УДАЛЕНИЕ**

Будет удалено файлов: {count}

**Это действие необратимо!**
Архив НЕ будет создан.
"""
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data="cleanup_media_confirmed")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_archive_menu")]
        ]
        await safe_edit_message(query, text, InlineKeyboardMarkup(keyboard))

    elif action == "cleanup_media_confirmed" or action == "cleanup_media_after_archive":
        try:
            deleted_count = 0
            for filename in os.listdir(MEDIA_PATH):
                file_path = os.path.join(MEDIA_PATH, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                        deleted_count += 1
                except Exception as e:
                    print(f"[ERROR] Ошибка удаления {file_path}: {e}")

            cursor = db.conn.cursor()
            cursor.execute("UPDATE messages SET media_path = NULL WHERE media_type IS NOT NULL")
            db.conn.commit()

            await query.answer(f"✅ Удалено файлов: {deleted_count}", show_alert=True)
            text = f"""✅ **ОЧИСТКА ЗАВЕРШЕНА**

🗑 Удалено файлов: {deleted_count}
💾 Место освобождено
📝 Записи в БД сохранены
"""
            await safe_edit_message(
                query,
                text,
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_archive_menu")]]),
            )
        except Exception as e:
            print(f"[ERROR] Ошибка очистки: {e}")
            await query.answer("❌ Ошибка очистки медиа", show_alert=True)

    # ==================== НАСТРОЙКИ ====================
    elif action == "admin_settings":
        if is_scope_limited_admin(requester_id):
            text = f"""⚙️ **НАСТРОЙКИ**

🔒 Для scoped-ролей общесистемная статистика БД/медиа скрыта.

📁 **Медиа:**
📂 Путь: `{MEDIA_PATH}`
📥 Скачивание: {'✅ Включено' if DOWNLOAD_MEDIA else '❌ Отключено'}
"""
        else:
            db_info = db.get_database_size()
            text = f"""⚙️ **НАСТРОЙКИ**

🗄 **База данных:**
💾 Размер: {db_info['size_mb']} MB
📝 Сообщений: {db_info['messages']}
👥 Пользователей: {db_info['users']}
✏️ Изменений: {db_info['edits']}

📁 **Медиа:**
📂 Путь: `{MEDIA_PATH}`
📥 Скачивание: {'✅ Включено' if DOWNLOAD_MEDIA else '❌ Отключено'}

🔐 **Администраторы:** {len(ADMIN_IDS)}
"""
        
        keyboard = [[InlineKeyboardButton("🗑 Очистить старые (90+ дней)", callback_data="cleanup_old")]]
        if is_superadmin(requester_id):
            keyboard.append([InlineKeyboardButton("🧨 Полное удаление пользователя", callback_data="admin_user_hard_delete_start")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)
    
    # ==================== ОЧИСТКА СТАРЫХ СООБЩЕНИЙ ====================
    elif action == "cleanup_old":
        deleted_count = db.cleanup_old_messages(days=90)
        
        await query.answer(f"🗑 Удалено {deleted_count} сообщений старше 90 дней", show_alert=True)
        
        # Возврат в настройки
        if is_scope_limited_admin(requester_id):
            text = f"""⚙️ **НАСТРОЙКИ**

🔒 Для scoped-ролей общесистемная статистика БД/медиа скрыта.

📁 **Медиа:**
📂 Путь: `{MEDIA_PATH}`
📥 Скачивание: {'✅ Включено' if DOWNLOAD_MEDIA else '❌ Отключено'}

✅ Очистка выполнена: удалено {deleted_count} записей
"""
        else:
            db_info = db.get_database_size()
            text = f"""⚙️ **НАСТРОЙКИ**

🗄 **База данных:**
💾 Размер: {db_info['size_mb']} MB
📝 Сообщений: {db_info['messages']}
👥 Пользователей: {db_info['users']}
✏️ Изменений: {db_info['edits']}

📁 **Медиа:**
📂 Путь: `{MEDIA_PATH}`
📥 Скачивание: {'✅ Включено' if DOWNLOAD_MEDIA else '❌ Отключено'}

🔐 **Администраторы:** {len(ADMIN_IDS)}

✅ Очистка выполнена: удалено {deleted_count} записей
"""
        
        keyboard = [[InlineKeyboardButton("🗑 Очистить старые (90+ дней)", callback_data="cleanup_old")]]
        if is_superadmin(requester_id):
            keyboard.append([InlineKeyboardButton("🧨 Полное удаление пользователя", callback_data="admin_user_hard_delete_start")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(query, text, reply_markup)

    else:
        await query.answer("⚠️ Функция в разработке", show_alert=True)

# ==================== BUSINESS CONNECTION HANDLERS ====================

async def handle_business_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка подключения Business аккаунта"""
    # ✅ ИСПРАВЛЕНИЕ: Проверка наличия business_connection
    if not hasattr(update, 'business_connection') or not update.business_connection:
        return
    
    bc = update.business_connection
    can_reply = getattr(getattr(bc, "rights", None), "can_reply", getattr(bc, "can_reply", False))
    
    if bc.is_enabled:
        db.save_business_connection(bc.id, bc.user.id)
        print(f"[BUSINESS] Подключение активировано: {bc.id} -> user {bc.user.id}")
        
        # Получаем информацию о пользователе
        user = bc.user
        username = f"@{user.username}" if user.username else "без username"
        user_link = f"[{user.id}](tg://user?id={user.id})"
        
        # ✅ Уведомление АДМИНАМ о подключении
        admin_notification = f"""🟢 **НОВОЕ ПОДКЛЮЧЕНИЕ**

👤 Пользователь: {username}
🆔 User ID: {user_link}
🔗 Connection ID: `{bc.id}`
💬 Может отвечать: {'✅ Да' if can_reply else '❌ Нет'}
⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}
"""
        
        await send_admin_notification(context.bot, admin_notification)
        
        # Уведомление пользователю
        try:
            if has_active_subscription(bc.user.id):
                await context.bot.send_message(
                    bc.user.id,
                    "✅ BUSINESS подключён.\nБот активен и будет отслеживать изменения и удаления сообщений."
                )
            else:
                await context.bot.send_message(
                    bc.user.id,
                    "✅ BUSINESS подключён.\n\nДля работы функций нужна активная подписка.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💫 Купить подписку", callback_data="public_plans")],
                        [InlineKeyboardButton("Как работает бот", callback_data="public_how_it_works")]
                    ])
                )
        except:
            pass
    else:
        db.deactivate_connection(bc.id)
        print(f"[BUSINESS] Подключение деактивировано: {bc.id}")
        
        # Получаем информацию о пользователе
        user = bc.user
        username = f"@{user.username}" if user.username else "без username"
        user_link = f"[{user.id}](tg://user?id={user.id})"
        
        # ✅ Уведомление АДМИНАМ об отключении
        admin_notification = f"""🔴 **ОТКЛЮЧЕНИЕ БОТА**

👤 Пользователь: {username}
🆔 User ID: {user_link}
🔗 Connection ID: `{bc.id}`
⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}
"""
        
        await send_admin_notification(context.bot, admin_notification)


async def handle_business_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений из Business чатов"""
    msg = update.business_message or update.edited_business_message
    
    if not msg:
        return
    
    business_connection_id = msg.business_connection_id
    owner_id = db.get_owner_by_connection(business_connection_id)
    
    if not owner_id:
        if should_log_connection_warning(f"business_message:{business_connection_id}"):
            print(f"[WARNING] Owner не найден для connection {business_connection_id}")
        return

    if not await require_paid_access(update, context, owner_id, notify=True):
        return
    
    chat_id = msg.chat_id
    message_id = msg.message_id
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.first_name
    
    text = msg.text or msg.caption or ""
    media_type = None
    media_path = None
    
    # ==================== ✅ REPLY-TO-SAVE ====================
    if msg.reply_to_message and text:
        replied_msg = msg.reply_to_message
        
        # Проверка триггера
        is_save_trigger = text.strip().lower() in REPLY_SAVE_TRIGGER
        
        if is_save_trigger:
            print(f"[REPLY-SAVE] Триггер обнаружен от user {owner_id}")
            
            saved_media_path = None
            saved_media_type = None
            
            try:
                # ✅ ВАЖНО: video_note ДОЛЖЕН проверяться ПЕРВЫМ (до video)
                if replied_msg.video_note:
                    saved_media_type = "saved_video_note"
                    file = await replied_msg.video_note.get_file()
                    saved_media_path = os.path.join(MEDIA_PATH, f"saved_video_note_{owner_id}_{chat_id}_{replied_msg.message_id}.mp4")
                    await file.download_to_drive(saved_media_path)
                    print(f"[REPLY-SAVE] ✅ Сохранен кружок")
                
                elif replied_msg.photo:
                    saved_media_type = "saved_photo"
                    file = await replied_msg.photo[-1].get_file()
                    saved_media_path = os.path.join(MEDIA_PATH, f"saved_photo_{owner_id}_{chat_id}_{replied_msg.message_id}.jpg")
                    await file.download_to_drive(saved_media_path)
                    print(f"[REPLY-SAVE] ✅ Сохранено фото")
                
                elif replied_msg.video:
                    saved_media_type = "saved_video"
                    file = await replied_msg.video.get_file()
                    saved_media_path = os.path.join(MEDIA_PATH, f"saved_video_{owner_id}_{chat_id}_{replied_msg.message_id}.mp4")
                    await file.download_to_drive(saved_media_path)
                    print(f"[REPLY-SAVE] ✅ Сохранено видео")
                
                elif replied_msg.voice:
                    saved_media_type = "saved_voice"
                    file = await replied_msg.voice.get_file()
                    saved_media_path = os.path.join(MEDIA_PATH, f"saved_voice_{owner_id}_{chat_id}_{replied_msg.message_id}.ogg")
                    await file.download_to_drive(saved_media_path)
                    print(f"[REPLY-SAVE] ✅ Сохранен голос")
                
                elif replied_msg.audio:
                    saved_media_type = "saved_audio"
                    file = await replied_msg.audio.get_file()
                    saved_media_path = os.path.join(MEDIA_PATH, f"saved_audio_{owner_id}_{chat_id}_{replied_msg.message_id}.mp3")
                    await file.download_to_drive(saved_media_path)
                    print(f"[REPLY-SAVE] ✅ Сохранен аудио")
                
                # Если медиа сохранено - отправляем пользователю
                if saved_media_path and os.path.exists(saved_media_path):
                    sender_info = replied_msg.from_user.username if replied_msg.from_user and replied_msg.from_user.username else \
                                 replied_msg.from_user.first_name if replied_msg.from_user else "Unknown"
                    
                    caption = f"""💾 **СОХРАНЕНО МЕДИА**

👤 От: {sender_info}
📱 Чат ID: `{chat_id}`
🕐 {format_datetime_msk(replied_msg.date)}
📎 Тип: {saved_media_type}
"""
                    
                    # ✅ ИСПРАВЛЕНО: Правильная отправка разных типов медиа
                    with open(saved_media_path, 'rb') as f:
                        if saved_media_type == "saved_video_note":
                            # ✅ Кружки отправляются через send_video_note БЕЗ caption
                            await context.bot.send_video_note(owner_id, video_note=f)
                            # Caption отдельно
                            await context.bot.send_message(owner_id, text=caption)
                        elif saved_media_type == "saved_photo":
                            await context.bot.send_photo(owner_id, photo=f, caption=caption)
                        elif saved_media_type == "saved_video":
                            await context.bot.send_video(owner_id, video=f, caption=caption)
                        elif saved_media_type == "saved_voice":
                            await context.bot.send_voice(owner_id, voice=f, caption=caption)
                        elif saved_media_type == "saved_audio":
                            await context.bot.send_audio(owner_id, audio=f, caption=caption)
                        else:
                            await context.bot.send_document(owner_id, document=f, caption=caption)
                    
                    # Сохранение в БД
                    db.save_message(
                        message_id=replied_msg.message_id,
                        chat_id=chat_id,
                        owner_id=owner_id,
                        user_id=replied_msg.from_user.id if replied_msg.from_user else 0,
                        username=sender_info,
                        text=f"[Saved via reply] {replied_msg.caption or ''}",
                        media_type=saved_media_type,
                        media_path=saved_media_path,
                        date=replied_msg.date,
                        reply_to_message_id=None
                    )
                    
                    print(f"[✓] REPLY-SAVE успешно для owner {owner_id}")
                    return  # ✅ Выходим, не сохраняем триггерное сообщение
            
            except Exception as e:
                print(f"[ERROR] Ошибка REPLY-SAVE: {e}")
    
        # ==================== ОБЫЧНАЯ ОБРАБОТКА ====================
    
    # Определение типа медиа (только если НЕ reply-to-save)
    if DOWNLOAD_MEDIA and not (msg.reply_to_message and text.strip().lower() in REPLY_SAVE_TRIGGER):
        try:
            # ✅ ВАЖНО: video_note ДОЛЖЕН проверяться ПЕРВЫМ (до video)
            if msg.video_note:
                media_type = "video_note"
                file = await msg.video_note.get_file()
                media_path = os.path.join(
                    MEDIA_PATH,
                    build_media_filename("video_note", owner_id, user_id, chat_id, message_id, "mp4"),
                )
                await file.download_to_drive(media_path)
                print(f"[MEDIA] ✅ Saved video_note: {media_path}")
            
            elif msg.photo:
                media_type = "photo"
                file = await msg.photo[-1].get_file()
                file_ext = safe_file_extension(file.file_path, "jpg")
                media_path = os.path.join(
                    MEDIA_PATH,
                    build_media_filename("photo", owner_id, user_id, chat_id, message_id, file_ext),
                )
                await file.download_to_drive(media_path)
            
            elif msg.video:
                media_type = "video"
                file = await msg.video.get_file()
                file_ext = safe_file_extension(file.file_path, "mp4")
                media_path = os.path.join(
                    MEDIA_PATH,
                    build_media_filename("video", owner_id, user_id, chat_id, message_id, file_ext),
                )
                await file.download_to_drive(media_path)
            
            elif msg.voice:
                media_type = "voice"
                file = await msg.voice.get_file()
                file_ext = safe_file_extension(file.file_path, "ogg")
                media_path = os.path.join(
                    MEDIA_PATH,
                    build_media_filename("voice", owner_id, user_id, chat_id, message_id, file_ext),
                )
                await file.download_to_drive(media_path)
            
            elif msg.audio:
                media_type = "audio"
                file = await msg.audio.get_file()
                file_ext = safe_file_extension(file.file_path, "mp3")
                media_path = os.path.join(
                    MEDIA_PATH,
                    build_media_filename("audio", owner_id, user_id, chat_id, message_id, file_ext),
                )
                await file.download_to_drive(media_path)
            
            elif msg.document:
                media_type = "document"
                file = await msg.document.get_file()
                file_ext = safe_file_extension(file.file_path, "bin")
                media_path = os.path.join(
                    MEDIA_PATH,
                    build_media_filename("document", owner_id, user_id, chat_id, message_id, file_ext),
                )
                await file.download_to_drive(media_path)
        
        except Exception as e:
            print(f"[ERROR] Ошибка скачивания медиа: {e}")
    
    # Reply-to handling
    reply_to_message_id = msg.reply_to_message.message_id if msg.reply_to_message else None
    
        # Проверка на изменение
    existing = db.get_message(message_id, chat_id, owner_id)
    
    if existing and update.edited_business_message:
        old_text = existing[5]
        if old_text != text:
            db.save_edit(message_id, chat_id, owner_id, old_text, text)
            print(f"[EDIT] Сообщение изменено: {message_id} в чате {chat_id}")
            
            # ✅ УВЕДОМЛЕНИЕ ОБ ИЗМЕНЕНИИ
            try:
                full_datetime = format_datetime_msk(msg.date)
                
                notification = f"""✏️ **СООБЩЕНИЕ ИЗМЕНЕНО**

👤 От: {username}
💬 Chat: `{chat_id}`
📅 {full_datetime}

📝 **Было:**
{truncate_text(old_text, 200) if old_text else '(пусто)'}

📝 **Стало:**
{truncate_text(text, 200) if text else '(пусто)'}
"""
                
                await context.bot.send_message(owner_id, notification)
                print(f"[✓] Уведомление об изменении отправлено owner {owner_id}")
            except Exception as e:
                print(f"[ERROR] Не удалось отправить уведомление об изменении: {e}")
    else:
        # Новое сообщение
        db.save_message(
            message_id, chat_id, owner_id, user_id, username, text,
            media_type, media_path, msg.date, reply_to_message_id
        )
        # print(f"[MSG] Сохранено: {message_id} от {username} в чате {chat_id}")  # Отключено


async def handle_deleted_business_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка удалённых сообщений из Business"""
    if not hasattr(update, 'deleted_business_messages') or not update.deleted_business_messages:
        return
    
    deleted = update.deleted_business_messages
    business_connection_id = deleted.business_connection_id
    chat_id = deleted.chat.id
    
    owner_id = db.get_owner_by_connection(business_connection_id)
    
    if not owner_id:
        if should_log_connection_warning(f"deleted_business:{business_connection_id}"):
            print(f"[WARNING] Owner не найден для connection {business_connection_id}")
        return

    if not await require_paid_access(update, context, owner_id, notify=True):
        return
    
    for message_id in deleted.message_ids:
        db.mark_deleted(message_id, chat_id, owner_id)
        print(f"[DELETE] Помечено как удалённое: {message_id} в чате {chat_id}")
        
        msg_data = db.get_message(message_id, chat_id, owner_id)
        
        if msg_data:
            _, _, _, user_id_msg, username, text_msg, media_type, media_path, msg_date, _, _, _, _ = msg_data
            
            full_datetime = format_datetime_msk(msg_date)
            
            notification = f"""🗑 **СООБЩЕНИЕ УДАЛЕНО**

👤 От: {username}
💬 Chat: `{chat_id}`
📅 {full_datetime}

📝 Текст: {truncate_text(text_msg, 200) if text_msg else '(нет)'}

📎 Медиа: {media_type or 'нет'}
"""
            
            keyboard = [[InlineKeyboardButton("🗑 Скрыть", callback_data="hide_msg")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            try:
                # Отправляем текстовое уведомление с кнопкой
                await context.bot.send_message(owner_id, notification, reply_markup=reply_markup)
                print(f"[✓] Уведомление отправлено owner {owner_id}")
                
                # Если есть медиа - отправляем файл с кнопкой
                if media_path and is_safe_media_path(media_path) and os.path.exists(media_path):
                    media_type_normalized = media_type.strip().lower() if media_type else ""
                    
                    with open(media_path, 'rb') as f:
                        if media_type_normalized in ["video_note", "videonote", "saved_video_note", "saved_videonote"]:
                            await context.bot.send_video_note(owner_id, video_note=f, reply_markup=reply_markup)
                        elif "photo" in media_type_normalized:
                            await context.bot.send_photo(owner_id, photo=f, reply_markup=reply_markup)
                        elif "voice" in media_type_normalized:
                            await context.bot.send_voice(owner_id, voice=f, reply_markup=reply_markup)
                        elif "audio" in media_type_normalized:
                            await context.bot.send_audio(owner_id, audio=f, reply_markup=reply_markup)
                        elif "video" in media_type_normalized:
                            await context.bot.send_video(owner_id, video=f, reply_markup=reply_markup)
                        else:
                            await context.bot.send_document(owner_id, document=f, reply_markup=reply_markup)
                    
                    print(f"[✓] Медиа отправлено owner {owner_id}")
                elif media_path and not is_safe_media_path(media_path):
                    print(f"[WARNING] Пропущен небезопасный media_path: {media_path}")
            
            except Exception as e:
                print(f"[ERROR] Не удалось отправить уведомление/медиа: {e}")


# ==================== MAIN ====================

def main():
    configure_stdio_utf8()
    configure_telegram_network_logging()
    print("[START] Запуск бота...")
    
    try:
        runtime_token = get_runtime_token()
        config_errors = validate_runtime_config(runtime_token)
        if config_errors:
            print("❌ ОШИБКА КОНФИГА:")
            for err in config_errors:
                print(f"   - {err}")
            return 2

        bot_request, updates_request = build_telegram_requests()
        app = (
            Application.builder()
            .token(runtime_token)
            .request(bot_request)
            .get_updates_request(updates_request)
            .build()
        )
        
        print("[INFO] Token source: env BOT_TOKEN")
        
        # ==================== КОМАНДЫ ====================
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("cabinet", cabinet_command))
        app.add_handler(CommandHandler("plans", plans_command))
        app.add_handler(CommandHandler("subscription", subscription_command))
        app.add_handler(CommandHandler("paysupport", paysupport_command))
        app.add_handler(CommandHandler("stats", stats_command))
        app.add_handler(CommandHandler("admin", admin_command))
        
        print("[✓] Команды зарегистрированы")
        
        # ==================== PAYMENT FLOW ====================
        app.add_handler(PreCheckoutQueryHandler(pre_checkout_handler))
        app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))
        print("[✓] Payment handlers зарегистрированы")

        # ==================== CALLBACKS ====================
        app.add_handler(CallbackQueryHandler(public_callback, pattern=r"^public_"))
        app.add_handler(CallbackQueryHandler(admin_callback))
        print("[✓] CallbackQueryHandler зарегистрирован")
        
        # ==================== ОБРАБОТЧИК ПОИСКА ====================
        app.add_handler(MessageHandler(
            filters.UpdateType.MESSAGE & filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            handle_search_message
        ))
        print("[✓] Обработчик поиска зарегистрирован")
        
        # ==================== BUSINESS HANDLERS ====================
        app.add_handler(TypeHandler(type=Update, callback=handle_business_connection), group=1)
        app.add_handler(
            MessageHandler(
                filters.UpdateType.BUSINESS_MESSAGE | filters.UpdateType.EDITED_BUSINESS_MESSAGE,
                handle_business_message
            ),
            group=2
        )
        app.add_handler(TypeHandler(type=Update, callback=handle_deleted_business_message), group=3)
        
        print("[✓] Business handlers зарегистрированы")
        
        # ==================== ХУКИ ЗАПУСКА/ОСТАНОВКИ ====================
        app.post_init = on_startup
        app.post_shutdown = on_shutdown
        app.add_error_handler(global_error_handler)
        print("[✓] Хуки уведомлений установлены")
        
        print("[✓] Бот успешно запущен!")
        print(f"[✓] Админы: {ADMIN_IDS}")
        print(f"[✓] База: {DB_PATH}")
        print(f"[✓] Медиа: {MEDIA_PATH}")
        print(f"[✓] Триггеры: {REPLY_SAVE_TRIGGER}")
        print(
            "[✓] Network: "
            f"bot(connect/read/write/pool={BOT_REQUEST_CONNECT_TIMEOUT}/{BOT_REQUEST_READ_TIMEOUT}/"
            f"{BOT_REQUEST_WRITE_TIMEOUT}/{BOT_REQUEST_POOL_TIMEOUT}), "
            f"updates(connect/read/write/pool={UPDATES_REQUEST_CONNECT_TIMEOUT}/{UPDATES_REQUEST_READ_TIMEOUT}/"
            f"{UPDATES_REQUEST_WRITE_TIMEOUT}/{UPDATES_REQUEST_POOL_TIMEOUT}), "
            f"polling_timeout={POLLING_TIMEOUT_SECONDS}s"
        )
        print("[✓] Ожидание сообщений...")
        
        # Запуск бота с обработкой исключений
        app.run_polling(
            allowed_updates=["message", "edited_message", "business_connection", 
                             "business_message", "edited_business_message", 
                             "deleted_business_messages", "callback_query", "pre_checkout_query"],
            drop_pending_updates=True,
            timeout=POLLING_TIMEOUT_SECONDS,
            bootstrap_retries=POLLING_BOOTSTRAP_RETRIES,
        )
        return 0
        
    except Exception as e:
        print(f"[❌] КРИТИЧЕСКАЯ ОШИБКА: {e}")
        print("[❌] Бот остановлен. Подробности ошибки:")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


