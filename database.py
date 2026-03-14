# database.py
import sqlite3
from datetime import datetime, timedelta, UTC
import os
import json
from typing import Any, Dict, List, Optional, Tuple


def utcnow_naive() -> datetime:
    """UTC now as naive datetime for backward-compatible SQLite timestamps."""
    return datetime.now(UTC).replace(tzinfo=None)

class MessageDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # busy timeout + WAL reduce "database is locked" risk on bursty writes
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.create_tables()
    
    def create_tables(self):
        """РЎРѕР·РґР°РЅРёРµ/РѕР±РЅРѕРІР»РµРЅРёРµ С‚Р°Р±Р»РёС† СЃ Р°РІС‚РѕРјРёРіСЂР°С†РёРµР№"""
        cursor = self.conn.cursor()
        
        # РўР°Р±Р»РёС†Р° Business Connections
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS business_connections (
            business_connection_id TEXT PRIMARY KEY,
            owner_id INTEGER,
            is_active INTEGER DEFAULT 1,
            connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # РџСЂРѕРІРµСЂСЏРµРј СЃСѓС‰РµСЃС‚РІРѕРІР°РЅРёРµ С‚Р°Р±Р»РёС†С‹ messages
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages'")
        messages_exists = cursor.fetchone()
        
        if messages_exists:
            cursor.execute("PRAGMA table_info(messages)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»РµРЅРёРµ owner_id
            if 'owner_id' not in columns:
                print("[AUTO-MIGRATION] РћР±РЅРѕРІР»РµРЅРёРµ СЃС‚СЂСѓРєС‚СѓСЂС‹ Р±Р°Р·С‹ РґР°РЅРЅС‹С…...")
                
                cursor.execute("""
                CREATE TABLE messages_temp (
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
                
                try:
                    cursor.execute("""
                    INSERT INTO messages_temp 
                    SELECT message_id, chat_id, user_id, user_id, username, text, 
                           media_type, media_path, date, is_deleted, is_edited, original_text, NULL
                    FROM messages
                    """)
                except Exception as e:
                    print(f"[WARNING] РћС€РёР±РєР° РєРѕРїРёСЂРѕРІР°РЅРёСЏ РґР°РЅРЅС‹С…: {e}")
                
                cursor.execute("DROP TABLE messages")
                cursor.execute("ALTER TABLE messages_temp RENAME TO messages")
                
                print("[AUTO-MIGRATION] вњ… Р‘Р°Р·Р° РґР°РЅРЅС‹С… РѕР±РЅРѕРІР»РµРЅР° (owner_id)")
            
            # РњРёРіСЂР°С†РёСЏ: РґРѕР±Р°РІР»РµРЅРёРµ reply_to_message_id
            elif 'reply_to_message_id' not in columns:
                print("[AUTO-MIGRATION] Р”РѕР±Р°РІР»РµРЅРёРµ РїРѕР»СЏ reply_to_message_id...")
                cursor.execute("ALTER TABLE messages ADD COLUMN reply_to_message_id INTEGER DEFAULT NULL")
                self.conn.commit()
                print("[AUTO-MIGRATION] вњ… РџРѕР»Рµ reply_to_message_id РґРѕР±Р°РІР»РµРЅРѕ")
        else:
            cursor.execute("""
            CREATE TABLE messages (
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
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS edit_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            chat_id INTEGER,
            owner_id INTEGER,
            old_text TEXT,
            new_text TEXT,
            edit_date TIMESTAMP
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            total_messages INTEGER DEFAULT 0,
            total_deleted INTEGER DEFAULT 0,
            total_edited INTEGER DEFAULT 0,
            total_saved_media INTEGER DEFAULT 0,
            last_activity TIMESTAMP
        )
        """)

        # РРЅРґРµРєСЃС‹ РґР»СЏ С‡Р°СЃС‚С‹С… РІС‹Р±РѕСЂРѕРє/С„РёР»СЊС‚СЂРѕРІ
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_owner_date ON messages(owner_id, date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_owner_date ON messages(chat_id, owner_id, date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_deleted_date ON messages(is_deleted, date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_edited_date ON messages(is_edited, date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_media_date ON messages(media_type, date DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_reply_chain ON messages(reply_to_message_id, chat_id, owner_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_edit_history_lookup ON edit_history(message_id, chat_id, owner_id, edit_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_business_connections_owner_active ON business_connections(owner_id, is_active)")

        # ==================== RBAC ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_roles (
            user_id INTEGER PRIMARY KEY,
            role TEXT NOT NULL CHECK(role IN ('admin', 'admin_lite')),
            assigned_by INTEGER,
            assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_scopes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER NOT NULL,
            owner_id INTEGER NOT NULL,
            chat_id INTEGER,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(admin_user_id, owner_id, chat_id)
        )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_scopes_admin ON admin_scopes(admin_user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_scopes_owner_chat ON admin_scopes(owner_id, chat_id)")

        # ==================== SUBSCRIPTIONS ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY,
            plan_code TEXT,
            duration_days INTEGER NOT NULL,
            starts_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            is_active INTEGER DEFAULT 1,
            source TEXT NOT NULL DEFAULT 'manual',
            telegram_payment_charge_id TEXT,
            invoice_payload TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscription_grants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            plan_code TEXT,
            duration_days INTEGER NOT NULL,
            starts_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            source TEXT NOT NULL,
            granted_by INTEGER,
            grant_comment TEXT,
            telegram_payment_charge_id TEXT,
            invoice_payload TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_active_expires ON subscriptions(is_active, expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_subscription_grants_user_created ON subscription_grants(user_id, created_at DESC)")

        # ==================== STARS PAYMENTS ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS star_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            plan_code TEXT NOT NULL,
            amount_stars INTEGER NOT NULL,
            duration_days INTEGER NOT NULL,
            invoice_payload TEXT NOT NULL UNIQUE,
            telegram_payment_charge_id TEXT,
            status TEXT NOT NULL DEFAULT 'invoice_sent',
            purchased_at TIMESTAMP,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_star_payments_user_created ON star_payments(user_id, created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_star_payments_status ON star_payments(status)")

        # ==================== REFERRALS ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            invited_user_id INTEGER PRIMARY KEY,
            referrer_user_id INTEGER NOT NULL,
            source_payload TEXT,
            first_paid_at TIMESTAMP,
            first_payment_payload TEXT,
            invited_bonus_granted_at TIMESTAMP,
            referrer_bonus_granted_at TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'linked',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS referral_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invited_user_id INTEGER NOT NULL,
            referrer_user_id INTEGER NOT NULL,
            beneficiary_user_id INTEGER NOT NULL,
            reward_type TEXT NOT NULL CHECK(reward_type IN ('invited_bonus', 'referrer_bonus')),
            bonus_days INTEGER NOT NULL,
            source_invoice_payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(invited_user_id, reward_type)
        )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_referrals_first_paid ON referrals(first_paid_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_referral_rewards_beneficiary ON referral_rewards(beneficiary_user_id, created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_referral_rewards_invited ON referral_rewards(invited_user_id)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS referral_retry_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER NOT NULL,
            invoice_payload TEXT NOT NULL,
            payment_user_id INTEGER NOT NULL,
            result_status TEXT NOT NULL,
            invited_bonus_granted INTEGER NOT NULL DEFAULT 0,
            referrer_bonus_granted INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_referral_retry_audit_created ON referral_retry_audit(created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_referral_retry_audit_actor ON referral_retry_audit(actor_user_id, created_at DESC)")

        # ==================== TRIAL / REMINDERS / ACTIVITY ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trials (
            user_id INTEGER PRIMARY KEY,
            duration_days INTEGER NOT NULL,
            starts_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            activated_by INTEGER,
            source TEXT NOT NULL DEFAULT 'self',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trials_expires_status ON trials(status, expires_at)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscription_reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reminder_kind TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, reminder_kind, expires_at)
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_subscription_reminders_user ON subscription_reminders(user_id, sent_at DESC)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS activity_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_text TEXT NOT NULL,
            meta_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_activity_history_user_created ON activity_history(user_id, created_at DESC)")

        # ==================== PROMOCODES ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            promo_type TEXT NOT NULL,
            bonus_days INTEGER DEFAULT 0,
            free_days INTEGER DEFAULT 0,
            discount_percent INTEGER DEFAULT 0,
            fixed_stars_30 INTEGER,
            fixed_stars_90 INTEGER,
            fixed_stars_180 INTEGER,
            plan_code_override TEXT,
            starts_at TIMESTAMP,
            expires_at TIMESTAMP,
            max_activations INTEGER DEFAULT 0,
            per_user_limit INTEGER DEFAULT 1,
            only_new_users INTEGER DEFAULT 0,
            first_payment_only INTEGER DEFAULT 0,
            allow_with_trial INTEGER DEFAULT 1,
            allow_with_other_bonus INTEGER DEFAULT 1,
            is_active INTEGER DEFAULT 1,
            created_by INTEGER,
            comment TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_codes_active_dates ON promo_codes(is_active, starts_at, expires_at)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS promo_code_usages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            promo_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'applied',
            invoice_payload TEXT,
            details TEXT,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_code_usages_promo ON promo_code_usages(promo_id, used_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_code_usages_user ON promo_code_usages(user_id, used_at DESC)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS promo_user_benefits (
            user_id INTEGER PRIMARY KEY,
            promo_id INTEGER NOT NULL,
            benefit_type TEXT NOT NULL,
            discount_percent INTEGER DEFAULT 0,
            fixed_stars_30 INTEGER,
            fixed_stars_90 INTEGER,
            fixed_stars_180 INTEGER,
            plan_code_override TEXT,
            expires_at TIMESTAMP,
            used_invoice_payload TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_promo_user_benefits_active ON promo_user_benefits(is_active, expires_at)")

        # ==================== GIFTS ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS gift_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gift_payload TEXT NOT NULL UNIQUE,
            payer_user_id INTEGER NOT NULL,
            recipient_user_id INTEGER NOT NULL,
            plan_code TEXT NOT NULL,
            duration_days INTEGER NOT NULL,
            amount_stars INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'invoice_sent',
            telegram_payment_charge_id TEXT,
            purchased_at TIMESTAMP,
            expires_at TIMESTAMP,
            notified_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gift_payments_payer ON gift_payments(payer_user_id, created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gift_payments_recipient ON gift_payments(recipient_user_id, created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gift_payments_status ON gift_payments(status)")

        # ==================== BLACKLIST / ANTISPAM ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            blocked_until TIMESTAMP,
            blocked_by INTEGER,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_blacklist_active_until ON blacklist(is_active, blocked_until)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS anti_spam_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            action_key TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_anti_spam_user_action_time ON anti_spam_events(user_id, action_key, created_at DESC)")

        # ==================== TEAM RBAC V2 ====================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS role_templates_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_name TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            is_system INTEGER DEFAULT 1,
            is_active INTEGER DEFAULT 1,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS role_template_permissions_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_name TEXT NOT NULL,
            permission_key TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(template_name, permission_key)
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_role_template_permissions_template ON role_template_permissions_v2(template_name)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_member_roles_v2 (
            user_id INTEGER PRIMARY KEY,
            template_name TEXT NOT NULL,
            is_custom INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            assigned_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_member_permissions_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            permission_key TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, permission_key)
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_team_member_permissions_user ON team_member_permissions_v2(user_id)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_scopes_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            scope_type TEXT NOT NULL,
            owner_id INTEGER,
            chat_id INTEGER,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, scope_type, owner_id, chat_id)
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_team_scopes_user ON team_scopes_v2(user_id)")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER NOT NULL,
            action_key TEXT NOT NULL,
            target_user_id INTEGER,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_actor_time ON admin_audit_log(actor_user_id, created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_action_time ON admin_audit_log(action_key, created_at DESC)")
        
        self.conn.commit()
        self.ensure_default_role_templates_v2()

    @staticmethod
    def _parse_dt(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            candidate = value.replace("T", " ").replace("Z", "")
            if "." in candidate:
                candidate = candidate.split(".", 1)[0]
            try:
                return datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        return None
    
    # ==================== BUSINESS CONNECTIONS ====================
    
    def save_business_connection(self, business_connection_id, owner_id):
        """РЎРѕС…СЂР°РЅРёС‚СЊ СЃРІСЏР·СЊ РјРµР¶РґСѓ business_connection_id Рё owner_id"""
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO business_connections 
        (business_connection_id, owner_id, is_active)
        VALUES (?, ?, 1)
        """, (business_connection_id, owner_id))
        self.conn.commit()
        print(f"[DB] РЎРѕС…СЂР°РЅРµРЅР° СЃРІСЏР·СЊ: {business_connection_id} -> owner {owner_id}")
    
    def get_owner_by_connection(self, business_connection_id):
        """РџРѕР»СѓС‡РёС‚СЊ owner_id РїРѕ business_connection_id"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT owner_id FROM business_connections 
        WHERE business_connection_id = ? AND is_active = 1
        """, (business_connection_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def deactivate_connection(self, business_connection_id):
        """Р”РµР°РєС‚РёРІРёСЂРѕРІР°С‚СЊ СЃРѕРµРґРёРЅРµРЅРёРµ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE business_connections 
        SET is_active = 0 
        WHERE business_connection_id = ?
        """, (business_connection_id,))
        self.conn.commit()
    
    # ==================== РџРћР›Р¬Р—РћР’РђРўР•Р›Р ====================
    
    def register_user(self, user_id, username, first_name):
        """Р РµРіРёСЃС‚СЂР°С†РёСЏ РЅРѕРІРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ. Р’РѕР·РІСЂР°С‰Р°РµС‚ True РµСЃР»Рё РЅРѕРІС‹Р№"""
        cursor = self.conn.cursor()
        
        # РџСЂРѕРІРµСЂСЏРµРј СЃСѓС‰РµСЃС‚РІРѕРІР°РЅРёРµ
        cursor.execute("SELECT user_id, registered_at FROM users WHERE user_id = ?", (user_id,))
        existing = cursor.fetchone()
        
        if existing:
            # РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚
            registered_time = datetime.strptime(existing[1], '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            # Р•СЃР»Рё Р·Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅ РјРµРЅРµРµ 5 СЃРµРєСѓРЅРґ РЅР°Р·Р°Рґ - СЃС‡РёС‚Р°РµРј РЅРѕРІС‹Рј
            is_new = (now - registered_time).total_seconds() < 5
            return is_new
        
        # РќРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ
        cursor.execute("""
        INSERT INTO users (user_id, username, first_name)
        VALUES (?, ?, ?)
        """, (user_id, username, first_name))
        
        cursor.execute("""
        INSERT OR IGNORE INTO user_stats (user_id)
        VALUES (?)
        """, (user_id,))
        
        self.conn.commit()
        return True  # РќРѕРІС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ
    
    def get_user(self, user_id):
        """РџРѕР»СѓС‡РёС‚СЊ РёРЅС„РѕСЂРјР°С†РёСЋ Рѕ РїРѕР»СЊР·РѕРІР°С‚РµР»Рµ"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()
    
    def get_user_info(self, user_id):
        """РџРѕР»СѓС‡РµРЅРёРµ РёРЅС„РѕСЂРјР°С†РёРё Рѕ РїРѕР»СЊР·РѕРІР°С‚РµР»Рµ (alias РґР»СЏ get_user)"""
        return self.get_user(user_id)
    
    def get_all_users(self):
        """РџРѕР»СѓС‡РёС‚СЊ РІСЃРµС… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users ORDER BY registered_at DESC")
        return cursor.fetchall()
    
    def update_user_activity(self, user_id):
        """РћР±РЅРѕРІРёС‚СЊ РІСЂРµРјСЏ РїРѕСЃР»РµРґРЅРµР№ Р°РєС‚РёРІРЅРѕСЃС‚Рё"""
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE user_stats
        SET last_activity = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """, (user_id,))
        self.conn.commit()
    
    # ==================== РЎРћРћР‘Р©Р•РќРРЇ ====================
    
    def save_message(self, message_id, chat_id, owner_id, user_id, username, text, 
                     media_type=None, media_path=None, date=None, reply_to_message_id=None):
        """РЎРѕС…СЂР°РЅРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR IGNORE INTO messages 
        (message_id, chat_id, owner_id, user_id, username, text, media_type, media_path, date, reply_to_message_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (message_id, chat_id, owner_id, user_id, username, text, media_type, 
              media_path, date or datetime.now(), reply_to_message_id))
        
        inserted = cursor.rowcount > 0
        if inserted:
            cursor.execute("""
            UPDATE user_stats
            SET total_messages = total_messages + 1,
                last_activity = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """, (owner_id,))
            
            if media_type:
                cursor.execute("""
                UPDATE user_stats
                SET total_saved_media = total_saved_media + 1
                WHERE user_id = ?
                """, (owner_id,))
        
        self.conn.commit()
    
    def mark_deleted(self, message_id, chat_id, owner_id):
        """РџРѕРјРµС‚РёС‚СЊ РєР°Рє СѓРґР°Р»С‘РЅРЅРѕРµ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE messages 
        SET is_deleted = 1 
        WHERE message_id = ? AND chat_id = ? AND owner_id = ? AND is_deleted = 0
        """, (message_id, chat_id, owner_id))

        if cursor.rowcount > 0:
            cursor.execute("""
            UPDATE user_stats
            SET total_deleted = total_deleted + 1
            WHERE user_id = ?
            """, (owner_id,))
        
        self.conn.commit()
    
    def get_message(self, message_id, chat_id, owner_id):
        """РџРѕР»СѓС‡РёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT * FROM messages 
        WHERE message_id = ? AND chat_id = ? AND owner_id = ?
        """, (message_id, chat_id, owner_id))
        return cursor.fetchone()
    
    def get_reply_chain(self, message_id, chat_id, owner_id, limit=10):
        """РџРѕР»СѓС‡РёС‚СЊ С†РµРїРѕС‡РєСѓ replies (РєС‚Рѕ РЅР° С‡С‚Рѕ РѕС‚РІРµС‚РёР»)"""
        cursor = self.conn.cursor()
        chain = []
        current_id = message_id
        
        for _ in range(limit):
            cursor.execute("""
            SELECT * FROM messages
            WHERE message_id = ? AND chat_id = ? AND owner_id = ?
            """, (current_id, chat_id, owner_id))
            
            msg = cursor.fetchone()
            if not msg:
                break
            
            chain.append(msg)
            
            # РРЅРґРµРєСЃ reply_to_message_id = 12 (РїРѕСЃР»РµРґРЅРµРµ РїРѕР»Рµ)
            if msg[12]:
                current_id = msg[12]
            else:
                break
        
        return chain
    
    def save_edit(self, message_id, chat_id, owner_id, old_text, new_text):
        """РЎРѕС…СЂР°РЅРёС‚СЊ РёР·РјРµРЅРµРЅРёРµ"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
        UPDATE messages 
        SET is_edited = 1, text = ?, original_text = COALESCE(original_text, ?)
        WHERE message_id = ? AND chat_id = ? AND owner_id = ?
        """, (new_text, old_text, message_id, chat_id, owner_id))

        if cursor.rowcount > 0:
            cursor.execute("""
            INSERT INTO edit_history 
            (message_id, chat_id, owner_id, old_text, new_text, edit_date)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (message_id, chat_id, owner_id, old_text, new_text, datetime.now()))
            
            cursor.execute("""
            UPDATE user_stats
            SET total_edited = total_edited + 1
            WHERE user_id = ?
            """, (owner_id,))
        
        self.conn.commit()
    
    def get_edit_history(self, message_id, chat_id, owner_id):
        """РџРѕР»СѓС‡РёС‚СЊ РІСЃСЋ РёСЃС‚РѕСЂРёСЋ РёР·РјРµРЅРµРЅРёР№ СЃРѕРѕР±С‰РµРЅРёСЏ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT old_text, new_text, edit_date FROM edit_history
        WHERE message_id = ? AND chat_id = ? AND owner_id = ?
        ORDER BY edit_date ASC
        """, (message_id, chat_id, owner_id))
        return cursor.fetchall()
    
    # ==================== Р¤РР›Р¬РўР Р« РџРћ Р’Р Р•РњР•РќР ====================
    
    def get_messages_last_hours(self, hours=24, limit=100):
        """РЎРѕРѕР±С‰РµРЅРёСЏ Р·Р° РїРѕСЃР»РµРґРЅРёРµ N С‡Р°СЃРѕРІ"""
        cursor = self.conn.cursor()
        time_threshold = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute("""
        SELECT * FROM messages
        WHERE date >= ?
        ORDER BY date DESC
        LIMIT ?
        """, (time_threshold, limit))
        return cursor.fetchall()
    
    def get_messages_last_days(self, days=7, limit=100):
        """РЎРѕРѕР±С‰РµРЅРёСЏ Р·Р° РїРѕСЃР»РµРґРЅРёРµ N РґРЅРµР№"""
        cursor = self.conn.cursor()
        time_threshold = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute("""
        SELECT * FROM messages
        WHERE date >= ?
        ORDER BY date DESC
        LIMIT ?
        """, (time_threshold, limit))
        return cursor.fetchall()
    
    def get_stats_by_time_range(self, hours=None, days=None):
        """РЎС‚Р°С‚РёСЃС‚РёРєР° Р·Р° РІСЂРµРјРµРЅРЅРѕР№ РґРёР°РїР°Р·РѕРЅ"""
        cursor = self.conn.cursor()
        
        if hours:
            time_threshold = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
        elif days:
            time_threshold = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            time_threshold = '1970-01-01 00:00:00'
        
        cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN is_deleted = 1 THEN 1 ELSE 0 END) as deleted,
            SUM(CASE WHEN is_edited = 1 THEN 1 ELSE 0 END) as edited,
            COUNT(DISTINCT user_id) as users,
            COUNT(DISTINCT chat_id) as chats,
            SUM(CASE WHEN media_type IS NOT NULL THEN 1 ELSE 0 END) as media
        FROM messages
        WHERE date >= ?
        """, (time_threshold,))
        return cursor.fetchone()
    
        # ==================== РђР”РњРРќ Р¤РЈРќРљР¦РР ====================
    
    def get_user_stats(self, user_id):
        """РЎС‚Р°С‚РёСЃС‚РёРєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM user_stats WHERE user_id = ?", (user_id,))
        return cursor.fetchone()
    
    def get_all_stats(self):
        """РћР±С‰Р°СЏ СЃС‚Р°С‚РёСЃС‚РёРєР°"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 
            COUNT(DISTINCT owner_id) as total_users,
            COUNT(*) as total_messages,
            SUM(is_deleted) as total_deleted,
            SUM(is_edited) as total_edited,
            COUNT(CASE WHEN media_type IS NOT NULL THEN 1 END) as total_media
        FROM messages
        """)
        return cursor.fetchone()
    
    def get_user_messages(self, user_id, limit=None):
        """РЎРѕРѕР±С‰РµРЅРёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ (Р’РЎР• РµРіРѕ С‡Р°С‚С‹)"""
        cursor = self.conn.cursor()
        if limit:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE owner_id = ? 
            ORDER BY date DESC 
            LIMIT ?
            """, (user_id, limit))
        else:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE owner_id = ? 
            ORDER BY date DESC
            """, (user_id,))
        return cursor.fetchall()
    
    def get_chat_messages(self, chat_id, owner_id, limit=None):
        """РџРѕР»СѓС‡РµРЅРёРµ СЃРѕРѕР±С‰РµРЅРёР№ РёР· РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ С‡Р°С‚Р°"""
        cursor = self.conn.cursor()
        
        # вњ… РРЎРџР РђР’Р›Р•РќРћ: РџСЂР°РІРёР»СЊРЅР°СЏ РѕР±СЂР°Р±РѕС‚РєР° limit
        if limit:
            cursor.execute("""
            SELECT * FROM messages
            WHERE chat_id = ? AND owner_id = ?
            ORDER BY date DESC
            LIMIT ?
            """, (chat_id, owner_id, limit))
        else:
            # Р‘Р•Р— РћР“Р РђРќРР§Р•РќРР™ - РІСЃРµ СЃРѕРѕР±С‰РµРЅРёСЏ
            cursor.execute("""
            SELECT * FROM messages
            WHERE chat_id = ? AND owner_id = ?
            ORDER BY date DESC
            """, (chat_id, owner_id))
        
        return cursor.fetchall()
    
    def get_user_chats(self, owner_id):
        """РџРѕР»СѓС‡РёС‚СЊ СЃРїРёСЃРѕРє РІСЃРµС… С‡Р°С‚РѕРІ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ СЃ РёРЅС„РѕСЂРјР°С†РёРµР№"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 
            m.chat_id, 
            COUNT(*) as msg_count,
            (SELECT username FROM messages 
             WHERE chat_id = m.chat_id AND owner_id = m.owner_id 
             ORDER BY date DESC LIMIT 1) as last_username
        FROM messages m
        WHERE m.owner_id = ?
        GROUP BY m.chat_id
        ORDER BY msg_count DESC
        """, (owner_id,))
        return cursor.fetchall()
    
    def get_chat_info(self, chat_id, owner_id):
        """РџРѕР»СѓС‡РёС‚СЊ РёРЅС„РѕСЂРјР°С†РёСЋ Рѕ С‡Р°С‚Рµ (РїРѕСЃР»РµРґРЅРёР№ username РёР· СЃРѕРѕР±С‰РµРЅРёР№)"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT username, user_id
        FROM messages
        WHERE chat_id = ? AND owner_id = ?
        ORDER BY date DESC
        LIMIT 1
        """, (chat_id, owner_id))
        result = cursor.fetchone()
        if result:
            return {'username': result[0], 'user_id': result[1]}
        return None
    
    def search_messages(self, query, user_id=None):
        """РџРѕРёСЃРє"""
        cursor = self.conn.cursor()
        if user_id:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE owner_id = ? AND text LIKE ?
            ORDER BY date DESC LIMIT 100
            """, (user_id, f"%{query}%"))
        else:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE text LIKE ?
            ORDER BY date DESC LIMIT 100
            """, (f"%{query}%",))
        return cursor.fetchall()

    
    # ==================== РџРћРРЎРљ РџРћ РўР•РљРЎРўРЈ ====================
    
    def search_messages_by_text(self, search_text, owner_id=None, chat_id=None, limit=200):
        """РџРѕРёСЃРє СЃРѕРѕР±С‰РµРЅРёР№ РїРѕ С‚РµРєСЃС‚Сѓ"""
        cursor = self.conn.cursor()
        
        if owner_id and chat_id:
            # РџРѕРёСЃРє РІ РєРѕРЅРєСЂРµС‚РЅРѕРј С‡Р°С‚Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
            query = """
            SELECT * FROM messages 
            WHERE owner_id = ? AND chat_id = ? 
            AND (text LIKE ? OR original_text LIKE ?)
            ORDER BY date DESC 
            LIMIT ?
            """
            params = (owner_id, chat_id, f'%{search_text}%', f'%{search_text}%', limit)
        elif owner_id:
            # РџРѕРёСЃРє Сѓ РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
            query = """
            SELECT * FROM messages 
            WHERE owner_id = ? 
            AND (text LIKE ? OR original_text LIKE ?)
            ORDER BY date DESC 
            LIMIT ?
            """
            params = (owner_id, f'%{search_text}%', f'%{search_text}%', limit)
        else:
            # Р“Р»РѕР±Р°Р»СЊРЅС‹Р№ РїРѕРёСЃРє
            query = """
            SELECT * FROM messages 
            WHERE text LIKE ? OR original_text LIKE ?
            ORDER BY date DESC 
            LIMIT ?
            """
            params = (f'%{search_text}%', f'%{search_text}%', limit)
        
        cursor.execute(query, params)
        return cursor.fetchall()
    
    def get_database_size(self):
        """Р Р°Р·РјРµСЂ Р±Р°Р·С‹ РґР°РЅРЅС‹С… Рё РєРѕР»РёС‡РµСЃС‚РІРѕ Р·Р°РїРёСЃРµР№"""
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM messages")
        msg_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM edit_history")
        edit_count = cursor.fetchone()[0]
        
        try:
            db_size = os.path.getsize(self.db_path) / (1024 * 1024)
        except:
            db_size = 0
        
        return {
            'messages': msg_count,
            'users': user_count,
            'edits': edit_count,
            'size_mb': round(db_size, 2)
        }
        def _rebuild_user_stats_with_cursor(self, cursor):
        """Rebuild user_stats from current messages state inside an active transaction."""
        cursor.execute("DELETE FROM user_stats")
        cursor.execute("""
        INSERT INTO user_stats (user_id, total_messages, total_deleted, total_edited, total_saved_media, last_activity)
        SELECT
            owner_id AS user_id,
            COUNT(*) AS total_messages,
            COALESCE(SUM(CASE WHEN is_deleted = 1 THEN 1 ELSE 0 END), 0) AS total_deleted,
            COALESCE(SUM(CASE WHEN is_edited = 1 THEN 1 ELSE 0 END), 0) AS total_edited,
            COALESCE(SUM(CASE WHEN media_type IS NOT NULL THEN 1 ELSE 0 END), 0) AS total_saved_media,
            MAX(date) AS last_activity
        FROM messages
        WHERE owner_id IS NOT NULL
        GROUP BY owner_id
        """)
        cursor.execute("""
        INSERT OR IGNORE INTO user_stats (user_id, total_messages, total_deleted, total_edited, total_saved_media, last_activity)
        SELECT user_id, 0, 0, 0, 0, NULL
        FROM users
        """)

    def cleanup_old_messages(self, days=90):
        """Delete old messages with linked edit_history cleanup and user_stats resync."""
        days = int(days)
        if days <= 0:
            raise ValueError("days must be > 0")

        threshold = (utcnow_naive() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")

            cursor.execute("""
            DELETE FROM edit_history
            WHERE EXISTS (
                SELECT 1
                FROM messages m
                WHERE m.message_id = edit_history.message_id
                  AND m.chat_id = edit_history.chat_id
                  AND (
                      (m.owner_id = edit_history.owner_id)
                      OR (m.owner_id IS NULL AND edit_history.owner_id IS NULL)
                  )
                  AND m.date < ?
            )
            """, (threshold,))

            cursor.execute("""
            DELETE FROM messages
            WHERE date < ?
            """, (threshold,))
            deleted_messages = int(cursor.rowcount or 0)

            cursor.execute("""
            DELETE FROM edit_history
            WHERE NOT EXISTS (
                SELECT 1
                FROM messages m
                WHERE m.message_id = edit_history.message_id
                  AND m.chat_id = edit_history.chat_id
                  AND (
                      (m.owner_id = edit_history.owner_id)
                      OR (m.owner_id IS NULL AND edit_history.owner_id IS NULL)
                  )
            )
            """)

            self._rebuild_user_stats_with_cursor(cursor)
            self.conn.commit()
            return deleted_messages
        except Exception:
            self.conn.rollback()
            raise

    def get_user_last_activity(self, user_id):
        """РџРѕСЃР»РµРґРЅРµРµ СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT * FROM messages
        WHERE owner_id = ?
        ORDER BY date DESC
        LIMIT 1
        """, (user_id,))
        return cursor.fetchone()
    
    # ==================== РџР РћРЎРњРћРўР  РџРћ Р”РђРўРђРњ ====================
    
    def get_messages_by_date(self, date_str=None, limit=None):
        """РџРѕР»СѓС‡РёС‚СЊ РІСЃРµ СЃРѕРѕР±С‰РµРЅРёСЏ Р·Р° РєРѕРЅРєСЂРµС‚РЅСѓСЋ РґР°С‚Сѓ"""
        cursor = self.conn.cursor()
        
        if date_str is None:
            from datetime import date
            date_str = date.today().strftime('%Y-%m-%d')
        
        if limit:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE DATE(date) = ?
            ORDER BY date DESC 
            LIMIT ?
            """, (date_str, limit))
        else:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE DATE(date) = ?
            ORDER BY date DESC
            """, (date_str,))
        return cursor.fetchall()
    
    def get_messages_by_period(self, start_date, end_date, limit=None):
        """РџРѕР»СѓС‡РёС‚СЊ СЃРѕРѕР±С‰РµРЅРёСЏ Р·Р° РїРµСЂРёРѕРґ"""
        cursor = self.conn.cursor()
        if limit:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE DATE(date) BETWEEN ? AND ?
            ORDER BY date DESC 
            LIMIT ?
            """, (start_date, end_date, limit))
        else:
            cursor.execute("""
            SELECT * FROM messages 
            WHERE DATE(date) BETWEEN ? AND ?
            ORDER BY date DESC
            """, (start_date, end_date))
        return cursor.fetchall()
    
    def get_stats_by_date(self, date_str=None):
        """РЎС‚Р°С‚РёСЃС‚РёРєР° Р·Р° РєРѕРЅРєСЂРµС‚РЅСѓСЋ РґР°С‚Сѓ"""
        cursor = self.conn.cursor()
        
        if date_str is None:
            from datetime import date
            date_str = date.today().strftime('%Y-%m-%d')
        
        cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(is_deleted) as deleted,
            SUM(is_edited) as edited,
            COUNT(CASE WHEN media_type IS NOT NULL THEN 1 END) as media,
            COUNT(DISTINCT owner_id) as users,
            COUNT(DISTINCT chat_id) as chats
        FROM messages
        WHERE DATE(date) = ?
        """, (date_str,))
        return cursor.fetchone()
    
    def get_available_dates(self, limit=30):
        """РџРѕР»СѓС‡РёС‚СЊ СЃРїРёСЃРѕРє РґР°С‚ СЃ Р°РєС‚РёРІРЅРѕСЃС‚СЊСЋ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT DATE(date) as date, COUNT(*) as count
        FROM messages
        GROUP BY DATE(date)
        ORDER BY date DESC
        LIMIT ?
        """, (limit,))
        return cursor.fetchall()
    
    # ==================== Р РђРЎРЁРР Р•РќРќРђРЇ РђРќРђР›РРўРРљРђ ====================
    
    def get_hourly_stats(self, date_str=None):
        """РЎС‚Р°С‚РёСЃС‚РёРєР° РїРѕ С‡Р°СЃР°Рј Р·Р° РґРµРЅСЊ"""
        cursor = self.conn.cursor()
        
        if date_str is None:
            from datetime import date
            date_str = date.today().strftime('%Y-%m-%d')
        
        cursor.execute("""
        SELECT 
            strftime('%H', date) as hour,
            COUNT(*) as count,
            SUM(is_deleted) as deleted,
            SUM(is_edited) as edited
        FROM messages
        WHERE DATE(date) = ?
        GROUP BY hour
        ORDER BY hour
        """, (date_str,))
        return cursor.fetchall()
    
    def get_top_users_by_messages(self, limit=10):
        """РўРѕРї РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РїРѕ РєРѕР»РёС‡РµСЃС‚РІСѓ СЃРѕРѕР±С‰РµРЅРёР№"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 
            u.user_id,
            u.username,
            u.first_name,
            us.total_messages,
            us.total_deleted,
            us.total_edited
        FROM users u
        JOIN user_stats us ON u.user_id = us.user_id
        ORDER BY us.total_messages DESC
        LIMIT ?
        """, (limit,))
        return cursor.fetchall()
    
    def get_top_users_by_deleted(self, limit=10):
        """РўРѕРї РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РїРѕ СѓРґР°Р»С‘РЅРЅС‹Рј СЃРѕРѕР±С‰РµРЅРёСЏРј"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 
            u.user_id,
            u.username,
            u.first_name,
            us.total_deleted,
            us.total_messages
        FROM users u
        JOIN user_stats us ON u.user_id = us.user_id
        WHERE us.total_deleted > 0
        ORDER BY us.total_deleted DESC
        LIMIT ?
        """, (limit,))
        return cursor.fetchall()
    
    def get_media_stats(self):
        """РЎС‚Р°С‚РёСЃС‚РёРєР° РїРѕ С‚РёРїР°Рј РјРµРґРёР°"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 
            media_type,
            COUNT(*) as count
        FROM messages
        WHERE media_type IS NOT NULL
        GROUP BY media_type
        ORDER BY count DESC
        """)
        return cursor.fetchall()
    
    def get_recent_activity(self, limit=20):
        """РџРѕСЃР»РµРґРЅСЏСЏ Р°РєС‚РёРІРЅРѕСЃС‚СЊ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT * FROM messages
        ORDER BY date DESC
        LIMIT ?
        """, (limit,))
        return cursor.fetchall()
    
    def get_deleted_recent(self, limit=20):
        """РџРѕСЃР»РµРґРЅРёРµ СѓРґР°Р»С‘РЅРЅС‹Рµ СЃРѕРѕР±С‰РµРЅРёСЏ"""
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT * FROM messages
        WHERE is_deleted = 1
        ORDER BY date DESC
        LIMIT ?
        """, (limit,))
        return cursor.fetchall()
    
    def search_advanced(self, query_text=None, owner_id=None, chat_id=None, 
                       media_only=False, deleted_only=False, edited_only=False, limit=100):
        """РџСЂРѕРґРІРёРЅСѓС‚С‹Р№ РїРѕРёСЃРє СЃ С„РёР»СЊС‚СЂР°РјРё"""
        cursor = self.conn.cursor()
        
        conditions = []
        params = []
        
        if query_text:
            conditions.append("text LIKE ?")
            params.append(f"%{query_text}%")
        
        if owner_id:
            conditions.append("owner_id = ?")
            params.append(owner_id)
        
        if chat_id:
            conditions.append("chat_id = ?")
            params.append(chat_id)
        
        if media_only:
            conditions.append("media_type IS NOT NULL")
        
        if deleted_only:
            conditions.append("is_deleted = 1")
        
        if edited_only:
            conditions.append("is_edited = 1")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        cursor.execute(f"""
        SELECT * FROM messages
        WHERE {where_clause}
        ORDER BY date DESC
        LIMIT ?
        """, params + [limit])
        return cursor.fetchall()
    
    # ==================== ADMIN ROLES / SCOPES ====================

    def get_admin_role(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT role FROM admin_roles WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        return row[0] if row else None

    def set_admin_role(self, user_id, role, assigned_by=None):
        if role not in ("admin", "admin_lite"):
            raise ValueError("Unsupported admin role")

        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO admin_roles (user_id, role, assigned_by, assigned_at, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            role = excluded.role,
            assigned_by = excluded.assigned_by,
            updated_at = CURRENT_TIMESTAMP
        """, (user_id, role, assigned_by))
        self.conn.commit()

    def remove_admin_role(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM admin_roles WHERE user_id = ?", (user_id,))
        removed = cursor.rowcount
        cursor.execute("DELETE FROM admin_scopes WHERE admin_user_id = ?", (user_id,))
        self.conn.commit()
        return removed

    def list_admin_roles(self):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, role, assigned_by, assigned_at, updated_at
        FROM admin_roles
        ORDER BY role ASC, updated_at DESC
        """)
        return cursor.fetchall()

    def add_admin_scope(self, admin_user_id, owner_id, chat_id=None, created_by=None):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR IGNORE INTO admin_scopes (admin_user_id, owner_id, chat_id, created_by)
        VALUES (?, ?, ?, ?)
        """, (admin_user_id, owner_id, chat_id, created_by))
        self.conn.commit()
        return cursor.rowcount > 0

    def remove_admin_scope(self, admin_user_id, owner_id=None, chat_id=None):
        cursor = self.conn.cursor()

        if owner_id is None:
            cursor.execute("DELETE FROM admin_scopes WHERE admin_user_id = ?", (admin_user_id,))
        elif chat_id is None:
            cursor.execute("""
            DELETE FROM admin_scopes
            WHERE admin_user_id = ? AND owner_id = ?
            """, (admin_user_id, owner_id))
        else:
            cursor.execute("""
            DELETE FROM admin_scopes
            WHERE admin_user_id = ? AND owner_id = ? AND chat_id = ?
            """, (admin_user_id, owner_id, chat_id))

        deleted = cursor.rowcount
        self.conn.commit()
        return deleted

    def get_admin_scopes(self, admin_user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, admin_user_id, owner_id, chat_id, created_by, created_at
        FROM admin_scopes
        WHERE admin_user_id = ?
        ORDER BY owner_id, chat_id
        """, (admin_user_id,))
        return cursor.fetchall()

    # ==================== SUBSCRIPTIONS ====================

    def get_subscription(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, plan_code, duration_days, starts_at, expires_at, is_active,
               source, telegram_payment_charge_id, invoice_payload, updated_at
        FROM subscriptions
        WHERE user_id = ?
        """, (user_id,))
        return cursor.fetchone()

    def get_active_subscription(self, user_id):
        row = self.get_subscription(user_id)
        if not row:
            return None

        expires_at = self._parse_dt(row[4])
        is_active = int(row[5]) == 1
        if not is_active or expires_at is None or expires_at <= utcnow_naive():
            return None
        return row

    def _grant_subscription_with_cursor(self, cursor, user_id, plan_code, duration_days, source="manual",
                                        granted_by=None, grant_comment=None,
                                        telegram_payment_charge_id=None, invoice_payload=None):
        now = utcnow_naive()

        cursor.execute("""
        SELECT user_id, plan_code, duration_days, starts_at, expires_at, is_active,
               source, telegram_payment_charge_id, invoice_payload, updated_at
        FROM subscriptions
        WHERE user_id = ?
        """, (user_id,))
        current = cursor.fetchone()

        current_expires = self._parse_dt(current[4]) if current else None
        current_active = bool(
            current and int(current[5]) == 1 and current_expires and current_expires > now
        )
        starts_at = current_expires if current_active else now
        expires_at = starts_at + timedelta(days=int(duration_days))

        cursor.execute("""
        INSERT INTO subscriptions
            (user_id, plan_code, duration_days, starts_at, expires_at, is_active, source,
             telegram_payment_charge_id, invoice_payload, updated_at)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            plan_code = excluded.plan_code,
            duration_days = excluded.duration_days,
            starts_at = excluded.starts_at,
            expires_at = excluded.expires_at,
            is_active = 1,
            source = excluded.source,
            telegram_payment_charge_id = excluded.telegram_payment_charge_id,
            invoice_payload = excluded.invoice_payload,
            updated_at = CURRENT_TIMESTAMP
        """, (
            user_id,
            plan_code,
            int(duration_days),
            starts_at.strftime("%Y-%m-%d %H:%M:%S"),
            expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            source,
            telegram_payment_charge_id,
            invoice_payload,
        ))

        cursor.execute("""
        INSERT INTO subscription_grants
            (user_id, plan_code, duration_days, starts_at, expires_at, source, granted_by,
             grant_comment, telegram_payment_charge_id, invoice_payload, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        """, (
            user_id,
            plan_code,
            int(duration_days),
            starts_at.strftime("%Y-%m-%d %H:%M:%S"),
            expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            source,
            granted_by,
            grant_comment,
            telegram_payment_charge_id,
            invoice_payload,
        ))

        return {
            "starts_at": starts_at,
            "expires_at": expires_at,
        }

    def grant_subscription(self, user_id, plan_code, duration_days, source="manual",
                           granted_by=None, grant_comment=None,
                           telegram_payment_charge_id=None, invoice_payload=None):
        cursor = self.conn.cursor()
        result = self._grant_subscription_with_cursor(
            cursor=cursor,
            user_id=user_id,
            plan_code=plan_code,
            duration_days=duration_days,
            source=source,
            granted_by=granted_by,
            grant_comment=grant_comment,
            telegram_payment_charge_id=telegram_payment_charge_id,
            invoice_payload=invoice_payload,
        )
        self.conn.commit()
        return result

    def cancel_subscription(self, user_id, granted_by=None, grant_comment=None):
        now = utcnow_naive()
        cursor = self.conn.cursor()
        existing = self.get_subscription(user_id)

        cursor.execute("""
        UPDATE subscriptions
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """, (user_id,))

        if existing:
            cursor.execute("""
            INSERT INTO subscription_grants
                (user_id, plan_code, duration_days, starts_at, expires_at, source, granted_by,
                 grant_comment, telegram_payment_charge_id, invoice_payload, status)
            VALUES (?, ?, 0, ?, ?, 'manual', ?, ?, ?, ?, 'cancelled')
            """, (
                user_id,
                existing[1],
                now.strftime("%Y-%m-%d %H:%M:%S"),
                now.strftime("%Y-%m-%d %H:%M:%S"),
                granted_by,
                grant_comment,
                existing[7],
                existing[8],
            ))

        self.conn.commit()
        return cursor.rowcount > 0

    def list_subscription_grants(self, user_id=None, limit=20):
        cursor = self.conn.cursor()
        if user_id is None:
            cursor.execute("""
            SELECT id, user_id, plan_code, duration_days, starts_at, expires_at, source,
                   granted_by, grant_comment, telegram_payment_charge_id, invoice_payload,
                   status, created_at
            FROM subscription_grants
            ORDER BY created_at DESC
            LIMIT ?
            """, (limit,))
        else:
            cursor.execute("""
            SELECT id, user_id, plan_code, duration_days, starts_at, expires_at, source,
                   granted_by, grant_comment, telegram_payment_charge_id, invoice_payload,
                   status, created_at
            FROM subscription_grants
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """, (user_id, limit))
        return cursor.fetchall()

    # ==================== LEGACY MIGRATION ====================

    def list_active_business_owner_ids(self) -> List[int]:
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT DISTINCT owner_id
        FROM business_connections
        WHERE is_active = 1
          AND owner_id IS NOT NULL
        ORDER BY owner_id ASC
        """)
        rows = cursor.fetchall()
        owner_ids: List[int] = []
        for row in rows:
            try:
                owner_id = int(row[0])
            except Exception:
                continue
            if owner_id > 0:
                owner_ids.append(owner_id)
        return owner_ids

    def migrate_existing_business_users_to_legacy_access(
        self,
        dry_run: bool = True,
        duration_days: int = 3650,
        plan_code: str = "legacy_grandfathered",
        source: str = "legacy_migration",
        grant_comment: str = "Grandfathered existing production user during migration",
        actor_user_id: Optional[int] = None,
        target_user_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        now = utcnow_naive()
        duration_days = int(duration_days)
        if duration_days <= 0:
            raise ValueError("duration_days must be > 0")

        if target_user_ids is None:
            candidate_ids = self.list_active_business_owner_ids()
        else:
            normalized = set()
            for value in target_user_ids:
                try:
                    user_id = int(value)
                except Exception:
                    continue
                if user_id > 0:
                    normalized.add(user_id)
            candidate_ids = sorted(normalized)

        already_active: List[Dict[str, Any]] = []
        to_grant: List[Dict[str, Any]] = []

        for user_id in candidate_ids:
            active = self.get_active_subscription(user_id)
            if active:
                expires_at = self._parse_dt(active[4])
                already_active.append(
                    {
                        "user_id": user_id,
                        "plan_code": active[1],
                        "source": active[6],
                        "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S") if expires_at else active[4],
                    }
                )
                continue

            to_grant.append({"user_id": user_id})

        preview = {
            "dry_run": bool(dry_run),
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "source_selector": "business_connections.is_active = 1",
            "plan_code": plan_code,
            "source": source,
            "duration_days": duration_days,
            "candidate_total": len(candidate_ids),
            "already_active_total": len(already_active),
            "to_grant_total": len(to_grant),
            "candidate_user_ids": candidate_ids,
            "already_active": already_active,
            "to_grant": to_grant,
        }
        if dry_run:
            return preview

        cursor = self.conn.cursor()
        granted: List[Dict[str, Any]] = []
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            for row in to_grant:
                user_id = int(row["user_id"])
                grant = self._grant_subscription_with_cursor(
                    cursor=cursor,
                    user_id=user_id,
                    plan_code=plan_code,
                    duration_days=duration_days,
                    source=source,
                    granted_by=actor_user_id,
                    grant_comment=grant_comment,
                )
                granted.append(
                    {
                        "user_id": user_id,
                        "starts_at": grant["starts_at"].strftime("%Y-%m-%d %H:%M:%S"),
                        "expires_at": grant["expires_at"].strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

            if actor_user_id is not None:
                try:
                    cursor.execute("""
                    INSERT INTO admin_audit_log (actor_user_id, action_key, target_user_id, details)
                    VALUES (?, ?, NULL, ?)
                    """, (
                        int(actor_user_id),
                        "legacy_migration_grandfather",
                        (
                            f"candidates={len(candidate_ids)};"
                            f"already_active={len(already_active)};"
                            f"granted={len(granted)};"
                            f"plan_code={plan_code};source={source};duration_days={duration_days}"
                        ),
                    ))
                except Exception:
                    # Keep migration successful even if admin_audit_log is unavailable.
                    pass

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        preview["dry_run"] = False
        preview["granted_total"] = len(granted)
        preview["granted"] = granted
        return preview

    # ==================== STARS PAYMENTS ====================

    def create_star_payment(self, user_id, plan_code, amount_stars, duration_days, invoice_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO star_payments
            (user_id, plan_code, amount_stars, duration_days, invoice_payload, status, updated_at)
        VALUES (?, ?, ?, ?, ?, 'invoice_sent', CURRENT_TIMESTAMP)
        ON CONFLICT(invoice_payload) DO UPDATE SET
            user_id = excluded.user_id,
            plan_code = excluded.plan_code,
            amount_stars = excluded.amount_stars,
            duration_days = excluded.duration_days,
            status = 'invoice_sent',
            updated_at = CURRENT_TIMESTAMP
        """, (user_id, plan_code, int(amount_stars), int(duration_days), invoice_payload))
        self.conn.commit()

    def mark_star_payment_precheckout(self, invoice_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE star_payments
        SET status = 'pre_checkout_ok', updated_at = CURRENT_TIMESTAMP
        WHERE invoice_payload = ?
        """, (invoice_payload,))
        self.conn.commit()

    def mark_star_payment_paid(self, invoice_payload, telegram_payment_charge_id, purchased_at, expires_at):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE star_payments
        SET status = 'paid',
            telegram_payment_charge_id = ?,
            purchased_at = ?,
            expires_at = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE invoice_payload = ?
        """, (
            telegram_payment_charge_id,
            purchased_at.strftime("%Y-%m-%d %H:%M:%S"),
            expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            invoice_payload,
        ))
        self.conn.commit()

    def get_star_payment_by_payload(self, invoice_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, user_id, plan_code, amount_stars, duration_days, invoice_payload,
               telegram_payment_charge_id, status, purchased_at, expires_at, created_at, updated_at
        FROM star_payments
        WHERE invoice_payload = ?
        """, (invoice_payload,))
        return cursor.fetchone()

    def process_star_payment_success(self, user_id, invoice_payload, telegram_payment_charge_id, purchased_at=None):
        """Atomically mark Stars payment paid and grant subscription once (idempotent)."""
        user_id = int(user_id)
        payload = str(invoice_payload or "").strip()
        paid_at_dt = purchased_at if isinstance(purchased_at, datetime) else utcnow_naive()
        paid_at_str = paid_at_dt.strftime("%Y-%m-%d %H:%M:%S")

        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("""
            SELECT id, user_id, plan_code, amount_stars, duration_days, invoice_payload,
                   telegram_payment_charge_id, status, purchased_at, expires_at, created_at, updated_at
            FROM star_payments
            WHERE invoice_payload = ?
            """, (payload,))
            payment = cursor.fetchone()
            if not payment:
                self.conn.commit()
                return {"ok": False, "status": "not_found", "invoice_payload": payload}

            payment_user_id = int(payment[1])
            if payment_user_id != user_id:
                self.conn.commit()
                return {
                    "ok": False,
                    "status": "payer_mismatch",
                    "invoice_payload": payload,
                    "expected_user_id": payment_user_id,
                    "actual_user_id": user_id,
                }

            if (payment[7] or "") == "paid":
                self.conn.commit()
                return {
                    "ok": True,
                    "status": "already_paid",
                    "invoice_payload": payload,
                    "plan_code": payment[2],
                    "duration_days": int(payment[4] or 0),
                    "purchased_at": self._parse_dt(payment[8]) or paid_at_dt,
                    "expires_at": self._parse_dt(payment[9]),
                }

            plan_code = payment[2]
            duration_days = int(payment[4] or 0)
            if duration_days <= 0:
                self.conn.commit()
                return {
                    "ok": False,
                    "status": "invalid_duration",
                    "invoice_payload": payload,
                    "plan_code": plan_code,
                }

            grant = self._grant_subscription_with_cursor(
                cursor=cursor,
                user_id=user_id,
                plan_code=plan_code,
                duration_days=duration_days,
                source="stars",
                granted_by=None,
                grant_comment="Telegram Stars payment",
                telegram_payment_charge_id=telegram_payment_charge_id,
                invoice_payload=payload,
            )

            cursor.execute("""
            UPDATE star_payments
            SET status = 'paid',
                telegram_payment_charge_id = ?,
                purchased_at = ?,
                expires_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE invoice_payload = ?
            """, (
                telegram_payment_charge_id,
                paid_at_str,
                grant["expires_at"].strftime("%Y-%m-%d %H:%M:%S"),
                payload,
            ))

            cursor.execute("""
            UPDATE promo_user_benefits
            SET is_active = 0, used_invoice_payload = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND is_active = 1
            """, (payload, user_id))
            promo_consumed = cursor.rowcount > 0

            self.conn.commit()
            return {
                "ok": True,
                "status": "paid",
                "invoice_payload": payload,
                "plan_code": plan_code,
                "duration_days": duration_days,
                "purchased_at": paid_at_dt,
                "expires_at": grant["expires_at"],
                "promo_consumed": promo_consumed,
                "grant": grant,
            }
        except Exception:
            self.conn.rollback()
            raise

    # ==================== REFERRALS ====================

    def bind_referrer(self, invited_user_id, referrer_user_id, source_payload=None):
        invited_user_id = int(invited_user_id)
        referrer_user_id = int(referrer_user_id)

        if invited_user_id <= 0 or referrer_user_id <= 0:
            return {"linked": False, "reason": "invalid_id"}
        if invited_user_id == referrer_user_id:
            return {"linked": False, "reason": "self_referral"}
        if not self.get_user(referrer_user_id):
            return {"linked": False, "reason": "referrer_not_found"}
        if self.has_successful_paid_subscription(invited_user_id):
            return {"linked": False, "reason": "already_paid"}

        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT referrer_user_id
        FROM referrals
        WHERE invited_user_id = ?
        """, (invited_user_id,))
        existing = cursor.fetchone()
        if existing:
            if int(existing[0]) == referrer_user_id:
                return {"linked": False, "reason": "already_linked_same", "referrer_user_id": referrer_user_id}
            return {"linked": False, "reason": "already_linked_other", "referrer_user_id": int(existing[0])}

        cursor.execute("""
        INSERT INTO referrals
            (invited_user_id, referrer_user_id, source_payload, status, updated_at)
        VALUES (?, ?, ?, 'linked', CURRENT_TIMESTAMP)
        """, (invited_user_id, referrer_user_id, source_payload))
        self.conn.commit()
        return {"linked": True, "reason": "linked", "referrer_user_id": referrer_user_id}

    def has_successful_paid_subscription(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 1
        FROM star_payments
        WHERE user_id = ? AND status = 'paid'
        LIMIT 1
        """, (user_id,))
        if cursor.fetchone():
            return True

        cursor.execute("""
        SELECT 1
        FROM subscription_grants
        WHERE user_id = ? AND source = 'stars'
        LIMIT 1
        """, (user_id,))
        return cursor.fetchone() is not None

    def get_referral_by_invited(self, invited_user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT invited_user_id, referrer_user_id, source_payload, first_paid_at, first_payment_payload,
               invited_bonus_granted_at, referrer_bonus_granted_at, status, created_at, updated_at
        FROM referrals
        WHERE invited_user_id = ?
        """, (invited_user_id,))
        return cursor.fetchone()

    def get_user_referral_stats(self, user_id, recent_limit=5):
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM referrals WHERE referrer_user_id = ?", (user_id,))
        invited_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM referrals
        WHERE referrer_user_id = ? AND first_paid_at IS NOT NULL
        """, (user_id,))
        paid_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COALESCE(SUM(bonus_days), 0)
        FROM referral_rewards
        WHERE beneficiary_user_id = ?
        """, (user_id,))
        bonus_days_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COALESCE(SUM(bonus_days), 0)
        FROM referral_rewards
        WHERE beneficiary_user_id = ? AND reward_type = 'referrer_bonus'
        """, (user_id,))
        referrer_bonus_days_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT r.invited_user_id, r.created_at, r.first_paid_at,
               COALESCE(u.username, ''), COALESCE(u.first_name, '')
        FROM referrals r
        LEFT JOIN users u ON u.user_id = r.invited_user_id
        WHERE r.referrer_user_id = ?
        ORDER BY r.created_at DESC
        LIMIT ?
        """, (user_id, int(recent_limit)))
        recent_referrals = cursor.fetchall()

        return {
            "invited_total": invited_total,
            "paid_total": paid_total,
            "bonus_days_total": bonus_days_total,
            "referrer_bonus_days_total": referrer_bonus_days_total,
            "recent_referrals": recent_referrals,
        }

    def get_admin_referral_overview(self, top_limit=10):
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM referrals")
        total_links = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("SELECT COUNT(*) FROM referrals WHERE first_paid_at IS NOT NULL")
        paid_links = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("SELECT COUNT(*) FROM referral_rewards")
        rewards_count = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("SELECT COALESCE(SUM(bonus_days), 0) FROM referral_rewards")
        rewards_days_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT r.referrer_user_id,
               COUNT(*) AS invited_total,
               SUM(CASE WHEN r.first_paid_at IS NOT NULL THEN 1 ELSE 0 END) AS paid_total,
               COALESCE(SUM(COALESCE(rb.bonus_days, 0)), 0) AS bonus_days
        FROM referrals r
        LEFT JOIN (
            SELECT invited_user_id, bonus_days
            FROM referral_rewards
            WHERE reward_type = 'referrer_bonus'
        ) rb ON rb.invited_user_id = r.invited_user_id
        GROUP BY r.referrer_user_id
        ORDER BY paid_total DESC, invited_total DESC, r.referrer_user_id ASC
        LIMIT ?
        """, (int(top_limit),))
        top_referrers = cursor.fetchall()

        return {
            "total_links": total_links,
            "paid_links": paid_links,
            "rewards_count": rewards_count,
            "rewards_days_total": rewards_days_total,
            "top_referrers": top_referrers,
        }

    def get_referrals_for_referrer(self, referrer_user_id, limit=20):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT r.invited_user_id, r.created_at, r.first_paid_at,
               r.invited_bonus_granted_at, r.referrer_bonus_granted_at, r.status,
               COALESCE(u.username, ''), COALESCE(u.first_name, '')
        FROM referrals r
        LEFT JOIN users u ON u.user_id = r.invited_user_id
        WHERE r.referrer_user_id = ?
        ORDER BY r.created_at DESC
        LIMIT ?
        """, (referrer_user_id, int(limit)))
        return cursor.fetchall()

    def log_referral_retry_audit(
        self,
        actor_user_id,
        invoice_payload,
        payment_user_id,
        result_status,
        invited_bonus_granted,
        referrer_bonus_granted,
    ):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO referral_retry_audit
            (actor_user_id, invoice_payload, payment_user_id, result_status, invited_bonus_granted, referrer_bonus_granted)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            int(actor_user_id),
            str(invoice_payload or "").strip(),
            int(payment_user_id) if payment_user_id is not None else 0,
            str(result_status or "unknown")[:120],
            1 if invited_bonus_granted else 0,
            1 if referrer_bonus_granted else 0,
        ))
        self.conn.commit()

    def process_referral_bonus_for_successful_payment(
        self,
        invited_user_id,
        invoice_payload,
        purchased_at,
        invited_bonus_days,
        referrer_bonus_days,
    ):
        now = purchased_at if isinstance(purchased_at, datetime) else utcnow_naive()
        invited_user_id = int(invited_user_id)
        invited_bonus_days = int(invited_bonus_days)
        referrer_bonus_days = int(referrer_bonus_days)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        if invited_user_id <= 0:
            return {
                "applied": False,
                "reason": "invalid_invited_user",
                "referral_found": False,
                "invited_bonus_granted": False,
                "referrer_bonus_granted": False,
                "invited_bonus_already_granted": False,
                "referrer_bonus_already_granted": False,
            }

        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.execute("""
            SELECT invited_user_id, referrer_user_id, first_paid_at,
                   invited_bonus_granted_at, referrer_bonus_granted_at, status
            FROM referrals
            WHERE invited_user_id = ?
            """, (invited_user_id,))
            referral = cursor.fetchone()
            if not referral:
                self.conn.commit()
                return {
                    "applied": False,
                    "reason": "no_referral",
                    "referral_found": False,
                    "invited_bonus_granted": False,
                    "referrer_bonus_granted": False,
                    "invited_bonus_already_granted": False,
                    "referrer_bonus_already_granted": False,
                }

            referrer_user_id = int(referral[1])
            if referrer_user_id == invited_user_id:
                self.conn.commit()
                return {
                    "applied": False,
                    "reason": "self_referral_invalid",
                    "referral_found": True,
                    "referrer_user_id": referrer_user_id,
                    "invited_bonus_granted": False,
                    "referrer_bonus_granted": False,
                    "invited_bonus_already_granted": False,
                    "referrer_bonus_already_granted": False,
                }

            first_paid_marked = False
            if referral[2] is None:
                cursor.execute("""
                UPDATE referrals
                SET first_paid_at = ?, first_payment_payload = ?, status = 'paid', updated_at = CURRENT_TIMESTAMP
                WHERE invited_user_id = ?
                """, (now_str, invoice_payload, invited_user_id))
                first_paid_marked = True

            invited_reward_created = False
            referrer_reward_created = False
            invited_grant = None
            referrer_grant = None

            cursor.execute("""
            SELECT id
            FROM referral_rewards
            WHERE invited_user_id = ? AND reward_type = 'invited_bonus'
            """, (invited_user_id,))
            invited_reward_exists = cursor.fetchone() is not None

            if not invited_reward_exists and invited_bonus_days > 0:
                invited_grant = self._grant_subscription_with_cursor(
                    cursor=cursor,
                    user_id=invited_user_id,
                    plan_code="ref_bonus_invited",
                    duration_days=invited_bonus_days,
                    source="referral_bonus_invited",
                    granted_by=referrer_user_id,
                    grant_comment=f"Referral invited bonus from {referrer_user_id}",
                    invoice_payload=invoice_payload,
                )
                cursor.execute("""
                INSERT INTO referral_rewards
                    (invited_user_id, referrer_user_id, beneficiary_user_id, reward_type, bonus_days, source_invoice_payload)
                VALUES (?, ?, ?, 'invited_bonus', ?, ?)
                """, (
                    invited_user_id,
                    referrer_user_id,
                    invited_user_id,
                    invited_bonus_days,
                    invoice_payload,
                ))
                cursor.execute("""
                UPDATE referrals
                SET invited_bonus_granted_at = ?, updated_at = CURRENT_TIMESTAMP
                WHERE invited_user_id = ?
                """, (now_str, invited_user_id))
                invited_reward_created = True

            cursor.execute("""
            SELECT id
            FROM referral_rewards
            WHERE invited_user_id = ? AND reward_type = 'referrer_bonus'
            """, (invited_user_id,))
            referrer_reward_exists = cursor.fetchone() is not None

            if not referrer_reward_exists and referrer_bonus_days > 0:
                referrer_grant = self._grant_subscription_with_cursor(
                    cursor=cursor,
                    user_id=referrer_user_id,
                    plan_code="ref_bonus_referrer",
                    duration_days=referrer_bonus_days,
                    source="referral_bonus_referrer",
                    granted_by=invited_user_id,
                    grant_comment=f"Referral referrer bonus for invited {invited_user_id}",
                    invoice_payload=invoice_payload,
                )
                cursor.execute("""
                INSERT INTO referral_rewards
                    (invited_user_id, referrer_user_id, beneficiary_user_id, reward_type, bonus_days, source_invoice_payload)
                VALUES (?, ?, ?, 'referrer_bonus', ?, ?)
                """, (
                    invited_user_id,
                    referrer_user_id,
                    referrer_user_id,
                    referrer_bonus_days,
                    invoice_payload,
                ))
                cursor.execute("""
                UPDATE referrals
                SET referrer_bonus_granted_at = ?, updated_at = CURRENT_TIMESTAMP
                WHERE invited_user_id = ?
                """, (now_str, invited_user_id))
                referrer_reward_created = True

            invited_reward_final = invited_reward_exists or invited_reward_created
            referrer_reward_final = referrer_reward_exists or referrer_reward_created
            if invited_reward_final and referrer_reward_final:
                cursor.execute("""
                UPDATE referrals
                SET status = 'rewarded', updated_at = CURRENT_TIMESTAMP
                WHERE invited_user_id = ?
                """, (invited_user_id,))

            self.conn.commit()
            return {
                "applied": invited_reward_created or referrer_reward_created,
                "reason": "ok",
                "referral_found": True,
                "invited_user_id": invited_user_id,
                "referrer_user_id": referrer_user_id,
                "first_paid_marked": first_paid_marked,
                "invited_bonus_granted": invited_reward_created,
                "referrer_bonus_granted": referrer_reward_created,
                "invited_bonus_already_granted": invited_reward_exists and not invited_reward_created,
                "referrer_bonus_already_granted": referrer_reward_exists and not referrer_reward_created,
                "invited_bonus_days": invited_bonus_days if invited_reward_created else 0,
                "referrer_bonus_days": referrer_bonus_days if referrer_reward_created else 0,
                "invited_expires_at": invited_grant["expires_at"] if invited_grant else None,
                "referrer_expires_at": referrer_grant["expires_at"] if referrer_grant else None,
            }
        except sqlite3.IntegrityError:
            self.conn.rollback()
            return {
                "applied": False,
                "reason": "already_processed",
                "referral_found": True,
                "invited_bonus_granted": False,
                "referrer_bonus_granted": False,
                "invited_bonus_already_granted": True,
                "referrer_bonus_already_granted": True,
            }
        except Exception:
            self.conn.rollback()
            raise

    def delete_chat_messages(self, chat_id, owner_id, media_root=None):
        """Delete all chat messages for one owner with safe media cleanup."""
        cursor = self.conn.cursor()
        root_abs = os.path.abspath(media_root) if media_root else None

        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("""
            SELECT DISTINCT media_path
            FROM messages
            WHERE chat_id = ? AND owner_id = ? AND media_path IS NOT NULL AND TRIM(media_path) != ''
            """, (chat_id, owner_id))
            media_paths = [str(row[0]) for row in cursor.fetchall() if row and row[0]]

            cursor.execute("""
            DELETE FROM messages
            WHERE chat_id = ? AND owner_id = ?
            """, (chat_id, owner_id))
            deleted_messages = int(cursor.rowcount or 0)

            cursor.execute("""
            DELETE FROM edit_history
            WHERE chat_id = ? AND owner_id = ?
            """, (chat_id, owner_id))

            shared_paths = []
            unique_paths = []
            seen_paths = set()
            for raw_path in media_paths:
                normalized = raw_path.strip()
                if not normalized or normalized in seen_paths:
                    continue
                seen_paths.add(normalized)
                cursor.execute("SELECT COUNT(*) FROM messages WHERE media_path = ?", (normalized,))
                remaining_refs = int((cursor.fetchone() or [0])[0] or 0)
                if remaining_refs > 0:
                    shared_paths.append((normalized, remaining_refs))
                else:
                    unique_paths.append(normalized)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        deleted_files = 0
        files_skipped_shared = 0
        files_skipped_unsafe = 0
        files_missing = 0
        files_errors = 0

        for raw_path, refs in shared_paths:
            files_skipped_shared += 1
            print(f"[INFO] Skip shared media delete: {raw_path} (remaining_refs={refs})")

        for raw_path in unique_paths:
            abs_path = os.path.abspath(raw_path)
            if not root_abs or not self._is_path_inside_root(abs_path, root_abs):
                files_skipped_unsafe += 1
                print(f"[WARNING] Skip unsafe media path deletion: {raw_path}")
                continue

            if not os.path.isfile(abs_path):
                files_missing += 1
                continue

            try:
                os.unlink(abs_path)
                deleted_files += 1
            except Exception as e:
                files_errors += 1
                print(f"[ERROR] Failed to delete media file {raw_path}: {e}")

        return {
            "messages": deleted_messages,
            "files": deleted_files,
            "files_skipped_shared": files_skipped_shared,
            "files_skipped_unsafe": files_skipped_unsafe,
            "files_missing": files_missing,
            "files_errors": files_errors,
        }

    # ==================== UTILS ====================

    @staticmethod
    def _dt_to_str(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    # ==================== TRIAL ====================

    def get_trial(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, duration_days, starts_at, expires_at, status, activated_by, source, created_at, updated_at
        FROM trials
        WHERE user_id = ?
        """, (user_id,))
        return cursor.fetchone()

    def get_active_trial(self, user_id: int) -> Optional[Tuple[Any, ...]]:
        row = self.get_trial(user_id)
        if not row:
            return None
        expires_at = self._parse_dt(row[3])
        if row[4] != "active" or not expires_at or expires_at <= utcnow_naive():
            return None
        return row

    def has_used_trial(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM trials WHERE user_id = ? LIMIT 1", (user_id,))
        return cursor.fetchone() is not None

    def has_any_subscription_history(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 1
        FROM subscription_grants
        WHERE user_id = ? AND duration_days > 0
        LIMIT 1
        """, (user_id,))
        return cursor.fetchone() is not None

    def can_activate_trial(self, user_id):
        if self.has_used_trial(user_id):
            return False, "trial_already_used"
        if self.has_any_subscription_history(user_id):
            return False, "subscription_already_used"
        return True, "ok"

    def activate_trial(self, user_id, duration_days, activated_by=None, source="self"):
        allowed, reason = self.can_activate_trial(user_id)
        if not allowed:
            return {"ok": False, "reason": reason}

        now = utcnow_naive()
        starts_at = now
        expires_at = now + timedelta(days=int(duration_days))

        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO trials
            (user_id, duration_days, starts_at, expires_at, status, activated_by, source, updated_at)
        VALUES (?, ?, ?, ?, 'active', ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            duration_days = excluded.duration_days,
            starts_at = excluded.starts_at,
            expires_at = excluded.expires_at,
            status = 'active',
            activated_by = excluded.activated_by,
            source = excluded.source,
            updated_at = CURRENT_TIMESTAMP
        """, (
            user_id,
            int(duration_days),
            self._dt_to_str(starts_at),
            self._dt_to_str(expires_at),
            activated_by,
            source,
        ))

        self._grant_subscription_with_cursor(
            cursor=cursor,
            user_id=user_id,
            plan_code="trial",
            duration_days=int(duration_days),
            source="trial",
            granted_by=activated_by,
            grant_comment="Trial activation",
        )
        self.conn.commit()
        return {"ok": True, "starts_at": starts_at, "expires_at": expires_at, "reason": "activated"}

    # ==================== REMINDERS ====================

    def was_reminder_sent(self, user_id, reminder_kind, expires_at):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 1
        FROM subscription_reminders
        WHERE user_id = ? AND reminder_kind = ? AND expires_at = ?
        LIMIT 1
        """, (user_id, reminder_kind, self._dt_to_str(expires_at)))
        return cursor.fetchone() is not None

    def mark_reminder_sent(self, user_id, reminder_kind, expires_at):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR IGNORE INTO subscription_reminders (user_id, reminder_kind, expires_at)
        VALUES (?, ?, ?)
        """, (user_id, reminder_kind, self._dt_to_str(expires_at)))
        self.conn.commit()
        return cursor.rowcount > 0

    def get_expiring_access_candidates(self):
        now = utcnow_naive()
        now_str = self._dt_to_str(now)
        cursor = self.conn.cursor()

        cursor.execute("""
        SELECT user_id, plan_code, expires_at, source, 'subscription' AS access_kind
        FROM subscriptions
        WHERE is_active = 1 AND expires_at > ? AND source != 'trial'
        """, (now_str,))
        subscriptions = cursor.fetchall()

        cursor.execute("""
        SELECT user_id, 'trial', expires_at, 'trial', 'trial' AS access_kind
        FROM trials
        WHERE status = 'active' AND expires_at > ?
        """, (now_str,))
        trials = cursor.fetchall()

        return subscriptions + trials

    # ==================== ACTIVITY ====================

    def log_activity(self, user_id, event_type, event_text, meta=None):
        cursor = self.conn.cursor()
        meta_json = None
        if meta is not None:
            try:
                meta_json = json.dumps(meta, ensure_ascii=False)
            except Exception:
                meta_json = str(meta)
        cursor.execute("""
        INSERT INTO activity_history (user_id, event_type, event_text, meta_json)
        VALUES (?, ?, ?, ?)
        """, (user_id, event_type, event_text, meta_json))
        self.conn.commit()

    def list_user_activity(self, user_id, limit=20, offset=0):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, user_id, event_type, event_text, meta_json, created_at
        FROM activity_history
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
        """, (user_id, int(limit), int(offset)))
        return cursor.fetchall()

    def get_user_action_history(self, user_id, limit=20):
        events = []

        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT date, 'message_deleted', 'РЎРѕРѕР±С‰РµРЅРёРµ Р±С‹Р»Рѕ СѓРґР°Р»РµРЅРѕ'
        FROM messages
        WHERE owner_id = ? AND is_deleted = 1
        ORDER BY date DESC
        LIMIT ?
        """, (user_id, int(limit)))
        events.extend(cursor.fetchall())

        cursor.execute("""
        SELECT date, 'message_edited', 'РЎРѕРѕР±С‰РµРЅРёРµ Р±С‹Р»Рѕ РёР·РјРµРЅРµРЅРѕ'
        FROM messages
        WHERE owner_id = ? AND is_edited = 1
        ORDER BY date DESC
        LIMIT ?
        """, (user_id, int(limit)))
        events.extend(cursor.fetchall())

        cursor.execute("""
        SELECT date, 'media_saved', 'Р’С‹ СЃРѕС…СЂР°РЅРёР»Рё РјРµРґРёР°'
        FROM messages
        WHERE owner_id = ? AND media_type LIKE 'saved_%'
        ORDER BY date DESC
        LIMIT ?
        """, (user_id, int(limit)))
        events.extend(cursor.fetchall())

        cursor.execute("""
        SELECT created_at, 'subscription_event',
               CASE
                   WHEN source = 'manual' THEN 'РџРѕРґРїРёСЃРєР° РІС‹РґР°РЅР° Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂРѕРј'
                   WHEN source = 'stars' THEN 'РџРѕРґРїРёСЃРєР° Р°РєС‚РёРІРёСЂРѕРІР°РЅР° РїРѕ РѕРїР»Р°С‚Рµ'
                   WHEN source = 'trial' THEN 'РђРєС‚РёРІРёСЂРѕРІР°РЅ РїСЂРѕР±РЅС‹Р№ РїРµСЂРёРѕРґ'
                   WHEN source LIKE 'promo_%' THEN 'РџРѕРґРїРёСЃРєР° Р°РєС‚РёРІРёСЂРѕРІР°РЅР° РїРѕ РїСЂРѕРјРѕРєРѕРґСѓ'
                   WHEN source = 'gift' THEN 'РџРѕР»СѓС‡РµРЅР° РїРѕРґР°СЂРѕС‡РЅР°СЏ РїРѕРґРїРёСЃРєР°'
                   WHEN source LIKE 'referral_bonus_%' THEN 'РќР°С‡РёСЃР»РµРЅ СЂРµС„РµСЂР°Р»СЊРЅС‹Р№ Р±РѕРЅСѓСЃ'
                   ELSE 'РЎРѕР±С‹С‚РёРµ РїРѕРґРїРёСЃРєРё'
               END
        FROM subscription_grants
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """, (user_id, int(limit)))
        events.extend(cursor.fetchall())

        cursor.execute("""
        SELECT created_at, event_type, event_text
        FROM activity_history
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """, (user_id, int(limit)))
        events.extend(cursor.fetchall())

        normalized = []
        for created_at, event_type, event_text in events:
            dt = self._parse_dt(created_at) if not isinstance(created_at, datetime) else created_at
            if dt is None:
                dt = utcnow_naive()
            normalized.append((dt, event_type, event_text))

        normalized.sort(key=lambda x: x[0], reverse=True)
        return normalized[: int(limit)]

    # ==================== PROMOCODES ====================

    @staticmethod
    def _normalize_promo_code(code):
        return (code or "").strip().upper()

    def create_promo_code(self, code, promo_type, created_by=None, **kwargs):
        code = self._normalize_promo_code(code)
        if not code:
            return {"ok": False, "reason": "empty_code"}

        cursor = self.conn.cursor()
        try:
            cursor.execute("""
            INSERT INTO promo_codes
                (code, promo_type, bonus_days, free_days, discount_percent,
                 fixed_stars_30, fixed_stars_90, fixed_stars_180, plan_code_override,
                 starts_at, expires_at, max_activations, per_user_limit,
                 only_new_users, first_payment_only, allow_with_trial, allow_with_other_bonus,
                 is_active, created_by, comment, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                code,
                promo_type,
                int(kwargs.get("bonus_days", 0) or 0),
                int(kwargs.get("free_days", 0) or 0),
                int(kwargs.get("discount_percent", 0) or 0),
                kwargs.get("fixed_stars_30"),
                kwargs.get("fixed_stars_90"),
                kwargs.get("fixed_stars_180"),
                kwargs.get("plan_code_override"),
                kwargs.get("starts_at"),
                kwargs.get("expires_at"),
                int(kwargs.get("max_activations", 0) or 0),
                int(kwargs.get("per_user_limit", 1) or 1),
                1 if kwargs.get("only_new_users") else 0,
                1 if kwargs.get("first_payment_only") else 0,
                1 if kwargs.get("allow_with_trial", True) else 0,
                1 if kwargs.get("allow_with_other_bonus", True) else 0,
                1 if kwargs.get("is_active", True) else 0,
                created_by,
                kwargs.get("comment"),
            ))
            self.conn.commit()
            return {"ok": True, "code": code}
        except sqlite3.IntegrityError:
            self.conn.rollback()
            return {"ok": False, "reason": "duplicate_code"}

    def update_promo_code(self, code, **fields):
        code = self._normalize_promo_code(code)
        if not code:
            return False

        allowed = {
            "starts_at", "expires_at", "max_activations", "per_user_limit",
            "only_new_users", "first_payment_only", "allow_with_trial",
            "allow_with_other_bonus", "comment", "is_active",
            "bonus_days", "free_days", "discount_percent",
            "fixed_stars_30", "fixed_stars_90", "fixed_stars_180",
            "plan_code_override",
        }
        updates = []
        params = []
        for key, value in fields.items():
            if key not in allowed:
                continue
            updates.append(f"{key} = ?")
            params.append(value)
        if not updates:
            return False
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(code)
        cursor = self.conn.cursor()
        cursor.execute(
            f"UPDATE promo_codes SET {', '.join(updates)} WHERE code = ?",
            tuple(params),
        )
        changed = cursor.rowcount > 0
        self.conn.commit()
        return changed

    def list_promo_codes(self, limit=50):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, code, promo_type, is_active, starts_at, expires_at,
               max_activations, per_user_limit, created_by, comment, created_at
        FROM promo_codes
        ORDER BY created_at DESC
        LIMIT ?
        """, (int(limit),))
        return cursor.fetchall()

    def get_promo_code(self, code):
        code = self._normalize_promo_code(code)
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, code, promo_type, bonus_days, free_days, discount_percent,
               fixed_stars_30, fixed_stars_90, fixed_stars_180, plan_code_override,
               starts_at, expires_at, max_activations, per_user_limit,
               only_new_users, first_payment_only, allow_with_trial, allow_with_other_bonus,
               is_active, created_by, comment, created_at, updated_at
        FROM promo_codes
        WHERE code = ?
        """, (code,))
        return cursor.fetchone()

    def set_promo_active(self, code, is_active):
        code = self._normalize_promo_code(code)
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE promo_codes
        SET is_active = ?, updated_at = CURRENT_TIMESTAMP
        WHERE code = ?
        """, (1 if is_active else 0, code))
        changed = cursor.rowcount > 0
        self.conn.commit()
        return changed

    def get_promo_usage_stats(self, code):
        promo = self.get_promo_code(code)
        if not promo:
            return None
        promo_id = promo[0]
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM promo_code_usages WHERE promo_id = ?", (promo_id,))
        total = int((cursor.fetchone() or [0])[0] or 0)
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM promo_code_usages WHERE promo_id = ?", (promo_id,))
        users = int((cursor.fetchone() or [0])[0] or 0)
        return {"total_uses": total, "users": users}

    def get_active_promo_benefit(self, user_id):
        now = utcnow_naive()
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, promo_id, benefit_type, discount_percent, fixed_stars_30, fixed_stars_90,
               fixed_stars_180, plan_code_override, expires_at, used_invoice_payload, is_active
        FROM promo_user_benefits
        WHERE user_id = ? AND is_active = 1
        """, (user_id,))
        row = cursor.fetchone()
        if not row:
            return None
        expires_at = self._parse_dt(row[8])
        if expires_at and expires_at <= now:
            cursor.execute("""
            UPDATE promo_user_benefits
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """, (user_id,))
            self.conn.commit()
            return None
        return row

    def consume_promo_benefit(self, user_id, invoice_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE promo_user_benefits
        SET is_active = 0, used_invoice_payload = ?, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ? AND is_active = 1
        """, (invoice_payload, user_id))
        changed = cursor.rowcount > 0
        self.conn.commit()
        return changed

    def get_discounted_stars_for_plan(self, user_id, plan_code, default_stars):
        benefit = self.get_active_promo_benefit(user_id)
        if not benefit:
            return default_stars, None

        benefit_type = benefit[2]
        if benefit_type == "fixed_price_override":
            mapping = {
                "plan_30": benefit[4],
                "plan_90": benefit[5],
                "plan_180": benefit[6],
            }
            fixed = mapping.get(plan_code)
            if isinstance(fixed, int) and fixed > 0:
                return fixed, "promo_fixed"
            return default_stars, None

        if benefit_type == "discount_percent":
            percent = int(benefit[3] or 0)
            if percent <= 0:
                return default_stars, None
            discounted = max(1, int(round(default_stars * (100 - percent) / 100.0)))
            return discounted, "promo_discount"

        return default_stars, None

    def apply_promo_code(self, user_id, code, actor_id=None):
        now = utcnow_naive()
        code = self._normalize_promo_code(code)
        promo = self.get_promo_code(code)
        if not promo:
            return {"ok": False, "reason": "not_found"}

        (
            promo_id, _, promo_type, bonus_days, free_days, discount_percent,
            fixed_30, fixed_90, fixed_180, plan_override,
            starts_at, expires_at, max_activations, per_user_limit,
            only_new_users, first_payment_only, allow_with_trial, _allow_with_other_bonus,
            is_active, _, _, _, _
        ) = promo

        if int(is_active) != 1:
            return {"ok": False, "reason": "inactive"}

        starts_dt = self._parse_dt(starts_at)
        expires_dt = self._parse_dt(expires_at)
        if starts_dt and now < starts_dt:
            return {"ok": False, "reason": "not_started"}
        if expires_dt and now > expires_dt:
            return {"ok": False, "reason": "expired"}

        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM promo_code_usages WHERE promo_id = ?", (promo_id,))
        total_uses = int((cursor.fetchone() or [0])[0] or 0)
        if int(max_activations or 0) > 0 and total_uses >= int(max_activations):
            return {"ok": False, "reason": "global_limit_reached"}

        cursor.execute("SELECT COUNT(*) FROM promo_code_usages WHERE promo_id = ? AND user_id = ?", (promo_id, user_id))
        user_uses = int((cursor.fetchone() or [0])[0] or 0)
        if user_uses >= int(per_user_limit or 1):
            return {"ok": False, "reason": "user_limit_reached"}

        if int(only_new_users or 0) == 1:
            if self.has_any_subscription_history(user_id):
                return {"ok": False, "reason": "only_new_users"}

        if int(first_payment_only or 0) == 1:
            if self.has_successful_paid_subscription(user_id):
                return {"ok": False, "reason": "first_payment_only"}

        if int(allow_with_trial or 1) == 0 and self.get_active_trial(user_id):
            return {"ok": False, "reason": "trial_conflict"}

        result = {"ok": True, "promo_type": promo_type, "code": code}
        details = ""
        if promo_type == "bonus_days":
            grant = self.grant_subscription(
                user_id=user_id,
                plan_code="promo_bonus",
                duration_days=int(bonus_days or 0),
                source="promo_bonus",
                granted_by=actor_id,
                grant_comment=f"Promo {code}",
            )
            result["expires_at"] = grant["expires_at"]
            details = f"bonus_days={int(bonus_days or 0)}"
        elif promo_type == "free_access":
            grant = self.grant_subscription(
                user_id=user_id,
                plan_code="promo_free",
                duration_days=int(free_days or 0),
                source="promo_free",
                granted_by=actor_id,
                grant_comment=f"Promo {code}",
            )
            result["expires_at"] = grant["expires_at"]
            details = f"free_days={int(free_days or 0)}"
        elif promo_type in ("discount_percent", "fixed_price_override", "plan_override"):
            cursor.execute("""
            INSERT INTO promo_user_benefits
                (user_id, promo_id, benefit_type, discount_percent, fixed_stars_30, fixed_stars_90,
                 fixed_stars_180, plan_code_override, expires_at, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                promo_id = excluded.promo_id,
                benefit_type = excluded.benefit_type,
                discount_percent = excluded.discount_percent,
                fixed_stars_30 = excluded.fixed_stars_30,
                fixed_stars_90 = excluded.fixed_stars_90,
                fixed_stars_180 = excluded.fixed_stars_180,
                plan_code_override = excluded.plan_code_override,
                expires_at = excluded.expires_at,
                is_active = 1,
                updated_at = CURRENT_TIMESTAMP
            """, (
                user_id,
                promo_id,
                promo_type,
                int(discount_percent or 0),
                fixed_30,
                fixed_90,
                fixed_180,
                plan_override,
                self._dt_to_str(expires_dt) if expires_dt else None,
            ))
            details = f"benefit={promo_type}"
        else:
            return {"ok": False, "reason": "unsupported_type"}

        cursor.execute("""
        INSERT INTO promo_code_usages (promo_id, user_id, status, details)
        VALUES (?, ?, 'applied', ?)
        """, (promo_id, user_id, details))
        self.conn.commit()
        return result

    # ==================== GIFTS ====================

    def create_gift_payment(self, gift_payload, payer_user_id, recipient_user_id, plan_code, duration_days, amount_stars):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO gift_payments
            (gift_payload, payer_user_id, recipient_user_id, plan_code, duration_days, amount_stars, status, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'invoice_sent', CURRENT_TIMESTAMP)
        ON CONFLICT(gift_payload) DO UPDATE SET
            payer_user_id = excluded.payer_user_id,
            recipient_user_id = excluded.recipient_user_id,
            plan_code = excluded.plan_code,
            duration_days = excluded.duration_days,
            amount_stars = excluded.amount_stars,
            status = 'invoice_sent',
            updated_at = CURRENT_TIMESTAMP
        """, (
            gift_payload, payer_user_id, recipient_user_id, plan_code, int(duration_days), int(amount_stars)
        ))
        self.conn.commit()

    def get_gift_payment_by_payload(self, gift_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, gift_payload, payer_user_id, recipient_user_id, plan_code, duration_days, amount_stars,
               status, telegram_payment_charge_id, purchased_at, expires_at, notified_at, created_at, updated_at
        FROM gift_payments
        WHERE gift_payload = ?
        """, (gift_payload,))
        return cursor.fetchone()

    def mark_gift_payment_precheckout(self, gift_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE gift_payments
        SET status = 'pre_checkout_ok', updated_at = CURRENT_TIMESTAMP
        WHERE gift_payload = ?
        """, (gift_payload,))
        self.conn.commit()

    def mark_gift_payment_paid(self, gift_payload, telegram_payment_charge_id, purchased_at, expires_at):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE gift_payments
        SET status = 'paid',
            telegram_payment_charge_id = ?,
            purchased_at = ?,
            expires_at = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE gift_payload = ?
        """, (
            telegram_payment_charge_id,
            self._dt_to_str(purchased_at),
            self._dt_to_str(expires_at),
            gift_payload,
        ))
        self.conn.commit()

    def process_gift_payment_success(self, payer_user_id, gift_payload, telegram_payment_charge_id, purchased_at=None):
        """Atomically mark gift payment paid and grant recipient subscription once (idempotent)."""
        payer_user_id = int(payer_user_id)
        payload = str(gift_payload or "").strip()
        paid_at_dt = purchased_at if isinstance(purchased_at, datetime) else utcnow_naive()
        paid_at_str = paid_at_dt.strftime("%Y-%m-%d %H:%M:%S")

        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("""
            SELECT id, gift_payload, payer_user_id, recipient_user_id, plan_code, duration_days, amount_stars,
                   status, telegram_payment_charge_id, purchased_at, expires_at, notified_at, created_at, updated_at
            FROM gift_payments
            WHERE gift_payload = ?
            """, (payload,))
            payment = cursor.fetchone()
            if not payment:
                self.conn.commit()
                return {"ok": False, "status": "not_found", "gift_payload": payload}

            db_payer_user_id = int(payment[2])
            recipient_user_id = int(payment[3])
            plan_code = payment[4]
            duration_days = int(payment[5] or 0)
            status = payment[7] or ""

            if db_payer_user_id != payer_user_id:
                self.conn.commit()
                return {
                    "ok": False,
                    "status": "payer_mismatch",
                    "gift_payload": payload,
                    "expected_user_id": db_payer_user_id,
                    "actual_user_id": payer_user_id,
                }

            if status == "paid":
                self.conn.commit()
                return {
                    "ok": True,
                    "status": "already_paid",
                    "gift_payload": payload,
                    "payer_user_id": db_payer_user_id,
                    "recipient_user_id": recipient_user_id,
                    "plan_code": plan_code,
                    "duration_days": duration_days,
                    "purchased_at": self._parse_dt(payment[9]) or paid_at_dt,
                    "expires_at": self._parse_dt(payment[10]),
                }

            if duration_days <= 0:
                self.conn.commit()
                return {
                    "ok": False,
                    "status": "invalid_duration",
                    "gift_payload": payload,
                    "plan_code": plan_code,
                }

            grant = self._grant_subscription_with_cursor(
                cursor=cursor,
                user_id=recipient_user_id,
                plan_code=plan_code,
                duration_days=duration_days,
                source="gift",
                granted_by=db_payer_user_id,
                grant_comment=f"Gift from {db_payer_user_id}",
                telegram_payment_charge_id=telegram_payment_charge_id,
                invoice_payload=payload,
            )

            cursor.execute("""
            UPDATE gift_payments
            SET status = 'paid',
                telegram_payment_charge_id = ?,
                purchased_at = ?,
                expires_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE gift_payload = ?
            """, (
                telegram_payment_charge_id,
                paid_at_str,
                grant["expires_at"].strftime("%Y-%m-%d %H:%M:%S"),
                payload,
            ))

            self.conn.commit()
            return {
                "ok": True,
                "status": "paid",
                "gift_payload": payload,
                "payer_user_id": db_payer_user_id,
                "recipient_user_id": recipient_user_id,
                "plan_code": plan_code,
                "duration_days": duration_days,
                "purchased_at": paid_at_dt,
                "expires_at": grant["expires_at"],
                "grant": grant,
            }
        except Exception:
            self.conn.rollback()
            raise

    def mark_gift_payment_notified(self, gift_payload):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE gift_payments
        SET notified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE gift_payload = ?
        """, (gift_payload,))
        self.conn.commit()

    # ==================== BLACKLIST ====================

    def set_blacklist(self, user_id, reason=None, blocked_until=None, blocked_by=None):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO blacklist (user_id, reason, blocked_until, blocked_by, is_active, updated_at)
        VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            reason = excluded.reason,
            blocked_until = excluded.blocked_until,
            blocked_by = excluded.blocked_by,
            is_active = 1,
            updated_at = CURRENT_TIMESTAMP
        """, (
            int(user_id),
            reason,
            self._dt_to_str(blocked_until) if blocked_until else None,
            blocked_by,
        ))
        self.conn.commit()
        return True

    def remove_blacklist(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE blacklist
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """, (int(user_id),))
        self.conn.commit()
        return cursor.rowcount > 0

    def get_blacklist_entry(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, reason, blocked_until, blocked_by, is_active, created_at, updated_at
        FROM blacklist
        WHERE user_id = ?
        """, (int(user_id),))
        return cursor.fetchone()

    def is_blacklisted(self, user_id):
        row = self.get_blacklist_entry(user_id)
        if not row:
            return False, None
        is_active = int(row[4]) == 1
        if not is_active:
            return False, None
        blocked_until = self._parse_dt(row[2])
        if blocked_until and blocked_until <= utcnow_naive():
            self.remove_blacklist(user_id)
            return False, None
        return True, {
            "user_id": row[0],
            "reason": row[1],
            "blocked_until": row[2],
            "blocked_by": row[3],
        }

    def list_blacklist(self, limit=100):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, reason, blocked_until, blocked_by, is_active, created_at
        FROM blacklist
        WHERE is_active = 1
        ORDER BY created_at DESC
        LIMIT ?
        """, (int(limit),))
        return cursor.fetchall()

    # ==================== ANTI SPAM ====================

    def add_anti_spam_event(self, user_id, action_key):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO anti_spam_events (user_id, action_key)
        VALUES (?, ?)
        """, (int(user_id), str(action_key)))
        self.conn.commit()

    def count_anti_spam_events(self, user_id, action_key, window_seconds):
        since = utcnow_naive() - timedelta(seconds=int(window_seconds))
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT COUNT(*)
        FROM anti_spam_events
        WHERE user_id = ? AND action_key = ? AND created_at >= ?
        """, (int(user_id), str(action_key), self._dt_to_str(since)))
        return int((cursor.fetchone() or [0])[0] or 0)

    # ==================== TEAM RBAC V2 ====================

    def ensure_default_role_templates_v2(self):
        defaults = {
            "admin": {
                "title": "Admin",
                "permissions": [
                    "view_users", "view_chats", "view_messages", "search_messages",
                    "view_media", "export_data", "manage_subscriptions", "manage_trials",
                    "manage_promocodes", "manage_gifts", "manage_referrals", "manage_blacklist",
                    "view_diagnostics", "manage_roles", "manage_scopes", "retry_referrals",
                    "cleanup_media", "archive_media", "manual_grants",
                ],
            },
            "manager": {
                "title": "Manager",
                "permissions": [
                    "view_users", "view_chats", "view_messages", "search_messages",
                    "manage_subscriptions", "manage_trials", "manage_gifts",
                    "manage_referrals", "view_diagnostics", "manual_grants",
                ],
            },
            "support": {
                "title": "Support",
                "permissions": [
                    "view_users", "view_chats", "view_messages", "search_messages",
                    "manage_subscriptions", "manage_trials", "manage_referrals",
                ],
            },
            "analyst": {
                "title": "Analyst",
                "permissions": [
                    "view_users", "view_chats", "view_messages", "search_messages",
                    "view_media", "export_data", "view_diagnostics",
                ],
            },
            "viewer": {
                "title": "Viewer",
                "permissions": [
                    "view_users", "view_chats", "view_messages", "search_messages",
                ],
            },
            "custom": {
                "title": "Custom",
                "permissions": [],
            },
        }

        cursor = self.conn.cursor()
        for template_name, cfg in defaults.items():
            cursor.execute("""
            INSERT OR IGNORE INTO role_templates_v2 (template_name, title, is_system, is_active)
            VALUES (?, ?, 1, 1)
            """, (template_name, cfg["title"]))
            for perm in cfg["permissions"]:
                cursor.execute("""
                INSERT OR IGNORE INTO role_template_permissions_v2 (template_name, permission_key)
                VALUES (?, ?)
                """, (template_name, perm))
        self.conn.commit()

    def list_role_templates_v2(self):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, template_name, title, is_system, is_active, created_at, updated_at
        FROM role_templates_v2
        WHERE is_active = 1
        ORDER BY is_system DESC, template_name ASC
        """)
        return cursor.fetchall()

    def get_role_template_permissions_v2(self, template_name):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT permission_key
        FROM role_template_permissions_v2
        WHERE template_name = ?
        ORDER BY permission_key ASC
        """, (template_name,))
        return [row[0] for row in cursor.fetchall()]

    def set_role_template_permissions_v2(self, template_name, permissions):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM role_template_permissions_v2 WHERE template_name = ?", (template_name,))
        for perm in set(permissions or []):
            cursor.execute("""
            INSERT OR IGNORE INTO role_template_permissions_v2 (template_name, permission_key)
            VALUES (?, ?)
            """, (template_name, perm))
        self.conn.commit()

    def assign_team_role_v2(self, user_id, template_name, assigned_by=None, is_custom=False):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO team_member_roles_v2 (user_id, template_name, is_custom, is_active, assigned_by, updated_at)
        VALUES (?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            template_name = excluded.template_name,
            is_custom = excluded.is_custom,
            is_active = 1,
            assigned_by = excluded.assigned_by,
            updated_at = CURRENT_TIMESTAMP
        """, (int(user_id), str(template_name), 1 if is_custom else 0, assigned_by))
        self.conn.commit()

    def remove_team_role_v2(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute("""
        UPDATE team_member_roles_v2
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """, (int(user_id),))
        self.conn.commit()
        return cursor.rowcount > 0

    def get_team_role_v2(self, user_id: int) -> Optional[Tuple[Any, ...]]:
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, template_name, is_custom, is_active, assigned_by, created_at, updated_at
        FROM team_member_roles_v2
        WHERE user_id = ?
        """, (int(user_id),))
        return cursor.fetchone()

    def list_team_members_v2(self, limit=200):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT user_id, template_name, is_custom, is_active, assigned_by, created_at, updated_at
        FROM team_member_roles_v2
        WHERE is_active = 1
        ORDER BY updated_at DESC
        LIMIT ?
        """, (int(limit),))
        return cursor.fetchall()

    def set_team_custom_permissions_v2(self, user_id, permissions):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM team_member_permissions_v2 WHERE user_id = ?", (int(user_id),))
        for perm in set(permissions or []):
            cursor.execute("""
            INSERT OR IGNORE INTO team_member_permissions_v2 (user_id, permission_key)
            VALUES (?, ?)
            """, (int(user_id), perm))
        self.conn.commit()

    def get_team_permissions_v2(self, user_id: int) -> List[str]:
        role = self.get_team_role_v2(user_id)
        if not role or int(role[3]) != 1:
            return []

        template_name = role[1]
        template_permissions = set(self.get_role_template_permissions_v2(template_name))

        if int(role[2]) == 1:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT permission_key
            FROM team_member_permissions_v2
            WHERE user_id = ?
            """, (int(user_id),))
            custom = {row[0] for row in cursor.fetchall()}
            return sorted(custom)

        return sorted(template_permissions)

    def add_team_scope_v2(self, user_id, scope_type, owner_id=None, chat_id=None, created_by=None):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR IGNORE INTO team_scopes_v2 (user_id, scope_type, owner_id, chat_id, created_by)
        VALUES (?, ?, ?, ?, ?)
        """, (int(user_id), str(scope_type), owner_id, chat_id, created_by))
        self.conn.commit()
        return cursor.rowcount > 0

    def remove_team_scope_v2(self, user_id, scope_type=None, owner_id=None, chat_id=None):
        cursor = self.conn.cursor()
        conditions = ["user_id = ?"]
        params = [int(user_id)]
        if scope_type is not None:
            conditions.append("scope_type = ?")
            params.append(scope_type)
        if owner_id is not None:
            conditions.append("owner_id = ?")
            params.append(owner_id)
        if chat_id is not None:
            conditions.append("chat_id = ?")
            params.append(chat_id)
        sql = "DELETE FROM team_scopes_v2 WHERE " + " AND ".join(conditions)
        cursor.execute(sql, tuple(params))
        deleted = cursor.rowcount
        self.conn.commit()
        return deleted

    def get_team_scopes_v2(self, user_id: int) -> List[Tuple[Any, ...]]:
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, user_id, scope_type, owner_id, chat_id, created_by, created_at
        FROM team_scopes_v2
        WHERE user_id = ?
        ORDER BY scope_type, owner_id, chat_id
        """, (int(user_id),))
        return cursor.fetchall()

    # ==================== ADMIN AUDIT / DIAGNOSTICS ====================

    def log_admin_audit(self, actor_user_id, action_key, target_user_id=None, details=None):
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO admin_audit_log (actor_user_id, action_key, target_user_id, details)
        VALUES (?, ?, ?, ?)
        """, (
            int(actor_user_id),
            str(action_key),
            int(target_user_id) if target_user_id is not None else None,
            details,
        ))
        self.conn.commit()

    def get_admin_audit_recent(self, limit=50):
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT id, actor_user_id, action_key, target_user_id, details, created_at
        FROM admin_audit_log
        ORDER BY created_at DESC
        LIMIT ?
        """, (int(limit),))
        return cursor.fetchall()

    def get_diagnostics_snapshot(self):
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM users")
        users_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM subscriptions
        WHERE is_active = 1 AND expires_at > CURRENT_TIMESTAMP
        """)
        subscriptions_active = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM subscriptions
        WHERE expires_at > CURRENT_TIMESTAMP AND expires_at <= DATETIME('now', '+3 day')
        """)
        expiring_soon = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM subscriptions
        WHERE is_active = 1 AND expires_at <= CURRENT_TIMESTAMP
        """)
        expired = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM trials
        WHERE status = 'active' AND expires_at > CURRENT_TIMESTAMP
        """)
        trials_active = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM business_connections
        WHERE is_active = 1
        """)
        business_connections_active = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM star_payments
        WHERE status = 'paid' AND DATE(updated_at) = DATE('now')
        """)
        paid_today = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM blacklist
        WHERE is_active = 1
        """)
        blacklist_active = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("SELECT COUNT(*) FROM referrals")
        referrals_total = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute("""
        SELECT COUNT(*)
        FROM promo_codes
        WHERE is_active = 1
        """)
        promo_active = int((cursor.fetchone() or [0])[0] or 0)

        return {
            "users_total": users_total,
            "subscriptions_active": subscriptions_active,
            "expiring_soon": expiring_soon,
            "expired": expired,
            "trials_active": trials_active,
            "business_connections_active": business_connections_active,
            "paid_today": paid_today,
            "blacklist_active": blacklist_active,
            "referrals_total": referrals_total,
            "promo_active": promo_active,
        }

    # ==================== HARD DELETE USER ====================

    @staticmethod
    def _is_path_inside_root(candidate_path: str, media_root: str) -> bool:
        try:
            root_abs = os.path.abspath(media_root)
            candidate_abs = os.path.abspath(candidate_path)
            return os.path.commonpath([root_abs, candidate_abs]) == root_abs
        except Exception:
            return False

    def _get_existing_tables(self) -> set:
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {row[0] for row in cursor.fetchall()}

    @staticmethod
    def _count_where_if_exists(cursor, existing_tables, table_name: str, where_sql: str, params: Tuple[Any, ...]) -> int:
        if table_name not in existing_tables:
            return 0
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where_sql}", params)
        return int((cursor.fetchone() or [0])[0] or 0)

    @staticmethod
    def _delete_where_if_exists(cursor, existing_tables, table_name: str, where_sql: str, params: Tuple[Any, ...]) -> int:
        if table_name not in existing_tables:
            return 0
        cursor.execute(f"DELETE FROM {table_name} WHERE {where_sql}", params)
        return int(cursor.rowcount or 0)

    @staticmethod
    def _update_where_if_exists(
        cursor,
        existing_tables,
        table_name: str,
        set_sql: str,
        where_sql: str,
        params: Tuple[Any, ...],
    ) -> int:
        if table_name not in existing_tables:
            return 0
        cursor.execute(f"UPDATE {table_name} SET {set_sql} WHERE {where_sql}", params)
        return int(cursor.rowcount or 0)

    def _ensure_admin_audit_actor_nullable(self):
        """Allow anonymization actor_user_id=NULL in admin_audit_log for old schemas."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='admin_audit_log'")
        if not cursor.fetchone():
            return

        cursor.execute("PRAGMA table_info(admin_audit_log)")
        columns = cursor.fetchall()
        actor_column = next((col for col in columns if col[1] == "actor_user_id"), None)
        if not actor_column:
            return

        # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
        actor_notnull = int(actor_column[3] or 0)
        if actor_notnull == 0:
            return

        cursor.execute("""
        CREATE TABLE admin_audit_log_tmp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER,
            action_key TEXT NOT NULL,
            target_user_id INTEGER,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
        INSERT INTO admin_audit_log_tmp (id, actor_user_id, action_key, target_user_id, details, created_at)
        SELECT id, actor_user_id, action_key, target_user_id, details, created_at
        FROM admin_audit_log
        """)
        cursor.execute("DROP TABLE admin_audit_log")
        cursor.execute("ALTER TABLE admin_audit_log_tmp RENAME TO admin_audit_log")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_actor_time ON admin_audit_log(actor_user_id, created_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_audit_log_action_time ON admin_audit_log(action_key, created_at DESC)")
        self.conn.commit()

    def _build_user_hard_delete_plans(self, user_id: int):
        uid = int(user_id)
        delete_plan = [
            ("messages", "owner_id = ?", (uid,)),
            ("edit_history", "owner_id = ?", (uid,)),
            ("user_stats", "user_id = ?", (uid,)),
            ("business_connections", "owner_id = ?", (uid,)),
            ("subscriptions", "user_id = ?", (uid,)),
            ("subscription_grants", "user_id = ?", (uid,)),
            ("star_payments", "user_id = ?", (uid,)),
            ("trials", "user_id = ?", (uid,)),
            ("subscription_reminders", "user_id = ?", (uid,)),
            ("activity_history", "user_id = ?", (uid,)),
            ("referrals", "invited_user_id = ? OR referrer_user_id = ?", (uid, uid)),
            (
                "referral_rewards",
                "invited_user_id = ? OR referrer_user_id = ? OR beneficiary_user_id = ?",
                (uid, uid, uid),
            ),
            ("referral_retry_audit", "actor_user_id = ? OR payment_user_id = ?", (uid, uid)),
            ("gift_payments", "payer_user_id = ? OR recipient_user_id = ?", (uid, uid)),
            ("promo_code_usages", "user_id = ?", (uid,)),
            ("promo_user_benefits", "user_id = ?", (uid,)),
            ("blacklist", "user_id = ?", (uid,)),
            ("anti_spam_events", "user_id = ?", (uid,)),
            ("team_member_permissions_v2", "user_id = ?", (uid,)),
            ("team_scopes_v2", "user_id = ? OR owner_id = ?", (uid, uid)),
            ("team_member_roles_v2", "user_id = ?", (uid,)),
            ("admin_scopes", "admin_user_id = ? OR owner_id = ?", (uid, uid)),
            ("admin_roles", "user_id = ?", (uid,)),
            ("users", "user_id = ?", (uid,)),
        ]

        # For shared/system records we anonymize actor columns instead of deleting rows.
        anonymize_plan = [
            ("subscription_grants", "granted_by = NULL", "granted_by = ?", (uid,), "subscription_grants.granted_by"),
            ("promo_codes", "created_by = NULL", "created_by = ?", (uid,), "promo_codes.created_by"),
            ("blacklist", "blocked_by = NULL", "blocked_by = ?", (uid,), "blacklist.blocked_by"),
            ("admin_roles", "assigned_by = NULL", "assigned_by = ?", (uid,), "admin_roles.assigned_by"),
            ("admin_scopes", "created_by = NULL", "created_by = ?", (uid,), "admin_scopes.created_by"),
            ("team_member_roles_v2", "assigned_by = NULL", "assigned_by = ?", (uid,), "team_member_roles_v2.assigned_by"),
            ("team_scopes_v2", "created_by = NULL", "created_by = ?", (uid,), "team_scopes_v2.created_by"),
            ("admin_audit_log", "actor_user_id = NULL", "actor_user_id = ?", (uid,), "admin_audit_log.actor_user_id"),
            ("admin_audit_log", "target_user_id = NULL", "target_user_id = ?", (uid,), "admin_audit_log.target_user_id"),
        ]
        return delete_plan, anonymize_plan

    def _collect_user_media_entries(
        self,
        cursor,
        existing_tables,
        user_id: int,
        media_root: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if "messages" not in existing_tables:
            return []

        uid = int(user_id)
        cursor.execute(
            """
            SELECT DISTINCT media_path
            FROM messages
            WHERE owner_id = ?
              AND media_path IS NOT NULL
              AND TRIM(media_path) != ''
            """,
            (uid,),
        )
        rows = cursor.fetchall()

        root_abs = os.path.abspath(media_root) if media_root else None
        entries: List[Dict[str, Any]] = []
        seen_abs_paths = set()

        for row in rows:
            raw_path = str(row[0] or "").strip()
            if not raw_path:
                continue
            abs_path = os.path.abspath(raw_path)
            if abs_path in seen_abs_paths:
                continue
            seen_abs_paths.add(abs_path)

            is_safe = bool(root_abs) and self._is_path_inside_root(abs_path, root_abs)
            exists = os.path.isfile(abs_path)
            size_bytes = 0
            if exists:
                try:
                    size_bytes = int(os.path.getsize(abs_path))
                except Exception:
                    size_bytes = 0

            entries.append(
                {
                    "raw_path": raw_path,
                    "abs_path": abs_path,
                    "is_safe": is_safe,
                    "exists": exists,
                    "size_bytes": size_bytes,
                }
            )
        return entries

    def get_user_hard_delete_preview(self, user_id: int, media_root: Optional[str] = None) -> Dict[str, Any]:
        uid = int(user_id)
        cursor = self.conn.cursor()
        existing_tables = self._get_existing_tables()
        delete_plan, anonymize_plan = self._build_user_hard_delete_plans(uid)

        user_row = None
        if "users" in existing_tables:
            cursor.execute(
                """
                SELECT user_id, username, first_name, registered_at, is_active
                FROM users
                WHERE user_id = ?
                """,
                (uid,),
            )
            user_row = cursor.fetchone()

        delete_counts: Dict[str, int] = {}
        for table_name, where_sql, params in delete_plan:
            delete_counts[table_name] = self._count_where_if_exists(
                cursor, existing_tables, table_name, where_sql, params
            )

        anonymize_counts: Dict[str, int] = {}
        for table_name, _set_sql, where_sql, params, label in anonymize_plan:
            anonymize_counts[label] = self._count_where_if_exists(
                cursor, existing_tables, table_name, where_sql, params
            )

        owned_chats_count = 0
        owned_messages_count = 0
        if "messages" in existing_tables:
            cursor.execute("SELECT COUNT(*) FROM messages WHERE owner_id = ?", (uid,))
            owned_messages_count = int((cursor.fetchone() or [0])[0] or 0)
            cursor.execute("SELECT COUNT(DISTINCT chat_id) FROM messages WHERE owner_id = ?", (uid,))
            owned_chats_count = int((cursor.fetchone() or [0])[0] or 0)

        media_entries = self._collect_user_media_entries(cursor, existing_tables, uid, media_root=media_root)
        media_total = len(media_entries)
        media_existing = sum(1 for item in media_entries if item["exists"])
        media_safe = sum(1 for item in media_entries if item["is_safe"])
        media_bytes = sum(item["size_bytes"] for item in media_entries if item["exists"] and item["is_safe"])
        media_shared_paths: List[str] = []
        if "messages" in existing_tables:
            for item in media_entries:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM messages
                    WHERE media_path = ?
                      AND (owner_id IS NULL OR owner_id != ?)
                    """,
                    (item["raw_path"], uid),
                )
                shared_refs = int((cursor.fetchone() or [0])[0] or 0)
                if shared_refs > 0:
                    media_shared_paths.append(item["raw_path"])
        media_shared_set = set(media_shared_paths)
        media_shared_skipped = len(media_shared_set)
        media_shared_bytes = sum(
            item["size_bytes"]
            for item in media_entries
            if item["raw_path"] in media_shared_set and item["exists"] and item["is_safe"]
        )
        media_deletable_bytes = max(int(media_bytes) - int(media_shared_bytes), 0)

        subscription_row = None
        if "subscriptions" in existing_tables:
            cursor.execute(
                """
                SELECT plan_code, expires_at, is_active, source
                FROM subscriptions
                WHERE user_id = ?
                LIMIT 1
                """,
                (uid,),
            )
            subscription_row = cursor.fetchone()

        delete_total = sum(delete_counts.values())
        user_found = bool(user_row) or delete_total > 0

        return {
            "user_id": uid,
            "user_found": user_found,
            "user_row": user_row,
            "delete_counts": delete_counts,
            "anonymize_counts": anonymize_counts,
            "delete_total": delete_total,
            "owned_messages_count": owned_messages_count,
            "owned_chats_count": owned_chats_count,
            "media_total": media_total,
            "media_existing": media_existing,
            "media_safe": media_safe,
            "media_bytes": int(media_bytes),
            "media_shared_skipped": int(media_shared_skipped),
            "media_shared_bytes": int(media_shared_bytes),
            "media_deletable_bytes": int(media_deletable_bytes),
            "subscription_row": subscription_row,
        }

    def hard_delete_user(self, user_id: int, media_root: Optional[str] = None) -> Dict[str, Any]:
        uid = int(user_id)
        self._ensure_admin_audit_actor_nullable()
        cursor = self.conn.cursor()
        existing_tables = self._get_existing_tables()
        delete_plan, anonymize_plan = self._build_user_hard_delete_plans(uid)
        preview = self.get_user_hard_delete_preview(uid, media_root=media_root)

        media_entries = self._collect_user_media_entries(cursor, existing_tables, uid, media_root=media_root)

        deleted_counts: Dict[str, int] = {}
        anonymized_counts: Dict[str, int] = {}

        try:
            self.conn.execute("BEGIN")
            for table_name, where_sql, params in delete_plan:
                deleted_counts[table_name] = self._delete_where_if_exists(
                    cursor, existing_tables, table_name, where_sql, params
                )
            for table_name, set_sql, where_sql, params, label in anonymize_plan:
                anonymized_counts[label] = self._update_where_if_exists(
                    cursor, existing_tables, table_name, set_sql, where_sql, params
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        files_deleted = 0
        files_missing = 0
        files_unsafe = 0
        files_shared_skipped = 0
        files_shared: List[Dict[str, Any]] = []
        files_errors: List[Dict[str, str]] = []
        files_freed_bytes = 0

        for item in media_entries:
            if not item["is_safe"]:
                files_unsafe += 1
                continue

            if "messages" in existing_tables:
                cursor.execute(
                    "SELECT COUNT(*) FROM messages WHERE media_path = ?",
                    (item["raw_path"],),
                )
                shared_refs = int((cursor.fetchone() or [0])[0] or 0)
                if shared_refs > 0:
                    files_shared_skipped += 1
                    files_shared.append(
                        {
                            "path": item["raw_path"],
                            "remaining_refs": shared_refs,
                        }
                    )
                    continue

            abs_path = item["abs_path"]
            if not os.path.isfile(abs_path):
                files_missing += 1
                continue
            try:
                os.unlink(abs_path)
                files_deleted += 1
                files_freed_bytes += int(item["size_bytes"] or 0)
            except Exception as exc:
                files_errors.append(
                    {
                        "path": item["raw_path"],
                        "error": str(exc),
                    }
                )

        return {
            "ok": True,
            "user_id": uid,
            "preview": preview,
            "deleted_counts": deleted_counts,
            "anonymized_counts": anonymized_counts,
            "deleted_total": int(sum(deleted_counts.values())),
            "files": {
                "total_candidates": len(media_entries),
                "deleted": files_deleted,
                "missing": files_missing,
                "unsafe": files_unsafe,
                "shared_skipped": files_shared_skipped,
                "shared": files_shared,
                "errors_count": len(files_errors),
                "errors": files_errors,
                "freed_bytes": int(files_freed_bytes),
            },
        }

