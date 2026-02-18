from __future__ import annotations

import json
import os
import hashlib
import re
import sqlite3
import threading
import time
import html
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import Flask, request
import requests
import telebot
from bs4 import BeautifulSoup
from telebot import types

app = Flask(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "8553449845:AAHqUxxhLIhUBwV4cJH7KGJUSSkv2lBDDQk").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set BOT_TOKEN environment variable before running.")

bot = telebot.TeleBot(BOT_TOKEN)
GROUP_SETTINGS_FILE = os.getenv(
    "GROUP_SETTINGS_FILE", os.path.join(os.path.dirname(__file__), "group_settings.json")
).strip()
SCORES_FILE = os.getenv("SCORES_FILE", os.path.join(os.path.dirname(__file__), "scores.json")).strip()
EXPENSES_FILE = os.getenv("EXPENSES_FILE", os.path.join(os.path.dirname(__file__), "expenses.json")).strip()
ARCHIVE_FILE = os.getenv("ARCHIVE_FILE", os.path.join(os.path.dirname(__file__), "archive.json")).strip()
RECO_SETTINGS_FILE = os.getenv(
    "RECO_SETTINGS_FILE", os.path.join(os.path.dirname(__file__), "recommendation_settings.json")
).strip()
GLOBAL_SETTINGS_FILE = os.getenv(
    "GLOBAL_SETTINGS_FILE", os.path.join(os.path.dirname(__file__), "global_settings.json")
).strip()
DB_PATH = os.getenv("BOT_DB_PATH", os.path.join(os.path.dirname(__file__), "bot_data.db")).strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "afd12q_bot").strip().lstrip("@").lower()
SCHEDULER_SLEEP_SEC = int(os.getenv("SCHEDULER_SLEEP_SEC", "20"))
OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "0"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-nano").strip()
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").strip().rstrip("/")
DEBUG_SAVE_MSG = os.getenv("DEBUG_SAVE_MSG", "1").strip() in ("1", "true", "on", "yes")
TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))

ITEM_ICON = {
    "price_dollar_rl": "💵",
    "price_eur": "💶",
    "price_aed": "🇦🇪",
    "price_try": "🇹🇷",
    "price_gbp": "💷",
    "price_cad": "🇨🇦",
    "price_aud": "🇦🇺",
    "price_chf": "🏦",
    "price_cny": "🇨🇳",
    "price_jpy": "🇯🇵",
    "price_rub": "🇷🇺",
    "price_sar": "🇸🇦",
    "sekee": "🟡",
    "nim": "🥈",
    "rob": "🥉",
    "gerami": "🪙",
    "sekeb": "🪙",
    "geram18": "✨",
    "ons": "🌍",
    "mesghal": "⚖️",
    "crypto-tether-irr": "💠",
    "crypto-bitcoin": "₿",
    "crypto-ethereum": "♦️",
}

PRICE_SOURCE_URL = os.getenv("PRICE_SOURCE_URL", "https://www.tgju.org/").strip()
PRICE_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}
PRICE_REQUEST_TIMEOUT = 15
MARKET_CURRENCY_MAP = {
    "price_dollar_rl": "دلار آمریکا",
    "price_eur": "یورو",
    "price_aed": "درهم امارات",
    "price_try": "لیر ترکیه",
    "price_gbp": "پوند انگلیس",
    "price_cad": "دلار کانادا",
    "price_aud": "دلار استرالیا",
    "price_chf": "فرانک سوئیس",
    "price_cny": "یوان چین",
    "price_jpy": "ین ژاپن",
    "price_rub": "روبل روسیه",
    "price_sar": "ریال عربستان",
}
MARKET_COIN_MAP = {
    "sekee": "سکه تمام امامی",
    "nim": "نیم سکه",
    "rob": "ربع سکه",
    "gerami": "سکه گرمی",
    "sekeb": "سکه بهار آزادی",
}
MARKET_GOLD_MAP = {
    "geram18": "طلای 18 عیار",
    "ons": "اونس جهانی طلا",
    "mesghal": "مثقال طلا",
}
MARKET_CRYPTO_MAP = {
    "crypto-tether-irr": "تتر (تومان)",
    "crypto-bitcoin": "بیت کوین",
    "crypto-ethereum": "اتریوم",
}

SETTINGS_LOCK = threading.Lock()
TRIGGER_COOLDOWN_TS: dict[str, int] = {}
BOT_USER_ID: int | None = None
DB_LOCK = threading.Lock()
USER_RECO_SESSIONS: dict[str, dict[str, Any]] = {}
REMINDER_DRAFTS: dict[str, dict[str, Any]] = {}
REMINDER_DRAFT_COUNTER = 0

NEGATIVE_REPLY_KEYWORDS = ("کسشر شناسایی شد", "کسشر", "چرت بود", "مزخرف بود")
POSITIVE_REPLY_KEYWORDS = ("جمله طلایی", "خفن بود", "عالی بود", "دمت گرم")

DAILY_MOVIES = [
    (
        "The Truman Show",
        "ترومن متوجه می‌شود زندگی روزمره‌اش یک برنامه تلویزیونی عظیم بوده است. "
        "داستان با لحنی سرگرم‌کننده شروع می‌شود اما خیلی زود به نقد جدی رسانه و کنترل ذهن می‌رسد. "
        "بازی جیم کری همزمان طنز و اضطراب شخصیت را خوب منتقل می‌کند. "
        "فیلم درباره انتخاب آگاهانه، آزادی فردی و شکستن قاب‌های تحمیلی است. "
        "اگر فیلم ایده‌محور دوست داری، این یکی هم فکر می‌دهد هم سرگرم می‌کند."
    ),
    (
        "12 Angry Men",
        "کل فیلم تقریبا در یک اتاق می‌گذرد اما تنش لحظه‌به‌لحظه بیشتر می‌شود. "
        "موضوع اصلی قضاوت عجولانه و اثر پیش‌داوری روی تصمیم جمعی است. "
        "دیالوگ‌ها دقیق و منطقی‌اند و هر شخصیت نماینده یک نوع طرز فکر است. "
        "بدون صحنه‌های پرزرق‌وبرق، فقط با متن و بازیگری تو را درگیر می‌کند. "
        "برای بحث گروهی بعد از تماشا، انتخاب فوق‌العاده‌ای است."
    ),
    (
        "Whiplash",
        "روایت رابطه‌ی سخت یک هنرجو با استاد بسیار کمال‌گراست. "
        "فیلم مرز بین انگیزه‌دادن و تخریب‌کردن را به شکل تند نشان می‌دهد. "
        "ریتم تدوین و موسیقی باعث می‌شود فشار تمرین را با پوستت حس کنی. "
        "پایان‌بندی اثرگذار فیلم بحث‌های زیادی درباره موفقیت و هزینه آن باز می‌کند. "
        "اگر در مسیر پیشرفت شخصی هستی، این فیلم از زاویه‌ای متفاوت تکانت می‌دهد."
    ),
]
DAILY_SERIES = [
    (
        "Dark",
        "داستان با گم‌شدن یک نوجوان شروع می‌شود اما خیلی زود به معمای زمانی چندنسلی تبدیل می‌شود. "
        "سریال به‌دقت روابط خانوادگی و اثر تصمیم‌ها در زمان‌های مختلف را می‌سازد. "
        "فضاسازی سرد و موسیقی خاصش حس تعلیق مداوم ایجاد می‌کند. "
        "هر فصل پازل را بزرگ‌تر می‌کند و بیننده را وادار به توجه دقیق می‌کند. "
        "برای مخاطبی که داستان پیچیده و منسجم می‌خواهد، انتخاب خیلی خوبی است."
    ),
    (
        "Chernobyl",
        "این مینی‌سریال با جزئیات بالا فاجعه چرنوبیل را بازسازی می‌کند. "
        "تمرکز اصلی روی هزینه انسانی پنهان‌کاری و تصمیم‌های اشتباه مدیریتی است. "
        "بازی‌ها، طراحی صحنه و روایت مستندگونه حس واقعیت را تقویت می‌کنند. "
        "هر قسمت علاوه بر هیجان، اطلاعات دقیق و قابل بحث ارائه می‌دهد. "
        "اگر درام واقع‌گرا می‌خواهی، یکی از بهترین گزینه‌هاست."
    ),
    (
        "Severance",
        "ایده اصلی سریال جداسازی ذهن کاری از زندگی شخصی است. "
        "این ایده ساده، به مرور به یک نقد عمیق درباره هویت و اختیار تبدیل می‌شود. "
        "تصویرپردازی مینیمال و فضای اداری عجیب، حس ناآرامی دائمی می‌سازد. "
        "ریتم داستان با معماهای کوچک جلو می‌رود و کم‌کم تصویر بزرگ را می‌سازد. "
        "برای طرفداران داستان‌های متفاوت و هوشمند، تجربه‌ای خاص است."
    ),
]
DAILY_BOOKS = [
    (
        "Atomic Habits",
        "این کتاب توضیح می‌دهد تغییرهای کوچک اما مداوم چطور نتیجه‌های بزرگ می‌سازند. "
        "تمرکز اصلی روی طراحی سیستم به جای تکیه صرف به انگیزه است. "
        "با مثال‌های ساده نشان می‌دهد چگونه عادت بد را سخت‌تر و عادت خوب را آسان‌تر کنیم. "
        "نکته مهم کتاب این است که هویت جدید با تکرار رفتار کوچک ساخته می‌شود. "
        "برای شروع تغییر پایدار، یکی از کاربردی‌ترین کتاب‌هاست."
    ),
    (
        "Sapiens",
        "کتاب مسیر تحول انسان را از شکارگری تا تمدن مدرن روایت می‌کند. "
        "ایده اصلی این است که قدرت همکاری انسان از داستان‌ها و باورهای مشترک آمده است. "
        "نویسنده با زبان روان، موضوعات پیچیده تاریخ و اقتصاد و زیست‌شناسی را پیوند می‌زند. "
        "کتاب پرسش‌های جدی درباره پول، دین، دولت و آینده بشر مطرح می‌کند. "
        "اگر دنبال دید کلان و فکری هستی، انتخاب خیلی خوبی است."
    ),
    (
        "1984",
        "این رمان دنیایی را نشان می‌دهد که در آن نظارت و سانسور همه‌چیز را کنترل می‌کند. "
        "روایت کتاب درباره دستکاری حقیقت و اثر آن روی ذهن و آزادی انسان است. "
        "زبان اثر ساده اما فضای آن عمیق و هشداردهنده است. "
        "با اینکه قدیمی است، موضوعاتش هنوز برای عصر رسانه بسیار قابل لمس است. "
        "برای خواننده‌ای که داستان جدی و فکری می‌خواهد، بسیار مناسب است."
    ),
]

MOVIE_GENRES = {
    "The Truman Show": "درام، کمدی-درام، اجتماعی",
    "12 Angry Men": "درام دادگاهی، روان‌شناختی",
    "Whiplash": "درام، موسیقی، انگیزشی",
}
SERIES_GENRES = {
    "Dark": "معمایی، علمی‌تخیلی، درام",
    "Chernobyl": "درام تاریخی، واقع‌گرا",
    "Severance": "معمایی، علمی‌تخیلی، تریلر روان‌شناختی",
}
BOOK_CATEGORIES = {
    "Atomic Habits": "خودسازی، عادت‌سازی، بهره‌وری",
    "Sapiens": "تاریخ، انسان‌شناسی، علوم اجتماعی",
    "1984": "رمان کلاسیک، دیستوپیا، سیاسی-اجتماعی",
}

DEFAULT_RECO_RANDOM_INPUTS = [
    "کاربر پیشنهاد با ریتم مناسب و داستان درگیرکننده می‌خواهد.",
    "کاربر حال‌وهوای الهام‌بخش و بدون خشونت زیاد می‌خواهد.",
    "کاربر اثر فکر‌برانگیز با روایت قوی می‌خواهد.",
    "کاربر محتوای سرگرم‌کننده اما با کیفیت و امتیاز خوب می‌خواهد.",
]

RECO_PROMPT_TEMPLATE = (
    "تو یک پیشنهاددهنده حرفه‌ای هستی.\n"
    "نوع درخواست: {kind_fa}\n"
    "ورودی کاربر:\n{profile}\n\n"
    "خروجی را فقط فارسی و دقیقا با این ساختار بده:\n"
    "نام: ...\n"
    "{taxonomy_label}: ...\n"
    "چرا این پیشنهاد؟\n"
    "1) ...\n"
    "2) ...\n"
    "3) ...\n"
    "4) ...\n"
    "5) ...\n"
)


def _default_group_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "trigger": "!prices",
        "auto_enabled": False,
        "interval_min": 30,
        "title": "📊 گزارش قیمت",
        "silent": False,
        "last_sent_ts": 0,
        "show_percent": True,
        "include_crypto": False,
        "pin_report": False,
        "mention_enabled": True,
        "cooldown_sec": 20,
    }


def load_group_settings() -> dict[str, Any]:
    if not os.path.exists(GROUP_SETTINGS_FILE):
        return {}
    try:
        with open(GROUP_SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _load_json_map(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def save_group_settings(settings: dict[str, Any]) -> None:
    with SETTINGS_LOCK:
        with open(GROUP_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)


GROUP_SETTINGS = load_group_settings()
SCORES = _load_json_map(SCORES_FILE)
EXPENSES = _load_json_map(EXPENSES_FILE)
ARCHIVE = _load_json_map(ARCHIVE_FILE)
RECO_SETTINGS = _load_json_map(RECO_SETTINGS_FILE)
GLOBAL_SETTINGS = _load_json_map(GLOBAL_SETTINGS_FILE)


def save_scores() -> None:
    with SETTINGS_LOCK:
        with open(SCORES_FILE, "w", encoding="utf-8") as f:
            json.dump(SCORES, f, ensure_ascii=False, indent=2)


def save_expenses() -> None:
    with SETTINGS_LOCK:
        with open(EXPENSES_FILE, "w", encoding="utf-8") as f:
            json.dump(EXPENSES, f, ensure_ascii=False, indent=2)


def save_archive() -> None:
    with SETTINGS_LOCK:
        with open(ARCHIVE_FILE, "w", encoding="utf-8") as f:
            json.dump(ARCHIVE, f, ensure_ascii=False, indent=2)


def save_reco_settings() -> None:
    with SETTINGS_LOCK:
        with open(RECO_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(RECO_SETTINGS, f, ensure_ascii=False, indent=2)


def save_global_settings() -> None:
    with SETTINGS_LOCK:
        with open(GLOBAL_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(GLOBAL_SETTINGS, f, ensure_ascii=False, indent=2)


def init_database() -> None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    updated_at INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS group_members (
                    chat_id INTEGER,
                    user_id INTEGER,
                    role TEXT,
                    updated_at INTEGER,
                    PRIMARY KEY (chat_id, user_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS action_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    user_id INTEGER,
                    action TEXT,
                    payload TEXT,
                    created_at INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS recommendation_cache (
                    cache_key TEXT PRIMARY KEY,
                    response_text TEXT NOT NULL,
                    model TEXT,
                    created_at INTEGER,
                    hit_count INTEGER DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    requester_user_id INTEGER NOT NULL,
                    target_user_id INTEGER NOT NULL,
                    target_name TEXT,
                    reminder_text TEXT,
                    due_ts INTEGER NOT NULL,
                    sent INTEGER DEFAULT 0,
                    created_at INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_group_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    save_key TEXT NOT NULL,
                    message_id INTEGER,
                    message_link TEXT,
                    author_user_id INTEGER,
                    note TEXT,
                    created_at INTEGER,
                    UNIQUE(chat_id, save_key)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def db_touch_user(user, chat_id: int | None = None, role: str = "member") -> None:
    if user is None:
        return
    now_ts = int(time.time())
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    updated_at=excluded.updated_at
                """,
                (
                    int(user.id),
                    getattr(user, "username", None),
                    getattr(user, "first_name", None),
                    getattr(user, "last_name", None),
                    now_ts,
                ),
            )
            if chat_id is not None:
                cur.execute(
                    """
                    INSERT INTO group_members (chat_id, user_id, role, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id, user_id) DO UPDATE SET
                        role=excluded.role,
                        updated_at=excluded.updated_at
                    """,
                    (int(chat_id), int(user.id), role, now_ts),
                )
            conn.commit()
        finally:
            conn.close()


def db_log_action(chat_id: int | None, user_id: int | None, action: str, payload: str = "") -> None:
    now_ts = int(time.time())
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO action_logs (chat_id, user_id, action, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                (chat_id, user_id, action, payload[:1000], now_ts),
            )
            conn.commit()
        finally:
            conn.close()


def _cache_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def db_get_recommendation_cache(cache_key: str) -> str | None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT response_text FROM recommendation_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cur.fetchone()
            if row and row[0]:
                cur.execute(
                    "UPDATE recommendation_cache SET hit_count = COALESCE(hit_count, 0) + 1 WHERE cache_key = ?",
                    (cache_key,),
                )
                conn.commit()
                return str(row[0])
        finally:
            conn.close()
    return None


def db_set_recommendation_cache(cache_key: str, response_text: str, model_name: str) -> None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO recommendation_cache (cache_key, response_text, model, created_at, hit_count)
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(cache_key) DO UPDATE SET
                    response_text=excluded.response_text,
                    model=excluded.model,
                    created_at=excluded.created_at
                """,
                (cache_key, response_text, model_name, int(time.time())),
            )
            conn.commit()
        finally:
            conn.close()


def db_add_reminder(
    chat_id: int,
    requester_user_id: int,
    target_user_id: int,
    target_name: str,
    reminder_text: str,
    due_ts: int,
) -> None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO reminders
                (chat_id, requester_user_id, target_user_id, target_name, reminder_text, due_ts, sent, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    chat_id,
                    requester_user_id,
                    target_user_id,
                    target_name,
                    reminder_text[:1500],
                    int(due_ts),
                    int(time.time()),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def db_get_due_reminders(now_ts: int, limit: int = 20) -> list[tuple]:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, chat_id, target_user_id, target_name, reminder_text, due_ts
                FROM reminders
                WHERE sent = 0 AND due_ts <= ?
                ORDER BY due_ts ASC
                LIMIT ?
                """,
                (int(now_ts), int(limit)),
            )
            return cur.fetchall()
        finally:
            conn.close()


def db_mark_reminder_sent(reminder_id: int) -> None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE reminders SET sent = 1 WHERE id = ?", (int(reminder_id),))
            conn.commit()
        finally:
            conn.close()


def db_save_group_message(
    chat_id: int,
    save_key: str,
    message_id: int | None,
    message_link: str | None,
    author_user_id: int,
    note: str = "",
) -> None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO saved_group_messages
                (chat_id, save_key, message_id, message_link, author_user_id, note, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, save_key) DO UPDATE SET
                    message_id=excluded.message_id,
                    message_link=excluded.message_link,
                    author_user_id=excluded.author_user_id,
                    note=excluded.note,
                    created_at=excluded.created_at
                """,
                (
                    int(chat_id),
                    save_key.strip().lower(),
                    message_id if message_id is not None else None,
                    message_link or "",
                    int(author_user_id),
                    (note or "")[:500],
                    int(time.time()),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def db_get_saved_group_message(chat_id: int, save_key: str) -> tuple | None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT save_key, message_id, message_link, note, created_at
                FROM saved_group_messages
                WHERE chat_id = ? AND save_key = ?
                """,
                (int(chat_id), save_key.strip().lower()),
            )
            return cur.fetchone()
        finally:
            conn.close()


def db_list_saved_group_messages(chat_id: int, limit: int = 50) -> list[tuple]:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT save_key, message_link, note, created_at
                FROM saved_group_messages
                WHERE chat_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (int(chat_id), int(limit)),
            )
            return cur.fetchall()
        finally:
            conn.close()


def db_delete_saved_group_message(chat_id: int, save_key: str) -> bool:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM saved_group_messages WHERE chat_id = ? AND save_key = ?",
                (int(chat_id), save_key.strip().lower()),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def track_message_context(message, action: str = "message") -> None:
    try:
        user = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        if user is not None:
            if chat is not None and getattr(chat, "type", "") in ("group", "supergroup"):
                db_touch_user(user, chat_id=int(chat.id), role="member")
            else:
                db_touch_user(user)
            db_log_action(
                int(chat.id) if chat is not None else None,
                int(user.id),
                action,
                (getattr(message, "text", "") or "")[:500],
            )
    except Exception:
        pass


def is_group_chat(message) -> bool:
    return message.chat.type in ("group", "supergroup")


def is_command_message(message) -> bool:
    text = (message.text or "").strip()
    return text.startswith("/")


def normalize_text(text: str | None) -> str:
    return (text or "").strip().lower()


def compact_text(text: str | None) -> str:
    value = normalize_text(text)
    value = value.replace("\u200c", " ").replace("_", " ")
    return " ".join(value.split())


def normalize_fa_text(text: str | None) -> str:
    s = compact_text(text)
    s = s.replace("ي", "ی").replace("ك", "ک")
    for ch in (".", "!", "?", "؟", "،", ",", ":", ";", "؛", "\"", "'", "(", ")", "[", "]"):
        s = s.replace(ch, " ")
    return " ".join(s.split())


def log_save_msg_debug(payload: str) -> None:
    if not DEBUG_SAVE_MSG:
        return
    ts = datetime.now(TEHRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[save_msg {ts}] {payload}")


def is_for_this_bot(command_text: str) -> bool:
    cmd = normalize_text(command_text)
    if "@" not in cmd:
        return True
    _, at, username = cmd.partition("@")
    return bool(at) and username == BOT_USERNAME


def get_bot_user_id() -> int:
    global BOT_USER_ID
    if BOT_USER_ID is None:
        BOT_USER_ID = bot.get_me().id
    return BOT_USER_ID


def is_reply_to_this_bot(message) -> bool:
    reply = getattr(message, "reply_to_message", None)
    if not reply:
        return False
    reply_user = getattr(reply, "from_user", None)
    if reply_user is None:
        return False

    reply_username = normalize_text(getattr(reply_user, "username", ""))
    if reply_username and reply_username == BOT_USERNAME:
        return True

    if getattr(reply_user, "is_bot", False):
        return True

    try:
        return int(reply_user.id) == int(get_bot_user_id())
    except Exception:
        return False


def is_group_trigger_message(message) -> bool:
    if not is_group_chat(message):
        return False
    if is_command_message(message):
        return False
    cfg = get_group_config(message.chat.id)
    if not bool(cfg.get("enabled", True)):
        return False
    trigger = normalize_text(cfg.get("trigger", "!prices"))
    text = normalize_text(message.text)
    mention_trigger = f"{trigger}@{BOT_USERNAME}"
    return text in (trigger, mention_trigger)


def get_group_config(chat_id: int) -> dict[str, Any]:
    key = str(chat_id)
    defaults = _default_group_config()
    with SETTINGS_LOCK:
        cfg = GROUP_SETTINGS.setdefault(key, {})
        for k, v in defaults.items():
            cfg.setdefault(k, v)
        return cfg


def get_score_map(chat_id: int) -> dict[str, Any]:
    key = str(chat_id)
    with SETTINGS_LOCK:
        SCORES.setdefault(key, {})
        return SCORES[key]


def get_expense_state(chat_id: int) -> dict[str, Any]:
    key = str(chat_id)
    with SETTINGS_LOCK:
        state = EXPENSES.setdefault(key, {})
        state.setdefault("entries", [])
        state.setdefault("participants", {})
        return state


def get_archive_map(chat_id: int) -> dict[str, Any]:
    key = str(chat_id)
    with SETTINGS_LOCK:
        ARCHIVE.setdefault(key, {})
        return ARCHIVE[key]


def get_reco_config(chat_id: int) -> dict[str, Any]:
    key = str(chat_id)
    with SETTINGS_LOCK:
        cfg = RECO_SETTINGS.setdefault(key, {})
        cfg.setdefault("enabled", False)
        cfg.setdefault("hour", 21)
        cfg.setdefault("minute", 0)
        cfg.setdefault("mode", "movie")
        cfg.setdefault("send_movie", True)
        cfg.setdefault("send_series", False)
        cfg.setdefault("send_book", False)
        cfg.setdefault("last_sent_date", "")
        return cfg


def get_global_config() -> dict[str, Any]:
    with SETTINGS_LOCK:
        GLOBAL_SETTINGS.setdefault("gpt_enabled", False)
        GLOBAL_SETTINGS.setdefault("gpt_daily_limit", 5)
        GLOBAL_SETTINGS.setdefault("gpt_day", "")
        GLOBAL_SETTINGS.setdefault("gpt_daily_count", 0)
        return GLOBAL_SETTINGS


def _new_reminder_draft_id() -> str:
    global REMINDER_DRAFT_COUNTER
    with SETTINGS_LOCK:
        REMINDER_DRAFT_COUNTER += 1
        return f"r{int(time.time())}{REMINDER_DRAFT_COUNTER}"


def _extract_message_text(msg) -> str:
    if msg is None:
        return ""
    return (getattr(msg, "text", None) or getattr(msg, "caption", None) or "").strip()


def _next_day_due_ts(hour: int, minute: int) -> int:
    now = now_tehran()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=1)
    return int(target.timestamp())


def _reminder_panel_text(draft: dict[str, Any]) -> str:
    return (
        "⏰ تنظیم یادآوری فردا\n"
        f"• ساعت انتخابی: {int(draft.get('hour', 9)):02d}:{int(draft.get('minute', 0)):02d}\n"
        f"• مخاطب: {draft.get('target_name', 'کاربر')}\n\n"
        "با دکمه‌ها ساعت را تنظیم کن و تایید بزن."
    )


def _reminder_panel_markup(draft_id: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("⏪ ساعت -1", callback_data=f"rm:h:-1:{draft_id}"),
        types.InlineKeyboardButton("⏩ ساعت +1", callback_data=f"rm:h:+1:{draft_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("⏪ دقیقه -15", callback_data=f"rm:m:-15:{draft_id}"),
        types.InlineKeyboardButton("⏩ دقیقه +15", callback_data=f"rm:m:+15:{draft_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("✅ ثبت یادآوری", callback_data=f"rm:confirm:0:{draft_id}"),
        types.InlineKeyboardButton("❌ لغو", callback_data=f"rm:cancel:0:{draft_id}"),
    )
    return kb


def maybe_start_reminder_flow(message) -> bool:
    if not is_group_chat(message):
        return False
    txt = normalize_fa_text(message.text or "")
    if not ("فردا" in txt and "یادم بنداز" in txt):
        return False

    target_user = message.from_user
    target_name = target_user.first_name or target_user.username or str(target_user.id)
    source = _extract_message_text(getattr(message, "reply_to_message", None)) or "یادآوری طبق درخواست شما"
    draft_id = _new_reminder_draft_id()
    REMINDER_DRAFTS[draft_id] = {
        "chat_id": int(message.chat.id),
        "creator_user_id": int(message.from_user.id),
        "target_user_id": int(target_user.id),
        "target_name": target_name,
        "source_text": source,
        "hour": 9,
        "minute": 0,
        "created_at": int(time.time()),
    }
    bot.reply_to(
        message,
        _reminder_panel_text(REMINDER_DRAFTS[draft_id]),
        reply_markup=_reminder_panel_markup(draft_id),
    )
    return True


def maybe_send_due_reminders() -> None:
    now_ts = int(time.time())
    due_items = db_get_due_reminders(now_ts, limit=30)
    for item in due_items:
        reminder_id, chat_id, target_user_id, target_name, reminder_text, due_ts = item
        safe_name = html.escape(str(target_name or "دوست"))
        safe_text = html.escape(str(reminder_text or "یادآوری"))
        due_local = datetime.fromtimestamp(int(due_ts), TEHRAN_TZ).strftime("%H:%M")
        text = (
            f"⏰ <a href=\"tg://user?id={int(target_user_id)}\">{safe_name}</a>\n"
            f"یادآوری ساعت {due_local}\n"
            f"{safe_text}"
        )
        try:
            bot.send_message(int(chat_id), text, parse_mode="HTML")
            db_mark_reminder_sent(int(reminder_id))
        except Exception as exc:
            print(f"Reminder send error {reminder_id}: {exc}")


def now_tehran() -> datetime:
    return datetime.now(TEHRAN_TZ)


def today_key_tehran() -> str:
    return now_tehran().strftime("%Y-%m-%d")


def should_trigger_positive(text: str) -> bool:
    t = normalize_fa_text(text)
    return any(k in t for k in POSITIVE_REPLY_KEYWORDS)


def should_trigger_negative(text: str) -> bool:
    t = normalize_fa_text(text)
    return any(k in t for k in NEGATIVE_REPLY_KEYWORDS)


def update_user_score(chat_id: int, user, delta: int) -> dict[str, Any]:
    users = get_score_map(chat_id)
    uid = str(user.id)
    row = users.setdefault(uid, {"name": "", "score": 0, "positive": 0, "negative": 0})
    row["name"] = user.first_name or user.username or uid
    row["score"] = int(row.get("score", 0)) + int(delta)
    if delta > 0:
        row["positive"] = int(row.get("positive", 0)) + delta
    elif delta < 0:
        row["negative"] = int(row.get("negative", 0)) + abs(delta)
    save_scores()
    return row


def maybe_handle_reply_scoring(message) -> bool:
    if not is_group_chat(message):
        return False
    reply = getattr(message, "reply_to_message", None)
    if not reply or not getattr(reply, "from_user", None):
        return False

    text = message.text or ""
    target = reply.from_user
    if should_trigger_negative(text):
        row = update_user_score(message.chat.id, target, delta=-1)
        bot.reply_to(
            message,
            f"🚨 کسشر شناسایی شد.\nامتیاز {row['name']}: {row['score']} (منفی: {row['negative']})",
        )
        return True
    if should_trigger_positive(text):
        row = update_user_score(message.chat.id, target, delta=1)
        bot.reply_to(
            message,
            f"🌟 جمله طلایی ثبت شد.\nامتیاز {row['name']}: {row['score']} (مثبت: {row['positive']})",
        )
        return True
    return False


def daily_pick(items: list[tuple[str, str]]) -> tuple[str, str]:
    day = now_tehran().toordinal()
    idx = day % len(items)
    return items[idx]


def reco_enabled_kinds(cfg: dict[str, Any]) -> list[str]:
    kinds: list[str] = []
    if cfg.get("send_movie", False):
        kinds.append("movie")
    if cfg.get("send_series", False):
        kinds.append("series")
    if cfg.get("send_book", False):
        kinds.append("book")
    if not kinds:
        kinds = ["movie"]
    return kinds


def build_daily_recommendation_text(mode: str = "movie", cfg: dict[str, Any] | None = None) -> str:
    if cfg is not None:
        parts = ["🎬 پیشنهاد روزانه"]
        for kind in reco_enabled_kinds(cfg):
            if kind == "series":
                parts.append("\n📺 سریال\n" + ai_recommend("series", daily=True))
            elif kind == "book":
                parts.append("\n📚 کتاب\n" + ai_recommend("book", daily=True))
            else:
                parts.append("\n🎬 فیلم\n" + ai_recommend("movie", daily=True))
        return "\n\n".join(parts)

    normalized_mode = (mode or "movie").strip().lower()
    if normalized_mode == "series":
        return "📺 پیشنهاد روزانه سریال\n\n" + ai_recommend("series", daily=True)
    if normalized_mode == "book":
        return "📚 پیشنهاد روزانه کتاب\n\n" + ai_recommend("book", daily=True)
    return "🎬 پیشنهاد روزانه فیلم\n\n" + ai_recommend("movie", daily=True)


def reco_settings_text(chat_id: int) -> str:
    cfg = get_reco_config(chat_id)
    status = "روشن" if cfg.get("enabled", False) else "خاموش"
    enabled_map = {
        "فیلم": cfg.get("send_movie", False),
        "سریال": cfg.get("send_series", False),
        "کتاب": cfg.get("send_book", False),
    }
    enabled_types = "، ".join([k for k, v in enabled_map.items() if v]) or "هیچکدام"
    hour = int(cfg.get("hour", 21))
    minute = int(cfg.get("minute", 0))
    return (
        "🎬 پنل پیشنهاد روزانه\n"
        f"• وضعیت: {status}\n"
        f"• نوع‌های فعال: {enabled_types}\n"
        f"• زمان ارسال: {hour:02d}:{minute:02d}\n"
        "• تنظیمات با دکمه‌های زیر انجام می‌شود."
    )


def reco_settings_markup(chat_id: int):
    cfg = get_reco_config(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('enabled', False))} فعال/غیرفعال",
            callback_data="rc:toggle_enabled",
        ),
        types.InlineKeyboardButton("🚀 ارسال تستی", callback_data="rc:send_now"),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('send_movie', False))} فیلم",
            callback_data="rc:toggle_movie",
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('send_series', False))} سریال",
            callback_data="rc:toggle_series",
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('send_book', False))} کتاب",
            callback_data="rc:toggle_book",
        ),
    )
    kb.add(
        types.InlineKeyboardButton("⏪ 30 دقیقه", callback_data="rc:time_minus_30"),
        types.InlineKeyboardButton("⏩ 30 دقیقه", callback_data="rc:time_plus_30"),
    )
    kb.add(types.InlineKeyboardButton("🔄 تازه سازی", callback_data="rc:show"))
    return kb


def maybe_send_daily_recommendations() -> None:
    now = now_tehran()
    today = now.strftime("%Y-%m-%d")
    with SETTINGS_LOCK:
        items = list(RECO_SETTINGS.items())
    for chat_id_str, cfg in items:
        if not cfg.get("enabled", False):
            continue
        hour = int(cfg.get("hour", 21))
        minute = int(cfg.get("minute", 0))
        if cfg.get("last_sent_date") == today:
            continue
        if now.hour < hour or (now.hour == hour and now.minute < minute):
            continue
        chat_id = int(chat_id_str)
        try:
            bot.send_message(chat_id, build_daily_recommendation_text(cfg=cfg))
            with SETTINGS_LOCK:
                if chat_id_str in RECO_SETTINGS:
                    RECO_SETTINGS[chat_id_str]["last_sent_date"] = today
            save_reco_settings()
        except Exception as exc:
            print(f"Daily recommendation error for {chat_id}: {exc}")


def call_recommendation_model(prompt: str, max_output_tokens: int = 300) -> str:
    if not OPENAI_API_KEY:
        return ""
    url = f"{OPENAI_API_BASE}/responses"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "input": prompt,
        "max_output_tokens": max_output_tokens,
    }
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=25)
        res.raise_for_status()
        data = res.json()
        text = data.get("output_text")
        if isinstance(text, str) and text.strip():
            return text.strip()
    except Exception:
        return ""
    return ""


def fallback_recommendation(kind: str) -> str:
    if kind == "series":
        title, summary = daily_pick(DAILY_SERIES)
        genre = SERIES_GENRES.get(title, "درام")
        return f"📺 پیشنهاد سریال\nنام: {title}\nژانر: {genre}\n\nمعرفی:\n{summary}"
    if kind == "book":
        title, summary = daily_pick(DAILY_BOOKS)
        category = BOOK_CATEGORIES.get(title, "عمومی")
        return f"📚 پیشنهاد کتاب\nنام: {title}\nدسته‌بندی: {category}\n\nمعرفی:\n{summary}"
    title, summary = daily_pick(DAILY_MOVIES)
    genre = MOVIE_GENRES.get(title, "درام")
    return f"🎬 پیشنهاد فیلم\nنام: {title}\nژانر: {genre}\n\nمعرفی:\n{summary}"


def ai_recommend(kind: str, profile_text: str = "", daily: bool = False) -> str:
    normalized_kind = (kind or "movie").strip().lower()
    if normalized_kind not in ("movie", "series", "book"):
        normalized_kind = "movie"
    daily_key = today_key_tehran() if daily else "adhoc"
    cache_raw = f"{normalized_kind}|{profile_text.strip().lower()}|{daily_key}"
    key = _cache_key(cache_raw)
    cached = db_get_recommendation_cache(key)
    if cached:
        return cached

    default_profile = random.choice(DEFAULT_RECO_RANDOM_INPUTS)
    effective_profile = profile_text.strip() or default_profile
    kind_fa = "فیلم" if normalized_kind == "movie" else ("سریال" if normalized_kind == "series" else "کتاب")
    taxonomy_label = "دسته‌بندی" if normalized_kind == "book" else "ژانر"
    prompt = RECO_PROMPT_TEMPLATE.format(
        kind_fa=kind_fa,
        profile=effective_profile,
        taxonomy_label=taxonomy_label,
    )

    text = call_recommendation_model(prompt, max_output_tokens=380)
    if not text:
        text = fallback_recommendation(normalized_kind)
    db_set_recommendation_cache(key, text, OPENAI_MODEL)
    return text


PERSONAL_RECO_QUESTIONS = [
    ("kind", "چه نوع پیشنهادی می‌خوای؟ (فیلم / سریال / کتاب)"),
    ("genre", "ژانر یا دسته‌بندی مورد علاقه‌ات چیه؟ (مثلا درام، معمایی، خودسازی، تاریخی)"),
    ("mood", "حال و هوای دلخواهت چیه؟ (مثلا سبک، هیجانی، عمیق، تاریک)"),
    ("limits", "محدودیت خاصی داری؟ (خشونت کم، مدت کوتاه، زبان خاص، سال تولید...)"),
]


def _normalize_kind(text: str) -> str:
    t = normalize_fa_text(text)
    if any(k in t for k in ("کتاب", "book")):
        return "book"
    if any(k in t for k in ("سریال", "series", "show")):
        return "series"
    return "movie"


def start_personal_reco_session(user_id: int) -> None:
    USER_RECO_SESSIONS[str(user_id)] = {"idx": 0, "answers": {}, "started_at": int(time.time())}


def get_personal_reco_question(user_id: int) -> str | None:
    sess = USER_RECO_SESSIONS.get(str(user_id))
    if not sess:
        return None
    idx = int(sess.get("idx", 0))
    if idx >= len(PERSONAL_RECO_QUESTIONS):
        return None
    return PERSONAL_RECO_QUESTIONS[idx][1]


def consume_personal_reco_answer(user_id: int, text: str) -> tuple[bool, str]:
    key = str(user_id)
    sess = USER_RECO_SESSIONS.get(key)
    if not sess:
        return False, ""
    idx = int(sess.get("idx", 0))
    if idx >= len(PERSONAL_RECO_QUESTIONS):
        return False, ""
    field = PERSONAL_RECO_QUESTIONS[idx][0]
    sess["answers"][field] = (text or "").strip()
    sess["idx"] = idx + 1
    USER_RECO_SESSIONS[key] = sess

    next_q = get_personal_reco_question(user_id)
    if next_q:
        return True, next_q

    answers = sess.get("answers", {})
    kind = _normalize_kind(str(answers.get("kind", "")))
    profile = (
        f"نوع: {answers.get('kind', '')}\n"
        f"ژانر: {answers.get('genre', '')}\n"
        f"حال‌وهوا: {answers.get('mood', '')}\n"
        f"محدودیت‌ها: {answers.get('limits', '')}"
    )
    result = ai_recommend(kind, profile_text=profile, daily=False)
    USER_RECO_SESSIONS.pop(key, None)
    kind_title = {"movie": "فیلم", "series": "سریال", "book": "کتاب"}.get(kind, "فیلم")
    return True, f"🎯 پیشنهاد شخصی ({kind_title})\n\n{result}"


def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


def has_group_management_access(chat_id: int, user_id: int) -> bool:
    if is_owner(user_id):
        return True
    return is_admin(chat_id, user_id)


def require_group_admin(message) -> bool:
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور فقط داخل گروه قابل استفاده است.")
        return False
    if not has_group_management_access(message.chat.id, message.from_user.id):
        bot.reply_to(message, "فقط ادمین گروه یا ادمین بات می تواند این تنظیم را تغییر دهد.")
        return False
    return True


def _menu_keyboard(is_group: bool = False):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(types.KeyboardButton("📈 بازار (ارز/سکه)"), types.KeyboardButton("🎭 امتیاز و شوخی"))
    kb.add(types.KeyboardButton("💰 خرج و دنگ"), types.KeyboardButton("📂 آرشیو"))
    kb.add(types.KeyboardButton("🎯 پیشنهاد شخصی"), types.KeyboardButton("🎬 پیشنهاد روزانه"))
    kb.add(types.KeyboardButton("⚙️ راهنما"))
    if is_group:
        kb.add(types.KeyboardButton("⚙️ تنظیمات گروه"), types.KeyboardButton("🚀 ارسال فوری"))
    return kb


def expense_panel_text(chat_id: int) -> str:
    state = get_expense_state(chat_id)
    entries = state.get("entries", [])
    total = sum(float(x.get("amount", 0)) for x in entries)
    participants = state.get("participants", {})
    return (
        "💰 پنل دنگ و خرج گروه\n"
        f"• تعداد خرج‌ها: {len(entries)}\n"
        f"• مجموع خرج: {total:g}\n"
        f"• اعضای تسویه: {len(participants)}\n\n"
        "ثبت خرج همچنان با دستور سریع است:\n"
        "/add 480 پیتزا"
    )


def expense_panel_markup(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("➕ عضویت در تسویه", callback_data=f"ex:join:{chat_id}"),
        types.InlineKeyboardButton("🧾 نمایش خرج‌ها", callback_data=f"ex:show:{chat_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("💳 محاسبه دنگ", callback_data=f"ex:split:{chat_id}"),
        types.InlineKeyboardButton("🗑 پاکسازی خرج‌ها", callback_data=f"ex:clear:{chat_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("🔄 تازه سازی", callback_data=f"ex:refresh:{chat_id}"),
        types.InlineKeyboardButton("❌ بستن پنل", callback_data=f"ex:close:{chat_id}"),
    )
    return kb


def _state_dot(enabled: bool) -> str:
    return "🟢" if enabled else "⚫"


def _group_settings_markup(cfg: dict[str, Any], chat_id: int, page: str = "main"):
    reco = get_reco_config(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    if page == "reco":
        kb.add(
            types.InlineKeyboardButton(
                f"{_state_dot(reco.get('enabled', False))} فعال/غیرفعال", callback_data="gs:r:toggle_enabled"
            ),
            types.InlineKeyboardButton("🚀 ارسال تستی", callback_data="gs:r:send_now"),
        )
        kb.add(
            types.InlineKeyboardButton(
                f"{_state_dot(reco.get('send_movie', False))} فیلم", callback_data="gs:r:toggle_movie"
            ),
            types.InlineKeyboardButton(
                f"{_state_dot(reco.get('send_series', False))} سریال",
                callback_data="gs:r:toggle_series",
            ),
        )
        kb.add(
            types.InlineKeyboardButton(
                f"{_state_dot(reco.get('send_book', False))} کتاب",
                callback_data="gs:r:toggle_book",
            ),
        )
        kb.add(
            types.InlineKeyboardButton("⏪ 30 دقیقه", callback_data="gs:r:time_minus_30"),
            types.InlineKeyboardButton("⏩ 30 دقیقه", callback_data="gs:r:time_plus_30"),
        )
        kb.add(
            types.InlineKeyboardButton("⬅️ بازگشت به پنل اصلی", callback_data="gs:page:main"),
            types.InlineKeyboardButton("❌ بستن پنل", callback_data="gs:close"),
        )
        return kb

    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('enabled', True))} ربات", callback_data="gs:t:enabled"
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('auto_enabled', False))} خودکار", callback_data="gs:t:auto"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('include_crypto', False))} کریپتو",
            callback_data="gs:t:crypto",
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('pin_report', False))} پین گزارش", callback_data="gs:t:pin"
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('mention_enabled', True))} منشن پاسخ",
            callback_data="gs:t:mention",
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('silent', False))} بی‌صدا", callback_data="gs:t:silent"
        ),
    )
    kb.add(
        types.InlineKeyboardButton("⏪ بازه -5", callback_data="gs:n:interval_minus"),
        types.InlineKeyboardButton("⏩ بازه +5", callback_data="gs:n:interval_plus"),
    )
    kb.add(
        types.InlineKeyboardButton("⏪ کول‌داون -5", callback_data="gs:n:cooldown_minus"),
        types.InlineKeyboardButton("⏩ کول‌داون +5", callback_data="gs:n:cooldown_plus"),
    )
    kb.add(
        types.InlineKeyboardButton("🚀 ارسال فوری", callback_data="gs:send_now"),
        types.InlineKeyboardButton("🎬 پیشنهاد روزانه", callback_data="gs:page:reco"),
    )
    kb.add(
        types.InlineKeyboardButton("♻️ بازنشانی تنظیمات", callback_data="gs:reset"),
        types.InlineKeyboardButton("❌ بستن پنل", callback_data="gs:close"),
    )
    kb.add(
        types.InlineKeyboardButton("🧹 ریست امتیازها", callback_data="gs:reset_scores"),
        types.InlineKeyboardButton("🧾 ریست خرج/دنگ", callback_data="gs:reset_expenses"),
    )
    kb.add(types.InlineKeyboardButton("📂 ریست آرشیو", callback_data="gs:reset_archive"))
    return kb


def _percent_to_float(percent: str) -> float:
    try:
        return float((percent or "0").replace("%", "").replace("+", "").strip())
    except Exception:
        return 0.0


def _format_item_line(item: dict[str, Any], show_percent: bool = True) -> str:
    icon = ITEM_ICON.get(item.get("id", ""), "•")
    name = item.get("name", "آیتم")
    price = item.get("price", "نامشخص")
    return f"{icon} {name}: `{price}`"


def _format_section(title: str, items: list[dict[str, Any]], show_percent: bool = True) -> list[str]:
    if not items:
        return []
    lines = [title]
    for item in items:
        lines.append(_format_item_line(item, show_percent=show_percent))
    lines.append("")
    return lines


def _find_item_by_id(data: dict[str, Any], target_id: str) -> dict[str, Any] | None:
    for section in ("currencies", "coins", "gold", "crypto"):
        for item in data.get(section, []):
            if item.get("id") == target_id:
                return item
    return None


def _market_clean(value: str) -> str:
    return (value or "").replace("\u200c", "").strip()


def _market_parse_change(change_text: str) -> tuple[str, str, str]:
    text = _market_clean(change_text)
    if not text:
        return "0", "0%", "flat"
    percent_match = re.search(r"([+-]?\d+(?:\.\d+)?)%", text)
    amount_match = re.search(r"([+-]?[\d,]+)(?!.*[\d,])", text)
    change_percent = f"{percent_match.group(1)}%" if percent_match else "0%"
    change_amount = amount_match.group(1) if amount_match else "0"
    try:
        p = float(change_percent.replace("%", ""))
    except ValueError:
        p = 0.0
    direction = "up" if p > 0 else ("down" if p < 0 else "flat")
    if p > 0 and not change_percent.startswith("+"):
        change_percent = f"+{change_percent}"
    if change_amount not in ("0", ""):
        try:
            a = float(change_amount.replace(",", ""))
            if a > 0 and not change_amount.startswith("+"):
                change_amount = f"+{change_amount}"
        except ValueError:
            pass
    return change_amount, change_percent, direction


def _market_extract_item(row, code: str, name: str) -> dict[str, Any]:
    tds = row.find_all("td")
    price = _market_clean(row.get("data-price") or (tds[0].get_text(" ", strip=True) if tds else ""))
    change_text = tds[1].get_text(" ", strip=True) if len(tds) > 1 else ""
    change_amount, change_percent, direction = _market_parse_change(change_text)
    return {
        "id": code,
        "name": name,
        "price": price or "نامشخص",
        "change_amount": change_amount,
        "change_percent": change_percent,
        "direction": direction,
    }


def _scrape_prices_local() -> dict[str, Any]:
    response = requests.get(
        PRICE_SOURCE_URL,
        headers=PRICE_REQUEST_HEADERS,
        timeout=PRICE_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    rows = {row.get("data-market-row"): row for row in soup.find_all("tr", {"data-market-row": True})}

    def collect(mapped_codes: dict[str, str]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for code, name in mapped_codes.items():
            row = rows.get(code)
            if row is not None:
                items.append(_market_extract_item(row, code, name))
        return items

    currencies = collect(MARKET_CURRENCY_MAP)
    coins = collect(MARKET_COIN_MAP)
    gold = collect(MARKET_GOLD_MAP)
    crypto = collect(MARKET_CRYPTO_MAP)
    return {
        "meta": {
            "source": PRICE_SOURCE_URL,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        "currencies": currencies,
        "coins": coins,
        "gold": gold,
        "crypto": crypto,
    }


def build_quick_keyword_message(text: str | None, require_price_word: bool = False) -> str | None:
    q = compact_text(text)
    if not q:
        return None

    has_price_word = any(k in q for k in ("قیمت", "price", "نرخ"))
    if require_price_word and not has_price_word:
        return None

    intent = None
    if any(k in q for k in ("قیمت دلار", "دلار", "usd", "دالر", "price dollar")):
        intent = "dollar"
    elif any(k in q for k in ("قیمت بیت کوین", "بیت کوین", "بیتکوین", "bitcoin", "btc")):
        intent = "bitcoin"
    elif any(k in q for k in ("قیمت سکه", "سکه", "coin")):
        intent = "coins"
    elif any(k in q for k in ("قیمت طلا", "طلا", "gold")):
        intent = "gold"
    elif any(k in q for k in ("قیمت تتر", "تتر", "usdt", "tether")):
        intent = "tether"

    if intent is None:
        return None

    data = fetch_prices_data()
    lines: list[str] = []

    if intent == "dollar":
        item = _find_item_by_id(data, "price_dollar_rl")
        if not item:
            return "قیمت دلار الان در دسترس نیست."
        lines = ["💵 قیمت دلار", _format_item_line(item)]
    elif intent == "bitcoin":
        item = _find_item_by_id(data, "crypto-bitcoin")
        if not item:
            return "قیمت بیت کوین الان در دسترس نیست."
        lines = ["₿ قیمت بیت کوین", _format_item_line(item)]
    elif intent == "tether":
        item = _find_item_by_id(data, "crypto-tether-irr")
        if not item:
            return "قیمت تتر الان در دسترس نیست."
        lines = ["💠 قیمت تتر", _format_item_line(item)]
    elif intent == "coins":
        lines = ["🪙 قیمت سکه"]
        for item in data.get("coins", []):
            lines.append(_format_item_line(item))
    elif intent == "gold":
        lines = ["🥇 قیمت طلا"]
        for item in data.get("gold", []):
            lines.append(_format_item_line(item))

    if not lines:
        return None

    updated_at = data.get("meta", {}).get("updated_at", "")
    if updated_at:
        lines.append("")
        lines.append(f"⏱ `{updated_at}`")

    return "\n".join(lines)


def fetch_prices_data() -> dict[str, Any]:
    try:
        data = _scrape_prices_local()
        if not any((data.get("currencies"), data.get("coins"), data.get("gold"), data.get("crypto"))):
            raise RuntimeError("هیچ داده‌ای استخراج نشد")
        return data
    except Exception as exc:
        raise RuntimeError(f"خطا در دریافت قیمت از منبع مستقیم: {exc}") from exc


def get_prices_message(
    title: str = "📊 قیمت های لحظه ای",
    include_sections: tuple[str, ...] = ("currencies", "coins", "gold", "crypto"),
    show_percent: bool = True,
    include_crypto: bool = True,
) -> str:
    data = fetch_prices_data()

    lines = [title, ""]
    section_labels = {
        "currencies": "💱 ارزهای پرکاربرد",
        "coins": "🪙 قیمت سکه",
        "gold": "🥇 طلا",
        "crypto": "🧬 کریپتو",
    }

    for section in include_sections:
        if section == "crypto" and not include_crypto:
            continue
        items = data.get(section, [])
        lines.extend(_format_section(section_labels[section], items, show_percent=show_percent))

    updated_at = data.get("meta", {}).get("updated_at", "")
    if updated_at:
        lines.append(f"⏱ بروزرسانی: `{updated_at}`")
    else:
        lines.append("⏱ بروزرسانی: نامشخص")

    return "\n".join(lines).strip()


def get_top_changes_message(limit: int = 5) -> str:
    data = fetch_prices_data()
    all_items: list[dict[str, Any]] = []
    for key in ("currencies", "coins", "gold", "crypto"):
        all_items.extend(data.get(key, []))

    if not all_items:
        return "داده ای برای محاسبه بیشترین تغییر موجود نیست."

    all_items.sort(key=lambda x: abs(_percent_to_float(x.get("change_percent", "0%"))), reverse=True)
    top_items = all_items[: max(1, min(limit, 10))]

    lines = ["📈 بیشترین تغییرات روزانه", ""]
    for item in top_items:
        lines.append(_format_item_line(item, show_percent=True))
    return "\n".join(lines)


def send_prices_to_chat(
    chat_id: int,
    title: str = "📊 قیمت های لحظه ای",
    silent: bool = False,
    show_percent: bool = True,
    include_crypto: bool = True,
    include_sections: tuple[str, ...] = ("currencies", "coins", "gold", "crypto"),
):
    text = get_prices_message(
        title=title,
        include_sections=include_sections,
        show_percent=show_percent,
        include_crypto=include_crypto,
    )
    return bot.send_message(chat_id, text, parse_mode="Markdown", disable_notification=silent)


def maybe_pin_report(chat_id: int, message_id: int, cfg: dict[str, Any]) -> None:
    if not cfg.get("pin_report", False):
        return
    try:
        bot.pin_chat_message(chat_id, message_id, disable_notification=True)
    except Exception:
        pass


def help_text() -> str:
    return (
        "🤖 راهنمای ربات\n"
        "• خصوصی: /prices یا /menu\n"
        "• گروه: /gprices یا تریگر سفارشی (پیش فرض !prices)\n"
        "• نمایش بیشترین تغییر: /top_changes\n"
        "• پنل تعاملی گروه: /group_menu\n\n"
        "🎭 امتیاز شوخی گروه:\n"
        "• روی پیام کسی ریپلای کن و بنویس: «کسشر شناسایی شد» یا «جمله طلایی»\n"
        "• /scoreboard | /my_score\n\n"
        "💰 خرج و دنگ:\n"
        "/add 480 پیتزا\n"
        "/join_split\n"
        "/expenses\n"
        "/split\n"
        "/clear_expenses\n\n"
        "📂 آرشیو گروه:\n"
        "/save کلید | مقدار\n"
        "/get کلید\n"
        "/list_saved\n\n"
        "🎯 پیشنهاد شخصی با سوال‌وجواب:\n"
        "/recommend_me\n\n"
        "🎬 پیشنهاد روزانه:\n"
        "• از دکمه «🎬 پیشنهاد روزانه» پنل کامل را باز کن (بدون کامند)\n"
        "• نوع ارسال: فیلم/سریال/کتاب (هرکدام جدا روشن/خاموش)\n"
        "/reco_on | /reco_off\n"
        "/set_reco_time 21:30\n"
        "/send_reco_now\n\n"
        "⚙️ تنظیمات گروه (فقط ادمین):\n"
        "/group_settings\n"
        "/group_on | /group_off\n"
        "/set_group_cmd !yourcmd\n"
        "/auto_on | /auto_off\n"
        "/set_interval 15\n"
        "/set_title متن دلخواه\n"
        "/set_silent on|off\n"
        "/set_cooldown 20\n"
        "/toggle_crypto\n"
        "/toggle_pin\n"
        "/toggle_mention\n"
        "/send_now\n"
        "/group_reset"
    )


def group_settings_text(cfg: dict[str, Any], chat_id: int | None = None) -> str:
    enabled = "فعال" if cfg.get("enabled", True) else "غیرفعال"
    auto_enabled = "فعال" if cfg.get("auto_enabled", False) else "غیرفعال"
    silent = "روشن" if cfg.get("silent", False) else "خاموش"
    include_crypto = "روشن" if cfg.get("include_crypto", False) else "خاموش"
    pin_report = "روشن" if cfg.get("pin_report", False) else "خاموش"
    mention_enabled = "روشن" if cfg.get("mention_enabled", True) else "خاموش"
    reco_cfg = get_reco_config(chat_id) if chat_id is not None else None
    reco_status = "نامشخص"
    reco_time = "--:--"
    if reco_cfg:
        reco_status = "روشن" if reco_cfg.get("enabled", False) else "خاموش"
        reco_time = f"{int(reco_cfg.get('hour', 21)):02d}:{int(reco_cfg.get('minute', 0)):02d}"
    reco_types = []
    if (reco_cfg or {}).get("send_movie", False):
        reco_types.append("فیلم")
    if (reco_cfg or {}).get("send_series", False):
        reco_types.append("سریال")
    if (reco_cfg or {}).get("send_book", False):
        reco_types.append("کتاب")
    reco_mode = "، ".join(reco_types) if reco_types else "هیچکدام"

    return (
        "╭─────────────── ✦\n"
        "│  ✨ پنل شیشه ای تنظیمات گروه\n"
        "├───────────────\n"
        f"│  {_state_dot(cfg.get('enabled', True))} وضعیت ربات: {enabled}\n"
        f"│  {_state_dot(cfg.get('auto_enabled', False))} ارسال خودکار: {auto_enabled}\n"
        f"│  {_state_dot(cfg.get('include_crypto', False))} نمایش کریپتو: {include_crypto}\n"
        f"│  {_state_dot(cfg.get('pin_report', False))} پین خودکار: {pin_report}\n"
        f"│  {_state_dot(cfg.get('mention_enabled', True))} پاسخ منشن: {mention_enabled}\n"
        f"│  🔕 بی صدا: {silent}\n"
        f"│  ⏳ بازه: هر {cfg.get('interval_min', 30)} دقیقه\n"
        f"│  🧊 کول‌داون: {cfg.get('cooldown_sec', 20)} ثانیه\n"
        f"│  🎯 تریگر: {cfg.get('trigger', '!prices')}\n"
        f"│  🏷 عنوان: {cfg.get('title', '📊 گزارش قیمت')}\n"
        f"│  🎬 پیشنهاد روزانه: {reco_status} ({reco_time} | {reco_mode})\n"
        "╰─────────────── ✦"
    )


def scheduler_loop() -> None:
    while True:
        try:
            maybe_send_due_reminders()
            maybe_send_daily_recommendations()
            now = int(time.time())
            with SETTINGS_LOCK:
                items = list(GROUP_SETTINGS.items())

            for chat_id_str, cfg in items:
                if not cfg.get("enabled", True) or not cfg.get("auto_enabled", False):
                    continue

                interval_min = max(1, int(cfg.get("interval_min", 30)))
                last_sent_ts = int(cfg.get("last_sent_ts", 0))
                if now - last_sent_ts < interval_min * 60:
                    continue

                chat_id = int(chat_id_str)
                msg = send_prices_to_chat(
                    chat_id,
                    title=cfg.get("title", "📊 گزارش قیمت"),
                    silent=bool(cfg.get("silent", False)),
                    show_percent=bool(cfg.get("show_percent", True)),
                    include_crypto=bool(cfg.get("include_crypto", False)),
                )
                maybe_pin_report(chat_id, msg.message_id, cfg)

                with SETTINGS_LOCK:
                    if chat_id_str in GROUP_SETTINGS:
                        GROUP_SETTINGS[chat_id_str]["last_sent_ts"] = now
                save_group_settings(GROUP_SETTINGS)

        except Exception as e:
            print(f"Scheduler error: {e}")

        time.sleep(max(5, SCHEDULER_SLEEP_SEC))


def _command_head(message) -> str:
    return (message.text or "").split(maxsplit=1)[0]


def _toggle_setting(message, key: str, on_label: str, off_label: str) -> None:
    cfg = get_group_config(message.chat.id)
    cfg[key] = not bool(cfg.get(key, False))
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, on_label if cfg[key] else off_label)


@bot.message_handler(commands=["start"])
def start(message):
    track_message_context(message, action="start")
    if is_group_chat(message):
        cfg = get_group_config(message.chat.id)
        bot.send_message(
            message.chat.id,
            group_settings_text(cfg, chat_id=message.chat.id) + "\n\nبرای تغییر سریع از دکمه های زیر استفاده کن.",
            reply_markup=_group_settings_markup(cfg, chat_id=message.chat.id, page="main"),
        )
        return
    bot.send_message(message.chat.id, help_text(), reply_markup=_menu_keyboard(is_group=False))


@bot.message_handler(commands=["help", "menu"])
def help_cmd(message):
    track_message_context(message, action="help_menu")
    if is_group_chat(message):
        bot.send_message(message.chat.id, help_text(), reply_markup=types.ReplyKeyboardRemove())
    else:
        bot.send_message(message.chat.id, help_text(), reply_markup=_menu_keyboard(is_group=False))


@bot.message_handler(commands=["prices"])
def prices(message):
    if not is_for_this_bot(_command_head(message)):
        return
    try:
        if is_group_chat(message):
            cfg = get_group_config(message.chat.id)
            if not cfg.get("enabled", True):
                return
            msg = send_prices_to_chat(
                message.chat.id,
                title=cfg.get("title", "📊 گزارش قیمت"),
                silent=cfg.get("silent", False),
                show_percent=cfg.get("show_percent", True),
                include_crypto=cfg.get("include_crypto", False),
            )
            maybe_pin_report(message.chat.id, msg.message_id, cfg)
        else:
            send_prices_to_chat(message.chat.id)
    except Exception as e:
        bot.reply_to(message, f"خطا در دریافت قیمت: {e}")


@bot.message_handler(commands=["gprices"])
def group_prices(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور فقط داخل گروه کار می کند.")
        return
    cfg = get_group_config(message.chat.id)
    if not cfg.get("enabled", True):
        return
    try:
        msg = send_prices_to_chat(
            message.chat.id,
            title=cfg.get("title", "📊 گزارش قیمت"),
            silent=cfg.get("silent", False),
            show_percent=cfg.get("show_percent", True),
            include_crypto=cfg.get("include_crypto", False),
        )
        maybe_pin_report(message.chat.id, msg.message_id, cfg)
    except Exception as e:
        bot.reply_to(message, f"خطا در ارسال گزارش: {e}")


@bot.message_handler(commands=["top_changes"])
def top_changes(message):
    if not is_for_this_bot(_command_head(message)):
        return
    try:
        bot.send_message(message.chat.id, get_top_changes_message(), parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"خطا در محاسبه تغییرات: {e}")


@bot.message_handler(commands=["group_menu"])
def group_menu(message):
    track_message_context(message, action="group_menu")
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    bot.send_message(
        message.chat.id,
        group_settings_text(cfg, chat_id=message.chat.id),
        reply_markup=_group_settings_markup(cfg, chat_id=message.chat.id, page="main"),
    )


@bot.callback_query_handler(func=lambda call: (call.data or "").startswith("gs:"))
def group_settings_callbacks(call):
    if call.message is None or call.message.chat is None:
        return
    chat_id = call.message.chat.id
    if not has_group_management_access(chat_id, call.from_user.id):
        bot.answer_callback_query(call.id, "فقط ادمین گروه یا ادمین بات", show_alert=True)
        return
    role = "owner" if is_owner(call.from_user.id) else "admin"
    db_touch_user(call.from_user, chat_id=chat_id, role=role)
    db_log_action(chat_id, int(call.from_user.id), "group_panel_callback", call.data or "")

    cfg = get_group_config(chat_id)
    reco = get_reco_config(chat_id)
    payload = (call.data or "").split(":")
    page = "main"

    if len(payload) >= 2 and payload[1] == "close":
        try:
            bot.edit_message_text(
                "پنل تنظیمات بسته شد. برای باز کردن دوباره: /group_menu",
                chat_id=chat_id,
                message_id=call.message.message_id,
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if len(payload) >= 3 and payload[1] == "page":
        page = "reco" if payload[2] == "reco" else "main"
    elif len(payload) >= 3 and payload[1] == "t":
        key = payload[2]
        if key == "enabled":
            cfg["enabled"] = not cfg.get("enabled", True)
        elif key == "auto":
            cfg["auto_enabled"] = not cfg.get("auto_enabled", False)
            if cfg["auto_enabled"]:
                cfg["last_sent_ts"] = 0
        elif key == "crypto":
            cfg["include_crypto"] = not cfg.get("include_crypto", False)
        elif key == "pin":
            cfg["pin_report"] = not cfg.get("pin_report", False)
        elif key == "mention":
            cfg["mention_enabled"] = not cfg.get("mention_enabled", True)
        elif key == "silent":
            cfg["silent"] = not cfg.get("silent", False)
        save_group_settings(GROUP_SETTINGS)
    elif len(payload) >= 3 and payload[1] == "n":
        key = payload[2]
        if key == "interval_minus":
            cfg["interval_min"] = max(1, int(cfg.get("interval_min", 30)) - 5)
        elif key == "interval_plus":
            cfg["interval_min"] = min(1440, int(cfg.get("interval_min", 30)) + 5)
        elif key == "cooldown_minus":
            cfg["cooldown_sec"] = max(0, int(cfg.get("cooldown_sec", 20)) - 5)
        elif key == "cooldown_plus":
            cfg["cooldown_sec"] = min(3600, int(cfg.get("cooldown_sec", 20)) + 5)
        save_group_settings(GROUP_SETTINGS)
    elif len(payload) >= 3 and payload[1] == "r":
        page = "reco"
        key = payload[2]
        if key == "toggle_enabled":
            reco["enabled"] = not reco.get("enabled", False)
        elif key == "mode_movie":
            reco["mode"] = "movie"
            reco["send_movie"] = True
        elif key == "mode_series":
            reco["mode"] = "series"
            reco["send_series"] = True
        elif key == "mode_book":
            reco["mode"] = "book"
            reco["send_book"] = True
        elif key == "toggle_movie":
            reco["send_movie"] = not reco.get("send_movie", False)
        elif key == "toggle_series":
            reco["send_series"] = not reco.get("send_series", False)
        elif key == "toggle_book":
            reco["send_book"] = not reco.get("send_book", False)
        elif key in ("time_minus_30", "time_plus_30"):
            cur_minutes = int(reco.get("hour", 21)) * 60 + int(reco.get("minute", 0))
            cur_minutes = (cur_minutes + (-30 if key == "time_minus_30" else 30)) % (24 * 60)
            reco["hour"] = cur_minutes // 60
            reco["minute"] = cur_minutes % 60
        elif key == "send_now":
            try:
                bot.send_message(chat_id, build_daily_recommendation_text(cfg=reco))
                reco["last_sent_date"] = today_key_tehran()
            except Exception as exc:
                bot.answer_callback_query(call.id, f"خطا: {exc}", show_alert=True)
        save_reco_settings()
    elif len(payload) >= 2 and payload[1] == "send_now":
        try:
            msg = send_prices_to_chat(
                chat_id,
                title=cfg.get("title", "📊 گزارش قیمت"),
                silent=cfg.get("silent", False),
                show_percent=cfg.get("show_percent", True),
                include_crypto=cfg.get("include_crypto", False),
            )
            maybe_pin_report(chat_id, msg.message_id, cfg)
            cfg["last_sent_ts"] = int(time.time())
            save_group_settings(GROUP_SETTINGS)
        except Exception as exc:
            bot.answer_callback_query(call.id, f"خطا: {exc}", show_alert=True)
    elif len(payload) >= 2 and payload[1] == "reset":
        with SETTINGS_LOCK:
            GROUP_SETTINGS[str(chat_id)] = _default_group_config()
        save_group_settings(GROUP_SETTINGS)
    elif len(payload) >= 2 and payload[1] == "reset_scores":
        with SETTINGS_LOCK:
            SCORES[str(chat_id)] = {}
        save_scores()
    elif len(payload) >= 2 and payload[1] == "reset_expenses":
        with SETTINGS_LOCK:
            EXPENSES[str(chat_id)] = {"entries": [], "participants": {}}
        save_expenses()
    elif len(payload) >= 2 and payload[1] == "reset_archive":
        with SETTINGS_LOCK:
            ARCHIVE[str(chat_id)] = {}
        save_archive()

    try:
        if page == "reco":
            text = reco_settings_text(chat_id)
        else:
            text = group_settings_text(cfg, chat_id=chat_id)
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=_group_settings_markup(cfg, chat_id=chat_id, page=page),
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id)


@bot.message_handler(commands=["group_settings"])
def group_settings(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not is_group_chat(message):
        bot.reply_to(message, "تنظیمات گروه فقط داخل گروه قابل استفاده است.")
        return
    cfg = get_group_config(message.chat.id)
    bot.reply_to(message, group_settings_text(cfg, chat_id=message.chat.id))


@bot.callback_query_handler(func=lambda call: (call.data or "").startswith("rc:"))
def reco_settings_callbacks(call):
    if call.message is None or call.message.chat is None:
        return
    chat_id = call.message.chat.id
    if not has_group_management_access(chat_id, call.from_user.id):
        bot.answer_callback_query(call.id, "فقط ادمین گروه یا ادمین بات", show_alert=True)
        return
    role = "owner" if is_owner(call.from_user.id) else "admin"
    db_touch_user(call.from_user, chat_id=chat_id, role=role)
    db_log_action(chat_id, int(call.from_user.id), "reco_panel_callback", call.data or "")

    cfg = get_reco_config(chat_id)
    action = (call.data or "").split(":", 1)[1]

    if action == "toggle_enabled":
        cfg["enabled"] = not cfg.get("enabled", False)
    elif action == "mode_movie":
        cfg["mode"] = "movie"
        cfg["send_movie"] = True
    elif action == "mode_series":
        cfg["mode"] = "series"
        cfg["send_series"] = True
    elif action == "mode_book":
        cfg["mode"] = "book"
        cfg["send_book"] = True
    elif action == "toggle_movie":
        cfg["send_movie"] = not cfg.get("send_movie", False)
    elif action == "toggle_series":
        cfg["send_series"] = not cfg.get("send_series", False)
    elif action == "toggle_book":
        cfg["send_book"] = not cfg.get("send_book", False)
    elif action in ("time_minus_30", "time_plus_30"):
        cur_minutes = int(cfg.get("hour", 21)) * 60 + int(cfg.get("minute", 0))
        delta = -30 if action == "time_minus_30" else 30
        cur_minutes = (cur_minutes + delta) % (24 * 60)
        cfg["hour"] = cur_minutes // 60
        cfg["minute"] = cur_minutes % 60
    elif action == "send_now":
        try:
            bot.send_message(chat_id, build_daily_recommendation_text(cfg=cfg))
            cfg["last_sent_date"] = today_key_tehran()
            save_reco_settings()
            bot.answer_callback_query(call.id, "ارسال شد")
        except Exception as exc:
            bot.answer_callback_query(call.id, f"خطا: {exc}", show_alert=True)
        return
    elif action == "show":
        pass

    save_reco_settings()
    try:
        bot.edit_message_text(
            reco_settings_text(chat_id),
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=reco_settings_markup(chat_id),
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id, "ذخیره شد")


@bot.message_handler(commands=["group_on"])
def group_on(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    cfg["enabled"] = True
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, "ارسال قیمت در گروه فعال شد.")


@bot.message_handler(commands=["group_off"])
def group_off(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    cfg["enabled"] = False
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, "ارسال قیمت در گروه غیرفعال شد.")


@bot.message_handler(commands=["set_group_cmd"])
def set_group_cmd(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت درست: /set_group_cmd !prices")
        return

    trigger = parts[1].strip()
    if not trigger:
        bot.reply_to(message, "دستور سفارشی خالی نباشد.")
        return
    if len(trigger) > 32:
        bot.reply_to(message, "دستور سفارشی خیلی طولانی است (حداکثر 32 کاراکتر).")
        return

    cfg = get_group_config(message.chat.id)
    cfg["trigger"] = trigger
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"دستور سفارشی گروه روی {trigger} تنظیم شد.")


@bot.message_handler(commands=["auto_on"])
def auto_on(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    cfg["auto_enabled"] = True
    cfg["last_sent_ts"] = 0
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"ارسال خودکار فعال شد. هر {cfg.get('interval_min', 30)} دقیقه ارسال می شود.")


@bot.message_handler(commands=["auto_off"])
def auto_off(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    cfg["auto_enabled"] = False
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, "ارسال خودکار غیرفعال شد.")


@bot.message_handler(commands=["set_interval"])
def set_interval(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت درست: /set_interval 15")
        return

    try:
        interval = int(parts[1].strip())
    except ValueError:
        bot.reply_to(message, "عدد معتبر وارد کن. مثال: /set_interval 15")
        return

    if interval < 1 or interval > 1440:
        bot.reply_to(message, "بازه باید بین 1 تا 1440 دقیقه باشد.")
        return

    cfg = get_group_config(message.chat.id)
    cfg["interval_min"] = interval
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"بازه ارسال خودکار روی هر {interval} دقیقه تنظیم شد.")


@bot.message_handler(commands=["set_title"])
def set_title(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(message, "فرمت درست: /set_title گزارش بازار ارز")
        return

    title = parts[1].strip()
    if len(title) > 80:
        bot.reply_to(message, "عنوان خیلی طولانی است (حداکثر 80 کاراکتر).")
        return

    cfg = get_group_config(message.chat.id)
    cfg["title"] = title
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"عنوان پیام روی \"{title}\" تنظیم شد.")


@bot.message_handler(commands=["set_silent"])
def set_silent(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت درست: /set_silent on یا /set_silent off")
        return

    val = normalize_text(parts[1])
    if val in ("on", "true", "1", "yes"):
        silent = True
    elif val in ("off", "false", "0", "no"):
        silent = False
    else:
        bot.reply_to(message, "فقط on یا off قابل قبول است.")
        return

    cfg = get_group_config(message.chat.id)
    cfg["silent"] = silent
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"ارسال بی صدا {'روشن' if silent else 'خاموش'} شد.")


@bot.message_handler(commands=["set_cooldown"])
def set_cooldown(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت درست: /set_cooldown 20")
        return

    try:
        seconds = int(parts[1].strip())
    except ValueError:
        bot.reply_to(message, "عدد معتبر وارد کن. مثال: /set_cooldown 20")
        return

    if seconds < 0 or seconds > 3600:
        bot.reply_to(message, "مقدار باید بین 0 تا 3600 ثانیه باشد.")
        return

    cfg = get_group_config(message.chat.id)
    cfg["cooldown_sec"] = seconds
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"کول‌داون تریگر روی {seconds} ثانیه تنظیم شد.")


@bot.message_handler(commands=["toggle_crypto"])
def toggle_crypto(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    _toggle_setting(message, "include_crypto", "نمایش کریپتو روشن شد.", "نمایش کریپتو خاموش شد.")


@bot.message_handler(commands=["toggle_pin"])
def toggle_pin(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    _toggle_setting(message, "pin_report", "پین خودکار روشن شد.", "پین خودکار خاموش شد.")


@bot.message_handler(commands=["toggle_mention"])
def toggle_mention(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return
    _toggle_setting(message, "mention_enabled", "پاسخ به منشن روشن شد.", "پاسخ به منشن خاموش شد.")


@bot.message_handler(commands=["send_now"])
def send_now(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    cfg = get_group_config(message.chat.id)
    try:
        msg = send_prices_to_chat(
            message.chat.id,
            title=cfg.get("title", "📊 گزارش قیمت"),
            silent=cfg.get("silent", False),
            show_percent=cfg.get("show_percent", True),
            include_crypto=cfg.get("include_crypto", False),
        )
        maybe_pin_report(message.chat.id, msg.message_id, cfg)
        cfg["last_sent_ts"] = int(time.time())
        save_group_settings(GROUP_SETTINGS)
    except Exception as e:
        bot.reply_to(message, f"خطا در ارسال: {e}")


@bot.message_handler(commands=["group_reset"])
def group_reset(message):
    if not is_for_this_bot(_command_head(message)):
        return
    if not require_group_admin(message):
        return

    key = str(message.chat.id)
    with SETTINGS_LOCK:
        if key in GROUP_SETTINGS:
            del GROUP_SETTINGS[key]
    cfg = get_group_config(message.chat.id)
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(
        message,
        f"تنظیمات گروه به حالت پیش فرض برگشت. بازه: {cfg['interval_min']} دقیقه، تریگر: {cfg['trigger']}",
    )


@bot.message_handler(commands=["scoreboard"])
def scoreboard(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور برای گروه است.")
        return
    users = get_score_map(message.chat.id)
    if not users:
        bot.reply_to(message, "هنوز امتیازی ثبت نشده.")
        return
    ranking = sorted(users.values(), key=lambda x: int(x.get("score", 0)), reverse=True)[:10]
    lines = ["🏆 جدول امتیاز گروه"]
    for i, row in enumerate(ranking, start=1):
        lines.append(f"{i}. {row.get('name', 'کاربر')}: {row.get('score', 0)}")
    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["my_score"])
def my_score(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور برای گروه است.")
        return
    users = get_score_map(message.chat.id)
    row = users.get(str(message.from_user.id), {"score": 0, "positive": 0, "negative": 0})
    bot.reply_to(
        message,
        f"امتیاز شما: {row.get('score', 0)}\nمثبت: {row.get('positive', 0)}\nمنفی: {row.get('negative', 0)}",
    )


@bot.message_handler(commands=["add"])
def add_expense(message):
    if not is_group_chat(message):
        bot.reply_to(message, "خرج گروهی فقط در گروه ثبت می‌شود.")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /add 480 پیتزا")
        return
    try:
        amount = float(parts[1].replace(",", ""))
    except ValueError:
        bot.reply_to(message, "مبلغ معتبر نیست. مثال: /add 480 پیتزا")
        return
    desc = parts[2].strip() if len(parts) > 2 else "بدون توضیح"
    state = get_expense_state(message.chat.id)
    uid = str(message.from_user.id)
    name = message.from_user.first_name or message.from_user.username or uid
    state["entries"].append(
        {
            "by_id": uid,
            "by_name": name,
            "amount": amount,
            "desc": desc,
            "ts": int(time.time()),
        }
    )
    state["participants"].setdefault(uid, name)
    save_expenses()
    bot.reply_to(message, f"ثبت شد: {amount:g} - {desc}")


@bot.message_handler(commands=["join_split"])
def join_split(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    uid = str(message.from_user.id)
    state["participants"][uid] = message.from_user.first_name or message.from_user.username or uid
    save_expenses()
    bot.reply_to(message, "شما به لیست تسویه اضافه شدید.")


@bot.message_handler(commands=["expenses"])
def expenses_summary(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    entries = state.get("entries", [])
    if not entries:
        bot.reply_to(message, "هنوز خرجی ثبت نشده.")
        return
    total = sum(float(x.get("amount", 0)) for x in entries)
    by_user: dict[str, float] = {}
    by_name: dict[str, str] = {}
    for item in entries:
        uid = str(item.get("by_id"))
        by_user[uid] = by_user.get(uid, 0.0) + float(item.get("amount", 0))
        by_name[uid] = str(item.get("by_name", uid))
    lines = [f"💰 مجموع خرج: {total:g}", "", "پرداخت هر نفر:"]
    for uid, value in sorted(by_user.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"• {by_name.get(uid, uid)}: {value:g}")
    bot.reply_to(message, "\n".join(lines))


def _compute_split_settlements(state: dict[str, Any]) -> list[str]:
    entries = state.get("entries", [])
    if not entries:
        return []
    paid: dict[str, float] = {}
    names: dict[str, str] = {}
    for item in entries:
        uid = str(item.get("by_id"))
        paid[uid] = paid.get(uid, 0.0) + float(item.get("amount", 0))
        names[uid] = str(item.get("by_name", uid))
    participants: dict[str, str] = state.get("participants", {}) or {}
    if not participants:
        participants = {uid: names.get(uid, uid) for uid in paid.keys()}
    for uid, nm in participants.items():
        names.setdefault(uid, nm)
        paid.setdefault(uid, 0.0)

    total = sum(paid.values())
    n = max(1, len(participants))
    share = total / n

    debtors: list[tuple[str, float]] = []
    creditors: list[tuple[str, float]] = []
    for uid in participants.keys():
        bal = paid.get(uid, 0.0) - share
        if bal < -0.01:
            debtors.append((uid, -bal))
        elif bal > 0.01:
            creditors.append((uid, bal))

    settlements: list[str] = [f"💳 سهم هر نفر: {share:g}"]
    i, j = 0, 0
    while i < len(debtors) and j < len(creditors):
        d_uid, d_amt = debtors[i]
        c_uid, c_amt = creditors[j]
        pay = min(d_amt, c_amt)
        settlements.append(f"• {names.get(d_uid, d_uid)} ➜ {names.get(c_uid, c_uid)} : {pay:g}")
        d_amt -= pay
        c_amt -= pay
        debtors[i] = (d_uid, d_amt)
        creditors[j] = (c_uid, c_amt)
        if d_amt <= 0.01:
            i += 1
        if c_amt <= 0.01:
            j += 1
    return settlements


@bot.message_handler(commands=["split"])
def split_expense(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    settlements = _compute_split_settlements(state)
    if not settlements:
        bot.reply_to(message, "برای تسویه ابتدا خرج ثبت کنید: /add مبلغ توضیح")
        return
    bot.reply_to(message, "\n".join(["🧾 نتیجه تسویه"] + settlements))


@bot.message_handler(commands=["clear_expenses"])
def clear_expenses(message):
    if not require_group_admin(message):
        return
    EXPENSES[str(message.chat.id)] = {"entries": [], "participants": {}}
    save_expenses()
    bot.reply_to(message, "لیست خرج و تسویه پاک شد.")


@bot.message_handler(commands=["expense_panel"])
def expense_panel(message):
    if not is_group_chat(message):
        bot.reply_to(message, "پنل دنگ فقط داخل گروه است.")
        return
    bot.send_message(
        message.chat.id,
        expense_panel_text(message.chat.id),
        reply_markup=expense_panel_markup(message.chat.id),
    )


@bot.callback_query_handler(func=lambda call: (call.data or "").startswith("ex:"))
def expense_panel_callbacks(call):
    if call.message is None or call.message.chat is None:
        return
    parts = (call.data or "").split(":")
    if len(parts) < 3:
        return
    action = parts[1]
    chat_id = int(parts[2])
    if call.message.chat.id != chat_id:
        bot.answer_callback_query(call.id, "پنل مربوط به گروه دیگری است.", show_alert=True)
        return

    if action == "join":
        state = get_expense_state(chat_id)
        uid = str(call.from_user.id)
        state["participants"][uid] = call.from_user.first_name or call.from_user.username or uid
        save_expenses()
    elif action == "show":
        state = get_expense_state(chat_id)
        entries = state.get("entries", [])
        if entries:
            total = sum(float(x.get("amount", 0)) for x in entries)
            lines = [f"💰 مجموع خرج: {total:g}"]
            for item in entries[-10:]:
                lines.append(f"• {item.get('by_name', 'کاربر')}: {float(item.get('amount', 0)):g} - {item.get('desc', '')}")
            try:
                bot.answer_callback_query(call.id, "نمایش ۱۰ مورد آخر")
                bot.send_message(chat_id, "\n".join(lines))
            except Exception:
                pass
        else:
            bot.answer_callback_query(call.id, "هنوز خرجی ثبت نشده.")
    elif action == "split":
        state = get_expense_state(chat_id)
        settlements = _compute_split_settlements(state)
        if settlements:
            bot.send_message(chat_id, "\n".join(["🧾 نتیجه تسویه"] + settlements))
        else:
            bot.answer_callback_query(call.id, "خرجی برای تسویه نیست.")
    elif action == "clear":
        if not has_group_management_access(chat_id, call.from_user.id):
            bot.answer_callback_query(call.id, "فقط ادمین گروه یا ادمین بات", show_alert=True)
            return
        EXPENSES[str(chat_id)] = {"entries": [], "participants": {}}
        save_expenses()
    elif action == "close":
        try:
            bot.edit_message_text("پنل دنگ بسته شد.", chat_id=chat_id, message_id=call.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return
    elif action == "refresh":
        pass

    try:
        bot.edit_message_text(
            expense_panel_text(chat_id),
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=expense_panel_markup(chat_id),
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: (call.data or "").startswith("rm:"))
def reminder_callbacks(call):
    if call.message is None or call.message.chat is None:
        return
    parts = (call.data or "").split(":")
    if len(parts) < 4:
        return
    action = parts[1]
    value = parts[2]
    draft_id = parts[3]
    draft = REMINDER_DRAFTS.get(draft_id)
    if not draft:
        bot.answer_callback_query(call.id, "این پنل منقضی شده.", show_alert=True)
        return
    if int(call.from_user.id) != int(draft.get("creator_user_id", 0)):
        bot.answer_callback_query(call.id, "فقط سازنده یادآوری می‌تواند تغییر دهد.", show_alert=True)
        return

    if action == "cancel":
        REMINDER_DRAFTS.pop(draft_id, None)
        try:
            bot.edit_message_text("یادآوری لغو شد.", chat_id=call.message.chat.id, message_id=call.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if action == "h":
        draft["hour"] = (int(draft.get("hour", 9)) + int(value)) % 24
    elif action == "m":
        total = int(draft.get("hour", 9)) * 60 + int(draft.get("minute", 0)) + int(value)
        total %= (24 * 60)
        draft["hour"] = total // 60
        draft["minute"] = total % 60
    elif action == "confirm":
        due_ts = _next_day_due_ts(int(draft.get("hour", 9)), int(draft.get("minute", 0)))
        db_add_reminder(
            chat_id=int(draft["chat_id"]),
            requester_user_id=int(draft["creator_user_id"]),
            target_user_id=int(draft["target_user_id"]),
            target_name=str(draft.get("target_name", "")),
            reminder_text=str(draft.get("source_text", "یادآوری")),
            due_ts=due_ts,
        )
        REMINDER_DRAFTS.pop(draft_id, None)
        due_hm = datetime.fromtimestamp(due_ts, TEHRAN_TZ).strftime("%H:%M")
        try:
            bot.edit_message_text(
                f"✅ یادآوری ثبت شد برای فردا ساعت {due_hm}",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    REMINDER_DRAFTS[draft_id] = draft
    try:
        bot.edit_message_text(
            _reminder_panel_text(draft),
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=_reminder_panel_markup(draft_id),
        )
    except Exception:
        pass
    bot.answer_callback_query(call.id)


@bot.message_handler(commands=["save"])
def save_item(message):
    if not is_group_chat(message):
        bot.reply_to(message, "آرشیو برای گروه است.")
        return
    raw = (message.text or "").strip()
    body = raw.split(maxsplit=1)
    if len(body) < 2:
        bot.reply_to(message, "فرمت: /save کلید | مقدار")
        return
    payload = body[1].strip()
    key = ""
    value = ""
    if "|" in payload:
        key, value = [x.strip() for x in payload.split("|", 1)]
    else:
        tokens = payload.split()
        if len(tokens) < 2:
            bot.reply_to(message, "فرمت: /save کلید | مقدار")
            return
        if len(tokens) >= 3 and tokens[0] in ("کارت", "card", "شماره", "لینک", "آدرس", "لوکیشن"):
            key = f"{tokens[0]} {tokens[1]}"
            value = " ".join(tokens[2:])
        else:
            key = tokens[0]
            value = " ".join(tokens[1:])
    if not key or not value:
        bot.reply_to(message, "کلید یا مقدار خالی است.")
        return
    data = get_archive_map(message.chat.id)
    data[key] = value
    save_archive()
    bot.reply_to(message, f"📂 ذخیره شد: {key}")


@bot.message_handler(commands=["get"])
def get_item(message):
    if not is_group_chat(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /get کلید")
        return
    key = parts[1].strip()
    data = get_archive_map(message.chat.id)
    value = data.get(key)
    if value is None:
        bot.reply_to(message, "چیزی با این کلید پیدا نشد.")
        return
    bot.reply_to(message, f"📎 {key}\n{value}")


@bot.message_handler(commands=["list_saved"])
def list_saved(message):
    if not is_group_chat(message):
        return
    data = get_archive_map(message.chat.id)
    if not data:
        bot.reply_to(message, "آرشیو خالی است.")
        return
    keys = sorted(data.keys())
    bot.reply_to(message, "📂 کلیدهای ذخیره‌شده:\n" + "\n".join(f"• {k}" for k in keys[:50]))


@bot.message_handler(commands=["save_msg"])
def save_group_message_ref(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این قابلیت فقط داخل گروه است.")
        return
    extra = (message.text or "").split(maxsplit=1)
    note = extra[1].strip() if len(extra) > 1 else ""

    reply = getattr(message, "reply_to_message", None)
    reply_id = getattr(message, "reply_to_message_id", None)
    log_save_msg_debug(
        "incoming "
        f"chat_id={message.chat.id} msg_id={getattr(message, 'message_id', None)} "
        f"user_id={getattr(getattr(message, 'from_user', None), 'id', None)} "
        f"has_reply_obj={reply is not None} reply_to_message_id={reply_id} "
        f"text={repr((message.text or '')[:120])}"
    )
    message_id = None
    message_link = ""
    save_key = ""
    if reply is not None:
        message_id = int(reply.message_id)
        log_save_msg_debug(
            f"reply_obj message_id={message_id} "
            f"reply_text={repr((_extract_message_text(reply) or '')[:120])}"
        )
        save_key = f"msg_{message_id}"
        if str(message.chat.id).startswith("-100"):
            channel_id = str(message.chat.id)[4:]
            message_link = f"https://t.me/c/{channel_id}/{message_id}"
    elif reply_id is not None:
        # Some updates include only reply_to_message_id without full reply payload.
        message_id = int(reply_id)
        save_key = f"msg_{message_id}"
        if str(message.chat.id).startswith("-100"):
            channel_id = str(message.chat.id)[4:]
            message_link = f"https://t.me/c/{channel_id}/{message_id}"
        log_save_msg_debug(f"fallback reply_to_message_id used: {message_id}")
    else:
        text = (message.text or "")
        match = re.search(r"(https?://t\.me/\S+)", text)
        if match:
            message_link = match.group(1).strip()
            # Auto key from link tail; fallback to hash-like readable key.
            tail = message_link.rstrip("/").split("/")[-1]
            save_key = f"msg_{tail}" if tail.isdigit() else f"msg_{abs(hash(message_link)) % 1000000}"
            if not note:
                note = text.replace(match.group(1), "").replace("/save_msg", "").strip()
            log_save_msg_debug(f"link fallback used: {message_link}")
        else:
            log_save_msg_debug("failed: no reply object, no reply_to_message_id, no t.me link in text")
            bot.reply_to(message, "ریپلای تشخیص داده نشد. یکبار دیگر روی پیام ریپلای کن و فقط /save_msg بزن.")
            return

    db_save_group_message(
        chat_id=message.chat.id,
        save_key=save_key,
        message_id=message_id,
        message_link=message_link,
        author_user_id=message.from_user.id,
        note=note,
    )
    log_save_msg_debug(
        f"saved key={save_key} message_id={message_id} link={message_link or '-'} note={repr(note[:80])}"
    )
    out = f"✅ پیام ذخیره شد\n🔑 کلید: {save_key}"
    if message_link:
        out += f"\n🔗 {message_link}"
    if note:
        out += f"\n📝 {note}"
    bot.reply_to(message, out)


@bot.message_handler(commands=["get_msg"])
def get_group_message_ref(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این قابلیت فقط داخل گروه است.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /get_msg کلید")
        return
    save_key = parts[1].strip().lower()
    row = db_get_saved_group_message(message.chat.id, save_key)
    if not row:
        bot.reply_to(message, "چیزی با این کلید پیدا نشد.")
        return
    _, message_id, message_link, note, _ = row
    lines = [f"📌 {save_key}"]
    if note:
        lines.append(f"📝 {note}")
    if message_link:
        lines.append(f"🔗 {message_link}")
    elif message_id:
        lines.append(f"🧾 message_id: {message_id}")
    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["list_msgs"])
def list_group_message_refs(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این قابلیت فقط داخل گروه است.")
        return
    rows = db_list_saved_group_messages(message.chat.id, limit=50)
    if not rows:
        bot.reply_to(message, "هنوز پیام مهمی ذخیره نشده.")
        return
    lines = ["📌 لیست پیام‌های مهم:"]
    for save_key, message_link, note, _ in rows:
        line = f"• {save_key}"
        if note:
            line += f" - {note[:40]}"
        if message_link:
            line += f" | {message_link}"
        lines.append(line)
    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["del_msg"])
def delete_group_message_ref(message):
    if not require_group_admin(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /del_msg کلید")
        return
    save_key = parts[1].strip().lower()
    ok = db_delete_saved_group_message(message.chat.id, save_key)
    bot.reply_to(message, "🗑 حذف شد." if ok else "چیزی با این کلید پیدا نشد.")


@bot.message_handler(commands=["recommend_me"])
def recommend_me(message):
    if is_group_chat(message):
        bot.reply_to(message, "برای پیشنهاد شخصی لطفا در پی‌وی بات پیام بده.")
        return
    start_personal_reco_session(message.from_user.id)
    first_q = get_personal_reco_question(message.from_user.id) or "نوع محتوا را بگو: فیلم / سریال / کتاب"
    bot.reply_to(message, f"🎯 برای پیشنهاد شخصی، چند سوال کوتاه:\n\n{first_q}")


@bot.message_handler(commands=["ask"])
def ask_gpt(message):
    bot.reply_to(message, "بخش چت GPT حذف شده. برای پیشنهاد شخصی از /recommend_me استفاده کن.")


def is_owner(user_id: int) -> bool:
    return OWNER_USER_ID > 0 and int(user_id) == OWNER_USER_ID


def owner_groups() -> list[int]:
    ids: set[int] = set()
    for k in GROUP_SETTINGS.keys():
        try:
            ids.add(int(k))
        except Exception:
            pass
    for k in RECO_SETTINGS.keys():
        try:
            ids.add(int(k))
        except Exception:
            pass
    for k in EXPENSES.keys():
        try:
            ids.add(int(k))
        except Exception:
            pass
    return sorted(ids)


def owner_panel_text() -> str:
    groups = owner_groups()
    return (
        "🛡 سوپر پنل ادمین بات\n"
        f"• تعداد گروه‌ها: {len(groups)}\n"
        f"• فایل دیتابیس: {DB_PATH}\n"
        f"• مدل پیشنهاد: {OPENAI_MODEL}\n"
        "یک گروه را از دکمه‌ها انتخاب کن."
    )


def owner_panel_markup():
    kb = types.InlineKeyboardMarkup(row_width=1)
    groups = owner_groups()
    for gid in groups[:20]:
        kb.add(types.InlineKeyboardButton(f"👥 گروه {gid}", callback_data=f"ow:open:{gid}"))
    kb.add(
        types.InlineKeyboardButton("♻️ ریست کش پیشنهادها", callback_data="ow:cache_reset"),
        types.InlineKeyboardButton("🔄 تازه سازی", callback_data="ow:refresh"),
    )
    return kb


def owner_group_text(chat_id: int) -> str:
    cfg = get_group_config(chat_id)
    reco = get_reco_config(chat_id)
    reco_types = []
    if reco.get("send_movie", False):
        reco_types.append("فیلم")
    if reco.get("send_series", False):
        reco_types.append("سریال")
    if reco.get("send_book", False):
        reco_types.append("کتاب")
    return (
        f"🛠 مدیریت گروه {chat_id}\n"
        f"• ربات: {'فعال' if cfg.get('enabled', True) else 'غیرفعال'}\n"
        f"• خودکار: {'فعال' if cfg.get('auto_enabled', False) else 'غیرفعال'}\n"
        f"• بازه: {cfg.get('interval_min', 30)} دقیقه\n"
        f"• کریپتو: {'روشن' if cfg.get('include_crypto', False) else 'خاموش'}\n"
        f"• پیشنهاد روزانه: {'روشن' if reco.get('enabled', False) else 'خاموش'}\n"
        f"• زمان reco: {int(reco.get('hour', 21)):02d}:{int(reco.get('minute', 0)):02d}\n"
        f"• نوع‌های reco: {', '.join(reco_types) if reco_types else 'هیچکدام'}"
    )


def owner_group_markup(chat_id: int):
    cfg = get_group_config(chat_id)
    reco = get_reco_config(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('enabled', True))} ربات",
            callback_data=f"ow:t:{chat_id}:enabled",
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('auto_enabled', False))} خودکار",
            callback_data=f"ow:t:{chat_id}:auto",
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('include_crypto', False))} کریپتو",
            callback_data=f"ow:t:{chat_id}:crypto",
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(cfg.get('silent', False))} بی‌صدا",
            callback_data=f"ow:t:{chat_id}:silent",
        ),
    )
    kb.add(
        types.InlineKeyboardButton("⏪ بازه -5", callback_data=f"ow:n:{chat_id}:interval_minus"),
        types.InlineKeyboardButton("⏩ بازه +5", callback_data=f"ow:n:{chat_id}:interval_plus"),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(reco.get('enabled', False))} reco",
            callback_data=f"ow:r:{chat_id}:enabled",
        ),
        types.InlineKeyboardButton("ارسال reco", callback_data=f"ow:r:{chat_id}:send"),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(reco.get('send_movie', False))} فیلم",
            callback_data=f"ow:r:{chat_id}:toggle_movie",
        ),
        types.InlineKeyboardButton(
            f"{_state_dot(reco.get('send_series', False))} سریال",
            callback_data=f"ow:r:{chat_id}:toggle_series",
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"{_state_dot(reco.get('send_book', False))} کتاب",
            callback_data=f"ow:r:{chat_id}:toggle_book",
        ),
    )
    kb.add(
        types.InlineKeyboardButton("🧹 ریست امتیاز", callback_data=f"ow:reset:{chat_id}:scores"),
        types.InlineKeyboardButton("🧾 ریست خرج", callback_data=f"ow:reset:{chat_id}:expenses"),
    )
    kb.add(
        types.InlineKeyboardButton("📂 ریست آرشیو", callback_data=f"ow:reset:{chat_id}:archive"),
        types.InlineKeyboardButton("♻️ ریست تنظیمات", callback_data=f"ow:reset:{chat_id}:settings"),
    )
    kb.add(types.InlineKeyboardButton("⬅️ برگشت", callback_data="ow:back"))
    return kb


@bot.message_handler(commands=["owner_panel"])
def owner_panel(message):
    if not is_owner(message.from_user.id):
        bot.reply_to(message, "فقط صاحب بات دسترسی دارد.")
        return
    bot.send_message(
        message.chat.id,
        owner_panel_text(),
        reply_markup=owner_panel_markup(),
    )


@bot.message_handler(commands=["set_gpt"])
def set_gpt(message):
    bot.reply_to(message, "بخش چت GPT حذف شده. GPT فقط برای بخش پیشنهادها استفاده می‌شود.")


@bot.message_handler(commands=["set_gpt_limit"])
def set_gpt_limit(message):
    bot.reply_to(message, "این دستور غیرفعال شده.")


@bot.callback_query_handler(func=lambda call: (call.data or "").startswith("ow:"))
def owner_panel_callbacks(call):
    if call.message is None or call.message.chat is None:
        return
    if not is_owner(call.from_user.id):
        bot.answer_callback_query(call.id, "فقط صاحب بات", show_alert=True)
        return
    data = (call.data or "").split(":")
    action = data[1] if len(data) > 1 else ""

    if action in ("refresh", "back"):
        try:
            bot.edit_message_text(
                owner_panel_text(),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_panel_markup(),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if action == "cache_reset":
        with DB_LOCK:
            conn = sqlite3.connect(DB_PATH)
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM recommendation_cache")
                conn.commit()
            finally:
                conn.close()
        bot.answer_callback_query(call.id, "کش پیشنهادها پاک شد")
        try:
            bot.edit_message_text(
                owner_panel_text(),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_panel_markup(),
            )
        except Exception:
            pass
        return

    if action == "open" and len(data) >= 3:
        chat_id = int(data[2])
        try:
            bot.edit_message_text(
                owner_group_text(chat_id),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_group_markup(chat_id),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if action == "t" and len(data) >= 4:
        chat_id = int(data[2])
        key = data[3]
        cfg = get_group_config(chat_id)
        if key == "enabled":
            cfg["enabled"] = not cfg.get("enabled", True)
        elif key == "auto":
            cfg["auto_enabled"] = not cfg.get("auto_enabled", False)
            if cfg["auto_enabled"]:
                cfg["last_sent_ts"] = 0
        elif key == "crypto":
            cfg["include_crypto"] = not cfg.get("include_crypto", False)
        elif key == "silent":
            cfg["silent"] = not cfg.get("silent", False)
        save_group_settings(GROUP_SETTINGS)
        try:
            bot.edit_message_text(
                owner_group_text(chat_id),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_group_markup(chat_id),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if action == "n" and len(data) >= 4:
        chat_id = int(data[2])
        key = data[3]
        cfg = get_group_config(chat_id)
        if key == "interval_minus":
            cfg["interval_min"] = max(1, int(cfg.get("interval_min", 30)) - 5)
        elif key == "interval_plus":
            cfg["interval_min"] = min(1440, int(cfg.get("interval_min", 30)) + 5)
        save_group_settings(GROUP_SETTINGS)
        try:
            bot.edit_message_text(
                owner_group_text(chat_id),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_group_markup(chat_id),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if action == "r" and len(data) >= 4:
        chat_id = int(data[2])
        key = data[3]
        rcfg = get_reco_config(chat_id)
        if key == "enabled":
            rcfg["enabled"] = not rcfg.get("enabled", False)
            save_reco_settings()
        elif key == "toggle_movie":
            rcfg["send_movie"] = not rcfg.get("send_movie", False)
            save_reco_settings()
        elif key == "toggle_series":
            rcfg["send_series"] = not rcfg.get("send_series", False)
            save_reco_settings()
        elif key == "toggle_book":
            rcfg["send_book"] = not rcfg.get("send_book", False)
            save_reco_settings()
        elif key == "send":
            bot.send_message(chat_id, build_daily_recommendation_text(cfg=rcfg))
            rcfg["last_sent_date"] = today_key_tehran()
            save_reco_settings()
        try:
            bot.edit_message_text(
                owner_group_text(chat_id),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_group_markup(chat_id),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if action == "reset" and len(data) >= 4:
        chat_id = int(data[2])
        key = data[3]
        if key == "scores":
            SCORES[str(chat_id)] = {}
            save_scores()
        elif key == "expenses":
            EXPENSES[str(chat_id)] = {"entries": [], "participants": {}}
            save_expenses()
        elif key == "archive":
            ARCHIVE[str(chat_id)] = {}
            save_archive()
        elif key == "settings":
            GROUP_SETTINGS[str(chat_id)] = _default_group_config()
            save_group_settings(GROUP_SETTINGS)
        try:
            bot.edit_message_text(
                owner_group_text(chat_id),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_group_markup(chat_id),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id, "انجام شد")
        return


@bot.message_handler(commands=["reco_on"])
def reco_on(message):
    if not require_group_admin(message):
        return
    cfg = get_reco_config(message.chat.id)
    cfg["enabled"] = True
    save_reco_settings()
    bot.reply_to(message, "ارسال روزانه پیشنهاد روشن شد. (حالت: فیلم/سریال از پنل)")


@bot.message_handler(commands=["reco_off"])
def reco_off(message):
    if not require_group_admin(message):
        return
    cfg = get_reco_config(message.chat.id)
    cfg["enabled"] = False
    save_reco_settings()
    bot.reply_to(message, "ارسال روزانه پیشنهاد خاموش شد.")


@bot.message_handler(commands=["set_reco_time"])
def set_reco_time(message):
    if not require_group_admin(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /set_reco_time 21:30")
        return
    hm = parts[1].strip()
    if ":" not in hm:
        bot.reply_to(message, "فرمت زمان صحیح نیست.")
        return
    h_str, m_str = hm.split(":", 1)
    try:
        hour = int(h_str)
        minute = int(m_str)
    except ValueError:
        bot.reply_to(message, "عدد معتبر برای زمان وارد کن.")
        return
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        bot.reply_to(message, "زمان خارج از بازه است.")
        return
    cfg = get_reco_config(message.chat.id)
    cfg["hour"] = hour
    cfg["minute"] = minute
    save_reco_settings()
    bot.reply_to(message, f"زمان ارسال روزانه روی {hour:02d}:{minute:02d} تنظیم شد.")


@bot.message_handler(commands=["send_reco_now"])
def send_reco_now(message):
    if not require_group_admin(message):
        return
    rcfg = get_reco_config(message.chat.id)
    bot.send_message(
        message.chat.id,
        build_daily_recommendation_text(cfg=rcfg),
    )
    rcfg["last_sent_date"] = today_key_tehran()
    save_reco_settings()


@bot.message_handler(func=lambda m: is_group_trigger_message(m))
def group_custom_trigger(message):
    cfg = get_group_config(message.chat.id)
    trigger = normalize_text(cfg.get("trigger", "!prices"))
    text = normalize_text(message.text)
    mention_trigger = f"{trigger}@{BOT_USERNAME}"
    if text not in (trigger, mention_trigger):
        return

    cooldown_sec = max(0, int(cfg.get("cooldown_sec", 20)))
    chat_key = str(message.chat.id)
    now = int(time.time())
    last = TRIGGER_COOLDOWN_TS.get(chat_key, 0)
    if cooldown_sec and (now - last < cooldown_sec):
        return
    TRIGGER_COOLDOWN_TS[chat_key] = now

    try:
        msg = send_prices_to_chat(
            message.chat.id,
            title=cfg.get("title", "📊 گزارش قیمت"),
            silent=cfg.get("silent", False),
            show_percent=cfg.get("show_percent", True),
            include_crypto=cfg.get("include_crypto", False),
        )
        maybe_pin_report(message.chat.id, msg.message_id, cfg)
    except Exception as e:
        bot.reply_to(message, f"خطا: {e}")


@bot.message_handler(func=lambda m: is_group_chat(m) and normalize_text(m.text).startswith("@" + BOT_USERNAME))
def mention_reply(message):
    cfg = get_group_config(message.chat.id)
    if not cfg.get("mention_enabled", True):
        return

    text = normalize_text(message.text).split(maxsplit=1)
    if len(text) == 1:
        bot.reply_to(message, help_text())
        return

    command_or_trigger = text[1]
    trigger = normalize_text(cfg.get("trigger", "!prices"))
    if command_or_trigger in ("prices", "/prices", "gprices", "/gprices", trigger):
        if cfg.get("enabled", True):
            try:
                msg = send_prices_to_chat(
                    message.chat.id,
                    title=cfg.get("title", "📊 گزارش قیمت"),
                    silent=cfg.get("silent", False),
                    show_percent=cfg.get("show_percent", True),
                    include_crypto=cfg.get("include_crypto", False),
                )
                maybe_pin_report(message.chat.id, msg.message_id, cfg)
            except Exception as e:
                bot.reply_to(message, f"خطا: {e}")
    elif command_or_trigger in ("help", "/help", "settings", "/group_settings", "/group_menu"):
        bot.reply_to(message, help_text())
    else:
        try:
            quick_msg = build_quick_keyword_message(command_or_trigger)
            if quick_msg:
                bot.reply_to(message, quick_msg, parse_mode="Markdown")
        except Exception as e:
            bot.reply_to(message, f"خطا: {e}")


@bot.message_handler(func=lambda m: not is_command_message(m) and bool((m.text or "").strip()))
def quick_keyword_reply(message):
    track_message_context(message, action="text_message")
    if maybe_start_reminder_flow(message):
        return

    if not is_group_chat(message) and str(message.from_user.id) in USER_RECO_SESSIONS:
        handled, out = consume_personal_reco_answer(message.from_user.id, message.text or "")
        if handled:
            bot.reply_to(message, out)
            return

    menu_texts = {
        "📈 بازار (ارز/سکه)",
        "🎭 امتیاز و شوخی",
        "💰 خرج و دنگ",
        "📂 آرشیو",
        "🎯 پیشنهاد شخصی",
        "🎬 پیشنهاد روزانه",
        "⚙️ راهنما",
        "⚙️ تنظیمات گروه",
        "🚀 ارسال فوری",
        "📊 قیمت کامل",
        "💱 ارزهای مهم",
        "🪙 سکه ها",
        "📈 بیشترین تغییر",
    }
    if normalize_text(message.text) in menu_texts:
        # Route reply-keyboard taps directly to menu handler (important in private chat).
        menu_buttons(message)
        return

    if maybe_handle_reply_scoring(message):
        return

    # In groups, check all normal messages but only if they include price wording.
    if is_group_chat(message):
        if not any(k in compact_text(message.text) for k in ("قیمت", "price", "نرخ")):
            return

    try:
        quick_msg = build_quick_keyword_message(
            message.text,
            require_price_word=is_group_chat(message),
        )
        if quick_msg:
            bot.reply_to(message, quick_msg, parse_mode="Markdown")
        elif is_reply_to_this_bot(message):
            bot.reply_to(
                message,
                "برای دریافت سریع بنویس: دلار، طلا، سکه، بیت کوین یا تتر",
            )
    except Exception as e:
        bot.reply_to(message, f"خطا در پاسخ سریع: {e}")


@bot.message_handler(
    func=lambda m: normalize_text(m.text)
    in {
        "📈 بازار (ارز/سکه)",
        "🎭 امتیاز و شوخی",
        "💰 خرج و دنگ",
        "📂 آرشیو",
        "🎯 پیشنهاد شخصی",
        "🎬 پیشنهاد روزانه",
        "⚙️ راهنما",
        "⚙️ تنظیمات گروه",
        "🚀 ارسال فوری",
        "📊 قیمت کامل",
        "💱 ارزهای مهم",
        "🪙 سکه ها",
        "📈 بیشترین تغییر",
    }
)
def menu_buttons(message):
    track_message_context(message, action="menu_button")
    txt = normalize_text(message.text)

    if txt == "⚙️ راهنما":
        if is_group_chat(message):
            bot.send_message(message.chat.id, help_text(), reply_markup=types.ReplyKeyboardRemove())
        else:
            bot.send_message(message.chat.id, help_text(), reply_markup=_menu_keyboard(is_group=False))
        return

    if txt == "🎭 امتیاز و شوخی":
        if not is_group_chat(message):
            bot.reply_to(message, "این بخش مخصوص گروه است.\nدستورات: /scoreboard و /my_score")
            return
        bot.reply_to(
            message,
            "🎭 سیستم شوخی فعاله.\n"
            "روی پیام هر شخص ریپلای کن:\n"
            "• «کسشر شناسایی شد» => امتیاز منفی\n"
            "• «جمله طلایی» => امتیاز مثبت\n\n"
            "دستورات: /scoreboard | /my_score",
        )
        return

    if txt == "💰 خرج و دنگ":
        if not is_group_chat(message):
            bot.reply_to(message, "این بخش برای گروه است.")
            return
        bot.send_message(
            message.chat.id,
            expense_panel_text(message.chat.id),
            reply_markup=expense_panel_markup(message.chat.id),
        )
        return

    if txt == "📂 آرشیو":
        if not is_group_chat(message):
            bot.reply_to(message, "این بخش برای گروه است.")
            return
        bot.reply_to(
            message,
            "📂 آرشیو گروه\n"
            "/save کارت علی | 6037...\n"
            "/get کارت علی\n"
            "/list_saved\n\n"
            "📌 پیام مهم گروه\n"
            "/save_msg [توضیح اختیاری] (روی پیام ریپلای کن)\n"
            "/get_msg کلید\n"
            "/list_msgs\n"
            "/del_msg کلید",
        )
        return

    if txt == "🎯 پیشنهاد شخصی":
        if is_group_chat(message):
            bot.reply_to(message, "برای پیشنهاد شخصی لطفا در پی‌وی از /recommend_me استفاده کن.")
            return
        start_personal_reco_session(message.from_user.id)
        q = get_personal_reco_question(message.from_user.id) or "نوع محتوا؟ فیلم / سریال / کتاب"
        bot.reply_to(message, f"🎯 شروع پیشنهاد شخصی\n\n{q}")
        return

    if txt == "🎬 پیشنهاد روزانه":
        if is_group_chat(message):
            if not require_group_admin(message):
                return
            bot.send_message(
                message.chat.id,
                reco_settings_text(message.chat.id),
                reply_markup=reco_settings_markup(message.chat.id),
            )
        else:
            bot.send_message(message.chat.id, build_daily_recommendation_text(mode="movie"))
        return

    if txt == "📈 بیشترین تغییر":
        try:
            bot.send_message(message.chat.id, get_top_changes_message(), parse_mode="Markdown")
        except Exception as e:
            bot.reply_to(message, f"خطا: {e}")
        return

    if txt == "⚙️ تنظیمات گروه":
        if not require_group_admin(message):
            return
        cfg = get_group_config(message.chat.id)
        bot.send_message(
            message.chat.id,
            group_settings_text(cfg, chat_id=message.chat.id),
            reply_markup=_group_settings_markup(cfg, chat_id=message.chat.id, page="main"),
        )
        return

    if txt == "🚀 ارسال فوری":
        if not require_group_admin(message):
            return
        send_now(message)
        return

    try:
        cfg = get_group_config(message.chat.id) if is_group_chat(message) else _default_group_config()
        show_percent = cfg.get("show_percent", True)
        include_crypto = cfg.get("include_crypto", False) if is_group_chat(message) else True

        if txt in ("📊 قیمت کامل", "📈 بازار (ارز/سکه)"):
            sections = ("currencies", "coins", "gold", "crypto")
            title = "📈 بازار ارز و سکه"
        elif txt == "💱 ارزهای مهم":
            sections = ("currencies",)
            title = "💱 قیمت ارزهای مهم"
        elif txt == "🪙 سکه ها":
            sections = ("coins",)
            title = "🪙 قیمت سکه (تمام/نیم/ربع)"
        else:
            return

        msg = send_prices_to_chat(
            message.chat.id,
            title=title,
            silent=cfg.get("silent", False) if is_group_chat(message) else False,
            show_percent=show_percent,
            include_crypto=include_crypto,
            include_sections=sections,
        )
        if is_group_chat(message):
            maybe_pin_report(message.chat.id, msg.message_id, cfg)
    except Exception as e:
        bot.reply_to(message, f"خطا: {e}")


@app.route("/" + BOT_TOKEN, methods=["POST"])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode("utf-8"))
    bot.process_new_updates([update])
    return "OK", 200


if __name__ == "__main__":
    init_database()
    me = bot.get_me()
    print(f"Bot started: @{me.username}")
    print("If group commands do not work, disable privacy in BotFather: /setprivacy -> Disable")

    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()

    bot.remove_webhook()
    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
