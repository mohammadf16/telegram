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
from collections import Counter
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from math import exp, sqrt
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urlparse, urlsplit

from flask import Flask, request
import requests
import telebot
from bs4 import BeautifulSoup
from telebot import types
try:
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import PromptTemplate
    from langchain_openai import ChatOpenAI
except Exception:
    StrOutputParser = None
    PromptTemplate = None
    ChatOpenAI = None

app = Flask(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
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
LANGCHAIN_ENABLED = os.getenv("LANGCHAIN_ENABLED", "1").strip().lower() in ("1", "true", "on", "yes")
AI_CACHE_ENABLED = os.getenv("AI_CACHE_ENABLED", "1").strip().lower() in ("1", "true", "on", "yes")
AI_CACHE_TTL_SEC = max(60, int(os.getenv("AI_CACHE_TTL_SEC", "21600")))
AI_CACHE_MAX_ITEMS = max(50, int(os.getenv("AI_CACHE_MAX_ITEMS", "600")))
AI_PROMPT_MAX_CHARS = max(1000, int(os.getenv("AI_PROMPT_MAX_CHARS", "7000")))
DEBUG_SAVE_MSG = os.getenv("DEBUG_SAVE_MSG", "1").strip() in ("1", "true", "on", "yes")
AI_DEBUG = os.getenv("AI_DEBUG", "1").strip().lower() in ("1", "true", "on", "yes")
RUN_MODE = os.getenv(
    "RUN_MODE", "webhook" if os.getenv("RAILWAY_ENVIRONMENT") else "polling"
).strip().lower()
APP_HOST = os.getenv("APP_HOST", "0.0.0.0").strip()
APP_PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip().rstrip("/")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip()
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

NEWS_HTTP_TIMEOUT = max(3, int(os.getenv("NEWS_HTTP_TIMEOUT", "6")))
NEWS_REFRESH_INTERVAL_MIN = max(10, int(os.getenv("NEWS_REFRESH_INTERVAL_MIN", "60")))
NEWS_REFRESH_BUDGET_SEC = max(12, int(os.getenv("NEWS_REFRESH_BUDGET_SEC", "55")))
NEWS_REFRESH_MAX_FEEDS = max(25, int(os.getenv("NEWS_REFRESH_MAX_FEEDS", "85")))
FACTCHECK_QUERY_MAX = max(2, min(8, int(os.getenv("FACTCHECK_QUERY_MAX", "6"))))
FACTCHECK_MAX_EVIDENCE = max(4, min(12, int(os.getenv("FACTCHECK_MAX_EVIDENCE", "8"))))
FACTCHECK_RECENT_DAYS = max(3, int(os.getenv("FACTCHECK_RECENT_DAYS", "120")))
LIVE_SEARCH_ENABLED = os.getenv("LIVE_SEARCH_ENABLED", "1").strip().lower() in ("1", "true", "on", "yes")
LIVE_SEARCH_MAX_RESULTS = max(4, min(40, int(os.getenv("LIVE_SEARCH_MAX_RESULTS", "16"))))
LIVE_FETCH_MAX_ARTICLES = max(0, min(20, int(os.getenv("LIVE_FETCH_MAX_ARTICLES", "6"))))
FACTCHECK_AI_WEBSEARCH_ENABLED = os.getenv("FACTCHECK_AI_WEBSEARCH_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "on",
    "yes",
)
FACTCHECK_AI_WEBSEARCH_MAX_SOURCES = max(2, min(10, int(os.getenv("FACTCHECK_AI_WEBSEARCH_MAX_SOURCES", "6"))))
NEWS_INDEX_KEEP_DAYS = max(7, int(os.getenv("NEWS_INDEX_KEEP_DAYS", "30")))

NEWS_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}

NEWS_SOURCE_FEEDS_IR: list[dict[str, str]] = [
    {"name": "IRNA", "url": "https://www.irna.ir/rss", "region": "ir", "lang": "fa", "tier": "high"},
    {"name": "ISNA", "url": "https://www.isna.ir/rss", "region": "ir", "lang": "fa", "tier": "high"},
    {"name": "Mehr News", "url": "https://www.mehrnews.com/rss", "region": "ir", "lang": "fa", "tier": "medium"},
    {
        "name": "Tasnim",
        "url": "https://www.tasnimnews.com/fa/rss/feed/0/7/0/%D8%A2%D8%AE%D8%B1%DB%8C%D9%86-%D8%A7%D8%AE%D8%A8%D8%A7%D8%B1",
        "region": "ir",
        "lang": "fa",
        "tier": "medium",
    },
    {"name": "Fars News", "url": "https://www.farsnews.ir/rss", "region": "ir", "lang": "fa", "tier": "medium"},
    {"name": "ILNA", "url": "https://www.ilna.ir/fa/rss/allnews", "region": "ir", "lang": "fa", "tier": "medium"},
    {"name": "Khabar Online", "url": "https://www.khabaronline.ir/rss", "region": "ir", "lang": "fa", "tier": "medium"},
    {"name": "Hamshahri", "url": "https://www.hamshahrionline.ir/rss", "region": "ir", "lang": "fa", "tier": "medium"},
    {"name": "YJC", "url": "https://www.yjc.news/fa/rss/allnews", "region": "ir", "lang": "fa", "tier": "medium"},
    {"name": "Tabnak", "url": "https://www.tabnak.ir/fa/rss/allnews", "region": "ir", "lang": "fa", "tier": "medium"},
]

NEWS_SOURCE_FEEDS_GLOBAL: list[dict[str, str]] = [
    {"name": "BBC", "url": "http://feeds.bbci.co.uk/news/world/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "BBC Business", "url": "http://feeds.bbci.co.uk/news/business/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "BBC Tech", "url": "http://feeds.bbci.co.uk/news/technology/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "BBC Science", "url": "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "CNN", "url": "http://rss.cnn.com/rss/edition.rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CNN World", "url": "http://rss.cnn.com/rss/edition_world.rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CNN Business", "url": "http://rss.cnn.com/rss/money_latest.rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CNN Tech", "url": "http://rss.cnn.com/rss/edition_technology.rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Reuters", "url": "https://feeds.reuters.com/reuters/worldNews", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Reuters Business", "url": "https://feeds.reuters.com/reuters/businessNews", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Reuters Top", "url": "https://feeds.reuters.com/reuters/topNews", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "The Guardian", "url": "https://www.theguardian.com/world/rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Guardian Business", "url": "https://www.theguardian.com/uk/business/rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Guardian Tech", "url": "https://www.theguardian.com/uk/technology/rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Guardian Science", "url": "https://www.theguardian.com/science/rss", "region": "intl", "lang": "en", "tier": "high"},
    {
        "name": "NYTimes",
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "region": "intl",
        "lang": "en",
        "tier": "high",
    },
    {"name": "NYTimes Business", "url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "NYTimes Technology", "url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "NYTimes Science", "url": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Al Jazeera MiddleEast", "url": "https://www.aljazeera.com/xml/rss/all.xml?topic=middle-east", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "DW", "url": "https://rss.dw.com/xml/rss-en-all", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "DW World", "url": "https://rss.dw.com/rdf/rss-en-world", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "France24", "url": "https://www.france24.com/en/rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "France24 International", "url": "https://www.france24.com/en/international/rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "NPR", "url": "https://feeds.npr.org/1004/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "NPR World", "url": "https://feeds.npr.org/1001/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "NPR Business", "url": "https://feeds.npr.org/1006/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "NPR Health", "url": "https://feeds.npr.org/1128/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "AP Top", "url": "https://rsshub.app/apnews/topics/apf-topnews", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "AP World", "url": "https://rsshub.app/apnews/topics/apf-intlnews", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "ABC News", "url": "https://abcnews.go.com/abcnews/topstories", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CBS News", "url": "https://www.cbsnews.com/latest/rss/main", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "NBC News", "url": "https://feeds.nbcnews.com/nbcnews/public/news", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Fox News", "url": "https://moxie.foxnews.com/google-publisher/latest.xml", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CNBC Top", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CNBC World", "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Euronews", "url": "https://www.euronews.com/rss?level=theme&name=news", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Sky News", "url": "https://feeds.skynews.com/feeds/rss/world.xml", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "The Independent", "url": "https://www.independent.co.uk/news/world/rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "The Hill", "url": "https://thehill.com/feed/", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "VOA", "url": "https://www.voanews.com/api/epiqq", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "UN News", "url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "WSJ World", "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "WSJ Business", "url": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "FT World", "url": "https://www.ft.com/world?format=rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "FT Global Economy", "url": "https://www.ft.com/global-economy?format=rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Bloomberg Markets", "url": "https://feeds.bloomberg.com/markets/news.rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Bloomberg Politics", "url": "https://feeds.bloomberg.com/politics/news.rss", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "The Times of India", "url": "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Hindustan Times", "url": "https://www.hindustantimes.com/feeds/rss/world-news/rssfeed.xml", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "The Hindu", "url": "https://www.thehindu.com/news/international/?service=rss", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Straits Times", "url": "https://www.straitstimes.com/news/world/rss.xml", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Japan Times", "url": "https://www.japantimes.co.jp/news/feed/world/", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "SCMP", "url": "https://www.scmp.com/rss/91/feed", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "SMH", "url": "https://www.smh.com.au/rss/feed.xml", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "The Age", "url": "https://www.theage.com.au/rss/feed.xml", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CBC", "url": "https://www.cbc.ca/webfeed/rss/rss-world", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "CNA", "url": "https://www.channelnewsasia.com/rssfeeds/8395986", "region": "intl", "lang": "en", "tier": "medium"},
    {"name": "Deutsche Welle Business", "url": "https://rss.dw.com/xml/rss-en-business", "region": "intl", "lang": "en", "tier": "high"},
    {"name": "Politico", "url": "https://www.politico.com/rss/politicopicks.xml", "region": "intl", "lang": "en", "tier": "medium"},
]

NEWS_GLOBAL_SITE_DOMAINS: list[str] = [
    "axios.com", "reuters.com", "apnews.com", "bbc.com", "cnn.com", "nytimes.com",
    "washingtonpost.com", "wsj.com", "bloomberg.com", "ft.com", "economist.com",
    "theguardian.com", "independent.co.uk", "telegraph.co.uk", "thetimes.co.uk", "skynews.com",
    "aljazeera.com", "dw.com", "france24.com", "euronews.com", "voanews.com", "npr.org",
    "cbsnews.com", "nbcnews.com", "abcnews.go.com", "foxnews.com", "cnbc.com", "forbes.com",
    "businessinsider.com", "time.com", "newsweek.com", "politico.com", "thehill.com",
    "theatlantic.com", "newyorker.com", "latimes.com", "usatoday.com", "seattletimes.com",
    "chicagotribune.com", "bostonglobe.com", "globalnews.ca", "cbc.ca", "ctvnews.ca",
    "thestar.com", "smh.com.au", "theage.com.au", "abc.net.au", "sbs.com.au",
    "japantimes.co.jp", "scmp.com", "straitstimes.com", "channelnewsasia.com", "thehindu.com",
    "hindustantimes.com", "timesofindia.indiatimes.com", "indianexpress.com", "dawn.com",
    "arabnews.com", "haaretz.com", "jpost.com", "aa.com.tr", "tass.com", "xinhua.net",
    "chinadaily.com.cn", "globaltimes.cn", "koreatimes.co.kr", "koreaherald.com", "nhk.or.jp",
    "asahi.com", "yomiuri.co.jp", "mainichi.jp", "elpais.com", "lemonde.fr", "lefigaro.fr",
    "liberation.fr", "corriere.it", "repubblica.it", "ansa.it", "spiegel.de", "faz.net",
    "sueddeutsche.de", "tagesspiegel.de", "elmundo.es", "abc.es", "infobae.com", "clarin.com",
    "folha.uol.com.br", "estadao.com.br", "globo.com", "uol.com.br", "afp.com", "upi.com",
    "guardian.ng", "allafrica.com", "news24.com", "alarabiya.net", "middleeasteye.net",
    "iranintl.com", "albawaba.com", "nikkei.com", "jiji.com", "kompas.com", "tempo.co",
    "jakartapost.com", "manilatimes.net", "inquirer.net", "rappler.com", "tribune.com.pk",
    "thenews.com.pk", "detik.com", "vietnamnews.vn", "bangkokpost.com", "nationthailand.com",
]

NEWS_SOURCE_FEEDS_GLOBAL_SITE: list[dict[str, str]] = []
for _domain in NEWS_GLOBAL_SITE_DOMAINS:
    _d = _domain.strip().lower()
    if not _d:
        continue
    _q_site = quote_plus(f"site:{_d}")
    NEWS_SOURCE_FEEDS_GLOBAL_SITE.append(
        {
            "name": f"Site { _d } US",
            "url": f"https://news.google.com/rss/search?q={_q_site}&hl=en-US&gl=US&ceid=US:en",
            "region": "intl",
            "lang": "en",
            "tier": "medium",
        }
    )
    NEWS_SOURCE_FEEDS_GLOBAL_SITE.append(
        {
            "name": f"Site { _d } GB",
            "url": f"https://news.google.com/rss/search?q={_q_site}&hl=en-GB&gl=GB&ceid=GB:en",
            "region": "intl",
            "lang": "en",
            "tier": "medium",
        }
    )


def _dedupe_news_feeds(feeds: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for feed in feeds:
        url = str(feed.get("url", "")).strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        out.append(feed)
    return out


NEWS_SOURCE_FEEDS_GLOBAL_ACTIVE: list[dict[str, str]] = _dedupe_news_feeds(
    NEWS_SOURCE_FEEDS_GLOBAL + NEWS_SOURCE_FEEDS_GLOBAL_SITE
)
NEWS_SOURCE_FEEDS: list[dict[str, str]] = _dedupe_news_feeds(NEWS_SOURCE_FEEDS_IR + NEWS_SOURCE_FEEDS_GLOBAL_ACTIVE)

SOURCE_TIER_NAME_HINTS = {
    "reuters": "high",
    "associated press": "high",
    "ap news": "high",
    "bbc": "high",
    "axios": "high",
    "irna": "high",
    "isna": "high",
    "the guardian": "high",
    "nytimes": "high",
    "new york times": "high",
    "al jazeera": "high",
    "dw": "high",
    "npr": "high",
    "cnn": "medium",
    "tasnim": "medium",
    "fars": "medium",
    "mehr": "medium",
    "ilna": "medium",
    "khabar online": "medium",
    "hamshahri": "medium",
    "yjc": "medium",
    "tabnak": "medium",
    "asr iran": "medium",
}

SOURCE_TIER_WEIGHTS = {"high": 1.0, "medium": 0.78, "low": 0.55}
FACT_LABEL_WEIGHTS = {"support": 1.0, "refute": -1.0, "related": 0.15, "irrelevant": 0.0}
FACTCHECK_MODES = ("news", "brief", "pro")
FACT_NEGATION_TERMS = (
    "تکذیب",
    "رد شد",
    "شایعه",
    "نادرست",
    "دروغ",
    "false",
    "fake",
    "hoax",
    "not true",
    "denied",
    "rumor",
)

FACTCHECK_FA_STOPWORDS = {
    "از",
    "به",
    "در",
    "با",
    "برای",
    "که",
    "این",
    "آن",
    "را",
    "می",
    "شود",
    "شده",
    "کرد",
    "کرده",
    "یک",
    "بر",
    "تا",
    "یا",
    "هم",
    "اما",
    "اگر",
    "بود",
    "است",
    "نیست",
    "و",
}

FACTCHECK_EN_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "of",
    "in",
    "on",
    "for",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "with",
    "from",
    "at",
    "by",
    "about",
    "that",
    "this",
    "it",
    "as",
}

SETTINGS_LOCK = threading.Lock()
TRIGGER_COOLDOWN_TS: dict[str, int] = {}
BOT_USER_ID: int | None = None
DB_LOCK = threading.Lock()
USER_RECO_SESSIONS: dict[str, dict[str, Any]] = {}
REMINDER_DRAFTS: dict[str, dict[str, Any]] = {}
REMINDER_DRAFT_COUNTER = 0
AI_THREADS: dict[str, list[dict[str, str]]] = {}
AI_MESSAGE_TO_THREAD: dict[str, str] = {}
AI_LAST_THREAD_BY_CHAT: dict[str, str] = {}
AI_CACHE_LOCK = threading.Lock()
AI_RESPONSE_CACHE: dict[str, tuple[int, str]] = {}
LANGCHAIN_CLIENT: Any = None
LANGCHAIN_AVAILABLE = bool(ChatOpenAI and PromptTemplate and StrOutputParser)
NEWS_LOCK = threading.Lock()
NEWS_LAST_REFRESH_TS = 0
NEWS_FEED_CURSOR = 0
DB_READY = False

NEGATIVE_REPLY_KEYWORDS = ("کسشر شناسایی شد", "کسشر", "چرت بود", "مزخرف بود")
POSITIVE_REPLY_KEYWORDS = ("شاهکار","جمله طلایی", "خفن بود", "عالی بود", "دمت گرم")

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
        "ai_enabled": True,
        "ai_reply_only": False,
        "ai_daily_limit": 50,
        "ai_output_tokens": 1000,
        "smart_forward_enabled": False,
        "smart_forward_channels": [],
        "smart_forward_keywords": [],
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
    global DB_READY
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS news_index (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    source_region TEXT,
                    source_lang TEXT,
                    source_tier TEXT,
                    title TEXT NOT NULL,
                    summary TEXT,
                    link TEXT NOT NULL,
                    published_ts INTEGER,
                    normalized_title TEXT,
                    fetched_at INTEGER,
                    UNIQUE(source, link)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_index_title ON news_index(normalized_title)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_index_published ON news_index(published_ts)"
            )
            conn.commit()
        finally:
            conn.close()
    DB_READY = True


def ensure_database_ready() -> None:
    if DB_READY:
        return
    init_database()


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


def _clean_html_text(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    if "<" not in text and ">" not in text:
        return " ".join(html.unescape(text).split())
    try:
        return " ".join(BeautifulSoup(text, "lxml").get_text(" ", strip=True).split())
    except Exception:
        return " ".join(re.sub(r"<[^>]+>", " ", text).split())


def _guess_news_lang(text: str) -> str:
    sample = (text or "").strip()
    if not sample:
        return "unknown"
    fa_count = len(re.findall(r"[\u0600-\u06FF]", sample))
    en_count = len(re.findall(r"[A-Za-z]", sample))
    if fa_count > en_count:
        return "fa"
    if en_count > fa_count:
        return "en"
    return "unknown"


def _infer_source_tier(source_name: str, fallback: str = "medium") -> str:
    name = normalize_text(source_name)
    for hint, tier in SOURCE_TIER_NAME_HINTS.items():
        if hint in name:
            return tier
    return fallback if fallback in SOURCE_TIER_WEIGHTS else "medium"


def _canonical_news_link(link: str) -> str:
    value = (link or "").strip()
    if not value:
        return ""
    if value.startswith("//"):
        value = "https:" + value
    if not value.startswith("http"):
        return value
    try:
        parsed = urlparse(value)
        cleaned = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        if parsed.query and "news.google.com" not in parsed.netloc:
            cleaned += "?" + parsed.query
        return cleaned or value
    except Exception:
        return value


def _parse_rss_datetime(value: str) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass
    normalized = raw.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(normalized).timestamp())
    except Exception:
        return None


def _extract_feed_items(xml_text: str, source_meta: dict[str, str], max_items: int = 40) -> list[dict[str, Any]]:
    if not xml_text.strip():
        return []
    try:
        soup = BeautifulSoup(xml_text, "xml")
    except Exception:
        return []
    nodes = soup.find_all(["item", "entry"])
    out: list[dict[str, Any]] = []
    now_ts = int(time.time())

    def node_text(node_obj, tag_name: str) -> str:
        tag = node_obj.find(tag_name)
        if tag is None:
            return ""
        return tag.get_text(" ", strip=True)

    for node in nodes[:max_items]:
        title = _clean_html_text(node_text(node, "title"))
        if not title:
            continue

        link = ""
        link_tag = node.find("link")
        if link_tag is not None:
            href = (link_tag.get("href") or "").strip()
            link = href or _clean_html_text(link_tag.get_text(" ", strip=True))
        if not link:
            link = _clean_html_text(node_text(node, "guid"))
        if not link:
            continue

        summary = _clean_html_text(
            node_text(node, "description")
            or node_text(node, "summary")
            or node_text(node, "content")
        )
        source_tag = node.find("source")
        source_name = source_meta.get("name", "Unknown Source")
        if source_tag is not None:
            source_text = _clean_html_text(source_tag.get_text(" ", strip=True))
            if source_text:
                source_name = source_text

        published_ts = (
            _parse_rss_datetime(node_text(node, "pubDate"))
            or _parse_rss_datetime(node_text(node, "updated"))
            or _parse_rss_datetime(node_text(node, "published"))
            or _parse_rss_datetime(node_text(node, "dc:date") or node_text(node, "date"))
        )
        source_tier = _infer_source_tier(source_name, source_meta.get("tier", "medium"))
        out.append(
            {
                "source": source_name[:120],
                "source_region": (source_meta.get("region") or "").strip()[:20],
                "source_lang": (source_meta.get("lang") or "").strip()[:10],
                "source_tier": source_tier,
                "title": title[:600],
                "summary": summary[:1500],
                "link": _canonical_news_link(link)[:1200],
                "published_ts": int(published_ts) if published_ts else None,
                "normalized_title": normalize_fa_text(title)[:700],
                "fetched_at": now_ts,
            }
        )
    return out


def _fetch_rss_items(feed_url: str, source_meta: dict[str, str], max_items: int = 40) -> list[dict[str, Any]]:
    try:
        res = requests.get(
            feed_url,
            headers=NEWS_REQUEST_HEADERS,
            timeout=NEWS_HTTP_TIMEOUT,
        )
        if res.status_code >= 400:
            return []
        return _extract_feed_items(res.text, source_meta=source_meta, max_items=max_items)
    except Exception:
        return []


def db_upsert_news_items(items: list[dict[str, Any]]) -> int:
    ensure_database_ready()
    if not items:
        return 0
    rows = []
    for item in items:
        source = str(item.get("source") or "").strip()
        link = str(item.get("link") or "").strip()
        title = str(item.get("title") or "").strip()
        if not source or not link or not title:
            continue
        rows.append(
            (
                source,
                str(item.get("source_region") or "")[:20],
                str(item.get("source_lang") or "")[:10],
                _infer_source_tier(str(item.get("source_tier") or ""), fallback="medium"),
                title[:600],
                str(item.get("summary") or "")[:1500],
                link[:1200],
                int(item["published_ts"]) if item.get("published_ts") else None,
                normalize_fa_text(title)[:700],
                int(item.get("fetched_at") or time.time()),
            )
        )
    if not rows:
        return 0
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO news_index
                (source, source_region, source_lang, source_tier, title, summary, link, published_ts, normalized_title, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, link) DO UPDATE SET
                    source_region=excluded.source_region,
                    source_lang=excluded.source_lang,
                    source_tier=excluded.source_tier,
                    title=excluded.title,
                    summary=excluded.summary,
                    published_ts=excluded.published_ts,
                    normalized_title=excluded.normalized_title,
                    fetched_at=excluded.fetched_at
                """,
                rows,
            )
            conn.commit()
            return len(rows)
        finally:
            conn.close()


def db_latest_news_fetch_ts() -> int:
    ensure_database_ready()
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(MAX(fetched_at), 0) FROM news_index")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        finally:
            conn.close()


def db_prune_news_index(keep_days: int = NEWS_INDEX_KEEP_DAYS) -> None:
    ensure_database_ready()
    cutoff = int(time.time()) - max(1, keep_days) * 86400
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM news_index
                WHERE COALESCE(published_ts, fetched_at, 0) < ?
                """,
                (cutoff,),
            )
            conn.commit()
        finally:
            conn.close()


def _news_feed_batch(feeds: list[dict[str, str]], max_count: int) -> list[dict[str, str]]:
    global NEWS_FEED_CURSOR
    if not feeds:
        return []
    cap = max(1, min(max_count, len(feeds)))
    if len(feeds) <= cap:
        return list(feeds)
    start = NEWS_FEED_CURSOR % len(feeds)
    batch = [feeds[(start + i) % len(feeds)] for i in range(cap)]
    NEWS_FEED_CURSOR = (start + cap) % len(feeds)
    return batch


def refresh_news_index(force: bool = False) -> dict[str, Any]:
    global NEWS_LAST_REFRESH_TS
    now_ts = int(time.time())
    with NEWS_LOCK:
        latest_ts = max(NEWS_LAST_REFRESH_TS, db_latest_news_fetch_ts())
        min_gap_sec = NEWS_REFRESH_INTERVAL_MIN * 60
        if not force and latest_ts > 0 and (now_ts - latest_ts) < min_gap_sec:
            return {"refreshed": False, "inserted": 0, "feeds": 0}

        start_ts = time.time()
        total_inserted = 0
        used_feeds = 0
        feed_batch = _news_feed_batch(NEWS_SOURCE_FEEDS, NEWS_REFRESH_MAX_FEEDS)
        for feed in feed_batch:
            if not force and (time.time() - start_ts) > NEWS_REFRESH_BUDGET_SEC:
                break
            items = _fetch_rss_items(feed.get("url", ""), source_meta=feed, max_items=35)
            if items:
                total_inserted += db_upsert_news_items(items)
            used_feeds += 1
        NEWS_LAST_REFRESH_TS = now_ts
        db_prune_news_index()
        return {"refreshed": True, "inserted": total_inserted, "feeds": used_feeds}


def _news_query_feed_defs(query: str) -> list[dict[str, str]]:
    q = quote_plus((query or "").strip())
    if not q:
        return []
    return [
        {
            "url": f"https://news.google.com/rss/search?q={q}&hl=fa&gl=IR&ceid=IR:fa",
            "name": "Google News",
            "region": "mixed",
            "lang": "fa",
            "tier": "medium",
        },
        {
            "url": f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en",
            "name": "Google News",
            "region": "mixed",
            "lang": "en",
            "tier": "medium",
        },
        {
            "url": f"https://www.bing.com/news/search?q={q}&format=rss",
            "name": "Bing News",
            "region": "mixed",
            "lang": "en",
            "tier": "low",
        },
    ]


def _fetch_query_news_items(query: str) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for feed in _news_query_feed_defs(query):
        items = _fetch_rss_items(feed["url"], source_meta=feed, max_items=28)
        if items:
            merged.extend(items)
    return merged


def _domain_from_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    try:
        netloc = (urlparse(value).netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""


def _unwrap_search_redirect(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""
    try:
        parsed = urlsplit(url)
        host = (parsed.netloc or "").lower()
        if "duckduckgo.com" in host and parsed.path.startswith("/l/"):
            qs = parse_qs(parsed.query or "")
            uddg = (qs.get("uddg") or [""])[0]
            if uddg:
                return unquote(uddg).strip()
        if "bing.com" in host and "/news/apiclick.aspx" in parsed.path.lower():
            qs = parse_qs(parsed.query or "")
            target = (qs.get("url") or [""])[0]
            if target:
                return unquote(target).strip()
    except Exception:
        return url
    return url


def _parse_html_datetime(raw: str) -> int | None:
    value = (raw or "").strip()
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(cleaned).timestamp())
    except Exception:
        pass
    return _parse_rss_datetime(value)


def _extract_article_metadata(url: str) -> dict[str, Any]:
    out = {"title": "", "summary": "", "published_ts": None}
    target = _canonical_news_link(url)
    if not target:
        return out
    try:
        res = requests.get(target, headers=NEWS_REQUEST_HEADERS, timeout=NEWS_HTTP_TIMEOUT)
        if res.status_code >= 400 or not res.text:
            return out
        soup = BeautifulSoup(res.text, "lxml")
        title = ""
        for selector in (
            "meta[property='og:title']",
            "meta[name='twitter:title']",
            "meta[name='title']",
            "title",
        ):
            node = soup.select_one(selector)
            if node is None:
                continue
            value = (node.get("content") if node.name == "meta" else node.get_text(" ", strip=True)) or ""
            value = _clean_html_text(value)
            if value:
                title = value
                break
        summary = ""
        for selector in (
            "meta[property='og:description']",
            "meta[name='description']",
            "meta[name='twitter:description']",
        ):
            node = soup.select_one(selector)
            if node is None:
                continue
            value = _clean_html_text(node.get("content") or "")
            if value:
                summary = value
                break
        published_ts = None
        for selector in (
            "meta[property='article:published_time']",
            "meta[name='pubdate']",
            "meta[name='publishdate']",
            "meta[itemprop='datePublished']",
            "time[datetime]",
        ):
            node = soup.select_one(selector)
            if node is None:
                continue
            raw_dt = node.get("content") or node.get("datetime") or node.get_text(" ", strip=True)
            published_ts = _parse_html_datetime(raw_dt or "")
            if published_ts:
                break
        out["title"] = title[:600]
        out["summary"] = summary[:1500]
        out["published_ts"] = published_ts
        return out
    except Exception:
        return out


def _fetch_live_search_page(url: str) -> str:
    try:
        res = requests.get(url, headers=NEWS_REQUEST_HEADERS, timeout=NEWS_HTTP_TIMEOUT)
        if res.status_code >= 400:
            return ""
        return res.text or ""
    except Exception:
        return ""


def _parse_ddg_html_results(html_text: str, max_items: int = 12) -> list[dict[str, Any]]:
    if not html_text.strip():
        return []
    out: list[dict[str, Any]] = []
    try:
        soup = BeautifulSoup(html_text, "lxml")
        nodes = soup.select("a.result__a")
        for node in nodes[: max_items * 2]:
            href = _unwrap_search_redirect((node.get("href") or "").strip())
            title = _clean_html_text(node.get_text(" ", strip=True))
            if not href or not title:
                continue
            parent = node.find_parent("div", class_=re.compile(r"result"))
            snippet = ""
            if parent is not None:
                sn = parent.select_one(".result__snippet")
                if sn is not None:
                    snippet = _clean_html_text(sn.get_text(" ", strip=True))
            source_name = _domain_from_url(href) or "Web"
            out.append(
                {
                    "source": source_name,
                    "source_region": "mixed",
                    "source_lang": "unknown",
                    "source_tier": _infer_source_tier(source_name, fallback="medium"),
                    "title": title[:600],
                    "summary": snippet[:1500],
                    "link": _canonical_news_link(href)[:1200],
                    "published_ts": None,
                    "normalized_title": normalize_fa_text(title)[:700],
                    "fetched_at": int(time.time()),
                }
            )
            if len(out) >= max_items:
                break
    except Exception:
        return out
    return out


def _parse_bing_html_results(html_text: str, max_items: int = 12) -> list[dict[str, Any]]:
    if not html_text.strip():
        return []
    out: list[dict[str, Any]] = []
    try:
        soup = BeautifulSoup(html_text, "lxml")
        nodes = soup.select("li.news-card, div.news-card, li.b_algo")
        for node in nodes:
            a = node.select_one("a[href]")
            if a is None:
                continue
            href = _unwrap_search_redirect((a.get("href") or "").strip())
            title = _clean_html_text(a.get_text(" ", strip=True))
            if not href or not title:
                continue
            snippet = ""
            for selector in ("p.snippet", "div.snippet", "div.news-card-snippet", "p"):
                sn = node.select_one(selector)
                if sn is not None:
                    snippet = _clean_html_text(sn.get_text(" ", strip=True))
                    if snippet:
                        break
            source_name = _domain_from_url(href) or "Web"
            out.append(
                {
                    "source": source_name,
                    "source_region": "mixed",
                    "source_lang": "unknown",
                    "source_tier": _infer_source_tier(source_name, fallback="medium"),
                    "title": title[:600],
                    "summary": snippet[:1500],
                    "link": _canonical_news_link(href)[:1200],
                    "published_ts": None,
                    "normalized_title": normalize_fa_text(title)[:700],
                    "fetched_at": int(time.time()),
                }
            )
            if len(out) >= max_items:
                break
    except Exception:
        return out
    return out


def _news_item_timestamp(item: dict[str, Any]) -> int:
    try:
        return int(item.get("published_ts") or item.get("fetched_at") or 0)
    except Exception:
        return 0


def _merge_news_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_key: dict[str, int] = {}
    for raw in items:
        item = dict(raw or {})
        link = _canonical_news_link(str(item.get("link", "")))[:1200]
        title = _clean_html_text(str(item.get("title", "")))[:600]
        if not link and not title:
            continue
        item["link"] = link
        item["title"] = title
        item["summary"] = _clean_html_text(str(item.get("summary", "")))[:1500]
        if not item.get("normalized_title"):
            item["normalized_title"] = normalize_fa_text(title)[:700]
        if not item.get("source"):
            item["source"] = _domain_from_url(link) or "Web"
        if not item.get("source_tier"):
            item["source_tier"] = _infer_source_tier(str(item.get("source", "")), fallback="medium")
        if not item.get("fetched_at"):
            item["fetched_at"] = int(time.time())

        key = normalize_text(link) or f"{normalize_text(item.get('source', ''))}|{normalize_text(title)}"
        if not key:
            continue

        existing_idx = index_by_key.get(key)
        if existing_idx is None:
            index_by_key[key] = len(merged)
            merged.append(item)
            continue

        existing = merged[existing_idx]
        old_ts = _news_item_timestamp(existing)
        new_ts = _news_item_timestamp(item)
        old_text_len = len(str(existing.get("title", ""))) + len(str(existing.get("summary", "")))
        new_text_len = len(str(item.get("title", ""))) + len(str(item.get("summary", "")))
        if (new_ts, new_text_len) > (old_ts, old_text_len):
            merged[existing_idx] = item
    return merged


def _merge_live_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _merge_news_items(items)


def _fetch_live_search_items(query: str, max_items: int = LIVE_SEARCH_MAX_RESULTS) -> list[dict[str, Any]]:
    if not LIVE_SEARCH_ENABLED:
        return []
    q = quote_plus((query or "").strip())
    if not q:
        return []
    ddg_url = f"https://duckduckgo.com/html/?q={q}%20news"
    bing_url = f"https://www.bing.com/news/search?q={q}"
    merged: list[dict[str, Any]] = []
    ddg_html = _fetch_live_search_page(ddg_url)
    if ddg_html:
        merged.extend(_parse_ddg_html_results(ddg_html, max_items=max_items))
    bing_html = _fetch_live_search_page(bing_url)
    if bing_html:
        merged.extend(_parse_bing_html_results(bing_html, max_items=max_items))
    merged = _merge_live_items(merged)
    if LIVE_FETCH_MAX_ARTICLES <= 0:
        return merged[:max_items]

    # Pull extra metadata directly from article pages for top live links.
    for item in merged[: min(LIVE_FETCH_MAX_ARTICLES, len(merged))]:
        meta = _extract_article_metadata(str(item.get("link", "")))
        if meta.get("title"):
            item["title"] = str(meta.get("title"))
            item["normalized_title"] = normalize_fa_text(item["title"])[:700]
        if meta.get("summary"):
            item["summary"] = str(meta.get("summary"))
        if meta.get("published_ts"):
            try:
                item["published_ts"] = int(meta["published_ts"])
            except Exception:
                pass
    return merged[:max_items]


def _tokenize_fact_text(text: str) -> list[str]:
    if not text:
        return []
    return re.findall(r"[A-Za-z0-9\u0600-\u06FF]{2,}", normalize_fa_text(text))


def _extract_fact_keywords(text: str, limit: int = 10) -> list[str]:
    tokens = _tokenize_fact_text(text)
    if not tokens:
        return []
    lang = _guess_news_lang(text)
    stopwords = FACTCHECK_FA_STOPWORDS if lang == "fa" else FACTCHECK_EN_STOPWORDS
    filtered = [t for t in tokens if t not in stopwords and len(t) >= 3]
    ranked = [tok for tok, _ in Counter(filtered).most_common(limit * 2)]
    out: list[str] = []
    for tok in ranked:
        if tok not in out:
            out.append(tok)
        if len(out) >= limit:
            break
    return out


def _text_similarity(a: str, b: str) -> float:
    left = normalize_fa_text(a)
    right = normalize_fa_text(b)
    if not left or not right:
        return 0.0
    ratio = SequenceMatcher(None, left, right).ratio()
    left_set = set(_tokenize_fact_text(left))
    right_set = set(_tokenize_fact_text(right))
    if not left_set or not right_set:
        return ratio
    inter = len(left_set & right_set)
    union = len(left_set | right_set)
    jaccard = inter / union if union else 0.0
    contains = 1.0 if (left in right or right in left) else 0.0
    return (0.52 * ratio) + (0.38 * jaccard) + (0.10 * contains)


def db_search_news_candidates(search_terms: list[str], limit: int = 220) -> list[dict[str, Any]]:
    ensure_database_ready()
    terms = [t for t in search_terms if t and len(t) >= 3][:10]
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            if terms:
                where_parts: list[str] = []
                like_params: list[str] = []
                for t in terms:
                    norm_t = f"%{normalize_fa_text(t)}%"
                    raw_t = f"%{t}%"
                    where_parts.append("(normalized_title LIKE ? OR title LIKE ? OR summary LIKE ?)")
                    like_params.extend([norm_t, raw_t, raw_t])
                where = " OR ".join(where_parts)
                cur.execute(
                    f"""
                    SELECT source, source_region, source_lang, source_tier, title, summary, link, published_ts, normalized_title, fetched_at
                    FROM news_index
                    WHERE {where}
                    ORDER BY COALESCE(published_ts, fetched_at, 0) DESC
                    LIMIT ?
                    """,
                    (*like_params, int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT source, source_region, source_lang, source_tier, title, summary, link, published_ts, normalized_title, fetched_at
                    FROM news_index
                    ORDER BY COALESCE(published_ts, fetched_at, 0) DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                )
            rows = cur.fetchall()
        finally:
            conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "source": str(row[0] or ""),
                "source_region": str(row[1] or ""),
                "source_lang": str(row[2] or ""),
                "source_tier": _infer_source_tier(str(row[0] or ""), str(row[3] or "medium")),
                "title": str(row[4] or ""),
                "summary": str(row[5] or ""),
                "link": str(row[6] or ""),
                "published_ts": int(row[7]) if row[7] is not None else None,
                "normalized_title": str(row[8] or ""),
                "fetched_at": int(row[9]) if row[9] is not None else 0,
            }
        )
    return out


def _extract_json_payload(raw: str) -> str:
    txt = (raw or "").strip()
    if not txt:
        return ""
    if txt.startswith("```"):
        txt = re.sub(r"^```(?:json)?", "", txt).strip()
        txt = re.sub(r"```$", "", txt).strip()
    arr = re.search(r"(\[[\s\S]+\])", txt)
    if arr:
        return arr.group(1)
    obj = re.search(r"(\{[\s\S]+\})", txt)
    if obj:
        return obj.group(1)
    return txt


def _ai_translate_fact_text(text: str, target_lang: str) -> str:
    if not OPENAI_API_KEY:
        return ""
    source = (text or "").strip()
    if not source:
        return ""
    target = "English" if target_lang == "en" else "Persian"
    prompt = (
        "Translate this news claim accurately. "
        f"Target language: {target}. "
        "Keep names, numbers, and dates exact. "
        "Return only the translated sentence.\n\n"
        f"Claim: {source}"
    )
    return call_recommendation_model(prompt, max_output_tokens=140, reasoning_effort="low").strip()


def _ai_distill_claim(text: str) -> str:
    if not OPENAI_API_KEY:
        return ""
    source = (text or "").strip()
    if not source:
        return ""
    prompt = (
        "You are extracting a single fact-checkable claim from a news text.\n"
        "Return one concise sentence (max 35 words) that preserves entities, numbers, and dates.\n"
        "Do not explain. Do not add uncertainty words.\n\n"
        f"Text:\n{source}"
    )
    return call_recommendation_model(prompt, max_output_tokens=120, reasoning_effort="low").strip()


def _ai_label_evidence(claim: str, evidence: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    if not OPENAI_API_KEY or not evidence:
        return {}
    lines: list[str] = []
    for idx, item in enumerate(evidence, start=1):
        title = _clean_html_text(item.get("title", ""))[:180]
        summary = _clean_html_text(item.get("summary", ""))[:220]
        source = _clean_html_text(item.get("source", ""))[:60]
        lines.append(f"{idx}. source={source} | title={title} | summary={summary}")
    prompt = (
        "You are a fact-check evidence classifier.\n"
        "Given one claim and evidence headlines/snippets, label each line as one of:\n"
        "support, refute, related, irrelevant.\n"
        "Return JSON array only with shape:\n"
        "[{\"idx\":1,\"label\":\"support\",\"confidence\":0-100,\"reason\":\"short\"}]\n"
        "Confidence must reflect evidence strength for this specific claim.\n\n"
        f"Claim: {claim}\n\n"
        "Evidence:\n"
        + "\n".join(lines)
    )
    raw = call_recommendation_model(prompt, max_output_tokens=700, reasoning_effort="low")
    payload = _extract_json_payload(raw)
    if not payload:
        return {}
    try:
        data = json.loads(payload)
    except Exception:
        return {}
    if not isinstance(data, list):
        return {}
    out: dict[int, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        idx_raw = item.get("idx")
        try:
            idx = int(idx_raw)
        except Exception:
            continue
        label = normalize_text(str(item.get("label", "related")))
        if label not in FACT_LABEL_WEIGHTS:
            label = "related"
        try:
            conf = float(item.get("confidence", 50))
        except Exception:
            conf = 50.0
        out[idx - 1] = {
            "label": label,
            "confidence": max(0.0, min(100.0, conf)) / 100.0,
            "reason": str(item.get("reason", ""))[:200],
        }
    return out


def _ai_factcheck_reasoning(
    claim: str,
    evidence: list[dict[str, Any]],
    scored_summary: dict[str, Any],
) -> dict[str, Any]:
    if not OPENAI_API_KEY or not evidence:
        return {}
    lines: list[str] = []
    for idx, item in enumerate(evidence, start=1):
        title = _clean_html_text(item.get("title", ""))[:170]
        summary = _clean_html_text(item.get("summary", ""))[:220]
        source = _clean_html_text(item.get("source", ""))[:70]
        date_text = _fmt_news_date(item.get("published_ts"))
        label = normalize_text(str(item.get("label", "related")))
        lines.append(
            f"{idx}. source={source} | date={date_text} | label={label} | title={title} | summary={summary} | link={item.get('link','')}"
        )

    prompt = (
        "You are an evidence-grounded fact-check analyst.\n"
        "Use ONLY the provided evidence. Do not invent facts.\n"
        "Return strict JSON object with this shape:\n"
        "{"
        "\"overall\":\"short verdict in Persian\","
        "\"why\":\"concise explanation in Persian\","
        "\"parts\":[{\"claim_part\":\"...\",\"status\":\"true|false|uncertain\",\"why\":\"...\",\"evidence\":[1,2]}],"
        "\"missing\":\"what is missing to conclude with high confidence\""
        "}\n"
        "Rules:\n"
        "- For each part, cite evidence indexes that directly support the statement.\n"
        "- If evidence conflicts, mark uncertain.\n"
        "- Keep output concise and factual.\n\n"
        f"Claim: {claim}\n"
        f"Score summary: truth_prob={float(scored_summary.get('truth_prob',0.5)):.2f}, "
        f"fake_prob={float(scored_summary.get('fake_prob',0.5)):.2f}, "
        f"confidence={float(scored_summary.get('confidence',0.2)):.2f}\n\n"
        "Evidence list:\n"
        + "\n".join(lines)
    )
    raw = call_recommendation_model(prompt, max_output_tokens=820, reasoning_effort="low")
    payload = _extract_json_payload(raw)
    if not payload:
        return {}
    try:
        data = json.loads(payload)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    parts = data.get("parts")
    if isinstance(parts, list):
        normalized_parts = []
        for part in parts[:6]:
            if not isinstance(part, dict):
                continue
            idxs = []
            for i in (part.get("evidence") or []):
                try:
                    ii = int(i)
                except Exception:
                    continue
                if 1 <= ii <= len(evidence):
                    idxs.append(ii)
            status = normalize_text(str(part.get("status", "uncertain")))
            if status not in ("true", "false", "uncertain"):
                status = "uncertain"
            normalized_parts.append(
                {
                    "claim_part": str(part.get("claim_part", ""))[:220],
                    "status": status,
                    "why": str(part.get("why", ""))[:260],
                    "evidence": sorted(set(idxs)),
                }
            )
        data["parts"] = normalized_parts
    else:
        data["parts"] = []
    data["overall"] = str(data.get("overall", ""))[:260]
    data["why"] = str(data.get("why", ""))[:360]
    data["missing"] = str(data.get("missing", ""))[:320]
    return data


def _contains_negation(text: str) -> bool:
    norm = normalize_fa_text(text)
    return any(term in norm for term in FACT_NEGATION_TERMS)


def _heuristic_label(claim: str, item: dict[str, Any], relevance: float) -> tuple[str, float, str]:
    combined = f"{item.get('title', '')} {item.get('summary', '')}".strip()
    neg_claim = _contains_negation(claim)
    neg_item = _contains_negation(combined)
    if relevance < 0.14:
        return "irrelevant", 0.28, "ارتباط واژگانی کم"
    if relevance >= 0.50:
        if neg_claim != neg_item:
            return "refute", 0.56, "واژگان تکذیب/رد متفاوت است"
        return "support", 0.58, "شباهت محتوایی بالا"
    if relevance >= 0.24:
        return "related", 0.50, "موضوع مشابه ولی تایید قطعی ندارد"
    return "irrelevant", 0.33, "ارتباط مستقیم کافی نیست"


def _score_factcheck(
    claim: str,
    items: list[dict[str, Any]],
    ai_labels: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    if not items:
        return {
            "verdict": "نامطمئن",
            "truth_prob": 0.5,
            "fake_prob": 0.5,
            "confidence": 0.15,
            "evidence": [],
            "support_count": 0,
            "refute_count": 0,
            "related_count": 0,
            "source_count": 0,
            "score_reason": "no_evidence",
            "score_why": "هیچ سند خبری مرتبطی با آستانه شباهت لازم پیدا نشد؛ امتیاز به صورت خنثی 50/50 تنظیم شد.",
        }

    now_ts = int(time.time())
    net_score = 0.0
    total_strength = 0.0
    support_count = 0
    refute_count = 0
    related_count = 0
    seen_sources: set[str] = set()
    scored: list[dict[str, Any]] = []

    for idx, item in enumerate(items):
        evidence_text = f"{item.get('title', '')} {item.get('summary', '')}".strip()
        relevance = float(item.get("relevance", _text_similarity(claim, evidence_text)))
        ai = ai_labels.get(idx, {})
        if ai:
            label = normalize_text(str(ai.get("label", "related")))
            conf = float(ai.get("confidence", 0.55))
            reason = str(ai.get("reason", ""))[:200]
            if label not in FACT_LABEL_WEIGHTS:
                label, conf, reason = _heuristic_label(claim, item, relevance)
        else:
            label, conf, reason = _heuristic_label(claim, item, relevance)

        tier = _infer_source_tier(str(item.get("source", "")), str(item.get("source_tier", "medium")))
        source_weight = SOURCE_TIER_WEIGHTS.get(tier, SOURCE_TIER_WEIGHTS["medium"])
        published_ts = int(item.get("published_ts") or item.get("fetched_at") or now_ts)
        age_days = max(0.0, (now_ts - published_ts) / 86400.0)
        freshness = 1.0 / (1.0 + (age_days / 14.0))
        unit = source_weight * (0.35 + 0.65 * max(0.0, min(1.0, conf)))
        unit *= (0.30 + 0.70 * max(0.0, min(1.0, relevance)))
        unit *= (0.55 + 0.45 * freshness)
        effect = FACT_LABEL_WEIGHTS.get(label, 0.0) * unit

        if label == "support":
            support_count += 1
        elif label == "refute":
            refute_count += 1
        elif label == "related":
            related_count += 1

        seen_sources.add(normalize_text(str(item.get("source", ""))))
        net_score += effect
        total_strength += abs(effect)
        scored.append(
            {
                **item,
                "label": label,
                "label_conf": conf,
                "reason": reason,
                "tier": tier,
                "effect": effect,
                "relevance": relevance,
                "freshness": freshness,
            }
        )

    coverage = min(1.0, len([s for s in seen_sources if s]) / 6.0)
    base_prob = 1.0 / (1.0 + exp(-2.7 * net_score))
    truth_prob = (0.88 * base_prob) + (0.12 * coverage)
    truth_prob = max(0.03, min(0.97, truth_prob))
    fake_prob = 1.0 - truth_prob
    confidence = 0.23 + min(0.43, total_strength / max(1.0, len(items)))
    confidence += 0.20 * coverage
    confidence += 0.12 * min(1.0, len(items) / 8.0)
    confidence = max(0.10, min(0.97, confidence))

    verdict = "نامطمئن"
    if confidence >= 0.45:
        if truth_prob >= 0.65:
            verdict = "احتمالاً واقعی"
        elif truth_prob <= 0.35:
            verdict = "احتمالاً فیک/گمراه‌کننده"
        else:
            verdict = "نیازمند بررسی بیشتر"

    scored.sort(key=lambda x: abs(float(x.get("effect", 0.0))), reverse=True)
    score_reason = "scored"
    score_why = "امتیاز بر اساس شواهد موافق/مخالف، اعتبار منبع، تازگی خبر و شباهت محتوایی محاسبه شد."
    if len([s for s in seen_sources if s]) == 0 or (support_count + refute_count + related_count) == 0:
        score_reason = "no_evidence"
        score_why = "سند مستقیم کافی پیدا نشد؛ نتیجه عددی خنثی/کم‌اطمینان است."
    elif support_count > 0 and refute_count > 0:
        score_reason = "conflicting_evidence"
        score_why = "شواهد معتبر هم‌زمان موافق و مخالف وجود دارد؛ نتیجه نهایی با اطمینان پایین‌تر گزارش شد."
    elif confidence < 0.35:
        score_reason = "low_confidence"
        score_why = "تعداد یا کیفیت شواهد برای نتیجه قطعی کافی نبود."
    elif abs(truth_prob - 0.5) <= 0.08:
        score_reason = "near_balanced"
        score_why = "وزن شواهد نزدیک به تعادل است و نتیجه قطعی به دست نیامده است."
    return {
        "verdict": verdict,
        "truth_prob": truth_prob,
        "fake_prob": fake_prob,
        "confidence": confidence,
        "evidence": scored,
        "support_count": support_count,
        "refute_count": refute_count,
        "related_count": related_count,
        "source_count": len([s for s in seen_sources if s]),
        "score_reason": score_reason,
        "score_why": score_why,
    }


def _fmt_news_date(ts: int | None) -> str:
    if not ts:
        return "--"
    try:
        return datetime.fromtimestamp(int(ts), tz=TEHRAN_TZ).strftime("%Y-%m-%d")
    except Exception:
        return "--"


def _normalize_claim_seed(text: str) -> str:
    src = _clean_html_text(text or "")
    if not src:
        return ""
    src = src.replace("🔴", " ").replace("🟡", " ").replace("🟢", " ")
    src = re.sub(r"#\S+", " ", src)
    src = re.sub(r"\s+", " ", src).strip()
    return src


def run_news_factcheck(text: str, mode: str = "news") -> dict[str, Any]:
    raw_text = _normalize_claim_seed(text or "")
    if not raw_text:
        return {"ok": False, "error": "متن خبر خالی است."}
    check_mode = normalize_text(mode)
    if check_mode not in FACTCHECK_MODES:
        check_mode = "news"
    allow_ai = bool(OPENAI_API_KEY)
    use_ai = check_mode == "pro" and allow_ai

    claim = (_ai_distill_claim(raw_text) if use_ai else "") or raw_text[:520]
    claim = " ".join(claim.split())[:500]
    lang = _guess_news_lang(claim)
    translated = ""
    if allow_ai:
        if lang == "fa":
            translated = _ai_translate_fact_text(claim, "en")
        elif lang == "en":
            translated = _ai_translate_fact_text(claim, "fa")

    queries: list[str] = [claim]
    if translated and normalize_text(translated) != normalize_text(claim):
        queries.append(translated)
    norm_claim = normalize_text(claim)
    norm_raw = normalize_text(raw_text)
    if "axios" in norm_claim or "axios" in norm_raw:
        site_query = f"site:axios.com {(translated or claim)}"
        queries.append(site_query[:240])
    keywords = _extract_fact_keywords(claim, limit=7)
    if keywords:
        keyword_query = " ".join(keywords)
        if keyword_query and keyword_query not in queries:
            queries.append(keyword_query)
    if translated:
        en_keywords = _extract_fact_keywords(translated, limit=6)
        if en_keywords:
            en_query = " ".join(en_keywords)
            if en_query and en_query not in queries:
                queries.append(en_query)
    # Keep the search compact to avoid long user wait.
    uniq_queries: list[str] = []
    for q in queries:
        qq = " ".join((q or "").split()).strip()
        if qq and qq not in uniq_queries:
            uniq_queries.append(qq)
    uniq_queries = uniq_queries[:FACTCHECK_QUERY_MAX]

    mode_evidence_limit = 4 if check_mode == "brief" else (10 if check_mode == "news" else FACTCHECK_MAX_EVIDENCE)
    refresh_info = refresh_news_index(force=False)
    fetched_rss: list[dict[str, Any]] = []
    fetched_live: list[dict[str, Any]] = []
    live_query_cap = 1 if check_mode == "brief" else (2 if check_mode == "news" else 3)
    live_items_per_query = min(
        LIVE_SEARCH_MAX_RESULTS,
        8 if check_mode == "brief" else (12 if check_mode == "news" else max(12, LIVE_SEARCH_MAX_RESULTS)),
    )
    live_queries_run = 0
    for idx, query in enumerate(uniq_queries):
        rss_items = _fetch_query_news_items(query)
        if rss_items:
            fetched_rss.extend(rss_items)
        if LIVE_SEARCH_ENABLED and idx < live_query_cap:
            live_queries_run += 1
            live_items = _fetch_live_search_items(query, max_items=live_items_per_query)
            if live_items:
                fetched_live.extend(live_items)

    fetched_combined = _merge_news_items(fetched_rss + fetched_live)
    if fetched_combined:
        db_upsert_news_items(fetched_combined)

    search_terms = _extract_fact_keywords(" ".join(uniq_queries + [claim, translated]), limit=20)
    local_limit = 180 if check_mode == "brief" else (280 if check_mode == "news" else 360)
    local_candidates = db_search_news_candidates(search_terms, limit=local_limit)
    candidate_pool = _merge_news_items(fetched_combined + local_candidates)
    now_ts = int(time.time())
    recent_cutoff = now_ts - (FACTCHECK_RECENT_DAYS * 86400)
    claim_signatures: list[str] = []
    for sig in (
        claim,
        " ".join(_extract_fact_keywords(claim, limit=18)),
        translated,
        " ".join(_extract_fact_keywords(translated, limit=14)) if translated else "",
    ):
        cleaned = " ".join((sig or "").split()).strip()
        if cleaned and cleaned not in claim_signatures:
            claim_signatures.append(cleaned)
    if not claim_signatures:
        claim_signatures.append(claim)

    def _claim_relevance(evidence_text: str) -> float:
        return max(_text_similarity(sig, evidence_text) for sig in claim_signatures)

    ranked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in candidate_pool:
        key = normalize_text(item.get("link", "")) or (
            f"{normalize_text(item.get('source', ''))}|{normalize_text(item.get('title', ''))}"
        )
        if not key or key in seen:
            continue
        combined = f"{item.get('title', '')} {item.get('summary', '')}".strip()
        relevance = _claim_relevance(combined)
        if relevance < 0.12:
            continue
        published_ts = _news_item_timestamp(item)
        age_days = max(0.0, (now_ts - published_ts) / 86400.0) if published_ts > 0 else 0.0
        freshness = 1.0 / (1.0 + (age_days / 14.0))
        if published_ts > 0 and published_ts < recent_cutoff and relevance < 0.33:
            continue
        seen.add(key)
        item["relevance"] = relevance
        item["rank_score"] = (0.82 * relevance) + (0.18 * freshness)
        item["age_days"] = round(age_days, 2)
        item["is_recent"] = bool(published_ts >= recent_cutoff) if published_ts > 0 else False
        ranked.append(item)

    # If strict recency filter removed too much, keep a small relaxed backup set.
    need_floor = max(8, mode_evidence_limit * 2)
    if len(ranked) < need_floor:
        for item in candidate_pool:
            key = normalize_text(item.get("link", "")) or (
                f"{normalize_text(item.get('source', ''))}|{normalize_text(item.get('title', ''))}"
            )
            if not key or key in seen:
                continue
            combined = f"{item.get('title', '')} {item.get('summary', '')}".strip()
            relevance = _claim_relevance(combined)
            if relevance < 0.10:
                continue
            published_ts = _news_item_timestamp(item)
            age_days = max(0.0, (now_ts - published_ts) / 86400.0) if published_ts > 0 else 0.0
            freshness = 1.0 / (1.0 + (age_days / 18.0))
            seen.add(key)
            item["relevance"] = relevance
            item["rank_score"] = (0.86 * relevance) + (0.14 * freshness)
            item["age_days"] = round(age_days, 2)
            item["is_recent"] = bool(published_ts >= recent_cutoff) if published_ts > 0 else False
            ranked.append(item)
            if len(ranked) >= local_limit:
                break

    ranked.sort(
        key=lambda x: (
            float(x.get("rank_score", x.get("relevance", 0.0))),
            int(x.get("published_ts") or x.get("fetched_at") or 0),
        ),
        reverse=True,
    )
    selected = ranked[: max(mode_evidence_limit * 2, 12)]
    ai_labels = _ai_label_evidence(claim, selected[:mode_evidence_limit]) if use_ai else {}
    scored = _score_factcheck(claim, selected[:mode_evidence_limit], ai_labels)
    top_evidence = scored.get("evidence", [])[:mode_evidence_limit]
    ai_reasoning = _ai_factcheck_reasoning(claim, top_evidence, scored) if use_ai else {}
    ai_web = _ai_factcheck_web_verdict(claim, translated_claim=translated) if (check_mode == "pro" and use_ai) else {}
    if ai_web:
        scored = _blend_fact_scores(scored, ai_web)
    recent_candidate_count = len([x for x in ranked if bool(x.get("is_recent", False))])
    return {
        "ok": True,
        "mode": check_mode,
        "ai_used": use_ai,
        "claim": claim,
        "lang": lang,
        "translated_claim": translated,
        "queries": uniq_queries,
        "refresh_info": refresh_info,
        "query_attempts": len(uniq_queries),
        "fetched_count": len(fetched_combined),
        "fetched_rss_count": len(fetched_rss),
        "fetched_live_count": len(fetched_live),
        "live_queries_used": live_queries_run,
        "live_enabled": LIVE_SEARCH_ENABLED,
        "candidate_count": len(ranked),
        "recent_candidate_count": recent_candidate_count,
        "configured_ir_sources": len(NEWS_SOURCE_FEEDS_IR),
        "configured_global_sources": len(NEWS_SOURCE_FEEDS_GLOBAL_ACTIVE),
        "ai_reasoning": ai_reasoning,
        "ai_web": ai_web,
        **scored,
    }


def build_factcheck_report(result: dict[str, Any]) -> str:
    if not result.get("ok", False):
        return f"⛔️ خطا در راستی‌آزمایی: {result.get('error', 'نامشخص')}"

    truth_pct = int(round(float(result.get("truth_prob", 0.5)) * 100))
    fake_pct = int(round(float(result.get("fake_prob", 0.5)) * 100))
    conf_pct = int(round(float(result.get("confidence", 0.2)) * 100))
    claim = str(result.get("claim", ""))[:500]
    translated = str(result.get("translated_claim", "")).strip()
    evidence = list(result.get("evidence", []))
    verdict = str(result.get("verdict", "نامطمئن"))
    mode = normalize_text(str(result.get("mode", "news")))
    ai_used = bool(result.get("ai_used", False))
    reasoning = result.get("ai_reasoning", {}) if isinstance(result.get("ai_reasoning"), dict) else {}
    ai_web = result.get("ai_web", {}) if isinstance(result.get("ai_web"), dict) else {}
    ir_count = int(result.get("configured_ir_sources", len(NEWS_SOURCE_FEEDS_IR)))
    global_count = int(result.get("configured_global_sources", len(NEWS_SOURCE_FEEDS_GLOBAL_ACTIVE)))
    score_reason = normalize_text(str(result.get("score_reason", "")))
    score_why = str(result.get("score_why", "")).strip()
    query_attempts = int(result.get("query_attempts", 0))
    fetched_count = int(result.get("fetched_count", 0))
    fetched_rss_count = int(result.get("fetched_rss_count", 0))
    fetched_live_count = int(result.get("fetched_live_count", 0))
    live_queries_used = int(result.get("live_queries_used", 0))
    live_enabled = bool(result.get("live_enabled", False))
    candidate_count = int(result.get("candidate_count", 0))
    recent_candidate_count = int(result.get("recent_candidate_count", 0))

    lines = [
        "🧪 راستی‌آزمایی خبر",
        f"🔧 حالت بررسی: {'پیشرفته (AI)' if mode == 'pro' else ('خیلی خلاصه' if mode == 'brief' else 'اخبار')}",
        f"🎯 ادعای اصلی: {claim}",
        f"🧭 نتیجه: {verdict}",
        f"• احتمال واقعی بودن: {truth_pct}٪",
        f"• احتمال فیک/گمراه‌کننده بودن: {fake_pct}٪",
        f"• سطح اطمینان تحلیل: {conf_pct}٪",
        (
            f"📚 شواهد: موافق {int(result.get('support_count', 0))} | "
            f"مخالف {int(result.get('refute_count', 0))} | "
            f"مرتبط {int(result.get('related_count', 0))}"
        ),
        f"🌐 تنوع منبع: {int(result.get('source_count', 0))} منبع",
        f"🗂 پایگاه بررسی: {global_count} منبع جهانی + {ir_count} منبع داخلی",
        (
            f"🔍 بازیابی سند: کوئری {query_attempts} | RSS {fetched_rss_count} | "
            f"Live {fetched_live_count} | مجموع {fetched_count}"
        ),
        f"🕒 فیلتر زمانی: {FACTCHECK_RECENT_DAYS} روز اخیر | کاندید مرتبط: {candidate_count} | اخیر: {recent_candidate_count}",
    ]
    if live_enabled:
        lines.append(f"🌍 جست‌وجوی زنده وب: فعال (اجرا شده روی {live_queries_used} کوئری)")
    else:
        lines.append("🌍 جست‌وجوی زنده وب: غیرفعال (فقط ایندکس خبری/آرشیو)")
    if score_why:
        lines.append(f"📌 دلیل امتیاز: {score_why}")
    if score_reason == "no_evidence":
        lines.append("📎 چرا 50/50؟ چون هیچ سند کافی پیدا نشد و نتیجه به‌صورت خنثی/کم‌اطمینان گزارش شده است.")
    if translated:
        lang = str(result.get("lang", "unknown"))
        if lang == "fa":
            lines.append(f"🔄 ترجمه انگلیسی ادعا: {translated[:260]}")
        elif lang == "en":
            lines.append(f"🔄 ترجمه فارسی ادعا: {translated[:260]}")

    if mode == "pro" and reasoning:
        overall = str(reasoning.get("overall", "")).strip()
        why = str(reasoning.get("why", "")).strip()
        missing = str(reasoning.get("missing", "")).strip()
        parts = reasoning.get("parts", []) if isinstance(reasoning.get("parts"), list) else []
        if overall:
            lines.append(f"🧠 تحلیل هوشمند: {overall}")
        if why:
            lines.append(f"• چرا: {why}")
        if parts:
            lines.append("🔬 بررسی جزءبه‌جزء ادعا:")
            status_map = {"true": "✅ درست", "false": "❌ نادرست", "uncertain": "⚪️ نامطمئن"}
            for part in parts[:5]:
                if not isinstance(part, dict):
                    continue
                ptxt = str(part.get("claim_part", "")).strip()
                pwhy = str(part.get("why", "")).strip()
                status = status_map.get(normalize_text(str(part.get("status", "uncertain"))), "⚪️ نامطمئن")
                evid_ids = []
                for eidx in (part.get("evidence") or []):
                    try:
                        evid_ids.append(int(eidx))
                    except Exception:
                        pass
                refs = f" [منابع: {', '.join(str(x) for x in sorted(set(evid_ids)))}]" if evid_ids else ""
                if ptxt:
                    lines.append(f"• {status}: {ptxt}{refs}")
                if pwhy:
                    lines.append(f"  ↳ {pwhy}")
        if missing:
            lines.append(f"🧩 شکاف اطلاعاتی: {missing}")
    elif mode == "pro" and not ai_used:
        lines.append("ℹ️ تحلیل پیشرفته AI فعال نشد (کلید API موجود نیست)؛ خروجی با موتور سندی داخلی تولید شد.")
    if mode == "pro" and ai_used:
        if ai_web:
            aiw_verdict = normalize_text(str(ai_web.get("verdict", "uncertain")))
            aiw_label = {
                "likely_true": "احتمالاً واقعی",
                "likely_false": "احتمالاً فیک/گمراه‌کننده",
                "uncertain": "نامطمئن",
            }.get(aiw_verdict, "نامطمئن")
            aiw_truth = int(round(float(ai_web.get("truth_prob", 0.5)) * 100))
            aiw_conf = int(round(float(ai_web.get("confidence", 0.2)) * 100))
            lines.append(f"🛰 راستی‌آزمایی وب‌محور AI: {aiw_label} | واقعی {aiw_truth}٪ | اطمینان {aiw_conf}٪")
            aiw_why = str(ai_web.get("why", "")).strip()
            if aiw_why:
                lines.append(f"• تحلیل AI: {aiw_why[:260]}")
            checks = ai_web.get("checks") if isinstance(ai_web.get("checks"), list) else []
            if checks:
                lines.append("• نکات تحلیلی AI:")
                for item in checks[:4]:
                    txt = str(item).strip()
                    if txt:
                        lines.append(f"  - {txt[:180]}")
            web_sources = ai_web.get("sources") if isinstance(ai_web.get("sources"), list) else []
            if web_sources:
                lines.append("🌐 منابع استنادی AI (وب):")
                stance_icon = {"support": "✅", "refute": "❌", "related": "➖"}
                for idx, src in enumerate(web_sources[: FACTCHECK_AI_WEBSEARCH_MAX_SOURCES], start=1):
                    if not isinstance(src, dict):
                        continue
                    st = normalize_text(str(src.get("stance", "related")))
                    icon = stance_icon.get(st, "➖")
                    title = str(src.get("title", ""))[:120]
                    pub = str(src.get("publisher", ""))[:50]
                    dt = str(src.get("date", "unknown"))[:16]
                    url = str(src.get("url", ""))[:300]
                    note = str(src.get("note", "")).strip()[:140]
                    lines.append(f"{idx}) {icon} [{pub}] ({dt}) {title}")
                    if note:
                        lines.append(f"  ↳ {note}")
                    if url:
                        lines.append(url)
        else:
            lines.append("🛰 راستی‌آزمایی وب‌محور AI: برای این ادعا سند معتبر کافی برنگرداند.")

    lines.append("🔎 منابع شاخص:")
    label_icon = {"support": "✅", "refute": "❌", "related": "➖", "irrelevant": "▫️"}
    source_limit = 3 if mode == "brief" else 6
    if not evidence:
        lines.append("• سند قابل اتکای مستقیمی پیدا نشد (در این اجرا).")
    for idx, item in enumerate(evidence[:source_limit], start=1):
        label = normalize_text(str(item.get("label", "related")))
        icon = label_icon.get(label, "▫️")
        source = str(item.get("source", "منبع ناشناس"))[:50]
        title = str(item.get("title", ""))[:160]
        link = str(item.get("link", ""))[:300]
        date_text = _fmt_news_date(item.get("published_ts"))
        lines.append(f"{idx}) {icon} [{source}] ({date_text}) {title}")
        if link:
            lines.append(link)

    if mode == "brief":
        lines.append("📝 جمع‌بندی کوتاه: این نتیجه سریع است؛ برای بررسی عمیق از /fact_pro استفاده کن.")

    lines.append("⚠️ این خروجی خودکار است و برای تصمیم حساس باید با منابع رسمی تکمیلی چک شود.")

    # Telegram hard limit guard.
    output = "\n".join(lines)
    while len(output) > 3900 and len(lines) > 14:
        lines.pop(-2 if len(lines) > 2 else -1)
        output = "\n".join(lines)
    return output


def maybe_refresh_news_index_background() -> None:
    try:
        refresh_news_index(force=False)
    except Exception:
        pass


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


def _normalize_channel_ref(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith("@"):
        return v.lower()
    if v.startswith("-100") and v[1:].isdigit():
        return v
    if v.startswith("-") and v[1:].isdigit():
        return v
    if v.isdigit():
        return v
    return "@" + v.lower()


def _channel_keys_from_chat(chat) -> set[str]:
    keys: set[str] = set()
    if chat is None:
        return keys
    cid = getattr(chat, "id", None)
    if cid is not None:
        keys.add(str(int(cid)))
    username = (getattr(chat, "username", "") or "").strip()
    if username:
        keys.add("@" + username.lower().lstrip("@"))
    return keys


def _text_matches_keywords(text: str, keywords: list[str]) -> bool:
    src = normalize_fa_text(text)
    if not src:
        return False
    for kw in keywords:
        needle = normalize_fa_text(kw)
        if needle and needle in src:
            return True
    return False


def log_save_msg_debug(payload: str) -> None:
    if not DEBUG_SAVE_MSG:
        return
    ts = datetime.now(TEHRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[save_msg {ts}] {payload}")


def log_ai_debug(payload: str) -> None:
    if not AI_DEBUG:
        return
    ts = datetime.now(TEHRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ai {ts}] {payload}")


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
        lists = state.setdefault("lists", {})
        active = str(state.get("active_list_id", "")).strip()

        # One-time migration from old single-list structure.
        if not lists:
            default_id = "l1"
            lists[default_id] = {
                "id": default_id,
                "title": "لیست اصلی",
                "created_by": "0",
                "created_at": int(time.time()),
                "participants": dict(state.get("participants", {}) or {}),
                "entries": list(state.get("entries", []) or []),
            }
            state["active_list_id"] = default_id
            active = default_id

        # Ensure list shape.
        for lid, lst in lists.items():
            if not isinstance(lst, dict):
                lists[lid] = {"id": lid, "title": f"لیست {lid}", "created_by": "0", "created_at": int(time.time())}
                lst = lists[lid]
            lst.setdefault("id", lid)
            lst.setdefault("title", f"لیست {lid}")
            lst.setdefault("created_by", "0")
            lst.setdefault("created_at", int(time.time()))
            lst.setdefault("participants", {})
            lst.setdefault("entries", [])

        if not active or active not in lists:
            state["active_list_id"] = next(iter(lists.keys()))
        return state


def _expense_user_name(user) -> str:
    return user.first_name or user.username or str(user.id)


def _new_empty_expense_state() -> dict[str, Any]:
    return {
        "entries": [],
        "participants": {},
        "lists": {
            "l1": {
                "id": "l1",
                "title": "لیست اصلی",
                "created_by": "0",
                "created_at": int(time.time()),
                "participants": {},
                "entries": [],
            }
        },
        "active_list_id": "l1",
    }


def _new_expense_list_id(state: dict[str, Any]) -> str:
    lists = state.get("lists", {})
    max_num = 0
    for lid in lists.keys():
        if isinstance(lid, str) and lid.startswith("l") and lid[1:].isdigit():
            max_num = max(max_num, int(lid[1:]))
    return f"l{max_num + 1}"


def _get_expense_list(state: dict[str, Any], list_id: str | None = None) -> dict[str, Any]:
    lists = state.get("lists", {})
    lid = (list_id or state.get("active_list_id") or "").strip()
    if lid in lists:
        return lists[lid]
    if lists:
        first_key = next(iter(lists.keys()))
        state["active_list_id"] = first_key
        return lists[first_key]
    # Should not happen after get_expense_state, but keep safe fallback.
    new_id = "l1"
    state["lists"] = {
        new_id: {
            "id": new_id,
            "title": "لیست اصلی",
            "created_by": "0",
            "created_at": int(time.time()),
            "participants": {},
            "entries": [],
        }
    }
    state["active_list_id"] = new_id
    return state["lists"][new_id]


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
        GLOBAL_SETTINGS.setdefault("group_ai_daily_limit", 50)
        GLOBAL_SETTINGS.setdefault("group_ai_day", "")
        GLOBAL_SETTINGS.setdefault("group_ai_counts", {})
        GLOBAL_SETTINGS.setdefault("group_ai_max_output_tokens", 1000)
        return GLOBAL_SETTINGS


def _group_ai_usage_state() -> tuple[str, dict[str, int]]:
    today = today_key_tehran()
    cfg = get_global_config()
    should_save = False
    with SETTINGS_LOCK:
        if cfg.get("group_ai_day") != today:
            cfg["group_ai_day"] = today
            cfg["group_ai_counts"] = {}
            should_save = True
        counts = cfg.setdefault("group_ai_counts", {})
    if should_save:
        save_global_settings()
    return today, counts


def get_group_ai_limit(chat_id: int | None = None) -> int:
    gcfg = get_global_config()
    base = int(gcfg.get("group_ai_daily_limit", 50))
    if chat_id is None:
        return max(1, min(500, base))
    cfg = get_group_config(chat_id)
    value = int(cfg.get("ai_daily_limit", base))
    return max(1, min(500, value))


def get_group_ai_usage(chat_id: int) -> tuple[int, int]:
    _, counts = _group_ai_usage_state()
    used = int(counts.get(str(chat_id), 0))
    limit = get_group_ai_limit(chat_id)
    return used, max(1, limit)


def consume_group_ai_usage(chat_id: int) -> tuple[int, int]:
    _, counts = _group_ai_usage_state()
    key = str(chat_id)
    used = 0
    with SETTINGS_LOCK:
        used = int(counts.get(key, 0)) + 1
        counts[key] = used
    save_global_settings()
    limit = get_group_ai_limit(chat_id)
    return used, max(1, limit)


def get_group_ai_output_tokens(chat_id: int | None = None) -> int:
    gcfg = get_global_config()
    base = int(gcfg.get("group_ai_max_output_tokens", 1000))
    if chat_id is None:
        value = base
    else:
        cfg = get_group_config(chat_id)
        value = int(cfg.get("ai_output_tokens", base))
    return max(200, min(2000, value))


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


def _normalize_ai_prompt(prompt: str) -> str:
    text = (prompt or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = text.strip()
    if len(text) > AI_PROMPT_MAX_CHARS:
        return text[:AI_PROMPT_MAX_CHARS].rstrip()
    return text


def _ai_cache_key(
    prompt: str,
    max_output_tokens: int,
    reasoning_effort: str | None = None,
    tools_mode: str = "none",
) -> str:
    payload = "|".join(
        [
            OPENAI_MODEL,
            OPENAI_API_BASE,
            str(max_output_tokens),
            normalize_text(reasoning_effort or ""),
            tools_mode,
            _normalize_ai_prompt(prompt),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()


def _ai_cache_get(key: str) -> str:
    if not AI_CACHE_ENABLED:
        return ""
    now_ts = int(time.time())
    with AI_CACHE_LOCK:
        row = AI_RESPONSE_CACHE.get(key)
        if not row:
            return ""
        expire_ts, value = row
        if expire_ts < now_ts:
            AI_RESPONSE_CACHE.pop(key, None)
            return ""
        return value


def _ai_cache_put(key: str, value: str) -> None:
    if not AI_CACHE_ENABLED or not value.strip():
        return
    now_ts = int(time.time())
    exp = now_ts + AI_CACHE_TTL_SEC
    with AI_CACHE_LOCK:
        AI_RESPONSE_CACHE[key] = (exp, value)
        if len(AI_RESPONSE_CACHE) <= AI_CACHE_MAX_ITEMS:
            return
        # Remove oldest entries first.
        old_items = sorted(AI_RESPONSE_CACHE.items(), key=lambda kv: kv[1][0])[: max(1, AI_CACHE_MAX_ITEMS // 6)]
        for k, _ in old_items:
            AI_RESPONSE_CACHE.pop(k, None)


def _get_langchain_client() -> Any:
    global LANGCHAIN_CLIENT
    if LANGCHAIN_CLIENT is not None:
        return LANGCHAIN_CLIENT
    if not LANGCHAIN_AVAILABLE or not LANGCHAIN_ENABLED or not OPENAI_API_KEY:
        return None
    try:
        LANGCHAIN_CLIENT = ChatOpenAI(
            model=OPENAI_MODEL,
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_API_BASE,
            temperature=0,
            timeout=25,
        )
        return LANGCHAIN_CLIENT
    except Exception as exc:
        log_ai_debug(f"LangChain init failed: {exc}")
        LANGCHAIN_CLIENT = None
        return None


def _call_recommendation_model_langchain(prompt: str, max_output_tokens: int) -> str:
    llm = _get_langchain_client()
    if llm is None or PromptTemplate is None or StrOutputParser is None:
        return ""
    try:
        prompt_tmpl = PromptTemplate.from_template("{input}")
        chain = prompt_tmpl | llm.bind(max_tokens=max_output_tokens) | StrOutputParser()
        out = chain.invoke({"input": prompt})
        if isinstance(out, str):
            return out.strip()
    except Exception as exc:
        log_ai_debug(f"LangChain invoke failed: {exc}")
    return ""


def call_recommendation_model(
    prompt: str,
    max_output_tokens: int = 300,
    reasoning_effort: str | None = None,
) -> str:
    if not OPENAI_API_KEY:
        log_ai_debug("OPENAI_API_KEY is empty; skipping model call.")
        return ""
    clean_prompt = _normalize_ai_prompt(prompt)
    cache_key = _ai_cache_key(clean_prompt, max_output_tokens=max_output_tokens, reasoning_effort=reasoning_effort)
    cached = _ai_cache_get(cache_key)
    if cached:
        return cached

    # For normal text generation/classification flows, use LangChain pipeline first.
    lc_out = _call_recommendation_model_langchain(clean_prompt, max_output_tokens=max_output_tokens)
    if lc_out:
        _ai_cache_put(cache_key, lc_out)
        return lc_out

    url = f"{OPENAI_API_BASE}/responses"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "input": clean_prompt,
        "max_output_tokens": max_output_tokens,
    }
    if reasoning_effort:
        payload["reasoning"] = {"effort": reasoning_effort}
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=25)
        if res.status_code >= 400:
            log_ai_debug(f"OpenAI API HTTP {res.status_code}: {res.text[:500]}")
            return ""
        data = res.json()
        out_text = _extract_responses_output_text(data)
        if out_text:
            _ai_cache_put(cache_key, out_text)
            return out_text

        status = data.get("status")
        err = data.get("error")
        incomplete = data.get("incomplete_details")
        log_ai_debug(
            "OpenAI response had no extractable text. "
            f"status={status} error={err} incomplete={incomplete} keys={list(data.keys())[:14]}"
        )
    except requests.Timeout:
        log_ai_debug("OpenAI API request timed out.")
        return ""
    except requests.RequestException as exc:
        log_ai_debug(f"OpenAI API request error: {exc}")
        return ""
    except Exception as exc:
        log_ai_debug(f"OpenAI response parse error: {exc}")
        return ""
    return ""


def _extract_responses_output_text(data: dict[str, Any]) -> str:
    if not isinstance(data, dict):
        return ""
    text = data.get("output_text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    output = data.get("output")
    if not isinstance(output, list):
        return ""
    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            part_text = part.get("text")
            if isinstance(part_text, str) and part_text.strip():
                chunks.append(part_text.strip())
    return "\n".join(chunks).strip()


def _normalize_fact_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("www."):
        raw = f"https://{raw}"
    if not raw.startswith("http://") and not raw.startswith("https://"):
        return ""
    return _canonical_news_link(raw)


def _ai_factcheck_web_verdict(claim: str, translated_claim: str = "") -> dict[str, Any]:
    if not OPENAI_API_KEY or not FACTCHECK_AI_WEBSEARCH_ENABLED:
        return {}
    source_claim = " ".join((claim or "").split()).strip()[:520]
    if not source_claim:
        return {}
    translated = " ".join((translated_claim or "").split()).strip()[:520]
    prompt = (
        "Strict evidence-first fact-check.\n"
        "Use web_search tool, cite URLs, no speculation.\n"
        "Return ONLY JSON with shape:\n"
        "{"
        "\"verdict\":\"likely_true|likely_false|uncertain\","
        "\"truth_prob\":0-100,"
        "\"confidence\":0-100,"
        "\"why\":\"short Persian explanation\","
        "\"checks\":[\"short Persian bullet\", \"...\"],"
        "\"sources\":["
        "{\"title\":\"...\",\"publisher\":\"...\",\"url\":\"https://...\","
        "\"date\":\"YYYY-MM-DD or unknown\",\"stance\":\"support|refute|related\",\"note\":\"short Persian\"}"
        "]"
        "}\n"
        "Rules: 2-8 sources; prefer official/primary outlets; conflicts=>uncertain; weak evidence=>low confidence.\n\n"
        f"Claim (original): {source_claim}\n"
        f"Claim (translation): {translated if translated else 'N/A'}"
    )
    prompt = _normalize_ai_prompt(prompt)
    cache_key = _ai_cache_key(
        prompt,
        max_output_tokens=1200,
        reasoning_effort="medium",
        tools_mode="web_search",
    )
    cached_raw = _ai_cache_get(cache_key)
    if cached_raw:
        payload_txt = _extract_json_payload(cached_raw)
        if payload_txt:
            try:
                parsed_cached = json.loads(payload_txt)
                if isinstance(parsed_cached, dict):
                    parsed = parsed_cached
                else:
                    parsed = None
            except Exception:
                parsed = None
        else:
            parsed = None
    else:
        parsed = None

    if parsed is None:
        url = f"{OPENAI_API_BASE}/responses"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        payload: dict[str, Any] = {
            "model": OPENAI_MODEL,
            "input": prompt,
            "max_output_tokens": 1200,
            "reasoning": {"effort": "medium"},
            "tools": [{"type": "web_search_preview"}],
        }
        try:
            res = requests.post(url, headers=headers, json=payload, timeout=45)
            if res.status_code >= 400:
                log_ai_debug(f"OpenAI factcheck web HTTP {res.status_code}: {res.text[:500]}")
                return {}
            data = res.json()
        except Exception as exc:
            log_ai_debug(f"OpenAI factcheck web error: {exc}")
            return {}

        raw = _extract_responses_output_text(data)
        if raw:
            _ai_cache_put(cache_key, raw)
        payload_txt = _extract_json_payload(raw)
        if not payload_txt:
            return {}
        try:
            parsed = json.loads(payload_txt)
        except Exception:
            return {}
        if not isinstance(parsed, dict):
            return {}

    verdict = normalize_text(str(parsed.get("verdict", "uncertain")))
    if verdict not in ("likely_true", "likely_false", "uncertain"):
        verdict = "uncertain"
    try:
        truth_prob = int(float(parsed.get("truth_prob", 50)))
    except Exception:
        truth_prob = 50
    try:
        confidence = int(float(parsed.get("confidence", 20)))
    except Exception:
        confidence = 20
    truth_prob = max(1, min(99, truth_prob))
    confidence = max(1, min(99, confidence))
    why = str(parsed.get("why", "")).strip()[:420]
    checks = parsed.get("checks") if isinstance(parsed.get("checks"), list) else []
    clean_checks = [str(x).strip()[:180] for x in checks if str(x).strip()][:6]

    raw_sources = parsed.get("sources") if isinstance(parsed.get("sources"), list) else []
    sources: list[dict[str, Any]] = []
    for item in raw_sources[: FACTCHECK_AI_WEBSEARCH_MAX_SOURCES + 2]:
        if not isinstance(item, dict):
            continue
        link = _normalize_fact_url(str(item.get("url", "")))
        title = _clean_html_text(str(item.get("title", "")))[:170]
        publisher = _clean_html_text(str(item.get("publisher", "")))[:70]
        stance = normalize_text(str(item.get("stance", "related")))
        if stance not in ("support", "refute", "related"):
            stance = "related"
        date_text = normalize_text(str(item.get("date", "unknown")))[:20] or "unknown"
        note = _clean_html_text(str(item.get("note", "")))[:180]
        if not link or not title:
            continue
        sources.append(
            {
                "title": title,
                "publisher": publisher or (_domain_from_url(link) or "unknown"),
                "url": link,
                "date": date_text,
                "stance": stance,
                "note": note,
            }
        )
    # Deduplicate and cap.
    seen_links: set[str] = set()
    final_sources: list[dict[str, Any]] = []
    for src in sources:
        key = normalize_text(src.get("url", ""))
        if not key or key in seen_links:
            continue
        seen_links.add(key)
        final_sources.append(src)
        if len(final_sources) >= FACTCHECK_AI_WEBSEARCH_MAX_SOURCES:
            break

    if len(final_sources) < 2:
        return {}
    return {
        "verdict": verdict,
        "truth_prob": truth_prob / 100.0,
        "fake_prob": 1.0 - (truth_prob / 100.0),
        "confidence": confidence / 100.0,
        "why": why,
        "checks": clean_checks,
        "sources": final_sources,
    }


def _blend_fact_scores(internal: dict[str, Any], ai_web: dict[str, Any]) -> dict[str, Any]:
    if not ai_web:
        return internal
    try:
        ai_truth = float(ai_web.get("truth_prob", 0.5))
        ai_conf = float(ai_web.get("confidence", 0.2))
    except Exception:
        return internal
    if len(ai_web.get("sources") or []) < 2:
        return internal

    base_truth = float(internal.get("truth_prob", 0.5))
    base_conf = float(internal.get("confidence", 0.2))
    blend_weight = 0.22 + (0.28 * max(0.0, min(1.0, ai_conf)))
    merged_truth = ((1.0 - blend_weight) * base_truth) + (blend_weight * ai_truth)
    merged_truth = max(0.02, min(0.98, merged_truth))
    merged_fake = 1.0 - merged_truth
    merged_conf = max(base_conf, (0.75 * base_conf) + (0.25 * ai_conf))
    merged_conf = max(0.10, min(0.98, merged_conf))

    merged_verdict = "نامطمئن"
    if merged_conf >= 0.45:
        if merged_truth >= 0.65:
            merged_verdict = "احتمالاً واقعی"
        elif merged_truth <= 0.35:
            merged_verdict = "احتمالاً فیک/گمراه‌کننده"
        else:
            merged_verdict = "نیازمند بررسی بیشتر"

    out = dict(internal)
    out["truth_prob"] = merged_truth
    out["fake_prob"] = merged_fake
    out["confidence"] = merged_conf
    out["verdict"] = merged_verdict
    base_why = str(out.get("score_why", "")).strip()
    ai_why = str(ai_web.get("why", "")).strip()
    if ai_why:
        if base_why:
            out["score_why"] = f"{base_why} | تحلیل وب‌محور AI: {ai_why}"
        else:
            out["score_why"] = f"تحلیل وب‌محور AI: {ai_why}"
    out["score_reason"] = "blended_with_ai_web"
    return out


AI_SYSTEM_PROMPT = (
    "تو دستیار فارسی هستی. پاسخ‌ها باید کاربردی، روشن و جمع‌وجور باشند. "
    "معمولاً در 4 تا 8 خط جواب بده. فقط اگر کاربر صریحاً جزئیات بیشتر خواست، طولانی‌تر پاسخ بده. "
    "اگر کاربر ادامه مکالمه خواست، در همان مسیر ادامه بده."
)


def _render_ai_prompt(messages: list[dict[str, str]]) -> str:
    lines = [f"System: {AI_SYSTEM_PROMPT}"]
    for msg in messages:
        role = (msg.get("role") or "user").strip().lower()
        text = (msg.get("text") or "").strip()
        if not text:
            continue
        speaker = "User" if role == "user" else "Assistant"
        lines.append(f"{speaker}: {text}")
    lines.append("Assistant:")
    return "\n".join(lines)


def call_ai_chat_model(messages: list[dict[str, str]], max_output_tokens: int | None = None) -> str:
    if not OPENAI_API_KEY:
        return ""
    token_budget = max_output_tokens if max_output_tokens is not None else get_group_ai_output_tokens()
    prompt = _render_ai_prompt(messages)
    return call_recommendation_model(
        prompt,
        max_output_tokens=token_budget,
        reasoning_effort="low",
    )


SUMMARIZER_MAX_INPUT_CHARS = max(1500, int(os.getenv("SUMMARIZER_MAX_INPUT_CHARS", "12000")))
SUMMARIZER_MAX_OUTPUT_CHARS = max(700, int(os.getenv("SUMMARIZER_MAX_OUTPUT_CHARS", "1900")))
SUMMARIZER_TARGET_RATIO = min(0.34, max(0.10, float(os.getenv("SUMMARIZER_TARGET_RATIO", "0.18"))))
SUMMARIZER_MIN_TARGET_CHARS = max(100, int(os.getenv("SUMMARIZER_MIN_TARGET_CHARS", "150")))
SUMMARIZER_MAX_TARGET_CHARS = max(200, int(os.getenv("SUMMARIZER_MAX_TARGET_CHARS", "360")))
SUMMARIZER_EXTRA_STOPWORDS = {
    "اون",
    "خیلی",
    "مال",
    "یه",
    "اینو",
    "اونو",
    "دیگه",
    "همون",
    "اینور",
    "اونور",
    "وقتی",
    "وقت",
    "داریم",
    "داره",
    "میشه",
    "میتونی",
    "میگی",
    "میگه",
    "بودن",
    "بود",
}
SUMMARIZER_PRIORITY_KEYWORDS = (
    "درخواست",
    "همکاری",
    "وارث",
    "حساب",
    "بانک",
    "واریز",
    "سپرده",
    "تراکنش",
    "مبلغ",
    "دلار",
    "یورو",
    "تقسیم",
    "درصد",
    "۵۰",
    "50",
    "محرمانه",
    "ایمیل",
    "شماره",
    "مدیر",
    "عامل",
    "شرکت",
    "تاریخ",
    "مرگ",
    "فوت",
)
SUMMARIZER_REQUEST_KEYWORDS = (
    "درخواست",
    "لطفا",
    "ایمیل بزنید",
    "پاسخ",
    "همکاری",
    "معرفی",
    "ارسال",
    "تماس",
)


def _prepare_summary_text(raw: str) -> str:
    text = (raw or "").replace("\r\n", "\n").replace("\r", "\n")
    if "<" in text and ">" in text:
        try:
            text = BeautifulSoup(text, "lxml").get_text("\n", strip=True)
        except Exception:
            text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text).replace("\u200c", " ")
    lines = []
    for line in text.split("\n"):
        clean = " ".join(line.strip().split())
        if clean:
            lines.append(clean)
    return "\n".join(lines).strip()


def _shrink_clause(text: str, max_chars: int = 165, add_ellipsis: bool = True) -> str:
    value = " ".join((text or "").split()).strip(" \t-•")
    if len(value) <= max_chars:
        return value
    cut = value[:max_chars]
    split_pos = max(cut.rfind("."), cut.rfind("؟"), cut.rfind("?"), cut.rfind("!"), cut.rfind("،"), cut.rfind(","), cut.rfind("؛"), cut.rfind(" "))
    if split_pos >= int(max_chars * 0.55):
        cut = cut[:split_pos]
    cut_clean = cut.rstrip(" ،,:;.!؟?")
    if not cut_clean:
        cut_clean = value[: max(12, min(max_chars, 36))].strip()
    tail = "…" if add_ellipsis and len(cut_clean) < len(value) else ""
    return cut_clean + tail


def _split_sentences(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    out: list[str] = []
    blocks = [b.strip() for b in re.split(r"\n+", raw) if b.strip()]
    for block in blocks:
        normalized = " ".join(block.split())
        if not normalized:
            continue
        # Split by end punctuation first.
        chunks = re.split(r"(?<=[\.\!\?؟؛])\s+", normalized)
        for chunk in chunks:
            part = chunk.strip(" \t-•")
            if not part:
                continue
            # If still too long (common in chat text), split by commas/clauses.
            if len(part) > 230:
                sub_chunks = re.split(
                    r"[،,؛;]\s+|(?<=[😂🤣😅😆😁😄😊🙂🙃😉])\s*",
                    part,
                )
                for sub in sub_chunks:
                    s = sub.strip(" \t-•")
                    if len(s) >= 12:
                        out.append(_shrink_clause(s, max_chars=170))
            else:
                out.append(part)

    merged: list[str] = []
    for part in out:
        if merged and re.match(r"^(ولی|اما|که|و|بنابراین|پس)\b", normalize_fa_text(part)):
            merged[-1] = f"{merged[-1]} {part}".strip()
        else:
            merged.append(part)

    compacted: list[str] = []
    for part in merged:
        if compacted and len(part) < 22:
            compacted[-1] = f"{compacted[-1]} {part}".strip()
        else:
            compacted.append(part)
    return compacted[:90]


def _sentence_tokens(sentence: str, stopwords: set[str]) -> list[str]:
    return [
        t
        for t in _tokenize_fact_text(sentence)
        if len(t) >= 3 and t not in stopwords and t not in SUMMARIZER_EXTRA_STOPWORDS
    ]


def _sentence_signal_score(sentence: str) -> float:
    s = " " + normalize_fa_text(sentence) + " "
    score = 0.0
    priority_hits = sum(1 for k in SUMMARIZER_PRIORITY_KEYWORDS if k in s)
    request_hits = sum(1 for k in SUMMARIZER_REQUEST_KEYWORDS if k in s)
    score += min(0.30, priority_hits * 0.07)
    score += min(0.24, request_hits * 0.08)
    if "?" in sentence or "؟" in sentence:
        score += 0.08
    if re.search(r"\b(\d{1,4}|[۰-۹]{1,4})\b", sentence):
        score += 0.14
    if re.search(r"[\w\.-]+@[\w\.-]+\.\w{2,}", sentence):
        score += 0.22
    if re.search(r"https?://|www\.", sentence):
        score += 0.16
    if re.search(r"\b(آقای|خانم|Mr|Mrs|CEO|مدیر عامل|بانک|شرکت)\b", sentence, flags=re.IGNORECASE):
        score += 0.10
    if len(sentence) > 180:
        score -= 0.06
    return max(0.0, score)


def _extract_sensitive_facts(text: str) -> list[str]:
    src = _prepare_summary_text(text)
    if not src:
        return []
    out: list[str] = []
    emails = re.findall(r"[\w\.-]+@[\w\.-]+\.\w{2,}", src)
    if emails:
        out.append("ایمیل/تماس: " + " | ".join(sorted(set(emails))[:3]))
    amounts = re.findall(
        r"(?:\d[\d,\.]*|[۰-۹][۰-۹,\.]*)\s*(?:دلار|یورو|تومان|ریال|usd|eur|dollar|dollars|percent|٪|%)",
        src,
        flags=re.IGNORECASE,
    )
    if amounts:
        out.append("مبالغ/درصدها: " + " | ".join(sorted(set(amounts))[:4]))
    years = re.findall(r"\b(?:19|20)\d{2}\b|[۱۲۳۴۵۶۷۸۹۰]{4}", src)
    if years:
        out.append("سال/تاریخ: " + " | ".join(sorted(set(years))[:5]))
    if re.search(r"\b(وارث|حساب|بانک|سپرده|تراکنش|تقسیم|محرمانه)\b", normalize_fa_text(src)):
        out.append("موضوع حساس: درخواست مالی/انتقال حساب/محرمانگی")
    return out[:3]


def _is_probable_financial_scam_text(text: str) -> bool:
    s = normalize_fa_text(text or "")
    if not s:
        return False
    flags = 0
    if re.search(r"[\w\.-]+@[\w\.-]+\.\w{2,}", s):
        flags += 1
    if re.search(r"\b(وارث|ارث|حساب|بانک|سپرده|تراکنش|مدیر عامل|ceo)\b", s):
        flags += 1
    if re.search(r"\b(محرمانه|محرمانگی|مخفی|فوری)\b", s):
        flags += 1
    if re.search(r"\b(دلار|یورو|usd|eur|٪|%)\b", s):
        flags += 1
    if re.search(r"\b(تقسیم|پنجاه|50|۵۰)\b", s):
        flags += 1
    if re.search(r"\b(پاسخ|ایمیل بزنید|همکاری|درخواست)\b", s):
        flags += 1
    return flags >= 3


def _build_scam_focused_summary(source: str, sensitive_facts: list[str]) -> str:
    mail = ""
    m = re.search(r"[\w\.-]+@[\w\.-]+\.\w{2,}", source)
    if m:
        mail = m.group(0).strip()
    amount = ""
    m2 = re.search(
        r"((?:\d[\d,\.]*|[۰-۹][۰-۹,\.]*)\s*(?:دلار|یورو|تومان|ریال|usd|eur))",
        source,
        flags=re.IGNORECASE,
    )
    if m2:
        amount = m2.group(1).strip()
    lines = [
        "📝 خلاصه",
        "این متن یک سناریوی رایج کلاهبرداری مالی/ارث است: فرستنده با ادعای مدیر بانکی بودن، وجود حساب راکد و "
        "انتقال پول کلان، از مخاطب می‌خواهد به‌عنوان وارث همکاری کند و بخشی از مبلغ را بگیرد.",
        "",
        "🔹 نکات کلیدی",
        "• ادعای مقام بانکی + درخواست محرمانگی + وعده سود بالا، الگوی کلاسیک اسکم است.",
        "• هدف پیام، کشاندن مخاطب به ادامه تماس و دریافت اطلاعات/اقدام مالی است.",
    ]
    if mail:
        lines.append(f"• مسیر تماس در متن: {mail}")
    if amount:
        lines.append(f"• مبلغ ادعایی: {amount}")
    if sensitive_facts:
        lines += ["", "📎 فکت‌های مهم"]
        for f in sensitive_facts[:2]:
            lines.append(f"• {f}")
    return "\n".join(lines).strip()


def _dedupe_summary_points(summary: str, points: list[str]) -> list[str]:
    out: list[str] = []
    for p in points:
        pp = (p or "").strip()
        if not pp:
            continue
        if _text_similarity(summary, pp) >= 0.74:
            continue
        if any(_text_similarity(pp, existing) >= 0.86 for existing in out):
            continue
        out.append(pp)
    return out


def _extract_ranked_points(sentences: list[str]) -> list[str]:
    ranked: list[tuple[int, str]] = []
    rank_map = {"اول": 1, "دوم": 2, "سوم": 3, "چهارم": 4, "پنجم": 5}
    for s in sentences:
        norm = normalize_fa_text(s)
        m = re.search(r"مقام\s*(اول|دوم|سوم|چهارم|پنجم)", norm)
        if not m:
            continue
        rank = rank_map.get(m.group(1), 99)
        txt = _shrink_clause(s, max_chars=128, add_ellipsis=False)
        if txt:
            ranked.append((rank, txt))
    ranked.sort(key=lambda x: x[0])
    out: list[str] = []
    for _, txt in ranked:
        if any(_text_similarity(txt, ex) >= 0.88 for ex in out):
            continue
        out.append(txt)
    return out[:3]


def _render_summary_sections(
    summary: str,
    points: list[str],
    key_terms: list[str],
    sensitive_facts: list[str],
    source_len: int,
) -> str:
    compact_mode = source_len < 720 and not sensitive_facts
    points_cap = 2 if compact_mode else 3
    points = _dedupe_summary_points(summary, points)[:points_cap]
    show_keywords = bool(key_terms) and not compact_mode and source_len >= 900

    # Keep total output shorter than input (with safety margin).
    budget = max(140, int(source_len * 0.50))
    if source_len < 360:
        budget = max(110, int(source_len * 0.60))

    while True:
        lines = ["📝 خلاصه", summary.strip()]
        if points:
            lines += ["", "🔹 نکات کلیدی"]
            lines.extend([f"• {p}" for p in points])
        if sensitive_facts:
            lines += ["", "📎 فکت‌های مهم"]
            lines.extend([f"• {f}" for f in sensitive_facts[:3]])
        if show_keywords and key_terms:
            lines += ["", "🏷 کلیدواژه‌ها: " + " | ".join(key_terms[:5])]
        body = "\n".join(lines).strip()
        if len(body) <= budget:
            return body

        if show_keywords:
            show_keywords = False
            continue
        if len(points) > 1:
            points = points[:-1]
            continue
        # Last-resort tightening: aggressively shrink summary sentence.
        tighter = max(80, min(170, int(budget * 0.48)))
        new_summary = _shrink_clause(summary, max_chars=tighter)
        if new_summary == summary:
            safe_summary = _shrink_clause(summary, max_chars=max(72, min(150, int(budget * 0.55))))
            return "\n".join(["📝 خلاصه", safe_summary]).strip()
        summary = new_summary


def _normalize_score_map(scores: dict[int, float]) -> dict[int, float]:
    if not scores:
        return {}
    vals = list(scores.values())
    mn = min(vals)
    mx = max(vals)
    if abs(mx - mn) < 1e-9:
        return {k: 1.0 for k in scores}
    return {k: (v - mn) / (mx - mn) for k, v in scores.items()}


def _textrank_sentence_scores(sentences: list[str], sent_tokens: list[list[str]]) -> dict[int, float]:
    n = len(sentences)
    if n == 0:
        return {}
    if n == 1:
        return {0: 1.0}
    edges: list[dict[int, float]] = [dict() for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            sim = _text_similarity(sentences[i], sentences[j])
            if sim < 0.10:
                continue
            len_i = max(1, len(sent_tokens[i]))
            len_j = max(1, len(sent_tokens[j]))
            len_factor = 2.0 / (sqrt(len_i) + sqrt(len_j))
            w = max(0.0, sim * len_factor)
            if w <= 0:
                continue
            edges[i][j] = w
            edges[j][i] = w

    ranks = [1.0 / n for _ in range(n)]
    damping = 0.85
    for _ in range(24):
        new_ranks = [(1.0 - damping) / n for _ in range(n)]
        for i in range(n):
            out_sum = sum(edges[i].values())
            if out_sum <= 1e-9:
                continue
            for j, w in edges[i].items():
                new_ranks[j] += damping * ranks[i] * (w / out_sum)
        ranks = new_ranks
    return _normalize_score_map({i: ranks[i] for i in range(n)})


def _choose_summary_indexes(
    sentences: list[str],
    final_scores: dict[int, float],
    target_chars: int,
    keep_cap: int,
) -> list[int]:
    if not sentences:
        return []
    candidates = sorted(final_scores.keys(), key=lambda i: final_scores.get(i, 0.0), reverse=True)
    selected: list[int] = []
    consumed = 0
    while len(selected) < keep_cap and candidates:
        best_idx = None
        best_val = -1e9
        for idx in candidates:
            if idx in selected:
                continue
            rel = final_scores.get(idx, 0.0)
            redundancy = 0.0
            if selected:
                redundancy = max(_text_similarity(sentences[idx], sentences[s]) for s in selected)
            mmr = (0.78 * rel) - (0.22 * redundancy)
            if mmr > best_val:
                best_val = mmr
                best_idx = idx
        if best_idx is None:
            break
        est_len = len(_shrink_clause(sentences[best_idx], max_chars=130)) + 1
        # Keep at least 2 sentences even if over budget.
        if selected and (consumed + est_len > target_chars) and len(selected) >= 2:
            break
        selected.append(best_idx)
        consumed += est_len
        candidates.remove(best_idx)
    return sorted(set(selected))


def _extractive_summary_local(text: str, max_sentences: int = 5, max_points: int = 5) -> str:
    source = _prepare_summary_text(text or "")
    if not source:
        return "متنی برای خلاصه‌سازی پیدا نشد."

    sentences = _split_sentences(source)
    if not sentences:
        clipped = source[:420].rstrip()
        return f"خلاصه:\n{clipped}"

    if len(sentences) == 1:
        single = _shrink_clause(sentences[0], max_chars=170)
        return f"📝 خلاصه\n{single}\n\n🔹 نکات کلیدی\n• {single}"

    ranked_points = _extract_ranked_points(sentences)
    if len(ranked_points) >= 2:
        intro = ""
        for s in sentences[:8]:
            if ("?" in s or "؟" in s) and len(normalize_fa_text(s)) >= 20:
                intro = _shrink_clause(s, max_chars=110, add_ellipsis=False)
                break
        summary_line = "متن با لحن طنز، درباره بحث گروهی و میزان حوصله/کیفیت پاسخ افراد مقایسه و رتبه‌بندی می‌کند."
        lines = ["📝 خلاصه", summary_line, "", "🔹 نکات کلیدی"]
        if intro:
            lines.append(f"• محور بحث: {intro}")
        for p in ranked_points:
            lines.append(f"• {p}")
        body = "\n".join(lines).strip()
        src_len = len(source)
        budget = max(170, int(src_len * 0.56))
        if len(body) > budget:
            # Keep summary meaningful, then trim extra points from the end.
            while len(lines) > 6 and len("\n".join(lines).strip()) > budget:
                lines.pop()
            body = "\n".join(lines).strip()
        return body

    lang = _guess_news_lang(source)
    stopwords = FACTCHECK_FA_STOPWORDS if lang == "fa" else FACTCHECK_EN_STOPWORDS
    token_freq: Counter[str] = Counter()
    sent_tokens: list[list[str]] = []
    signal_scores: dict[int, float] = {}
    for s in sentences:
        toks = _sentence_tokens(s, stopwords)
        sent_tokens.append(toks)
        token_freq.update(toks)

    base_scores: dict[int, float] = {}
    for idx, toks in enumerate(sent_tokens):
        if not toks:
            base_scores[idx] = 0.0
            continue
        base = sum(token_freq.get(t, 0) for t in toks) / max(1.0, len(toks) ** 0.65)
        num_boost = 1.12 if re.search(r"\d", sentences[idx]) else 1.0
        name_boost = 1.08 if len(re.findall(r"[A-Za-z\u0600-\u06FF]{3,}", sentences[idx])) >= 5 else 1.0
        list_boost = 1.22 if re.search(r"\b(مقام|اول|دوم|سوم|چهارم|پنجم)\b", normalize_fa_text(sentences[idx])) else 1.0
        question_boost = 1.10 if ("?" in sentences[idx] or "؟" in sentences[idx]) else 1.0
        pos_boost = 0.98 if idx == 0 else (0.92 if idx <= 2 else 0.80)
        len_penalty = 0.88 if len(sentences[idx]) > 190 else 1.0
        base_scores[idx] = base * num_boost * name_boost * list_boost * question_boost * pos_boost * len_penalty
        signal_scores[idx] = _sentence_signal_score(sentences[idx])

    # Keep list-like conversational structure when present (e.g., مقام اول/دوم/سوم).
    list_like = [i for i, s in enumerate(sentences) if re.search(r"\b(مقام|اول|دوم|سوم|چهارم|پنجم)\b", normalize_fa_text(s))]
    question_like = [i for i, s in enumerate(sentences) if ("?" in s or "؟" in s)]
    base_norm = _normalize_score_map(base_scores)
    tr_norm = _textrank_sentence_scores(sentences, sent_tokens)
    sig_norm = _normalize_score_map(signal_scores)
    final_scores: dict[int, float] = {}
    for idx in range(len(sentences)):
        list_hint = 0.12 if idx in list_like else 0.0
        final_scores[idx] = (
            (0.52 * base_norm.get(idx, 0.0))
            + (0.28 * tr_norm.get(idx, 0.0))
            + (0.20 * sig_norm.get(idx, 0.0))
            + list_hint
        )

    if len(sentences) >= 8:
        keep_n = min(max_sentences, 4)
    else:
        keep_n = min(max_sentences, 3)
    keep_n = max(2, keep_n)

    target_chars = int(len(source) * SUMMARIZER_TARGET_RATIO)
    target_chars = max(SUMMARIZER_MIN_TARGET_CHARS, min(SUMMARIZER_MAX_TARGET_CHARS, target_chars))
    ordered_idx = _choose_summary_indexes(
        sentences=sentences,
        final_scores=final_scores,
        target_chars=target_chars,
        keep_cap=keep_n,
    )
    if list_like:
        for idx in list_like[:2]:
            if idx in ordered_idx:
                continue
            if len(ordered_idx) < keep_n:
                ordered_idx.append(idx)
        ordered_idx = sorted(set(ordered_idx))
    if question_like and not any(i in ordered_idx for i in question_like) and len(ordered_idx) < keep_n:
        qidx = question_like[0]
        ordered_idx.append(qidx)
        ordered_idx = sorted(set(ordered_idx))
    # Preserve at least one highly-signaled factual sentence.
    if signal_scores:
        top_signal = sorted(signal_scores.keys(), key=lambda i: signal_scores.get(i, 0.0), reverse=True)
        for idx in top_signal[:1]:
            if signal_scores.get(idx, 0.0) < 0.22:
                continue
            if idx not in ordered_idx and len(ordered_idx) < keep_n:
                ordered_idx.append(idx)
        ordered_idx = sorted(set(ordered_idx))

    summary_chunks = [_shrink_clause(sentences[i], max_chars=92) for i in ordered_idx[:3]]
    summary = " ".join([c for c in summary_chunks if c]).strip()
    summary = summary[: max(160, target_chars + 28)].rstrip()

    point_limit = min(max_points, len(ordered_idx))
    points = [_shrink_clause(sentences[i], max_chars=88) for i in ordered_idx[: min(point_limit, 3)]]
    if len(points) < 2 and ranked_points:
        for rp in ranked_points:
            if len(points) >= 2:
                break
            if any(_text_similarity(rp, p) >= 0.86 for p in points):
                continue
            points.append(rp)
    selected_freq: Counter[str] = Counter()
    for i in ordered_idx:
        selected_freq.update(sent_tokens[i])
    key_terms = [
        k
        for k, v in selected_freq.most_common(12)
        if len(k) >= 3 and v >= 1 and k not in SUMMARIZER_EXTRA_STOPWORDS and k not in stopwords
    ][:5]
    sensitive_facts = _extract_sensitive_facts(source)
    if _is_probable_financial_scam_text(source):
        return _build_scam_focused_summary(source, sensitive_facts)
    return _render_summary_sections(
        summary=summary,
        points=points,
        key_terms=key_terms,
        sensitive_facts=sensitive_facts,
        source_len=len(source),
    )


def _extract_summarize_input_text(message) -> str:
    reply = getattr(message, "reply_to_message", None)
    reply_text = _extract_message_text(reply) if reply is not None else ""
    if reply_text and reply_text.strip():
        return reply_text.strip()
    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) >= 2:
        return parts[1].strip()
    return ""


def run_text_summarizer(message, source_text: str) -> str:
    source = _prepare_summary_text(source_text or "")
    if not source:
        return "⛔️ متن قابل خلاصه‌سازی پیدا نشد."
    if len(source) > SUMMARIZER_MAX_INPUT_CHARS:
        source = source[:SUMMARIZER_MAX_INPUT_CHARS].rstrip() + "..."

    if is_group_chat(message):
        cfg = get_group_config(message.chat.id)
        if not cfg.get("ai_enabled", True):
            return "خلاصه‌ساز داخلی برای این گروه خاموش است."

    out = _extractive_summary_local(source)
    out = _normalize_ai_output(out, max_chars=SUMMARIZER_MAX_OUTPUT_CHARS)
    lines = ["🧠 خلاصه‌ساز داخلی", out]
    return "\n\n".join(x for x in lines if x).strip()


def _normalize_ai_output(text: str, max_chars: int = 1100) -> str:
    out = (text or "").strip()
    if not out:
        return out
    if len(out) > max_chars:
        out = out[: max_chars - 1].rstrip() + "…"
    return out


def _strip_ai_prefix(text: str) -> str:
    value = (text or "").strip()
    prefixes = ("🤖 پاسخ هوش مصنوعی", "🤖")
    for p in prefixes:
        if value.startswith(p):
            value = value[len(p) :].strip(" \n:-")
    return value.strip()


def _thread_key(chat_id: int, root_msg_id: int) -> str:
    return f"{int(chat_id)}:{int(root_msg_id)}"


def _trim_ai_thread(messages: list[dict[str, str]], max_items: int = 14) -> list[dict[str, str]]:
    if len(messages) <= max_items:
        return messages
    return messages[-max_items:]


def _is_reply_to_ai_message(message) -> bool:
    reply = getattr(message, "reply_to_message", None)
    if not reply:
        return False
    key = f"{int(message.chat.id)}:{int(reply.message_id)}"
    if key in AI_MESSAGE_TO_THREAD:
        return True
    if not is_reply_to_this_bot(message):
        return False
    return _extract_message_text(reply).strip().startswith("🤖 پاسخ هوش مصنوعی")


def _continue_thread_from_reply(message) -> tuple[str | None, list[dict[str, str]]]:
    reply = getattr(message, "reply_to_message", None)
    if not reply:
        return None, []
    key = f"{int(message.chat.id)}:{int(reply.message_id)}"
    thread_id = AI_MESSAGE_TO_THREAD.get(key)
    if thread_id and thread_id in AI_THREADS:
        return thread_id, list(AI_THREADS.get(thread_id, []))

    if not is_reply_to_this_bot(message):
        return None, []
    reply_text = _extract_message_text(reply)
    clean_assistant = _strip_ai_prefix(reply_text)
    if not clean_assistant:
        return None, []
    fallback_thread = _thread_key(message.chat.id, reply.message_id)
    return fallback_thread, [{"role": "assistant", "text": clean_assistant}]


def _send_ai_reply(message, text: str, footer: str = ""):
    suffix = f"\n\n{footer.strip()}" if footer.strip() else ""
    return bot.reply_to(message, f"🤖 پاسخ هوش مصنوعی\n\n{text}{suffix}")


def run_ai_chat(message, user_text: str, force_new: bool = False) -> bool:
    prompt = (user_text or "").strip()
    if not prompt:
        return False

    group_used = 0
    group_limit = 0
    group_cfg: dict[str, Any] | None = None
    if is_group_chat(message):
        group_cfg = get_group_config(message.chat.id)
        if not group_cfg.get("ai_enabled", True):
            bot.reply_to(message, "چت هوش مصنوعی برای این گروه خاموش است.")
            return True
        group_used, group_limit = get_group_ai_usage(message.chat.id)
        if group_used >= group_limit:
            bot.reply_to(
                message,
                f"⛔️ سهمیه روزانه چت AI این گروه پر شده ({group_used}/{group_limit}). فردا دوباره فعال می‌شود.",
            )
            return True

    thread_id: str
    history: list[dict[str, str]]
    if force_new:
        thread_id = _thread_key(message.chat.id, message.message_id)
        history = []
    else:
        reply_thread, reply_history = _continue_thread_from_reply(message)
        if reply_thread:
            thread_id = reply_thread
            history = reply_history
        else:
            last_key = str(message.chat.id)
            last_thread = AI_LAST_THREAD_BY_CHAT.get(last_key)
            if last_thread and last_thread in AI_THREADS and not is_group_chat(message):
                thread_id = last_thread
                history = list(AI_THREADS.get(thread_id, []))
            else:
                thread_id = _thread_key(message.chat.id, message.message_id)
                history = []

    history.append({"role": "user", "text": prompt})
    history = _trim_ai_thread(history)
    token_budget = get_group_ai_output_tokens(message.chat.id if is_group_chat(message) else None)
    answer = _normalize_ai_output(call_ai_chat_model(history, max_output_tokens=token_budget))
    if not answer:
        retry_history = _trim_ai_thread(history, max_items=6)
        retry_budget = min(2000, token_budget + 200)
        answer = _normalize_ai_output(call_ai_chat_model(retry_history, max_output_tokens=retry_budget))
    if not answer:
        log_ai_debug(
            "AI chat returned empty output "
            f"(chat_id={message.chat.id}, user_id={message.from_user.id}, "
            f"is_group={is_group_chat(message)}, prompt_len={len(prompt)})"
        )
        bot.reply_to(message, "پاسخ AI موقتاً در دسترس نیست. چند لحظه بعد دوباره تلاش کن.")
        return True

    history.append({"role": "assistant", "text": answer})
    history = _trim_ai_thread(history)
    footer = ""
    if is_group_chat(message):
        remaining = max(0, group_limit - (group_used + 1))
        footer = f"📊 باقی‌مانده سهمیه AI امروز گروه: {remaining}/{group_limit}"
    sent = _send_ai_reply(message, answer, footer=footer)

    AI_THREADS[thread_id] = history
    AI_LAST_THREAD_BY_CHAT[str(message.chat.id)] = thread_id
    AI_MESSAGE_TO_THREAD[f"{int(message.chat.id)}:{int(sent.message_id)}"] = thread_id
    if is_group_chat(message):
        consume_group_ai_usage(message.chat.id)
    return True


def maybe_handle_ai_text_message(message) -> bool:
    text = (message.text or "").strip()
    if not text:
        return False
    group_cfg: dict[str, Any] | None = None
    if is_group_chat(message):
        group_cfg = get_group_config(message.chat.id)
        if not group_cfg.get("ai_enabled", True):
            return False

    if _is_reply_to_ai_message(message):
        return run_ai_chat(message, text, force_new=False)

    if is_group_chat(message) and bool((group_cfg or {}).get("ai_reply_only", False)):
        return False

    normalized = normalize_fa_text(text)
    if normalized.startswith("هوش "):
        payload = text[4:].strip()
        if not payload:
            bot.reply_to(message, "بعد از «هوش» سوالت را بنویس. مثال: هوش یک فیلم معمایی معرفی کن")
            return True
        return run_ai_chat(message, payload, force_new=True)
    if normalize_text(text).startswith("ai "):
        payload = text[3:].strip()
        if not payload:
            bot.reply_to(message, "بعد از ai سوالت را بنویس. مثال: ai یک کتاب کوتاه معرفی کن")
            return True
        return run_ai_chat(message, payload, force_new=True)
    return False


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
    kb.add(types.KeyboardButton("🤖 چت هوش مصنوعی"))
    kb.add(types.KeyboardButton("⚙️ راهنما"), types.KeyboardButton("📘 راهنمای کامل"))
    if is_group:
        kb.add(types.KeyboardButton("⚙️ تنظیمات گروه"), types.KeyboardButton("🚀 ارسال فوری"))
    return kb


def expense_panel_text(chat_id: int) -> str:
    state = get_expense_state(chat_id)
    current = _get_expense_list(state)
    entries = current.get("entries", [])
    total = sum(float(x.get("amount", 0)) for x in entries)
    participants = current.get("participants", {})
    list_id = current.get("id", "-")
    title = current.get("title", "لیست")
    lists_count = len(state.get("lists", {}))
    return (
        "💰 پنل دنگ و خرج گروه (لیست‌محور)\n"
        f"• لیست فعال: {title} ({list_id})\n"
        f"• تعداد لیست‌ها: {lists_count}\n"
        f"• تعداد خرج‌ها: {len(entries)}\n"
        f"• مجموع خرج: {total:g}\n"
        f"• اعضای دنگ این لیست: {len(participants)}\n\n"
        "ثبت خرج در لیست فعال:\n"
        "/add 480 پیتزا\n"
        "ساخت لیست جدید:\n"
        "/list_new خرید آخر هفته"
    )


def expense_panel_markup(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🆕 لیست جدید", callback_data=f"ex:new:{chat_id}"),
        types.InlineKeyboardButton("🔁 لیست بعدی", callback_data=f"ex:next:{chat_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("➕ عضویت در دنگ", callback_data=f"ex:join:{chat_id}"),
        types.InlineKeyboardButton("👥 اعضای لیست", callback_data=f"ex:members:{chat_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("🧾 خرج‌های لیست", callback_data=f"ex:show:{chat_id}"),
        types.InlineKeyboardButton("💳 تسویه لیست", callback_data=f"ex:split:{chat_id}"),
    )
    kb.add(
        types.InlineKeyboardButton("🗑 پاکسازی لیست فعال", callback_data=f"ex:clear:{chat_id}"),
        types.InlineKeyboardButton("🔄 تازه سازی", callback_data=f"ex:refresh:{chat_id}"),
    )
    kb.add(types.InlineKeyboardButton("❌ بستن پنل", callback_data=f"ex:close:{chat_id}"))
    return kb


def _state_dot(enabled: bool) -> str:
    return "🟢" if enabled else "⚫"


def _group_market_settings_text(cfg: dict[str, Any], chat_id: int) -> str:
    return (
        "╭─────────────── ✦\n"
        "│  💱 تنظیمات بازار و ارسال\n"
        "├───────────────\n"
        f"│  {_state_dot(cfg.get('enabled', True))} ربات: {'فعال' if cfg.get('enabled', True) else 'غیرفعال'}\n"
        f"│  {_state_dot(cfg.get('auto_enabled', False))} ارسال خودکار: {'روشن' if cfg.get('auto_enabled', False) else 'خاموش'}\n"
        f"│  {_state_dot(cfg.get('show_percent', True))} نمایش درصد تغییر: {'روشن' if cfg.get('show_percent', True) else 'خاموش'}\n"
        f"│  {_state_dot(cfg.get('include_crypto', False))} کریپتو: {'روشن' if cfg.get('include_crypto', False) else 'خاموش'}\n"
        f"│  {_state_dot(cfg.get('pin_report', False))} پین گزارش: {'روشن' if cfg.get('pin_report', False) else 'خاموش'}\n"
        f"│  {_state_dot(cfg.get('mention_enabled', True))} پاسخ منشن: {'روشن' if cfg.get('mention_enabled', True) else 'خاموش'}\n"
        f"│  {_state_dot(cfg.get('silent', False))} بی‌صدا: {'روشن' if cfg.get('silent', False) else 'خاموش'}\n"
        f"│  ⏳ بازه ارسال: هر {cfg.get('interval_min', 30)} دقیقه\n"
        f"│  🧊 کول‌داون تریگر: {cfg.get('cooldown_sec', 20)} ثانیه\n"
        f"│  🎯 تریگر: {cfg.get('trigger', '!prices')}\n"
        f"│  🏷 عنوان گزارش: {cfg.get('title', '📊 گزارش قیمت')}\n"
        "│  ✏️ تغییر تریگر: /set_group_cmd !yourcmd\n"
        "│  ✏️ تغییر عنوان: /set_title متن دلخواه\n"
        "╰─────────────── ✦"
    )


def _group_ai_settings_text(chat_id: int) -> str:
    cfg = get_group_config(chat_id)
    used, limit = get_group_ai_usage(chat_id)
    remaining = max(0, limit - used)
    out_tokens = get_group_ai_output_tokens(chat_id)
    return (
        "╭─────────────── ✦\n"
        "│  🤖 تنظیمات هوش مصنوعی گروه\n"
        "├───────────────\n"
        f"│  {_state_dot(cfg.get('ai_enabled', True))} AI گروه: {'فعال' if cfg.get('ai_enabled', True) else 'غیرفعال'}\n"
        f"│  {_state_dot(cfg.get('ai_reply_only', False))} فقط با ریپلای: {'روشن' if cfg.get('ai_reply_only', False) else 'خاموش'}\n"
        f"│  📊 مصرف امروز: {used}/{limit}\n"
        f"│  📉 باقی‌مانده امروز: {remaining}\n"
        f"│  🧠 سقف خروجی هر پاسخ: {out_tokens} توکن\n"
        "╰─────────────── ✦"
    )


def _group_data_settings_text(chat_id: int) -> str:
    scores = get_score_map(chat_id)
    expenses = get_expense_state(chat_id)
    archive = get_archive_map(chat_id)
    return (
        "╭─────────────── ✦\n"
        "│  🗂 مدیریت داده‌های گروه\n"
        "├───────────────\n"
        f"│  🎭 تعداد رکورد امتیاز: {len(scores)}\n"
        f"│  💰 تعداد خرج‌ها: {len(expenses.get('entries', []))}\n"
        f"│  💳 اعضای تسویه: {len(expenses.get('participants', {}))}\n"
        f"│  📂 کلیدهای آرشیو: {len(archive)}\n"
        "│\n"
        "│  ریست هر بخش فقط همان بخش را پاک می‌کند.\n"
        "│  ریست کامل، تنظیمات بازار/AI را به پیش‌فرض برمی‌گرداند.\n"
        "╰─────────────── ✦"
    )


def _group_settings_page_text(cfg: dict[str, Any], chat_id: int, page: str) -> str:
    if page == "market":
        return _group_market_settings_text(cfg, chat_id)
    if page == "ai":
        return _group_ai_settings_text(chat_id)
    if page == "data":
        return _group_data_settings_text(chat_id)
    if page == "reco":
        return reco_settings_text(chat_id)
    return group_settings_text(cfg, chat_id=chat_id)


def _group_settings_markup(cfg: dict[str, Any], chat_id: int, page: str = "main"):
    reco = get_reco_config(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    if page == "ai":
        used, limit = get_group_ai_usage(chat_id)
        out_tokens = get_group_ai_output_tokens(chat_id)
        kb.add(
            types.InlineKeyboardButton(
                f"{_state_dot(cfg.get('ai_enabled', True))} AI گروه",
                callback_data="gs:ai:toggle_enabled",
            ),
            types.InlineKeyboardButton(
                f"{_state_dot(cfg.get('ai_reply_only', False))} فقط ریپلای",
                callback_data="gs:ai:toggle_reply_only",
            ),
        )
        kb.add(
            types.InlineKeyboardButton(
                f"⏪ لیمیت -5 ({limit})",
                callback_data="gs:ai:limit_minus",
            ),
            types.InlineKeyboardButton("⏩ لیمیت +5", callback_data="gs:ai:limit_plus"),
        )
        kb.add(
            types.InlineKeyboardButton(
                f"⏪ خروجی -100 ({out_tokens})",
                callback_data="gs:ai:output_minus",
            ),
            types.InlineKeyboardButton("⏩ خروجی +100", callback_data="gs:ai:output_plus"),
        )
        kb.add(
            types.InlineKeyboardButton(f"🧮 ریست مصرف ({used})", callback_data="gs:ai:reset_usage"),
        )
        kb.add(
            types.InlineKeyboardButton("⬅️ بازگشت", callback_data="gs:page:main"),
            types.InlineKeyboardButton("❌ بستن پنل", callback_data="gs:close"),
        )
        return kb

    if page == "data":
        kb.add(
            types.InlineKeyboardButton("🧹 ریست امتیازها", callback_data="gs:data:reset_scores"),
            types.InlineKeyboardButton("🧾 ریست خرج/دنگ", callback_data="gs:data:reset_expenses"),
        )
        kb.add(types.InlineKeyboardButton("📂 ریست آرشیو", callback_data="gs:data:reset_archive"))
        kb.add(
            types.InlineKeyboardButton("♻️ ریست کامل تنظیمات", callback_data="gs:data:reset_all"),
        )
        kb.add(
            types.InlineKeyboardButton("⬅️ بازگشت", callback_data="gs:page:main"),
            types.InlineKeyboardButton("❌ بستن پنل", callback_data="gs:close"),
        )
        return kb

    if page == "market":
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
                f"{_state_dot(cfg.get('show_percent', True))} درصد تغییر",
                callback_data="gs:t:percent",
            ),
            types.InlineKeyboardButton(
                f"{_state_dot(cfg.get('include_crypto', False))} کریپتو",
                callback_data="gs:t:crypto",
            ),
        )
        kb.add(
            types.InlineKeyboardButton(
                f"{_state_dot(cfg.get('pin_report', False))} پین گزارش", callback_data="gs:t:pin"
            ),
            types.InlineKeyboardButton(
                f"{_state_dot(cfg.get('mention_enabled', True))} پاسخ منشن",
                callback_data="gs:t:mention",
            ),
        )
        kb.add(
            types.InlineKeyboardButton(
                f"{_state_dot(cfg.get('silent', False))} بی‌صدا", callback_data="gs:t:silent"
            ),
            types.InlineKeyboardButton("🚀 ارسال فوری", callback_data="gs:send_now"),
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
            types.InlineKeyboardButton("⬅️ بازگشت", callback_data="gs:page:main"),
            types.InlineKeyboardButton("❌ بستن پنل", callback_data="gs:close"),
        )
        return kb

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
        types.InlineKeyboardButton("💱 بازار و ارسال", callback_data="gs:page:market"),
        types.InlineKeyboardButton("🎬 پیشنهاد روزانه", callback_data="gs:page:reco"),
    )
    kb.add(
        types.InlineKeyboardButton("🤖 هوش مصنوعی گروه", callback_data="gs:page:ai"),
        types.InlineKeyboardButton("🗂 داده‌ها و ریست", callback_data="gs:page:data"),
    )
    kb.add(types.InlineKeyboardButton("❌ بستن پنل", callback_data="gs:close"))
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
        "🤖 راهنمای کامل دستورات ربات\n\n"
        "📌 عمومی:\n"
        "/start\n"
        "/help | /menu\n"
        "/guide | /full_guide | /group_guide\n\n"
        "📈 بازار ارز/سکه/طلا/کریپتو:\n"
        "/prices (خصوصی)\n"
        "/gprices (گروه)\n"
        "/top_changes\n"
        "تریگر گروهی (پیش‌فرض): !prices\n\n"
        "🤖 چت هوش مصنوعی:\n"
        "/ai سوال | /ask سوال\n"
        "/ai_new سوال | /asknew سوال\n"
        "/ai_usage (گروه)\n"
        "ادامه گفتگو: روی پاسخ AI ریپلای کن\n\n"
        "🧠 خلاصه‌سازی متن (کاملا داخلی، بدون API):\n"
        "/summarize متن | /sum متن | /tldr متن | /tl_dr متن | /kholase متن\n"
        "یا روی پیام طولانی ریپلای کن و /summarize بزن\n\n"
        "🧪 راستی‌آزمایی خبر:\n"
        "/fact_news متن | /factcheck متن | /verify_news متن | /newscheck متن\n"
        "/cred_short متن | /verify_short متن | /check_short متن\n"
        "/fact_pro متن | /factcheck_pro متن | /verify_claim متن\n"
        "یا روی خبر ریپلای کن و یکی از دستورات بالا را بزن\n"
        "موتور بررسی: RSS + جست‌وجوی زنده وب + امتیازدهی سندمحور\n\n"
        "🎯 پیشنهاد محتوا:\n"
        "/recommend_me (خصوصی)\n"
        "/reco_on | /reco_off (گروه)\n"
        "/set_reco_time 21:30 (گروه)\n"
        "/send_reco_now (گروه)\n\n"
        "🎭 امتیاز شوخی گروه:\n"
        "روی پیام شخص ریپلای کن: «کسشر شناسایی شد» یا «جمله طلایی»\n"
        "/scoreboard\n"
        "/my_score\n\n"
        "💰 خرج و دنگ گروهی:\n"
        "/add 480 پیتزا\n"
        "/list_new عنوان\n"
        "/list_use l2\n"
        "/lists\n"
        "/join_split\n"
        "/list_add_member (روی پیام کاربر ریپلای)\n"
        "/list_remove_member (روی پیام کاربر ریپلای)\n"
        "/list_members\n"
        "/expenses\n"
        "/split\n"
        "/clear_expenses\n"
        "/expense_panel\n\n"
        "📂 آرشیو گروه:\n"
        "/save کلید | مقدار\n"
        "/get کلید\n"
        "/list_saved\n"
        "/save_msg (روی پیام ریپلای)\n"
        "/get_msg کلید\n"
        "/list_msgs\n"
        "/del_msg کلید\n\n"
        "📡 فوروارد هوشمند کانال (گروه):\n"
        "/fw_on | /fw_off\n"
        "/fw_add_channel @channel یا -100...\n"
        "/fw_del_channel @channel\n"
        "/fw_add_keyword عبارت\n"
        "/fw_del_keyword عبارت\n"
        "/fw_list\n\n"
        "⚙️ تنظیمات گروه (ادمین):\n"
        "/group_menu | /group_settings\n"
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
        "/group_reset\n\n"
        "🛡 دستورات صاحب بات:\n"
        "/owner_panel\n"
        "/set_gpt on|off\n"
        "/set_gpt_limit عدد"
    )


def full_guide_text(is_group: bool = False) -> str:
    scope = "گروه" if is_group else "خصوصی"
    group_tip = (
        "• در گروه، برای دستورهای دارای @BotUsername هم می‌تونی از نسخه ساده دستور استفاده کنی.\n"
        "• اگر Privacy Mode روشن باشد، برخی دستورات فقط با /command یا ریپلای دقیق بهتر جواب می‌دهند.\n\n"
    )
    if not is_group:
        group_tip = (
            "• برای قابلیت‌های گروهی (امتیاز، دنگ، تنظیمات، فوروارد هوشمند) باید داخل گروه از بات استفاده کنی.\n\n"
        )
    return (
        "📘 راهنمای کامل ربات\n"
        f"حالت فعلی چت: {scope}\n\n"
        "1) بازار ارز/سکه/طلا/کریپتو\n"
        "• قیمت کامل: /prices (خصوصی) یا /gprices (گروه)\n"
        "• بیشترین تغییر: /top_changes\n"
        "• کلیدواژه سریع: دلار، یورو، طلا، سکه، بیت کوین، تتر\n"
        "• دکمه‌ها: 📊 قیمت کامل | 💱 ارزهای مهم | 🪙 سکه ها | 📈 بیشترین تغییر\n\n"
        "2) چت هوش مصنوعی\n"
        "• شروع جدید: /ai سوال\n"
        "• شروع جدید اجباری: /ai_new سوال\n"
        "• ادامه گفتگو: روی آخرین پاسخ AI ریپلای کن و پیام بده\n"
        "• حالت سریع: «هوش ...» یا «ai ...»\n"
        "• مصرف گروه: /ai_usage\n\n"
        "3) خلاصه‌سازی متن طولانی\n"
        "• روی پیام طولانی ریپلای کن: /summarize\n"
        "• یا مستقیم: /summarize متن\n"
        "• خلاصه‌سازی کاملا با موتور داخلی خود ربات انجام می‌شود (بدون API).\n\n"
        "4) راستی‌آزمایی خبر\n"
        "• حالت اخبار (پایگاه 200+ جهانی + 10 داخلی + جست‌وجوی زنده وب): /fact_news یا /factcheck\n"
        "• حالت خیلی خلاصه: /cred_short\n"
        "• حالت پیشرفته AI + تحلیل جزءبه‌جزء + فکت‌چک وب‌محور با لینک منبع: /fact_pro\n"
        "• خروجی: درصد احتمال واقعی/فیک + دلیل امتیاز + منابع شاخص + جمع‌بندی\n\n"
        "5) خرج و دنگ گروهی\n"
        "• ثبت خرج: /add 480 پیتزا\n"
        "• ساخت/تعویض لیست: /list_new عنوان | /lists | /list_use l2\n"
        "• اعضا: /join_split | /list_add_member (روی پیام کاربر) | /list_remove_member\n"
        "• گزارش/تسویه: /expenses | /split | /clear_expenses\n"
        "• پنل: /expense_panel\n\n"
        "6) آرشیو گروه\n"
        "• متن/یادداشت: /save کلید | مقدار ، /get کلید ، /list_saved\n"
        "• ذخیره ارجاع پیام: /save_msg (روی پیام ریپلای) ، /get_msg کلید ، /list_msgs ، /del_msg کلید\n\n"
        "7) فوروارد هوشمند کانال به گروه\n"
        "• روشن/خاموش: /fw_on | /fw_off\n"
        "• مدیریت منابع: /fw_add_channel @channel ، /fw_del_channel @channel\n"
        "• کلیدواژه‌ها: /fw_add_keyword عبارت ، /fw_del_keyword عبارت ، /fw_list\n\n"
        "8) امتیاز و شوخی گروه\n"
        "• روی پیام کاربر ریپلای کن و بنویس: «کسشر شناسایی شد» یا «جمله طلایی»\n"
        "• جدول: /scoreboard ، امتیاز من: /my_score\n\n"
        "9) پیشنهاد محتوا\n"
        "• پیشنهاد شخصی (خصوصی): /recommend_me\n"
        "• پیشنهاد روزانه گروه: /reco_on | /reco_off | /set_reco_time 21:30 | /send_reco_now\n\n"
        "10) تنظیمات گروه (ادمین)\n"
        "• پنل: /group_menu یا /group_settings\n"
        "• کلیدها: /group_on | /group_off | /set_group_cmd !prices\n"
        "• زمان‌بندی: /auto_on | /auto_off | /set_interval 15 | /set_cooldown 20\n"
        "• نمایش/ارسال: /set_title ... | /set_silent on|off | /toggle_crypto | /toggle_pin | /toggle_mention\n"
        "• ارسال فوری/ریست: /send_now | /group_reset\n\n"
        "نکات مهم\n"
        f"{group_tip}"
        "مثال‌های سریع\n"
        "• /ai یه تحلیل کوتاه از وضعیت دلار بده\n"
        "• /summarize (روی یک پیام طولانی ریپلای)\n"
        "• /fact_news (روی یک خبر ریپلای)\n"
        "• /add 260000 تاکسی"
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
    ai_status = "نامشخص"
    ai_limit = "-"
    ai_out = "-"
    if chat_id is not None:
        ai_status = "روشن" if cfg.get("ai_enabled", True) else "خاموش"
        ai_limit = str(get_group_ai_limit(chat_id))
        ai_out = str(get_group_ai_output_tokens(chat_id))
    fw_status = "روشن" if cfg.get("smart_forward_enabled", False) else "خاموش"
    fw_channels = len(cfg.get("smart_forward_channels", []) or [])
    fw_keywords = len(cfg.get("smart_forward_keywords", []) or [])

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
        f"│  🤖 AI گروه: {ai_status} (لیمیت {ai_limit} | خروجی {ai_out})\n"
        f"│  📡 فوروارد هوشمند: {fw_status} (کانال: {fw_channels} | کلمه: {fw_keywords})\n"
        "╰─────────────── ✦"
    )


def scheduler_loop() -> None:
    while True:
        try:
            maybe_refresh_news_index_background()
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
            _group_settings_page_text(cfg, chat_id=message.chat.id, page="main")
            + "\n\nبرای تغییر هر بخش، صفحه مربوطه را انتخاب کن.",
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


@bot.message_handler(commands=["guide", "full_guide", "group_guide"])
def guide_cmd(message):
    track_message_context(message, action="guide")
    text = full_guide_text(is_group=is_group_chat(message))
    if is_group_chat(message):
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
    else:
        bot.send_message(message.chat.id, text, reply_markup=_menu_keyboard(is_group=False))


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
        _group_settings_page_text(cfg, chat_id=message.chat.id, page="main"),
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
        page = payload[2] if payload[2] in {"main", "market", "reco", "ai", "data"} else "main"
    elif len(payload) >= 3 and payload[1] == "t":
        page = "market"
        key = payload[2]
        if key == "enabled":
            cfg["enabled"] = not cfg.get("enabled", True)
        elif key == "auto":
            cfg["auto_enabled"] = not cfg.get("auto_enabled", False)
            if cfg["auto_enabled"]:
                cfg["last_sent_ts"] = 0
        elif key == "percent":
            cfg["show_percent"] = not cfg.get("show_percent", True)
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
        page = "market"
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
    elif len(payload) >= 3 and payload[1] == "ai":
        page = "ai"
        key = payload[2]
        if key == "toggle_enabled":
            cfg["ai_enabled"] = not cfg.get("ai_enabled", True)
            save_group_settings(GROUP_SETTINGS)
        elif key == "toggle_reply_only":
            cfg["ai_reply_only"] = not cfg.get("ai_reply_only", False)
            save_group_settings(GROUP_SETTINGS)
        elif key == "limit_minus":
            cfg["ai_daily_limit"] = max(1, int(cfg.get("ai_daily_limit", 50)) - 5)
            save_group_settings(GROUP_SETTINGS)
        elif key == "limit_plus":
            cfg["ai_daily_limit"] = min(500, int(cfg.get("ai_daily_limit", 50)) + 5)
            save_group_settings(GROUP_SETTINGS)
        elif key == "output_minus":
            cfg["ai_output_tokens"] = max(200, int(cfg.get("ai_output_tokens", 1000)) - 100)
            save_group_settings(GROUP_SETTINGS)
        elif key == "output_plus":
            cfg["ai_output_tokens"] = min(2000, int(cfg.get("ai_output_tokens", 1000)) + 100)
            save_group_settings(GROUP_SETTINGS)
        elif key == "reset_usage":
            gcfg = get_global_config()
            with SETTINGS_LOCK:
                counts = gcfg.setdefault("group_ai_counts", {})
                counts[str(chat_id)] = 0
            save_global_settings()
    elif len(payload) >= 2 and payload[1] == "send_now":
        page = "market"
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
    elif len(payload) >= 3 and payload[1] == "data":
        page = "data"
        data_action = payload[2]
        if data_action == "reset_all":
            with SETTINGS_LOCK:
                GROUP_SETTINGS[str(chat_id)] = _default_group_config()
            save_group_settings(GROUP_SETTINGS)
        elif data_action == "reset_scores":
            with SETTINGS_LOCK:
                SCORES[str(chat_id)] = {}
            save_scores()
        elif data_action == "reset_expenses":
            with SETTINGS_LOCK:
                EXPENSES[str(chat_id)] = _new_empty_expense_state()
            save_expenses()
        elif data_action == "reset_archive":
            with SETTINGS_LOCK:
                ARCHIVE[str(chat_id)] = {}
            save_archive()
    elif len(payload) >= 2 and payload[1] == "reset_scores":
        page = "data"
        with SETTINGS_LOCK:
            SCORES[str(chat_id)] = {}
        save_scores()
    elif len(payload) >= 2 and payload[1] == "reset_expenses":
        page = "data"
        with SETTINGS_LOCK:
            EXPENSES[str(chat_id)] = _new_empty_expense_state()
        save_expenses()
    elif len(payload) >= 2 and payload[1] == "reset_archive":
        page = "data"
        with SETTINGS_LOCK:
            ARCHIVE[str(chat_id)] = {}
        save_archive()
    elif len(payload) >= 2 and payload[1] == "reset":
        page = "data"
        with SETTINGS_LOCK:
            GROUP_SETTINGS[str(chat_id)] = _default_group_config()
        save_group_settings(GROUP_SETTINGS)

    try:
        text = _group_settings_page_text(cfg, chat_id=chat_id, page=page)
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
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    bot.send_message(
        message.chat.id,
        _group_settings_page_text(cfg, chat_id=message.chat.id, page="main"),
        reply_markup=_group_settings_markup(cfg, chat_id=message.chat.id, page="main"),
    )


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


def _smart_forward_text(cfg: dict[str, Any]) -> str:
    channels = cfg.get("smart_forward_channels", []) or []
    keywords = cfg.get("smart_forward_keywords", []) or []
    return (
        "📡 وضعیت فوروارد هوشمند\n"
        f"• وضعیت: {'روشن' if cfg.get('smart_forward_enabled', False) else 'خاموش'}\n"
        f"• تعداد کانال‌ها: {len(channels)}\n"
        f"• تعداد کلمات: {len(keywords)}\n\n"
        "کانال‌ها:\n"
        + ("\n".join(f"• {c}" for c in channels[:30]) if channels else "• ثبت نشده")
        + "\n\nکلمات:\n"
        + ("\n".join(f"• {k}" for k in keywords[:30]) if keywords else "• ثبت نشده")
    )


@bot.message_handler(commands=["fw_on"])
def fw_on(message):
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    cfg["smart_forward_enabled"] = True
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, "✅ فوروارد هوشمند روشن شد.")


@bot.message_handler(commands=["fw_off"])
def fw_off(message):
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    cfg["smart_forward_enabled"] = False
    save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, "⛔️ فوروارد هوشمند خاموش شد.")


@bot.message_handler(commands=["fw_add_channel"])
def fw_add_channel(message):
    if not require_group_admin(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /fw_add_channel @channel یا -100123...")
        return
    ref = _normalize_channel_ref(parts[1])
    if not ref:
        bot.reply_to(message, "ورودی کانال معتبر نیست.")
        return
    cfg = get_group_config(message.chat.id)
    items = cfg.setdefault("smart_forward_channels", [])
    if ref not in items:
        items.append(ref)
        save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"✅ کانال اضافه شد: {ref}")


@bot.message_handler(commands=["fw_del_channel"])
def fw_del_channel(message):
    if not require_group_admin(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /fw_del_channel @channel")
        return
    ref = _normalize_channel_ref(parts[1])
    cfg = get_group_config(message.chat.id)
    items = cfg.setdefault("smart_forward_channels", [])
    if ref in items:
        items.remove(ref)
        save_group_settings(GROUP_SETTINGS)
        bot.reply_to(message, f"🗑 حذف شد: {ref}")
    else:
        bot.reply_to(message, "این کانال در لیست نیست.")


@bot.message_handler(commands=["fw_add_keyword"])
def fw_add_keyword(message):
    if not require_group_admin(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /fw_add_keyword کلمه یا عبارت")
        return
    kw = normalize_fa_text(parts[1])[:80]
    if not kw:
        bot.reply_to(message, "کلمه معتبر نیست.")
        return
    cfg = get_group_config(message.chat.id)
    items = cfg.setdefault("smart_forward_keywords", [])
    if kw not in items:
        items.append(kw)
        save_group_settings(GROUP_SETTINGS)
    bot.reply_to(message, f"✅ کلمه اضافه شد: {kw}")


@bot.message_handler(commands=["fw_del_keyword"])
def fw_del_keyword(message):
    if not require_group_admin(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /fw_del_keyword کلمه")
        return
    kw = normalize_fa_text(parts[1])[:80]
    cfg = get_group_config(message.chat.id)
    items = cfg.setdefault("smart_forward_keywords", [])
    if kw in items:
        items.remove(kw)
        save_group_settings(GROUP_SETTINGS)
        bot.reply_to(message, f"🗑 حذف شد: {kw}")
    else:
        bot.reply_to(message, "این کلمه در لیست نیست.")


@bot.message_handler(commands=["fw_list"])
def fw_list(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور برای گروه است.")
        return
    if not require_group_admin(message):
        return
    cfg = get_group_config(message.chat.id)
    bot.reply_to(message, _smart_forward_text(cfg))


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
    current = _get_expense_list(state)
    uid = str(message.from_user.id)
    name = _expense_user_name(message.from_user)
    current["entries"].append(
        {
            "by_id": uid,
            "by_name": name,
            "amount": amount,
            "desc": desc,
            "ts": int(time.time()),
        }
    )
    current["participants"].setdefault(uid, name)
    save_expenses()
    bot.reply_to(
        message,
        f"ثبت شد در {current.get('title', current.get('id', 'لیست'))}: {amount:g} - {desc}",
    )


@bot.message_handler(commands=["list_new"])
def expense_list_new(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور فقط داخل گروه است.")
        return
    title = (message.text or "").split(maxsplit=1)[1].strip() if len((message.text or "").split(maxsplit=1)) > 1 else ""
    if not title:
        title = f"لیست {now_tehran().strftime('%m/%d %H:%M')}"
    state = get_expense_state(message.chat.id)
    new_id = _new_expense_list_id(state)
    creator_id = str(message.from_user.id)
    creator_name = _expense_user_name(message.from_user)
    state["lists"][new_id] = {
        "id": new_id,
        "title": title[:60],
        "created_by": creator_id,
        "created_at": int(time.time()),
        "participants": {creator_id: creator_name},
        "entries": [],
    }
    state["active_list_id"] = new_id
    save_expenses()
    bot.reply_to(message, f"✅ لیست جدید ساخته شد: {title} ({new_id})\nاین لیست الان فعال است.")


@bot.message_handler(commands=["list_use"])
def expense_list_use(message):
    if not is_group_chat(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /list_use l2")
        return
    list_id = parts[1].strip().lower()
    state = get_expense_state(message.chat.id)
    if list_id not in state.get("lists", {}):
        bot.reply_to(message, "این لیست پیدا نشد. /lists را بزن.")
        return
    state["active_list_id"] = list_id
    save_expenses()
    lst = state["lists"][list_id]
    bot.reply_to(message, f"✅ لیست فعال شد: {lst.get('title', list_id)} ({list_id})")


@bot.message_handler(commands=["lists"])
def expense_lists(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    lists = state.get("lists", {})
    active = state.get("active_list_id", "")
    lines = ["🧾 لیست‌های خرج گروه:"]
    for lid, lst in lists.items():
        mark = "⭐" if lid == active else "•"
        lines.append(
            f"{mark} {lid} | {lst.get('title', 'لیست')} | "
            f"اعضا: {len(lst.get('participants', {}))} | خرج: {len(lst.get('entries', []))}"
        )
    lines.append("\nبرای انتخاب: /list_use l2")
    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["join_split"])
def join_split(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    current = _get_expense_list(state)
    uid = str(message.from_user.id)
    current["participants"][uid] = _expense_user_name(message.from_user)
    save_expenses()
    bot.reply_to(message, f"شما به لیست {current.get('title', current.get('id', 'لیست'))} اضافه شدید.")


@bot.message_handler(commands=["list_add_member"])
def list_add_member(message):
    if not is_group_chat(message):
        return
    if not getattr(message, "reply_to_message", None) or not getattr(message.reply_to_message, "from_user", None):
        bot.reply_to(message, "روی پیام کاربر ریپلای کن و /list_add_member بزن.")
        return
    target = message.reply_to_message.from_user
    state = get_expense_state(message.chat.id)
    current = _get_expense_list(state)
    uid = str(target.id)
    current["participants"][uid] = _expense_user_name(target)
    save_expenses()
    bot.reply_to(message, f"✅ {current['participants'][uid]} به لیست فعال اضافه شد.")


@bot.message_handler(commands=["list_remove_member"])
def list_remove_member(message):
    if not is_group_chat(message):
        return
    if not getattr(message, "reply_to_message", None) or not getattr(message.reply_to_message, "from_user", None):
        bot.reply_to(message, "روی پیام کاربر ریپلای کن و /list_remove_member بزن.")
        return
    target = message.reply_to_message.from_user
    state = get_expense_state(message.chat.id)
    current = _get_expense_list(state)
    uid = str(target.id)
    if uid in current.get("participants", {}):
        del current["participants"][uid]
        save_expenses()
        bot.reply_to(message, "✅ از لیست فعال حذف شد.")
    else:
        bot.reply_to(message, "این کاربر داخل اعضای لیست فعال نیست.")


@bot.message_handler(commands=["list_members"])
def list_members(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    current = _get_expense_list(state)
    members = current.get("participants", {})
    if not members:
        bot.reply_to(message, "هنوز عضوی برای دنگ در لیست فعال ثبت نشده.")
        return
    lines = [f"👥 اعضای لیست {current.get('title', current.get('id', 'لیست'))}:"]
    for name in members.values():
        lines.append(f"• {name}")
    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["expenses"])
def expenses_summary(message):
    if not is_group_chat(message):
        return
    state = get_expense_state(message.chat.id)
    current = _get_expense_list(state)
    entries = current.get("entries", [])
    if not entries:
        bot.reply_to(message, "در لیست فعال هنوز خرجی ثبت نشده.")
        return
    total = sum(float(x.get("amount", 0)) for x in entries)
    by_user: dict[str, float] = {}
    by_name: dict[str, str] = {}
    for item in entries:
        uid = str(item.get("by_id"))
        by_user[uid] = by_user.get(uid, 0.0) + float(item.get("amount", 0))
        by_name[uid] = str(item.get("by_name", uid))
    lines = [f"🧾 لیست: {current.get('title', current.get('id', 'لیست'))}", f"💰 مجموع خرج: {total:g}", "", "پرداخت هر نفر:"]
    for uid, value in sorted(by_user.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"• {by_name.get(uid, uid)}: {value:g}")
    bot.reply_to(message, "\n".join(lines))


def _compute_split_settlements(expense_list: dict[str, Any]) -> list[str]:
    entries = expense_list.get("entries", [])
    if not entries:
        return []
    paid: dict[str, float] = {}
    names: dict[str, str] = {}
    for item in entries:
        uid = str(item.get("by_id"))
        paid[uid] = paid.get(uid, 0.0) + float(item.get("amount", 0))
        names[uid] = str(item.get("by_name", uid))
    participants: dict[str, str] = expense_list.get("participants", {}) or {}
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

    settlements: list[str] = [
        f"🧾 لیست: {expense_list.get('title', expense_list.get('id', 'لیست'))}",
        f"💳 سهم هر نفر: {share:g}",
    ]
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
    current = _get_expense_list(state)
    settlements = _compute_split_settlements(current)
    if not settlements:
        bot.reply_to(message, "برای تسویه ابتدا خرج ثبت کنید: /add مبلغ توضیح")
        return
    bot.reply_to(message, "\n".join(["🧾 نتیجه تسویه"] + settlements))


@bot.message_handler(commands=["clear_expenses"])
def clear_expenses(message):
    if not require_group_admin(message):
        return
    state = get_expense_state(message.chat.id)
    current = _get_expense_list(state)
    current["entries"] = []
    current["participants"] = {}
    save_expenses()
    bot.reply_to(message, "لیست فعال پاک شد.")


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

    state = get_expense_state(chat_id)
    current = _get_expense_list(state)
    answered = False

    if action == "new":
        new_id = _new_expense_list_id(state)
        creator_id = str(call.from_user.id)
        creator_name = _expense_user_name(call.from_user)
        state["lists"][new_id] = {
            "id": new_id,
            "title": f"لیست {now_tehran().strftime('%m/%d %H:%M')}",
            "created_by": creator_id,
            "created_at": int(time.time()),
            "participants": {creator_id: creator_name},
            "entries": [],
        }
        state["active_list_id"] = new_id
        save_expenses()
    elif action == "next":
        lids = list(state.get("lists", {}).keys())
        if lids:
            active = str(state.get("active_list_id", lids[0]))
            idx = lids.index(active) if active in lids else 0
            state["active_list_id"] = lids[(idx + 1) % len(lids)]
            save_expenses()
    elif action == "join":
        uid = str(call.from_user.id)
        current["participants"][uid] = _expense_user_name(call.from_user)
        save_expenses()
    elif action == "members":
        participants = current.get("participants", {})
        if participants:
            lines = [f"👥 اعضای {current.get('title', current.get('id', 'لیست'))}:"]
            lines.extend([f"• {name}" for name in participants.values()])
            try:
                bot.send_message(chat_id, "\n".join(lines))
            except Exception:
                pass
        else:
            bot.answer_callback_query(call.id, "عضوی ثبت نشده.")
            answered = True
    elif action == "show":
        entries = current.get("entries", [])
        if entries:
            total = sum(float(x.get("amount", 0)) for x in entries)
            lines = [f"🧾 {current.get('title', current.get('id', 'لیست'))}", f"💰 مجموع خرج: {total:g}"]
            for item in entries[-10:]:
                lines.append(f"• {item.get('by_name', 'کاربر')}: {float(item.get('amount', 0)):g} - {item.get('desc', '')}")
            try:
                bot.send_message(chat_id, "\n".join(lines))
            except Exception:
                pass
        else:
            bot.answer_callback_query(call.id, "هنوز خرجی ثبت نشده.")
            answered = True
    elif action == "split":
        settlements = _compute_split_settlements(current)
        if settlements:
            bot.send_message(chat_id, "\n".join(["🧾 نتیجه تسویه"] + settlements))
        else:
            bot.answer_callback_query(call.id, "خرجی برای تسویه نیست.")
            answered = True
    elif action == "clear":
        if not has_group_management_access(chat_id, call.from_user.id):
            bot.answer_callback_query(call.id, "فقط ادمین گروه یا ادمین بات", show_alert=True)
            return
        current["entries"] = []
        current["participants"] = {}
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
    if not answered:
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


def _handle_ai_command(message, force_new: bool = True):
    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(
            message,
            "برای شروع جدید بنویس:\n/ai سوالت\n\n"
            "برای ادامه گفتگو، روی آخرین جواب AI ریپلای کن و پیام بده.",
        )
        return
    run_ai_chat(message, parts[1], force_new=force_new)


@bot.message_handler(commands=["ai", "ask"])
def ask_gpt(message):
    _handle_ai_command(message, force_new=True)


@bot.message_handler(commands=["ai_new", "asknew"])
def ask_gpt_new(message):
    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(message, "فرمت درست: /ai_new سوال")
        return
    run_ai_chat(message, parts[1], force_new=True)


@bot.message_handler(commands=["ai_usage"])
def ai_usage(message):
    if not is_group_chat(message):
        bot.reply_to(message, "این دستور مخصوص گروه است.")
        return
    used, limit = get_group_ai_usage(message.chat.id)
    remaining = max(0, limit - used)
    out_tokens = get_group_ai_output_tokens(message.chat.id)
    bot.reply_to(
        message,
        f"📊 مصرف AI امروز گروه: {used}/{limit}\n"
        f"• باقی‌مانده: {remaining}\n"
        f"• سقف خروجی هر پاسخ: {out_tokens} توکن",
    )


@bot.message_handler(commands=["summarize", "sum", "tldr", "tl_dr", "kholase"])
def summarize_text_command(message):
    track_message_context(message, action="summarize_command")
    if is_group_chat(message) and not is_for_this_bot(_command_head(message)):
        return

    source_text = _extract_summarize_input_text(message)
    if not source_text:
        bot.reply_to(
            message,
            "برای خلاصه‌سازی:\n"
            "1) روی پیام طولانی ریپلای کن و /summarize بزن\n"
            "2) یا مستقیم بنویس: /summarize متن طولانی",
        )
        return

    status = bot.reply_to(
        message,
        "🧠 در حال خلاصه‌سازی دقیق متن...\n"
        "• استخراج نکات اصلی\n"
        "• حفظ اعداد/نام‌ها/زمان‌ها\n"
        "• تولید نسخه کوتاه و دقیق",
    )
    report = run_text_summarizer(message, source_text)
    try:
        bot.edit_message_text(
            report,
            chat_id=status.chat.id,
            message_id=status.message_id,
            disable_web_page_preview=True,
        )
    except Exception:
        bot.reply_to(message, report, disable_web_page_preview=True)


def _extract_factcheck_input_text(message) -> str:
    reply = getattr(message, "reply_to_message", None)
    reply_text = _extract_message_text(reply) if reply is not None else ""
    if reply_text and reply_text.strip():
        return reply_text.strip()
    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) >= 2:
        return parts[1].strip()
    return ""


@bot.message_handler(commands=["factcheck", "verify_news", "newscheck", "fact_news"])
def factcheck_news(message):
    track_message_context(message, action="factcheck_command")
    if is_group_chat(message) and not is_for_this_bot(_command_head(message)):
        return

    source_text = _extract_factcheck_input_text(message)
    if not source_text:
        bot.reply_to(
            message,
            "برای فکت‌سنجی اخبار:\n"
            "1) روی پیام خبر ریپلای کن و /fact_news بزن\n"
            "2) یا مستقیم بنویس: /fact_news متن خبر",
        )
        return

    status = bot.reply_to(
        message,
        "🔎 در حال فکت‌سنجی اخبار...\n"
        "• بررسی گسترده در 200+ منبع جهانی + 10 منبع داخلی\n"
        "• جست‌وجوی ترکیبی: RSS + وب زنده (DuckDuckGo/Bing News)\n"
        "• امتیازدهی موافق/مخالف و جمع‌بندی سندمحور",
    )
    result = run_news_factcheck(source_text, mode="news")
    report = build_factcheck_report(result)
    try:
        bot.edit_message_text(
            report,
            chat_id=status.chat.id,
            message_id=status.message_id,
            disable_web_page_preview=True,
        )
    except Exception:
        bot.reply_to(message, report, disable_web_page_preview=True)


@bot.message_handler(commands=["cred_short", "verify_short", "check_short"])
def factcheck_news_brief(message):
    track_message_context(message, action="factcheck_brief_command")
    if is_group_chat(message) and not is_for_this_bot(_command_head(message)):
        return

    source_text = _extract_factcheck_input_text(message)
    if not source_text:
        bot.reply_to(
            message,
            "برای اعتبارسنجی خیلی خلاصه:\n"
            "1) روی پیام ریپلای کن و /cred_short بزن\n"
            "2) یا مستقیم بنویس: /cred_short متن ادعا/خبر",
        )
        return

    status = bot.reply_to(
        message,
        "⚡️ اعتبارسنجی خیلی خلاصه در حال انجام است...\n"
        "• نتیجه سریع\n"
        "• منابع کلیدی محدود",
    )
    result = run_news_factcheck(source_text, mode="brief")
    report = build_factcheck_report(result)
    try:
        bot.edit_message_text(
            report,
            chat_id=status.chat.id,
            message_id=status.message_id,
            disable_web_page_preview=True,
        )
    except Exception:
        bot.reply_to(message, report, disable_web_page_preview=True)


@bot.message_handler(commands=["fact_pro", "factcheck_pro", "verify_claim"])
def factcheck_news_pro(message):
    track_message_context(message, action="factcheck_pro_command")
    if is_group_chat(message) and not is_for_this_bot(_command_head(message)):
        return

    source_text = _extract_factcheck_input_text(message)
    if not source_text:
        bot.reply_to(
            message,
            "برای راستی‌آزمایی پیشرفته:\n"
            "1) روی پیام خبر ریپلای کن و /fact_pro بزن\n"
            "2) یا مستقیم بنویس: /fact_pro متن خبر",
        )
        return

    status = bot.reply_to(
        message,
        "🧪 در حال راستی‌آزمایی پیشرفته...\n"
        "• تحلیل خبر با هوش مصنوعی + مدل‌سازی شواهد\n"
        "• بازیابی ترکیبی: RSS + جست‌وجوی زنده وب\n"
        "• فکت‌چک وب‌محور با OpenAI و منابع لینک‌دار\n"
        "• ترجمه ادعای فارسی برای چک در منابع بین‌المللی\n"
        "• استخراج فکت‌های جزئی و تناقض‌ها\n"
        "• ارائه سند، لینک و استدلال جزءبه‌جزء",
    )
    result = run_news_factcheck(source_text, mode="pro")
    report = build_factcheck_report(result)
    try:
        bot.edit_message_text(
            report,
            chat_id=status.chat.id,
            message_id=status.message_id,
            disable_web_page_preview=True,
        )
    except Exception:
        bot.reply_to(message, report, disable_web_page_preview=True)


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
    gcfg = get_global_config()
    _, counts = _group_ai_usage_state()
    total_used = sum(int(v) for v in counts.values())
    per_answer_tokens = get_group_ai_output_tokens()
    return (
        "🛡 سوپر پنل ادمین بات\n"
        f"• تعداد گروه‌ها: {len(groups)}\n"
        f"• فایل دیتابیس: {DB_PATH}\n"
        f"• مدل پیشنهاد: {OPENAI_MODEL}\n"
        f"• لیمیت روزانه AI هر گروه: {int(gcfg.get('group_ai_daily_limit', 50))}\n"
        f"• سقف خروجی هر پاسخ AI: {per_answer_tokens} توکن\n"
        f"• مصرف کل AI امروز (همه گروه‌ها): {total_used}\n"
        "یک گروه را از دکمه‌ها انتخاب کن."
    )


def owner_panel_markup():
    kb = types.InlineKeyboardMarkup(row_width=1)
    groups = owner_groups()
    gcfg = get_global_config()
    kb.add(
        types.InlineKeyboardButton(
            f"AI Limit -5 ({int(gcfg.get('group_ai_daily_limit', 50))})",
            callback_data="ow:ai:limit_minus",
        ),
        types.InlineKeyboardButton(
            "AI Limit +5",
            callback_data="ow:ai:limit_plus",
        ),
    )
    kb.add(
        types.InlineKeyboardButton(
            f"Output -100 ({get_group_ai_output_tokens()})",
            callback_data="ow:ai:out_minus",
        ),
        types.InlineKeyboardButton(
            "Output +100",
            callback_data="ow:ai:out_plus",
        ),
    )
    kb.add(types.InlineKeyboardButton("🧮 ریست مصرف AI امروز", callback_data="ow:ai:reset_today"))
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
    ai_used, ai_limit = get_group_ai_usage(chat_id)
    ai_remaining = max(0, ai_limit - ai_used)
    ai_tokens = get_group_ai_output_tokens(chat_id)
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
        f"• نوع‌های reco: {', '.join(reco_types) if reco_types else 'هیچکدام'}\n"
        f"• مصرف AI امروز: {ai_used}/{ai_limit}\n"
        f"• باقی‌مانده AI امروز: {ai_remaining}\n"
        f"• سقف خروجی هر پاسخ AI: {ai_tokens} توکن"
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
    bot.reply_to(
        message,
        "چت AI فعاله:\n"
        "• شروع جدید: /ai سوال\n"
        "• شروع جدید اجباری: /ai_new سوال\n"
        "• ادامه: ریپلای روی جواب AI\n"
        "• مصرف گروه: /ai_usage",
    )


@bot.message_handler(commands=["set_gpt_limit"])
def set_gpt_limit(message):
    if not is_owner(message.from_user.id):
        bot.reply_to(message, "فقط صاحب بات دسترسی دارد.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "فرمت: /set_gpt_limit 50")
        return
    try:
        limit = int(parts[1].strip())
    except Exception:
        bot.reply_to(message, "عدد معتبر وارد کن.")
        return
    if limit < 1 or limit > 500:
        bot.reply_to(message, "بازه مجاز: 1 تا 500")
        return
    cfg = get_global_config()
    with SETTINGS_LOCK:
        cfg["group_ai_daily_limit"] = limit
    save_global_settings()
    bot.reply_to(message, f"✅ لیمیت روزانه چت AI برای هر گروه روی {limit} تنظیم شد.")


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

    if action == "ai" and len(data) >= 3:
        key = data[2]
        cfg = get_global_config()
        notice = "به‌روزرسانی شد"
        with SETTINGS_LOCK:
            if key == "limit_minus":
                cfg["group_ai_daily_limit"] = max(1, int(cfg.get("group_ai_daily_limit", 50)) - 5)
            elif key == "limit_plus":
                cfg["group_ai_daily_limit"] = min(500, int(cfg.get("group_ai_daily_limit", 50)) + 5)
            elif key == "out_minus":
                cfg["group_ai_max_output_tokens"] = max(200, int(cfg.get("group_ai_max_output_tokens", 1000)) - 100)
            elif key == "out_plus":
                cfg["group_ai_max_output_tokens"] = min(2000, int(cfg.get("group_ai_max_output_tokens", 1000)) + 100)
            elif key == "reset_today":
                cfg["group_ai_day"] = today_key_tehran()
                cfg["group_ai_counts"] = {}
                notice = "مصرف امروز AI ریست شد"
        save_global_settings()
        try:
            bot.edit_message_text(
                owner_panel_text(),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=owner_panel_markup(),
            )
        except Exception:
            pass
        bot.answer_callback_query(call.id, notice)
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
            EXPENSES[str(chat_id)] = _new_empty_expense_state()
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


@bot.channel_post_handler(func=lambda m: True)
def smart_forward_channel_post(message):
    source_keys = _channel_keys_from_chat(getattr(message, "chat", None))
    if not source_keys:
        return
    text = _extract_message_text(message)
    if not text:
        return

    with SETTINGS_LOCK:
        groups = list(GROUP_SETTINGS.items())

    for chat_id_str, cfg in groups:
        try:
            target_chat_id = int(chat_id_str)
        except Exception:
            continue
        if not bool(cfg.get("smart_forward_enabled", False)):
            continue
        channels = [str(x).strip() for x in (cfg.get("smart_forward_channels", []) or []) if str(x).strip()]
        if not channels:
            continue
        if not any(ch in source_keys for ch in channels):
            continue
        keywords = [str(x).strip() for x in (cfg.get("smart_forward_keywords", []) or []) if str(x).strip()]
        if not keywords or not _text_matches_keywords(text, keywords):
            continue
        try:
            bot.forward_message(target_chat_id, message.chat.id, message.message_id)
        except Exception as exc:
            print(f"Smart forward error to {target_chat_id}: {exc}")


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
        "🤖 چت هوش مصنوعی",
        "⚙️ راهنما",
        "📘 راهنمای کامل",
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

    if maybe_handle_ai_text_message(message):
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
        "🤖 چت هوش مصنوعی",
        "⚙️ راهنما",
        "📘 راهنمای کامل",
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

    if txt == "📘 راهنمای کامل":
        if is_group_chat(message):
            bot.send_message(
                message.chat.id,
                full_guide_text(is_group=True),
                reply_markup=types.ReplyKeyboardRemove(),
            )
        else:
            bot.send_message(
                message.chat.id,
                full_guide_text(is_group=False),
                reply_markup=_menu_keyboard(is_group=False),
            )
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

    if txt == "🤖 چت هوش مصنوعی":
        if is_group_chat(message):
            used, limit = get_group_ai_usage(message.chat.id)
            bot.reply_to(
                message,
                "🤖 چت AI گروه\n"
                "• شروع جدید: /ai سوال\n"
                "• شروع جدید اجباری: /ai_new سوال\n"
                "• ادامه: روی جواب AI ریپلای کن\n"
                "• شروع سریع: اول پیام بنویس «هوش »\n"
                f"• مصرف امروز گروه: {used}/{limit}",
            )
        else:
            bot.reply_to(
                message,
                "🤖 چت AI خصوصی\n"
                "• شروع جدید: /ai سوال\n"
                "• شروع جدید اجباری: /ai_new سوال\n"
                "• ادامه: به پاسخ قبلی ریپلای کن\n"
                "• شروع سریع: اول پیام بنویس «هوش »",
            )
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
            _group_settings_page_text(cfg, chat_id=message.chat.id, page="main"),
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
    if WEBHOOK_SECRET_TOKEN:
        incoming = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if incoming != WEBHOOK_SECRET_TOKEN:
            return "Forbidden", 403
    update = telebot.types.Update.de_json(request.stream.read().decode("utf-8"))
    bot.process_new_updates([update])
    return "OK", 200


@app.route("/", methods=["GET"])
def healthcheck():
    return "OK", 200


def configure_webhook() -> str:
    if not WEBHOOK_BASE_URL:
        raise RuntimeError(
            "WEBHOOK_BASE_URL is not set. Example: https://your-app.up.railway.app"
        )
    webhook_url = f"{WEBHOOK_BASE_URL}/{BOT_TOKEN}"
    bot.remove_webhook()
    time.sleep(0.5)
    kwargs: dict[str, Any] = {}
    if WEBHOOK_SECRET_TOKEN:
        kwargs["secret_token"] = WEBHOOK_SECRET_TOKEN
    ok = bot.set_webhook(url=webhook_url, **kwargs)
    if not ok:
        raise RuntimeError("Failed to set Telegram webhook.")
    return webhook_url


if __name__ == "__main__":
    init_database()
    me = bot.get_me()
    print(f"Bot started: @{me.username}")
    print("If group commands do not work, disable privacy in BotFather: /setprivacy -> Disable")

    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()

    if RUN_MODE == "webhook":
        webhook_url = configure_webhook()
        print(f"Webhook mode enabled: {webhook_url}")
        app.run(host=APP_HOST, port=APP_PORT)
    elif RUN_MODE == "polling":
        bot.remove_webhook()
        bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
    else:
        raise RuntimeError("RUN_MODE must be either 'webhook' or 'polling'.")
