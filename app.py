# app.py — WorthItService (simplified & production-safe)
# - Endpoints: /transcript, /comments, /openai/responses, /openai/responses/<id>
# - Static:     /, /_health, /privacy, /terms, /support, /favicon.ico
# - Features:   request_id, JSON logging, CORS, rate limits, Webshare/generic proxy, in-flight dedupe,
#               in-memory + persistent cache, robust transcript pipeline (primary + 2 fallbacks),
#               comments pipeline (downloader + yt-dlp), non-streaming OpenAI proxy, sane headers.

import os
import re
import json
import time
import uuid
import shelve
import html
import random
import logging
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse

from flask import Flask, request, jsonify, g, make_response
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# YouTube transcripts & comments
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
# (Proxy helpers are optional; we guard imports)
try:
    from youtube_transcript_api.proxies import GenericProxyConfig, WebshareProxyConfig
except Exception:  # pragma: no cover
    GenericProxyConfig = None
    WebshareProxyConfig = None

from youtube_comment_downloader import YoutubeCommentDownloader
from yt_dlp import YoutubeDL

APP_NAME = "WorthItService"

CONSENT_COOKIE_VALUE = os.getenv("CONSENT_COOKIE", "YES+cb.20210328-17-p0.en+FX+888")
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE", "").strip()
NOT_AVAILABLE_TTL = int(os.getenv("TRANSCRIPT_NOT_AVAILABLE_TTL", "600"))
NOT_AVAILABLE_SENTINEL = "__NOT_AVAILABLE__"

# ----------------------- Configuration -----------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "50"))
MAX_COMMENTS_FETCH = int(os.getenv("MAX_COMMENTS_FETCH", str(COMMENT_LIMIT)))

TRANSCRIPT_CACHE_TTL = int(os.getenv("TRANSCRIPT_CACHE_TTL", "7200"))  # 2h
COMMENTS_CACHE_TTL   = int(os.getenv("COMMENT_CACHE_TTL", "7200"))     # 2h
MAX_INMEM_ITEMS      = int(os.getenv("MAX_INMEM_ITEMS", "300"))

# Persistent cache (Cloud Run => /tmp is writable)
PERSIST_DIR = os.getenv("CACHE_DIR", "/tmp/persistent_cache")
os.makedirs(PERSIST_DIR, exist_ok=True)
DB_TRANSCRIPTS = os.path.join(PERSIST_DIR, "transcripts.shelve")
DB_COMMENTS    = os.path.join(PERSIST_DIR, "comments.shelve")

if YTDL_COOKIE_FILE and not os.path.isfile(YTDL_COOKIE_FILE):
    YTDL_COOKIE_FILE = ""

if not YTDL_COOKIE_FILE:
    consent_path = Path(PERSIST_DIR) / "consent_cookies.txt"
    if not consent_path.exists():
        consent_path.write_text(
            "# Netscape HTTP Cookie File\n"
            ".youtube.com\tTRUE\t/\tFALSE\t2145916800\tCONSENT\t"
            f"{CONSENT_COOKIE_VALUE}\n",
            encoding="utf-8"
        )
    YTDL_COOKIE_FILE = str(consent_path)

# Proxies (Generic or Webshare rotating gateway)
WS_USER = os.getenv("WEBSHARE_USER")
WS_PASS = os.getenv("WEBSHARE_PASS")
GEN_HTTP = os.getenv("PROXY_HTTP_URL") or os.getenv("HTTP_PROXY")
GEN_HTTPS = os.getenv("PROXY_HTTPS_URL") or os.getenv("HTTPS_PROXY")

# Transcript language priority (editable via env)
TRANSCRIPT_LANGS = [
    c.strip() for c in os.getenv(
        "TRANSCRIPT_LANGS",
        "en,es,pt,hi,ar,ja,ru,de,fr,vi,ko,th"
    ).split(",") if c.strip()
]

# Language behavior (align with old flow: prefer original, not strict, no auto-translate by default)
STRICT_LANGUAGES = os.getenv("STRICT_LANGUAGES", "false").lower() == "true"   # if true, only return requested languages
PREFER_ORIGINAL  = os.getenv("PREFER_ORIGINAL", "true").lower() == "true"     # if true, prefer any manual/original transcript
ALLOW_TRANSLATE  = os.getenv("ALLOW_TRANSLATE", "false").lower() == "true"    # enable API-side translation as a last resort

# CORS: defaults to '*' to avoid breaking, allow explicit lock via ALLOWED_ORIGINS
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]

# HTTP timeouts
TIMEOUT_YT      = float(os.getenv("TIMEOUT_YT", "8"))     # YouTube endpoints
TIMEOUT_OPENAI  = float(os.getenv("TIMEOUT_OPENAI", "20"))
TIMEOUT_TIMEDTEXT = float(os.getenv("TIMEOUT_TIMEDTEXT", "4"))
TIMEOUT_YTDLP     = float(os.getenv("TIMEOUT_YTDLP", "15"))

# Budgets (seconds) to avoid iOS/share-extension timeouts
COMMENTS_TOTAL_BUDGET_S   = float(os.getenv("COMMENTS_TOTAL_BUDGET_S", "8.0"))
TRANSCRIPT_TOTAL_BUDGET_S = float(os.getenv("TRANSCRIPT_TOTAL_BUDGET_S", "12.0"))
# How long inflight followers wait for a leader before proceeding
INFLIGHT_FOLLOWER_WAIT_S  = float(os.getenv("INFLIGHT_FOLLOWER_WAIT_S", "6.0"))
# Only attempt yt-dlp steps if at least this much time remains
YTDLP_STEP_BUDGET_S       = float(os.getenv("YTDLP_STEP_BUDGET_S", "4.0"))

# User agents
USER_AGENTS = [
    # A small, rotated set
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

ALT_PIPED_HOSTS = [
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.adminforge.de",
    "https://pipedapi.tokhmi.xyz",
]

# Proxy helpers ------------------------------------------------
def _proxy_url() -> Optional[str]:
    if GEN_HTTPS: return GEN_HTTPS
    if GEN_HTTP:  return GEN_HTTP
    if WS_USER and WS_PASS:
        user = WS_USER if WS_USER.endswith("-rotate") else f"{WS_USER}-rotate"
        return f"http://{user}:{WS_PASS}@p.webshare.io:80"
    return None

PROXY_URL = _proxy_url()
PROXY_DICT = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else {}

def requests_proxies() -> Dict[str, str]:
    return dict(PROXY_DICT)

def yta_proxy_cfg():
    """Proxy config for youtube_transcript_api (if available)."""
    try:
        if GEN_HTTP or GEN_HTTPS:
            if GenericProxyConfig:
                return GenericProxyConfig(http_url=GEN_HTTP or GEN_HTTPS,
                                          https_url=GEN_HTTPS or GEN_HTTP)
        if WS_USER and WS_PASS and WebshareProxyConfig:
            user = WS_USER if WS_USER.endswith("-rotate") else f"{WS_USER}-rotate"
            return WebshareProxyConfig(proxy_username=user, proxy_password=WS_PASS)
    except Exception:
        pass
    return None

PROXY_CFG: Optional[Any] = None

# yt-dlp options baseline (safe & quiet)
_YDL_BASE = {
    "quiet": True, "no_warnings": True, "nocheckcertificate": True,
    "retries": 2, "fragment_retries": 2, "geo_bypass": True,
    "extractor_args": {"youtube": ["player_client=ios"]},
    **({"cookiefile": YTDL_COOKIE_FILE} if YTDL_COOKIE_FILE else {}),
}

# ----------------------- App, CORS, Limiter -----------------------
app = Flask(__name__)
if ALLOWED_ORIGINS == ["*"]:
    CORS(app, resources={r"/*": {"origins": "*"}})
else:
    CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}})

limiter = Limiter(get_remote_address, app=app, default_limits=[])

# ----------------------- Logging -----------------------
logger = logging.getLogger(APP_NAME)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)

def log_event(level: str, event: str, **fields):
    rec = {"event": event, "logger": APP_NAME}
    if hasattr(g, "request_id"): rec["request_id"] = g.request_id
    rec.update(fields)
    msg = json.dumps(rec, ensure_ascii=False)
    getattr(logger, level if level in ("debug","info","warning","error","critical") else "info")(msg)

PROXY_CFG = yta_proxy_cfg()

# ----------------------- Request/Response hooks -----------------------
@app.before_request
def _inject_request_id():
    g.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
    g.request_start = time.perf_counter()

@app.after_request
def _add_common_headers(resp):
    # Security
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("Permissions-Policy", "interest-cohort=()")
    resp.headers.setdefault("Content-Security-Policy", "default-src 'none'")
    # Observability
    resp.headers["X-Request-ID"] = g.request_id
    return resp

# ----------------------- HTTP Sessions -----------------------
def _retrying_session(total=2, backoff=0.3) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET', 'POST'])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    if PROXY_DICT:
        s.proxies.update(PROXY_DICT)
    return s

def _youtube_session() -> requests.Session:
    s = _retrying_session()
    s.headers.update({"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "en-US,en;q=0.9"})
    s.cookies.set("CONSENT", CONSENT_COOKIE_VALUE, domain=".youtube.com")
    return s

session = _retrying_session()
yt_http = _youtube_session()

_yta_session_local = threading.local()

def _yta_session() -> requests.Session:
    sess: Optional[requests.Session] = getattr(_yta_session_local, "session", None)
    if sess is None:
        sess = _retrying_session(total=3, backoff=0.4)
        sess.cookies.set("CONSENT", CONSENT_COOKIE_VALUE, domain=".youtube.com")
        _yta_session_local.session = sess
    return sess

# ----------------------- Tiny TTL Cache -----------------------
class TTLCache:
    def __init__(self, ttl: int, capacity: int):
        self.ttl = ttl
        self.capacity = capacity
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            val = self._data.get(key)
            if not val: return None
            exp, obj = val
            if exp < time.time():
                self._data.pop(key, None)
                return None
            return obj

    def set(self, key: str, obj: Any):
        with self._lock:
            if len(self._data) >= self.capacity:
                # drop oldest by expiry (simple, good enough)
                oldest = sorted(self._data.items(), key=lambda kv: kv[1][0])[0][0]
                self._data.pop(oldest, None)
            self._data[key] = (time.time() + self.ttl, obj)

transcript_cache = TTLCache(TRANSCRIPT_CACHE_TTL, MAX_INMEM_ITEMS)
comments_cache   = TTLCache(COMMENTS_CACHE_TTL,   MAX_INMEM_ITEMS)

# In-flight single-flight to avoid duplicate upstream work
_inflight: Dict[str, threading.Event] = {}
_inflight_lock = threading.Lock()

# ----------------------- Utils -----------------------
def extract_video_id(video_or_url: str) -> Optional[str]:
    s = (video_or_url or "").strip()
    if not s: return None
    # Handle URL cases
    m = re.search(r"(?:v=|youtu\.be/|youtube\.com/(?:shorts/|embed/))([A-Za-z0-9_\-]{6,})", s)
    if m: return m.group(1)
    # Raw ID
    if re.fullmatch(r"[A-Za-z0-9_\-]{6,}", s): return s
    return None

def expand_langs(langs: Optional[List[str]]) -> List[str]:
    base = langs or TRANSCRIPT_LANGS
    out, seen = [], set()
    for code in base:
        c = code.lower().strip()
        if not c: continue
        # include regional variant + base
        parts = [c]
        if "-" in c: parts.append(c.split("-")[0])
        for p in parts:
            if p not in seen:
                out.append(p); seen.add(p)
    # Always append English as a safety fallback
    if "en" not in seen:
        out.append("en")
    return out[:8]

def ua_hdr(lang: str = "en") -> Dict[str,str]:
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": f"{lang};q=1.0, en;q=0.8"}

def accept_language_value(languages: List[str]) -> str:
    if not languages:
        return "en;q=1.0"
    parts: List[str] = []
    seen = set()
    for idx, code in enumerate(languages[:5]):
        base = code.split("-")[0]
        if base in seen:
            continue
        seen.add(base)
        q = max(0.1, 1.0 - idx * 0.1)
        parts.append(f"{base};q={q:.1f}")
    if "en" not in seen:
        parts.append("en;q=0.1")
    return ", ".join(parts)

def _parse_caption_text(raw: str, ext: str = "") -> Optional[str]:
    if not raw:
        return None
    data = raw.strip()
    if not data:
        return None
    ext = (ext or "").lower()
    if "<text" in data or ext.endswith("srv3"):
        text = " ".join(html.unescape(chunk) for chunk in re.findall(r">([^<]+)<", data))
        return text.strip() or None
    if ext.endswith("vtt") or data.startswith("WEBVTT"):
        lines = [ln.strip() for ln in data.splitlines() if ln and "-->" not in ln and not ln.startswith("WEBVTT")]
        text = " ".join(lines).strip()
        return text or None
    if ext.endswith("srt") or "-->" in data:
        lines = [ln.strip() for ln in data.splitlines() if ln and "-->" not in ln and not ln.isdigit()]
        text = " ".join(lines).strip()
        return text or None
    fallback = " ".join(data.split()).strip()
    return fallback or None

def deadline(seconds: float) -> float:
    """Return an absolute deadline based on perf_counter()."""
    return time.perf_counter() + seconds

def time_left(dl: float) -> float:
    """How many seconds remain until the given deadline (non-negative)."""
    return max(0.0, dl - time.perf_counter())

# ----------------------- Transcript: Primary + 2 Fallbacks -----------------------
def transcript_via_api(video_id: str, languages: List[str]) -> Optional[Dict[str, Any]]:
    """Primary: youtube-transcript-api (with proxy if configured).
    Order:
      1) Requested languages: manual → asr
      2) If not STRICT: any manual (original) → any asr (original)
      3) If ALLOW_TRANSLATE: translate an available manual to first requested
    """
    t0 = time.perf_counter()
    headers = ua_hdr(languages[0] if languages else "en")
    headers["Accept-Language"] = accept_language_value(languages)
    client = _yta_session()
    client.headers["User-Agent"] = headers["User-Agent"]
    client.headers["Accept-Language"] = headers["Accept-Language"]
    try:
        kwargs: Dict[str, Any] = {"http_client": client}
        if PROXY_CFG:
            kwargs["proxy_config"] = PROXY_CFG
        yta = YouTubeTranscriptApi(**kwargs)
        tr = yta.list_transcripts(video_id)

        # 1) Requested languages first (manual → asr)
        for lang in languages:
            try:
                tx = tr.find_manually_created_transcript([lang])
                data = tx.fetch()
                txt = " ".join([seg.get("text","").replace("\n"," ").strip() for seg in data]).strip()
                if txt:
                    log_event("info", "transcript_method_success", strategy="youtube-transcript-api_manual", duration_ms=int((time.perf_counter()-t0)*1000), text_len=len(txt), video_id=video_id)
                    return {"text": txt, "language": tx.language_code, "is_generated": False}
            except Exception:
                pass
        for lang in languages:
            try:
                tx = tr.find_generated_transcript([lang])
                data = tx.fetch()
                txt = " ".join([seg.get("text","").replace("\n"," ").strip() for seg in data]).strip()
                if txt:
                    log_event("info", "transcript_method_success", strategy="youtube-transcript-api_asr", duration_ms=int((time.perf_counter()-t0)*1000), text_len=len(txt), video_id=video_id)
                    return {"text": txt, "language": tx.language_code, "is_generated": True}
            except Exception:
                pass

        if not STRICT_LANGUAGES:
            # 2a) Any manual/original (prefer original if configured)
            if PREFER_ORIGINAL:
                for tx in tr:
                    try:
                        if not getattr(tx, "is_generated", False):
                            data = tx.fetch()
                            txt = " ".join([seg.get("text","").replace("\n"," ").strip() for seg in data]).strip()
                            if txt:
                                log_event("info", "transcript_method_success", strategy="youtube-transcript-api_manual_any", duration_ms=int((time.perf_counter()-t0)*1000), text_len=len(txt), video_id=video_id)
                                return {"text": txt, "language": tx.language_code, "is_generated": False}
                    except Exception:
                        continue
            # 2b) Any ASR/original
            for tx in tr:
                try:
                    if getattr(tx, "is_generated", False):
                        data = tx.fetch()
                        txt = " ".join([seg.get("text","").replace("\n"," ").strip() for seg in data]).strip()
                        if txt:
                            log_event("info", "transcript_method_success", strategy="youtube-transcript-api_asr_any", duration_ms=int((time.perf_counter()-t0)*1000), text_len=len(txt), video_id=video_id)
                            return {"text": txt, "language": tx.language_code, "is_generated": True}
                except Exception:
                    continue

            # 3) Optional: translate available manual to the first requested language
            if ALLOW_TRANSLATE and languages:
                target = languages[0]
                for tx in tr:
                    try:
                        if not getattr(tx, "is_generated", False):
                            # Does this manual transcript advertise translation to 'target'?
                            trans_langs = getattr(tx, "translation_languages", None) or []
                            if any((isinstance(tl, dict) and tl.get("language_code") == target) or (isinstance(tl, str) and tl == target) for tl in trans_langs):
                                txx = tx.translate(target)
                                data = txx.fetch()
                                txt = " ".join([seg.get("text","").replace("\n"," ").strip() for seg in data]).strip()
                                if txt:
                                    log_event("info", "transcript_method_success", strategy="youtube-transcript-api_manual_translated", duration_ms=int((time.perf_counter()-t0)*1000), text_len=len(txt), video_id=video_id)
                                    return {"text": txt, "language": target, "is_generated": False}
                    except Exception:
                        continue

    except (NoTranscriptFound, TranscriptsDisabled) as exc:
        log_event("warning", "transcript_api_no_transcript", video_id=video_id, error=str(exc))
        return None
    except Exception as e:
        if "blocked" in str(e).lower():
            setattr(g, "transcript_blocked", True)
        log_event("warning", "transcript_api_error", video_id=video_id, error=str(e))
        return None
    return None

def _timedtext_list(video_id: str, use_proxy: bool) -> List[Tuple[str,str]]:
    """Return list of (lang_code, kind)."""
    params = {"v": video_id, "type": "list"}
    try:
        r = yt_http.get("https://www.youtube.com/api/timedtext", params=params, timeout=TIMEOUT_YT, proxies=(requests_proxies() if use_proxy else None))
        if r.status_code != 200: return []
        tags = re.findall(r"<track\s+([^>]+)>", r.text)
        out: List[Tuple[str,str]] = []
        for attrs in tags:
            m = re.search(r'lang_code="([^"]+)"', attrs)
            if not m: continue
            code = m.group(1)
            kind = 'asr' if 'kind="asr"' in attrs else 'manual'
            out.append((code, kind))
        return out
    except Exception:
        return []

def _timedtext_fetch(video_id: str, code: str, kind_asr: bool, use_proxy: bool, target: Optional[str] = None) -> Optional[str]:
    params = {"v": video_id, "fmt": "vtt", "lang": code}
    if kind_asr: params["kind"] = "asr"
    if target: params["tlang"] = target
    try:
        r = yt_http.get("https://www.youtube.com/api/timedtext", params=params, headers=ua_hdr(code), timeout=TIMEOUT_TIMEDTEXT, proxies=(requests_proxies() if use_proxy else None))
        if r.status_code != 200: return None
        txt = r.text.strip()
        if not txt or ("<transcript/>" in txt): return None
        # Minimal parse: drop WEBVTT lines/timestamps → join text
        lines = [ln.strip() for ln in txt.splitlines() if ln and not ln.startswith("WEBVTT") and not re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}", ln)]
        out = " ".join(lines).strip()
        return out or None
    except Exception:
        return None

def transcript_via_timedtext(video_id: str, languages: List[str]) -> Optional[Dict[str,Any]]:
    """Fallback #1: YouTube timedtext (manual → asr, direct → proxy), with relaxed language fallback."""
    tracks = _timedtext_list(video_id, use_proxy=False) or _timedtext_list(video_id, use_proxy=True)
    if not tracks: return None
    bases = list(dict.fromkeys([c.split("-")[0] for c in languages]))  # unique order

    # 1) Requested bases: Manual
    for base in bases:
        for code, kind in tracks:
            if kind == "manual" and code.startswith(base):
                out = _timedtext_fetch(video_id, code, False, use_proxy=False) or _timedtext_fetch(video_id, code, False, use_proxy=True)
                if out:
                    return {"text": out, "language": code, "is_generated": False}
    # 2) Requested bases: ASR
    for base in bases:
        for code, kind in tracks:
            if kind == "asr" and code.startswith(base):
                out = _timedtext_fetch(video_id, code, True, use_proxy=False) or _timedtext_fetch(video_id, code, True, use_proxy=True)
                if out:
                    return {"text": out, "language": code, "is_generated": True}

    if not STRICT_LANGUAGES:
        if ALLOW_TRANSLATE and bases:
            target = bases[0]
            for code, kind in tracks:
                if kind == "manual" and not code.startswith(target):
                    out = _timedtext_fetch(video_id, code, False, use_proxy=False, target=target) or _timedtext_fetch(video_id, code, False, use_proxy=True, target=target)
                    if out:
                        return {"text": out, "language": target, "is_generated": False}
        # 3) Any manual track
        for code, kind in tracks:
            if kind == "manual":
                out = _timedtext_fetch(video_id, code, False, use_proxy=False) or _timedtext_fetch(video_id, code, False, use_proxy=True)
                if out:
                    return {"text": out, "language": code, "is_generated": False}
        # 4) Any ASR track
        for code, kind in tracks:
            if kind == "asr":
                out = _timedtext_fetch(video_id, code, True, use_proxy=False) or _timedtext_fetch(video_id, code, True, use_proxy=True)
                if out:
                    return {"text": out, "language": code, "is_generated": True}
    return None

def transcript_via_piped(video_id: str, languages: List[str], deadline_ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """Fallback via lightweight Piped mirrors."""
    prefs: List[str] = []
    for code in languages:
        base = code.split("-")[0].lower()
        if base and base not in prefs:
            prefs.append(base)
    if not prefs:
        prefs = ["en"]
    headers = ua_hdr(prefs[0])
    proxies = requests_proxies()

    def pick_caption(subs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for base in prefs:
            base_l = base.lower()
            for item in subs:
                lang_fields = [
                    str(item.get("code", "")),
                    str(item.get("language", "")),
                    str(item.get("languageCode", "")),
                ]
                lang_fields = [val.lower() for val in lang_fields if val]
                if any(val == base_l or val.startswith(base_l) for val in lang_fields):
                    return item
        return subs[0] if subs else None

    for host in ALT_PIPED_HOSTS:
        if deadline_ts and time_left(deadline_ts) <= 0.5:
            break
        try:
            base = host if host.endswith('/') else f"{host}/"
            captions_url = urljoin(base, f"api/v1/captions/{video_id}")
            meta = session.get(captions_url, headers=headers, timeout=2.5, proxies=proxies or None)
            if meta.status_code != 200:
                continue
            data = meta.json()
            subs = data.get("captions") or []
            chosen = pick_caption(subs)
            if not chosen:
                continue
            url = (chosen.get("url") or "").strip()
            if not url:
                continue
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                url = urljoin(base, url.lstrip("/"))
            ext = Path(urlparse(url).path).suffix
            resp = session.get(url, headers=headers, timeout=2.5, proxies=proxies or None)
            if resp.status_code != 200:
                continue
            text = _parse_caption_text(resp.text, ext)
            if text:
                log_event("info", "transcript_method_success", strategy="piped_api",
                          video_id=video_id, text_len=len(text), mirror=host)
                lang_code = chosen.get("languageCode") or chosen.get("code") or prefs[0]
                is_generated = bool(chosen.get("autoGenerated") or chosen.get("automatic"))
                return {"text": text, "language": lang_code, "is_generated": is_generated}
        except Exception as exc:
            log_event("warning", "piped_transcript_error", video_id=video_id, mirror=host, error=str(exc))
            continue
    return None

def transcript_via_ytdlp(video_id: str, languages: List[str], max_seconds: float) -> Optional[Dict[str,Any]]:
    """Fallback #2: yt-dlp subtitle extraction (manual→auto), bounded by a per-step budget."""
    if max_seconds < 1.0:
        return None
    t0 = time.perf_counter()
    lang_list = languages or TRANSCRIPT_LANGS
    sub_langs: List[str] = []
    for code in lang_list:
        c = code.strip()
        if not c:
            continue
        sub_langs.extend([f"{c}.*", c])
    opts = {
        "quiet": True,
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": sub_langs or ["en.*", "en"],
        "subtitlesformat": "best[ext=srv3]/best[ext=vtt]/best[ext=srt]",
        "socket_timeout": max(1.0, min(max_seconds, TIMEOUT_YTDLP)),
        "nocheckcertificate": True,
        "extractor_args": {"youtube": ["player_client=ios"]},
        "http_headers": {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.8",
            "Cookie": f"CONSENT={CONSENT_COOKIE_VALUE}",
        },
    }
    if PROXY_URL:
        opts["proxy"] = PROXY_URL
    if YTDL_COOKIE_FILE:
        opts["cookiefile"] = YTDL_COOKIE_FILE
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            opts["outtmpl"] = os.path.join(tmp_dir, "%(id)s.%(ext)s")
            with YoutubeDL(opts) as ydl:
                ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
            for ext in ("srv3", "vtt", "srt"):
                target = next(Path(tmp_dir).glob(f"{video_id}*.{ext}"), None)
                if not target:
                    continue
                raw = target.read_text(encoding="utf-8", errors="ignore")
                text = _parse_caption_text(raw, ext)
                if not text:
                    continue
                log_event("info", "transcript_method_success", strategy="yt-dlp_subs",
                          duration_ms=int((time.perf_counter()-t0)*1000), text_len=len(text), video_id=video_id)
                guess_lang = (languages[0] if languages else "en") or "en"
                return {"text": text, "language": guess_lang, "is_generated": True}
    except Exception as e:
        log_event("warning", "ytdlp_subs_error", video_id=video_id, error=str(e))
    return None

def make_404_not_available() -> tuple:
    resp = make_response(jsonify({"error": "Transcript not available"}), 404)
    resp.headers["Cache-Control"] = "public, max-age=600"
    return resp

# ----------------------- Comments: Primary + Fallback -----------------------
def comments_via_downloader(video_id: str) -> List[str]:
    t0 = time.perf_counter()
    try:
        dl = YoutubeCommentDownloader()
        gen = dl.get_comments(f"https://www.youtube.com/watch?v={video_id}")
        out = []
        for i, c in enumerate(gen):
            txt = (c.get("text") or "").strip()
            if txt: out.append(txt)
            if len(out) >= COMMENT_LIMIT: break
        log_event("info", "comment_method_success", method="youtube-comment-downloader", count=len(out), duration_ms=int((time.perf_counter()-t0)*1000), video_id=video_id)
        return out
    except Exception as e:
        log_event("warning", "comment_method_failure", method="youtube-comment-downloader", error=str(e), duration_ms=int((time.perf_counter()-t0)*1000), video_id=video_id)
        return []

def comments_via_ytdlp(video_id: str, max_seconds: float) -> List[str]:
    """Fallback via yt-dlp, but bounded by a per-step budget."""
    if max_seconds < 1.0:
        return []
    t0 = time.perf_counter()
    opts = _YDL_BASE.copy()
    opts["getcomments"] = True
    # Bound yt-dlp socket timeout to our per-step budget
    opts["socket_timeout"] = max(1.0, min(max_seconds, TIMEOUT_YTDLP))
    if _proxy_url():
        opts["proxy"] = _proxy_url()
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        raw = info.get("comments") or []
        out = []
        for c in raw:
            val = c.get("text") or c.get("comment") or ""
            if val: out.append(val.strip())
            if len(out) >= COMMENT_LIMIT: break
        if out:
            log_event("info", "comment_method_success", method="yt-dlp_comments", count=len(out), duration_ms=int((time.perf_counter()-t0)*1000), video_id=video_id)
            return out
        log_event("warning", "comment_method_failure", method="yt-dlp_comments", reason="no_comments", duration_ms=int((time.perf_counter()-t0)*1000), video_id=video_id)
        return []
    except Exception as e:
        log_event("warning", "comment_method_failure", method="yt-dlp_comments", error=str(e), duration_ms=int((time.perf_counter()-t0)*1000), video_id=video_id)
        return []

# ----------------------- Endpoints -----------------------
@app.route("/transcript", methods=["GET"])
@limiter.limit("1000/hour;200/minute")
def transcript_endpoint():
    video_or_url = request.args.get("videoId", "")
    if not video_or_url:
        return jsonify({"error": "videoId parameter is missing"}), 400
    video_id = extract_video_id(video_or_url)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    # languages parameter (comma list) or default
    langs_raw = request.args.get("languages", "") or ""
    langs = [c.strip().lower() for c in langs_raw.split(",") if c.strip()] if langs_raw else None
    languages = expand_langs(langs)

    setattr(g, "transcript_blocked", False)

    cache_key = f"{video_id}::langs={','.join(languages)}"
    dl = deadline(TRANSCRIPT_TOTAL_BUDGET_S)

    # in-memory cache
    hit = transcript_cache.get(cache_key)
    if hit:
        log_event("info", "transcript_cache_hit", video_id=video_id, text_len=len(hit.get("text","")))
        return jsonify({"video_id": video_id, **hit}), 200

    # persistent cache
    with shelve.open(DB_TRANSCRIPTS) as db:
        val = db.get(cache_key)
        if isinstance(val, dict) and val.get("status") == NOT_AVAILABLE_SENTINEL:
            ts = float(val.get("ts", 0) or 0)
            if ts and (time.time() - ts) <= NOT_AVAILABLE_TTL:
                log_event("info", "transcript_persisted_not_available", video_id=video_id)
                return make_404_not_available()
            db.pop(cache_key, None)
            val = None
        elif val == NOT_AVAILABLE_SENTINEL:
            db.pop(cache_key, None)
            val = None
        if isinstance(val, dict) and val.get("text"):
            transcript_cache.set(cache_key, val)
            log_event("info", "transcript_persisted_hit", video_id=video_id, text_len=len(val.get("text","")))
            return jsonify({"video_id": video_id, **val}), 200
        if isinstance(val, str) and val:
            payload = {"text": val, "language": "unknown", "is_generated": False}
            transcript_cache.set(cache_key, payload)
            log_event("info", "transcript_persisted_hit", video_id=video_id, text_len=len(val))
            return jsonify({"video_id": video_id, **payload}), 200

    # single-flight
    evt = None
    leader = False
    with _inflight_lock:
        evt = _inflight.get(cache_key)
        if not evt:
            evt = threading.Event()
            _inflight[cache_key] = evt
            leader = True

    try:
        if not leader:
            # Followers wait briefly, not 15s
            evt.wait(INFLIGHT_FOLLOWER_WAIT_S)
            hit = transcript_cache.get(cache_key)
            if hit:
                log_event("info", "transcript_inflight_follower_hit", video_id=video_id)
                return jsonify({"video_id": video_id, **hit}), 200
            # Continue as non-leader if nothing showed up, but budget still applies

        methods = []
        # Primary
        methods.append(("youtube-transcript-api", lambda: transcript_via_api(video_id, languages)))
        # Fallback #1
        methods.append(("timedtext", lambda: transcript_via_timedtext(video_id, languages)))

        def maybe_ytdlp():
            rem = time_left(dl)
            if rem >= YTDLP_STEP_BUDGET_S:
                return transcript_via_ytdlp(video_id, languages, rem)
            return None

        methods.append(("yt-dlp_subs", maybe_ytdlp))
        # Fallback mirrors (only if time left after yt-dlp)
        methods.append(("piped_api", lambda: transcript_via_piped(video_id, languages, dl)))

        payload = None
        for name, func in methods:
            if time_left(dl) <= 0.3:
                break
            log_event("info", "transcript_method_attempt", method=name, video_id=video_id)
            payload = func()
            if payload and payload.get("text"):
                break

        if not payload or not payload.get("text"):
            if getattr(g, "transcript_blocked", False):
                return make_404_not_available()
            with shelve.open(DB_TRANSCRIPTS) as db:
                db[cache_key] = {"status": NOT_AVAILABLE_SENTINEL, "ts": time.time()}
            return make_404_not_available()

        transcript_cache.set(cache_key, payload)
        with shelve.open(DB_TRANSCRIPTS) as db:
            db[cache_key] = payload

        log_event("info", "transcript_result", strategy=("manual" if not payload.get("is_generated") else "asr"),
                  text_len=len(payload.get("text","")), video_id=video_id, duration_ms=int((time.perf_counter()-g.request_start)*1000))
        return jsonify({"video_id": video_id, **payload}), 200
    finally:
        if leader:
            with _inflight_lock:
                e = _inflight.pop(cache_key, None)
                if e: e.set()
            log_event("info", "transcript_inflight_done", video_id=video_id)

@app.route("/comments", methods=["GET"])
@limiter.limit("120/hour;20/minute")
def comments_endpoint():
    video_or_url = request.args.get("videoId", "")
    if not video_or_url:
        return jsonify({"error": "videoId parameter is missing"}), 400
    video_id = extract_video_id(video_or_url)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    dl = deadline(COMMENTS_TOTAL_BUDGET_S)

    cache_key = f"{video_id}"
    hit = comments_cache.get(cache_key)
    if hit:
        log_event("info", "comments_cache_hit", video_id=video_id, count=len(hit))
        resp = jsonify({"video_id": video_id, "comments": hit})
        resp.headers["Cache-Control"] = "public, max-age=60"
        return resp, 200

    with shelve.open(DB_COMMENTS) as db:
        if cache_key in db:
            val = db[cache_key]
            comments_cache.set(cache_key, val)
            log_event("info", "comments_persisted_hit", video_id=video_id, count=len(val))
            resp = jsonify({"video_id": video_id, "comments": val})
            resp.headers["Cache-Control"] = "public, max-age=60"
            return resp, 200

    # Primary (fast)
    t0 = time.perf_counter()
    comments = comments_via_downloader(video_id)
    if comments:
        comments = comments[:COMMENT_LIMIT]
        comments_cache.set(cache_key, comments)
        with shelve.open(DB_COMMENTS) as db:
            db[cache_key] = comments
        log_event("info", "comments_result", strategy="youtube-comment-downloader",
                  video_id=video_id, count=len(comments),
                  duration_ms=int((time.perf_counter()-t0)*1000), request_id=g.request_id)
        resp = jsonify({"video_id": video_id, "comments": comments})
        resp.headers["Cache-Control"] = "public, max-age=60"
        return resp, 200

    # Only attempt yt-dlp if there's enough time left
    rem = time_left(dl)
    if rem >= YTDLP_STEP_BUDGET_S:
        comments = comments_via_ytdlp(video_id, rem)
        if comments:
            comments = comments[:COMMENT_LIMIT]
            comments_cache.set(cache_key, comments)
            with shelve.open(DB_COMMENTS) as db:
                db[cache_key] = comments
            log_event("info", "comments_result", strategy="yt-dlp", video_id=video_id,
                      count=len(comments), duration_ms=int((time.perf_counter()-t0)*1000), request_id=g.request_id)
            resp = jsonify({"video_id": video_id, "comments": comments})
            resp.headers["Cache-Control"] = "public, max-age=60"
            return resp, 200

    # Budget exhausted or no comments → graceful 200 + []
    log_event("warning", "comments_result_empty", video_id=video_id,
              duration_ms=int((time.perf_counter()-t0)*1000), request_id=g.request_id)
    resp = jsonify({"video_id": video_id, "comments": []})
    resp.headers["Cache-Control"] = "public, max-age=15"
    return resp, 200

# ----------------------- OpenAI Responses Proxy (non-streaming) -----------------------
@app.route("/openai/responses", methods=["POST"])
@limiter.limit("200/hour;50/minute")
def openai_proxy():
    if not OPENAI_API_KEY:
        return jsonify({"error": "OpenAI API key not configured on server"}), 500
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            return jsonify({"error": "Invalid JSON payload"}), 400
    except Exception:
        return jsonify({"error": "Bad request JSON"}), 400

    # Ensure store=true unless the client explicitly chose otherwise
    payload.setdefault("store", True)

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    t0 = time.perf_counter()
    try:
        r = session.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=TIMEOUT_OPENAI)
        content = r.json() if r.content else {}
        log_event("info", "openai_proxy_response",
                  status=r.status_code, duration_ms=int((time.perf_counter()-t0)*1000),
                  model=str(payload.get("model","")), usage=content.get("usage"))
        resp = make_response(jsonify(content), r.status_code)
        resp.headers["Cache-Control"] = "no-store"
        return resp
    except requests.Timeout:
        log_event("error", "openai_proxy_timeout", model=str(payload.get("model","")))
        return jsonify({"error": "Request to OpenAI timed out"}), 504
    except Exception as e:
        log_event("error", "openai_proxy_error", error=str(e))
        return jsonify({"error": "Internal server error during OpenAI proxy"}), 500

@app.route("/openai/responses/<response_id>", methods=["GET"])
@limiter.limit("200/hour;50/minute")
def openai_get_response(response_id: str):
    if not OPENAI_API_KEY:
        return jsonify({"error": "OpenAI API key not configured"}), 500
    try:
        r = session.get(f"https://api.openai.com/v1/responses/{response_id}",
                        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"}, timeout=10)
        content = r.json() if r.content else {}
        return make_response(jsonify(content), r.status_code)
    except requests.Timeout:
        return jsonify({"error": "OpenAI fetch timed out"}), 504
    except Exception as e:
        log_event("error", "openai_get_response_error", error=str(e))
        return jsonify({"error": "Internal server error during OpenAI GET"}), 500

# ----------------------- Static/Utility Routes -----------------------
@app.route("/")
def root():
    return jsonify({"service": APP_NAME, "status": "ok"})

@app.route("/_health")
def health():
    return jsonify({"ok": True})

@app.route("/privacy")
def privacy():
    return jsonify({"title": "Privacy", "status": "ok"})

@app.route("/terms")
def terms():
    return jsonify({"title": "Terms", "status": "ok"})

@app.route("/support")
def support():
    # keep route for app links
    return jsonify({"email": "support@tuliai.com", "status": "ok"})

@app.route("/favicon.ico")
def favicon():
    # Avoid noisy 404s in logs
    return ("", 204)

# ----------------------- Entrypoint -----------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
