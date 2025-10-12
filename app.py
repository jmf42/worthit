import os
import itertools
import shelve
import re
import json
import logging
import time
import random
import html
import tempfile
import pathlib
from typing import List, Optional, Dict, Any
from functools import lru_cache
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError, Future

from flask import Flask, request, jsonify, g, make_response
import uuid
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
    RequestBlocked,
    AgeRestricted
)
from youtube_transcript_api._errors import CouldNotRetrieveTranscript
from cachetools import TTLCache
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

from yt_dlp import YoutubeDL

import functools

# --- Patch for youtube-transcript-api compatibility -----
import inspect

# Determine accepted parameters for YouTubeTranscriptApi.list_transcripts at runtime
_LIST_TRANSCRIPT_ACCEPTED_PARAMS = set(
    inspect.signature(YouTubeTranscriptApi.list_transcripts).parameters.keys()
)

def _list_transcripts_safe(video_id: str, **kwargs):
    """
    Call YouTubeTranscriptApi.list_transcripts while passing only the parameters
    that are actually accepted by the installed library version.  This prevents
    runtime TypeError like 'unexpected keyword argument'.
    """
    filtered = {k: v for k, v in kwargs.items() if k in _LIST_TRANSCRIPT_ACCEPTED_PARAMS}
    return YouTubeTranscriptApi.list_transcripts(video_id, **filtered)
 
from youtube_comment_downloader import YoutubeCommentDownloader
from flask import send_from_directory


# --- Configuration ---
APP_NAME = "WorthItService"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(min(4, (os.cpu_count() or 1)))))
MAX_TIMEDTEXT_LANGS = int(os.getenv("MAX_TIMEDTEXT_LANGS", "3"))
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "50"))
TRANSCRIPT_CACHE_SIZE = int(os.getenv("TRANSCRIPT_CACHE_SIZE", "200"))
TRANSCRIPT_CACHE_TTL = int(os.getenv("TRANSCRIPT_CACHE_TTL", "7200")) # 2 hours
# Preferred languages for transcript API (comma‑separated env var)
# Default priority (override via env in prod if needed):
# en, hi, es, pt, id, ja, ru, ar, bn, tr, de, fr, vi, ko, th
# Parse preferred languages from env and sanitize (trim + drop empties)
TRANSCRIPT_LANGS = [
    c.strip() for c in os.getenv(
        "TRANSCRIPT_LANGS",
        "en,hi,es,pt,id,ja,ru,ar,bn,tr,de,fr,vi,ko,th"
    ).split(",") if c.strip()
]
TRANSCRIPT_PRIMARY_LANG_LIMIT = max(int(os.getenv("TRANSCRIPT_PRIMARY_LANG_LIMIT", "2")), 1)
TRANSCRIPT_LANG_EXPANSION_LIMIT = max(int(os.getenv("TRANSCRIPT_LANG_EXPANSION_LIMIT", "6")), 1)
TRANSCRIPT_FALLBACK_DELAY_SEC = max(float(os.getenv("TRANSCRIPT_FALLBACK_DELAY_SEC", "3")), 0.5)
# (Deprecated) TRANSCRIPT_HTTP_TIMEOUT was unused; per-request timeouts are set on sessions.
COMMENT_CACHE_SIZE = int(os.getenv("COMMENT_CACHE_SIZE", "150"))
COMMENT_CACHE_TTL = int(os.getenv("COMMENT_CACHE_TTL", "7200")) # 2 hours
# In Cloud Run, only /tmp is writable. Allow override via env.
PERSISTENT_CACHE_DIR = os.getenv("CACHE_DIR", "/tmp/persistent_cache")
PERSISTENT_TRANSCRIPT_DB = os.path.join(PERSISTENT_CACHE_DIR, "transcript_cache.db")
PERSISTENT_COMMENT_DB = os.path.join(PERSISTENT_CACHE_DIR, "comment_cache.db")
# Cap per-pull items when iterating comment generators (safety net)
MAX_COMMENTS_FETCH = int(os.getenv("MAX_COMMENTS_FETCH", str(COMMENT_LIMIT)))
# YTDL Cookie file configuration
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE", "/etc/secrets/cookies_chrome2.txt")
CONSENT_COOKIE_HEADER = "CONSENT=YES+cb.20210328-17-p0.en+FX+888"

# ------------------------------------------------------------------
# Universal CONSENT cookie (avoids 204/empty captions from YouTube)
# If an explicit cookie file is configured but does not exist, ignore it.
if YTDL_COOKIE_FILE and not os.path.isfile(YTDL_COOKIE_FILE):
    YTDL_COOKIE_FILE = None

# If no cookie file is configured, create a minimal consent cookie in writable cache dir.
if not YTDL_COOKIE_FILE:
    CONSENT_COOKIE_STRING = (
        "# Netscape HTTP Cookie File\n"
        ".youtube.com\tTRUE\t/\tFALSE\t2145916800\tCONSENT\tYES+cb.20210328-17-p0.en+FX+888\n"
    )
    _consent_path = os.path.join(PERSISTENT_CACHE_DIR, "consent_cookies.txt")
    os.makedirs(PERSISTENT_CACHE_DIR, exist_ok=True)
    if not os.path.isfile(_consent_path):
        with open(_consent_path, "w", encoding="utf-8") as fh:
            fh.write(CONSENT_COOKIE_STRING)
    YTDL_COOKIE_FILE = _consent_path
# ------------------------------------------------------------------


# --- User-Agent Rotation ---
# A list of realistic, modern browser User-Agents to avoid being flagged as a bot.
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
]

# Ensure cache directory exists
os.makedirs(PERSISTENT_CACHE_DIR, exist_ok=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Thread-local session reuse for youtube-transcript-api ------------------
_thread_local_ytt_session = threading.local()

class _TimeoutSession(requests.Session):
    def __init__(self, timeout: float):
        super().__init__()
        self._default_timeout = timeout

    def request(self, *args, **kwargs):
        if "timeout" not in kwargs or kwargs["timeout"] is None:
            kwargs["timeout"] = self._default_timeout
        return super().request(*args, **kwargs)

def _build_ytt_session() -> requests.Session:
    default_timeout = float(os.getenv("YTT_REQUEST_TIMEOUT", "15"))
    session = _TimeoutSession(default_timeout)
    retry_total = int(os.getenv("YTT_SESSION_RETRY_TOTAL", "3"))
    backoff = float(os.getenv("YTT_SESSION_BACKOFF", "0.5"))
    pool_connections = int(os.getenv("YTT_SESSION_POOL_CONNECTIONS", "20"))
    pool_maxsize = int(os.getenv("YTT_SESSION_POOL_MAXSIZE", "20"))
    retry = Retry(
        total=retry_total,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]),
    )
    adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": CONSENT_COOKIE_HEADER,
    })
    return session

def get_thread_local_ytt_session(user_agent: str, accept_language: str) -> requests.Session:
    session: Optional[requests.Session] = getattr(_thread_local_ytt_session, "session", None)
    if session is None:
        session = _build_ytt_session()
        _thread_local_ytt_session.session = session
    # Update per-request headers (thread-local session ensures no cross-thread races)
    session.headers["User-Agent"] = user_agent
    session.headers["Accept-Language"] = accept_language
    session.headers["Cookie"] = CONSENT_COOKIE_HEADER
    return session


# --- Logging Setup ---

def make_transcript_payload(
    text: str,
    language_code: str,
    language_label: str,
    is_generated: bool,
    tracks: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "text": text,
        "language": {
            "code": language_code or "unknown",
            "label": language_label or language_code or "unknown",
            "is_generated": bool(is_generated),
        },
        "tracks": tracks or [],
    }

def make_fallback_payload(
    text: str,
    languages: Optional[List[str]],
    is_generated: bool,
) -> Dict[str, Any]:
    limited = limit_languages(languages)
    code = "unknown"
    label = "unknown"
    if limited:
        base = (limited[0] or "").strip()
        if base:
            code = base.split("-")[0] or base
            label = base
    return make_transcript_payload(text, code, label, is_generated, [])

def ensure_payload(value: Any, languages: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict) and "text" in value:
        return value
    if isinstance(value, str):
        return make_fallback_payload(value, languages, is_generated=False)
    return None

def setup_logging():
    """
    Configure a single JSON logger to stdout with no propagation to avoid
    duplicates under Gunicorn. Root stays at WARNING to keep 3rd‑party quiet.
    
    Cloud Run best practice: write to stdout/stderr only; avoid local files.
    """
    # Root for libraries
    logging.basicConfig(level=logging.WARNING)

    logger = logging.getLogger(APP_NAME)
    logger.setLevel(LOG_LEVEL)
    # Prevent messages from bubbling to root/gunicorn handlers (duplication)
    logger.propagate = False
    # Ensure a single handler
    if logger.hasHandlers():
        logger.handlers.clear()

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            try:
                base = getattr(record, "structured", None)
                if not isinstance(base, dict):
                    # Fallback – wrap record message
                    base = {"message": record.getMessage()}
                base.setdefault("logger", APP_NAME)
                base.setdefault("severity", record.levelname)
                return json.dumps(base, default=str, ensure_ascii=False)
            except Exception:
                return json.dumps({
                    "logger": APP_NAME,
                    "severity": record.levelname,
                    "message": record.getMessage(),
                }, ensure_ascii=False)

    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    ch.setFormatter(JsonFormatter())
    logger.addHandler(ch)

    # Keep noisy libs contained
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    return logger

logger = setup_logging()
app_start_time = time.time()





# --- HTTP Session with Retries ---

session = requests.Session()
session.request = functools.partial(session.request, timeout=15)  # ≤15 s per external request
retry_cfg = Retry(
    total=3, connect=3, read=3, status=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(['GET', 'POST']), # Retry only for safe methods or idempotent ones
    raise_on_status=False
)
session.mount("https://", HTTPAdapter(max_retries=retry_cfg))
session.mount("http://", HTTPAdapter(max_retries=retry_cfg))

# --- Global YouTube HTTP client with consent cookie ---
youtube_http = requests.Session()
# Always pretend to be a normal browser
youtube_http.headers.update({
    "User-Agent": random.choice(USER_AGENTS),
    "Accept-Language": "en-US,en;q=0.9"
})
# Bypass EU‑consent page that breaks transcripts
youtube_http.cookies.set("CONSENT", "YES+1", domain=".youtube.com")
# Re‑use same retry / timeout policy as the default session
youtube_http.request = functools.partial(youtube_http.request, timeout=15)
youtube_http.mount("https://", HTTPAdapter(max_retries=retry_cfg))
youtube_http.mount("http://", HTTPAdapter(max_retries=retry_cfg))


def get_random_user_agent_header():
    """Returns a dictionary with a randomly chosen User-Agent header and Accept-Language."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.8"
    }

# --- Minimal timedtext direct fetch (non-official) ---------------------------
def _timedtext_fetch_vtt(video_id: str, lang: str, asr: bool, use_proxy: bool = False, request_id: str = "", tlang: Optional[str] = None) -> Optional[str]:
    try:
        params = {
            "v": video_id,
            "fmt": "vtt",
            "lang": lang
        }
        if asr:
            params["kind"] = "asr"
        if tlang:
            params["tlang"] = tlang
        # Prefer per-request Accept-Language reflecting current attempt
        headers = {
            "Accept-Language": f"{lang};q=1.0, en;q=0.8",
            "User-Agent": random.choice(USER_AGENTS)
        }
        # Use proxy only if requested and configured (no cookies required)
        proxies = get_proxy_dict() if use_proxy else {}
        r = youtube_http.get("https://www.youtube.com/api/timedtext", params=params, headers=headers, timeout=3, proxies=proxies)
        if r.status_code != 200:
            return None
        txt = r.text.strip()
        if not txt or txt.startswith("<?xml") and "<transcript/>" in txt:
            return None
        # Basic VTT parsing: drop headers and timestamps
        lines = [l.strip() for l in txt.splitlines() if l and "-->" not in l and not l.startswith("WEBVTT") and not l.startswith("Kind:")]
        out = " ".join(lines).strip()
        return out or None
    except Exception as e:
        log_event('debug', 'timedtext_fetch_error', extra={"video_id": video_id, "lang": lang, "asr": asr, "error": str(e), "request_id": request_id})
    return None

def _timedtext_list_tracks(video_id: str, use_proxy: bool = False, request_id: str = "") -> list[tuple[str, str]]:
    """Return list of (lang_code, kind) for available timedtext tracks.
    Robustly parses <track ...> elements and infers kind=manual when missing.
    """
    try:
        params = {"v": video_id, "type": "list"}
        proxies = get_proxy_dict() if use_proxy else {}
        r = youtube_http.get("https://www.youtube.com/api/timedtext", params=params, timeout=6, proxies=proxies)
        if r.status_code != 200:
            return []
        xml = r.text
        # Extract each <track ...> tag
        tags = re.findall(r"<track\s+([^>]+)>", xml)
        out: list[tuple[str, str]] = []
        for attrs in tags:
            # lang_code is required
            m_lang = re.search(r"lang_code=\"([^\"]+)\"", attrs)
            if not m_lang:
                continue
            code = m_lang.group(1)
            # kind may be missing; treat as manual unless explicitly 'asr'
            kind = 'manual'
            if re.search(r"kind=\"asr\"", attrs):
                kind = 'asr'
            out.append((code, kind))
        return out
    except Exception as e:
        log_event('debug', 'timedtext_list_error', extra={"video_id": video_id, "error": str(e), "request_id": request_id})
        return []

def timedtext_try_languages(video_id: str,
                            languages: Optional[List[str]],
                            request_id: str = "",
                            allow_translate: bool = True,
                            allow_proxy: bool = True) -> Optional[Dict[str, Any]]:
    """Try timedtext using discovery with this order:
    1) Manual (direct then proxy)
    2) ASR (direct then proxy)
    3) Translate manual to first preferred base (direct then proxy) – only if allow_translate
    """
    # Default to configured preferences if caller provided none
    if not languages:
        languages = TRANSCRIPT_LANGS
    languages = limit_languages(languages)
    # Build base language set (e.g., 'es' from 'es-419') preserving order
    base_langs: list[str] = []
    seen = set()
    for code in languages:
        base = code.split('-')[0].lower()
        if base and base not in seen:
            base_langs.append(base)
            seen.add(base)

    if base_langs and len(base_langs) > MAX_TIMEDTEXT_LANGS:
        original = base_langs[:]
        base_langs = base_langs[:MAX_TIMEDTEXT_LANGS]
        log_event('debug', 'timedtext_language_trim', video_id=video_id, trimmed_from=original, trimmed_to=base_langs, request_id=request_id)

    # Try direct list
    tracks = _timedtext_list_tracks(video_id, use_proxy=False, request_id=request_id)
    if not tracks and allow_proxy and get_proxy_dict():
        # Try via proxy if no tracks direct
        tracks = _timedtext_list_tracks(video_id, use_proxy=True, request_id=request_id)

    track_manifest = []
    if tracks:
        track_manifest = [
            {
                "code": code,
                "label": code,
                "is_generated": kind == "asr",
                "is_translatable": False,
                "base_url": "",
            }
            for code, kind in tracks
        ]
        log_event('info', 'timedtext_tracks_found', extra={"video_id": video_id, "count": len(tracks), "sample": tracks[:3], "request_id": request_id})
        # 1) Manual (direct then proxy) in requested base order
        for base in base_langs:
            for code, kind in tracks:
                if kind == 'manual' and code.startswith(base):
                    out = _timedtext_fetch_vtt(video_id, code, asr=False, use_proxy=False, request_id=request_id)
                    if out:
                        log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": code, "kind": "manual", "proxy": False, "request_id": request_id})
                        return make_transcript_payload(out, code, code, False, track_manifest)
        if allow_proxy and get_proxy_dict():
            for base in base_langs:
                for code, kind in tracks:
                    if kind == 'manual' and code.startswith(base):
                        out = _timedtext_fetch_vtt(video_id, code, asr=False, use_proxy=True, request_id=request_id)
                        if out:
                            log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": code, "kind": "manual", "proxy": True, "request_id": request_id})
                            return make_transcript_payload(out, code, code, False, track_manifest)
        # 2) ASR (direct then proxy) in requested base order
        for base in base_langs:
            for code, kind in tracks:
                if kind == 'asr' and code.startswith(base):
                    out = _timedtext_fetch_vtt(video_id, code, asr=True, use_proxy=False, request_id=request_id)
                    if out:
                        log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": code, "kind": "asr", "proxy": False, "request_id": request_id})
                        return make_transcript_payload(out, code, code, True, track_manifest)
        if allow_proxy and get_proxy_dict():
            for base in base_langs:
                for code, kind in tracks:
                    if kind == 'asr' and code.startswith(base):
                        out = _timedtext_fetch_vtt(video_id, code, asr=True, use_proxy=True, request_id=request_id)
                        if out:
                            log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": code, "kind": "asr", "proxy": True, "request_id": request_id})
                            return make_transcript_payload(out, code, code, True, track_manifest)
        # 3) Translation as last resort
        if allow_translate and base_langs:
            target_base = base_langs[0]
            for code, kind in tracks:
                if kind == 'manual' and not code.startswith(target_base):
                    out = _timedtext_fetch_vtt(video_id, code, asr=False, use_proxy=False, request_id=request_id, tlang=target_base)
                    if out:
                        log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": code, "kind": f"manual_translate_{target_base}", "proxy": False, "request_id": request_id})
                        return make_transcript_payload(out, target_base, target_base, False, track_manifest)
            if allow_proxy and get_proxy_dict():
                for code, kind in tracks:
                    if kind == 'manual' and not code.startswith(target_base):
                        out = _timedtext_fetch_vtt(video_id, code, asr=False, use_proxy=True, request_id=request_id, tlang=target_base)
                        if out:
                            log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": code, "kind": f"manual_translate_{target_base}", "proxy": True, "request_id": request_id})
                            return make_transcript_payload(out, target_base, target_base, False, track_manifest)

    # If list failed or nothing matched, do a simple brute force (manual then ASR; direct then proxy)
    for base in base_langs:
        if out := _timedtext_fetch_vtt(video_id, base, asr=False, use_proxy=False, request_id=request_id):
            log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": base, "kind": "manual", "proxy": False, "request_id": request_id})
            return make_transcript_payload(out, base, base, False, track_manifest)
    if allow_proxy and get_proxy_dict():
        for base in base_langs:
            if out := _timedtext_fetch_vtt(video_id, base, asr=False, use_proxy=True, request_id=request_id):
                log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": base, "kind": "manual", "proxy": True, "request_id": request_id})
                return make_transcript_payload(out, base, base, False, track_manifest)
    for base in base_langs:
        if out := _timedtext_fetch_vtt(video_id, base, asr=True, use_proxy=False, request_id=request_id):
            log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": base, "kind": "asr", "proxy": False, "request_id": request_id})
            return make_transcript_payload(out, base, base, True, track_manifest)
    if allow_proxy and get_proxy_dict():
        for base in base_langs:
            if out := _timedtext_fetch_vtt(video_id, base, asr=True, use_proxy=True, request_id=request_id):
                log_event('info', 'timedtext_success', extra={"video_id": video_id, "lang": base, "kind": "asr", "proxy": True, "request_id": request_id})
                return make_transcript_payload(out, base, base, True, track_manifest)
    return None

# Requires youtube-transcript-api >= 1.0.4 for ProxyConfig support



# --- Flask App Initialization ---
app = Flask(__name__)
_cors_origins_env = os.getenv("WORTHIT_CORS_ORIGINS", "*")
_cors_origins = [o.strip() for o in _cors_origins_env.split(",")] if _cors_origins_env != "*" else "*"
CORS(app, resources={r"/*": {"origins": _cors_origins}})
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", str(2 * 1024 * 1024)))


# Rate Limiting
RATELIMIT_STORAGE_URI = os.getenv("RATELIMIT_STORAGE_URI") # e.g., redis://localhost:6379/0
limiter_kwargs = {
    "app": app,
    "key_func": get_remote_address,
    "default_limits": ["300 per hour", "60 per minute"], # Generous defaults
    "headers_enabled": True,
}
if RATELIMIT_STORAGE_URI:
    limiter_kwargs["storage_uri"] = RATELIMIT_STORAGE_URI
    logger.info("Rate limiting configured with Redis: %s", RATELIMIT_STORAGE_URI)
else:
    logger.warning("Rate limiting using in-memory storage (not recommended for production scale).")
limiter = Limiter(**limiter_kwargs)

# --------- Request context + structured logging helpers ---------
def _client_ip():
    try:
        ip = request.headers.get('CF-Connecting-IP') or request.headers.get('X-Forwarded-For', '').split(',')[0].strip()
        return ip or request.remote_addr or 'unknown'
    except Exception:
        return 'unknown'

def _client_country() -> str:
    """Best-effort country detection from common proxy/CDN headers."""
    try:
        # Cloudflare
        country = request.headers.get('CF-IPCountry')
        if country:
            return country.upper()
        # App Engine / some GCP setups
        country = request.headers.get('X-Appengine-Country')
        if country and country not in ('ZZ',):
            return country.upper()
        # Some proxies/CDNs may forward Geo headers
        country = request.headers.get('X-Geo-Country') or request.headers.get('X-Country-Code')
        if country:
            return country.upper()
    except Exception:
        pass
    return 'unknown'

def _client_agent() -> str:
    """Short User-Agent summary to avoid logging huge strings."""
    ua = request.headers.get('User-Agent', '') or ''
    return ua[:120]

def _full_url() -> str:
    try:
        qs = request.query_string.decode() if request and request.query_string else ""
        return (request.base_url + (f"?{qs}" if qs else ""))
    except Exception:
        return ""

def log_event(level: str, event: str, include_http: bool = False, **fields):
    structured = {"event": event, "request_id": getattr(g, 'request_id', None), **fields}
    # Attach httpRequest per Google format when requested
    if include_http and request is not None:
        http = {
            "requestMethod": request.method,
            "requestUrl": _full_url(),
            "remoteIp": getattr(g, 'ip', None) or request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr,
            "userAgent": getattr(g, 'client', None) or request.headers.get('User-Agent', ''),
        }
        # If a status or latency is present in fields, pass them through
        if 'status' in fields:
            http["status"] = fields['status']
        if 'duration_ms' in fields:
            try:
                http["latency"] = f"{float(fields['duration_ms'])/1000:.3f}s"
            except Exception:
                pass
        structured["httpRequest"] = http

    extra = {"structured": structured}
    if level == 'debug': logger.debug("", extra=extra)
    elif level == 'warning': logger.warning("", extra=extra)
    elif level == 'error': logger.error("", extra=extra)
    else: logger.info("", extra=extra)

@app.before_request  # type: ignore
def _before_request():
    g.request_id = uuid.uuid4().hex[:12]
    g.started_at = time.perf_counter()
    g.ip = _client_ip()
    # Be defensive: in case of mismatched deployments where `_client_country`
    # isn't available yet, avoid 500s and default gracefully.
    try:
        g.country = _client_country()
    except NameError:
        g.country = 'unknown'
    g.client = _client_agent()
    log_event('info', 'request_start', include_http=True, method=request.method, path=request.path, ip=g.ip, country=g.country, client=g.client)

@app.after_request  # type: ignore
def _after_request(response):
    try:
        dur_ms = int((time.perf_counter() - getattr(g, 'started_at', time.perf_counter())) * 1000)
        response.headers['X-Request-ID'] = getattr(g, 'request_id', '') or ''
        # Security headers applied to JSON responses only (avoid breaking static pages)
        if response.mimetype == 'application/json':
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['Referrer-Policy'] = 'no-referrer'
            response.headers['Permissions-Policy'] = 'interest-cohort=()'
            response.headers['Content-Security-Policy'] = "default-src 'none'"
        log_event('info', 'request_end', include_http=True, method=request.method, path=request.path, status=response.status_code, duration_ms=dur_ms, ip=getattr(g, 'ip', 'unknown'), country=getattr(g, 'country', 'unknown'))
    except Exception:
        pass
    return response

# --- Caches ---
transcript_cache = TTLCache(maxsize=TRANSCRIPT_CACHE_SIZE, ttl=TRANSCRIPT_CACHE_TTL)
comment_cache = TTLCache(maxsize=COMMENT_CACHE_SIZE, ttl=COMMENT_CACHE_TTL)

# In-flight deduplication for transcript fetches (keyed by cache_key)
transcript_inflight_lock = threading.Lock()
transcript_inflight_events: Dict[str, threading.Event] = {}

logger.info(f"App initialized. Max workers: {MAX_WORKERS}. Comment limit: {COMMENT_LIMIT}")
logger.info(f"Transcript Cache: size={TRANSCRIPT_CACHE_SIZE}, ttl={TRANSCRIPT_CACHE_TTL}s")
logger.info(f"Comment Cache: size={COMMENT_CACHE_SIZE}, ttl={COMMENT_CACHE_TTL}s")
logger.info("Global timeouts → external requests: 15 s, worker hard-timeout: 15 s; UX budget ≤ 25 s")

# --- Video ID Validation & Extraction ---
VIDEO_ID_REGEX = re.compile(r'^[\w-]{11}$')

def validate_video_id(video_id: str) -> bool:
    return bool(VIDEO_ID_REGEX.fullmatch(video_id))

@lru_cache(maxsize=512) # Cache extraction results
def extract_video_id(url_or_id: str) -> str | None:
    if validate_video_id(url_or_id):
        return url_or_id
    
    # Common patterns for extraction
    patterns = [
        r'(?:v=|/|embed/|shorts/|live/)([a-zA-Z0-9_-]{11})', # Standard, embed, shorts, live
        r'youtu\.be/([a-zA-Z0-9_-]{11})' # Short links
    ]
    for pattern in patterns:
        match = re.search(pattern, url_or_id)
        if match:
            vid = match.group(1)
            if validate_video_id(vid):
                return vid
    logger.warning("Failed to extract valid video ID from: %s", url_or_id)
    return None

# --- Piped/Invidious API Helpers ---
_PIPE_COOLDOWN: dict[str, float] = {}
MAX_ALT_CAPTION_HOSTS = 3

def _fetch_from_alternative_api(hosts,
                                path: str,
                                cooldown_map: dict[str, float],
                                timeout: float = 2.0,
                                request_id: str = "",
                                use_proxy: bool = True) -> dict | None:
    """Fetch JSON from a list/deque of alternative API hosts with simple round-robin + cooldown.
    Accepts a list of host URLs or a deque. Converts to deque internally as needed.
    """
    if not isinstance(hosts, deque):
        hosts = deque(hosts or [])
    deadline = time.time() + timeout
    t_flow_start = time.perf_counter()
    log_event('info', 'alternative_api_attempt_flow_start', path=path, host_count=len(hosts), request_id=request_id)
    # Simple round-robin with cooldown
    for _ in range(len(hosts)):
        host = hosts.popleft() # Get from left
        hosts.append(host)    # Add to right for next cycle

        if time.time() >= deadline:
            log_event('warning', 'alternative_api_flow_timeout', path=path, host=host, reason='Deadline reached', request_id=request_id)
            break
        if cooldown_map.get(host, 0) > time.time():
            log_event('info', 'alternative_api_host_cooldown', path=path, host=host, request_id=request_id)
            continue
        
        url = f"{host}{path}"
        current_proxy = get_proxy_dict() if use_proxy else {}
        
        t_host_attempt = time.perf_counter()
        log_event('debug', 'alternative_api_host_attempt', url=url, host=host, proxy_used=bool(current_proxy), request_id=request_id)
        try:
            r = session.get(url, proxies=current_proxy, timeout=3) # Shorter timeout for alternatives
            r.raise_for_status()
            if "application/json" not in r.headers.get("Content-Type", ""):
                log_event('warning', 'alternative_api_non_json_response', url=url, host=host, duration_ms=int((time.perf_counter() - t_host_attempt) * 1000), request_id=request_id)
                cooldown_map[host] = time.time() + 600 # Longer cooldown for structural issues
                continue
            log_event('info', 'alternative_api_host_success', url=url, host=host, duration_ms=int((time.perf_counter() - t_host_attempt) * 1000), request_id=request_id)
            return r.json()
        except requests.exceptions.RequestException as e:
            log_event('warning', 'alternative_api_host_failure', url=url, host=host, error=str(e), duration_ms=int((time.perf_counter() - t_host_attempt) * 1000), request_id=request_id)
            cooldown_map[host] = time.time() + 300 # Cooldown on error
        except Exception as e:
            log_event('error', 'alternative_api_host_unexpected_error', url=url, host=host, error=str(e), duration_ms=int((time.perf_counter() - t_host_attempt) * 1000), request_id=request_id, exc_info=LOG_LEVEL=="DEBUG")
            cooldown_map[host] = time.time() + 300
    log_event('warning', 'alternative_api_all_hosts_failed', path=path, duration_ms=int((time.perf_counter() - t_flow_start) * 1000), request_id=request_id)
    return None

# --- Language preferences helper (shared) ---
def expand_preferred_langs(codes: Optional[List[str]], force_en_first: bool = False) -> List[str]:
    """Expand language preferences into variants while preserving caller intent.
    - When `codes` is None/empty: fall back to env default and optionally force English first.
    - When caller provides explicit `codes`: DO NOT force English unless requested.
    """
    if not codes:
        codes = TRANSCRIPT_LANGS
    mapping = {
        'es': ['es', 'es-419', 'es-ES', 'es-MX', 'es-AR', 'es-CL', 'es-CO', 'es-PE', 'es-VE'],
        'pt': ['pt', 'pt-BR', 'pt-PT'],
        'en': ['en', 'en-US', 'en-GB', 'en-IN'],
        'hi': ['hi', 'hi-IN'],
        'ar': ['ar', 'ar-SA', 'ar-EG', 'ar-AE'],
        'fr': ['fr', 'fr-FR', 'fr-CA'],
        'de': ['de', 'de-DE'],
        'it': ['it', 'it-IT'],
        'ru': ['ru', 'ru-RU'],
        'tr': ['tr', 'tr-TR'],
        'id': ['id', 'id-ID'],
        'ja': ['ja', 'ja-JP'],
        'ko': ['ko', 'ko-KR'],
        'zh': ['zh', 'zh-Hans', 'zh-Hant', 'zh-CN', 'zh-TW'],
        'vi': ['vi', 'vi-VN'],
        'pl': ['pl', 'pl-PL'],
        'nl': ['nl', 'nl-NL'],
        'fa': ['fa', 'fa-IR'],
        'ur': ['ur', 'ur-PK', 'ur-IN'],
        'bn': ['bn', 'bn-BD', 'bn-IN'],
        'ta': ['ta', 'ta-IN'],
        'te': ['te', 'te-IN'],
        'th': ['th', 'th-TH'],
    }
    # 1) De-dup while preserving order
    seen = set()
    ordered = []
    for c in codes:
        c = (c or '').strip()
        if not c or c in seen:
            continue
        ordered.append(c)
        seen.add(c)
    # 2) Optionally ensure English first (only when explicitly requested or using defaults)
   if force_en_first and 'en' in ordered:
       ordered.remove('en')
       ordered.insert(0, 'en')
   # 3) Expand variants
    expanded: list[str] = []
    seen_variants: set[str] = set()
    for c in ordered:
        variants = mapping.get(c, [c])
        for v in variants:
            if v not in seen_variants:
                expanded.append(v)
                seen_variants.add(v)
        if len(expanded) >= TRANSCRIPT_LANG_EXPANSION_LIMIT:
            break
    if not expanded:
        expanded = ordered[:TRANSCRIPT_LANG_EXPANSION_LIMIT]
    return expanded

# --- Accept-Language parsing helper ---
def _parse_accept_language_header(header_val: str | None) -> list[str]:
    """Parse an HTTP Accept-Language header into a list of base language codes in priority order.
    Examples:
      "es-419,es;q=0.9,en-US;q=0.8,en;q=0.7" -> ["es", "en"]
      "pt-BR,pt;q=0.9" -> ["pt"]
    Returns an empty list when header is missing or unusable.
    """
    if not header_val:
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        parts = [p.strip() for p in header_val.split(',') if p.strip()]
        for p in parts:
            # Token up to ';' is the language tag
            tag = p.split(';', 1)[0].strip().lower()
            if not tag or tag == '*':
                continue
            base = tag.split('-', 1)[0]
            if base and base not in seen:
                out.append(base)
                seen.add(base)
    except Exception:
        return []
    return out


def limit_languages(langs: Optional[List[str]]) -> List[str]:
    """Deduplicate and clamp the language list to the configured expansion limit."""
    if not langs:
        return []
    limited: List[str] = []
    seen: set[str] = set()
    for code in langs:
        code = (code or "").strip()
        if not code or code in seen:
            continue
        limited.append(code)
        seen.add(code)
        if len(limited) >= TRANSCRIPT_LANG_EXPANSION_LIMIT:
            break
    return limited

# --- yt-dlp Helper ---
_YDL_OPTS_BASE = {
    "quiet": True, "skip_download": True, "extract_flat": "discard_in_playlist",
    "no_warnings": True, "restrict_filenames": True, "nocheckcertificate": True,
    "ignoreerrors": True, "no_playlist": True, "writeinfojson": False,
    "writesubtitles": True, "writeautomaticsub": True,
    "extractor_args": {"youtube": ["player_client=ios"]},
    "subtitlesformat": "best[ext=srv3]/best[ext=vtt]/best[ext=srt]",
    **({"cookiefile": YTDL_COOKIE_FILE} if YTDL_COOKIE_FILE else {}),
}

WS_USER = os.getenv("WEBSHARE_USER")
WS_PASS = os.getenv("WEBSHARE_PASS")

# Generic/DecoDo/Smartproxy style URLs
GEN_HTTP = os.getenv("PROXY_HTTP_URL") or os.getenv("HTTP_PROXY")
GEN_HTTPS = os.getenv("PROXY_HTTPS_URL") or os.getenv("HTTPS_PROXY")

PROXY_CFG = None
if GEN_HTTP or GEN_HTTPS:
    try:
        from youtube_transcript_api.proxies import GenericProxyConfig
        PROXY_CFG = GenericProxyConfig(
            http_url=GEN_HTTP or GEN_HTTPS,
            https_url=GEN_HTTPS or GEN_HTTP,
        )
        logger.info("Using GenericProxyConfig (http=%s, https=%s)", bool(GEN_HTTP), bool(GEN_HTTPS))
    except Exception as e:
        logger.warning("Failed to create GenericProxyConfig: %s", e)
elif WS_USER and WS_PASS:
    if not WS_USER.endswith("-rotate"):
        WS_USER = f"{WS_USER}-rotate"
    from youtube_transcript_api.proxies import WebshareProxyConfig
    PROXY_CFG = WebshareProxyConfig(
        proxy_username=WS_USER,
        proxy_password=WS_PASS
    )
    logger.info("Using Webshare rotating residential proxies (username=%s)", WS_USER)
else:
    logger.info("No proxy credentials – transcript requests will go direct")

# ---------------------------------------------------------------------------
# Helper – build a single rotating Webshare gateway URL
def _gateway_url() -> str | None:
    # Prefer generic proxy URL if provided
    if GEN_HTTPS:
        return GEN_HTTPS
    if GEN_HTTP:
        return GEN_HTTP
    if WS_USER and WS_PASS:
        # Webshare rotating residential gateway host
        return f"http://{WS_USER}:{WS_PASS}@p.webshare.io:80"
    return None

def get_proxy_dict() -> dict:
    """Return requests proxy dict if a proxy is configured, else {}."""
    url = _gateway_url()
    return {"http": url, "https": url} if url else {}

# (Back-compat helpers previously lived here; removed as unused.)




# ---------------------------------------------------------------------------
# Primary fetch via youtube-transcript-api (v1.x compliant)
def fetch_api_once(video_id: str,
                   proxy_cfg,
                   timeout: int = 10,
                   languages: Optional[List[str]] = None,
                   request_id: str = "",
                   prefer_original: bool = True,
                   strict_languages: bool = False,
                   allow_translate: bool = True) -> Optional[Dict[str, Any]]:
    """Single attempt using youtube-transcript-api. Returns plain text or None."""
    t0 = time.perf_counter()
    # Respect caller languages; if none, expand defaults with English-first
    languages_final = languages if languages else expand_preferred_langs(TRANSCRIPT_LANGS, force_en_first=True)
    languages_final = [lang for lang in languages_final if lang]
    primary_languages = languages_final[:TRANSCRIPT_PRIMARY_LANG_LIMIT] if languages_final else []
    log_event('info', 'transcript_method_attempt', extra={
        "method": "youtube-transcript-api",
        "video_id": video_id,
        "languages": languages_final,
        "proxy_config": {
            "is_configured": proxy_cfg is not None,
            "type": type(proxy_cfg).__name__ if proxy_cfg else None
        },
        "timeout": timeout,
        "request_id": request_id
    })
    
    # Build Accept-Language header with q-values
    accept_lang = ", ".join(f"{code};q={1.0 - (idx*0.1):.1f}" for idx, code in enumerate(languages_final[:5]))
    user_agent = random.choice(USER_AGENTS)
    http_client = get_thread_local_ytt_session(user_agent=user_agent, accept_language=accept_lang)
    ytt_api = YouTubeTranscriptApi(http_client=http_client, proxy_config=proxy_cfg)
    if primary_languages:
        t_quick = time.perf_counter()
        log_event('info', 'transcript_method_attempt', extra={
            "method": "youtube-transcript-api_quick",
            "video_id": video_id,
            "languages": primary_languages,
            "proxy_config": {
                "is_configured": proxy_cfg is not None,
                "type": type(proxy_cfg).__name__ if proxy_cfg else None
            },
            "request_id": request_id
        })
        try:
            quick_segments = ytt_api.get_transcript(video_id, languages=primary_languages)
            quick_text = " ".join(
                (seg.get("text", "").strip() if isinstance(seg, dict) else getattr(seg, "text", "").strip())
                for seg in quick_segments
            ).strip()
            if quick_text:
                payload = make_transcript_payload(
                    quick_text,
                    primary_languages[0],
                    primary_languages[0],
                    is_generated=False,
                    tracks=[]
                )
                log_event('info', 'transcript_method_success', extra={
                    "method": "youtube-transcript-api_quick",
                    "video_id": video_id,
                    "text_len": len(quick_text),
                    "language_detected": payload["language"]["code"],
                    "duration_ms": int((time.perf_counter() - t_quick) * 1000),
                    "request_id": request_id
                })
                return payload
        except NoTranscriptFound:
            pass
        except (RequestBlocked, CouldNotRetrieveTranscript, VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
            log_event('warning', 'transcript_method_failure', extra={
                "method": "youtube-transcript-api_quick",
                "video_id": video_id,
                "reason": str(e),
                "error_type": type(e).__name__,
                "languages_attempted": primary_languages,
                "proxy_used": proxy_cfg is not None,
                "duration_ms": int((time.perf_counter() - t_quick) * 1000),
                "request_id": request_id
            })
        except Exception as e:
            log_event('error', 'transcript_method_failure', extra={
                "method": "youtube-transcript-api_quick",
                "video_id": video_id,
                "reason": str(e),
                "languages_attempted": primary_languages,
                "proxy_used": proxy_cfg is not None,
                "duration_ms": int((time.perf_counter() - t_quick) * 1000),
                "request_id": request_id
            }, exc_info=LOG_LEVEL == "DEBUG")

    selected_transcript = None
    try:
        # Prefer list-based selection for finer control. Fallback to simple fetch if list is unavailable.
        if hasattr(ytt_api, 'list'):
            tl = ytt_api.list(video_id)
            ft = None

            def fetch_from_transcript(transcript_obj) -> Optional[Any]:
                nonlocal selected_transcript
                try:
                    fetched = transcript_obj.fetch()
                    selected_transcript = transcript_obj
                    return fetched
                except Exception:
                    return None
            # Gather manual vs generated
            try:
                manual_list = [tr for tr in tl if not getattr(tr, 'is_generated', False)]
                generated_list = [tr for tr in tl if getattr(tr, 'is_generated', False)]
            except Exception:
                manual_list, generated_list = [], []

            # 1) Prefer original (unique manual or unique generated) when enabled
            if prefer_original and not strict_languages and ft is None:
                if manual_list:
                    ft = fetch_from_transcript(manual_list[0])
                elif generated_list:
                    ft = fetch_from_transcript(generated_list[0])

            # 2) Try manual in the exact user-provided order
            if ft is None and languages_final:
                try:
                    t = tl.find_manually_created_transcript(languages_final)
                    candidate = fetch_from_transcript(t)
                    if candidate is not None:
                        ft = candidate
                except Exception:
                    ft = None

            # 3) Try generated in user-provided order
            if ft is None and languages_final:
                try:
                    t = tl.find_generated_transcript(languages_final)
                    candidate = fetch_from_transcript(t)
                    if candidate is not None:
                        ft = candidate
                except Exception:
                    ft = None

            # 4) If strict_languages is false, allow original (first manual else generated) even if not requested
            if ft is None and not strict_languages:
                t_any = (manual_list[0] if manual_list else (generated_list[0] if generated_list else None))
                if t_any is not None:
                    ft = fetch_from_transcript(t_any)

            # 5) Last resort: translate to the first requested language, when allowed
            if ft is None and allow_translate and languages_final:
                t_any = next(iter(tl), None)
                if t_any is None:
                    raise NoTranscriptFound
                if getattr(t_any, 'is_translatable', False):
                    translated = None
                    for lang in languages_final:
                        try:
                            translated = t_any.translate(lang)
                            break
                        except Exception:
                            continue
                    if translated is not None:
                        candidate = fetch_from_transcript(translated)
                        if candidate is not None:
                            ft = candidate
                    else:
                        ft = fetch_from_transcript(t_any)
                else:
                    ft = fetch_from_transcript(t_any)
        else:
            fetched = ytt_api.fetch(video_id, languages=languages_final)
            ft = fetched
            selected_transcript = None
    except Exception:
        log_event('warning', 'transcript_method_failure', extra={
            "method": "youtube-transcript-api",
            "video_id": video_id,
            "reason": "NoTranscriptFound",
            "languages_attempted": languages_final,
            "duration_ms": int((time.perf_counter() - t0) * 1000),
            "request_id": request_id
        })
        return None
    except (RequestBlocked, CouldNotRetrieveTranscript, VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
        log_event('error', 'transcript_method_failure', extra={
            "method": "youtube-transcript-api",
            "video_id": video_id,
            "reason": str(e),
            "error_type": type(e).__name__,
            "languages_attempted": languages_final,
            "proxy_used": proxy_cfg is not None,
            "duration_ms": int((time.perf_counter() - t0) * 1000),
            "request_id": request_id
        })
        return None

    segments = ft.to_raw_data() if hasattr(ft, "to_raw_data") else ft
    text_content = " ".join(
        seg["text"] if isinstance(seg, dict) else getattr(seg, "text", "")
        for seg in segments
    ).strip() or None

    if not text_content:
        return None

    language_code = getattr(ft, "language_code", None)
    language_label = getattr(ft, "language", language_code)
    is_generated = getattr(ft, "is_generated", getattr(selected_transcript, "is_generated", False))
    if selected_transcript is not None:
        language_code = getattr(selected_transcript, "language_code", language_code)
        language_label = getattr(selected_transcript, "language", language_label)

    payload = make_transcript_payload(
        text_content,
        language_code or "unknown",
        language_label or language_code or "unknown",
        bool(is_generated),
    )

    log_event('info', 'transcript_method_success', extra={
        "method": "youtube-transcript-api",
        "video_id": video_id,
        "text_len": len(text_content),
        "language_detected": payload["language"]["code"],
        "duration_ms": int((time.perf_counter() - t0) * 1000),
        "request_id": request_id
    })
    return payload

# ---------------------------------------------------------------------------
# yt‑dlp subtitle fallback ---------------------------------------------------
# Simplified wrapper for yt-dlp to fetch subtitles
# NOTE: yt-dlp has its own internal retries; this is a single call.
def fetch_ytdlp(video_id: str,
                proxy_url: Optional[str],
                request_id: str = "",
                languages: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """Fetch subtitles via yt-dlp, falling back to auto-generated if manual fails."""
    t0 = time.perf_counter()
    logger.info("Attempting transcript fetch via yt-dlp", extra={
        "event": "transcript_method_attempt",
        "method": "yt-dlp",
        "video_id": video_id,
        "proxy_used": proxy_url is not None,
        "request_id": request_id
    })
    # Map desired language codes into yt-dlp subtitle patterns (include auto and manual variants)
    lang_list = limit_languages(languages) or limit_languages(TRANSCRIPT_LANGS)
    sub_langs: List[str] = []
    for code in lang_list:
        c = (code or "").strip()
        if not c:
            continue
        sub_langs.extend([f"{c}.*", c])

    # Slightly more robust defaults to reduce bot checks on fallback path only.
    opts = {
        "quiet": True,
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": sub_langs or ["en.*", "en"],
        "subtitlesformat": "best[ext=srv3]/best[ext=vtt]/best[ext=srt]",
        "proxy": proxy_url or None,
        "nocheckcertificate": True,
        # Use iOS client to avoid some web challenges; send realistic headers
        "extractor_args": {"youtube": ["player_client=ios"]},
        "http_headers": {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.8",
            "Cookie": CONSENT_COOKIE_HEADER,
        },
    }
    # If a cookie file is available (either user-provided via env or the
    # generated consent cookie), pass it to yt-dlp to reduce bot challenges.
    if YTDL_COOKIE_FILE:
        opts["cookiefile"] = YTDL_COOKIE_FILE
    # Ensure English is always a last‑resort subtitle option
    base_langs = limit_languages(languages) or []
    if "en" not in base_langs:
        langs = base_langs + ["en"]
    else:
        langs = base_langs
    # Expand subtitle patterns accordingly
    sub_langs = []
    for code in langs:
        c = (code or "").strip()
        if not c:
            continue
        if c not in opts.get("subtitleslangs", []):
            sub_langs.extend([f"{c}.*", c])
    if sub_langs:
        opts["subtitleslangs"] = sub_langs
    with tempfile.TemporaryDirectory() as td:
        opts["outtmpl"] = f"{td}/%(id)s.%(ext)s"
        try:
            with YoutubeDL(opts) as ydl:
                ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
            # Pick best available subtitle file, preferring srv3, then vtt, then srt
            fpath = None
            for ext in ("srv3", "vtt", "srt"):
                fpath = next(pathlib.Path(td).glob(f"{video_id}*.{ext}"), None)
                if fpath:
                    break
            if not fpath:
                return None
            raw = fpath.read_text(encoding="utf-8", errors="ignore")
            if fpath.suffix == ".srv3" or "<text" in raw:
                text = " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", raw))
                return make_fallback_payload(text, base_langs, is_generated=True)
            if fpath.suffix == ".vtt":
                lines = [l.strip() for l in raw.splitlines() if l and not l.startswith("WEBVTT") and "-->" not in l]
                text = " ".join(lines)
                return make_fallback_payload(text, base_langs, is_generated=True)
            if fpath.suffix == ".srt":
                lines = [l.strip() for l in raw.splitlines() if l and "-->" not in l and not re.match(r"^\d+$", l)]
                text = " ".join(lines)
                return make_fallback_payload(text, base_langs, is_generated=True)
            return None
        except Exception as e:
            logger.warning("yt-dlp failed for %s: %s", video_id, e)
            return None

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Parallel alternative API fallback: fetch from multiple APIs in parallel
from cachetools import cached

# Cache for instance lists (1 hour TTL, up to 10 different endpoints)
instance_cache = TTLCache(maxsize=10, ttl=3600)

@cached(instance_cache)
def fetch_live_instances(api_url):
    try:
        # Use proxy if configured to improve reachability under egress limits
        response = session.get(api_url, timeout=5, proxies=get_proxy_dict())
        response.raise_for_status()
        # For piped, the API returns a list of dicts with "api_url" and "active"
        # For invidious, the API returns a list of [url, info] where info["health"] and info["monitor"]["status"] may exist
        if "piped" in api_url:
            # Piped API
            lst = [instance["api_url"].rstrip("/") for instance in response.json() if instance.get("api_url") and instance.get("active")]
            if not lst:
                lst = [
                    "https://pipedapi.kavin.rocks",
                    "https://pipedapi.adminforge.de",
                    "https://pipedapi.tokhmi.xyz",
                    "https://piped-api.privacy.com.de",
                    "https://api-piped.mha.fi",
                ]
            return lst
        elif "invidious" in api_url:
            # Invidious API (https://api.invidious.io/instances.json?sort_by=health)
            lst = [
                url.rstrip("/")
                for url, info in response.json()
                if info.get("type") == "https" and info.get("health", 0) > 0 and info.get("monitor", {}).get("status") == "online"
            ]
            if not lst:
                lst = [
                    "https://yewtu.be",
                    "https://vid.puffyan.us",
                    "https://inv.nadeko.net",
                ]
            return lst
        else:
            return []
    except Exception as e:
        logger.warning(f"Failed to fetch instances from {api_url}: {e}")
        # Fallback to a small static list of known instances
        if "piped" in api_url:
            return [
                "https://pipedapi.kavin.rocks",
                "https://pipedapi.adminforge.de",
                "https://pipedapi.tokhmi.xyz",
                "https://piped-api.privacy.com.de",
                "https://api-piped.mha.fi",
            ]
        if "invidious" in api_url:
            return [
                "https://yewtu.be",
                "https://vid.puffyan.us",
                "https://inv.nadeko.net",
            ]
        return []

def _fetch_transcript_alternatives(video_id: str,
                                   request_id: str = "",
                                   languages: Optional[List[str]] = None,
                                   allow_proxy: bool = True) -> Optional[Dict[str, Any]]:
    """Try alternative APIs in parallel for transcript fetching."""
    t0 = time.perf_counter()
    logger.info("Attempting transcript fetch via alternative APIs", extra={
        "event": "transcript_method_attempt",
        "method": "alternative_apis",
        "video_id": video_id,
        "request_id": request_id
    })

    static_piped = [
        "https://pipedapi.kavin.rocks",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.tokhmi.xyz",
        "https://piped-api.privacy.com.de",
        "https://api-piped.mha.fi",
    ]
    static_invidious = [
        "https://yewtu.be",
        "https://vid.puffyan.us",
        "https://inv.nadeko.net",
    ]

    MAX_HOSTS = 3

    def _attempt_with_hosts(p_hosts: list[str], i_hosts: list[str], proxy_allowed: bool) -> Optional[Dict[str, Any]]:
        piped_subset = list((p_hosts or [])[:MAX_HOSTS])
        invidious_subset = list((i_hosts or [])[:MAX_HOSTS])

        fetchers: list[tuple[str, callable]] = []
        if piped_subset:
            fetchers.extend([
                (
                    "piped_direct",
                    lambda vid=video_id, hosts=piped_subset: _piped_captions_direct(
                        vid,
                        hosts,
                        languages=languages,
                        use_proxy=proxy_allowed
                    )
                ),
                (
                    "piped_api",
                    lambda vid=video_id, hosts=piped_subset: _piped_captions(
                        vid,
                        hosts,
                        languages=languages,
                        use_proxy=proxy_allowed
                    )
                ),
            ])
        if invidious_subset:
            fetchers.append(
                (
                    "invidious",
                    lambda vid=video_id, hosts=invidious_subset: _invidious_captions(
                        vid,
                        hosts,
                        languages=languages,
                        use_proxy=proxy_allowed
                    )
                )
            )

        if not fetchers:
            return None

        with ThreadPoolExecutor(max_workers=len(fetchers)) as executor:
            future_map = {executor.submit(fn): name for name, fn in fetchers}
            for future in as_completed(future_map, timeout=6):
                try:
                    result = future.result()
                    if result:
                        logger.info("✅ Alternative API transcript succeeded", extra={
                            "event": "transcript_step_success",
                            "step": 2,
                            "method": "alternative_apis",
                            "source": future_map[future],
                            "video_id": video_id,
                            "text_len": len(result),
                            "duration_ms": int((time.perf_counter() - t0) * 1000),
                            "request_id": request_id
                        })
                        return make_fallback_payload(result, languages, is_generated=True)
                except Exception as e:
                    logger.warning("Transcript fetch via %s failed", future_map[future], extra={
                        "event": "transcript_step_failure",
                        "step": 2,
                        "method": "alternative_apis",
                        "video_id": video_id,
                        "reason": str(e),
                        "request_id": request_id
                    })
        return None

    discovery_executor = ThreadPoolExecutor(max_workers=2)
    piped_future = discovery_executor.submit(fetch_live_instances, "https://piped-instances.kavin.rocks/")
    invidious_future = discovery_executor.submit(fetch_live_instances, "https://api.invidious.io/instances.json?sort_by=health")
    try:
        result_payload = _attempt_with_hosts(static_piped, static_invidious, proxy_allowed=False)
        if result_payload:
            return result_payload

        use_piped: list[str] = []
        use_invidious: list[str] = []

        try:
            use_piped = piped_future.result(timeout=2) or []
        except TimeoutError:
            log_event('warning', 'transcript_instance_discovery_timeout', provider='piped', video_id=video_id, request_id=request_id)
        except Exception as exc:
            log_event('warning', 'transcript_instance_discovery_failed', provider='piped', video_id=video_id, error=str(exc), request_id=request_id)

        try:
            use_invidious = invidious_future.result(timeout=2) or []
        except TimeoutError:
            log_event('warning', 'transcript_instance_discovery_timeout', provider='invidious', video_id=video_id, request_id=request_id)
        except Exception as exc:
            log_event('warning', 'transcript_instance_discovery_failed', provider='invidious', video_id=video_id, error=str(exc), request_id=request_id)

        if (use_piped and use_piped != static_piped) or (use_invidious and use_invidious != static_invidious):
            result_payload = _attempt_with_hosts(use_piped or static_piped, use_invidious or static_invidious, proxy_allowed=allow_proxy)
            if result_payload:
                return result_payload
    finally:
        discovery_executor.shutdown(wait=False)

    logger.warning("All alternative API transcript fetchers failed for %s", video_id, extra={
        "event": "transcript_step_failure",
        "step": 2,
        "method": "alternative_apis",
        "video_id": video_id,
        "reason": "All alternative API transcript fetchers failed",
        "request_id": request_id
    })
    return None

# ---------------------------------------------------------------------------
# Unified transcript fetching with clear fallback steps and logging
def get_transcript(video_id: str,
                   request_id: str = "",
                   languages: Optional[List[str]] = None,
                   prefer_original: bool = True,
                   strict_languages: bool = False,
                   allow_translate: bool = True) -> Dict[str, Any]:
    """
    Transcript fetch logic:
    1. Try youtube-transcript-api directly (and via proxy when configured).
    2. If that fails, kick off timedtext, alternative mirror APIs, and yt-dlp (no proxy) in parallel.
    Raise NoTranscriptFound if every option fails.
    """
    t0_workflow = time.perf_counter()
    logger.info("💡 Initiating unified transcript fetch workflow", extra={
        "event": "transcript_workflow_start",
        "video_id": video_id,
        "request_id": request_id
    })

    proxy_url = _gateway_url() if PROXY_CFG is not None else None
    proxy_attempt_details: dict[str, Any] = {
        "proxy_configured": PROXY_CFG is not None,
        "proxy_url": proxy_url,
        "attempts": []
    }
    if PROXY_CFG is not None and proxy_url is None:
        log_event('warning', 'proxy_url_missing', video_id=video_id, request_id=request_id)

    def run_primary_attempt() -> Optional[Dict[str, Any]]:
        if PROXY_CFG is not None:
            max_proxy_attempts = 2
            for attempt in range(1, max_proxy_attempts + 1):
                attempt_meta: dict[str, Any] = {"attempt": attempt}
                try:
                    txt = fetch_api_once(
                        video_id,
                        PROXY_CFG,
                        languages=languages,
                        request_id=request_id,
                        prefer_original=prefer_original,
                        strict_languages=strict_languages,
                        allow_translate=allow_translate
                    )
                    if txt:
                        attempt_meta.update({"status": "success", "text_len": len(txt["text"])})
                        proxy_attempt_details["attempts"].append(attempt_meta)
                        logger.info("Primary transcript fetch succeeded via proxy", extra={
                            "event": "transcript_step_success",
                            "step": 1,
                            "attempt": attempt,
                            "method": "youtube-transcript-api_proxy",
                            "video_id": video_id,
                            "text_len": len(txt["text"]),
                            "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
                            "request_id": request_id
                        })
                        return txt
                    attempt_meta.update({"status": "empty"})
                    proxy_attempt_details["attempts"].append(attempt_meta)
                    logger.info("Proxy attempt %d returned no transcript", attempt, extra={
                        "event": "transcript_step_failure",
                        "step": 1,
                        "attempt": attempt,
                        "method": "youtube-transcript-api_proxy",
                        "video_id": video_id,
                        "reason": "No transcript found",
                        "request_id": request_id
                    })
                except (RequestBlocked, CouldNotRetrieveTranscript, VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
                    attempt_meta.update({"status": "error", "error": str(e)})
                    proxy_attempt_details["attempts"].append(attempt_meta)
                    logger.warning("Proxy attempt %d blocked or failed", attempt, extra={
                        "event": "transcript_step_failure",
                        "step": 1,
                        "attempt": attempt,
                        "method": "youtube-transcript-api_proxy",
                        "video_id": video_id,
                        "reason": str(e),
                        "request_id": request_id
                    })
                except Exception as exc:
                    attempt_meta.update({"status": "error", "error": str(exc)})
                    proxy_attempt_details["attempts"].append(attempt_meta)
                    logger.warning("Proxy attempt %d raised unexpected exception", attempt, extra={
                        "event": "transcript_step_failure",
                        "step": 1,
                        "attempt": attempt,
                        "method": "youtube-transcript-api_proxy",
                        "video_id": video_id,
                        "reason": str(exc),
                        "request_id": request_id
                    })
            return None
        # Direct (no proxy) path
        try:
            txt = fetch_api_once(
                video_id,
                None,
                request_id=request_id,
                languages=languages,
                prefer_original=prefer_original,
                strict_languages=strict_languages,
                allow_translate=allow_translate
            )
            if txt:
                logger.info("Primary transcript fetch succeeded", extra={
                    "event": "transcript_step_success",
                    "step": 1,
                    "method": "youtube-transcript-api_direct",
                    "video_id": video_id,
                    "text_len": len(txt["text"]),
                    "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
                    "request_id": request_id
                })
                return txt
            logger.info("Primary transcript fetch failed (no transcript found)", extra={
                "event": "transcript_step_failure",
                "step": 1,
                "method": "youtube-transcript-api_direct",
                "video_id": video_id,
                "reason": "No transcript found",
                "request_id": request_id
            })
        except (RequestBlocked, CouldNotRetrieveTranscript, VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
            logger.warning("Primary transcript fetch blocked or failed (direct)", extra={
                "event": "transcript_step_failure",
                "step": 1,
                "method": "youtube-transcript-api_direct",
                "video_id": video_id,
                "reason": str(e),
                "request_id": request_id
            })
        except Exception as exc:
            logger.warning("Primary transcript fetch raised unexpected exception (direct)", extra={
                "event": "transcript_step_failure",
                "step": 1,
                "method": "youtube-transcript-api_direct",
                "video_id": video_id,
                "reason": str(exc),
                "request_id": request_id
            })
        return None

    # Step 2+: Run remaining fallbacks in parallel (timedtext, alt APIs, yt-dlp no-proxy)
    fallback_attempts = [
        (
            "timedtext",
            lambda: timedtext_try_languages(
                video_id,
                languages,
                request_id=request_id,
                allow_translate=allow_translate,
                allow_proxy=False
            )
        ),
        (
            "alternative_apis",
            lambda: _fetch_transcript_alternatives(
                video_id,
                request_id=request_id,
                languages=languages,
                allow_proxy=False
            )
        ),
        (
            "yt-dlp_no_proxy",
            lambda: fetch_ytdlp(video_id, None, request_id=request_id, languages=languages)
        ),
    ]

    with ThreadPoolExecutor(max_workers=1 + len(fallback_attempts)) as executor:
        future_map: dict[Future, tuple[str, str]] = {}
        pending: set[Future] = set()

        proxy_future = executor.submit(run_primary_attempt)
        future_map[proxy_future] = ("primary", "proxy" if PROXY_CFG is not None else "direct")
        pending.add(proxy_future)
        fallback_started = False

        def launch_fallbacks():
            nonlocal fallback_started
            if fallback_started:
                return
            fallback_started = True
            log_event('info', 'transcript_fallbacks_triggered', video_id=video_id, request_id=request_id)
            for name, fn in fallback_attempts:
                future = executor.submit(fn)
                future_map[future] = ("fallback", name)
                pending.add(future)

        try:
            primary_result = proxy_future.result(timeout=TRANSCRIPT_FALLBACK_DELAY_SEC)
            pending.discard(proxy_future)
            if primary_result:
                log_event(
                    'info',
                    'transcript_result',
                    strategy='youtube-transcript-api_proxy' if PROXY_CFG is not None else 'youtube-transcript-api_direct',
                    video_id=video_id,
                    text_len=len(primary_result["text"]),
                    duration_ms=int((time.perf_counter() - t0_workflow) * 1000),
                    proxy_health=proxy_attempt_details,
                    request_id=request_id
                )
                return primary_result
            launch_fallbacks()
        except TimeoutError:
            log_event(
                'warning',
                'transcript_primary_slow',
                video_id=video_id,
                request_id=request_id,
                wait_ms=int(TRANSCRIPT_FALLBACK_DELAY_SEC * 1000)
            )
            launch_fallbacks()
        except Exception as exc:
            pending.discard(proxy_future)
            log_event(
                'warning',
                'transcript_primary_exception',
                video_id=video_id,
                request_id=request_id,
                error=str(exc)
            )
            launch_fallbacks()

        while pending:
            future = next(as_completed(pending))
            pending.discard(future)
            mode, name = future_map.get(future, ("fallback", "unknown"))
            try:
                txt = future.result()
            except Exception as exc:
                if mode == "primary":
                    log_event('warning', 'transcript_primary_exception', video_id=video_id, request_id=request_id, error=str(exc))
                else:
                    log_event('warning', 'transcript_strategy_exception', strategy=name, video_id=video_id, error=str(exc), request_id=request_id, proxy_health=proxy_attempt_details)
                if not fallback_started:
                    launch_fallbacks()
                continue

            if not txt:
                if not fallback_started and mode == "primary":
                    launch_fallbacks()
                continue

            strategy_label = name if mode == "fallback" else ('youtube-transcript-api_proxy' if PROXY_CFG is not None else 'youtube-transcript-api_direct')
            log_event(
                'info',
                'transcript_result',
                strategy=strategy_label,
                video_id=video_id,
                text_len=len(txt["text"]),
                duration_ms=int((time.perf_counter() - t0_workflow) * 1000),
                proxy_health=proxy_attempt_details,
                request_id=request_id
            )
            return txt

    # If all fail
    logger.warning("❌ All transcript fetch methods FAILED.", extra={
        "event": "all_transcript_methods_failed",
        "video_id": video_id,
        "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
        "request_id": request_id
    })
    raise NoTranscriptFound

# --- Transcript Fallback Helpers ---
def _strip_tags(text: str) -> str:
    """Remove HTML/XML tags from a string."""
    return re.sub(r"<[^>]+>", "", text)

def _piped_captions_direct(video_id: str,
                           hosts: list[str],
                           languages: Optional[List[str]] = None,
                           use_proxy: bool = True) -> str | None:
    """Fetch captions directly from several Piped instances in parallel."""
    if not hosts:
        logger.warning("No Piped hosts available for direct captions for %s", video_id)
        return None

    subset = list((hosts or [])[:MAX_ALT_CAPTION_HOSTS])
    if not subset:
        logger.warning("No Piped hosts available for direct captions for %s", video_id)
        return None

    prefs = [c.strip().lower() for c in (limit_languages(languages) or limit_languages(TRANSCRIPT_LANGS))]

    def _fetch_from_host(host: str) -> str | None:
        try:
            meta = session.get(
                f"{host}/api/v1/captions/{video_id}",
                timeout=3,
                proxies=get_proxy_dict() if use_proxy else {}
            )
            if meta.status_code == 404:
                return None
            meta.raise_for_status()
            data = meta.json()
            subs = data.get("captions") or []
            if not subs:
                return None
            chosen = None
            for code in prefs:
                chosen = next((s for s in subs if s.get("language", "").lower().startswith(code)), None)
                if chosen:
                    break
            if not chosen:
                chosen = subs[0]
            url = chosen.get("url")
            if not url:
                return None
            if url.startswith("//"):
                url = "https:" + url
            raw = session.get(url, timeout=3, proxies=get_proxy_dict() if use_proxy else {}).text
            text = " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", raw)).strip()
            return text or None
        except Exception as exc:
            logger.debug("Piped captions host %s failed: %s", host, exc)
            return None

    with ThreadPoolExecutor(max_workers=len(subset)) as executor:
        futures = {executor.submit(_fetch_from_host, host): host for host in subset}
        try:
            for future in as_completed(futures, timeout=5):
                host = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover – defensive
                    logger.debug("Piped captions host %s raised: %s", host, exc)
                    continue
                if result:
                    return result
        except TimeoutError:
            logger.warning("Timed out waiting for direct Piped captions for %s", video_id)

    logger.warning("All direct Piped caption mirrors failed for %s", video_id)
    return None

def _piped_captions(video_id: str,
                    hosts: list[str],
                    languages: Optional[List[str]] = None,
                    use_proxy: bool = True) -> str | None:
    """Fetch captions via the Piped API using several mirrors concurrently."""
    if not hosts:
        logger.warning("No Piped hosts available for fallback captions for %s", video_id)
        return None

    subset = list((hosts or [])[:MAX_ALT_CAPTION_HOSTS])
    if not subset:
        logger.warning("No Piped hosts available for fallback captions for %s", video_id)
        return None

    prefs = [c.strip().lower() for c in (limit_languages(languages) or limit_languages(TRANSCRIPT_LANGS))]

    def _fetch_from_host(host: str) -> str | None:
        try:
            resp = session.get(
                f"{host}/streams/{video_id}",
                timeout=3,
                proxies=get_proxy_dict() if use_proxy else {}
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            subs = data.get("subtitles") or []
            if not subs:
                return None
            chosen = None
            for code in prefs:
                chosen = next((s for s in subs if s.get("language", "").lower().startswith(code)), None)
                if chosen:
                    break
            if not chosen:
                chosen = subs[0]
            url = chosen.get("url")
            if not url:
                return None
            r = session.get(url, timeout=3, proxies=get_proxy_dict() if use_proxy else {})
            r.raise_for_status()
            data_text = r.text
            if url.endswith(".vtt"):
                lines = [l.strip() for l in data_text.splitlines() if l and not l.startswith("WEBVTT") and not re.match(r"^\d\d:\d\d", l)]
                text = " ".join(_strip_tags(l) for l in lines if not re.match(r"^\d+$", l)).strip()
                return text or None
            if url.endswith(".srv3") or "<text" in data_text:
                return " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", data_text)).strip() or None
            if url.endswith(".srt"):
                lines = [l.strip() for l in data_text.splitlines() if l and "-->" not in l and not re.match(r"^\d+$", l)]
                return " ".join(lines).strip() or None
            return None
        except Exception as exc:
            logger.debug("Piped fallback captions host %s failed: %s", host, exc)
            return None

    with ThreadPoolExecutor(max_workers=len(subset)) as executor:
        futures = {executor.submit(_fetch_from_host, host): host for host in subset}
        try:
            for future in as_completed(futures, timeout=6):
                host = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover – defensive
                    logger.debug("Piped fallback captions host %s raised: %s", host, exc)
                    continue
                if result:
                    return result
        except TimeoutError:
            logger.warning("Timed out waiting for Piped API captions for %s", video_id)

    logger.warning("All fallback Piped API hosts failed for %s", video_id)
    return None

def _invidious_captions(video_id: str,
                        hosts: list[str],
                        languages: Optional[List[str]] = None,
                        use_proxy: bool = True) -> str | None:
    """Fetch captions from Invidious mirrors using limited parallelism."""
    if not hosts:
        logger.warning("No Invidious hosts available for captions for %s", video_id)
        return None

    subset = list((hosts or [])[:MAX_ALT_CAPTION_HOSTS])
    if not subset:
        logger.warning("No Invidious hosts available for captions for %s", video_id)
        return None

    prefs = [c.strip().lower() for c in (limit_languages(languages) or limit_languages(TRANSCRIPT_LANGS))]

    def _fetch_from_host(host: str) -> str | None:
        try:
            meta_url = f"{host}/api/v1/captions/{video_id}"
            meta_resp = session.get(
                meta_url,
                timeout=3,
                proxies=get_proxy_dict() if use_proxy else {}
            )
            if meta_resp.status_code == 404:
                return None
            meta_resp.raise_for_status()
            meta_json = meta_resp.json()
            subs = meta_json.get("captions") or []
            if not subs:
                return None
            chosen = None
            for code in prefs:
                chosen = next((s for s in subs if s.get("languageCode", "").lower().startswith(code)), None)
                if chosen:
                    break
            if not chosen:
                chosen = subs[0]
            rel_url = chosen.get("url")
            if not rel_url:
                return None
            caption_url = rel_url if rel_url.startswith("http") else urljoin(host, rel_url)
            cap_resp = session.get(
                caption_url,
                timeout=3,
                proxies=get_proxy_dict() if use_proxy else {}
            )
            cap_resp.raise_for_status()
            raw = cap_resp.text
            if caption_url.endswith(".vtt"):
                lines = [
                    l.strip()
                    for l in raw.splitlines()
                    if l and not l.startswith("WEBVTT") and not re.match(r"^\d\d:\d\d", l)
                ]
                return " ".join(_strip_tags(l) for l in lines if not re.match(r"^\d+$", l)).strip() or None
            if caption_url.endswith(".srv3") or "<text" in raw:
                return " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", raw)).strip() or None
            if caption_url.endswith(".srt"):
                lines = [
                    l.strip()
                    for l in raw.splitlines()
                    if l and "-->" not in l and not re.match(r"^\d+$", l)
                ]
                return " ".join(lines).strip() or None
            return None
        except Exception as exc:
            logger.debug("Invidious host %s failed: %s", host, exc)
            return None

    with ThreadPoolExecutor(max_workers=len(subset)) as executor:
        futures = {executor.submit(_fetch_from_host, host): host for host in subset}
        try:
            for future in as_completed(futures, timeout=6):
                host = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover – defensive
                    logger.debug("Invidious host %s raised: %s", host, exc)
                    continue
                if result:
                    return result
        except TimeoutError:
            logger.warning("Timed out waiting for Invidious captions for %s", video_id)

    logger.warning("All Invidious caption mirrors failed for %s", video_id)
    return None

def _get_or_spawn_transcript(video_id: str) -> str:
    """
    Return transcript from cache (RAM or persistent). Never spawns fetch jobs.
    Raises NoTranscriptFound if not cached.
    """
    # In‑memory first
    val = transcript_cache.get(video_id)
    if val is not None:
        if val == "__NOT_AVAILABLE__":
            raise NoTranscriptFound(video_id, [], None)
        logger.debug("Transcript cache HIT (in-memory) for %s", video_id)
        return val

    # Persistent shelf
    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
        val = db.get(video_id)
        if val is not None:
            if val == "__NOT_AVAILABLE__":
                raise NoTranscriptFound(video_id, [], None)
            logger.debug("Transcript cache HIT (persistent) for %s", video_id)
            transcript_cache[video_id] = val
            return val

    # Not cached
    raise NoTranscriptFound(video_id, [], None)

# --- Comment Fetching Logic ---
def _fetch_comments_downloader(video_id: str, use_proxy: bool = False, request_id: str = "") -> list[str]:
    """Fetches comments using youtube-comment-downloader."""
    t0 = time.perf_counter()
    log_event('info', 'comment_method_attempt', extra={
        "method": "youtube-comment-downloader",
        "video_id": video_id,
        "proxy_config": {
            "is_configured": use_proxy,
            "proxy_url": _gateway_url() if use_proxy else None
        },
        "language": 'en',  # Hardcoded in current implementation
        "request_id": request_id
    })
    try:
        proxy_url = _gateway_url() if use_proxy else None
        downloader_kwargs = {}
        log_event('debug', 'comment_downloader_proxy_config', extra={
            "video_id": video_id, 
            "proxy_url": proxy_url, 
            "request_id": request_id
        })
        downloader = YoutubeCommentDownloader(**downloader_kwargs)
        comments_generator = downloader.get_comments_from_url(
            f"https://www.youtube.com/watch?v={video_id}",
            sort_by=0,    # 0 for top (popular) comments
            language='en' # English comments
        )
        comments_text: list[str] = []
        for item in itertools.islice(comments_generator, MAX_COMMENTS_FETCH):
            text = item.get("text")
            if text:
                comments_text.append(text)
            if len(comments_text) >= COMMENT_LIMIT:
                break

        if comments_text:
            log_event('info', 'comment_method_success', extra={
                "method": "youtube-comment-downloader",
                "video_id": video_id,
                "count": len(comments_text),
                "proxy_used": use_proxy,
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "request_id": request_id
            })
            return comments_text
        
        log_event('warning', 'comment_method_failure', extra={
            "method": "youtube-comment-downloader",
            "video_id": video_id,
            "reason": "No comments returned",
            "proxy_used": use_proxy,
            "duration_ms": int((time.perf_counter() - t0) * 1000),
            "request_id": request_id
        })
        return []
    except Exception as e:
        log_event('error', 'comment_method_failure', extra={
            "method": "youtube-comment-downloader",
            "video_id": video_id,
            "error": str(e),
            "error_type": type(e).__name__,
            "proxy_used": use_proxy,
            "duration_ms": int((time.perf_counter() - t0) * 1000),
            "request_id": request_id,
            "exc_info": LOG_LEVEL == "DEBUG"
        })
        return []

def _fetch_comments_yt_dlp(video_id: str, use_proxy: bool = False, request_id: str = "") -> list[str] | None:
    """
    Fetch comments via yt-dlp's `getcomments` mechanism.
    Returns a list of comment texts or None/empty list.
    """
    t0 = time.perf_counter()
    log_event('info', 'comment_method_attempt', method='yt-dlp_comments', video_id=video_id, proxy_used=use_proxy, request_id=request_id)
    info = yt_dlp_extract_info(video_id, extract_comments=True, use_proxy=use_proxy, request_id=request_id)
    if not info:
        log_event('warning', 'comment_method_failure', method='yt-dlp_comments', video_id=video_id, reason='No info from yt-dlp', duration_ms=int((time.perf_counter() - t0) * 1000), request_id=request_id)
        return []
    comments_raw = info.get("comments") or []
    comments = [c.get("text") or c.get("comment") or "" for c in comments_raw]
    comments = [c for c in comments if c]
    if comments:
        log_event('info', 'comment_method_success', method='yt-dlp_comments', video_id=video_id, count=len(comments), proxy_used=use_proxy, duration_ms=int((time.perf_counter() - t0) * 1000), request_id=request_id)
        return comments
    log_event('warning', 'comment_method_failure', method='yt-dlp_comments', video_id=video_id, reason='No comments returned', proxy_used=use_proxy, duration_ms=int((time.perf_counter() - t0) * 1000), request_id=request_id)
    return []

def _fetch_comments_resilient(video_id: str, request_id: str = "") -> list[str]:
    """Fetch comments in a primary→fallback→proxy→Piped sequence."""
    t0_workflow = time.perf_counter()
    log_event('info', 'comments_workflow_start', extra={
        "video_id": video_id,
        "strategies": [
            "youtube-comment-downloader (no proxy)",
            "youtube-comment-downloader (with proxy)",
            "yt-dlp (no proxy)",
            "yt-dlp (with proxy)",
            "alternative APIs"
        ],
        "request_id": request_id
    })

    comment_retrieval_strategies = [
        ("youtube-comment-downloader (no proxy)", lambda: _fetch_comments_downloader(video_id, False, request_id)),
        ("youtube-comment-downloader (with proxy)", lambda: _fetch_comments_downloader(video_id, True, request_id)),
        ("yt-dlp (no proxy)", lambda: _fetch_comments_from_ytdlp(video_id, False, request_id)),
        ("yt-dlp (with proxy)", lambda: _fetch_comments_from_ytdlp(video_id, True, request_id))
    ]


    # First, try primary strategies sequentially
    for strategy_name, fetch_func in comment_retrieval_strategies:
        comments = fetch_func()
        if comments:
            log_event('info', 'comment_step_success', extra={
                "step": strategy_name,
                "video_id": video_id,
                "count": len(comments),
                "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
                "request_id": request_id
            })
            log_event('info', 'comments_result', strategy=strategy_name, video_id=video_id,
                      count=len(comments), duration_ms=int((time.perf_counter() - t0_workflow) * 1000),
                      cache='miss', request_id=request_id)
            return comments
        
        log_event('warning', 'comment_step_failure', extra={
            "step": strategy_name,
            "video_id": video_id,
            "reason": "No comments returned",
            "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
            "request_id": request_id
        })

    static_piped = [
        "https://pipedapi.kavin.rocks",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.tokhmi.xyz",
        "https://piped-api.privacy.com.de",
        "https://api-piped.mha.fi",
    ]
    static_invidious = [
        "https://yewtu.be",
        "https://vid.puffyan.us",
        "https://inv.nadeko.net",
    ]
    MAX_ALT_HOSTS = 3

    def _try_alternative_hosts(p_hosts: list[str], i_hosts: list[str]) -> list[str] | None:
        piped_subset = list((p_hosts or [])[:MAX_ALT_HOSTS])
        invidious_subset = list((i_hosts or [])[:MAX_ALT_HOSTS])

        tasks: list = []
        future_meta: dict = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            for host in piped_subset:
                future_direct = executor.submit(
                    _fetch_from_alternative_api,
                    [f"{host}/api"],
                    f"/comments/{video_id}",
                    _PIPE_COOLDOWN,
                    request_id=request_id,
                    use_proxy=False
                )
                future_meta[future_direct] = ("Alternative API (Piped direct)", host)
                tasks.append(future_direct)

                future_api = executor.submit(
                    _fetch_from_alternative_api,
                    [host],
                    f"/comments/{video_id}",
                    _PIPE_COOLDOWN,
                    request_id=request_id,
                    use_proxy=False
                )
                future_meta[future_api] = ("Alternative API (Piped)", host)
                tasks.append(future_api)

            for host in invidious_subset:
                future_inv = executor.submit(
                    _fetch_from_alternative_api,
                    [host],
                    f"/api/v1/comments/{video_id}",
                    _PIPE_COOLDOWN,
                    request_id=request_id,
                    use_proxy=False
                )
                future_meta[future_inv] = ("Alternative API (Invidious)", host)
                tasks.append(future_inv)

            if not tasks:
                return None

            try:
                for future in as_completed(tasks, timeout=10):
                    try:
                        result_json = future.result()
                    except Exception as exc:
                        source_name, host = future_meta.get(future, (None, None))
                        log_event('warning', 'comment_step_failure', extra={
                            "step": "alternative_apis_concurrent",
                            "video_id": video_id,
                            "source": source_name,
                            "host": host,
                            "reason": str(exc),
                            "request_id": request_id
                        })
                        continue

                    if result_json and "comments" in result_json:
                        comments = [c.get("text") for c in result_json["comments"] if c.get("text")]
                    if comments:
                        source_name, host = future_meta.get(future, (None, None))
                        log_event('info', 'comment_step_success', extra={
                            "step": "alternative_apis_concurrent",
                            "video_id": video_id,
                            "count": len(comments),
                            "source": source_name,
                            "host": host,
                            "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
                            "request_id": request_id
                        })
                        log_event('info', 'comments_result', strategy=source_name or "alternative_apis",
                                  video_id=video_id, count=len(comments), host=host,
                                  duration_ms=int((time.perf_counter() - t0_workflow) * 1000),
                                  cache='miss', request_id=request_id)
                        return comments
            except TimeoutError:
                log_event('warning', 'comment_alternative_timeout', extra={
                    "video_id": video_id,
                    "hosts_tried": piped_subset + invidious_subset,
                    "request_id": request_id
                })

        return None

    # First attempt with static hosts (no discovery penalty)
    discovery_executor = ThreadPoolExecutor(max_workers=2)
    piped_future = discovery_executor.submit(fetch_live_instances, "https://piped-instances.kavin.rocks/")
    invidious_future = discovery_executor.submit(fetch_live_instances, "https://api.invidious.io/instances.json?sort_by=health")
    try:
        comments = _try_alternative_hosts(static_piped, static_invidious)
        if comments:
            return comments

        use_piped: list[str] = []
        use_invidious: list[str] = []

        try:
            use_piped = piped_future.result(timeout=2) or []
        except TimeoutError:
            log_event('warning', 'comment_instance_discovery_timeout', extra={
                "video_id": video_id,
                "provider": "piped",
                "request_id": request_id
            })
        except Exception as exc:
            log_event('warning', 'comment_instance_discovery_failed', extra={
                "video_id": video_id,
                "provider": "piped",
                "error": str(exc),
                "request_id": request_id
            })

        try:
            use_invidious = invidious_future.result(timeout=2) or []
        except TimeoutError:
            log_event('warning', 'comment_instance_discovery_timeout', extra={
                "video_id": video_id,
                "provider": "invidious",
                "request_id": request_id
            })
        except Exception as exc:
            log_event('warning', 'comment_instance_discovery_failed', extra={
                "video_id": video_id,
                "provider": "invidious",
                "error": str(exc),
                "request_id": request_id
            })

        if not use_piped and not use_invidious:
            log_event('warning', 'comment_alternative_sources_unavailable', extra={
                "video_id": video_id,
                "reason": "No alternative hosts discovered",
                "request_id": request_id
            })
            log_event('error', 'all_comment_methods_failed', extra={
                "video_id": video_id,
                "strategies_attempted": [strategy[0] for strategy in comment_retrieval_strategies],
                "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
                "request_id": request_id
            })
            return []

        comments = _try_alternative_hosts(use_piped or static_piped, use_invidious or static_invidious)
        if comments:
            return comments
    finally:
        discovery_executor.shutdown(wait=False)

    # If all methods fail
    log_event('error', 'all_comment_methods_failed', extra={
        "video_id": video_id,
        "strategies_attempted": [
            strategy[0] for strategy in comment_retrieval_strategies
        ] + ['alternative_apis'],
        "duration_ms": int((time.perf_counter() - t0_workflow) * 1000),
        "request_id": request_id
    })
    return []

def yt_dlp_extract_info(video_id: str, extract_comments: bool = False, use_proxy: bool = False, request_id: str = "") -> dict | None:
    t0 = time.perf_counter()
    log_event('info', 'yt_dlp_info_extract_attempt', video_id=video_id, extract_comments=extract_comments, proxy_used=use_proxy, request_id=request_id)
    opts = _YDL_OPTS_BASE.copy()
    # --- CORRECTED LOGIC ---
    # Always add a random User-Agent to every yt-dlp request
    opts['http_headers'] = get_random_user_agent_header()
    # Apply proxy only if requested and configured
    if use_proxy and get_proxy_dict():
        proxy_url = _gateway_url()
        if proxy_url:
            opts["proxy"] = proxy_url
            log_event('debug', 'yt_dlp_proxy_config', video_id=video_id, proxy_url=proxy_url, request_id=request_id)
    # --- END OF CORRECTION ---

    if extract_comments:
        opts["getcomments"] = True
        opts["max_comments"] = COMMENT_LIMIT

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    log_event('debug', 'yt_dlp_extract_info_details', video_id=video_id, comments_requested=extract_comments, request_id=request_id)
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if info:
                log_event('info', 'yt_dlp_info_extract_success', video_id=video_id, duration_ms=int((time.perf_counter() - t0) * 1000), request_id=request_id)
                return info
            log_event('warning', 'yt_dlp_info_extract_failure', video_id=video_id, reason='No info returned', duration_ms=int((time.perf_counter() - t0) * 1000), request_id=request_id)
            return None
    except Exception as e:
        log_event('error', 'yt_dlp_info_extract_error', video_id=video_id, error=str(e), duration_ms=int((time.perf_counter() - t0) * 1000), request_id=request_id, exc_info=LOG_LEVEL == "DEBUG")
        return None

def _fetch_comments_from_ytdlp(video_id: str, use_proxy: bool = False, request_id: str = "") -> list[str] | None:
    """Fetches comments using yt-dlp. Supports auto and user comments."""
    t0 = time.perf_counter()
    log_event('info', 'comment_method_attempt', extra={
        "method": "yt-dlp_comments",
        "video_id": video_id,
        "proxy_config": {
            "is_configured": use_proxy,
            "proxy_url": _gateway_url() if use_proxy else None
        },
        "comment_types": ["user", "auto"],
        "request_id": request_id
    })

    info = yt_dlp_extract_info(video_id, extract_comments=True, use_proxy=use_proxy, request_id=request_id)
    if not info:
        log_event('warning', 'comment_method_failure', extra={
            "method": "yt-dlp_comments",
            "video_id": video_id,
            "reason": "No info from yt-dlp",
            "proxy_used": use_proxy,
            "duration_ms": int((time.perf_counter() - t0) * 1000),
            "request_id": request_id
        })
        return []

    comments_raw = info.get("comments") or []
    comments = [c.get("text") or c.get("comment") or "" for c in comments_raw]
    comments = [c for c in comments if c]

    if comments:
        log_event('info', 'comment_method_success', extra={
            "method": "yt-dlp_comments",
            "video_id": video_id,
            "count": len(comments),
            "comment_sources": list(set(
                c.get("source", "unknown") 
                for c in info.get("comments", []) 
                if c.get("text") or c.get("comment")
            )),
            "proxy_used": use_proxy,
            "duration_ms": int((time.perf_counter() - t0) * 1000),
            "request_id": request_id
        })
        return comments

    log_event('warning', 'comment_method_failure', extra={
        "method": "yt-dlp_comments",
        "video_id": video_id,
        "reason": "No comments returned",
        "raw_comments_count": len(comments_raw),
        "proxy_used": use_proxy,
        "duration_ms": int((time.perf_counter() - t0) * 1000),
        "request_id": request_id
    })
    return []

# --- Endpoints ---
@app.route("/transcript", methods=["GET"])
@limiter.limit("1000/hour;200/minute")  # Increased limits for transcript endpoint
def get_transcript_endpoint():
    g.request_start_time = time.perf_counter() # Mark request start time
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        log_event('warning', 'transcript_missing_video_id', video_id=video_url_or_id, ip=get_remote_address(), request_id=g.request_id)
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        log_event('warning', 'transcript_invalid_video_id', raw=video_url_or_id, ip=get_remote_address(), request_id=g.request_id)
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    # Parse preferred languages from query (CSV) → list of codes
    raw_langs = request.args.get("languages")
    languages: Optional[List[str]] = None
    # Behavior flags
    prefer_original = (request.args.get("preferOriginal", "true").lower() == "true")
    strict_languages = (request.args.get("strictLanguages", "false").lower() == "true")
    allow_translate = (request.args.get("allowTranslate", "false").lower() == "true")
    # Build a language-aware cache key that respects caller intent
    cache_key = video_id
    legacy_fallback_allowed = True  # Use legacy cache key only for default English-first path

    if raw_langs:
        # Normalize and de-dup caller-provided list, preserving order
        base_list = []
        seen = set()
        for c in str(raw_langs).split(","):
            cc = c.strip().lower()
            if not cc or cc in seen:
                continue
            base_list.append(cc)
            seen.add(cc)
        languages = expand_preferred_langs(base_list, force_en_first=False)
        cache_key = f"{video_id}::langs={','.join(base_list)}"
        legacy_fallback_allowed = False
    else:
        # No explicit languages provided by client → infer from Accept-Language header.
        # Keep English-first legacy behavior if the client prefers English, to avoid changing the current flow.
        accept_lang_hdr = request.headers.get("Accept-Language", "")
        inferred_bases = _parse_accept_language_header(accept_lang_hdr)
        if inferred_bases and inferred_bases[0] != 'en':
            # Non-English preference → expand accordingly and append English as a safety fallback
            if 'en' not in inferred_bases:
                inferred_bases.append('en')
            languages = expand_preferred_langs(inferred_bases, force_en_first=False)
            cache_key = f"{video_id}::langs={','.join(inferred_bases)}"
            legacy_fallback_allowed = False
            log_event('info', 'languages_inferred_from_accept_language', video_id=video_id, accept_language=accept_lang_hdr, inferred=inferred_bases, request_id=g.request_id)
        else:
            # Default behavior: English-first expansion, keep legacy cache key for compatibility
            languages = expand_preferred_langs(TRANSCRIPT_LANGS, force_en_first=True)

    languages = limit_languages(languages)

    # Log the start of transcript fetching
    log_event('info', 'transcript_fetch_workflow_start', video_id=video_id, languages=languages or TRANSCRIPT_LANGS,
              prefer_original=prefer_original, strict_languages=strict_languages, allow_translate=allow_translate,
              ip=get_remote_address(), request_id=g.request_id)

    # Check RAM cache
    cached = transcript_cache.get(cache_key)
    if cached is not None:
        if cached == "__NOT_AVAILABLE__":
            log_event('info', 'transcript_cache_miss_marker', video_id=video_id, request_id=g.request_id)
            return jsonify({"error": "Transcript not available"}), 404
        payload = ensure_payload(cached, languages)
        if payload:
            transcript_cache[cache_key] = payload
            log_event('info', 'transcript_cache_hit', video_id=video_id, text_len=len(payload["text"]), request_id=g.request_id)
            log_event('info', 'transcript_response_summary', video_id=video_id, cache='memory', strategy='cache', text_len=len(payload["text"]), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
            return jsonify({"video_id": video_id, **payload}), 200

    # Try persistent cache
    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
        # Prefer new language-aware key; fall back to legacy key only when languages not provided
        cached = db.get(cache_key)
        if cached is not None:
            if cached == "__NOT_AVAILABLE__":
                log_event('info', 'transcript_persisted_not_available', video_id=video_id, request_id=g.request_id)
                return jsonify({"error": "Transcript not available"}), 404
            payload = ensure_payload(cached, languages)
            if payload:
                transcript_cache[cache_key] = payload
                log_event('info', 'transcript_persisted_hit', video_id=video_id, text_len=len(payload["text"]), request_id=g.request_id)
                log_event('info', 'transcript_response_summary', video_id=video_id, cache='disk', strategy='cache', text_len=len(payload["text"]), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
                return jsonify({"video_id": video_id, **payload}), 200
        if not raw_langs and legacy_fallback_allowed:
            # Legacy fallback: old key without language dimension
            legacy = db.get(video_id)
            if legacy is not None:
                if legacy == "__NOT_AVAILABLE__":
                    log_event('info', 'transcript_persisted_not_available_legacy', video_id=video_id, request_id=g.request_id)
                    return jsonify({"error": "Transcript not available"}), 404
                payload = ensure_payload(legacy, languages)
                if payload:
                    transcript_cache[cache_key] = payload
                    log_event('info', 'transcript_persisted_hit_legacy', video_id=video_id, text_len=len(payload["text"]), request_id=g.request_id)
                    return jsonify({"video_id": video_id, **payload}), 200

    # In-flight deduplication: elect a single leader to perform the fetch
    leader = False
    with transcript_inflight_lock:
        evt = transcript_inflight_events.get(cache_key)
        if evt is None:
            evt = threading.Event()
            transcript_inflight_events[cache_key] = evt
            leader = True

    if not leader:
        # Another request is already fetching this transcript. Wait briefly for it to complete, then reuse cache/persisted result.
        log_event('info', 'transcript_inflight_wait', video_id=video_id, request_id=g.request_id)
        evt.wait(timeout=30)
        # Re-check RAM cache
        cached = transcript_cache.get(cache_key)
        if cached is not None:
            if cached == "__NOT_AVAILABLE__":
                log_event('info', 'transcript_cache_miss_marker', video_id=video_id, request_id=g.request_id)
                return jsonify({"error": "Transcript not available"}), 404
            payload = ensure_payload(cached, languages)
            if payload:
                transcript_cache[cache_key] = payload
                log_event('info', 'transcript_cache_hit_after_wait', video_id=video_id, text_len=len(payload["text"]), request_id=g.request_id)
                return jsonify({"video_id": video_id, **payload}), 200
        # Re-check persistent cache
        with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
            cached = db.get(cache_key)
            if cached is None and (not raw_langs and legacy_fallback_allowed):
                cached = db.get(video_id)
            if cached is not None:
                if cached == "__NOT_AVAILABLE__":
                    log_event('info', 'transcript_persisted_not_available_after_wait', video_id=video_id, request_id=g.request_id)
                    return jsonify({"error": "Transcript not available"}), 404
                payload = ensure_payload(cached, languages)
                if payload:
                    transcript_cache[cache_key] = payload
                    log_event('info', 'transcript_persisted_hit_after_wait', video_id=video_id, text_len=len(payload["text"]), request_id=g.request_id)
                    log_event('info', 'transcript_response_summary', video_id=video_id, cache='disk', strategy='cache_after_wait', text_len=len(payload["text"]), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
                    return jsonify({"video_id": video_id, **payload}), 200
        # Fallback: proceed to fetch ourselves (rare edge when leader failed/timeout)
        log_event('warning', 'transcript_inflight_promote_to_fetch', video_id=video_id, request_id=g.request_id)

    # Try fetch with short retry/backoff on transient errors (leader or promoted follower)
    attempts = 0
    last_err = None
    try:
        if leader:
            log_event('info', 'transcript_inflight_leader', video_id=video_id, request_id=g.request_id)
        while attempts < 2:
            try:
                # Log attempt start
                log_event('info', 'transcript_method_attempt', method='unified_fetch', attempt=attempts + 1, video_id=video_id, request_id=g.request_id)
                payload = get_transcript(video_id, request_id=g.request_id, languages=languages,
                                         prefer_original=prefer_original,
                                         strict_languages=strict_languages,
                                         allow_translate=allow_translate)
                transcript_cache[cache_key] = payload
                with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
                    db[cache_key] = payload
                log_event('info', 'transcript_fetched', video_id=video_id, text_len=len(payload["text"]), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
                log_event('info', 'transcript_response_summary', video_id=video_id, cache='miss', strategy='fresh_fetch', text_len=len(payload["text"]), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
                resp = make_response(jsonify({"video_id": video_id, **payload}), 200)
                resp.headers['Cache-Control'] = 'public, max-age=3600'
                return resp
            except NoTranscriptFound:
                if cache_key not in transcript_cache:
                    transcript_cache[cache_key] = "__NOT_AVAILABLE__"
                    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
                        db[cache_key] = "__NOT_AVAILABLE__"
                log_event('warning', 'transcript_not_found', video_id=video_id, duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
                resp = make_response(jsonify({"error": "Transcript not available"}), 404)
                resp.headers['Cache-Control'] = 'public, max-age=600'
                return resp
            except requests.exceptions.RequestException as e:
                # Transient network/SSL errors – backoff and retry once
                attempts += 1
                last_err = e
                log_event('warning', 'transcript_fetch_network_error', video_id=video_id, attempt=attempts, error=str(e), request_id=g.request_id)
                time.sleep(0.5 * attempts)
                continue
            except Exception as e:
                # Non-retryable internal error
                log_event('error', 'transcript_fetch_failed', video_id=video_id, error=str(e), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
                return jsonify({"error": "Transcript fetch failed"}), 500
        # Retries exhausted → treat as not available
        log_event('warning', 'transcript_unavailable_after_retry', video_id=video_id, error=str(last_err) if last_err else None, duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
        resp = make_response(jsonify({"error": "Transcript not available"}), 404)
        resp.headers['Cache-Control'] = 'public, max-age=600'
        return resp
    finally:
        if leader:
            with transcript_inflight_lock:
                evt = transcript_inflight_events.pop(cache_key, None)
                if evt is not None:
                    evt.set()
            log_event('info', 'transcript_inflight_done', video_id=video_id, request_id=g.request_id)

@app.route("/comments", methods=["GET"])
@limiter.limit("120/hour;20/minute")  # Limits for comments endpoint
def get_comments_endpoint():
    g.request_start_time = time.perf_counter() # Mark request start time
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        log_event('warning', 'comments_missing_video_id', video_id=video_url_or_id, ip=get_remote_address(), request_id=g.request_id)
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        log_event('warning', 'comments_invalid_video_id', raw=video_url_or_id, ip=get_remote_address(), request_id=g.request_id)
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    # Log the start of comment fetching
    log_event('info', 'comments_fetch_workflow_start', video_id=video_id, ip=get_remote_address(), request_id=g.request_id)

    cached = comment_cache.get(video_id)
    if cached is not None:
        log_event('info', 'comments_cache_hit', video_id=video_id, count=len(cached), request_id=g.request_id)
        log_event('info', 'comments_result', strategy='cache_memory', video_id=video_id,
                  count=len(cached), duration_ms=int((time.perf_counter()-g.request_start_time)*1000),
                  cache='memory', request_id=g.request_id)
        return jsonify({"video_id": video_id, "comments": cached}), 200

    with shelve.open(PERSISTENT_COMMENT_DB) as db:
        cached = db.get(video_id)
        if cached is not None:
            comment_cache[video_id] = cached
            log_event('info', 'comments_persisted_hit', video_id=video_id, count=len(cached), request_id=g.request_id)
            log_event('info', 'comments_result', strategy='cache_disk', video_id=video_id,
                      count=len(cached), duration_ms=int((time.perf_counter()-g.request_start_time)*1000),
                      cache='disk', request_id=g.request_id)
            return jsonify({"video_id": video_id, "comments": cached}), 200

    try:
        comments = _fetch_comments_resilient(video_id) # This calls the internal _fetch_comments_resilient function
        comment_cache[video_id] = comments
        with shelve.open(PERSISTENT_COMMENT_DB) as db:
            db[video_id] = comments
        log_event('info', 'comments_fetched', video_id=video_id, count=len(comments), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
        log_event('info', 'comments_result', strategy='fresh_fetch', video_id=video_id,
                  count=len(comments), duration_ms=int((time.perf_counter()-g.request_start_time)*1000),
                  cache='miss', request_id=g.request_id)
        return jsonify({"video_id": video_id, "comments": comments}), 200
    except Exception as e:
        log_event('warning', 'comments_fetch_failed_with_exception', video_id=video_id, error=str(e), duration_ms=int((time.perf_counter()-g.request_start_time)*1000), request_id=g.request_id)
        # On technical failure, return 200 OK with empty comments and a warning message.
        return jsonify({"video_id": video_id, "comments": [], "warning": "Comments could not be fetched due to a technical issue."}), 200

@app.route("/openai/responses", methods=["POST"])
@limiter.limit("200/hour;50/minute") # Limits for OpenAI proxy
def openai_proxy():
    if not OPENAI_API_KEY:
        logger.error("OpenAI API key not configured. Cannot proxy request.")
        return jsonify({'error': 'OpenAI API key not configured on server'}), 500

    try:
        payload = request.get_json()
        if not payload:
            return jsonify({'error': 'Invalid JSON payload'}), 400
    except Exception as e:
        logger.error(f"Failed to parse request JSON for OpenAI proxy: {e}", exc_info=True)
        return jsonify({'error': 'Bad request JSON'}), 400

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Log scrubbed payload
    logged_payload = payload.copy()
    # if 'input' in payload: logged_payload['input_length'] = len(payload['input'])
    # if 'text' in payload and isinstance(payload['text'], dict) and 'input' in payload['text'] : # Handle new structure
    #     logged_payload['text_input_length'] = len(payload['text']['input'])
    #     payload_text_copy = payload['text'].copy()
    #     payload_text_copy.pop('input', None)
    #     logged_payload['text_other_fields'] = payload_text_copy

    # --- GPT-5 compatibility: strip unsupported params & adjust timeout ---
    model = str(payload.get("model", ""))
    if model.startswith("gpt-5-"):
        # Remove unsupported parameters for GPT-5 Responses API
        if "temperature" in payload:
            payload.pop("temperature", None)
        # Allow more time for nano/mini which can be slower to return
        proxy_timeout = 60
    else:
        proxy_timeout = 15

    t0 = time.perf_counter()
    model_for_log = payload.get("model", "N/A")
    log_event('info', 'openai_proxy_request', model=model_for_log, payload=logged_payload)

    openai_endpoint = "https://api.openai.com/v1/responses" # Keep as original

    # --- Streaming mode (Server-Sent Events pass-through) ---
    if bool(payload.get('stream')):
        from flask import Response, stream_with_context

        def _iter_sse():
            client_disconnected = False
            first_event_sent = False
            try:
                # Open upstream stream
                up_headers = dict(headers)
                up_headers["Accept"] = "text/event-stream"
                upstream = session.post(openai_endpoint, headers=up_headers, json=payload, timeout=proxy_timeout, stream=True)
                if upstream.status_code != 200:
                    # Emit a single SSE error event and return
                    txt = upstream.text[:1000]
                    log_event('error', 'openai_proxy_stream_error', model=model_for_log, status=upstream.status_code, body_snippet=txt)
                    yield f"event: error\n"
                    err_json = json.dumps({'error': 'OpenAI API error', 'status': upstream.status_code})
                    yield f"data: {err_json}\n\n"
                    return

                log_event('info', 'openai_proxy_stream_start', model=model_for_log)

                # Forward upstream SSE lines directly to client
                for raw in upstream.iter_lines(decode_unicode=True):
                    try:
                        if raw is None:
                            # heartbeat when None (requests may yield None on keep-alive)
                            continue
                        yield raw + "\n"
                        if not first_event_sent:
                            first_event_sent = True
                            t_first = int((time.perf_counter() - t0) * 1000)
                            log_event('info', 'openai_proxy_first_event', model=model_for_log, duration_ms=t_first)
                    except GeneratorExit:
                        client_disconnected = True
                        break
                    except Exception:
                        # Attempt to continue on transient write errors
                        continue

                # Ensure final blank line to delimit
                yield "\n"

            except requests.exceptions.Timeout:
                log_event('error', 'openai_proxy_stream_timeout', model=model_for_log)
            except Exception as e:
                log_event('error', 'openai_proxy_stream_exception', model=model_for_log, error=str(e))
            finally:
                dur_ms = int((time.perf_counter() - t0) * 1000)
                log_event('info', 'openai_proxy_stream_end', model=model_for_log, client_cancelled=client_disconnected, duration_ms=dur_ms)

        headers_resp = {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            # Disable proxy buffering where respected (e.g., Nginx)
            'X-Accel-Buffering': 'no'
        }
        return Response(stream_with_context(_iter_sse()), headers=headers_resp)

    # --- Non-streaming mode (JSON pass-through) ---
    try:
        resp = session.post(openai_endpoint, headers=headers, json=payload, timeout=proxy_timeout)
        dur_ms = int((time.perf_counter() - t0) * 1000)
        usage = None
        try:
            usage = resp.json().get('usage')
        except Exception:
            usage = None
        log_event('info', 'openai_proxy_response', model=model_for_log, status=resp.status_code, duration_ms=dur_ms, usage=usage)

        if resp.status_code != 200:
            error_content = resp.text[:1000] # Log part of the error
            log_event('error', 'openai_proxy_error', model=model_for_log, status=resp.status_code, body_snippet=error_content)
            # Try to parse standard OpenAI error for clearer message to client
            try:
                error_json = resp.json()
                detailed_error = error_json.get("error", {}).get("message", "OpenAI API error")
                return jsonify({'error': detailed_error, 'details': error_json}), resp.status_code
            except json.JSONDecodeError:
                return jsonify({'error': 'OpenAI API error', 'details': error_content}), resp.status_code

        response_data = resp.json()
        response_preview = {k: (str(v)[:100] + '...' if isinstance(v, (str, list, dict)) and len(str(v)) > 100 else v) for k,v in response_data.items()}
        log_event('debug', 'openai_proxy_preview', model=model_for_log, preview=response_preview)

        return jsonify(response_data), resp.status_code

    except requests.exceptions.Timeout:
        log_event('error', 'openai_proxy_timeout', model=model_for_log)
        return jsonify({'error': 'Request to OpenAI timed out'}), 504
    except requests.exceptions.RequestException as e:
        log_event('error', 'openai_proxy_network_error', model=model_for_log, error=str(e))
        return jsonify({'error': 'Network error with OpenAI service'}), 503
    except Exception as e:
        log_event('error', 'openai_proxy_unexpected', model=model_for_log, error=str(e))
        return jsonify({'error': 'Internal server error during OpenAI proxy'}), 500

# --- Retrieve a stored OpenAI response by ID ---------------------------------
@app.route('/openai/responses/<response_id>', methods=['GET'])
@limiter.limit("200 per hour;50 per minute")
def get_openai_response(response_id):
    """
    Pass-through helper to pull a stored Response object directly from OpenAI.
    """
    if not OPENAI_API_KEY:
        logger.error("OpenAI API key not configured – cannot fetch stored response.")
        return jsonify({'error': 'OpenAI API key not configured'}), 500

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    try:
        resp = session.get(
            f"https://api.openai.com/v1/responses/{response_id}",
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        return jsonify(resp.json()), resp.status_code
    except requests.HTTPError as http_err:
        logger.error("OpenAI GET /responses error: %s – %s", http_err, resp.text)
        return jsonify({'error': 'OpenAI API error',
                        'details': resp.text}), resp.status_code
    except Exception as e:
        logger.error("Error fetching OpenAI response: %s", str(e))
        return jsonify({'error': 'OpenAI service unavailable'}), 503


# --- Static and landing routes ----------------------------------------------
@app.route("/", methods=["GET"])
def root_ok():
    """Landing page.
    Attempts multiple static locations for index.html; if none, returns JSON uptime.
    """
    base = pathlib.Path(__file__).resolve().parent
    candidates = [
        base / "static" / "index.html",                    # youtube-transcript-service/static/index.html
        base.parent / "WorthIt" / "static" / "index.html", # ../WorthIt/static/index.html (case-sensitive envs)
        base.parent / "worthit" / "static" / "index.html", # ../worthit/static/index.html
        base.parent / "static" / "index.html"              # ../static/index.html
    ]
    for p in candidates:
        if p.is_file():
            return send_from_directory(str(p.parent), p.name)
    uptime = round(time.time() - app_start_time)
    return jsonify({"status": "ok", "uptime": uptime, "note": "index.html not found"}), 200


@app.route("/_health", methods=["GET"])
def health():
    """JSON uptime check"""
    uptime = round(time.time() - app_start_time)
    return jsonify({"status": "ok", "uptime": uptime}), 200

def _send_static_multi(filename: str):
    base = pathlib.Path(__file__).resolve().parent
    for d in [base / "static", base.parent / "WorthIt" / "static", base.parent / "worthit" / "static", base.parent / "static"]:
        f = d / filename
        if f.is_file():
            return send_from_directory(str(d), filename)
    return ("Not Found", 404)

@app.route("/privacy")
def privacy():
    return _send_static_multi("privacy.html")

@app.route("/terms")
def terms():
    return _send_static_multi("terms.html")

@app.route("/support")
def support():
    return _send_static_multi("support.html")

@app.route('/favicon.ico')
def favicon():
    # Avoid noisy 404/500s when browsers request favicon
    return ("", 204)


# --- Cleanup on Exit ---
def cleanup_on_exit():
    # Close shelve databases properly (best-effort; they are opened on demand)
    logger.info("Application shutting down.")

import atexit
atexit.register(cleanup_on_exit)

# --- Run App ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Cloud Run expects $PORT, default 8080
    logger.info(f"Starting {APP_NAME} on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)

# For Cloud Run / Gunicorn
if __name__ != "__main__":
    gunicorn_app = app
