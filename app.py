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
from typing import List, Optional
from functools import lru_cache
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future, as_completed


from flask import Flask, request, jsonify
from flask import make_response
from flask import make_response
from flask import g
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
from logging.handlers import RotatingFileHandler
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.errors import RateLimitExceeded
from flask_limiter.util import get_remote_address
import requests
import shutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

from yt_dlp import YoutubeDL

import functools

# --- Patch for youtube-transcript-api compatibility ---
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
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "50"))
TRANSCRIPT_CACHE_SIZE = int(os.getenv("TRANSCRIPT_CACHE_SIZE", "200"))
TRANSCRIPT_CACHE_TTL = int(os.getenv("TRANSCRIPT_CACHE_TTL", "7200")) # 2 hours
# Preferred languages for transcript API (comma‑separated env var)
TRANSCRIPT_LANGS = os.getenv("TRANSCRIPT_LANGS", "en").split(",")
#
# Transcript tuning
TRANSCRIPT_HTTP_TIMEOUT   = int(os.getenv("TRANSCRIPT_HTTP_TIMEOUT", "15"))
COMMENT_CACHE_SIZE = int(os.getenv("COMMENT_CACHE_SIZE", "150"))
COMMENT_CACHE_TTL = int(os.getenv("COMMENT_CACHE_TTL", "7200")) # 2 hours
# In Cloud Run, only /tmp is writable. Allow override via env.
PERSISTENT_CACHE_DIR = os.getenv("CACHE_DIR", "/tmp/persistent_cache")
PERSISTENT_TRANSCRIPT_DB = os.path.join(PERSISTENT_CACHE_DIR, "transcript_cache.db")
PERSISTENT_COMMENT_DB = os.path.join(PERSISTENT_CACHE_DIR, "comment_cache.db")
# YTDL Cookie file configuration
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE", "/etc/secrets/cookies_chrome2.txt")

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


# --- Logging Setup ---
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
    # isn’t available yet, avoid 500s and default gracefully.
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

# --- Caches & Worker Pool ---
transcript_cache = TTLCache(maxsize=TRANSCRIPT_CACHE_SIZE, ttl=TRANSCRIPT_CACHE_TTL)
comment_cache = TTLCache(maxsize=COMMENT_CACHE_SIZE, ttl=COMMENT_CACHE_TTL)
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_pending_futures: dict[str, Future] = {}

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

# --- Piped/Invidious API Helpers (for potential future use, currently not primary) ---
PIPED_HOSTS = deque([
    "https://pipedapi.kavin.rocks", "https://pipedapi.adminforge.de", "https://pipedapi.tokhmi.xyz",
    "https://piped-api.privacy.com.de", "https://api-piped.mha.fi" 
])
_PIPE_COOLDOWN: dict[str, float] = {}

def _fetch_from_alternative_api(hosts: deque, path: str, cooldown_map: dict[str, float], timeout: float = 2.0) -> dict | None:
    deadline = time.time() + timeout
    # Simple round-robin with cooldown
    for _ in range(len(hosts)):
        host = hosts.popleft() # Get from left
        hosts.append(host)    # Add to right for next cycle

        if time.time() >= deadline:
            logger.warning("Timeout fetching from alternative API for path: %s", path)
            break
        if cooldown_map.get(host, 0) > time.time():
            continue
        
        url = f"{host}{path}"
        current_proxy = get_proxy_dict()
        
        try:
            logger.debug("Attempting alternative API: %s (Proxy: %s)", url, "Yes" if current_proxy else "No")
            r = session.get(url, proxies=current_proxy, timeout=3) # Shorter timeout for alternatives
            r.raise_for_status()
            if "application/json" not in r.headers.get("Content-Type", ""):
                logger.warning("Non-JSON response from %s", url)
                cooldown_map[host] = time.time() + 600 # Longer cooldown for structural issues
                continue
            logger.info("Successfully fetched from alternative API: %s", url)
            return r.json()
        except requests.exceptions.RequestException as e:
            logger.warning("Alternative API host %s failed: %s", host, str(e))
            cooldown_map[host] = time.time() + 300 # Cooldown on error
        except Exception as e:
            logger.error("Unexpected error with alternative API host %s: %s", host, str(e), exc_info=True)
            cooldown_map[host] = time.time() + 300
    return None

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

# ---------------------------------------------------------------------------
# Webshare rotating residential gateway (new transcript logic)
DISABLE_DIRECT = os.getenv("FORCE_PROXY", "false").lower() == "true"

WS_USER = os.getenv("WEBSHARE_USER")
WS_PASS = os.getenv("WEBSHARE_PASS")

PROXY_CFG = None
if WS_USER and WS_PASS:
    if not WS_USER.endswith("-rotate"):
        WS_USER = f"{WS_USER}-rotate"
    from youtube_transcript_api.proxies import WebshareProxyConfig
    PROXY_CFG = WebshareProxyConfig(
        proxy_username=WS_USER,
        proxy_password=WS_PASS
    )
    logger.info("Using Webshare rotating residential proxies (username=%s)", WS_USER)
else:
    logger.info("No Webshare credentials – transcript requests will go direct")

# ---------------------------------------------------------------------------
# Helper – build a single rotating Webshare gateway URL
def _gateway_url() -> str | None:
    if WS_USER and WS_PASS:
        return f"http://{WS_USER}:{WS_PASS}@proxy.webshare.io:80"
    return None

def get_proxy_dict() -> dict:
    """Return {'https': gateway_url} if Webshare is configured, else {}."""
    url = _gateway_url()
    return {"https": url} if url else {}

# Back‑compat shim for legacy helper names -------------------------------
# Some old logic still refers to `rnd_proxy()` or `PROXY_ROTATION`.
PROXY_ROTATION: list[dict] = [get_proxy_dict()] if get_proxy_dict() else []

def rnd_proxy() -> dict:
    """Return a proxy mapping compatible with old code paths."""
    return get_proxy_dict()




# ---------------------------------------------------------------------------
# Primary fetch via youtube-transcript-api (v1.x compliant)
def fetch_api_once(video_id: str,
                   proxy_cfg,
                   timeout: int = 10,
                   languages: Optional[List[str]] = None) -> Optional[str]:
    """Single attempt using youtube-transcript-api. Returns plain text or None."""
    languages = languages or TRANSCRIPT_LANGS
    ytt_api = YouTubeTranscriptApi(proxy_config=proxy_cfg)
    try:
        ft = ytt_api.fetch(video_id, languages=languages)
    except NoTranscriptFound:
        return None

    segments = ft.to_raw_data() if hasattr(ft, "to_raw_data") else ft
    # Log a clear success message so we know this path was taken
    if segments:
        logger.info("✅ youtube-transcript-api SUCCESS for %s (%d chars)",
                    video_id, len(" ".join(seg["text"] if isinstance(seg, dict) else getattr(seg, "text", "")
                                  for seg in segments)))
    return " ".join(
        seg["text"] if isinstance(seg, dict) else getattr(seg, "text", "")
        for seg in segments
    ).strip() or None

# ---------------------------------------------------------------------------
# yt‑dlp subtitle fallback ---------------------------------------------------
def fetch_ytdlp(video_id: str, proxy_url: Optional[str]) -> Optional[str]:
    logger.info("[yt-dlp] Attempt (proxy=%s) for %s", bool(proxy_url), video_id)
    opts = {
        "quiet": True,
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": ["en.*", "en", "es.*", "es"],
        "subtitlesformat": "best[ext=srv3]",
        "proxy": proxy_url or None,
        "nocheckcertificate": True,
    }
    # If a cookie file is available (either user-provided via env or the
    # generated consent cookie), pass it to yt-dlp to reduce bot challenges.
    if YTDL_COOKIE_FILE:
        opts["cookiefile"] = YTDL_COOKIE_FILE
    with tempfile.TemporaryDirectory() as td:
        opts["outtmpl"] = f"{td}/%(id)s.%(ext)s"
        try:
            with YoutubeDL(opts) as ydl:
                ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
            fpath = next(pathlib.Path(td).glob(f"{video_id}*.srv3"), None)
            if not fpath:
                return None
            xml = fpath.read_text(encoding="utf-8", errors="ignore")
            return " ".join(
                html.unescape(t) for t in re.findall(r">([^<]+)</text>", xml)
            )
        except Exception as e:
            logger.warning("yt-dlp failed for %s: %s", video_id, e)
            return None

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Parallel alternative API fallback: fetch from multiple APIs in parallel
from cachetools import cached, TTLCache

# Cache for instance lists (1 hour TTL, up to 10 different endpoints)
instance_cache = TTLCache(maxsize=10, ttl=3600)

@cached(instance_cache)
def fetch_live_instances(api_url):
    try:
        response = session.get(api_url, timeout=5)
        response.raise_for_status()
        # For piped, the API returns a list of dicts with "api_url" and "active"
        # For invidious, the API returns a list of [url, info] where info["health"] and info["monitor"]["status"] may exist
        if "piped" in api_url:
            # Piped API
            return [instance["api_url"].rstrip("/") for instance in response.json() if instance.get("api_url") and instance.get("active")]
        elif "invidious" in api_url:
            # Invidious API (https://api.invidious.io/instances.json?sort_by=health)
            return [
                url.rstrip("/")
                for url, info in response.json()
                if info.get("type") == "https" and info.get("health", 0) > 0 and info.get("monitor", {}).get("status") == "online"
            ]
        else:
            return []
    except Exception as e:
        logger.warning(f"Failed to fetch instances from {api_url}: {e}")
        return []

def _fetch_transcript_alternatives(video_id: str) -> str | None:
    """
    Attempt to fetch transcript from multiple alternative APIs (Piped direct, Piped, Invidious) in parallel.
    Returns transcript text if any succeed, else None.
    """
    piped_instances = fetch_live_instances("https://piped-instances.kavin.rocks/")
    invidious_instances = fetch_live_instances("https://api.invidious.io/instances.json?sort_by=health")

    fetchers = [
        lambda vid=video_id: _piped_captions_direct(vid, piped_instances),
        lambda vid=video_id: _piped_captions(vid, piped_instances),
        lambda vid=video_id: _invidious_captions(vid, invidious_instances)
    ]

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetcher): fetcher.__name__ for fetcher in fetchers}
        for future in as_completed(futures, timeout=6):
            try:
                result = future.result()
                if result:
                    logger.info("✅ Alternative API transcript succeeded for %s", video_id)
                    return result
            except Exception as e:
                logger.warning("Transcript fetch via %s failed: %s", futures[future], e)
    logger.warning("All alternative API transcript fetchers failed for %s", video_id)
    return None

# ---------------------------------------------------------------------------
# Unified transcript fetching with clear fallback steps and logging
def get_transcript(video_id: str) -> str:
    """
    Transcript fetch logic with explicit, concise fallback steps:
    1. Try youtube-transcript-api (proxy if configured)
    2. Try alternative APIs in parallel (Piped direct, Piped, Invidious)
    3. Try yt-dlp (no proxy)
    4. Try yt-dlp (with proxy)
    Raise NoTranscriptFound if all fail.
    """
    # Step 1: Try youtube-transcript-api (with proxy if configured)
    try:
        txt = fetch_api_once(video_id, PROXY_CFG)
        if txt:
            logger.info("✅ Primary transcript fetch succeeded via youtube-transcript-api for %s", video_id)
            return txt
        else:
            logger.info("Primary transcript fetch failed: No transcript found via youtube-transcript-api for %s", video_id)
    except (RequestBlocked, CouldNotRetrieveTranscript, VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
        logger.warning("Primary transcript fetch blocked or failed for %s: %s", video_id, e)

    # Step 2: Try alternative APIs in parallel
    txt = _fetch_transcript_alternatives(video_id)
    if txt:
        logger.info("✅ Fallback transcript fetch succeeded via alternative APIs for %s", video_id)
        return txt
    else:
        logger.info("Alternative APIs fallback failed: No transcript found for %s", video_id)

    # Step 3: Try yt-dlp (no proxy)
    txt = fetch_ytdlp(video_id, None)
    if txt:
        logger.info("✅ Fallback transcript fetch succeeded via yt-dlp (no proxy) for %s", video_id)
        return txt
    else:
        logger.info("yt-dlp (no proxy) fallback failed: No transcript found for %s", video_id)

    # Step 4: Try yt-dlp (with proxy)
    proxy_url = _gateway_url()
    if proxy_url:
        txt = fetch_ytdlp(video_id, proxy_url)
        if txt:
            logger.info("✅ Fallback transcript fetch succeeded via yt-dlp (with proxy) for %s", video_id)
            return txt
        else:
            logger.info("yt-dlp (with proxy) fallback failed: No transcript found for %s", video_id)

    # All methods failed
    logger.error("❌ All transcript fetch methods failed for %s. Raising NoTranscriptFound.", video_id)
    raise NoTranscriptFound(video_id, [], None)

# --- Transcript Fallback Helpers ---
def _strip_tags(text: str) -> str:
    """Remove HTML/XML tags from a string."""
    return re.sub(r"<[^>]+>", "", text)

def _piped_captions_direct(video_id: str, hosts: list[str]) -> str | None:
    """
    Try to get captions directly from a list of Piped API instances.
    """
    if not hosts:
        logger.warning("No Piped hosts available for direct captions for %s", video_id)
        return None
    for host in hosts:
        try:
            meta = session.get(f"{host}/api/v1/captions/{video_id}", timeout=4)
            if meta.status_code == 404:
                return None
            meta.raise_for_status()
            data = meta.json()
            subs = data.get("captions") or []
            if not subs:
                continue
            chosen = next(
                (s for s in subs if s.get("language", "").lower().startswith("en")),
                subs[0]
            )
            url = chosen.get("url")
            if not url:
                continue
            if url.startswith("//"):
                url = "https:" + url
            raw = session.get(url, timeout=4).text
            text = " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", raw)).strip()
            if text:
                return text
        except Exception as e:
            logger.debug("Piped captions host %s failed: %s", host, e)
            continue
    logger.warning("All direct Piped caption mirrors failed for %s", video_id)
    return None

def _piped_captions(video_id: str, hosts: list[str]) -> str | None:
    """Try to get captions from Piped API using a list of hosts."""
    if not hosts:
        logger.warning("No Piped hosts available for fallback captions for %s", video_id)
        return None
    # Use a simple round-robin, try each host in order
    for host in hosts:
        try:
            resp = session.get(f"{host}/streams/{video_id}", timeout=4)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            data = resp.json()
            if "subtitles" not in data or not data["subtitles"]:
                continue
            subs = data["subtitles"]
            en_sub = next((s for s in subs if s.get("language", "").lower().startswith("en")), None)
            chosen = en_sub or subs[0]
            url = chosen.get("url")
            if not url:
                continue
            r = session.get(url, timeout=5)
            r.raise_for_status()
            data_text = r.text
            if url.endswith(".vtt"):
                lines = [l.strip() for l in data_text.splitlines() if l and not l.startswith("WEBVTT") and not re.match(r"^\d\d:\d\d", l)]
                text = " ".join(_strip_tags(l) for l in lines if not re.match(r"^\d+$", l))
                return text.strip() or None
            elif url.endswith(".srv3") or "<text" in data_text:
                return " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", data_text)).strip() or None
            else:
                continue
        except Exception as e:
            logger.debug("Piped fallback captions host %s failed: %s", host, e)
            continue
    logger.warning("All fallback Piped API hosts failed for %s", video_id)
    return None

def _invidious_captions(video_id: str, hosts: list[str]) -> str | None:
    """
    Fetch captions from a list of Invidious mirrors.
    • One attempt per host (no endless cycle / retry storm)
    • Normalises relative caption URLs returned by some mirrors
    • Logs individual host failures at DEBUG level only; a single
      WARNING is emitted if *all* hosts fail.
    """
    if not hosts:
        logger.warning("No Invidious hosts available for captions for %s", video_id)
        return None
    for host in hosts:
        try:
            meta_url = f"{host}/api/v1/captions/{video_id}"
            meta_resp = session.get(meta_url, timeout=5)
            if meta_resp.status_code == 404:
                # Video has no captions at all; no need to probe others
                return None
            meta_resp.raise_for_status()
            meta_json = meta_resp.json()
            subs = meta_json.get("captions") or []
            if not subs:
                continue
            # Prefer English, otherwise first available
            chosen = next(
                (s for s in subs if s.get("languageCode", "").lower().startswith("en")),
                subs[0],
            )
            rel_url = chosen.get("url")
            if not rel_url:
                continue
            # Some mirrors return a relative path → make it absolute
            caption_url = rel_url if rel_url.startswith("http") else urljoin(host, rel_url)
            cap_resp = session.get(caption_url, timeout=5)
            cap_resp.raise_for_status()
            raw = cap_resp.text
            # --- basic format sniffing / parsing ---
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
            # Unknown format → skip to next mirror
            continue
        except Exception as exc:
            logger.debug("Invidious host %s failed: %s", host, exc)
            continue  # try next mirror
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
def _fetch_comments_downloader(video_id: str, use_proxy: bool = False) -> list[str] | None:
    logger.debug("Attempting comments via youtube-comment-downloader for %s", video_id)
    try:
        # Using a new downloader instance per call to avoid state issues if any
        proxy_url = _gateway_url()
        downloader_kwargs = {}
        if use_proxy and proxy_url:
            downloader_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
            logger.debug("youtube-comment-downloader: using proxy %s for %s", proxy_url, video_id)

        downloader = YoutubeCommentDownloader(**downloader_kwargs)
        comments_generator = downloader.get_comments_from_url(
            f"https://www.youtube.com/watch?v={video_id}",
            sort_by=0,    # 0 for top (popular) comments
            language='en' # English comments
        )
        comments_text: list[str] = []
        for item in comments_generator:
            text = item.get("text")
            if text:
                comments_text.append(text)
            if len(comments_text) >= COMMENT_LIMIT:
                break

        if comments_text:
            logger.debug("Fetched %d comments via youtube-comment-downloader for %s", len(comments_text), video_id)
            return comments_text
        logger.warning("No comments returned by youtube-comment-downloader for %s", video_id)
        return []
    except Exception as e:
        logger.error("youtube-comment-downloader failed for %s: %s", video_id, str(e), exc_info=LOG_LEVEL=="DEBUG")
        return []

def yt_dlp_extract_info(video_id: str, extract_comments: bool = False, use_proxy: bool = False) -> dict | None:
    opts = _YDL_OPTS_BASE.copy()
    # --- CORRECTED LOGIC ---
    # Always add a random User-Agent to every yt-dlp request
    opts['http_headers'] = get_random_user_agent_header()
    # Apply proxy only if requested and configured
    if use_proxy and get_proxy_dict():
        proxy_url = _gateway_url()
        if proxy_url:
            opts["proxy"] = proxy_url
            logger.debug("yt-dlp: using proxy %s for %s", proxy_url, video_id)
    # --- END OF CORRECTION ---

    if extract_comments:
        opts["getcomments"] = True
        opts["max_comments"] = COMMENT_LIMIT

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.debug("yt-dlp: Extracting info for %s (comments: %s)", video_id, extract_comments)
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if info:
                logger.info("yt-dlp: Successfully extracted info for %s", video_id)
                return info
            logger.warning("yt-dlp: No info returned for %s", video_id)
            return None
    except Exception as e:
        logger.error("yt-dlp: Failed to extract info for %s: %s", video_id, str(e), exc_info=LOG_LEVEL == "DEBUG")
        return None

# --- Comment Fetching Logic (improved racer) ---

def _fetch_comments_resilient(video_id: str) -> list[str]:
    """Fetch comments in a primary→fallback→proxy→Piped sequence.

    1. Try youtube-comment-downloader without proxy.
    2. Fallback to yt-dlp without proxy if no comments.
    3. Try yt-dlp with proxy if still no comments.
    4. Try youtube-comment-downloader with proxy.
    5. Finally, try Piped API as the last resort.
    """
    logger.info("Initiating resilient comment fetch for %s", video_id)
    # Primary: youtube-comment-downloader (no proxy)
    comments = _fetch_comments_downloader(video_id, False)
    if comments:
        logger.info("✅ Comments (primary): %d via youtube-comment-downloader for %s", len(comments), video_id)
        return comments[:COMMENT_LIMIT]
    # Fallback: yt-dlp (no proxy)
    comments = _fetch_comments_yt_dlp(video_id, False)
    if comments:
        logger.info("✅ Comments (fallback): %d via yt-dlp (no proxy) for %s", len(comments), video_id)
        return comments[:COMMENT_LIMIT]
    # Fallback: yt-dlp (proxy)
    comments = _fetch_comments_yt_dlp(video_id, True)
    if comments:
        logger.info("✅ Comments (proxy): %d via yt-dlp for %s", len(comments), video_id)
        return comments[:COMMENT_LIMIT]
    # Fallback: youtube-comment-downloader (proxy)
    comments = _fetch_comments_downloader(video_id, True)
    if comments:
        logger.info("✅ Comments (proxy): %d via youtube-comment-downloader for %s", len(comments), video_id)
        return comments[:COMMENT_LIMIT]
    # Fallback: Piped API
    piped_data = _fetch_from_alternative_api(PIPED_HOSTS, f"/comments/{video_id}", _PIPE_COOLDOWN)
    if piped_data and "comments" in piped_data:
        comments_text = [c.get("commentText") for c in piped_data["comments"] if c.get("commentText")]
        if comments_text:
            logger.info("✅ Comments (piped): %d via piped API for %s", len(comments_text), video_id)
            return comments_text[:COMMENT_LIMIT]
    logger.warning("All comment sources failed for video %s. Returning empty list.", video_id)
    return []

def _fetch_comments_yt_dlp(video_id: str, use_proxy: bool = False) -> list[str] | None:
    """
    Fetch comments via yt-dlp's `getcomments` mechanism.
    Returns a list of comment texts or None/empty list.
    """
    info = yt_dlp_extract_info(video_id,
                               extract_comments=True,
                               use_proxy=use_proxy)
    if not info:
        return []
    comments_raw = info.get("comments") or []
    comments = [c.get("text") or c.get("comment") or "" for c in comments_raw]
    comments = [c for c in comments if c]
    if comments:
        logger.info("Fetched %d comments via yt-dlp (%sproxy) for %s",
                    len(comments), "" if use_proxy else "no-", video_id)
    return comments


# --- Endpoints ---
@app.before_request
def log_request_info():
    if request.path not in ['/health', '/health/deep']: # Avoid spamming health checks
        ip_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
        logger.info(f"Request: {request.method} {request.path} from {ip_addr} | Args: {request.args}")
        if request.is_json:
            try:
                # Log small JSON payloads, summarize large ones
                payload = request.get_json()
                payload_str = json.dumps(payload)
                if len(payload_str) > 500:
                    payload_str = payload_str[:500] + "... (truncated)"
                logger.debug(f"JSON Payload: {payload_str}")
            except Exception as e:
                logger.warning(f"Could not parse/log JSON payload: {e}")



# --- Background transcript worker helper ---
def _background_transcript_worker(video_id: str):
    """Obtiene transcript en segundo plano, guarda en caché y limpia pending."""
    try:
        if video_id in transcript_cache:
            return

        # Llamada real
        result = get_transcript(video_id)
        transcript_cache[video_id] = result
        with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
            db[video_id] = result
        logger.info("Transcript processing complete for %s (background)", video_id)

    except NoTranscriptFound:
        if video_id not in transcript_cache:
            transcript_cache[video_id] = "__NOT_AVAILABLE__"
            with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
                db[video_id] = "__NOT_AVAILABLE__"
        logger.warning("Transcript marked NOT AVAILABLE for %s", video_id)

    except Exception as e:
        logger.error("Background transcript worker failed for %s: %s", video_id, str(e))

    finally:
        _pending_futures.pop(video_id, None)

# --- Background comment worker helper ---
def _background_comment_worker(video_id: str):
    """Fetch comments in background, cache result and clean pending."""
    try:
        if video_id in comment_cache:
            return
        result = _fetch_comments_resilient(video_id)
        comment_cache[video_id] = result
        with shelve.open(PERSISTENT_COMMENT_DB) as db:
            db[video_id] = result
        logger.info("Comment processing complete for %s (background)", video_id)
    except Exception as e:
        logger.error("Background comment worker failed for %s: %s", video_id, str(e))
    finally:
        _pending_futures.pop(f"comments_{video_id}", None)



@app.route("/transcript", methods=["GET"])
@limiter.limit("1000/hour;200/minute")  # Increased limits for transcript endpoint
def get_transcript_endpoint():
    t0 = time.perf_counter()
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        log_event('warning', 'transcript_missing_video_id', ip=g.ip)
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        log_event('warning', 'transcript_invalid_video_id', raw=video_url_or_id, ip=g.ip)
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    


    # Check RAM cache
    cached = transcript_cache.get(video_id)
    if cached is not None:
        if cached == "__NOT_AVAILABLE__":
            log_event('info', 'transcript_cache_miss_marker', video_id=video_id)
            return jsonify({"error": "Transcript not available"}), 404
        log_event('info', 'transcript_cache_hit', video_id=video_id, text_len=len(cached))
        return jsonify({"video_id": video_id, "text": cached}), 200

    # Try persistent cache
    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
        cached = db.get(video_id)
        if cached is not None:
            transcript_cache[video_id] = cached
            if cached == "__NOT_AVAILABLE__":
                log_event('info', 'transcript_persisted_not_available', video_id=video_id)
                return jsonify({"error": "Transcript not available"}), 404
            log_event('info', 'transcript_persisted_hit', video_id=video_id, text_len=len(cached))
            return jsonify({"video_id": video_id, "text": cached}), 200

    # Try fetch with short retry/backoff on transient errors
    attempts = 0
    last_err = None
    while attempts < 2:
        try:
            text = get_transcript(video_id)
            transcript_cache[video_id] = text
            with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
                db[video_id] = text
            log_event('info', 'transcript_fetched', video_id=video_id, text_len=len(text), duration_ms=int((time.perf_counter()-t0)*1000))
            resp = make_response(jsonify({"video_id": video_id, "text": text}), 200)
            resp.headers['Cache-Control'] = 'public, max-age=3600'
            return resp
        except NoTranscriptFound:
            if video_id not in transcript_cache:
                transcript_cache[video_id] = "__NOT_AVAILABLE__"
                with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
                    db[video_id] = "__NOT_AVAILABLE__"
            log_event('warning', 'transcript_not_found', video_id=video_id, duration_ms=int((time.perf_counter()-t0)*1000))
            resp = make_response(jsonify({"error": "Transcript not available"}), 404)
            resp.headers['Cache-Control'] = 'public, max-age=600'
            return resp
        except requests.exceptions.RequestException as e:
            # Transient network/SSL errors – backoff and retry once
            attempts += 1
            last_err = e
            time.sleep(0.5 * attempts)
            continue
        except Exception as e:
            # Non-retryable internal error
            log_event('error', 'transcript_fetch_failed', video_id=video_id, error=str(e), duration_ms=int((time.perf_counter()-t0)*1000))
            return jsonify({"error": "Transcript fetch failed"}), 500
    # Retries exhausted → treat as not available
    log_event('warning', 'transcript_unavailable_after_retry', video_id=video_id, error=str(last_err) if last_err else None, duration_ms=int((time.perf_counter()-t0)*1000))
    resp = make_response(jsonify({"error": "Transcript not available"}), 404)
    resp.headers['Cache-Control'] = 'public, max-age=600'
    return resp

@app.route("/comments", methods=["GET"])
@limiter.limit("120/hour;20/minute")  # Limits for comments endpoint
def get_comments_endpoint():
    t0 = time.perf_counter()
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    cached = comment_cache.get(video_id)
    if cached is not None:
        log_event('info', 'comments_cache_hit', video_id=video_id, count=len(cached))
        return jsonify({"video_id": video_id, "comments": cached}), 200

    with shelve.open(PERSISTENT_COMMENT_DB) as db:
        cached = db.get(video_id)
        if cached is not None:
            comment_cache[video_id] = cached
            log_event('info', 'comments_persisted_hit', video_id=video_id, count=len(cached))
            return jsonify({"video_id": video_id, "comments": cached}), 200

    try:
        comments = _fetch_comments_resilient(video_id)
        comment_cache[video_id] = comments
        with shelve.open(PERSISTENT_COMMENT_DB) as db:
            db[video_id] = comments
        log_event('info', 'comments_fetched', video_id=video_id, count=len(comments), duration_ms=int((time.perf_counter()-t0)*1000))
        return jsonify({"video_id": video_id, "comments": comments}), 200
    except Exception as e:
        log_event('error', 'comments_fetch_failed', video_id=video_id, error=str(e), duration_ms=int((time.perf_counter()-t0)*1000))
        return jsonify({"error": "Comment fetch failed"}), 500

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
    logged_payload = {k: v for k, v in payload.items() if k not in ['input', 'text']} # Avoid logging large text fields
    if 'input' in payload: logged_payload['input_length'] = len(payload['input'])
    if 'text' in payload and isinstance(payload['text'], dict) and 'input' in payload['text'] : # Handle new structure
        logged_payload['text_input_length'] = len(payload['text']['input'])
        payload_text_copy = payload['text'].copy()
        payload_text_copy.pop('input', None)
        logged_payload['text_other_fields'] = payload_text_copy

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
        import itertools as _it
        from flask import Response, stream_with_context

        def _iter_sse():
            client_disconnected = False
            try:
                # Open upstream stream
                upstream = session.post(openai_endpoint, headers=headers, json=payload, timeout=proxy_timeout, stream=True)
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


# --- Render root health check ------------------------------------------------
@app.route("/", methods=["GET"])
def root_ok():
    """
    Simple 200 OK for Render's default health check.
    Returns minimal JSON with uptime in seconds.
    """
    uptime = round(time.time() - app_start_time)
    return jsonify({"status": "ok", "uptime": uptime}), 200

@app.route("/privacy")
def privacy(): return send_from_directory("static", "privacy.html")

@app.route("/terms")
def terms(): return send_from_directory("static", "terms.html")

@app.route("/support")
def support(): return send_from_directory("static", "support.html")

@app.route('/favicon.ico')
def favicon():
    # Avoid noisy 404/500s when browsers request favicon
    return ("", 204)


# --- Cleanup on Exit ---
def cleanup_on_exit():
    logger.info("Shutting down thread pool...")
    executor.shutdown(wait=True) # Wait for tasks to complete
    # Clear pending futures to avoid issues on restart if process is killed abruptly
    _pending_futures.clear()
    
    # Close shelve databases properly (important for data integrity)
    # This is harder to guarantee with shelve if app crashes.
    # Consider more robust DBs if data loss is critical.
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
