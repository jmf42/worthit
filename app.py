import os
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
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future


from flask import Flask, request, jsonify
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
from flask_limiter.util import get_remote_address
import requests
import shutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
PERSISTENT_CACHE_DIR = os.path.join(os.getcwd(), "persistent_cache")
PERSISTENT_TRANSCRIPT_DB = os.path.join(PERSISTENT_CACHE_DIR, "transcript_cache.db")
PERSISTENT_COMMENT_DB = os.path.join(PERSISTENT_CACHE_DIR, "comment_cache.db")
# YTDL Cookie file configuration
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE", "/etc/secrets/cookies_chrome2.txt")

# ------------------------------------------------------------------
# Universal CONSENT cookie (avoids 204/empty captions from YouTube)
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
    logger.info("Using default CONSENT cookie at %s", _consent_path)
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
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "app_server.log")

    logging.basicConfig(level=logging.WARNING) # Keep external libraries less verbose

    logger = logging.getLogger(APP_NAME)
    logger.setLevel(LOG_LEVEL)
    if logger.hasHandlers():
        logger.handlers.clear()
    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    ch_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)')
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)

    # File Handler
    fh = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    fh.setLevel(LOG_LEVEL)
    fh_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)')
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)
    
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
CORS(app) # Allow all origins for simplicity in this context

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
# Unified entry point --------------------------------------------------------
def get_transcript(video_id: str) -> str:
    """Best‑effort transcript fetch using the new logic."""
    try:
        txt = fetch_api_once(video_id, PROXY_CFG)
        if txt:
            return txt
    except (RequestBlocked, CouldNotRetrieveTranscript,
            VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
        logger.warning("youtube-transcript-api blocked: %s", e.__class__.__name__)
    except Exception as e:
        logger.error("Unexpected youtube-transcript-api error: %s", e)

    txt = fetch_ytdlp(video_id, None)
    if txt:
        logger.info("✅ yt-dlp FALLBACK SUCCESS (no proxy) for %s (%d chars)", video_id, len(txt))
        return txt

    if PROXY_CFG:
        gateway_url = f"http://{WS_USER}:{WS_PASS}@proxy.webshare.io:80"
        txt = fetch_ytdlp(video_id, gateway_url)
        if txt:
            logger.info("✅ yt-dlp FALLBACK SUCCESS (proxy) for %s (%d chars)", video_id, len(txt))
            return txt

    raise NoTranscriptFound(video_id, [], None)

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
        downloader = YoutubeCommentDownloader()
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
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    


    # Check RAM cache
    cached = transcript_cache.get(video_id)
    if cached is not None:
        if cached == "__NOT_AVAILABLE__":
            return jsonify({"error": "Transcript not available"}), 404
        logger.info("📄 Transcript cache HIT (RAM) for %s", video_id)
        return jsonify({"video_id": video_id, "text": cached}), 200

    # Try persistent cache
    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
        cached = db.get(video_id)
        if cached is not None:
            transcript_cache[video_id] = cached
            if cached == "__NOT_AVAILABLE__":
                return jsonify({"error": "Transcript not available"}), 404
            logger.info("📄 Transcript cache HIT (disk) for %s", video_id)
            return jsonify({"video_id": video_id, "text": cached}), 200

    # Try fetch (max 7s)
    try:
        text = get_transcript(video_id)
        transcript_cache[video_id] = text
        with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
            db[video_id] = text
        return jsonify({"video_id": video_id, "text": text}), 200
    except NoTranscriptFound:
        if video_id not in transcript_cache:
            transcript_cache[video_id] = "__NOT_AVAILABLE__"
            with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
                db[video_id] = "__NOT_AVAILABLE__"
        return jsonify({"error": "Transcript not available"}), 404
    except Exception as e:
        logger.error("❌ Transcript fetch failed for %s: %s", video_id, e)
        return jsonify({"error": "Transcript fetch failed"}), 500

@app.route("/comments", methods=["GET"])
@limiter.limit("120/hour;20/minute")  # Limits for comments endpoint
def get_comments_endpoint():
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    cached = comment_cache.get(video_id)
    if cached is not None:
        return jsonify({"video_id": video_id, "comments": cached}), 200

    with shelve.open(PERSISTENT_COMMENT_DB) as db:
        cached = db.get(video_id)
        if cached is not None:
            comment_cache[video_id] = cached
            return jsonify({"video_id": video_id, "comments": cached}), 200

    try:
        comments = _fetch_comments_resilient(video_id)
        comment_cache[video_id] = comments
        with shelve.open(PERSISTENT_COMMENT_DB) as db:
            db[video_id] = comments
        return jsonify({"video_id": video_id, "comments": comments}), 200
    except Exception as e:
        logger.error("❌ Comment fetch failed for %s: %s", video_id, e)
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

    logger.info("🤖 OpenAI proxy → model=%s | scrubbed payload=%s", payload.get("model", "N/A"), json.dumps(logged_payload))

    openai_endpoint = "https://api.openai.com/v1/responses" # Keep as original

    try:
        resp = session.post(openai_endpoint, headers=headers, json=payload, timeout=15) # 15 s UX-budget
        logger.info("✅ OpenAI response → %d for model=%s", resp.status_code, payload.get("model", "N/A"))

        if resp.status_code != 200:
            error_content = resp.text[:1000] # Log part of the error
            logger.error(f"OpenAI API error {resp.status_code}. Body: {error_content}")
            # Try to parse standard OpenAI error for clearer message to client
            try:
                error_json = resp.json()
                detailed_error = error_json.get("error", {}).get("message", "OpenAI API error")
                return jsonify({'error': detailed_error, 'details': error_json}), resp.status_code
            except json.JSONDecodeError:
                return jsonify({'error': 'OpenAI API error', 'details': error_content}), resp.status_code

        response_data = resp.json()
        # Log a snippet of the successful response for debugging structure
        response_preview = {k: (str(v)[:100] + '...' if isinstance(v, (str, list, dict)) and len(str(v)) > 100 else v) for k,v in response_data.items()}
        logger.debug(f"OpenAI successful response preview: {json.dumps(response_preview)}")

        return jsonify(response_data), resp.status_code

    except requests.exceptions.Timeout:
        logger.error("Request to OpenAI API timed out.")
        return jsonify({'error': 'Request to OpenAI timed out'}), 504
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error communicating with OpenAI: {e}", exc_info=True)
        return jsonify({'error': 'Network error with OpenAI service'}), 503
    except Exception as e:
        logger.error(f"Unexpected error in OpenAI proxy: {e}", exc_info=True)
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
    port = int(os.environ.get("PORT", 5020)) # Changed port slightly from 5010
    logger.info(f"Starting {APP_NAME} on port {port}")
    # Use Waitress or Gunicorn for production instead of Flask's built-in server
    # For Render, it usually handles this.
    app.run(host="0.0.0.0", port=port, threaded=True)