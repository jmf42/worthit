import os
import shelve
import re
import json
import logging
import time
import itertools
import random
import urllib.parse
from functools import lru_cache
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future

# --- XML for robust transcript parsing ---
import xml.etree.ElementTree as _ET

# --- Proxy force env ---
FORCE_PROXY = os.getenv("FORCE_PROXY", "false").lower() == "true"

from flask import Flask, request, jsonify
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound
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
import functools
from youtube_comment_downloader import YoutubeCommentDownloader
from flask import send_from_directory

# --- Ensure logger is defined early ---
logger = logging.getLogger("WorthItService")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# --- Configuration ---
APP_NAME = "WorthItService"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(min(4, (os.cpu_count() or 1)))))
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "50"))
TRANSCRIPT_CACHE_SIZE = int(os.getenv("TRANSCRIPT_CACHE_SIZE", "200"))
TRANSCRIPT_CACHE_TTL = int(os.getenv("TRANSCRIPT_CACHE_TTL", "7200")) # 2 hours
#
# Transcript tuning
TRANSCRIPT_HTTP_TIMEOUT   = int(os.getenv("TRANSCRIPT_HTTP_TIMEOUT", "15"))
TRANSCRIPT_PROXY_ATTEMPTS = int(os.getenv("TRANSCRIPT_PROXY_ATTEMPTS", "3"))
TRANSCRIPT_DIRECT_ATTEMPT = os.getenv("TRANSCRIPT_DIRECT_ATTEMPT", "true").lower() != "false"
COMMENT_CACHE_SIZE = int(os.getenv("COMMENT_CACHE_SIZE", "150"))
COMMENT_CACHE_TTL = int(os.getenv("COMMENT_CACHE_TTL", "7200")) # 2 hours
PERSISTENT_CACHE_DIR = os.path.join(os.getcwd(), "persistent_cache")
PERSISTENT_TRANSCRIPT_DB = os.path.join(PERSISTENT_CACHE_DIR, "transcript_cache.db")
PERSISTENT_COMMENT_DB = os.path.join(PERSISTENT_CACHE_DIR, "comment_cache.db")
# YTDL Cookie file configuration
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE", "")

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


# High-quality Residential Proxy Configuration
SMARTPROXY_USER = os.getenv("SMARTPROXY_USER")
SMARTPROXY_PASS = os.getenv("SMARTPROXY_PASS")
SMARTPROXY_HOST = os.getenv("SMARTPROXY_HOST")
SMARTPROXY_PORTS_STR = os.getenv("SMARTPROXY_PORTS")

PROXY_ROTATION = []
# Check if all parts of the proxy config are present in the environment
if SMARTPROXY_USER and SMARTPROXY_PASS and SMARTPROXY_HOST and SMARTPROXY_PORTS_STR:
    SMARTPROXY_PORTS = SMARTPROXY_PORTS_STR.split(",")

    # URL-encode the password to handle special characters like '~' safely
    encoded_pass = urllib.parse.quote_plus(SMARTPROXY_PASS)

    # Build the list of proxy URLs using the SMARTPROXY variables
    PROXY_ROTATION = [
        {"https": f"http://{SMARTPROXY_USER}:{encoded_pass}@{SMARTPROXY_HOST}:{port}"}
        for port in SMARTPROXY_PORTS
    ]
    logger.info(f"Proxy rotation configured with {len(PROXY_ROTATION)} residential endpoints.")
else:
    # Fallback to no proxy if credentials are not fully configured
    PROXY_ROTATION = [{}]
    logger.warning("Proxy credentials not fully configured. Running without proxies.")

# Create the iterator for the rest of the app to use
_proxy_cycle = itertools.cycle(PROXY_ROTATION)



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

def rnd_proxy() -> dict:
    return next(_proxy_cycle)

def get_random_user_agent_header():
    """Returns a dictionary with a randomly chosen User-Agent header and Accept-Language."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.8"
    }


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
# --- Fast playerResponse fallback for captions ---
def _fetch_transcript_player(video_id: str) -> str | None:
    """
    Fetch captions by parsing YouTube playerResponse JSON.
    Often succeeds when list_transcripts fails (auto‑CC present).
    """
    url = f"https://www.youtube.com/watch?v={video_id}&pbj=1"
    headers = get_random_user_agent_header()
    proxy_cfg = {}  # first attempt direct
    try:
        r = session.get(url, headers=headers, proxies=proxy_cfg, timeout=10)
        r.raise_for_status()
        if '"captionTracks"' not in r.text:
            return None
        # Try to parse the correct playerResponse JSON structure
        try:
            js = r.json()
            if isinstance(js, list) and len(js) > 2 and "playerResponse" in js[2]:
                data = js[2]['playerResponse']
            elif isinstance(js, list) and js and "player_response" in js[0]:
                data = js[0]['player_response']
            else:
                return None
        except Exception:
            return None
        tracks = (data.get("captions", {})
                       .get("playerCaptionsTracklistRenderer", {})
                       .get("captionTracks", []))
        if not tracks:
            return None
        # Try VTT first, then SRV3 (XML) if VTT is empty
        import html
        base_url = tracks[0].get("baseUrl")
        text_payload = ""

        # 1️⃣ attempt VTT
        vtt_url = base_url + ("&fmt=vtt" if "fmt=" not in base_url else re.sub(r"fmt=\w+", "fmt=vtt", base_url))
        vtt_resp = session.get(vtt_url, headers=headers, proxies=proxy_cfg, timeout=10).text
        if vtt_resp.strip() and "-->" in vtt_resp:
            text_payload = vtt_resp
        else:
            # 2️⃣ attempt SRV3 XML
            srv3_url = base_url + ("&fmt=srv3" if "fmt=" not in base_url else re.sub(r"fmt=\w+", "fmt=srv3", base_url))
            srv3_resp = session.get(srv3_url, headers=headers, proxies=proxy_cfg, timeout=10).text
            if "<text" in srv3_resp:
                text_payload = srv3_resp

        if not text_payload:
            return None

        if text_payload.lstrip().startswith("WEBVTT"):
            # Parse VTT
            lines = [re.sub(r"<[^>]+>", "", ln).strip() for ln in text_payload.splitlines()
                     if ln and "-->" not in ln and not ln.startswith("WEBVTT")]
            return " ".join(lines).strip() or None
        else:
            # Parse SRV3 XML
            texts = re.findall(r'>([^<]+)</text>', text_payload)
            return " ".join(html.unescape(t).strip() for t in texts).strip() or None
    except Exception as e:
        logger.debug("playerResponse fallback failed for %s: %s", video_id, e)
        # second chance with proxy
        if not proxy_cfg and PROXY_ROTATION and PROXY_ROTATION[0]:
            try:
                proxy_cfg = rnd_proxy()
                return _fetch_transcript_player(video_id)  # recurse once with proxy
            except Exception:
                pass
        return None

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
        current_proxy = rnd_proxy() if PROXY_ROTATION[0] else {} # Use proxy if configured
        
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

def yt_dlp_extract_info(video_id: str, extract_comments: bool = False, use_proxy: bool = False) -> dict | None:
    opts = _YDL_OPTS_BASE.copy()
    # Apply Smartproxy if requested
    if use_proxy and PROXY_ROTATION and PROXY_ROTATION[0]:
        proxy_dict = rnd_proxy()
        proxy_url = proxy_dict.get("https")
        opts['http_headers'] = get_random_user_agent_header()
        if proxy_url:
            opts["proxy"] = proxy_url
            logger.debug("yt-dlp: using proxy %s for %s", proxy_url, video_id)
    if extract_comments:
        opts["getcomments"] = True
        opts["max_comments"] = COMMENT_LIMIT # yt-dlp specific max comments for this call
    
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

# --- Transcript Fetching Logic ---
FALLBACK_LANGUAGES = [
    'en', 'en-US', 'en-GB', 'en-CA', 'en-AU',
    'es', 'fr', 'de', 'pt', 'ru', 'it', 'nl',
    'hi', 'ja', 'ko', 'ar', 'zh', 'zh-Hans', 'zh-Hant'
]

def _fetch_transcript_api(video_id: str,
                          languages: list[str],
                          proxy_cfg: dict | None,
                          timeout: int = TRANSCRIPT_HTTP_TIMEOUT) -> str | None:
    """
    Perform a single transcript fetch using the specified proxy (or no proxy).
    Returns the transcript text or None.
    """
    headers = get_random_user_agent_header()
    response = None
    try:
        logger.info("[TRANSCRIPT] Paso 1: intentado obtener transcripts disponibles para video_id=%s, proxy=%s, timeout=%s", video_id, bool(proxy_cfg), timeout)
        # Small jitter so all requests do not look like a bot burst
        time.sleep(random.uniform(0.2, 0.7))
        logger.info("[TRANSCRIPT] Usando proxy IP: %s", proxy_cfg.get("https") if proxy_cfg else "direct")
        transcript_list = _list_transcripts_safe(
            video_id,
            proxies=proxy_cfg,
            timeout=timeout,
            http_headers=headers,
            cookies=YTDL_COOKIE_FILE if YTDL_COOKIE_FILE else None,
        )
        logger.info("[TRANSCRIPT] Paso 2: transcripts listados correctamente para video_id=%s", video_id)
    except Exception as e:
        logger.error("[TRANSCRIPT] Error al obtener lista de transcripts para video_id=%s: %s", video_id, e)
        raise
    for finder in (transcript_list.find_transcript, transcript_list.find_generated_transcript):
        try:
            logger.info("[TRANSCRIPT] Paso 3: intentando buscar transcript con finder=%s para video_id=%s", finder.__name__, video_id)
            tr = finder(languages)
            logger.info("[TRANSCRIPT] Paso 4: transcript encontrado, intentando fetch() para video_id=%s", video_id)
            try:
                text = " ".join(
                    seg.text if hasattr(seg, "text") else seg["text"]
                    for seg in tr.fetch()
                ).strip()
                if not text:
                    raise ValueError("Empty transcript payload")
            except (ValueError, _ET.ParseError) as e:
                # Likely a CAPTCHA / empty XML. Force next proxy.
                logger.warning(
                    "[TRANSCRIPT] Empty or invalid XML for %s via proxy=%s – forcing next proxy",
                    video_id, bool(proxy_cfg)
                )
                raise CouldNotRetrieveTranscript(video_id)
            except Exception as e:
                logger.error(
                    "[TRANSCRIPT] Error procesando segmentos para video_id=%s: %s",
                    video_id, e
                )
                raise
            if text:
                logger.info("[TRANSCRIPT] Paso 5: transcript obtenido con éxito para video_id=%s, longitud=%d", video_id, len(text))
                return text
            else:
                logger.info("[TRANSCRIPT] Paso 5: transcript vacío para video_id=%s", video_id)
        except NoTranscriptFound as nf:
            logger.info("[TRANSCRIPT] NoTranscriptFound usando finder=%s para video_id=%s: %s", finder.__name__, video_id, nf)
            continue
        except Exception as e:
            logger.error("[TRANSCRIPT] Error inesperado en finder=%s para video_id=%s: %s", finder.__name__, video_id, e)
            continue
    logger.info("[TRANSCRIPT] Paso final: no se encontró transcript disponible para video_id=%s", video_id)
    return None
# --- Helper for CAPTCHA detection ---
# --- Helper for CAPTCHA detection ---
def _is_captcha_response(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(marker in lower for marker in (
        "captcha", "recaptcha", "i am not a robot",
        "confirm you’re not a robot", "why did this happen"
    ))

# --- yt-dlp subtitle fallback ---
def _fetch_transcript_yt_dlp(video_id: str) -> str | None:
    """
    Last‑chance subtitle grab using yt‑dlp auto‑subs (VTT) and convert to plain text.
    Returns plain text or None.
    """
    import tempfile, pathlib, html

    with tempfile.TemporaryDirectory() as tmp_dir:
        outtmpl = str(pathlib.Path(tmp_dir) / "%(id)s.%(ext)s")

        opts = _YDL_OPTS_BASE.copy()
        opts.update({
            "writesubtitles": True,
            "writeautomaticsub": True,
            # “*” alone is invalid. Use regex wildcards accepted by yt‑dlp:
            "subtitleslangs": ["en", "en-.*", ".*"],
            "subtitlesformat": "best",   # yt‑dlp picks vtt → srt when convert_subs is set
            "convert_subs": "srt",
            "outtmpl": outtmpl,
            "quiet": True,
            "skip_download": True,
            "nooverwrites": True,
            "max_downloads": 1,
            "http_headers": get_random_user_agent_header(),
        })

        caption_files = []
        # Attempt 0 → no proxy (direct), then rotate proxies
        proxy_cycle = [None] + (PROXY_ROTATION if PROXY_ROTATION and PROXY_ROTATION[0] else [])
        for attempt, proxy_dict in enumerate(proxy_cycle[:6]):  # max 6 tries (1 direct + 5 proxies)
            opts_try = opts.copy()
            if proxy_dict:
                proxy_url = proxy_dict.get("https")
                if proxy_url:
                    opts_try["proxy"] = proxy_url
            try:
                with YoutubeDL(opts_try) as ydl:
                    ydl.extract_info(
                        f"https://www.youtube.com/watch?v={video_id}",
                        download=True
                    )
                caption_files = (
                    list(pathlib.Path(tmp_dir).glob(f"{video_id}*.srt")) +
                    list(pathlib.Path(tmp_dir).glob(f"{video_id}*.vtt"))
                )
                if caption_files:
                    break  # éxito
            except Exception as e:
                if "data blocks" in str(e) or "HTTP Error 4" in str(e):
                    logger.debug("yt-dlp attempt %d failed for %s: %s", attempt+1, video_id, e)
                    time.sleep(1.2)
                    continue
                raise
        if not caption_files:
            return None
        cap_text = caption_files[0].read_text(encoding="utf-8", errors="ignore")

    # Strip WEBVTT/SRT header and timestamps
    lines: list[str] = []
    for line in cap_text.splitlines():
        line = line.strip()
        if not line or line.startswith(("WEBVTT", "X-TIMESTAMP-MAP")):
            continue
        if "-->" in line:
            continue
        # For SRT, skip numeric index lines
        if line.isdigit():
            continue
        lines.append(html.unescape(line))
    plain = " ".join(lines).strip()
    return plain or None

def _fetch_transcript_resilient(video_id: str) -> str:
    """
    Try up to TRANSCRIPT_PROXY_ATTEMPTS rotating proxies, then (optionally)
    fall back to a direct request. Raises NoTranscriptFound if all fail.
    """
    langs = ["en"] + FALLBACK_LANGUAGES

    # Proxy loop
    for _ in range(TRANSCRIPT_PROXY_ATTEMPTS):
        proxy_cfg = rnd_proxy() if (FORCE_PROXY or (PROXY_ROTATION and PROXY_ROTATION[0])) else None
        try:
            if txt := _fetch_transcript_api(video_id, langs, proxy_cfg):
                logger.info("Transcript fetched for %s via %s",
                            video_id, "proxy" if proxy_cfg else "direct")
                return txt
        except (TranscriptsDisabled, CouldNotRetrieveTranscript, NoTranscriptFound) as e:
            logger.debug("Transcript not found via %s for %s: %s",
                         "proxy" if proxy_cfg else "direct", video_id, e)
            # Continue to next proxy or direct fallback
            continue
        except Exception as e:
            logger.debug("Proxy attempt failed for %s: %s", video_id, e)

    # Direct fallback
    if TRANSCRIPT_DIRECT_ATTEMPT:
        try:
            if txt := _fetch_transcript_api(video_id, langs, None):
                logger.info("Transcript fetched for %s via direct fallback", video_id)
                return txt
        except Exception as e:
            logger.debug("Direct fallback failed for %s: %s", video_id, e, exc_info=LOG_LEVEL=="DEBUG")

    # Player‑response JSON fallback (fast)
    try:
        if text := _fetch_transcript_player(video_id):
            logger.info("Transcript fetched for %s via playerResponse fallback", video_id)
            return text
    except Exception:
        pass

    # Ultimate fallback: yt‑dlp auto subs
    try:
        if text := _fetch_transcript_yt_dlp(video_id):
            logger.info("Transcript fetched for %s via yt‑dlp auto‑subs fallback", video_id)
            return text
    except Exception:
        pass

    logger.error("All transcript sources failed for video %s", video_id)
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
            logger.info("Fetched %d comments via youtube-comment-downloader for %s", len(comments_text), video_id)
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
    if use_proxy and PROXY_ROTATION and PROXY_ROTATION[0]:
        proxy_dict = rnd_proxy()
        proxy_url = proxy_dict.get("https")
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
    logger.info("Fetching comments with primary youtube-comment-downloader for %s", video_id)
    comments = _fetch_comments_downloader(video_id, False)
    if comments:
        logger.info("Fetched %d comments via youtube-comment-downloader (primary) for %s", len(comments), video_id)
        return comments[:COMMENT_LIMIT]
    logger.info("Primary fetcher returned no comments, falling back to yt-dlp for %s", video_id)
    comments = _fetch_comments_yt_dlp(video_id, False)
    if comments:
        logger.info("Fetched %d comments via yt-dlp (fallback)", len(comments), video_id)
        return comments[:COMMENT_LIMIT]

    # Tier 3: Try yt-dlp with proxy
    logger.info("Fallback to yt-dlp with proxy for %s", video_id)
    comments = _fetch_comments_yt_dlp(video_id, True)
    if comments:
        logger.info("Fetched %d comments via yt-dlp (proxy fallback)", len(comments), video_id)
        return comments[:COMMENT_LIMIT]

    # Tier 4: Try youtube-comment-downloader with proxy
    logger.info("Fallback to youtube-comment-downloader with proxy for %s", video_id)
    comments = _fetch_comments_downloader(video_id, True)
    if comments:
        logger.info("Fetched %d comments via youtube-comment-downloader (proxy fallback)", len(comments), video_id)
        return comments[:COMMENT_LIMIT]

    # Tier 5: Piped API
    piped_data = _fetch_from_alternative_api(PIPED_HOSTS, f"/comments/{video_id}", _PIPE_COOLDOWN)
    if piped_data and "comments" in piped_data:
        comments_text = [c.get("commentText") for c in piped_data["comments"] if c.get("commentText")]
        if comments_text:
            logger.info("Fetched %d comments via Piped API for %s", len(comments_text), video_id)
            return comments_text[:COMMENT_LIMIT]

    logger.warning("All comment sources failed for video %s. Returning empty list.", video_id)
    return []

def _get_or_spawn_comments(*_args, **_kwargs):
    """
    DEPRECATED: ya no se usa flujo bloqueante. Los comentarios se gestionan de forma
    asíncrona con _background_comment_worker y el endpoint /comments.
    """
    raise RuntimeError("_get_or_spawn_comments() is deprecated – use async endpoint flow")

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
        result = _fetch_transcript_resilient(video_id)
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
        return jsonify({"video_id": video_id, "text": cached}), 200

    # Try persistent cache
    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
        cached = db.get(video_id)
        if cached is not None:
            transcript_cache[video_id] = cached
            if cached == "__NOT_AVAILABLE__":
                return jsonify({"error": "Transcript not available"}), 404
            return jsonify({"video_id": video_id, "text": cached}), 200

    # Try fetch (max 7s)
    try:
        text = _fetch_transcript_resilient(video_id)
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
        logger.error(f"Transcript fetch failed for {video_id}: {e}")
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
        logger.error(f"Comment fetch failed for {video_id}: {e}")
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
    
    logger.info(f"Proxying to OpenAI. Model: {payload.get('model', 'N/A')}. Payload (scrubbed): {json.dumps(logged_payload)}")

    openai_endpoint = "https://api.openai.com/v1/responses" # Keep as original

    try:
        resp = session.post(openai_endpoint, headers=headers, json=payload, timeout=15) # 15 s UX-budget
        logger.info(f"OpenAI API response status: {resp.status_code}")

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