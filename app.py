import os
import shelve
import re
import json
import logging
import time
import itertools
from functools import lru_cache
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future

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
import functools
from youtube_comment_downloader import YoutubeCommentDownloader

# --- Configuration ---
APP_NAME = "WorthItService"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(min(8, (os.cpu_count() or 1) * 2))))
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "50"))
TRANSCRIPT_CACHE_SIZE = int(os.getenv("TRANSCRIPT_CACHE_SIZE", "200"))
TRANSCRIPT_CACHE_TTL = int(os.getenv("TRANSCRIPT_CACHE_TTL", "7200")) # 2 hours
COMMENT_CACHE_SIZE = int(os.getenv("COMMENT_CACHE_SIZE", "150"))
COMMENT_CACHE_TTL = int(os.getenv("COMMENT_CACHE_TTL", "7200")) # 2 hours
PERSISTENT_CACHE_DIR = os.path.join(os.getcwd(), "persistent_cache")
PERSISTENT_TRANSCRIPT_DB = os.path.join(PERSISTENT_CACHE_DIR, "transcript_cache.db")
PERSISTENT_COMMENT_DB = os.path.join(PERSISTENT_CACHE_DIR, "comment_cache.db")

# Ensure cache directory exists
os.makedirs(PERSISTENT_CACHE_DIR, exist_ok=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE")

# Smartproxy (Optional - if credentials are set)
SMARTPROXY_USER = os.getenv("SMARTPROXY_USER")
SMARTPROXY_PASS = os.getenv("SMARTPROXY_PASS")
SMARTPROXY_HOST = "gate.decodo.com" # Example host
SMARTPROXY_PORTS = ["10000", "10001", "10002", "10003", "10004"] # Example ports

PROXY_ROTATION = []
if SMARTPROXY_USER and SMARTPROXY_PASS:
    PROXY_ROTATION = [{"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{port}"} for port in SMARTPROXY_PORTS]
else:
    PROXY_ROTATION = [{}] # No proxy

_proxy_cycle = itertools.cycle(PROXY_ROTATION)

# --- Logging Setup ---
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "app_server.log")

    logging.basicConfig(level=logging.WARNING) # Keep external libraries less verbose

    logger = logging.getLogger(APP_NAME)
    logger.setLevel(LOG_LEVEL)
    
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
session.request = functools.partial(session.request, timeout=15) # Default timeout for requests
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

def _fetch_from_alternative_api(hosts: deque, path: str, cooldown_map: dict[str, float], timeout: float = 5.0) -> dict | None:
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
    "writesubtitles": False, "writeautomaticsub": False,
    **({"cookiefile": YTDL_COOKIE_FILE} if YTDL_COOKIE_FILE else {}),
}

def yt_dlp_extract_info(video_id: str, extract_comments: bool = False) -> dict | None:
    opts = _YDL_OPTS_BASE.copy()
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
FALLBACK_LANGUAGES = ['en', 'es', 'fr', 'de', 'pt', 'ru', 'it', 'nl', 'hi', 'ja', 'ko', 'ar', 'zh-Hans']

def _fetch_transcript_api(video_id: str, languages: list[str]) -> str | None:
    try:
        logger.debug("Attempting youtube_transcript_api for %s, langs: %s", video_id, languages)
        current_proxy = rnd_proxy() if PROXY_ROTATION[0] else None
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, proxies=current_proxy)
        
        # Try specified languages first
        for lang_code in languages:
            try:
                transcript = transcript_list.find_transcript([lang_code])
                fetched_segments = transcript.fetch()
                text = " ".join(s["text"] for s in fetched_segments).strip()
                if text:
                    logger.info("Transcript found via API for %s (lang: %s)", video_id, lang_code)
                    return text
            except NoTranscriptFound:
                continue # Try next language in the list
        
        # If not found in specified, try generated for those languages
        for lang_code in languages:
            try:
                transcript = transcript_list.find_generated_transcript([lang_code])
                fetched_segments = transcript.fetch()
                text = " ".join(s["text"] for s in fetched_segments).strip()
                if text:
                    logger.info("Generated transcript found via API for %s (lang: %s)", video_id, lang_code)
                    return text
            except NoTranscriptFound:
                continue
        logger.warning("No transcript found with youtube_transcript_api for %s in langs: %s", video_id, languages)
        return None
    except TranscriptsDisabled:
        logger.warning("Transcripts disabled for video %s", video_id)
        return None
    except CouldNotRetrieveTranscript as e: # More specific error
        logger.warning("Could not retrieve transcript for %s via API: %s", video_id, str(e))
        return None
    except Exception as e:
        logger.error("Unexpected error in _fetch_transcript_api for %s: %s", video_id, str(e), exc_info=LOG_LEVEL=="DEBUG")
        return None

def _fetch_transcript_resilient(video_id: str) -> str:
    logger.info("Initiating resilient transcript fetch for %s", video_id)
    
    # Priority 1: English (official then generated)
    transcript = _fetch_transcript_api(video_id, ["en"])
    if transcript: return transcript

    # Priority 2: Common fallback languages (official then generated)
    transcript = _fetch_transcript_api(video_id, FALLBACK_LANGUAGES)
    if transcript: return transcript
    
    # Priority 3: yt-dlp subtitles (if available, less reliable for clean text)
    # This is a deeper fallback and might return XML/VTT, needs careful handling if used.
    # For now, we rely on youtube_transcript_api which handles parsing.
    # If yt-dlp is enhanced to directly provide plain text, it could be added here.
    # info = yt_dlp_extract_info(video_id)
    # if info and info.get("automatic_captions"): ...
    
    logger.error("All transcript sources failed for video %s", video_id)
    raise NoTranscriptFound(video_id, [], None, None) # Use a specific error

def _get_or_spawn_transcript(video_id: str, timeout: float = 30.0) -> str:
    with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
        if video_id in db:
            logger.debug("Transcript cache HIT (persistent) for %s", video_id)
            return db[video_id]
            
    if (cached := transcript_cache.get(video_id)):
        logger.debug("Transcript cache HIT (in-memory) for %s", video_id)
        return cached

    future = _pending_futures.get(video_id)
    if future is None or future.done(): # Ensure future is not already completed or cancelled
        logger.info("Spawning new transcript fetch task for %s", video_id)
        future = executor.submit(_fetch_transcript_resilient, video_id)
        _pending_futures[video_id] = future
    else:
        logger.info("Waiting for existing transcript fetch task for %s", video_id)

    try:
        result = future.result(timeout=timeout)
        transcript_cache[video_id] = result
        with shelve.open(PERSISTENT_TRANSCRIPT_DB) as db:
            db[video_id] = result
        logger.info("Transcript processing complete for %s", video_id)
        return result
    except TimeoutError:
        logger.warning("Transcript fetch timed out for %s after %s s", video_id, timeout)
        raise # Re-raise to be handled by endpoint
    except Exception as e:
        logger.error("Transcript fetch failed for %s: %s", video_id, str(e), exc_info=LOG_LEVEL=="DEBUG")
        raise # Re-raise
    finally:
        # Clean up future if it's completed or no longer the active one for this video_id
        if video_id in _pending_futures and (_pending_futures[video_id] == future and future.done()):
            _pending_futures.pop(video_id, None)

# --- Comment Fetching Logic ---
def _fetch_comments_downloader(video_id: str) -> list[str] | None:
    logger.debug("Attempting comments via youtube-comment-downloader for %s", video_id)
    try:
        # Using a new downloader instance per call to avoid state issues if any
        downloader = YoutubeCommentDownloader() 
        comments_data = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}", sort_by=0, language='en') # 0 for top comments
        
        comments_text: list[str] = []
        count = 0
        for comment_page in comments_data: # Generator yields pages of comments
            for c_item in comment_page['comments']:
                text = c_item.get("text")
                if text:
                    comments_text.append(text)
                    count += 1
                if count >= COMMENT_LIMIT:
                    break
            if count >= COMMENT_LIMIT:
                break
        
        if comments_text:
            logger.info("Fetched %d comments via youtube-comment-downloader for %s", len(comments_text), video_id)
            return comments_text
        logger.warning("No comments returned by youtube-comment-downloader for %s", video_id)
        return None
    except Exception as e:
        logger.error("youtube-comment-downloader failed for %s: %s", video_id, str(e), exc_info=LOG_LEVEL=="DEBUG")
        return None

def _fetch_comments_yt_dlp(video_id: str) -> list[str] | None:
    if not YTDL_COOKIE_FILE:
        logger.debug("yt-dlp comments skipped: No cookie file configured.")
        return None
    
    logger.debug("Attempting comments via yt-dlp for %s", video_id)
    info = yt_dlp_extract_info(video_id, extract_comments=True)
    if info and "comments" in info and info["comments"]:
        comments_text = [c["text"] for c in info["comments"] if c.get("text")]
        if comments_text:
            logger.info("Fetched %d comments via yt-dlp for %s", len(comments_text), video_id)
            return comments_text[:COMMENT_LIMIT] # Ensure limit
    logger.warning("No comments found via yt-dlp for %s", video_id)
    return None

def _fetch_comments_resilient(video_id: str) -> list[str]:
    logger.info("Initiating resilient comment fetch for %s", video_id)
    
    # Priority 1: youtube-comment-downloader
    comments = _fetch_comments_downloader(video_id)
    if comments: return comments

    # Priority 2: yt-dlp (if cookies provided, as it's more likely to succeed with auth)
    comments = _fetch_comments_yt_dlp(video_id)
    if comments: return comments

    # Priority 3: Piped API (as a public, less rate-limited option)
    # This might require adjustments if Piped API structure changes
    # piped_data = _fetch_from_alternative_api(PIPED_HOSTS, f"/comments/{video_id}", _PIPE_COOLDOWN)
    # if piped_data and "comments" in piped_data:
    #     comments_text = [c.get("commentText") for c in piped_data["comments"] if c.get("commentText")]
    #     if comments_text:
    #         logger.info("Fetched %d comments via Piped for %s", len(comments_text), video_id)
    #         return comments_text[:COMMENT_LIMIT]

    logger.warning("All comment sources failed for video %s. Returning empty list.", video_id)
    return []


def _get_or_spawn_comments(video_id: str, timeout: float = 45.0) -> list[str]:
    with shelve.open(PERSISTENT_COMMENT_DB) as db:
        if video_id in db:
            logger.debug("Comment cache HIT (persistent) for %s", video_id)
            return db[video_id]

    if (cached := comment_cache.get(video_id)):
        logger.debug("Comment cache HIT (in-memory) for %s", video_id)
        return cached
    
    cache_key = f"comments_{video_id}"
    future = _pending_futures.get(cache_key)
    if future is None or future.done():
        logger.info("Spawning new comment fetch task for %s", video_id)
        future = executor.submit(_fetch_comments_resilient, video_id)
        _pending_futures[cache_key] = future
    else:
        logger.info("Waiting for existing comment fetch task for %s", video_id)

    try:
        result = future.result(timeout=timeout)
        comment_cache[video_id] = result # Use video_id for TTLCache key consistency
        with shelve.open(PERSISTENT_COMMENT_DB) as db:
            db[video_id] = result
        logger.info("Comment processing complete for %s", video_id)
        return result
    except TimeoutError:
        logger.warning("Comment fetch timed out for %s after %s s", video_id, timeout)
        return [] # Return empty on timeout to not block analysis
    except Exception as e:
        logger.error("Comment fetch failed for %s: %s", video_id, str(e), exc_info=LOG_LEVEL=="DEBUG")
        return [] # Return empty on error
    finally:
        if cache_key in _pending_futures and (_pending_futures[cache_key] == future and future.done()):
            _pending_futures.pop(cache_key, None)

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


@app.route("/transcript", methods=["GET"])
@limiter.limit("120/hour;20/minute") # Limits for transcript endpoint
def get_transcript_endpoint():
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400

    try:
        transcript_text = _get_or_spawn_transcript(video_id)
        return jsonify({"video_id": video_id, "text": transcript_text}), 200
    except TimeoutError: # Raised by _get_or_spawn_transcript if its own timeout hits
        logger.warning("Transcript request for %s returned 202 (pending due to timeout)", video_id)
        return jsonify({"status": "pending", "message": "Transcript processing timed out, please try again shortly."}), 202
    except NoTranscriptFound:
        logger.warning("No transcript found for %s after all fallbacks.", video_id)
        return jsonify({"error": "Transcript not available for this video"}), 404
    except Exception as e:
        logger.error("Unhandled error in transcript endpoint for %s: %s", video_id, str(e), exc_info=True)
        return jsonify({"error": "Failed to retrieve transcript due to an internal server error"}), 500

@app.route("/comments", methods=["GET"])
@limiter.limit("120/hour;20/minute") # Limits for comments endpoint
def get_comments_endpoint():
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        return jsonify({"error": "videoId parameter is missing"}), 400

    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        return jsonify({"error": "Invalid videoId format or URL"}), 400
    
    try:
        comments_list = _get_or_spawn_comments(video_id)
        return jsonify({"video_id": video_id, "comments": comments_list}), 200
    except Exception as e: # Should ideally not happen if _get_or_spawn_comments handles its errors
        logger.error("Unhandled error in comments endpoint for %s: %s", video_id, str(e), exc_info=True)
        return jsonify({"error": "Failed to retrieve comments due to an internal server error"}), 500

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
        resp = session.post(openai_endpoint, headers=headers, json=payload, timeout=90) # Increased timeout for AI
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

# --- Health Checks ---
@app.route("/health", methods=["GET"])
@limiter.exempt
def health_check():
    return jsonify({"status": "ok", "version": "2.0.0"}), 200

@app.route("/health/deep", methods=["GET"])
@limiter.exempt
def deep_health_check():
    checks = {}
    # Env check
    checks['env'] = {'OPENAI_KEY_CONFIGURED': bool(OPENAI_API_KEY)}
    
    # Disk space
    try:
        total, used, free = shutil.disk_usage('/')
        checks['disk'] = {'free_gb': round(free / (1024**3), 2), 'ok': (free / total) > 0.05} # Check for >5% free
    except Exception as e:
        checks['disk'] = {'ok': False, 'error': str(e)}

    # Worker pool status
    checks['worker_pool'] = {
        'max_workers': MAX_WORKERS,
        'active_threads': executor._work_queue.qsize() + len(executor._threads), # Approx active
        'pending_futures': len(_pending_futures)
    }
    
    # Simple OpenAI API connectivity test (list models)
    openai_ok = False
    if OPENAI_API_KEY:
        try:
            r = session.get("https://api.openai.com/v1/models", headers={"Authorization": f"Bearer {OPENAI_API_KEY}"}, timeout=5)
            openai_ok = r.status_code == 200
        except Exception:
            openai_ok = False
    checks['external_services'] = {'openai_api_models_list': openai_ok}

    # Overall status
    all_ok = checks['env']['OPENAI_KEY_CONFIGURED'] and \
             checks['disk']['ok'] and \
             checks['external_services']['openai_api_models_list']
             
    status_code = 200 if all_ok else 503
    return jsonify({
        'status': 'ok' if all_ok else 'degraded',
        'uptime_seconds': round(time.time() - app_start_time, 2),
        'checks': checks
    }), status_code

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