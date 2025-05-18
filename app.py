# yt-dlp runs direct (no proxy) unless you see repeated rate limits, in which case set up proxy use for yt-dlp.
# YouTubeTranscriptApi always uses Smartproxy (if credentials provided) for maximum reliability.
# Piped fallback tries direct, then proxy if necessary.
# All transcript fetches are coordinated via a per-video_id lock to avoid race conditions.
# The list of fallback languages is configurable via the POPULAR_LANGS environment variable (comma-separated).

import os
# Gunicorn’s default worker timeout is 30 s; keep our worst‑case
# request below that by capping every external call at ≤ 5 s and
# limiting fallback attempts.  If you change these values, remember
# to adjust your `timeout` setting in Render / Heroku accordingly.
import random
import shelve
# Persistent cache files
PERSISTENT_TRANSCRIPT_DB = os.path.join(os.getcwd(), "transcript_cache_persistent.db")
PERSISTENT_COMMENT_DB    = os.path.join(os.getcwd(), "comment_cache_persistent.db")
PERSISTENT_ANALYSIS_DB   = os.path.join(os.getcwd(), "analysis_cache_persistent.db")
PERSISTENT_METADATA_DB = os.path.join(os.getcwd(), "metadata_cache_persistent.db")

# Ensure cache files are properly initialized and closed
def open_shelf(filename, flag='c'):
    try:
        return shelve.open(filename, flag=flag)
    except Exception as e:
        app.logger.error(f"Failed to open shelf {filename}: {e}. Trying with a new file.")
        # Attempt to delete corrupted file and retry, or use a temp name
        try:
            os.remove(filename + ".db") # Common extension for dbm
            os.remove(filename) # if no extension
        except OSError:
            pass
        return shelve.open(filename + "_new", flag=flag) # Or handle error more gracefully


transcript_shelf = open_shelf(PERSISTENT_TRANSCRIPT_DB)
comment_shelf    = open_shelf(PERSISTENT_COMMENT_DB)
analysis_shelf   = open_shelf(PERSISTENT_ANALYSIS_DB)
metadata_shelf = open_shelf(PERSISTENT_METADATA_DB)

import re
import time
import itertools
import json
import logging
from functools import lru_cache
from collections import deque 
from threading import Lock
_video_fetch_locks = {} # For transcript fetching
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future, wait, FIRST_COMPLETED
import concurrent.futures

from flask import Flask, request, jsonify, make_response
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound
)
from youtube_transcript_api._errors import CouldNotRetrieveTranscript
from cachetools import TTLCache
# from cachetools.keys import hashkey # Not explicitly used, can be removed if not needed elsewhere
from logging.handlers import RotatingFileHandler
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import shutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from yt_dlp import YoutubeDL  
import functools
from youtube_comment_downloader import YoutubeCommentDownloader

session = requests.Session()

# ── Global browser UA to dodge anti‑bot filters ─────────────
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
session.headers.update({"User-Agent": BROWSER_UA})

session.request = functools.partial(session.request, timeout=15) # Increased default timeout slightly
retry_cfg = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(['GET', 'POST', 'HEAD', 'OPTIONS']), # Explicitly list retryable methods
    raise_on_status=False
)
session.mount("https://", HTTPAdapter(max_retries=retry_cfg))
session.mount("http://", HTTPAdapter(max_retries=retry_cfg))

# ─────────────────────────────────────────────
# App startup timestamp
# ─────────────────────────────────────────────
app_start_time = time.time()

# --------------------------------------------------
# Smartproxy & API configuration  (env-driven)
# --------------------------------------------------
SMARTPROXY_USER  = os.getenv("SMARTPROXY_USER")
SMARTPROXY_PASS  = os.getenv("SMARTPROXY_PASS")
SMARTPROXY_HOST  = "gate.smartproxy.com"
SMARTPROXY_API_TOKEN = os.getenv("SMARTPROXY_API_TOKEN")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
YTDL_COOKIE_FILE = None # Force‑disable cookie usage

# ------------------------------------------------------------------
# Runtime tunables (env‑driven)
# ------------------------------------------------------------------
TRANSCRIPT_FETCH_TIMEOUT = int(os.getenv("TRANSCRIPT_FETCH_TIMEOUT", "25"))
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "120"))
METADATA_FETCH_TIMEOUT = int(os.getenv("METADATA_FETCH_TIMEOUT", "10")) # Total time for metadata attempts

PROXY_ROTATION = (
    [
        {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{10000+i}"}
        for i in range(20) # Max 20 different ports for Smartproxy
    ]
    if SMARTPROXY_USER and SMARTPROXY_PASS else
    [{}] # No proxy if no credentials
)

_proxy_cycle = itertools.cycle(PROXY_ROTATION)
def rnd_proxy() -> dict:
    return next(_proxy_cycle)

# Host lists (Consider making these dynamically updatable or regularly checked)
PIPED_HOSTS = deque([
    "https://pipedapi.kavin.rocks", "https://pipedapi.tokhmi.xyz",
    "https://pipedapi.moomoo.me", "https://pipedapi.adminforge.de",
    "https://piped-api.garudalinux.org", "https://pipedapi.pfcd.me",
    "https://pipedapi.frontendfriendly.xyz",
    # "https://piped.video", # Base domains might not be API hosts
    # "https://piped.video.proxycache.net", # Had resolution error
    # "https://piped.video.deno.dev", # Had SSL error
])
INVIDIOUS_HOSTS = deque([
    "https://yewtu.be", "https://inv.nadeko.net", "https://vid.puffyan.us",
    "https://invidious.no-logs.com", "https://invidious.baczek.me",
    "https://iv.ggtyler.dev", "https://invidious.protokoll.fi",
    # "https://ytdetail.8848.wtf", # check this one
])
_PIPE_COOLDOWN: dict[str, float] = {}
_IV_COOLDOWN: dict[str, float] = {}

HOST_COOLDOWN_SECONDS = 1800 # 30-minute cooldown
HOST_ERROR_THRESHOLD = 3 # Times a host can fail before longer cooldown
_host_error_counts: dict[str, int] = {}


# Top 10 global languages for transcript fallback
TOP_LANGS = ["es", "hi", "pt", "id", "ru", "ja", "ko", "de", "fr", "it"]

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Logging Setup ---
os.makedirs("logs", exist_ok=True)
file_handler = RotatingFileHandler(
    "logs/server.log", maxBytes=10_485_760, backupCount=5, encoding="utf-8" # 10MB
)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(module)s - %(funcName)s: %(message)s"
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.WARNING) # Silence verbose urllib3 logs


def _fetch_json(hosts: deque, path: str, cooldown_dict: dict[str, float],
                proxy_aware: bool = False, request_timeout: float = 5.0, hard_deadline: float = 7.0):
    host_success_times = getattr(_fetch_json, "_host_success_times", {})
    _fetch_json._host_success_times = host_success_times

    deadline = time.time() + hard_deadline
    
    # Shuffle hosts to prevent hammering the same one if sorted list is static
    live_hosts = list(hosts)
    random.shuffle(live_hosts)

    candidates = []
    for h in live_hosts:
        if cooldown_dict.get(h, 0) > time.time():
            continue
        if _host_error_counts.get(h, 0) >= HOST_ERROR_THRESHOLD: # Skip if too many errors
            if cooldown_dict.get(h, 0) <= time.time(): # If not in cooldown, put in long cooldown
                 cooldown_dict[h] = time.time() + HOST_COOLDOWN_SECONDS * 2 # Extra penalty
            continue
        candidates.append(h)
    
    candidates = sorted(candidates, key=lambda h_sort: -host_success_times.get(h_sort, 0))[:4] # Race top 4

    if not candidates:
        app.logger.warning(f"No healthy hosts available for {path} from {hosts}")
        return None

    with ThreadPoolExecutor(max_workers=len(candidates)) as pool:
        future_to_host = {
            pool.submit(
                session.get, f"{h}{path}",
                proxies=(rnd_proxy() if proxy_aware and PROXY_ROTATION[0] else {}),
                timeout=request_timeout,
                headers={"User-Agent": BROWSER_UA} # Ensure UA is set per request
            ): h for h in candidates
        }

        done_futures, _ = wait(future_to_host.keys(), timeout=hard_deadline, return_when=FIRST_COMPLETED)

        for fut in done_futures:
            host = future_to_host[fut]
            try:
                r = fut.result()
                r.raise_for_status()
                if "application/json" not in r.headers.get("Content-Type", "").lower():
                    app.logger.warning(f"Host {host} returned non-JSON content-type: {r.headers.get('Content-Type')}")
                    raise ValueError("non-JSON body")
                
                json_data = r.json()
                host_success_times[host] = time.time()
                _host_error_counts[host] = 0 # Reset error count on success
                hosts.rotate(-1) # Move successful host to the end to cycle
                return json_data
            except requests.exceptions.HTTPError as he:
                app.logger.warning(f"Host {host} HTTP error for {path}: {he.response.status_code} {he.response.reason}")
                if he.response.status_code in [403, 404, 429]: # Likely permanent or needs proxy
                     _host_error_counts[host] = _host_error_counts.get(host, 0) + 1
                     cooldown_dict[host] = time.time() + (HOST_COOLDOWN_SECONDS if _host_error_counts[host] >= HOST_ERROR_THRESHOLD else 600)
                else: # Server errors
                     cooldown_dict[host] = time.time() + 300
            except Exception as e:
                app.logger.warning(f"Host {host} failed for {path}: {type(e).__name__} {e}")
                _host_error_counts[host] = _host_error_counts.get(host, 0) + 1
                cooldown_dict[host] = time.time() + (HOST_COOLDOWN_SECONDS if _host_error_counts[host] >= HOST_ERROR_THRESHOLD else 600)
    return None


_YDL_OPTS_BASE = {
    "quiet": True,
    "skip_download": True,
    "user_agent": BROWSER_UA,
    "socket_timeout": 10, # Increased from 8s
    "retries": 2, # Increased from 1
    "no_check_certificate": True, # Can help with some SSL issues on invidious/piped instances
    "forcejson": True, # For metadata/info
    # "verbose": True, # For debugging yt-dlp
}

# In-memory caches
transcript_cache = TTLCache(maxsize=1000, ttl=86_400) # 24h
comment_cache    = TTLCache(maxsize=500, ttl=1800)   # 30 min
analysis_cache   = TTLCache(maxsize=300, ttl=3600)   # 1h
ytdl_cache       = TTLCache(maxsize=1000, ttl=1800)  # 30 min for general info
metadata_cache   = TTLCache(maxsize=1000, ttl=3600)  # 1h
NEGATIVE_TRANSCRIPT_CACHE = TTLCache(maxsize=2000, ttl=43_200) # 12h

executor = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 5)) # Standard Python formula
_pending_transcripts: dict[str, Future] = {}


def yt_dlp_info(video_id: str, use_proxy: bool = True):
    cache_key = f"{video_id}_{use_proxy}"
    if cache_key in ytdl_cache:
        return ytdl_cache[cache_key]

    opts = _YDL_OPTS_BASE.copy()
    opts["extract_flat"] = "discard_in_playlist" # Get info for single video
    
    current_proxy = rnd_proxy().get("https", None)
    if use_proxy and PROXY_ROTATION[0] and current_proxy:
        opts["proxy"] = current_proxy
    elif "proxy" in opts:
        del opts["proxy"] # Ensure no proxy if use_proxy is False

    app.logger.info(f"[yt_dlp_info] Fetching info for {video_id} with proxy: {opts.get('proxy', 'None')}")
    try:
        with YoutubeDL(opts) as ydl:
            # yt-dlp often prefers full URLs
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        
        if info and info.get("title"): # Basic check for valid response
            ytdl_cache[cache_key] = info
            return info
        else:
            app.logger.warning(f"[yt_dlp_info] Failed to get valid info for {video_id} (title missing). Response: {str(info)[:200]}")
            return None
    except Exception as e:
        app.logger.error(f"[yt_dlp_info] Error extracting info for {video_id} with proxy {opts.get('proxy', 'None')}: {type(e).__name__} {e}")
        # If proxy failed, try without proxy once as a fallback
        if use_proxy and opts.get("proxy"):
            app.logger.info(f"[yt_dlp_info] Retrying info for {video_id} without proxy.")
            return yt_dlp_info(video_id, use_proxy=False)
        return None


def _safe_put(shelf, key, value):
    try:
        shelf[key] = value
        shelf.sync()
    except Exception as e:
        app.logger.error(f"Shelve write error for key {key} in {shelf._filename if hasattr(shelf, '_filename') else 'unknown_shelf'}: {e}")

@app.before_request
def _access_log():
    app.logger.info(f"[IN] {request.method} {request.full_path} ← {request.headers.get('X-Forwarded-For', request.remote_addr)}")

def _rate_limit_key():
    ua = request.headers.get("User-Agent", "")
    # Use X-Forwarded-For if available (common in proxy setups like Render)
    ip_address = request.headers.get("X-Forwarded-For", get_remote_address())
    return f"{ip_address}-{hash(ua) % 1000}"

redis_url = os.getenv("REDIS_URL")
limiter_storage_uri = redis_url if redis_url else "memory://" # Default to memory if REDIS_URL not set

limiter = Limiter(
    app=app,
    key_func=_rate_limit_key,
    default_limits=["300 per hour", "60 per minute"], # Adjusted limits
    headers_enabled=True,
    storage_uri=limiter_storage_uri
)
if limiter_storage_uri == "memory://":
    app.logger.warning("Flask-Limiter is using in-memory storage. Not recommended for multi-process/worker production setups.")


VIDEO_ID_REGEX = re.compile(r'^[\w-]{11}$')

def validate_video_id(video_id: str) -> bool:
    return bool(VIDEO_ID_REGEX.fullmatch(video_id))

valid_id = validate_video_id # Alias

@lru_cache(maxsize=2048) # Increased cache size
def extract_video_id(input_str: str) -> str | None:
    if not input_str or not isinstance(input_str, str):
        return None
    patterns = [
        r'(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([\w-]{11})', # More robust
        r'^([\w-]{11})$'
    ]
    for p in patterns:
        m = re.search(p, input_str)
        if m and validate_video_id(m.group(1)):
            return m.group(1)
    return None


def _get_transcript_from_api(vid: str, langs, preserve_formatting=False):
    # Always use a rotating proxy for YouTubeTranscriptApi if configured
    current_proxy = rnd_proxy() if PROXY_ROTATION[0] else {}
    return YouTubeTranscriptApi.get_transcript(vid, languages=langs, proxies=current_proxy, preserve_formatting=preserve_formatting)


def _fetch_resilient(video_id: str) -> str:
    app.logger.info(f"[Transcript] Starting resilient fetch for {video_id}")
    if video_id in NEGATIVE_TRANSCRIPT_CACHE:
        app.logger.info(f"[Transcript] Negative cache hit for {video_id}")
        return "" # Return empty string for known unavailable

    # --- Define primary strategies ---
    def fetch_with_piped():
        # Piped seems to provide VTT or other formats that need parsing, 
        # or direct text links. Focus on direct text links if available.
        # This implementation expects Piped /api/v1/captions/{video_id} to return JSON
        # with URLs to transcript files (e.g., .srv3 or .xml that needs conversion)
        # For simplicity, we'll assume it can point to plain text if available, or we parse common formats
        # The old version's logic of iterating through `js` (caption tracks) was better.
        now = time.time()
        piped_hosts_list = list(PIPED_HOSTS) # Use a copy
        random.shuffle(piped_hosts_list)

        for host_idx, host in enumerate(piped_hosts_list):
            if _PIPE_COOLDOWN.get(host, 0) > now: continue
            if host_idx >= 3: break # Try max 3 Piped hosts per attempt to avoid long delays

            try:
                # Try direct first, then with proxy
                for use_proxy_attempt in (False, True) if PROXY_ROTATION[0] else (False,):
                    proxy_to_use = rnd_proxy() if use_proxy_attempt and PROXY_ROTATION[0] else {}
                    api_url = f"{host}/api/v1/captions/{video_id}"
                    app.logger.info(f"[Transcript] Trying Piped captions: {api_url} (proxy: {bool(proxy_to_use)})")
                    
                    r = session.get(api_url, proxies=proxy_to_use, timeout=7) # 7s timeout for Piped API call
                    r.raise_for_status()
                    caption_tracks_info = r.json()

                    # Prioritize English, then first available
                    sorted_tracks = sorted(caption_tracks_info, key=lambda t: t.get("languageCode","") != "en")

                    for track_info in sorted_tracks:
                        caption_url = track_info.get("url")
                        if caption_url:
                            app.logger.info(f"[Transcript] Fetching Piped caption track: {caption_url}")
                            r_caption = session.get(caption_url, timeout=7) # 7s for caption file
                            r_caption.raise_for_status()
                            # Basic VTT/SRT stripper (very naive)
                            text_content = r_caption.text
                            if "WEBVTT" in text_content or re.match(r"^\d+\s*\n", text_content): # Naive VTT/SRT check
                                lines = text_content.splitlines()
                                text_segments = [line for line in lines if line and not re.match(r"^\d+$", line) and "-->" not in line and "WEBVTT" not in line and "<c>" not in line and "</c>" not in line]
                                plain_text = " ".join(text_segments).strip()
                                # Further cleaning for XML/other formats might be needed
                                plain_text = re.sub(r'<[^>]+>', '', plain_text) # Strip all XML/HTML tags
                                plain_text = re.sub(r'\s+', ' ', plain_text).strip() # Normalize whitespace
                            else: # Assume plain text or needs more complex parsing
                                plain_text = re.sub(r'<[^>]+>', '', text_content) # Basic strip
                                plain_text = re.sub(r'\s+', ' ', plain_text).strip()

                            if plain_text and len(plain_text) > 10:
                                app.logger.info(f"[Transcript] Success: Piped captions ({track_info.get('languageCode', 'unknown')}) from {host} for {video_id}")
                                return plain_text
                    app.logger.warning(f"[Transcript] No usable caption content from Piped host {host} for {video_id}")
            except Exception as e:
                app.logger.warning(f"[Transcript] Piped failed at {host} for {video_id}: {type(e).__name__} {e}")
                _PIPE_COOLDOWN[host] = time.time() + 300 # 5 min cooldown
                continue
        return None

    def fetch_with_ytapi_en():
        try:
            segs = _get_transcript_from_api(video_id, ["en"])
            text = " ".join(s["text"] for s in segs).strip()
            if text and len(text) > 10:
                app.logger.info(f"[Transcript] Success: YouTubeTranscriptApi (en) for {video_id}")
                return text
            app.logger.warning(f"[Transcript] YouTubeTranscriptApi (en) returned empty/short for {video_id}")
        except (TranscriptsDisabled, NoTranscriptFound):
            app.logger.info(f"[Transcript] YouTubeTranscriptApi (en) not found or disabled for {video_id}")
        except Exception as e:
            app.logger.warning(f"[Transcript] YouTubeTranscriptApi (en) failed for {video_id}: {type(e).__name__} {e}")
        return None

    def fetch_with_ytdlp():
        try:
            # Try with proxy first, then without if it fails (handled by yt_dlp_info)
            info = yt_dlp_info(video_id, use_proxy=True) 
            if not info:
                return None

            text_results = []
            # Automatic captions
            auto_caps = info.get("automatic_captions") or {}
            if auto_caps:
                # Prefer English in automatic captions if available
                lang_codes_order = ['en'] + [lc for lc in auto_caps.keys() if lc != 'en']
                for lang_code in lang_codes_order:
                    tracks = auto_caps.get(lang_code, [])
                    for track in tracks:
                        if track.get("url") and track.get("ext") in ["srv3", "vtt", "ttml", "xml"]: # Common formats
                            try:
                                r_cap = session.get(track["url"], timeout=7)
                                r_cap.raise_for_status()
                                # Naive stripper (same as Piped, could be refactored)
                                content = r_cap.text
                                if "WEBVTT" in content or re.match(r"^\d+\s*\n", content):
                                    lines = content.splitlines()
                                    segments = [line for line in lines if line and not re.match(r"^\d+$",line) and "-->" not in line and "WEBVTT" not in line and "<c>" not in line and "</c>" not in line]
                                    plain = " ".join(segments).strip()
                                else: # XML based (ttml, srv3 often are)
                                    plain = " ".join(re.findall(r'>([^<]+)</', content)).strip() # Extract text between tags
                                plain = re.sub(r'\s+', ' ', plain).strip()
                                if plain and len(plain) > 10:
                                    app.logger.info(f"[Transcript] Success: yt-dlp automatic_captions ({lang_code}) for {video_id}")
                                    return plain
                            except Exception as e_track:
                                app.logger.warning(f"[Transcript] yt-dlp auto caption track failed for {video_id} ({lang_code}): {e_track}")
            
            # Manual captions (subtitles)
            manual_caps = info.get("subtitles") or info.get("captions") or {} # yt-dlp uses 'subtitles' more now
            if manual_caps:
                lang_codes_order = ['en'] + [lc for lc in manual_caps.keys() if lc != 'en']
                for lang_code in lang_codes_order:
                    tracks = manual_caps.get(lang_code, [])
                    for track in tracks:
                        if track.get("url") and track.get("ext") in ["srv3", "vtt", "ttml", "xml"]:
                            try:
                                r_cap = session.get(track["url"], timeout=7)
                                r_cap.raise_for_status()
                                content = r_cap.text
                                if "WEBVTT" in content or re.match(r"^\d+\s*\n", content):
                                    lines = content.splitlines()
                                    segments = [line for line in lines if line and not re.match(r"^\d+$",line) and "-->" not in line and "WEBVTT" not in line and "<c>" not in line and "</c>" not in line]
                                    plain = " ".join(segments).strip()
                                else:
                                    plain = " ".join(re.findall(r'>([^<]+)</', content)).strip()
                                plain = re.sub(r'\s+', ' ', plain).strip()
                                if plain and len(plain) > 10:
                                    app.logger.info(f"[Transcript] Success: yt-dlp manual_captions ({lang_code}) for {video_id}")
                                    return plain
                            except Exception as e_track:
                                app.logger.warning(f"[Transcript] yt-dlp manual caption track failed for {video_id} ({lang_code}): {e_track}")
            app.logger.warning(f"[Transcript] yt-dlp found no usable captions for {video_id}")
        except Exception as e:
            app.logger.error(f"[Transcript] yt-dlp strategy failed for {video_id}: {type(e).__name__} {e}")
        return None

    # --- Aggressively run primary strategies in parallel ---
    # Order of preference: ytapi_en (often cleanest), ytdlp (good fallback), piped (can be messy)
    primary_futures = {
        executor.submit(fetch_with_ytapi_en): "ytapi_en",
        executor.submit(fetch_with_ytdlp): "ytdlp",
        executor.submit(fetch_with_piped): "piped",
    }
    
    # Wait for up to 10 seconds total for primaries
    # This timeout is for the combined execution of these three.
    # Individual timeouts are within the functions (e.g., 7s for Piped/yt-dlp tracks).
    overall_primary_timeout = 12.0 
    start_primary_time = time.time()

    # Process in order of preference
    preferred_order = [fetch_with_ytapi_en, fetch_with_ytdlp, fetch_with_piped]
    future_to_name_map = {fut: name for fut, name in primary_futures.items()}

    # Check futures as they complete, or up to the timeout
    # This logic processes them as they complete, not strictly by FIRST_COMPLETED then others
    # It iterates based on completion.
    
    # Get results as they complete
    for future in concurrent.futures.as_completed(primary_futures.keys(), timeout=overall_primary_timeout):
        source_name = future_to_name_map[future]
        try:
            res = future.result(timeout=0.1) # Small timeout as it's already completed
            if res and len(res) > 10:
                app.logger.info(f"[Transcript] Primary source '{source_name}' succeeded for {video_id}.")
                # Cancel remaining primary futures
                for pf_key, pf_val in primary_futures.items():
                    if pf_key != future and not pf_key.done():
                        pf_key.cancel()
                return res
            elif res is not None: # Empty or too short
                 app.logger.info(f"[Transcript] Primary source '{source_name}' for {video_id} returned empty or too short.")
        except TimeoutError: # Should not happen with as_completed if overall_primary_timeout is respected
            app.logger.warning(f"[Transcript] Timeout processing result for '{source_name}' for {video_id}")
        except Exception as e:
            app.logger.warning(f"[Transcript] Primary source '{source_name}' for {video_id} failed: {type(e).__name__} {e}")
    
    app.logger.warning(f"[Transcript] All primary strategies failed or timed out for {video_id}. Trying fallbacks.")

    # Fallback 1: YouTubeTranscriptApi with top global languages (parallel attempts for speed)
    def fetch_with_ytapi_lang(lang_code: str):
        try:
            segs = _get_transcript_from_api(video_id, [lang_code])
            txt = " ".join(s["text"] for s in segs).strip()
            if txt and len(txt) > 10:
                app.logger.info(f"[Transcript] Success: YouTubeTranscriptApi ({lang_code}) for {video_id}")
                return txt
        except (TranscriptsDisabled, NoTranscriptFound):
            app.logger.debug(f"[Transcript] YTTA lang '{lang_code}' not found for {video_id}")
        except Exception as e: # Catch broader exceptions
            app.logger.debug(f"[Transcript] YTTA lang '{lang_code}' for {video_id} failed: {type(e).__name__} {e}")
        return None

    # Max 5 parallel language fetches to avoid excessive requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as lang_pool:
        lang_futures = {lang_pool.submit(fetch_with_ytapi_lang, lang): lang for lang in TOP_LANGS}
        # Wait up to 7 seconds for any language transcript
        done_lang_futures, _ = concurrent.futures.wait(lang_futures.keys(), timeout=7.0, return_when=concurrent.futures.FIRST_COMPLETED)
        for fut in done_lang_futures:
            res = fut.result()
            if res and len(res) > 10:
                # Cancel remaining lang futures
                for lf_key, lf_val in lang_futures.items():
                    if lf_key != fut and not lf_key.done():
                        lf_key.cancel()
                return res

    # Fallback 2: Lightweight HTML scrape (last resort, no proxy used here, could be added)
    try:
        app.logger.info(f"[Transcript] Trying HTML scrape for {video_id}")
        # Use a proxy for HTML scrape as well, as direct access can be blocked
        proxy_for_html = rnd_proxy() if PROXY_ROTATION[0] else {}
        html_resp = session.get(f"https://www.youtube.com/watch?v={video_id}", timeout=7, proxies=proxy_for_html, headers={"User-Agent": BROWSER_UA})
        html_resp.raise_for_status()
        html_content = html_resp.text
        
        # This regex is common but might need updating if YouTube changes page structure
        match = re.search(r'"captionTracks":\s*(\[.*?\])', html_content)
        if match:
            caption_tracks_json = json.loads(match.group(1))
            # Prefer English, then any other
            sorted_html_tracks = sorted(caption_tracks_json, key=lambda t: t.get("languageCode","") != "en")
            for track in sorted_html_tracks:
                if track.get("baseUrl"):
                    caption_data_url = track["baseUrl"]
                    # Append format=srv3 or similar if not present, yt-dlp does this.
                    # For simplicity, assume baseUrl is directly fetchable and parsable.
                    # srv3 is xml-like, ttml is xml, vtt is text-based.
                    # if "format=srv3" not in caption_data_url: caption_data_url += "&format=srv3"

                    r_caption_html = session.get(caption_data_url, timeout=7, proxies=proxy_for_html)
                    r_caption_html.raise_for_status()
                    text_content_html = r_caption_html.text
                    # Simple text extraction from XML/VTT like formats
                    plain_text_html = " ".join(re.findall(r'>([^<]+)</', text_content_html)).strip() # For XML
                    if not plain_text_html and "WEBVTT" in text_content_html: # For VTT
                        lines = text_content_html.splitlines()
                        text_segments_html = [line for line in lines if line and not re.match(r"^\d+$", line) and "-->" not in line and "WEBVTT" not in line]
                        plain_text_html = " ".join(text_segments_html).strip()
                    
                    plain_text_html = re.sub(r'\s+', ' ', plain_text_html).strip()
                    if plain_text_html and len(plain_text_html) > 10:
                        app.logger.info(f"[Transcript] Success: HTML Scrape ({track.get('languageCode','unknown')}) for {video_id}")
                        return plain_text_html
    except Exception as e:
        app.logger.warning(f"[Transcript] HTML scrape for {video_id} failed: {type(e).__name__} {e}")

    app.logger.error(f"[Transcript] All transcript sources conclusively failed for {video_id}")
    NEGATIVE_TRANSCRIPT_CACHE[video_id] = True # Cache as unavailable
    raise RuntimeError("Transcript unavailable from all sources")


def _get_or_spawn_transcript(video_id: str, timeout: float = TRANSCRIPT_FETCH_TIMEOUT) -> str:
    lock = _video_fetch_locks.setdefault(video_id, Lock())
    with lock:
        if video_id in transcript_shelf:
            app.logger.info(f"Transcript cache hit (shelf) for {video_id}")
            return transcript_shelf[video_id]
        if (cached := transcript_cache.get(video_id)):
            app.logger.info(f"Transcript cache hit (memory) for {video_id}")
            return cached
        
        fut = _pending_transcripts.get(video_id)
        if fut is None or fut.done(): # If no future or previous one finished (e.g. cancelled/failed)
            app.logger.info(f"Spawning new transcript fetch for {video_id}")
            fut = executor.submit(_fetch_resilient, video_id)
            _pending_transcripts[video_id] = fut
        else:
            app.logger.info(f"Waiting for existing transcript fetch for {video_id}")

    try:
        result = fut.result(timeout=timeout)
        if result: # Only cache non-empty results
            transcript_cache[video_id] = result
            _safe_put(transcript_shelf, video_id, result)
        return result
    except TimeoutError: # Timeout waiting for future.result()
        app.logger.warning(f"Timeout waiting for transcript result for {video_id}")
        raise # Re-raise to be caught by endpoint
    except Exception as e: # Exception from _fetch_resilient itself
        app.logger.error(f"Underlying transcript fetch for {video_id} raised: {e}")
        # Potentially remove from _pending_transcripts if it's an error from the function itself
        # and not a timeout on fut.result()
        if video_id in _pending_transcripts and _pending_transcripts[video_id] == fut:
            _pending_transcripts.pop(video_id, None)
        raise # Re-raise
    finally:
        # Clean up lock and pending future if this specific future is done
        # This check is important if a new future was spawned for the same id after a failure.
        if fut.done():
            if video_id in _pending_transcripts and _pending_transcripts[video_id] == fut:
                _pending_transcripts.pop(video_id, None)
            # Only remove lock if no other thread is waiting on this video_id
            # This is tricky; the lock is acquired at the start of this function.
            # If the lock is associated with this specific call, it's released when 'with lock' exits.
            # _video_fetch_locks.pop(video_id, None) # This might be too aggressive here.
            pass
        # The lock acquisition ensures only one worker spawns the task.
        # Once spawned, others might wait on the same future object if they get there before it's removed from _pending_transcripts.

@app.get("/transcript")
@limiter.limit("150/hour;30/minute") # Slightly higher limits
def transcript_endpoint():
    video_url_or_id = request.args.get("videoId", "")
    vid = extract_video_id(video_url_or_id)
    if not vid:
        return jsonify({"error": "invalid_video_id", "message": "Please provide a valid YouTube video ID or URL."}), 400

    try:
        text = _get_or_spawn_transcript(vid)
        if not text: # If _fetch_resilient returns empty (e.g. from negative cache)
            return jsonify({"status": "unavailable", "error": "Transcript not found or is empty."}), 404
        return jsonify({"video_id": vid, "text": text}), 200
    except TimeoutError: # From _get_or_spawn_transcript's fut.result(timeout)
        # This means the client's request timed out waiting for the result,
        # but the background task might still be running.
        return jsonify({"status": "pending", "message": "Transcript processing is taking longer than expected. Please try again shortly."}), 202
    except RuntimeError as e: # From _fetch_resilient if all sources fail
        app.logger.error(f"Transcript generation ultimately failed for {vid}: {e}")
        return jsonify({"status": "unavailable", "error": str(e)}), 404
    except Exception as e: # Other unexpected errors
        app.logger.error(f"Unexpected error in /transcript for {vid}: {type(e).__name__} {e}", exc_info=True)
        return jsonify({"status": "error", "error": "An internal server error occurred."}), 500

# ---------------- PROXY STATS (No changes from new version) ---------------------
@app.route("/proxy_stats", methods=["GET"])
def get_proxy_stats():
    if not SMARTPROXY_API_TOKEN or not SMARTPROXY_USER: # Guard this endpoint
        return jsonify({'error': 'Smartproxy not configured for stats'}), 403
        
    from datetime import datetime, timedelta
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)
    payload = {
        "proxyType": "residential_proxies", # or other types if used
        "startDate": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endDate": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "limit": 1
    }
    # Smartproxy API requires token in header
    headers = {
        "Authorization": f"Token {SMARTPROXY_API_TOKEN}",
        "Content-Type": "application/json"
    }
    app.logger.info("[OUT] Smartproxy stats request")
    try:
        r = session.post( # Smartproxy uses POST for this endpoint
            "https://dashboard.smartproxy.com/api/v1/users/current/statistics/traffic-usage", # Check official endpoint
            json=payload, headers=headers, timeout=10
        )
        app.logger.info(f"[OUT] Smartproxy stats response ← {r.status_code}")
        r.raise_for_status()
        return jsonify(r.json()), 200
    except Exception as e:
        app.logger.error(f"Proxy stats error: {type(e).__name__} {e}")
        return jsonify({'error': 'Could not retrieve proxy stats'}), 503

# ---------------- Comments ----------------
def _download_comments_native(video_id: str, limit: int = COMMENT_LIMIT) -> list[str] | None:
    try:
        downloader = YoutubeCommentDownloader()
        comments_data = downloader.get_comments(video_id, sort_by=0) # 0 for top comments
        
        comments_text: list[str] = []
        comment_iterator = itertools.islice(comments_data, limit) if limit else comments_data

        for c_data in comment_iterator:
            txt = c_data.get("text")
            if txt:
                comments_text.append(txt)
            if limit and len(comments_text) >= limit:
                break
        return comments_text or None
    except Exception as e:
        app.logger.warning(f"youtube-comment-downloader failed for {video_id}: {type(e).__name__} {e}")
        return None

def _fetch_comments_ytdlp(video_id: str, limit: int) -> list[str] | None:
    try:
        opts = _YDL_OPTS_BASE.copy()
        opts["getcomments"] = True
        opts["forcejson"] = False # Comments are part of the main JSON if requested
        opts["extract_flat"] = False # Need nested structure for comments
        
        current_proxy = rnd_proxy().get("https", None)
        if PROXY_ROTATION[0] and current_proxy: # Use proxy for comment fetching too
            opts["proxy"] = current_proxy
        
        app.logger.info(f"[Comments] Fetching comments via yt-dlp for {video_id} (proxy: {bool(opts.get('proxy'))})")
        with YoutubeDL(opts) as ydl:
            result = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            
        if result and result.get("comments"):
            comments_data = result.get("comments")
            extracted_comments = []
            for c_item in comments_data:
                text = c_item.get("text") # yt-dlp usually provides 'text'
                if text:
                    extracted_comments.append(str(text))
                if len(extracted_comments) >= limit:
                    break
            return extracted_comments or None
        app.logger.warning(f"[Comments] yt-dlp found no comments for {video_id}")
        return None
    except Exception as e:
        app.logger.warning(f"[Comments] _fetch_comments_ytdlp failed for {video_id}: {type(e).__name__} {e}")
        return None

@app.route("/video/comments") # Changed from /youtube/comments for consistency
@limiter.limit("100/hour;20/minute")
def comments_endpoint():
    video_url_or_id = request.args.get("videoId", "")
    vid = extract_video_id(video_url_or_id)
    if not vid:
        return jsonify({"error": "invalid_video_id"}), 400

    if vid in comment_shelf:
        app.logger.info(f"Comments cache hit (shelf) for {vid}")
        return jsonify({"comments": comment_shelf[vid]}), 200
    if (cached_comments := comment_cache.get(vid)):
        app.logger.info(f"Comments cache hit (memory) for {vid}")
        return jsonify({"comments": cached_comments}), 200

    fetched_comments: list[str] | None = None
    source_name = "N/A"

    # --- Parallel fetch: youtube-comment-downloader + Piped ---
    def _piped_comments_task():
        js = _fetch_json(
            PIPED_HOSTS, # Use main list, _fetch_json handles healthy selection
            f"/api/v1/comments/{vid}?continuation=0", # Using 'continuation' for Piped comments if API supports pagination
            _PIPE_COOLDOWN,
            proxy_aware=True, # Piped can be behind Cloudflare, proxy helps
            request_timeout=8.0, hard_deadline=10.0
        )
        if js and js.get("comments"):
            # Piped comment text can be in 'commentText' or 'comment'
            return [
                c.get("commentText") or c.get("comment")
                for c in js["comments"]
                if c.get("commentText") or c.get("comment")
            ][:COMMENT_LIMIT] or None
        return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        comment_futures = {
            pool.submit(_download_comments_native, vid, COMMENT_LIMIT): "native_downloader",
            pool.submit(_piped_comments_task): "piped"
        }
        # Wait up to 15 seconds for these primaries
        done_comment_futures, _ = wait(comment_futures.keys(), timeout=15.0, return_when=FIRST_COMPLETED)
        
        for fut in done_comment_futures: # Check completed ones first
            if fut.done() and not fut.cancelled():
                try:
                    res = fut.result(timeout=0.1) # Already complete, quick check
                    if res:
                        fetched_comments = res
                        source_name = comment_futures[fut]
                        # Cancel other primary if one succeeded quickly
                        for f_key, f_val in comment_futures.items():
                            if f_key != fut and not f_key.done(): f_key.cancel()
                        break 
                except Exception as e_fut:
                    app.logger.warning(f"[Comments] Primary task {comment_futures[fut]} failed: {e_fut}")
    
    if fetched_comments:
        app.logger.info(f"Comments fetched via {source_name} for {vid} (count: {len(fetched_comments)})")
    else:
        app.logger.info(f"[Comments] Primary sources failed for {vid}. Trying fallbacks.")
        # Fallback 1: yt-dlp
        fetched_comments = _fetch_comments_ytdlp(vid, COMMENT_LIMIT)
        if fetched_comments:
            source_name = "yt-dlp"
            app.logger.info(f"Comments fetched via {source_name} for {vid} (count: {len(fetched_comments)})")
        else:
            # Fallback 2: Invidious
            js_invidious = _fetch_json(
                INVIDIOUS_HOSTS,
                f"/api/v1/comments/{vid}", # Standard Invidious API path for comments
                _IV_COOLDOWN,
                proxy_aware=True, # Invidious also benefits from proxies
                request_timeout=8.0, hard_deadline=10.0
            )
            if js_invidious and js_invidious.get("comments"):
                # Invidious: c["content"] or c["contentHtml"] (needs stripping) or c["text"]
                comments_list = []
                for c_inv in js_invidious["comments"]:
                    text = c_inv.get("content") or c_inv.get("text")
                    if not text and c_inv.get("contentHtml"):
                        text = re.sub(r'<[^>]+>', '', c_inv["contentHtml"]) # Strip HTML
                    if text:
                        comments_list.append(text.strip())
                    if len(comments_list) >= COMMENT_LIMIT: break
                if comments_list:
                    fetched_comments = comments_list
                    source_name = "invidious"
                    app.logger.info(f"Comments fetched via {source_name} for {vid} (count: {len(fetched_comments)})")

    if fetched_comments:
        comment_cache[vid] = fetched_comments
        _safe_put(comment_shelf, vid, fetched_comments)
        return jsonify({"comments": fetched_comments}), 200
    else:
        app.logger.error(f"All comment sources failed for {vid}")
        # Return empty list for robustness, client can handle no comments
        _safe_put(comment_shelf, vid, []) # Cache empty result to avoid re-fetching constantly
        comment_cache[vid] = []
        return jsonify({"comments": []}), 200


# ---------------- Metadata ---------------
@app.route("/video/metadata") # Changed from /youtube/metadata
@limiter.limit("100/hour;20/minute")
def metadata_endpoint():
    video_url_or_id = request.args.get("videoId", "")
    vid = extract_video_id(video_url_or_id)
    if not vid:
        return jsonify({"error": "invalid_video_id"}), 400

    if vid in metadata_shelf:
        app.logger.info(f"Metadata cache hit (shelf) for {vid}")
        return jsonify({"items": [metadata_shelf[vid]]}), 200
    if (cached_meta := metadata_cache.get(vid)):
        app.logger.info(f"Metadata cache hit (memory) for {vid}")
        return jsonify({"items": [cached_meta]}), 200

    # --- Define parallel metadata fetch strategies ---
    def fetch_oembed_meta():
        try:
            oe_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={vid}&format=json"
            r = session.get(oe_url, timeout=5)
            r.raise_for_status()
            oe = r.json()
            return {
                "title": oe.get("title"), "channelTitle": oe.get("author_name"),
                "thumbnail": oe.get("thumbnail_url"), "source": "oembed"
            }
        except Exception as e:
            app.logger.debug(f"[Metadata] oEmbed fail for {vid}: {type(e).__name__} {e}")
            return None

    def fetch_ytdlp_meta():
        # Try with proxy first, then without if it fails
        info = yt_dlp_info(vid, use_proxy=True)
        if info:
            return {
                "title": info.get("title"), "channelTitle": info.get("uploader") or info.get("channel"),
                "thumbnail": info.get("thumbnail"), "duration": info.get("duration"),
                "viewCount": info.get("view_count"), "likeCount": info.get("like_count"),
                "uploadDate": info.get("upload_date"), # YYYYMMDD format
                "source": "ytdlp"
            }
        return None

    def fetch_piped_meta():
        js = _fetch_json(PIPED_HOSTS, f"/api/v1/streams/{vid}", _PIPE_COOLDOWN, proxy_aware=True, request_timeout=7, hard_deadline=8)
        if js:
            return {
                "title": js.get("title"), "channelTitle": js.get("uploader"),
                "thumbnail": js.get("thumbnailUrl"), "duration": js.get("duration"),
                "viewCount": js.get("views"), "likeCount": js.get("likes"),
                "uploadDate": str(js.get("uploadDate")).split("T")[0].replace("-","") if js.get("uploadDate") else None, # Convert ISO to YYYYMMDD
                "source": "piped"
            }
        return None

    def fetch_invidious_meta():
        # Invidious /api/v1/videos/{vid}
        js = _fetch_json(INVIDIOUS_HOSTS, f"/api/v1/videos/{vid}", _IV_COOLDOWN, proxy_aware=True, request_timeout=7, hard_deadline=8)
        if js:
            return {
                "title": js.get("title"), "channelTitle": js.get("author"),
                "thumbnail": (js.get("videoThumbnails") or [{}])[0].get("url") if js.get("videoThumbnails") else None, # Prioritize quality thumbnails
                "duration": js.get("lengthSeconds"), "viewCount": js.get("viewCount"),
                "likeCount": js.get("likeCount"), 
                "uploadDate": str(js.get("published")) if js.get("published") else None, # Unix timestamp, needs conversion, or use publishedText
                "publishedText": js.get("publishedText"), # "X years ago"
                "source": "invidious"
            }
        return None

    # --- Parallel fetching ---
    # Longer timeout for metadata overall, as it's critical
    # METADATA_FETCH_TIMEOUT defined as 10s
    all_sources_results = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        meta_futures = [
            pool.submit(fetch_ytdlp_meta), # Prioritize ytdlp
            pool.submit(fetch_oembed_meta),
            pool.submit(fetch_piped_meta),
            pool.submit(fetch_invidious_meta),
        ]
        for fut in concurrent.futures.as_completed(meta_futures, timeout=METADATA_FETCH_TIMEOUT):
            try:
                res = fut.result(timeout=0.1) # Small timeout as already completed
                if res:
                    all_sources_results.append(res)
            except Exception as e_fut:
                app.logger.warning(f"[Metadata] A metadata source failed: {e_fut}")
    
    # --- Intelligent Merging ---
    final_meta = {
        "title": None, "channelTitle": None,
        "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg", # Default
        "thumbnailUrl": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "duration": None, "viewCount": None, "likeCount": None, "videoId": vid,
        "uploadDate": None, "publishedText": None, "sourcePriority": []
    }

    # Define a preference order for sources
    source_preference = ["ytdlp", "oembed", "piped", "invidious"]

    def is_valid_title(title_str):
        if not title_str or not isinstance(title_str, str): return False
        title_lower = title_str.strip().lower()
        return title_lower and title_lower not in ["untitled video", "youtube video", "", "[private video]", "[deleted video]"]

    for source_name in source_preference:
        for res in all_sources_results:
            if res and res.get("source") == source_name:
                final_meta["sourcePriority"].append(source_name)
                if not final_meta["title"] and is_valid_title(res.get("title")):
                    final_meta["title"] = res["title"].strip()
                if not final_meta["channelTitle"] and res.get("channelTitle"):
                    final_meta["channelTitle"] = res["channelTitle"].strip()
                if res.get("thumbnail") and (final_meta["thumbnail"].startswith("https://i.ytimg.com/") or source_name == "ytdlp"): # Prefer yt-dlp/oembed thumbs
                    final_meta["thumbnail"] = final_meta["thumbnailUrl"] = res["thumbnail"]
                if final_meta["duration"] is None and res.get("duration") is not None:
                     try: final_meta["duration"] = int(res["duration"])
                     except: pass
                if final_meta["viewCount"] is None and res.get("viewCount") is not None:
                    try: final_meta["viewCount"] = int(res["viewCount"])
                    except: pass
                if final_meta["likeCount"] is None and res.get("likeCount") is not None:
                    try: final_meta["likeCount"] = int(res["likeCount"])
                    except: pass
                if not final_meta["uploadDate"] and res.get("uploadDate"): # Expects YYYYMMDD
                    final_meta["uploadDate"] = str(res["uploadDate"])
                if not final_meta["publishedText"] and res.get("publishedText"):
                    final_meta["publishedText"] = res.get("publishedText")

    # If title is still generic after preferred sources, try any non-empty title
    if not is_valid_title(final_meta["title"]):
        for res in all_sources_results: # Check all results again
            if res and res.get("title") and res.get("title").strip():
                final_meta["title"] = res["title"].strip() # Take first available non-empty
                break 
    
    # Final check for essential data
    if not is_valid_title(final_meta["title"]) or final_meta["viewCount"] is None:
        app.logger.error(f"[Metadata] Failed to retrieve essential metadata for {vid}. Title: '{final_meta['title']}', Views: {final_meta['viewCount']}. Sources attempted: {final_meta['sourcePriority']}")
        # Try one last direct yt-dlp call without proxy if everything else failed badly
        if not any(s == "ytdlp" for s in final_meta["sourcePriority"]): # if ytdlp wasn't even tried or failed early
            app.logger.info(f"[Metadata] Attempting final direct yt-dlp for {vid}")
            ytdlp_direct_res = fetch_ytdlp_meta() # This will try with proxy then without
            if ytdlp_direct_res and is_valid_title(ytdlp_direct_res.get("title")) and ytdlp_direct_res.get("viewCount") is not None:
                app.logger.info(f"[Metadata] Final direct yt-dlp successful for {vid}")
                final_meta = {**final_meta, **ytdlp_direct_res} # Merge, prioritizing new fields
            else:
                 return jsonify({"error": "Could not retrieve essential video metadata after all attempts."}), 503
        elif not is_valid_title(final_meta["title"]) or final_meta["viewCount"] is None : # if ytdlp was tried but still bad data
            return jsonify({"error": "Could not retrieve essential video metadata. Data quality poor."}), 503


    app.logger.info(f"[Metadata] Successfully fetched metadata for {vid} (Title: '{final_meta['title']}', Views: {final_meta['viewCount']}) using sources: {final_meta['sourcePriority']}")
    del final_meta["sourcePriority"] # Clean up internal field
    metadata_cache[vid] = final_meta
    _safe_put(metadata_shelf, vid, final_meta)
    return jsonify({"items": [final_meta]}), 200


# ---------------- OpenAI Proxy (with Circuit Breaker from new version) -----------
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, reset_timeout: int = 60): # seconds
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.is_open = False
        self._lock = Lock()
        app.logger.info(f"CircuitBreaker '{name}' initialized: threshold={failure_threshold}, timeout={reset_timeout}s")

    def allow_request(self) -> bool:
        with self._lock:
            if self.is_open:
                if (time.time() - self.last_failure_time) > self.reset_timeout:
                    self.is_open = False
                    self.failure_count = 0 # Reset on recovery
                    app.logger.info(f"Circuit '{self.name}' is now half-open (resetting).")
                    return True # Allow one request in half-open state
                app.logger.warning(f"Circuit '{self.name}' is OPEN. Request denied.")
                return False
            return True

    def record_success(self):
        with self._lock:
            if self.is_open : # If it was half-open and succeeded
                 app.logger.info(f"Circuit '{self.name}' is now CLOSED (recovered).")
            self.failure_count = 0
            self.is_open = False


    def record_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                if not self.is_open: # Log only when it trips open
                    app.logger.error(f"Circuit '{self.name}' OPENED after {self.failure_count} failures.")
                self.is_open = True

openai_circuit = CircuitBreaker("openai_proxy", failure_threshold=5, reset_timeout=120) # More tolerant

@app.route("/openai/responses", methods=["POST"])
@limiter.limit("60 per minute") # Assuming this is a paid API, protect it
def create_openai_response():
    if not OPENAI_API_KEY:
        app.logger.error("[OpenAI Proxy] OPENAI_API_KEY is not configured.")
        return jsonify({'error': 'OpenAI API key not configured'}), 500

    if not openai_circuit.allow_request():
        return jsonify({'error': 'OpenAI service temporarily unavailable (circuit open). Please try again later.'}), 503

    try:
        payload = request.get_json(force=True) # force=True can be risky if content-type is wrong
        if not payload:
            return jsonify({'error': 'Invalid or empty JSON payload'}), 400
    except Exception as json_err:
        app.logger.error(f"[OpenAI Proxy] Failed to parse request JSON: {json_err}")
        return jsonify({'error': 'Bad request JSON'}), 400

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Log essential info, be careful with logging full payload if it's large/sensitive
    model_requested = payload.get('model', 'N/A')
    app.logger.info(f"[OpenAI Proxy] Request for model: {model_requested}. Input length (if any): {len(str(payload.get('input', '')))}. Context ID (if any): {payload.get('previous_response_id', 'N/A')}")


    # OpenAI endpoint seems to be /v1/chat/completions or /v1/completions, not /v1/responses
    # Based on your old code, it was "/v1/responses". If this is a custom endpoint or older API, keep it.
    # If it's standard OpenAI, it should be e.g. "https://api.openai.com/v1/chat/completions"
    openai_api_url = "https://api.openai.com/v1/responses" # As per your original code
    # If your Swift client expects your backend to proxy to a specific OpenAI model type,
    # you might need to adjust the `openai_api_url` and payload structure.
    # For example, for chat models:
    # openai_api_url = "https://api.openai.com/v1/chat/completions"
    # And payload should be like: {"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": "Hello!"}]}

    max_retries = 2 # Total 3 attempts (initial + 2 retries)
    base_backoff = 1.0 # seconds

    for attempt in range(max_retries + 1):
        try:
            # Increased timeout, especially for potentially long OpenAI responses
            # Timeout for connect and read separately
            resp = session.post(openai_api_url, headers=headers, json=payload, timeout=(10, 60)) # (connect_timeout, read_timeout)
            
            app.logger.info(f"[OpenAI Proxy] Attempt {attempt+1}: OpenAI API ({openai_api_url}) status: {resp.status_code}")

            if resp.status_code == 429: # Rate limit
                retry_after_str = resp.headers.get("Retry-After", str(base_backoff * (2**attempt)))
                try:
                    retry_after_sec = int(retry_after_str)
                except ValueError: # If it's a date string
                    # This needs parsing, for simplicity using backoff
                    retry_after_sec = int(base_backoff * (2**attempt) * 2) # Default if parsing date is complex
                
                sleep_time = min(retry_after_sec, 30) # Cap sleep time
                app.logger.warning(f"[OpenAI Proxy] Rate limited (429). Retrying after {sleep_time}s.")
                if attempt < max_retries:
                    time.sleep(sleep_time)
                    continue # Retry
                else: # Max retries reached for 429
                    openai_circuit.record_failure() # Count this as a failure for circuit breaker
                    return jsonify({'error': 'OpenAI rate limit exceeded after multiple retries.'}), 429
            
            resp.raise_for_status() # Raises HTTPError for 4xx/5xx responses not handled above
            
            openai_circuit.record_success()
            # Log structure of successful response for debugging client DTOs
            response_data_preview = str(resp.content[:500]) # Log beginning of raw content
            app.logger.info(f"[OpenAI Proxy] Success. Response preview: {response_data_preview}")
            return resp.content, resp.status_code, resp.headers.items() # Return raw content and headers

        except requests.exceptions.HTTPError as he:
            app.logger.error(f"[OpenAI Proxy] Attempt {attempt+1} HTTPError: {he.response.status_code} - {he.response.text[:500]}")
            if he.response.status_code >= 500: # Server-side errors on OpenAI
                if attempt < max_retries:
                    time.sleep(base_backoff * (2**attempt))
                    continue # Retry for server errors
            # For 4xx client errors (other than 429) or 5xx after retries, break and return error
            openai_circuit.record_failure()
            error_details = he.response.text
            try:
                error_json = json.loads(error_details)
                if "error" in error_json and "message" in error_json["error"]:
                    error_details = error_json["error"]["message"]
            except: pass
            return jsonify({'error': 'OpenAI API error', 'details': error_details}), he.response.status_code
        except requests.exceptions.Timeout:
            app.logger.error(f"[OpenAI Proxy] Attempt {attempt+1} Timeout connecting to OpenAI.")
            if attempt < max_retries:
                time.sleep(base_backoff * (2**attempt))
                continue # Retry for timeout
            openai_circuit.record_failure()
            return jsonify({'error': 'Timeout communicating with OpenAI service.'}), 504
        except requests.exceptions.RequestException as req_err: # Other network errors
            app.logger.error(f"[OpenAI Proxy] Attempt {attempt+1} Network error: {req_err}")
            if attempt < max_retries:
                time.sleep(base_backoff * (2**attempt))
                continue
            openai_circuit.record_failure()
            return jsonify({'error': 'Network error communicating with OpenAI service.'}), 503
    
    # Fallthrough if all retries failed without specific handling above
    openai_circuit.record_failure()
    return jsonify({'error': 'Failed to communicate with OpenAI after multiple attempts.'}), 503


# ---------------- VADER SENTIMENT (No changes from new version) -----------------
analyzer = SentimentIntensityAnalyzer()

@app.route("/analyze/batch", methods=["POST"])
@limiter.limit("100 per minute") # Increased limit slightly
def analyze_batch():
    app.logger.info("[VADER_BATCH] Received request.")
    try:
        texts = request.get_json(force=True) or [] # force=True ensures it tries to parse even if content-type is not perfect
        if not texts:
            app.logger.info("[VADER_BATCH] Empty text list received.")
            return jsonify([]), 200

        num_texts = len(texts)
        first_text_preview = texts[0][:70] if texts and isinstance(texts[0], str) else "N/A"
        app.logger.info(f"[VADER_BATCH] Processing {num_texts} texts. First text preview: '{first_text_preview}...'")

        results = []
        start_time = time.time()
        
        # Chunk processing for very large batches (though client should ideally batch)
        MAX_CHUNK_VADER = 200 
        for i in range(0, len(texts), MAX_CHUNK_VADER):
            chunk = texts[i:i + MAX_CHUNK_VADER]
            for text_item in chunk:
                if isinstance(text_item, str):
                    score = analyzer.polarity_scores(text_item)["compound"]
                    results.append(score)
                else:
                    results.append(0.0) # Default for non-string items

        end_time = time.time()
        duration = end_time - start_time
        app.logger.info(f"[VADER_BATCH] Successfully processed {num_texts} texts in {duration:.3f} seconds.")
        return jsonify(results), 200

    except Exception as e:
        app.logger.error(f"[VADER_BATCH] Error during batch processing: {type(e).__name__} {e}", exc_info=True)
        return jsonify({"error": "Failed to process batch sentiment"}), 500
    
# ---------------- HEALTH CHECKS (Adapted from new version) ----------------
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({'status': 'ok', 'message': 'Backend is running'}), 200

@app.route("/health/deep", methods=["GET"])
def deep_health_check():
    checks = {}
    # Env vars
    checks['env'] = {
        'OPENAI_KEY_PRESENT': bool(OPENAI_API_KEY),
        'SMARTPROXY_CONFIGURED': bool(SMARTPROXY_USER and SMARTPROXY_PASS and SMARTPROXY_API_TOKEN)
    }
    
    # External services
    checks['external'] = {}
    # OpenAI API
    if OPENAI_API_KEY:
        try:
            # Using a light-weight, typically available model listing endpoint
            r_openai = session.get("https://api.openai.com/v1/models", 
                                   headers={"Authorization": f"Bearer {OPENAI_API_KEY}"}, timeout=5)
            checks['external']['openai_api_status'] = r_openai.status_code
            checks['external']['openai_api_ok'] = r_openai.status_code == 200
        except Exception as e_openai:
            app.logger.warning(f"Deep health check: OpenAI API failed: {e_openai}")
            checks['external']['openai_api_ok'] = False
            checks['external']['openai_api_error'] = str(e_openai)
    else:
        checks['external']['openai_api_ok'] = None # Not configured

    # Smartproxy API (if configured)
    if SMARTPROXY_USER and SMARTPROXY_PASS and SMARTPROXY_API_TOKEN:
        try:
            r_sp = session.post(
                "https://dashboard.smartproxy.com/api/v1/users/current/statistics/traffic-usage",
                headers={"Authorization": f"Token {SMARTPROXY_API_TOKEN}", "Content-Type": "application/json"},
                json={"proxyType": "residential_proxies", "limit": 1}, timeout=5
            )
            checks['external']['smartproxy_api_status'] = r_sp.status_code
            checks['external']['smartproxy_api_ok'] = r_sp.status_code == 200
        except Exception as e_sp:
            app.logger.warning(f"Deep health check: Smartproxy API failed: {e_sp}")
            checks['external']['smartproxy_api_ok'] = False
            checks['external']['smartproxy_api_error'] = str(e_sp)
    else:
        checks['external']['smartproxy_api_ok'] = None # Not configured

    # Disk space
    try:
        total, used, free = shutil.disk_usage('/')
        checks['disk'] = {
            'free_gb': round(free / (1024**3), 2),
            'total_gb': round(total / (1024**3), 2),
            'free_ratio': round(free / total, 2),
            'disk_ok': (free / total) > 0.1 # At least 10% free
        }
    except Exception as e_disk:
        app.logger.warning(f"Deep health check: Disk usage check failed: {e_disk}")
        checks['disk'] = {'disk_ok': False, 'error': str(e_disk)}

    # System Load (if available)
    try:
        cpu_count = os.cpu_count() or 1
        load1, _, _ = os.getloadavg()
        checks['load'] = {
            'load1': round(load1, 2),
            'cpu_count': cpu_count,
            'load_ok': load1 < (cpu_count * 2) # Load per core < 2
        }
    except (OSError, AttributeError) as e_load: # getloadavg not on all OS (e.g. Windows)
        app.logger.info(f"Deep health check: Load average not available: {e_load}")
        checks['load'] = {'load_ok': True, 'message': 'Not available on this system'}

    # Overall status decision logic
    env_ok = all(checks['env'].values())
    # For external_ok, consider 'None' as ok if not configured, or require True if configured
    external_services_configured_and_ok = True
    for key, val in checks['external'].items():
        if key.endswith("_ok") and val is False: # If any configured service is False, then it's not ok
            external_services_configured_and_ok = False
            break
            
    disk_ok = checks['disk'].get('disk_ok', False)
    load_ok = checks['load'].get('load_ok', False)

    final_status = 'ok'
    if not (env_ok and disk_ok and load_ok and external_services_configured_and_ok):
        final_status = 'degraded'
    if not env_ok or not disk_ok: # Critical failures
        final_status = 'fail'
    
    return jsonify({
        'status': final_status,
        'uptime_seconds': round(time.time() - app_start_time, 2),
        'checks': checks,
    }), 200 if final_status != 'fail' else 503


# --------------------------------------------------
# App Runner and Cleanup
# --------------------------------------------------
def close_shelves():
    app.logger.info("Closing shelve files...")
    if 'transcript_shelf' in globals() and transcript_shelf: transcript_shelf.close()
    if 'comment_shelf' in globals() and comment_shelf: comment_shelf.close()
    if 'analysis_shelf' in globals() and analysis_shelf: analysis_shelf.close()
    if 'metadata_shelf' in globals() and metadata_shelf: metadata_shelf.close()
    app.logger.info("Shelve files closed.")

def shutdown_executor():
    app.logger.info("Shutting down ThreadPoolExecutor...")
    executor.shutdown(wait=True) # Wait for tasks to complete during shutdown
    app.logger.info("ThreadPoolExecutor shut down.")

atexit.register(close_shelves)
atexit.register(shutdown_executor)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5010))
    # For development, threaded=True is fine.
    # For production, gunicorn (or similar) handles workers.
    # Flask's built-in server is not for production.
    app.logger.info(f"Starting Flask development server on host 0.0.0.0 port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)