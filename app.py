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
import os
# Persistent cache files
PERSISTENT_TRANSCRIPT_DB = os.path.join(os.getcwd(), "transcript_cache_persistent.db")
PERSISTENT_COMMENT_DB    = os.path.join(os.getcwd(), "comment_cache_persistent.db")
PERSISTENT_ANALYSIS_DB   = os.path.join(os.getcwd(), "analysis_cache_persistent.db")

transcript_shelf = shelve.open(PERSISTENT_TRANSCRIPT_DB)
comment_shelf    = shelve.open(PERSISTENT_COMMENT_DB)
analysis_shelf   = shelve.open(PERSISTENT_ANALYSIS_DB)
# Persistent metadata cache
PERSISTENT_METADATA_DB = os.path.join(os.getcwd(), "metadata_cache_persistent.db")
metadata_shelf = shelve.open(PERSISTENT_METADATA_DB)
import re
import time
import itertools
import json
import logging
from functools import lru_cache
from collections import deque 
from threading import Lock
_video_fetch_locks = {}
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
from cachetools.keys import hashkey
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
from youtube_comment_downloader import YoutubeCommentDownloader  # fallback comments scraper

session = requests.Session()

# ── Global browser UA to dodge anti‑bot filters ─────────────
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
session.headers.update({"User-Agent": BROWSER_UA})

session.request = functools.partial(session.request, timeout=10)
retry_cfg = Retry(
    total=3,                # retry up to 3 times for any error type
    connect=3,
    read=3,
    status=3,
    backoff_factor=0.5,     # exponential back‑off, 0.5 • 2^n
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=False,  # retry on *all* HTTP methods
    raise_on_status=False   # don’t throw after status_forcelist — let caller decide
)
session.mount("https://", HTTPAdapter(max_retries=retry_cfg))
session.mount("http://", HTTPAdapter(max_retries=retry_cfg))

#
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
SMARTPROXY_PORT  = "10000"
SMARTPROXY_API_TOKEN = os.getenv("SMARTPROXY_API_TOKEN")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# ✂️  Removed reliance on the official YouTube Data API and on local cookie files
YTDL_COOKIE_FILE = None      # Force‑disable cookie usage so yt‑dlp never sends personal cookies

# ------------------------------------------------------------------
# Runtime tunables (env‑driven)
# ------------------------------------------------------------------
# How long the Flask route will wait for a background transcript job
# before returning HTTP 202 (pending).  Can be overridden with
# TRANSCRIPT_FETCH_TIMEOUT env‑var – default is 25 s.
TRANSCRIPT_FETCH_TIMEOUT = int(os.getenv("TRANSCRIPT_FETCH_TIMEOUT", "25"))

# Maximum comments to retrieve per video (overridable via env)
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "120"))

PROXY_ROTATION = (
    [
        {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{10000+i}"}
        for i in range(20)
    ]
    if SMARTPROXY_USER else
    [{}]
)

_proxy_cycle = itertools.cycle(PROXY_ROTATION)
def rnd_proxy() -> dict:       # always returns {"https": "..."} or {}
    return next(_proxy_cycle)

PIPED_HOSTS = deque([
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.tokhmi.xyz",
    "https://pipedapi.moomoo.me",
    "https://piped.video",
    "https://piped.video.proxycache.net",
    "https://piped.video.deno.dev",
])
INVIDIOUS_HOSTS = deque([
    "https://yewtu.be",
    "https://inv.nadeko.net",
    "https://vid.puffyan.us",
    "https://ytdetail.8848.wtf",
])
_PIPE_COOLDOWN: dict[str, float] = {}
_IV_COOLDOWN: dict[str, float] = {}

# ── Healthy host pools (rotated & cooled down on failures) ───────────
HEALTHY_PIPED_HOSTS     = deque(list(PIPED_HOSTS))
HEALTHY_INVIDIOUS_HOSTS = deque(list(INVIDIOUS_HOSTS))
HOST_COOLDOWN_SECONDS   = 1800          # 30‑minute cooldown after hard failure

# Top 10 global languages for transcript fallback (used in resilient transcript fetch)
TOP_LANGS = [
    "es", "hi", "pt", "id", "ru",
    "ja", "ko", "de", "fr", "it"
]



def _fetch_json(hosts: deque, path: str,
                cooldown: dict[str, float],
                proxy_aware: bool = False,
                hard_deadline: float = 3.0):
    """
    Query up to 4 candidate hosts concurrently and return
    the first successful JSON response.  Hosts that error
    are put on cool‑down (5 min for network errors,
    10 min for malformed bodies).
    """
    host_success = getattr(_fetch_json, "_host_success", {})
    _fetch_json._host_success = host_success

    now = time.time()
    # Pick hosts not on cool‑down, best past success first
    candidates = [
        h for h in sorted(hosts, key=lambda h: -host_success.get(h, 0))
        if cooldown.get(h, 0) <= now
    ][:4]  # race at most 4

    if not candidates:
        return None

    with ThreadPoolExecutor(max_workers=len(candidates)) as pool:
        future_to_host = {
            pool.submit(
                session.get,
                f"{h}{path}",
                proxies=(rnd_proxy() if proxy_aware else {}),
                timeout=3
            ): h
            for h in candidates
        }

        done, _ = wait(future_to_host.keys(),
                       timeout=hard_deadline,
                       return_when=FIRST_COMPLETED)

        for fut in done:
            host = future_to_host[fut]
            try:
                r = fut.result()
                r.raise_for_status()
                if "application/json" not in r.headers.get("Content-Type", ""):
                    raise ValueError("non‑JSON body")
                host_success[host] = time.time()  # mark good
                return r.json()
            except Exception as e:
                app.logger.warning("Host %s failed: %s", host, e)
                cooldown[host] = time.time() + (
                    600 if isinstance(e, ValueError) else 300
                )
    return None


_YDL_OPTS = {
    "quiet": True,
    "skip_download": True,
    "innertube_key": "AIzaSyA-DkzGi-tv79Q",
    "user_agent": BROWSER_UA,
    "extract_flat": True,
    "forcejson": True,
    "socket_timeout": 8,   # Increased timeout slightly for reliability
    "retries": 1,          # Allowing a retry for improved reliability
    "proxy": rnd_proxy().get("https", None)  # Force usage of Smartproxy to avoid rate limits
}

def yt_dlp_info(video_id: str):
    """
    Thin wrapper around yt-dlp that
      • returns a cached JSON blob if we fetched the same video
        in the last 15 minutes,
      • injects a fresh proxy on every cold fetch,
      • uses aggressive time-outs so we fail fast when the exit IP
        is rate-limited.
    """
    if video_id in ytdl_cache:
        return ytdl_cache[video_id]

    opts = _YDL_OPTS.copy()
    opts["proxy"] = rnd_proxy().get("https", None)

    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(video_id, download=False, process=False)

    # Cache only if we obtained a sensible response (title present)
    if info.get("title"):
        ytdl_cache[video_id] = info
    return info

# --------------------------------------------------
# Flask init
# --------------------------------------------------
app = Flask(__name__)
CORS(app)

# ── Shelve helper ────────────────────────────────────────────
def _safe_put(shelf, key, value):
    """
    Safely write to a shelve object.  Any corruption or I/O errors are
    caught so that a single failure does not crash the whole request.
    """
    try:
        shelf[key] = value
        shelf.sync()
    except Exception as e:  # catches gdbm.error, shelve error, etc.
        app.logger.error("Shelve write error for key %s: %s", key, e)
        
# ── Minimal inbound access log ────────────────────
@app.before_request
def _access_log():
    app.logger.info("[IN] %s %s ← %s", request.method, request.path, request.headers.get("X-Real-IP", request.remote_addr))

# --------------------------------------------------
# Rate limiting
# --------------------------------------------------
# Combine remote IP with a hash of User‑Agent to avoid grouping
# every device on one home router into a single limiter bucket.
def _rate_limit_key():
    ua = request.headers.get("User-Agent", "")
    return f"{get_remote_address()}-{hash(ua) % 1000}"

redis_url = os.getenv("REDIS_URL")
limiter = Limiter(
    app=app,
    key_func=_rate_limit_key,
    default_limits=["200 per hour", "50 per minute"],
    headers_enabled=True,
    storage_uri=redis_url
)

# --------------------------------------------------
# Worker pool / cache
# --------------------------------------------------
transcript_cache = TTLCache(maxsize=2000, ttl=86_400)       # 24 h
comment_cache    = TTLCache(maxsize=300, ttl=300)           # 5 min         # 10 min
analysis_cache = TTLCache(maxsize=200, ttl=600)  # 10-minute in-memory cache for analysis
# 15-minute caches for yt-dlp info and metadata
ytdl_cache     = TTLCache(maxsize=1000, ttl=900)
metadata_cache = TTLCache(maxsize=2000, ttl=900)
NEGATIVE_TRANSCRIPT_CACHE = TTLCache(maxsize=5000, ttl=43_200)   # 12 h
executor         = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 4))
_pending: dict[str, Future] = {}  


# --------------------------------------------------
# Logging setup (console + rotating file)
# --------------------------------------------------
os.makedirs("logs", exist_ok=True)
file_handler = RotatingFileHandler(
    "logs/server.log", maxBytes=5_242_880, backupCount=3, encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s [%(module)s:%(lineno)d]: %(message)s"
))
file_handler.setLevel(logging.INFO)

app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

# --------------------------------------------------
# Helpers: validate / extract YT id
# --------------------------------------------------
VIDEO_ID_REGEX = re.compile(r'^[\w-]{11}$')

def validate_video_id(video_id: str) -> bool:
    return bool(VIDEO_ID_REGEX.fullmatch(video_id))

# Alias for legacy calls
valid_id = validate_video_id

@lru_cache(maxsize=1024)
def extract_video_id(input_str: str) -> str:
    patterns = [
        r'(?:v=|\/)([\w-]{11})',
        r'^([\w-]{11})$'
    ]
    for p in patterns:
        m = re.search(p, input_str)
        if m and validate_video_id(m.group(1)):
            return m.group(1)
    raise ValueError("Invalid YouTube URL or video ID")


# Discover available transcript languages for a video (fast, no proxy)
def discover_available_langs(video_id):
    "Return sorted list of transcript languages actually present for this video (fast, no proxy)."
    try:
        transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
        codes = [tr.language_code for tr in transcripts]
        # Move English first if present
        if "en" in codes:
            codes.remove("en")
            codes = ["en"] + codes
        return codes
    except Exception:
        # Fallback to common popular list
        return [
            "en", "es", "hi", "pt", "id", "ru", "ja", "ko", "de", "fr",
            "tr", "vi", "it", "ar", "pl", "uk", "fa", "nl", "th", "ro"
        ]





def _get_transcript(vid: str, langs, preserve):
    # Always use a rotating proxy (Smartproxy) for YouTubeTranscriptApi
    return YouTubeTranscriptApi.get_transcript(
        vid, languages=langs, preserve_formatting=preserve,
        proxies=rnd_proxy())


def _fetch_resilient(video_id: str) -> str:
    """
    Aggressively fetch a transcript for the given video ID using parallel and fallback strategies:
      1. Run Piped, YouTubeTranscriptApi (en), and yt-dlp in parallel. Return first valid result.
      2. Mark hosts as "bad" on any exception (for N minutes).
      3. If all primaries fail, run slow/serial fallbacks (lang fallback, HTML scrape).
    """
    if video_id in NEGATIVE_TRANSCRIPT_CACHE:
        return ""

    import concurrent.futures
    piped_errors = {}
    # --- Define all three primary strategies ---
    def fetch_with_piped():
        now = time.time()
        for host in list(PIPED_HOSTS):
            # Skip host if recently errored
            if piped_errors.get(host, 0) > now:
                continue
            try:
                url = f"{host}/api/v1/captions/{video_id}"
                # Try direct, then proxy if available
                for try_proxy in (False, True) if SMARTPROXY_USER else (False,):
                    proxy = rnd_proxy() if try_proxy else {}
                    app.logger.info("[Transcript] Trying Piped captions at %s (proxy=%s)", url, try_proxy)
                    r = session.get(url, proxies=proxy, timeout=5)
                    r.raise_for_status()
                    js = r.json()
                    for caption_track in js:
                        if caption_track.get("url"):
                            caption_url = caption_track["url"]
                            lang_code = caption_track.get("languageCode", "")
                            # Prefer English if possible
                            if lang_code == "en" or not js.index(caption_track):
                                r2 = session.get(caption_url, timeout=5)
                                r2.raise_for_status()
                                if r2.text.strip():
                                    app.logger.info(f"[Transcript] Success: Piped captions ({lang_code}) for %s (proxy={try_proxy})", video_id)
                                    return r2.text
                    app.logger.warning("[Transcript] No usable captions in Piped for %s (proxy=%s)", video_id, try_proxy)
            except Exception as e:
                # Mark host as bad for 2 minutes on error
                app.logger.warning(f"[Transcript] Piped failed at {host} for %s: %s", video_id, e)
                piped_errors[host] = time.time() + 120
                continue
        app.logger.error("[Transcript] All Piped endpoints failed for %s", video_id)
        return None

    def fetch_with_ytapi_en():
        try:
            segs = _get_transcript(video_id, ["en"], False)
            text = " ".join(s["text"] for s in segs).strip()
            if text:
                app.logger.info("[Transcript] Success: YouTubeTranscriptApi (en) for %s", video_id)
                return text
            app.logger.warning("[Transcript] YouTubeTranscriptApi (en) returned empty for %s", video_id)
        except Exception as e:
            # Mark as "bad" for 3 min on error (simulate marking host, here just log)
            app.logger.warning("[Transcript] YouTubeTranscriptApi (en) failed for %s: %s", video_id, e)
        return None

    def fetch_with_ytdlp():
        try:
            info = yt_dlp_info(video_id)
        except Exception as e:
            app.logger.warning("[Transcript] yt-dlp info failed for %s: %s", video_id, e)
            return None
        # Try automatic captions
        try:
            caps = info.get("automatic_captions") or {}
            first_track = next(iter(caps.values()), [])
            if first_track:
                url = first_track[0]["url"]
                r = session.get(url, timeout=5)
                r.raise_for_status()
                if r.text.strip():
                    app.logger.info("[Transcript] Success: yt-dlp automatic_captions for %s", video_id)
                    return r.text
            app.logger.warning("[Transcript] yt-dlp automatic_captions empty for %s", video_id)
        except Exception as e:
            app.logger.warning("[Transcript] yt-dlp automatic_captions failed for %s: %s", video_id, e)
        # Try manual captions
        try:
            caps = info.get("captions") or {}
            track = caps.get("en") or next(iter(caps.values()), [])
            if track:
                url = track[0]["url"]
                r = session.get(url, timeout=5)
                r.raise_for_status()
                if r.text.strip():
                    app.logger.info("[Transcript] Success: yt-dlp captions track for %s", video_id)
                    return r.text
            app.logger.warning("[Transcript] yt-dlp captions track empty for %s", video_id)
        except Exception as e:
            app.logger.warning("[Transcript] yt-dlp captions track failed for %s: %s", video_id, e)
        return None

    # --- Aggressively run all three primary strategies in parallel ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(fetch_with_piped): "piped",
            pool.submit(fetch_with_ytapi_en): "ytapi_en",
            pool.submit(fetch_with_ytdlp): "ytdlp"
        }
        # Wait for first completed
        done, not_done = concurrent.futures.wait(futures, timeout=6, return_when=concurrent.futures.FIRST_COMPLETED)
        # Return first valid result, cancel others
        for fut in done:
            try:
                res = fut.result()
                if res and len(res) > 10:
                    # Cancel others
                    for rem in not_done:
                        rem.cancel()
                    return res
            except Exception as e:
                # Mark as bad (simulate for all, e.g. piped_errors for Piped)
                pass
        # If none succeeded, wait a bit more for any slow ones
        for fut in not_done:
            try:
                res = fut.result(timeout=2)
                if res and len(res) > 10:
                    return res
            except Exception:
                continue

    # --- If all primaries fail, try fallback strategies ---
    app.logger.warning("[Transcript] All primary strategies failed for %s, running fallbacks.", video_id)

    # Fallback: YouTubeTranscriptApi with top-10 global languages (parallel)
    def fetch_with_ytapi_top_langs():
        def _attempt(lang_code: str):
            try:
                segs = _get_transcript(video_id, [lang_code], False)
                txt = " ".join(s["text"] for s in segs).strip()
                if txt:
                    app.logger.info("[Transcript] Success: YouTubeTranscriptApi (%s) for %s", lang_code, video_id)
                    return txt
            except (TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript):
                pass
            except Exception as e:
                app.logger.debug("[Transcript] %s failed for %s: %s", lang_code, video_id, e)
            return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(TOP_LANGS)) as pool:
            fut_map = {pool.submit(_attempt, lang): lang for lang in TOP_LANGS}
            done, _ = concurrent.futures.wait(
                fut_map,
                timeout=4,
                return_when=concurrent.futures.FIRST_COMPLETED
            )
            for fut in done:
                res = fut.result()
                if res:
                    return res
        return None

    ytapi_top_result = fetch_with_ytapi_top_langs()
    if ytapi_top_result and len(ytapi_top_result) > 10:
        return ytapi_top_result

    # Lightweight HTML scrape fallback (serial, only if all else fails)
    def fetch_with_html_scrape():
        try:
            resp = session.get(f"https://www.youtube.com/watch?v={video_id}", timeout=3)
            resp.raise_for_status()
            html = resp.text
            m = re.search(r'"captionTracks":\s*(\[\{.*?\}\])', html)
            if m:
                tracks = json.loads(m.group(1))
                base_url = tracks[0].get("baseUrl")
                if base_url:
                    r2 = session.get(base_url, timeout=3)
                    r2.raise_for_status()
                    # strip XML tags to plain text
                    return "".join(re.findall(r'<text[^>]*>(.*?)</text>', r2.text))
        except Exception:
            return None
        return None

    scraped = fetch_with_html_scrape()
    if scraped and len(scraped) > 10:
        return scraped

    # Final fallback: try Piped serially again (could have new hosts)
    piped_result = fetch_with_piped()
    if piped_result and len(piped_result) > 10:
        return piped_result

    app.logger.error("[Transcript] All transcript sources failed for %s", video_id)
    NEGATIVE_TRANSCRIPT_CACHE[video_id] = True
    raise RuntimeError("Transcript unavailable from all sources")


def _get_or_spawn(video_id: str, timeout: float = TRANSCRIPT_FETCH_TIMEOUT) -> str:
    """
    Ensure only one worker fetches a given transcript while others await it.
    Uses a lock per video_id to avoid race condition of launching the same fetch.
    """
    lock = _video_fetch_locks.setdefault(video_id, Lock())
    with lock:
        # Check persistent shelf first
        if video_id in transcript_shelf:
            return transcript_shelf[video_id]
        if (cached := transcript_cache.get(video_id)):
            return cached
        fut = _pending.get(video_id)
        if fut is None:
            fut = executor.submit(_fetch_resilient, video_id)
            _pending[video_id] = fut
        try:
            result = fut.result(timeout=timeout)
            transcript_cache[video_id] = result
            _safe_put(transcript_shelf, video_id, result)
            return result
        finally:
            if fut.done():
                _pending.pop(video_id, None)
            _video_fetch_locks.pop(video_id, None)

# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
@app.get("/transcript")
@limiter.limit("100/hour")
def transcript():
    video_id = request.args.get("videoId", "")
    try:
        vid = extract_video_id(video_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    try:
        text = _get_or_spawn(vid)
        return jsonify({"video_id": vid, "text": text}), 200
    except TimeoutError:
        return jsonify({"status": "pending"}), 202
    except Exception as e:
        app.logger.error("Transcript generation failed for %s: %s", vid, e)
        return jsonify({"status": "unavailable", "error": str(e)}), 404
    

# ---------------- PROXY STATS ---------------------
@app.route("/proxy_stats", methods=["GET"])
def get_proxy_stats():
    from datetime import datetime, timedelta
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)
    payload = {
        "proxyType": "residential_proxies",
        "startDate": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endDate": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "limit": 1
    }
    app.logger.info("[OUT] Smartproxy stats")
    try:
        r = session.post(
            "https://dashboard.smartproxy.com/subscription-api/v1/api/public/statistics/traffic",
            json=payload, timeout=10
        )
        app.logger.info("[OUT] Smartproxy ← %s (stats)", r.status_code)
        r.raise_for_status()
        return jsonify(r.json()), 200
    except Exception as e:
        app.logger.error("Proxy stats error: %s", e)
        return jsonify({'error': 'Could not retrieve proxy stats'}), 503

# ---------------- Comments ----------------

def _download_comments_downloader(video_id: str, limit: int = COMMENT_LIMIT) -> list[str] | None:
    """
    Fallback comments fetch that *does not* rely on Piped/Invidious or YouTube Data API.
    Uses the `youtube-comment-downloader` library (mimics YouTube web requests).

    Returns a list of comment strings (top-level only) or None on failure.
    """
    try:
        downloader = YoutubeCommentDownloader()
        comments: list[str] = []
        for c in downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}"):
            txt = c.get("text")
            if txt:
                comments.append(txt)
            if len(comments) >= limit:
                break
        return comments or None
    except Exception as e:
        app.logger.warning("youtube-comment-downloader failed for %s: %s", video_id, e)
        return None


@app.route("/video/comments")
def comments():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400
    # Check in-memory comments cache
    if vid in comment_cache:
        return jsonify({"comments": comment_cache[vid]}), 200
    # Check persistent comments shelf
    if vid in comment_shelf:
        cached_comments = comment_shelf[vid]
        comment_cache[vid] = cached_comments
        return jsonify({"comments": cached_comments}), 200

    # -------- Parallel fetch: downloader + Piped (fastest wins) --------
    import concurrent.futures

    def _piped_comments():
        js = _fetch_json(
            HEALTHY_PIPED_HOSTS,
            f"/api/v1/comments/{vid}?cursor=0",
            _PIPE_COOLDOWN
        )
        if js and js.get("comments"):
            return [
                c.get("comment") or c.get("commentText")
                for c in js["comments"]
                if c.get("comment") or c.get("commentText")
            ] or None
        return None

    def _downloader_comments():
        return _download_comments_downloader(vid, COMMENT_LIMIT)

    fetched_comments, winner = None, "N/A"
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fut_map = {
            pool.submit(_downloader_comments): "downloader",
            pool.submit(_piped_comments): "piped"
        }
        for fut in concurrent.futures.as_completed(fut_map, timeout=15):
            try:
                res = fut.result()
                if res:
                    fetched_comments = res[:COMMENT_LIMIT]
                    winner = fut_map[fut]
                    break
            except Exception:
                continue

    if fetched_comments:
        app.logger.info("Comments fetched via %s for %s (count: %d)",
                        winner, vid, len(fetched_comments))
        comment_cache[vid] = fetched_comments
        _safe_put(comment_shelf, vid, fetched_comments)
        return jsonify({"comments": fetched_comments}), 200
        # If we get here, all comment sources failed
    app.logger.error("All comment sources failed for %s", vid)
    return jsonify({"error": "Could not retrieve comments"}), 503

# ---------------- Metadata ---------------
@app.route("/video/metadata")
def metadata():
    vid = request.args.get("videoId", "")
    # Persistent and hot cache checks (unchanged)
    if vid in metadata_shelf:
        return jsonify({"items": [metadata_shelf[vid]]}), 200
    if vid in metadata_cache:
        return jsonify({"items": [metadata_cache[vid]]}), 200
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400

    base = {
        "title": None,
        "channelTitle": None,
        "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "thumbnailUrl": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "duration": None,
        "viewCount": None,
        "likeCount": None,
        "videoId": vid,
    }

    # Helper to merge fresh stats into the accumulating `base`
    def _merge(obj: dict):
        nonlocal base
        vc_raw = obj.get("viewCount")
        try:
            vc = int(vc_raw) if vc_raw not in (None, "", "NaN") else None
        except Exception:
            vc = None
        if vc and vc > 0:
            base["viewCount"] = vc
        if obj.get("likeCount") not in (None, "", "NaN"):
            try:
                base["likeCount"] = int(obj["likeCount"])
            except Exception:
                pass
        for k in ("title", "channelTitle", "thumbnail"):
            if obj.get(k) and not base.get(k):
                base[k] = obj[k]

    # --- Define parallel metadata fetch strategies ---
    def fetch_oembed():
        try:
            oe = session.get(
                "https://www.youtube.com/oembed",
                params={"url": f"https://youtu.be/{vid}", "format": "json"},
                timeout=3,
            ).json()
            return {
                "title": oe.get("title"),
                "channelTitle": oe.get("author_name"),
                "thumbnail": oe.get("thumbnail_url"),
            }
        except Exception as e:
            app.logger.debug("[Metadata] oEmbed fail for %s: %s", vid, e)
            return None

    def fetch_piped():
        try:
            js = _fetch_json(
                HEALTHY_PIPED_HOSTS,
                f"/api/v1/streams/{vid}",
                _PIPE_COOLDOWN,
                hard_deadline=3.0,
            )
            if js and "views" in js:
                return {
                    "title": js.get("title"),
                    "channelTitle": js.get("uploader"),
                    "thumbnail": js.get("thumbnailUrl"),
                    "viewCount": js.get("views"),
                    "likeCount": js.get("likes"),
                    "duration": js.get("duration"),
                }
        except Exception as e:
            # Mark host as bad for 10 min
            if HEALTHY_PIPED_HOSTS:
                _PIPE_COOLDOWN[HEALTHY_PIPED_HOSTS[0]] = time.time() + 600
            app.logger.warning("[Metadata] Piped error for %s: %s", vid, e)
        return None

    def fetch_invidious():
        try:
            js = _fetch_json(
                HEALTHY_INVIDIOUS_HOSTS,
                f"/api/v1/videos/{vid}",
                _IV_COOLDOWN,
                proxy_aware=bool(SMARTPROXY_USER),
                hard_deadline=3.0,
            )
            if js and "viewCount" in js:
                return {
                    "title": js.get("title"),
                    "channelTitle": js.get("author"),
                    "thumbnail": js.get("thumbnail"),
                    "viewCount": js.get("viewCount"),
                    "likeCount": js.get("likeCount"),
                    "duration": js.get("lengthSeconds") or js.get("durationSeconds"),
                }
        except Exception as e:
            if HEALTHY_INVIDIOUS_HOSTS:
                _IV_COOLDOWN[HEALTHY_INVIDIOUS_HOSTS[0]] = time.time() + 600
            app.logger.warning("[Metadata] Invidious error for %s: %s", vid, e)
        return None

    def fetch_ytdlp():
        try:
            yt = yt_dlp_info(vid)
            return {
                "title": yt.get("title"),
                "channelTitle": yt.get("uploader"),
                "thumbnail": yt.get("thumbnail"),
                "viewCount": yt.get("view_count"),
                "likeCount": yt.get("like_count"),
                "duration": yt.get("duration"),
            }
        except Exception as e:
            app.logger.warning("[Metadata] yt-dlp error for %s: %s", vid, e)
            return None

    # --- Parallel fetching: oEmbed, yt-dlp, Piped, Invidious ---
    import concurrent.futures
    strategies = [
        ("oembed", fetch_oembed),
        ("ytdlp", fetch_ytdlp),
        ("piped", fetch_piped),
        ("invidious", fetch_invidious),
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        fut_map = {pool.submit(fn): name for name, fn in strategies}
        # Wait for the first to return both title and viewCount
        done, not_done = concurrent.futures.wait(
            fut_map, timeout=4, return_when=concurrent.futures.FIRST_COMPLETED
        )
        first_partial = None
        for fut in done:
            try:
                res = fut.result()
                if res:
                    _merge(res)
                    # If both title and viewCount, return immediately
                    if base.get("title") and base.get("viewCount") is not None:
                        for rem in not_done:
                            rem.cancel()
                        base["thumbnailUrl"] = base.get("thumbnail", base["thumbnailUrl"])
                        metadata_cache[vid] = base
                        _safe_put(metadata_shelf, vid, base)
                        return jsonify({"items": [base]}), 200
                    # If partial (e.g. oEmbed), remember it
                    if not first_partial and (base.get("title") or base.get("channelTitle")):
                        first_partial = dict(base)
            except Exception:
                continue
        # If nothing valid yet, wait for remaining up to 3s more
        for fut in not_done:
            try:
                res = fut.result(timeout=3)
                if res:
                    _merge(res)
                    if base.get("title") and base.get("viewCount") is not None:
                        base["thumbnailUrl"] = base.get("thumbnail", base["thumbnailUrl"])
                        metadata_cache[vid] = base
                        _safe_put(metadata_shelf, vid, base)
                        return jsonify({"items": [base]}), 200
                    if not first_partial and (base.get("title") or base.get("channelTitle")):
                        first_partial = dict(base)
            except Exception:
                continue

    # If partial metadata available, return it immediately and launch background refresh for missing fields
    if first_partial and (first_partial.get("title") or first_partial.get("channelTitle")):
        app.logger.info("[Metadata] Returning partial metadata for %s, background refresh for missing fields.", vid)
        # Fire-and-forget background fetch for missing fields
        def _bg_refresh():
            try:
                # Try all strategies again, serially, with longer timeout
                for name, fn in strategies:
                    try:
                        res = fn()
                        if res:
                            _merge(res)
                            if base.get("title") and base.get("viewCount") is not None:
                                base["thumbnailUrl"] = base.get("thumbnail", base["thumbnailUrl"])
                                metadata_cache[vid] = base
                                _safe_put(metadata_shelf, vid, base)
                                break
                    except Exception:
                        continue
            except Exception:
                pass
        executor.submit(_bg_refresh)
        return jsonify({"items": [first_partial]}), 200

    # --- Fallback: Robust serial attempts if all parallel fail ---
    app.logger.info("[Metadata] Resorting to robust serial fallback for %s", vid)
    for name, fn in strategies:
        try:
            res = fn()
            if res:
                _merge(res)
                if base.get("title") and base.get("viewCount") is not None:
                    base["thumbnailUrl"] = base.get("thumbnail", base["thumbnailUrl"])
                    metadata_cache[vid] = base
                    _safe_put(metadata_shelf, vid, base)
                    return jsonify({"items": [base]}), 200
        except Exception:
            continue

    # yt‑dlp / some mirrors occasionally return the literal string 'Untitled Video'
    if base.get("title") == "Untitled Video" and (not base.get("viewCount") or base["viewCount"] == 0):
        base["title"] = None
        app.logger.warning("[Metadata] Dropped 'Untitled Video' with zero views for %s", vid)
    # Final check – if still missing essentials, return 503 so client can degrade gracefully
    if base.get("title") is None or base.get("viewCount") is None:
        app.logger.error(
            "[Metadata] Unrecoverable: missing essential metadata for %s after all strategies. Returning 503.",
            vid,
        )
        return jsonify({"error": "Could not retrieve essential video metadata"}), 503

    base["thumbnailUrl"] = base.get("thumbnail", base["thumbnailUrl"])
    metadata_cache[vid] = base
    _safe_put(metadata_shelf, vid, base)
    return jsonify({"items": [base]}), 200


# ─────────────────────────────────────────────
# Simple circuit-breaker for upstream errors (OpenAI proxy)
# ─────────────────────────────────────────────
class CircuitBreaker:
    """
    Minimal circuit-breaker implementation.
    Opens after N failures and stays open for timeout.
    """
    def __init__(self, name: str, failure_threshold: int = 3, reset_timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.is_open = False
        self._lock = Lock()

    def allow_request(self) -> bool:
        with self._lock:
            if self.is_open:
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.is_open = False
                    self.failure_count = 0
                    return True
                return False
            return True

    def record_success(self):
        with self._lock:
            self.failure_count = 0
            self.is_open = False

    def record_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.is_open = True
                app.logger.warning("Circuit %s opened after %d failures",
                                   self.name, self.failure_count)
   

# ---------------- OpenAI RESPONSES POST
openai_circuit = CircuitBreaker("openai", failure_threshold=3, reset_timeout=60)

@app.route("/openai/responses", methods=["POST"])
def create_response():
    """OpenAI proxy with retry and circuit-breaker."""
    if not OPENAI_API_KEY:
        return jsonify({'error': 'OpenAI API key not configured'}), 500

    if not openai_circuit.allow_request():
        return jsonify({'error': 'Service temporarily unavailable (circuit open)'}), 503

    try:
        payload = request.get_json(force=True)
    except:
        return jsonify({'error': 'Invalid JSON payload'}), 400

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    max_retries = 3
    backoff = 0.5
    for attempt in range(max_retries):
        try:
            timeout = 30 + attempt * 10
            resp = session.post(
                "https://api.openai.com/v1/responses",
                headers=headers,
                json=payload,
                timeout=timeout
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "1"))
                time.sleep(min(retry_after, 10))
                continue

            resp.raise_for_status()
            openai_circuit.record_success()
            return jsonify(resp.json()), resp.status_code

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(backoff * (2 ** attempt))
        except Exception:
            break

    openai_circuit.record_failure()
    return jsonify({
        'error': 'Service temporarily unavailable',
        'message': 'Failed to communicate with OpenAI after multiple attempts'
    }), 503

@app.route("/analyze/batch", methods=["POST"])
@limiter.limit("50 per minute")
def analyze_batch():
    """Chunked VADER processing to avoid long timeouts."""
    texts = request.get_json(force=True) or []
    if not texts:
        return jsonify([]), 200

    MAX_CHUNK = 50
    results: list[float] = []
    for i in range(0, len(texts), MAX_CHUNK):
        for t in texts[i:i + MAX_CHUNK]:
            try:
                results.append(analyzer.polarity_scores(t)["compound"])
            except Exception:
                results.append(0.0)

    return jsonify(results), 200
    

# ---------------- SIMPLE HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({'status': 'ok'}), 200

# ---------------- FULL HEALTH CHECK ------------------
@app.route("/health/deep", methods=["GET"])
def deep_health_check():
    checks = {}
    # NOTE: The service no longer calls the official YouTube Data API,
    # which improves robustness on high‑volume deployments that can’t
    # obtain or refresh API quotas.
    # env
    checks['env'] = {
        'OPENAI_KEY': bool(OPENAI_API_KEY),
        'SMARTPROXY_TOKEN': bool(SMARTPROXY_API_TOKEN)
    }
    # external
    checks['external'] = {}
    try:
        r = session.get("https://api.openai.com/v1/models", timeout=5)
        checks['external']['openai_api'] = r.status_code == 200
    except Exception:
        checks['external']['openai_api'] = False
    checks['external']['youtube_api'] = None  # skipped – official API disabled
    try:
        r = session.post("https://dashboard.smartproxy.com/subscription-api/v1/api/public/statistics/traffic",
                         json={"proxyType":"residential_proxies","limit":1},
                         timeout=5)
        checks['external']['smartproxy_api'] = r.status_code == 200
    except Exception:
        checks['external']['smartproxy_api'] = False
    # disk
    total, used, free = shutil.disk_usage('/')
    checks['disk'] = {'free_ratio': round(free/total,2), 'disk_ok': (free/total) > 0.1}
    # load
    try:
        load1, _, _ = os.getloadavg()
        checks['load'] = {'load1': round(load1,2), 'load_ok': load1 < ((os.cpu_count() or 1)*2)}
    except Exception:
        checks['load'] = {'load_ok': True}

    env_ok      = all(checks['env'].values())
    external_ok = all(v for v in checks['external'].values() if isinstance(v,bool))
    disk_ok     = checks['disk']['disk_ok']
    load_ok     = checks['load']['load_ok']

    status = 'ok' if (env_ok and disk_ok and load_ok and external_ok) else \
             ('degraded' if (env_ok and disk_ok and load_ok) else 'fail')

    return jsonify({
        'status': status,
        'checks': checks,
        'uptime_seconds': round(time.time() - app_start_time, 2)
    }), 200


# --------------------------------------------------
# Run
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5010))
    app.run(host="0.0.0.0", port=port, threaded=True)

import atexit
atexit.register(lambda: executor.shutdown(wait=False))
atexit.register(lambda: transcript_shelf.close())
atexit.register(lambda: comment_shelf.close())
atexit.register(lambda: analysis_shelf.close())

