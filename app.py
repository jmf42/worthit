# yt-dlp runs direct (no proxy) unless you see repeated rate limits, in which case set up proxy use for yt-dlp.
# YouTubeTranscriptApi always uses Smartproxy (if credentials provided) for maximum reliability.
# Piped fallback tries direct, then proxy if necessary.
# All transcript fetches are coordinated via a per-video_id lock to avoid race conditions.
# The list of fallback languages is configurable via the POPULAR_LANGS environment variable (comma-separated).

# Additions for improved language fallback and host cooldown logic
import os
import random
import shelve
# Persistent cache files
PERSISTENT_TRANSCRIPT_DB = os.path.join(os.getcwd(), "transcript_cache_persistent.db")
PERSISTENT_COMMENT_DB    = os.path.join(os.getcwd(), "comment_cache_persistent.db")
PERSISTENT_ANALYSIS_DB   = os.path.join(os.getcwd(), "analysis_cache_persistent.db")

transcript_shelf = shelve.open(PERSISTENT_TRANSCRIPT_DB)
comment_shelf    = shelve.open(PERSISTENT_COMMENT_DB)
analysis_shelf   = shelve.open(PERSISTENT_ANALYSIS_DB)
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

OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY")
YOUTUBE_DATA_API_KEY = os.getenv("YOUTUBE_API_KEY")
YTDL_COOKIE_FILE     = os.getenv("YTDL_COOKIE_FILE")

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



def _fetch_json(hosts: deque, path: str,
                cooldown: dict[str, float],
                proxy_aware: bool = False,
                hard_deadline: float = 6.0):
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
    **({"cookiefile": YTDL_COOKIE_FILE} if YTDL_COOKIE_FILE else {}),
}

# Light wrapper around yt‑dlp that injects a fresh proxy on every call so hundreds of concurrent users don’t all share the exact same exit IP.
def yt_dlp_info(video_id: str):
    """
    Light wrapper around yt‑dlp that injects a fresh proxy
    on every call so hundreds of concurrent users don’t all
    share the exact same exit IP.
    """
    opts = _YDL_OPTS.copy()
    opts["proxy"] = rnd_proxy().get("https", None)
    with YoutubeDL(opts) as ydl:
        return ydl.extract_info(video_id, download=False, process=False)

# --------------------------------------------------
# Flask init
# --------------------------------------------------
app = Flask(__name__)
CORS(app)

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

limiter = Limiter(app=app,
                  key_func=_rate_limit_key,
                  default_limits=["200 per hour", "50 per minute"],
                  headers_enabled=True)

# --------------------------------------------------
# Worker pool / cache
# --------------------------------------------------
transcript_cache = TTLCache(maxsize=2000, ttl=86_400)       # 24 h
comment_cache    = TTLCache(maxsize=300, ttl=300)           # 5 min         # 10 min
analysis_cache = TTLCache(maxsize=200, ttl=600)  # 10-minute in-memory cache for analysis
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
    Robustly fetch a transcript for the given video ID using parallel and fallback strategies:
      1. Run both yt-dlp and YouTubeTranscriptApi (English, with Smartproxy) in parallel as primary strategies.
         - yt-dlp runs without Smartproxy unless persistent rate limits are observed (option kept in code, but not default).
         - YouTubeTranscriptApi always uses Smartproxy.
      2. If both fail, fallback: YouTubeTranscriptApi with top 20 most popular languages (global YouTube order), with Smartproxy.
      3. If all above fail, fallback to Piped API, optionally with Smartproxy only if needed (i.e., if IP banned).
      4. Exit early upon first valid transcript.
      5. All steps are logged for observability.
    """
    import concurrent.futures

    # Graceful cooldown dicts for Piped and Invidious
    piped_errors = {}
    invidious_errors = {}

    # Allow POPULAR_LANGS override by environment variable (comma-separated)
    POPULAR_LANGS = os.getenv("POPULAR_LANGS")
    if POPULAR_LANGS:
        POPULAR_LANGS = [l.strip() for l in POPULAR_LANGS.split(",") if l.strip()]
    else:
        POPULAR_LANGS = [
            "en", "es", "hi", "pt", "id", "ru", "ja", "ko", "de", "fr",
            "tr", "vi", "it", "ar", "pl", "uk", "fa", "nl", "th", "ro"
        ]

    # --- Primary: YouTubeTranscriptApi (en), always with Smartproxy
    def fetch_with_ytapi_en():
        try:
            segs = _get_transcript(video_id, ["en"], False)
            text = " ".join(s["text"] for s in segs).strip()
            if text:
                app.logger.info("[Transcript] Success: YouTubeTranscriptApi (en) for %s", video_id)
                return text
            app.logger.warning("[Transcript] YouTubeTranscriptApi (en) returned empty for %s", video_id)
        except Exception as e:
            app.logger.warning("[Transcript] YouTubeTranscriptApi (en) failed for %s: %s", video_id, e)
        return None

    # --- Primary: yt-dlp, without Smartproxy by default
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
                r = session.get(url, timeout=8)
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
                r = session.get(url, timeout=8)
                r.raise_for_status()
                if r.text.strip():
                    app.logger.info("[Transcript] Success: yt-dlp captions track for %s", video_id)
                    return r.text
            app.logger.warning("[Transcript] yt-dlp captions track empty for %s", video_id)
        except Exception as e:
            app.logger.warning("[Transcript] yt-dlp captions track failed for %s: %s", video_id, e)
        return None

    # --- Fallback: YouTubeTranscriptApi with discovered languages (with Smartproxy, backoff)
    def fetch_with_ytapi_discovered_langs():
        langs = discover_available_langs(video_id)
        for idx, lang in enumerate(langs):
            if lang == "en":
                continue  # already tried in primary
            backoff = min(2 ** idx, 10)
            try:
                segs = _get_transcript(video_id, [lang], False)
                text = " ".join(s["text"] for s in segs).strip()
                if text:
                    app.logger.info("[Transcript] Success: YouTubeTranscriptApi (%s) for %s", lang, video_id)
                    return text
                app.logger.warning("[Transcript] YouTubeTranscriptApi (%s) returned empty for %s", lang, video_id)
            except Exception as e:
                app.logger.info("[Transcript] YouTubeTranscriptApi (%s) failed for %s: %s", lang, video_id, e)
                # Exponential backoff between failed languages
                time.sleep(backoff)
        return None

    # --- Fallback: Piped API, only use Smartproxy if needed
    def fetch_with_piped():
        now = time.time()
        for host in list(PIPED_HOSTS):
            # Skip host if recently errored
            if piped_errors.get(host, 0) > now:
                continue
            try:
                url = f"{host}/api/v1/captions/{video_id}"
                # Only use Smartproxy if needed (IP banned), try direct first
                for try_proxy in (False, True) if SMARTPROXY_USER else (False,):
                    proxy = rnd_proxy() if try_proxy else {}
                    app.logger.info("[Transcript] Trying Piped captions at %s (proxy=%s)", url, try_proxy)
                    r = session.get(url, proxies=proxy, timeout=6)
                    r.raise_for_status()
                    js = r.json()
                    # js is a list of available captions tracks (may be empty)
                    for caption_track in js:
                        if caption_track.get("url"):
                            caption_url = caption_track["url"]
                            lang_code = caption_track.get("languageCode", "")
                            # Prioritize English if possible, else take first
                            if lang_code == "en" or not js.index(caption_track):
                                r2 = session.get(caption_url, timeout=8)
                                r2.raise_for_status()
                                if r2.text.strip():
                                    app.logger.info(f"[Transcript] Success: Piped captions ({lang_code}) for %s (proxy={try_proxy})", video_id)
                                    return r2.text
                    app.logger.warning("[Transcript] No usable captions in Piped for %s (proxy=%s)", video_id, try_proxy)
            except Exception as e:
                app.logger.warning(f"[Transcript] Piped failed at {host} for %s: %s", video_id, e)
                piped_errors[host] = time.time() + random.randint(30, 90)  # skip 30–90s
                continue
        app.logger.error("[Transcript] All Piped endpoints failed for %s", video_id)
        return None

    # (To go fully async in the future: consider refactoring this block and all network calls with asyncio/gather.)
    # --- Run primary strategies in parallel: yt-dlp and YouTubeTranscriptApi (en)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_method = {
            executor.submit(fetch_with_ytapi_en): "ytapi_en",
            executor.submit(fetch_with_ytdlp): "ytdlp"
        }
        done, not_done = concurrent.futures.wait(
            future_to_method, timeout=18, return_when=concurrent.futures.FIRST_COMPLETED
        )
        # Try to get first success result
        for fut in done:
            res = fut.result()
            if res and len(res) > 10:
                # Cancel the other future if it's still running
                for remaining in not_done:
                    remaining.cancel()
                return res
        # If neither succeeded, wait for both to complete and see if any succeeded (edge case)
        for fut in not_done:
            try:
                res = fut.result(timeout=4)
                if res and len(res) > 10:
                    return res
            except Exception:
                continue

    app.logger.warning("[Transcript] Both primary methods failed for %s, trying YouTubeTranscriptApi fallback for popular languages.", video_id)

    # --- Fallback: YouTubeTranscriptApi with discovered languages
    ytapi_discovered_result = fetch_with_ytapi_discovered_langs()
    if ytapi_discovered_result and len(ytapi_discovered_result) > 10:
        return ytapi_discovered_result

    app.logger.warning("[Transcript] All YouTubeTranscriptApi strategies failed for %s, trying Piped fallback.", video_id)

    # --- Fallback: Try Piped API for captions
    piped_result = fetch_with_piped()
    if piped_result and len(piped_result) > 10:
        return piped_result

    app.logger.error("[Transcript] All transcript sources failed for %s", video_id)
    raise RuntimeError("Transcript unavailable from all sources")


def _get_or_spawn(video_id: str, timeout: float = 25.0) -> str:
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
            transcript_shelf[video_id] = result
            transcript_shelf.sync()
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

    # 1) youtube-comment-downloader (default)
    scraped = _download_comments_downloader(vid, COMMENT_LIMIT)
    if scraped:
        comment_cache[vid] = scraped
        comment_shelf[vid] = scraped
        comment_shelf.sync()
        app.logger.info("Comments fetched via youtube-comment-downloader for %s", vid)
        return jsonify({"comments": scraped}), 200

    # 2) Piped
    js = _fetch_json(PIPED_HOSTS, f"/api/v1/comments/{vid}?cursor=0", _PIPE_COOLDOWN)
    if js and (lst := [c["comment"] for c in js.get("comments", [])]):
        comment_cache[vid] = lst
        comment_shelf[vid] = lst
        comment_shelf.sync()
        app.logger.info("Comments fetched via Piped for %s", vid)
        return jsonify({"comments": lst}), 200

    # 3) yt-dlp (only if cookies)
    if YTDL_COOKIE_FILE:
        try:
            yt = yt_dlp_info(vid)
            lst = [c["content"] for c in yt.get("comments", [])][:40]
            if lst:
                comment_cache[vid] = lst
                comment_shelf[vid] = lst
                comment_shelf.sync()
                app.logger.info("Comments fetched via yt-dlp for %s", vid)
                return jsonify({"comments": lst}), 200
        except Exception as e:
            app.logger.info("yt-dlp comments failed: %s", e)

    # 4) Invidious
    js = _fetch_json(INVIDIOUS_HOSTS,
                     f"/api/v1/comments/{vid}?sort_by=top",
                     _IV_COOLDOWN, proxy_aware=bool(SMARTPROXY_USER))
    if js and (lst := [c["content"] for c in js.get("comments", [])]):
        comment_cache[vid] = lst
        comment_shelf[vid] = lst
        comment_shelf.sync()
        app.logger.info("Comments fetched via Invidious for %s", vid)
        return jsonify({"comments": lst}), 200

    app.logger.info("All comment sources failed for %s", vid)
    return jsonify({"comments": comment_cache.get(vid, [])}), 200

# ---------------- Metadata ---------------
@app.route("/video/metadata")
def metadata():
    vid = request.args.get("videoId", "")
    if not vid:
        app.logger.error("[Metadata] missing_video_id in request")
        return jsonify({"error": "missing_video_id"}), 400
    if not valid_id(vid):
        app.logger.error("[Metadata] invalid_video_id in request: %s", vid)
        return jsonify({"error": "invalid_video_id"}), 400

    # ---------- robust multi‑provider flow ----------
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

    # helper to merge fresh stats into the accumulating `base`
    def _merge_stats(obj: dict):
        nonlocal base
        if obj.get("viewCount") not in (None, 0, "0", ""):
            try:
                vc = int(obj["viewCount"])
                if vc > 0:
                    base["viewCount"] = vc
            except Exception:
                pass
        if obj.get("likeCount") not in (None, "", "NaN"):
            try:
                base["likeCount"] = int(obj["likeCount"])
            except Exception:
                pass
        for k in ("title", "channelTitle", "thumbnail"):
            if obj.get(k) and not base.get(k):
                base[k] = obj[k]
        base["thumbnailUrl"] = base["thumbnail"]

    # 1) oEmbed  – fast, metadata‑only (no stats)
    try:
        oe = session.get(
            "https://www.youtube.com/oembed",
            params={"url": f"https://youtu.be/{vid}", "format": "json"},
            timeout=4,
        ).json()
        _merge_stats(
            {
                "title": oe.get("title"),
                "channelTitle": oe.get("author_name"),
                "thumbnail": oe.get("thumbnail_url"),
            }
        )
        app.logger.info(
            "[Metadata] oEmbed partial for %s – title='%s'", vid, base["title"]
        )
    except Exception as e:
        app.logger.debug("[Metadata] oEmbed fail for %s: %s", vid, e)

    # 2) Piped  – primary stats source
    piped = _fetch_json(PIPED_HOSTS, f"/api/v1/streams/{vid}", _PIPE_COOLDOWN)
    if piped:
        _merge_stats(
            {
                "title": piped.get("title"),
                "channelTitle": piped.get("uploader"),
                "thumbnail": piped.get("thumbnailUrl"),
                "viewCount": piped.get("views"),
                "likeCount": piped.get("likes"),
            }
        )
        app.logger.info(
            "[Metadata] Piped for %s: views=%s likes=%s",
            vid,
            base["viewCount"],
            base["likeCount"],
        )
        if base["viewCount"]:
            return jsonify({"items": [base]}), 200  # done ✅

    # 3) Invidious  – secondary stats
    if not base["viewCount"]:
        iv = _fetch_json(INVIDIOUS_HOSTS, f"/api/v1/videos/{vid}", _IV_COOLDOWN)
        if iv:
            _merge_stats(
                {
                    "title": iv.get("title"),
                    "channelTitle": iv.get("author"),
                    "thumbnail": iv.get("thumbnail"),
                    "viewCount": iv.get("viewCount"),
                    "likeCount": iv.get("likeCount"),
                }
            )
            app.logger.info(
                "[Metadata] Invidious for %s: views=%s", vid, base["viewCount"]
            )
            if base["viewCount"]:
                return jsonify({"items": [base]}), 200  # done ✅

    # 4) yt‑dlp  – last‑resort scraper (slow)
    if not base["viewCount"]:
        try:
            yt = yt_dlp_info(vid)
            _merge_stats(
                {
                    "title": yt.get("title"),
                    "channelTitle": yt.get("uploader"),
                    "thumbnail": yt.get("thumbnail"),
                    "viewCount": yt.get("view_count"),
                    "likeCount": yt.get("like_count"),
                }
            )
            app.logger.info(
                "[Metadata] yt‑dlp for %s: views=%s", vid, base["viewCount"]
            )
        except Exception as e:
            app.logger.warning("[Metadata] yt‑dlp fail for %s: %s", vid, e)

    if not base["viewCount"]:
        app.logger.warning(
            "[Metadata] All providers lacked viewCount for %s – returning partial stub",
            vid,
        )

    return jsonify({"items": [base]}), 200


# ---------------- OpenAI RESPONSES POST (with Enhanced Logging) -----------
@app.route("/openai/responses", methods=["POST"])
def create_response():
    if not OPENAI_API_KEY:
        app.logger.error("[OpenAI Proxy /responses] OPENAI_API_KEY is not configured.")
        return jsonify({'error': 'OpenAI API key not configured'}), 500

    try:
        # Get the raw JSON payload sent by the iOS client
        payload = request.get_json()
        if not payload:
            app.logger.error("[OpenAI Proxy /responses] Received empty or invalid JSON payload.")
            return jsonify({'error': 'Invalid JSON payload'}), 400
    except Exception as json_err:
        app.logger.error(f"[OpenAI Proxy /responses] Failed to parse request JSON: {json_err}", exc_info=True)
        return jsonify({'error': 'Bad request JSON'}), 400

    # Prepare headers for the actual OpenAI API call
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    # Log the payload received from the client (excluding potentially large 'input')
    logged_payload = {k: v for k, v in payload.items() if k != 'input'}
    logged_payload['input_length'] = len(payload.get('input', ''))
    app.logger.info(f"[OpenAI Proxy /responses] Received request payload (excluding input): {json.dumps(logged_payload)}")
    app.logger.info(f"[OpenAI Proxy /responses] Calling OpenAI API (https://api.openai.com/v1/responses) -> Model: {payload.get('model', 'N/A')}")

    resp = None # Initialize resp to None
    try:
        # Make the POST request directly to OpenAI's /v1/responses endpoint
        resp = requests.post("https://api.openai.com/v1/responses",
                             headers=headers, json=payload, timeout=60) # Increased timeout

        app.logger.info(f"[OpenAI Proxy /responses] OpenAI API Response Status Code: {resp.status_code}")

        # Log response body especially if it's not 200 OK
        if resp.status_code != 200:
            response_text = resp.text[:1000] # Log first 1000 chars of error response
            app.logger.error(f"[OpenAI Proxy /responses] OpenAI API returned error {resp.status_code}. Response body: {response_text}")

        resp.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        # If successful, return the JSON directly from OpenAI
        response_data = resp.json()
        app.logger.info(f"[OpenAI Proxy /responses] Successfully received response from OpenAI.")

        # --- Log Response Structure ---
        # Log keys to understand the structure we get back from /v1/responses
        if isinstance(response_data, dict):
             app.logger.debug(f"[OpenAI Proxy /responses] Response Keys: {list(response_data.keys())}")
             # Try to log the specific fields your Swift code expects based on OpenAIProxyResponseDTO
             resp_id = response_data.get('id', 'N/A')
             output_text = response_data.get('output_text') # Check if SDK adds this convenience
             output_field = response_data.get('output')
             choices_field = response_data.get('choices')
             text_field = response_data.get('text')
             app.logger.debug(f"[OpenAI Proxy /responses] Response fields check: id='{resp_id}', output_text exists? {output_text is not None}, output exists? {output_field is not None}, choices exists? {choices_field is not None}, text exists? {text_field is not None}")
             # Log preview of nested content if possible
             content_preview = "N/A"
             if output_text:
                 content_preview = output_text[:200] + "..."
             elif isinstance(output_field, list) and output_field:
                 content_preview = str(output_field[0])[:200] + "..."
             elif isinstance(choices_field, list) and choices_field:
                 content_preview = str(choices_field[0])[:200] + "..."
             elif text_field:
                 content_preview = text_field[:200] + "..."
             app.logger.debug(f"[OpenAI Proxy /responses] Content Preview: {content_preview}")

        elif isinstance(response_data, list):
             app.logger.debug(f"[OpenAI Proxy /responses] Response is a List (length {len(response_data)}). First item preview: {str(response_data[0])[:200] if response_data else 'Empty List'}")
        else:
             app.logger.debug(f"[OpenAI Proxy /responses] Response type: {type(response_data)}. Preview: {str(response_data)[:200]}")
        # --- End Log Response Structure ---


        return jsonify(response_data), resp.status_code

    except requests.HTTPError as he:
        # Log the specific HTTP error from OpenAI
        err_msg = f"OpenAI API HTTP error: Status Code {he.response.status_code if he.response else 'N/A'}"
        err_details = resp.text[:1000] if resp else "No response object"
        app.logger.error(f"[OpenAI Proxy /responses] {err_msg}. Details: {err_details}", exc_info=True)
        clean_details = err_details
        try: # Try to parse standard OpenAI error format
            error_json = json.loads(err_details)
            if isinstance(error_json, dict) and "error" in error_json and isinstance(error_json["error"], dict) and "message" in error_json["error"]:
                 clean_details = error_json["error"]["message"]
        except: pass # Keep original details if parsing fails
        return jsonify({'error': 'OpenAI API error', 'details': clean_details}), he.response.status_code if he.response is not None else 500
    except requests.exceptions.RequestException as req_err:
        # Handle network errors (timeout, connection error, etc.)
        app.logger.error(f"[OpenAI Proxy /responses] Network error connecting to OpenAI: {req_err}", exc_info=True)
        return jsonify({'error': 'Network error communicating with OpenAI service'}), 503
    except Exception as e:
        # Catch any other unexpected errors
        app.logger.error(f"[OpenAI Proxy /responses] Unexpected error: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error during OpenAI request processing'}), 500


    
# ---------------- VADER SENTIMENT -----------------
analyzer = SentimentIntensityAnalyzer()

@app.route("/analyze/batch", methods=["POST"])
@limiter.limit("50 per minute")
def analyze_batch():
    app.logger.info("[VADER_BATCH] Received request.")
    try:
        texts = request.get_json(force=True) or []
        if not texts:
            app.logger.info("[VADER_BATCH] Empty text list received.")
            return jsonify([]), 200

        num_texts = len(texts)
        first_text_preview = texts[0][:50] if texts else "N/A"
        app.logger.info(f"[VADER_BATCH] Processing {num_texts} texts. First text preview: '{first_text_preview}...'")

        results = []
        start_time = time.time()
        for i, t in enumerate(texts):
            score = analyzer.polarity_scores(t)["compound"]
            results.append(score)
            # Optional: Log progress if needed, e.g., every 10 texts
            # if (i + 1) % 10 == 0:
            #    app.logger.debug(f"[VADER_BATCH] Processed {i+1}/{num_texts}...")

        end_time = time.time()
        duration = end_time - start_time
        app.logger.info(f"[VADER_BATCH] Successfully processed {num_texts} texts in {duration:.3f} seconds.")
        return jsonify(results), 200

    except Exception as e:
        app.logger.error(f"[VADER_BATCH] Error during batch processing: {e}", exc_info=True)
        return jsonify({"error": "Failed to process batch sentiment"}), 500
    

# ---------------- SIMPLE HEALTH CHECK ----------------
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({'status': 'ok'}), 200

# ---------------- FULL HEALTH CHECK ------------------
@app.route("/health/deep", methods=["GET"])
def deep_health_check():
    checks = {}
    # env
    checks['env'] = {
        'OPENAI_KEY': bool(OPENAI_API_KEY),
        'YOUTUBE_KEY': bool(YOUTUBE_DATA_API_KEY),
        'SMARTPROXY_TOKEN': bool(SMARTPROXY_API_TOKEN)
    }
    # external
    checks['external'] = {}
    try:
        r = session.get("https://api.openai.com/v1/models", timeout=5)
        checks['external']['openai_api'] = r.status_code == 200
    except Exception:
        checks['external']['openai_api'] = False
    try:
        r = session.get("https://www.googleapis.com/youtube/v3/videos",
                        params={'id':'dQw4w9WgXcQ','part':'id','key':YOUTUBE_DATA_API_KEY},
                        timeout=5)
        checks['external']['youtube_api'] = r.status_code == 200
    except Exception:
        checks['external']['youtube_api'] = False
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

