import os
import re
import json
import logging
import time
import itertools
from functools import lru_cache
from collections import deque 
from concurrent.futures import ThreadPoolExecutor, TimeoutError

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

session = requests.Session()
session.request = functools.partial(session.request, timeout=10)
retry_cfg = Retry(total=2, backoff_factor=0.4,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
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

PROXY_ROTATION = (
    [
        {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10000"},
        {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10001"},
    ]
    if SMARTPROXY_USER else
    [{}]
)

PIPED_HOSTS = deque([
    "https://piped.video",
    "https://piped.video.proxycache.net",
    "https://piped.video.deno.dev",
])
INVIDIOUS_HOSTS = deque([
    "https://vid.puffyan.us",
    "https://ytdetail.8848.wtf",
])
_PIPE_COOLDOWN: dict[str, float] = {}
_IV_COOLDOWN: dict[str, float] = {}


def _fetch_json(hosts: deque, path: str,
                cooldown: dict[str, float],
                proxy_aware: bool = False,
                hard_deadline: float = 8.0):
    deadline = time.time() + hard_deadline
    for host in list(hosts):
        if time.time() >= deadline:
            break
        if cooldown.get(host, 0) > time.time():
            continue
        url = f"{host}{path}"
        proxy = PROXY_ROTATION[0] if proxy_aware else {}
        app.logger.info("[OUT] → %s", url)
        try:
            r = session.get(url, proxies=proxy, timeout=4)
            r.raise_for_status()
            if "application/json" not in r.headers.get("Content-Type", ""):
                raise ValueError("non-JSON body")
            return r.json()
        except Exception as e:
            app.logger.warning("Host %s failed: %s", host, e)
            cooldown[host] = time.time() + (600 if isinstance(e, ValueError) else 300)
    return None


_YDL_OPTS = {
    "quiet": True,
    "skip_download": True,
    "innertube_key": "AIzaSyA-DkzGi-tv79Q",
    **({"cookiefile": YTDL_COOKIE_FILE} if YTDL_COOKIE_FILE else {}),
}

def yt_dlp_info(video_id: str):
    with YoutubeDL(_YDL_OPTS) as ydl:
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
limiter = Limiter(app=app,
                  key_func=get_remote_address,
                  default_limits=["200 per hour", "50 per minute"])

# --------------------------------------------------
# Worker pool / cache
# --------------------------------------------------
transcript_cache = TTLCache(maxsize=500, ttl=600)           # 10 min
comment_cache    = TTLCache(maxsize=300, ttl=300)           # 5 min         # 10 min
executor         = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 4))

# --------------------------------------------------
# Allowed fallback languages
# --------------------------------------------------
FALLBACK_LANGUAGES = [
    'en','es','fr','de','pt','ru','hi','ar','zh-Hans','ja',
    'ko','it','nl','tr','vi','id','pl','th','sv','fi','he','uk','da','no'
]

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





# --------------------------------------------------
# Transcript helpers
# --------------------------------------------------
FALLBACK_LANGS = ['en','es','fr','de','pt','ru','hi','ar','zh-Hans','ja']
executor = ThreadPoolExecutor(max_workers=8)

def _get_transcript(vid: str, langs, preserve):
    return YouTubeTranscriptApi.get_transcript(
        vid, languages=langs, preserve_formatting=preserve,
        proxies=PROXY_ROTATION[0])

# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
@app.route("/transcript", methods=["GET"])
@limiter.limit("100/hour")
def get_transcript_endpoint():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error":"invalid_video_id"}), 400

    full = request.args.get("format","text").lower() == "full"
    key  = (vid, full)
    if (cached := transcript_cache.get(key)):
        return jsonify(cached), 200

    try:
        tr = executor.submit(_get_transcript, vid, FALLBACK_LANGS, full
                             ).result(timeout=15)
    except (TimeoutError, TranscriptsDisabled, NoTranscriptFound,
            CouldNotRetrieveTranscript):
        return jsonify({"status":"unavailable"}), 204
    except Exception as e:
        app.logger.error("Transcript err: %s", e)
        return jsonify({"error":"server"}), 500

    data = (
        {"segments":[{"text":x["text"],
                      "start":round(x["start"],2),
                      "duration":round(x["duration"],2)} for x in tr]}
        if full else
        {"text":" ".join(x["text"] for x in tr)}
    )
    transcript_cache[key] = data
    return jsonify(data), 200

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
@app.route("/youtube/comments")
def comments():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400
    if (cached := comment_cache.get(vid)):
        return jsonify({"comments": cached}), 200

    # 1) Piped
    js = _fetch_json(PIPED_HOSTS, f"/api/v1/comments/{vid}?cursor=0",
                     _PIPE_COOLDOWN)
    if js and (lst := [c["comment"] for c in js.get("comments", [])]):
        comment_cache[vid] = lst
        return jsonify({"comments": lst}), 200

    # 2) yt-dlp (only if cookies)
    if YTDL_COOKIE_FILE:
        try:
            yt = yt_dlp_info(vid)
            lst = [c["content"] for c in yt.get("comments", [])][:40]
            if lst:
                comment_cache[vid] = lst
                return jsonify({"comments": lst}), 200
        except Exception as e:
            app.logger.info("yt-dlp comments failed: %s", e)

    # 3) Invidious
    js = _fetch_json(INVIDIOUS_HOSTS,
                     f"/api/v1/comments/{vid}?sort_by=top",
                     _IV_COOLDOWN, proxy_aware=bool(SMARTPROXY_USER))
    if js and (lst := [c["content"] for c in js.get("comments", [])]):
        comment_cache[vid] = lst

    if not comment_cache.get(vid):
        app.logger.info("All comment sources failed for %s", vid)
    return jsonify({"comments": comment_cache.get(vid, [])}), 200

# ---------------- Metadata ---------------
@app.route("/youtube/metadata")
def metadata():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400

    # 1) oEmbed
    try:
        m = session.get("https://www.youtube.com/oembed",
                        params={"url": f"https://youtu.be/{vid}",
                                "format": "json"}, timeout=4).json()
        base = {
            "title"        : m["title"],
            "channelTitle" : m["author_name"],
            "thumbnail"    : m["thumbnail_url"],
            "thumbnailUrl" : m["thumbnail_url"],    # alias
            "duration"     : None,
            "viewCount"    : None,
            "likeCount"    : None,
            "videoId"      : vid,
        }
        return jsonify({"items":[base]}), 200
    except Exception as e:
        app.logger.info("oEmbed failed: %s", e)

    # 2) Piped
    js = _fetch_json(PIPED_HOSTS, f"/api/v1/streams/{vid}", _PIPE_COOLDOWN)
    if js:
        base = {
            "title"        : js.get("title"),
            "channelTitle" : js.get("uploader"),
            "thumbnail"    : js.get("thumbnailUrl"),
            "thumbnailUrl" : js.get("thumbnailUrl"),
            "duration"     : js.get("duration"),
            "viewCount"    : js.get("views"),
            "likeCount"    : js.get("likes"),
            "videoId"      : vid,
        }
        return jsonify({"items":[base]}), 200

    # 3) yt-dlp
    try:
        yt = yt_dlp_info(vid)
        base = {
            "title"        : yt.get("title"),
            "channelTitle" : yt.get("uploader"),
            "thumbnail"    : yt.get("thumbnail"),
            "thumbnailUrl" : yt.get("thumbnail"),
            "duration"     : yt.get("duration"),
            "viewCount"    : yt.get("view_count"),
            "likeCount"    : yt.get("like_count"),
            "videoId"      : vid,
        }
        return jsonify({"items":[base]}), 200
    except Exception as e:
        app.logger.error("Metadata fallback failed: %s", e)
        return jsonify({"items":[]}), 200


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

