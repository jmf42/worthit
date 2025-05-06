import os
import re
import logging
import time
import itertools
from functools import lru_cache
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
import functools

session = requests.Session()
# Configure default timeouts
session.request = functools.partial(session.request, timeout=15)
# Add retries with exponential backoff
retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))

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

PROXIES = {
    "https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{SMARTPROXY_PORT}"
}
PROXY_CONFIGS = [
    {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10000"},
    {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10001"}
]

# -# --------------------------------------------------
# Invidious helper (host rotation + Retry-After)
# --------------------------------------------------
INVIDIOUS_HOSTS = os.getenv(
    "INVIDIOUS_HOSTS",
    "https://ytdetail.8848.wtf,https://piped.video,https://vid.puffyan.us"
).split(",")

invidious_cursor = itertools.cycle(INVIDIOUS_HOSTS)

def _next_host() -> str:
    return next(invidious_cursor).rstrip("/")

# In invidious_api function:
def invidious_api(path: str, *, max_retries: int = 4, proxy_round_robin: bool = True):
    """
    Generic helper for Invidious requests.
    Rotates host *and* SmartProxy exit IP to minimise rate-limits.
    """
    delay = 0.5
    for attempt in range(max_retries + 1):
        host  = _next_host()
        proxy = PROXY_CONFIGS[attempt % len(PROXY_CONFIGS)] if proxy_round_robin else {}
        url   = f"{host}{path}"

        local_session            = requests.Session()
        local_session.request    = functools.partial(local_session.request, timeout=15)
        retries                  = Retry(total=2, backoff_factor=0.5,
                                         status_forcelist=[429, 500, 502, 503, 504],
                                         allowed_methods=["GET"])
        local_session.mount("https://", HTTPAdapter(max_retries=retries))

        app.logger.info("[OUT] Invidious → %s (proxy=%s)", url, bool(proxy))
        try:
            resp = local_session.get(url, proxies=proxy, timeout=10)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", delay))
                app.logger.warning("429 from %s – wait %.1fs (try %d/%d)", host, wait, attempt+1, max_retries)
                time.sleep(wait); delay = min(delay*2, 8); continue
            resp.raise_for_status()
            app.logger.info("[OUT] Invidious ← OK (%s)", host)
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            app.logger.warning("[OUT] Invidious timeout (%s): %s", host, e)
            time.sleep(delay); delay = min(delay*1.5, 4); continue
        except Exception as e:
            app.logger.error("[OUT] Invidious FAILED (%s): %s", host, e)
            if attempt == max_retries:
                raise

# --------------------------------------------------
# Flask init
# --------------------------------------------------
app = Flask(__name__)
CORS(app)

# ── Minimal inbound access log ────────────────────
@app.before_request
def _log_request():
    app.logger.info(
        "[INBOUND] %s %s ← %s",
        request.method,
        request.path,
        request.headers.get("X-Real-IP", request.remote_addr)
    )

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


# ADD just under the helper
def _yt_style_from_invidious(vid: str, raw: dict) -> dict:
    """Convert Invidious /videos response to the Google Data API shape the app already parses."""
    thumbs = raw.get("videoThumbnails", [])
    high   = next((t for t in thumbs if t.get("quality") == "high"), thumbs[0] if thumbs else {})
    return {
        "items": [{
            "id": vid,
            "snippet": {
                "title":         raw.get("title", ""),
                "description":   raw.get("description", ""),
                "channelTitle":  raw.get("author", ""),
                "thumbnails":    { "high": { "url": high.get("url", "") } }
            },
            "contentDetails": { "duration": f"PT{raw.get('lengthSeconds', 0)}S" },
            "statistics": {
                "viewCount": str(raw.get("viewCount", 0)),
                "likeCount": str(raw.get("likeCount", 0)),
                "commentCount": str(raw.get("commentCount", 0)),
            }
        }]
    }


# --------------------------------------------------
# Transcript fetch helper (with retry + logging)
# --------------------------------------------------
def fetch_transcript_with_retry(video_id: str,
                                languages: list,
                                preserve_format: bool,
                                retries: int = 2):
    app.logger.info("[OUT] YouTubeTranscriptAPI → %s", video_id)
    for attempt in range(retries + 1):
        proxy_cfg = PROXY_CONFIGS[attempt % len(PROXY_CONFIGS)]
        try:
            tr = YouTubeTranscriptApi.get_transcript(
                video_id,
                languages=languages if attempt == 0 else ['*'],
                proxies=proxy_cfg,
                preserve_formatting=preserve_format
            )
            app.logger.info("[OUT] YouTubeTranscriptAPI ← ok (attempt %s)", attempt + 1)
            return tr
        except Exception as e:
            if attempt == retries:
                app.logger.error("[OUT] YouTubeTranscriptAPI FAILED: %s", e)
                raise
            time.sleep(0.5 * (attempt + 1))

# --------------------------------------------------
# Small utils
# --------------------------------------------------
def generate_cache_key(video_id, languages, preserve_format, return_full):
    return hashkey(video_id, languages, preserve_format, return_full)

def process_transcript(tr, full):
    if full:
        return {'segments': [
            {'text': x['text'],
             'start': round(x['start'], 2),
             'duration': round(x['duration'], 2)} for x in tr]}
    return {'text': ' '.join(x['text'] for x in tr)}

# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
@app.route("/transcript", methods=["GET"])
@limiter.limit("100/hour")
def get_transcript_endpoint():
    try:
        raw_id = request.args.get("videoId", "").strip()
        if not raw_id:
            return jsonify({'error': 'Missing videoId parameter'}), 400
        try:
            vid = extract_video_id(raw_id)
        except ValueError:
            return jsonify({'error': 'Invalid YouTube URL or video ID'}), 400

        langs_in  = [l.strip().lower() for l in request.args.get('language', 'en').split(',') if l.strip()]
        preserve  = request.args.get('preserveFormatting', 'false').lower() == 'true'
        returnfull = request.args.get('format', 'text').lower() == 'full'
        lang_priority = list(dict.fromkeys(langs_in + FALLBACK_LANGUAGES))

        key = generate_cache_key(vid, tuple(lang_priority), preserve, returnfull)
        if (cached := transcript_cache.get(key)):
            app.logger.info("Cache hit for %s", vid)
            return jsonify(cached), 200

        fut = executor.submit(fetch_transcript_with_retry, vid, lang_priority, preserve)
        tr  = fut.result(timeout=15)

        response = { 'status':'success',
                    'video_id':vid,
                    'detected_language': tr[0].get('language', 'unknown'),
                    **process_transcript(tr, returnfull) }
        transcript_cache[key] = response
        return jsonify(response), 200

    except (TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript, TimeoutError):
        # 👍 treat as *expected* absence instead of 5xx
        app.logger.info("Transcript unavailable for %s", vid)
        return jsonify({'status':'unavailable', 'video_id':vid}), 204

    except Exception as e:
        app.logger.error("Transcript endpoint error: %s", e, exc_info=True)
        return jsonify({'error':'Internal server error'}), 500

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

# ---------------- YouTube COMMENTS ----------------

@app.route("/youtube/comments", methods=["GET"])
def proxy_youtube_comments():
    vid = request.args.get("videoId")
    if not vid: return jsonify({'error': 'Missing videoId parameter'}), 400

    # 1️⃣  Invidious first
    try:
        data = invidious_api(f"/api/v1/comments/{vid}?sort_by=top")
        comments = [c.get("content", "") for c in data.get("comments", [])]
        if comments:
            app.logger.info("Comments fetched via Invidious (%d) for %s", len(comments), vid)
            return jsonify({'comments': comments}), 200
        app.logger.warning("Invidious returned 0 comments for %s", vid)
    except Exception as inv_err:
        app.logger.error("Invidious comments failed for %s: %s", vid, inv_err)

    # 2️⃣  Google fallback (quota heavy)
    if not YOUTUBE_DATA_API_KEY:
        return jsonify({'error': 'Comments unavailable (Invidious down & Google key missing)'}), 503
    params = { 'part':'snippet', 'videoId':vid, 'maxResults':30,
               'order':'relevance', 'key':YOUTUBE_DATA_API_KEY }
    try:
        r = session.get("https://www.googleapis.com/youtube/v3/commentThreads", params=params, timeout=10)
        r.raise_for_status()
        items = r.json().get('items', [])
        comments = [i['snippet']['topLevelComment']['snippet']['textOriginal']
                    for i in items if 'snippet' in i]
        return jsonify({'comments': comments}), 200
    except Exception as g_err:
        app.logger.error("Google comments fallback failed for %s: %s", vid, g_err)
        return jsonify({'error':'Comments service unavailable'}), 503
# ---------------- YouTube METADATA ----------------

@app.route("/youtube/metadata", methods=["GET"])
def proxy_youtube_metadata():
    vid = request.args.get("videoId")
    if not vid:                     return jsonify({'error':'Missing videoId parameter'}), 400

    # 1️⃣  Invidious → primary path
    try:
        raw = invidious_api(f"/api/v1/videos/{vid}")
        app.logger.info("Metadata fetched via Invidious for %s", vid)
        return jsonify(_yt_style_from_invidious(vid, raw)), 200
    except Exception as inv_err:
        app.logger.error("Invidious metadata failed for %s: %s", vid, inv_err)

    # 2️⃣  Google → only if key present *and* Invidious failed
    if not YOUTUBE_DATA_API_KEY:
        return jsonify({'error': 'Metadata unavailable (Invidious down & Google key missing)'}), 503

    params = { 'part':'snippet,contentDetails,statistics', 'id':vid, 'key':YOUTUBE_DATA_API_KEY }
    try:
        r = session.get("https://www.googleapis.com/youtube/v3/videos", params=params, timeout=10)
        r.raise_for_status()
        resp = make_response(r.content, r.status_code)
        resp.headers['Content-Type'] = 'application/json'
        return resp
    except Exception as g_err:
        app.logger.error("Google fallback failed for %s: %s", vid, g_err)
        return jsonify({'error':'Metadata service unavailable'}), 503

# ---------------- OpenAI RESPONSES POST -----------
@app.route("/openai/responses", methods=["POST"])
def create_response():
    if not OPENAI_API_KEY:
        return jsonify({'error':'OpenAI API key not configured'}), 500
    payload = request.get_json()
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}",
               "Content-Type": "application/json"}

    app.logger.info("[OUT] OpenAI createResponse")
    try:
        resp = requests.post("https://api.openai.com/v1/responses",
                             headers=headers, json=payload, timeout=30)
        app.logger.info("[OUT] OpenAI ← %s (create)", resp.status_code)
        resp.raise_for_status()
        return jsonify(resp.json()), resp.status_code
    except requests.HTTPError as he:
        app.logger.error("OpenAI HTTP error: %s – %s", he, resp.text)
        return jsonify({'error':'OpenAI API error','details':resp.text}), resp.status_code
    except Exception as e:
        app.logger.error("OpenAI create error: %s", e)
        return jsonify({'error':'OpenAI service unavailable'}), 503

# ---------------- OpenAI RESPONSES GET -----------
@app.route("/openai/responses/<response_id>", methods=["GET"])
def get_response(response_id):
    if not OPENAI_API_KEY:
        return jsonify({'error':'OpenAI API key not configured'}), 500
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    app.logger.info("[OUT] OpenAI getResponse %s", response_id)
    try:
        resp = requests.get(f"https://api.openai.com/v1/responses/{response_id}",
                            headers=headers, timeout=10)
        app.logger.info("[OUT] OpenAI ← %s (get)", resp.status_code)
        resp.raise_for_status()
        return jsonify(resp.json()), resp.status_code
    except requests.HTTPError as he:
        app.logger.error("OpenAI GET error: %s – %s", he, resp.text)
        return jsonify({'error':'OpenAI API error','details':resp.text}), resp.status_code
    except Exception as e:
        app.logger.error("OpenAI get error: %s", e)
        return jsonify({'error':'OpenAI service unavailable'}), 503

# ---------------- VADER SENTIMENT -----------------
analyzer = SentimentIntensityAnalyzer()

@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.json or {}
    text = data.get("text", "")
    if not text:
        return jsonify({'error':'Text is required'}), 400
    app.logger.info("[VADER] textlen=%d", len(text))
    scores = analyzer.polarity_scores(text)
    app.logger.info("[VADER] scores=%s", scores)
    return jsonify(scores), 200

@app.route("/analyze/batch", methods=["POST"])
@limiter.limit("50 per minute")
def analyze_batch():
    texts = request.get_json(force=True) or []
    results = [analyzer.polarity_scores(t)["compound"] for t in texts]
    return jsonify(results), 200

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
