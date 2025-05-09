
# ───────────────────────────────────────────────────── imports ──
import os, re, time, json, logging, functools
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from cachetools import TTLCache
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from youtube_transcript_api import (YouTubeTranscriptApi,
                                    TranscriptsDisabled, NoTranscriptFound)
from youtube_transcript_api._errors import CouldNotRetrieveTranscript
from yt_dlp import YoutubeDL

# ───────────────────────────────────────────────── HTTP base ──
session = requests.Session()
session.request = functools.partial(session.request, timeout=10)
retry_cfg = Retry(total=2, backoff_factor=0.4,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
session.mount("https://", HTTPAdapter(max_retries=retry_cfg))

# ───────────────────────────────────────────────── settings ──
APP_START = time.time()

SMARTPROXY_USER = os.getenv("SMARTPROXY_USER")
SMARTPROXY_PASS = os.getenv("SMARTPROXY_PASS")
SMARTPROXY_HOST = "gate.smartproxy.com"

PROXY_ROTATION = (
    [{"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10000"},
     {"https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10001"}]
    if SMARTPROXY_USER else [{}]
)

OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
YTDL_COOKIE_FILE = os.getenv("YTDL_COOKIE_FILE")  # optional

PIPED_HOSTS = deque(["https://piped.video",
                     "https://piped.video.proxycache.net",
                     "https://piped.video.deno.dev"])
INVIDIOUS_HOSTS = deque(["https://vid.puffyan.us",
                         "https://ytdetail.8848.wtf"])
_PIPE_CD: dict[str, float] = {}
_IV_CD  : dict[str, float] = {}

# ─────────────────────────────────────────────────── Flask ──
app = Flask(__name__)
CORS(app)
limiter = Limiter(app, key_func=get_remote_address,
                  default_limits=["200/hour", "40/minute"])

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")

@app.before_request
def _log():
    app.logger.info("[IN] %s %s ← %s",
                    request.method, request.path,
                    request.headers.get("X-Real-IP", request.remote_addr))

# ───────────────────────────────────── helper: host rotation ──
def _fetch_json(hosts: deque, path: str,
                cooldown: dict[str, float],
                proxy_aware: bool = False,
                deadline: float = 8.0):
    """Try the list of hosts until one returns valid JSON or we hit the deadline."""
    end = time.time() + deadline
    for host in list(hosts):
        if time.time() >= end:
            break
        if cooldown.get(host, 0) > time.time():
            continue
        url = f"{host}{path}"
        try:
            resp = session.get(url,
                               proxies=(PROXY_ROTATION[0] if proxy_aware else {}),
                               timeout=4)
            resp.raise_for_status()
            if "application/json" not in resp.headers.get("Content-Type", ""):
                raise ValueError("non‑JSON")
            return resp.json()
        except Exception as e:
            app.logger.warning("Host %s failed: %s", host, e)
            cooldown[host] = time.time() + (600 if isinstance(e, ValueError) else 300)
    return None

# ───────────────────────────────────────── yt-dlp helper ──
_YDL_OPTS = {"quiet": True, "skip_download": True,
             "innertube_key": "AIzaSyA-DkzGi-tv79Q"}
if YTDL_COOKIE_FILE:
    _YDL_OPTS["cookiefile"] = YTDL_COOKIE_FILE

def yt_dlp_info(vid: str):
    with YoutubeDL(_YDL_OPTS) as ydl:
        return ydl.extract_info(vid, download=False, process=False)

# ───────────────────────────────────────── utilities ──
ID_RE = re.compile(r"^[\w-]{11}$")
valid_id = lambda v: bool(ID_RE.fullmatch(v))

transcript_cache = TTLCache(maxsize=500, ttl=600)
comment_cache    = TTLCache(maxsize=300, ttl=300)

# ───────────────────────────────────────── endpoints ──
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

# ---------- comments ----------
@app.route("/youtube/comments")
def comments():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400
    if (c := comment_cache.get(vid)):
        return jsonify({"comments": c}), 200

    js = _fetch_json(PIPED_HOSTS, f"/api/v1/comments/{vid}?cursor=0", _PIPE_CD)
    if js and (lst := [c["comment"] for c in js.get("comments", [])]):
        comment_cache[vid] = lst
        return jsonify({"comments": lst}), 200

    if YTDL_COOKIE_FILE:
        try:
            yt = yt_dlp_info(vid)
            lst = [c["content"] for c in yt.get("comments", [])][:40]
            if lst:
                comment_cache[vid] = lst
                return jsonify({"comments": lst}), 200
        except Exception as e:
            app.logger.info("yt‑dlp comments: %s", e)

    js = _fetch_json(INVIDIOUS_HOSTS,
                     f"/api/v1/comments/{vid}?sort_by=top",
                     _IV_CD, proxy_aware=bool(SMARTPROXY_USER))
    if js and (lst := [c["content"] for c in js.get("comments", [])]):
        comment_cache[vid] = lst

    return jsonify({"comments": comment_cache.get(vid, [])}), 200

# ---------- metadata ----------
@app.route("/youtube/metadata")
def metadata():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400
    try:
        m = session.get("https://www.youtube.com/oembed",
                        params={"url": f"https://youtu.be/{vid}",
                                "format": "json"}, timeout=4).json()
        return jsonify({"items": [{
            "title": m["title"], "channelTitle": m["author_name"],
            "thumbnail": m["thumbnail_url"], "thumbnailUrl": m["thumbnail_url"],
            "duration": None, "viewCount": None, "likeCount": None,
            "videoId": vid
        }]}), 200
    except Exception as e:
        app.logger.info("oEmbed fail: %s", e)

    js = _fetch_json(PIPED_HOSTS, f"/api/v1/streams/{vid}", _PIPE_CD)
    if js:
        return jsonify({"items": [{
            "title": js.get("title"), "channelTitle": js.get("uploader"),
            "thumbnail": js.get("thumbnailUrl"), "thumbnailUrl": js.get("thumbnailUrl"),
            "duration": js.get("duration"), "viewCount": js.get("views"),
            "likeCount": js.get("likes"), "videoId": vid
        }]}), 200

    try:
        yt = yt_dlp_info(vid)
        return jsonify({"items": [{
            "title": yt.get("title"), "channelTitle": yt.get("uploader"),
            "thumbnail": yt.get("thumbnail"), "thumbnailUrl": yt.get("thumbnail"),
            "duration": yt.get("duration"), "viewCount": yt.get("view_count"),
            "likeCount": yt.get("like_count"), "videoId": vid
        }]}), 200
    except Exception as e:
        app.logger.error("metadata fallback: %s", e)
        return jsonify({"items": []}), 200

# ---------- transcript ----------
LANGS = ['en','es','fr','de','pt','ru','ar','hi','zh-Hans','ja']
executor = ThreadPoolExecutor(max_workers=8)
def _tx(vid, full):
    return YouTubeTranscriptApi.get_transcript(
        vid, languages=LANGS, preserve_formatting=full,
        proxies=PROXY_ROTATION[0])

@app.route("/transcript")
@limiter.limit("60/hour")
def transcript():
    vid = request.args.get("videoId", "")
    if not valid_id(vid):
        return jsonify({"error": "invalid_video_id"}), 400
    full = request.args.get("format", "text").lower() == "full"
    key = (vid, full)
    if (c := transcript_cache.get(key)):
        return jsonify(c), 200
    try:
        tr = executor.submit(_tx, vid, full).result(timeout=15)
    except (TimeoutError, TranscriptsDisabled, NoTranscriptFound,
            CouldNotRetrieveTranscript):
        return jsonify({"status": "unavailable"}), 204
    except Exception as e:
        app.logger.error("Transcript err: %s", e)
        return jsonify({"error": "server"}), 500
    data = {"segments": [{"text": t["text"],
                            "start": round(t["start"],2),
                            "duration": round(t["duration"],2)} for t in tr]}            if full else {"text": " ".join(t["text"] for t in tr)}
    transcript_cache[key] = data
    return jsonify(data), 200

# ---------- VADER batch ----------
analyzer = SentimentIntensityAnalyzer()
@app.route("/analyze/batch", methods=["POST"])
@limiter.limit("50/minute")
def vader_batch():
    txts = request.get_json(force=True) or []
    if not isinstance(txts, list):
        return jsonify({"error": "bad_payload"}), 400
    return jsonify([analyzer.polarity_scores(t)["compound"] for t in txts]), 200

# ---------- OpenAI proxy ----------
@app.route("/openai/responses", methods=["POST"])
def openai_proxy():
    if not OPENAI_API_KEY:
        return jsonify({"error": "OPENAI key missing"}), 500
    payload = request.get_json(silent=True) or {}
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}",
               "Content-Type": "application/json"}
    r = requests.post("https://api.openai.com/v1/responses",
                      headers=headers, json=payload, timeout=60)
    return jsonify(r.json()), r.status_code

# ---------- deep health ----------
@app.route("/health/deep")
def deep():
    return jsonify({"status": "ok", "uptime": int(time.time()-APP_START)}), 200

# ─────────────────────────────────────────── run ──
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5010)), threaded=True)
