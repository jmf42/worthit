import os
import logging
import re
import html
import tempfile
import pathlib
import itertools
import random
from typing import List, Optional

import requests
from flask import Flask, request, jsonify
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    RequestBlocked,
    AgeRestricted,
)
from youtube_transcript_api.proxies import GenericProxyConfig
from youtube_transcript_api._errors import CouldNotRetrieveTranscript
from yt_dlp import YoutubeDL

DISABLE_DIRECT = os.getenv("TRANSCRIPT_DIRECT_ATTEMPT", "false").lower() == "true"
MAX_PROXY_ATTEMPTS = int(os.getenv("TRANSCRIPT_PROXY_ATTEMPTS", "4"))

# ---------------------------------------------------------------------------
# Basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("TranscriptService")

# ---------------------------------------------------------------------------
# SmartProxy / Decodo rotating proxy pool -----------------------------------

SP_USER = os.getenv("SMARTPROXY_USER")
SP_PASS = os.getenv("SMARTPROXY_PASS")
SP_HOST = "gate.decodo.com"

ROTATING_PORT = 7000            # Decodo “rotating” port for fresh residential IPs
STICKY_PORTS: list[int] = []   # no sticky ports for YouTube

PROXIES: List[str] = []

if SP_USER and SP_PASS:
    encoded_pass = requests.utils.quote(SP_PASS, safe="")
    PROXIES = [
        f"http://{SP_USER}:{encoded_pass}@{SP_HOST}:{ROTATING_PORT}"
    ]
    logger.info("Loaded %d Decodo proxy endpoints: %s", len(PROXIES), PROXIES)
else:
    logger.info("SmartProxy credentials not set – running direct-only mode")

# deterministic cycle through proxies
_PROXY_CYCLE = itertools.cycle(PROXIES) if PROXIES else None

def next_proxy() -> Optional[str]:
    if not _PROXY_CYCLE:
        return None  # direct
    return next(_PROXY_CYCLE)

# helper to convert raw URL into GenericProxyConfig (runtime-safe)

def make_proxy_cfg(url: str) -> GenericProxyConfig:
    # GenericProxyConfig signature changed around v1.0 – test attributes
    try:
        return GenericProxyConfig(https_url=url, http_url=url)
    except TypeError:
        return GenericProxyConfig(https=url, http=url)  # legacy fallback

# ---------------------------------------------------------------------------
# Helper – validate YouTube video IDs (11 chars)
_VIDEO_ID_RE = re.compile(r"^[\w-]{11}$")

def valid_vid(vid: str) -> bool:
    return bool(_VIDEO_ID_RE.fullmatch(vid))

# ---------------------------------------------------------------------------
# Primary fetch via youtube-transcript-api (v1.x compliant)

def fetch_api_once(video_id: str, proxy_url: Optional[str], timeout: int = 10,
                   languages: Optional[List[str]] = None) -> Optional[str]:
    languages = languages or ["en", "es"]
    proxy_cfg = make_proxy_cfg(proxy_url) if proxy_url else None

    # list_transcripts parameters vary across versions: filter dynamically
    from inspect import signature
    list_sig = signature(YouTubeTranscriptApi.list_transcripts)
    kwargs = {"video_id": video_id, "proxy_config": proxy_cfg, "timeout": timeout}
    filtered = {k: v for k, v in kwargs.items() if k in list_sig.parameters}

    tl = YouTubeTranscriptApi.list_transcripts(**filtered)

    # prefer manually provided captions, then autos
    for finder in (tl.find_transcript, tl.find_generated_transcript):
        try:
            tr = finder(languages)
        except NoTranscriptFound:
            continue
        # fetch(); if v1.x returns FetchedTranscript convert to raw
        segments = tr.fetch()
        if hasattr(segments, "to_raw_data"):
            segments = segments.to_raw_data()
        text = " ".join(
            seg.get("text") if isinstance(seg, dict) else getattr(seg, "text", "")
            for seg in segments
        ).strip()
        if text:
            return text
    return None

# ---------------------------------------------------------------------------
# yt-dlp subtitle fallback ---------------------------------------------------

def fetch_ytdlp(video_id: str, proxy_url: Optional[str]) -> Optional[str]:
    logger.info("[yt-dlp] Attempt via %s", proxy_url or "direct")
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
            return " ".join(html.unescape(t) for t in re.findall(r">([^<]+)</text>", xml))
        except Exception as e:
            logger.warning("yt-dlp failed: %s", e)
            return None

# ---------------------------------------------------------------------------
# Shuffle proxies on each request to avoid being rate limited or blocked by endpoint reuse.
def get_transcript(video_id: str, max_attempts: int = 0) -> str:
    # scale attempts to proxy count if caller passes max_attempts=0
    if max_attempts == 0:
        max_attempts = MAX_PROXY_ATTEMPTS or ((1 if not DISABLE_DIRECT else 0) + len(PROXIES))

    attempts = [] if DISABLE_DIRECT else [None]      # None = direct attempt
    # add enough proxies to reach max_attempts
    room = max_attempts - len(attempts)
    if room > 0 and PROXIES:
        attempts += random.sample(PROXIES, min(room, len(PROXIES)))

    random.shuffle(attempts)
    # 1️⃣ try youtube-transcript-api
    for idx, p_url in enumerate(attempts, 1):
        try:
            logger.info("[API] Attempt %d/%d via %s", idx, len(attempts), p_url or "direct")
            txt = fetch_api_once(video_id, p_url)
            if txt:
                return txt
        except (RequestBlocked, CouldNotRetrieveTranscript, VideoUnavailable,
                AgeRestricted, TranscriptsDisabled) as e:
            logger.warning("Blocked on %s: %s", p_url or "direct", e.__class__.__name__)
            continue
        except Exception as e:
            logger.error("Unexpected API error (%s): %s", p_url or "direct", e)
            continue

    # 2️⃣ fallback to yt-dlp
    for p_url in attempts:  # reuse same endpoint order
        txt = fetch_ytdlp(video_id, p_url)
        if txt:
            return txt
    raise NoTranscriptFound(video_id, [], None)

# ---------------------------------------------------------------------------
# Flask setup ----------------------------------------------------------------
app = Flask(__name__)

@app.route("/transcript")
def transcript_ep():
    vid = request.args.get("videoId", "").strip()
    if not vid or not valid_vid(vid):
        return jsonify({"error": "Invalid or missing videoId"}), 400
    try:
        text = get_transcript(vid)
        return jsonify({"video_id": vid, "text": text}), 200
    except NoTranscriptFound:
        return jsonify({"error": "Transcript not available"}), 404
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route("/")
def root():
    return {"status": "ok"}, 200

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5020))
    logger.info("Starting on :%d", port)
    app.run(host="0.0.0.0", port=port, threaded=True)
# ===========================================================================
