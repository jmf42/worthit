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

# ---------------------------------------------------------------------------
# Basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("TranscriptService")
DISABLE_DIRECT = os.getenv("FORCE_PROXY", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Webshare rotating residential gateway
WS_USER = os.getenv("WEBSHARE_USER")
WS_PASS = os.getenv("WEBSHARE_PASS")
WS_HOST = os.getenv("WEBSHARE_HOST", "p.webshare.io")

# Ensure -rotate suffix on proxy username
if WS_USER and not WS_USER.endswith("-rotate"):
    WS_USER = f"{WS_USER}-rotate"

PROXY_CFG = None
if WS_USER and WS_PASS:
    quoted_pass = requests.utils.quote(WS_PASS, safe="")
    # Webshare rotating residential gateway host
    proxy_url = f"http://{WS_USER}:{quoted_pass}@p.webshare.io:80"
    PROXY_CFG = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
    logger.info("Using Webshare rotating residential proxies via %s", proxy_url)
else:
    logger.info("No Webshare credentials – requests will go direct")

# ---------------------------------------------------------------------------
# Helper – validate YouTube video IDs (11 chars)
_VIDEO_ID_RE = re.compile(r"^[\w-]{11}$")

def valid_vid(vid: str) -> bool:
    return bool(_VIDEO_ID_RE.fullmatch(vid))

# ---------------------------------------------------------------------------
# Primary fetch via youtube-transcript-api (v1.x compliant)

def fetch_api_once(video_id: str, proxy_cfg, timeout: int = 10,
                   languages: Optional[List[str]] = None) -> Optional[str]:
    languages = languages or ["en", "es"]

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
def get_transcript(video_id: str, max_attempts: int = 4) -> str:
    try:
        text = fetch_api_once(video_id, PROXY_CFG)
        if text:
            return text
    except (RequestBlocked, CouldNotRetrieveTranscript,
            VideoUnavailable, AgeRestricted, TranscriptsDisabled) as e:
        logger.warning("API blocked: %s", e.__class__.__name__)
    except Exception as e:
        logger.error("Unexpected API error: %s", e)

    proxy_url = None
    if WS_USER and WS_PASS:
        quoted = requests.utils.quote(WS_PASS, safe="")
        proxy_url = f"http://{WS_USER}:{quoted}@{WS_HOST}:80"

    text = fetch_ytdlp(video_id, proxy_url)
    if text:
        return text
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
