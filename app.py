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
from youtube_transcript_api.proxies import WebshareProxyConfig
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

PROXY_CFG = None
if WS_USER and WS_PASS:
    if not WS_USER.endswith("-rotate"):
        WS_USER = f"{WS_USER}-rotate"
    PROXY_CFG = WebshareProxyConfig(
        proxy_username=WS_USER,
        proxy_password=WS_PASS
    )
    logger.info("Using Webshare rotating residential proxies (username=%s)", WS_USER)
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
    """
    One shot via youtube‑transcript‑api using the provided proxy config.
    Newer versions return a FetchedTranscript object, older ones a list[dict].
    """
    languages = languages or ["en", "es"]

    # youtube‑transcript‑api >= 1.1.0 does not accept "timeout" in the constructor.
    # Instead pass it to each call of .fetch(...)
    ytt_api = YouTubeTranscriptApi(proxy_config=proxy_cfg)
    try:
        ft = ytt_api.fetch(video_id, languages=languages)
    except NoTranscriptFound:
        return None

    # unify to plain text string
    if hasattr(ft, "to_raw_data"):
        segments = ft.to_raw_data()
    else:
        segments = ft
    return " ".join(
        seg["text"] if isinstance(seg, dict) else getattr(seg, "text", "")
        for seg in segments
    ).strip() or None

# ---------------------------------------------------------------------------
# yt-dlp subtitle fallback ---------------------------------------------------

def fetch_ytdlp(video_id: str, proxy_url: Optional[str]) -> Optional[str]:
    logger.info("[yt-dlp] Attempt via proxy")
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

    text = fetch_ytdlp(video_id, None)   # yt-dlp goes direct; it will likely fail on age‑gated vids
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
