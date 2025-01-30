import os
import re
import time
import logging
import hashlib

from flask import Flask, request, jsonify
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound
)
from cachetools import TTLCache
from logging.handlers import RotatingFileHandler

# -----------------------------
# Flask App Initialization
# -----------------------------
app = Flask(__name__)

# -----------------------------
# Smartproxy Credentials
# -----------------------------
SMARTPROXY_USER = "spylrn4hi2"
SMARTPROXY_PASS = "f~6vgqT7bgqcVxYO73"
SMARTPROXY_HOST = "gate.smartproxy.com"
SMARTPROXY_PORT = "10001"

# Construct the "https" proxy string. The youtube_transcript_api library
# uses the 'requests' library's format for proxies.
PROXIES = {
    "https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{SMARTPROXY_PORT}"
}

# -----------------------------
# Transcript Cache (1 Hour TTL)
# -----------------------------
transcript_cache = TTLCache(maxsize=100, ttl=3600)

# -----------------------------
# Logging Configuration
# -----------------------------
if not os.path.exists('logs'):
    os.mkdir('logs')

file_handler = RotatingFileHandler(
    'logs/youtube_transcript.log',
    maxBytes=10_240,
    backupCount=10
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
)
file_handler.setLevel(logging.DEBUG)

app.logger.addHandler(file_handler)
app.logger.setLevel(logging.DEBUG)

# -----------------------------
# Helper: Extract Video ID
# -----------------------------
def extract_video_id(input_str: str) -> str:
    """
    If the user passed a full YouTube URL, extract the 11-char video ID.
    Otherwise return input as-is.
    This regex covers typical patterns like:
      - https://www.youtube.com/watch?v=XXXX
      - https://youtu.be/XXXX
    """
    # Basic pattern to capture the 11-character ID from typical YT URLs
    # If no match, assume user already provided an ID.
    match = re.search(r'(?:v=|\/)([0-9A-Za-z_-]{11})', input_str)
    if match:
        return match.group(1)
    return input_str

# -----------------------------
# Proxy-Only Fetch
# -----------------------------
def fetch_transcript_proxy(video_id: str, languages=None, preserve_format=False):
    """
    Fetch transcript from YouTube using the proxy, returning a list of transcript entries.
    Raises TranscriptsDisabled, NoTranscriptFound, or Exception on error.
    """
    if languages is None:
        languages = ['en']  # default to English if not provided

    # Preserve formatting if requested
    return YouTubeTranscriptApi.get_transcript(
        video_id,
        languages=languages,
        proxies=PROXIES,
        preserve_formatting=preserve_format
    )

# -----------------------------
# Flask Routes
# -----------------------------
@app.route('/transcript', methods=['GET'])
def get_transcript_endpoint():
    """
    GET /transcript?videoId=XYZ&language=en&preserveFormatting=true|false
    Always uses proxy on first request (no direct attempt).
    """
    raw_video_id = request.args.get('videoId', '').strip()
    user_language = request.args.get('language', 'en').strip()
    preserve_param = request.args.get('preserveFormatting', 'false').lower()

    if not raw_video_id:
        return jsonify({'error': 'Missing videoId parameter'}), 400

    # Extract or sanitize the video ID if a URL was passed
    video_id = extract_video_id(raw_video_id)

    # Convert preserveFormatting=... to a boolean
    preserve_format = (preserve_param == 'true')

    # Check cache
    cache_key = f"{video_id}:{user_language}:{preserve_format}"
    cached_result = transcript_cache.get(cache_key)
    if cached_result:
        app.logger.info(f"[Cache] HIT for {cache_key}")
        return jsonify(cached_result), 200

    # Language fallback: your preference
    # Put the requested language first, then fallback to a few defaults
    # (Adjust this list as desired)
    language_options = [user_language, 'en', 'es', 'fr', 'de', 'pt']

    # Attempt to fetch via Proxy
    app.logger.info(f"[Proxy Fetch] Attempting for video_id={video_id}, preserve={preserve_format}")
    try:
        transcript_data = fetch_transcript_proxy(
            video_id=video_id,
            languages=language_options,
            preserve_format=preserve_format
        )
    except (TranscriptsDisabled, NoTranscriptFound) as e:
        # Subtitles are not available or disabled
        app.logger.error(f"[Proxy Fetch] Subtitles unavailable for {video_id}. Error: {e}")
        return jsonify({'error': 'Subtitles are disabled/unavailable for this video'}), 404
    except Exception as e:
        # Some other failure
        app.logger.error(f"[Proxy Fetch] Unknown error for {video_id}: {e}")
        return jsonify({'error': 'Unable to fetch transcript', 'details': str(e)}), 500

    # Build response data
    response_data = {
        'status': 'success',
        'video_id': video_id,
        'transcript': ' '.join([entry['text'] for entry in transcript_data]),
        'preserve_formatting': preserve_format
    }

    # Cache the result
    transcript_cache[cache_key] = response_data
    app.logger.info(f"[Cache] MISS, storing new transcript for {cache_key}")

    return jsonify(response_data), 200


@app.route('/test', methods=['GET'])
def test_endpoint():
    """
    Health Check Endpoint
    """
    return jsonify({'status': 'ok', 'message': 'Service is running'}), 200


# -----------------------------
# Optional: An Endpoint to List Transcript Languages
# -----------------------------
@app.route('/available_transcripts', methods=['GET'])
def list_available_transcripts():
    """
    GET /available_transcripts?videoId=XYZ
    Returns the list of transcripts (including the type: generated/manually_created).
    Example usage from the docs: YouTubeTranscriptApi.list_transcripts(video_id)
    """
    raw_video_id = request.args.get('videoId', '').strip()
    if not raw_video_id:
        return jsonify({'error': 'Missing videoId parameter'}), 400

    video_id = extract_video_id(raw_video_id)

    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(
            video_id,
            proxies=PROXIES
        )
    except Exception as e:
        app.logger.error(f"[List Transcripts] Failed for {video_id}: {e}")
        return jsonify({'error': 'Unable to list transcripts', 'details': str(e)}), 500

    # Build a simple JSON representation
    transcripts_info = []
    for t in transcript_list:
        transcripts_info.append({
            'language': t.language,
            'language_code': t.language_code,
            'is_generated': t.is_generated,
            'is_translatable': t.is_translatable,
            'translation_languages': [lang['language'] for lang in t.translation_languages]
        })

    return jsonify({
        'video_id': video_id,
        'available_transcripts': transcripts_info
    }), 200


# -----------------------------
# Main Entry
# -----------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    app.run(host='0.0.0.0', port=port)
