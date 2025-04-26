import os
import re
import logging
import time
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from flask import Flask, request, jsonify
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound
)
from cachetools import TTLCache
from cachetools.keys import hashkey
from logging.handlers import RotatingFileHandler
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests




# --------------------------------------------------
# Hard-coded Smartproxy & API configuration
# --------------------------------------------------
# (Replace these values with your actual keys)
SMARTPROXY_USER = os.getenv("SMARTPROXY_USER")
SMARTPROXY_PASS = os.getenv("SMARTPROXY_PASS")
SMARTPROXY_HOST = "gate.smartproxy.com"
# Use port 10000 for rotating proxies (10001 is for sticky sessions)
SMARTPROXY_PORT = "10000"
SMARTPROXY_API_TOKEN = os.getenv("SMARTPROXY_API_TOKEN")

# Define PROXIES for Smartproxy usage
PROXIES = {
    "https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{SMARTPROXY_PORT}"
}

# Define alternative proxy configurations for rotation
PROXY_CONFIGS = [
    {
        "https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10000"
    },
    {
        "https": f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:10001"
    }
]


# Create a persistent session for Smartproxy requests.
session = requests.Session()
session.headers.update({
    "accept": "application/json",
    "content-type": "application/json",
    "Authorization": f"Token {SMARTPROXY_API_TOKEN}"
})


# --------------------------------------------------
# Flask App Initialization
# --------------------------------------------------
app = Flask(__name__)
CORS(app)

# --------------------------------------------------
# Rate Limiting
# --------------------------------------------------
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per hour", "50 per minute"]
)

# --------------------------------------------------
# Cache & Thread Configuration
# --------------------------------------------------
# Cache transcripts for 10 minutes (600 seconds) for freshness
transcript_cache = TTLCache(maxsize=500, ttl=600)
# Use up to min(32, CPU_count * 4) workers.
executor = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 4))

# --------------------------------------------------
# Language Configuration
# --------------------------------------------------
FALLBACK_LANGUAGES = [
    'en', 'es', 'fr', 'de', 'pt', 'ru', 'hi',
    'ar', 'zh-Hans', 'ja', 'ko', 'it', 'nl', 'tr', 'vi',
    'id', 'pl', 'th', 'sv', 'fi', 'he', 'uk', 'da', 'no'
]

# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
if not os.path.exists('logs'):
    os.mkdir('logs')

file_handler = RotatingFileHandler(
    'logs/youtube_transcript.log',
    maxBytes=5_242_880,
    backupCount=3,
    encoding='utf-8'
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s %(levelname)s [%(module)s:%(lineno)d]: %(message)s')
)
file_handler.setLevel(logging.INFO)

app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

# --------------------------------------------------
# Video ID Validation
# --------------------------------------------------
VIDEO_ID_REGEX = re.compile(r'^[\w-]{11}$')

def validate_video_id(video_id: str) -> bool:
    return bool(VIDEO_ID_REGEX.fullmatch(video_id))

# --------------------------------------------------
# Enhanced Video ID Extraction
# --------------------------------------------------
@lru_cache(maxsize=1024)
def extract_video_id(input_str: str) -> str:
    patterns = [
        r'(?:v=|\/)([\w-]{11})',  # Standard URLs
        r'^([\w-]{11})$'          # Direct ID
    ]
    
    for pattern in patterns:
        match = re.search(pattern, input_str)
        if match and validate_video_id(match.group(1)):
            return match.group(1)
    
    raise ValueError("Invalid YouTube URL or video ID")

# --------------------------------------------------
# Enhanced Async Transcript Fetching with Retry and Fallback
# --------------------------------------------------
def fetch_transcript_with_retry(video_id: str, languages: list, preserve_format: bool, retries=2):
    for attempt in range(retries + 1):
        proxy_config = PROXY_CONFIGS[attempt % len(PROXY_CONFIGS)]
        try:
            # Always use proxy from the first attempt
            transcript = YouTubeTranscriptApi.get_transcript(
                video_id,
                languages=languages if attempt == 0 else ['*'],
                proxies=proxy_config,
                preserve_formatting=preserve_format
            )
            app.logger.info(f"Successfully fetched transcript for {video_id} on attempt {attempt+1} with language: {transcript[0].get('language', 'unknown')}")
            return transcript
        except TranscriptsDisabled as e:
            app.logger.error(f"Transcripts disabled for video {video_id}: {e}")
            if attempt == retries:
                raise
        except NoTranscriptFound as e:
            app.logger.error(f"No transcript found for video {video_id} in languages {languages if attempt == 0 else ['*']}: {e}")
            if attempt == retries:
                raise
        except Exception as e:
            app.logger.error(f"Error fetching transcript for {video_id} (attempt {attempt+1}): {e}")
            if attempt == retries:
                raise
            time.sleep(0.5 * (attempt + 1))
    return None

# --------------------------------------------------
# Cache Key Generation
# --------------------------------------------------
def generate_cache_key(video_id: str, languages: tuple, preserve_format: bool, return_full: bool):
    return hashkey(video_id, languages, preserve_format, return_full)

# --------------------------------------------------
# Transcript Processing
# --------------------------------------------------
def process_transcript(transcript_data: list, return_full: bool) -> dict:
    if return_full:
        return {
            'segments': [
                {
                    'text': entry['text'],
                    'start': round(entry['start'], 2),
                    'duration': round(entry['duration'], 2)
                }
                for entry in transcript_data
            ]
        }
    return {'text': ' '.join(entry['text'] for entry in transcript_data)}

# --------------------------------------------------
# Main Endpoint: /transcript
# --------------------------------------------------
@app.route('/transcript', methods=['GET'])
@limiter.limit("100/hour")
def get_transcript_endpoint():
    try:
        raw_video_id = request.args.get('videoId', '').strip()
        if not raw_video_id:
            return jsonify({'error': 'Missing videoId parameter'}), 400

        try:
            video_id = extract_video_id(raw_video_id)
        except ValueError:
            return jsonify({'error': 'Invalid YouTube URL or video ID'}), 400

        user_languages = [
            lang.strip().lower()
            for lang in request.args.get('language', 'en').split(',')
            if lang.strip()
        ]
        preserve_format = request.args.get('preserveFormatting', 'false').lower() == 'true'
        return_full = request.args.get('format', 'text').lower() == 'full'
        language_priority = list(dict.fromkeys(user_languages + FALLBACK_LANGUAGES))

        cache_key = generate_cache_key(video_id, tuple(language_priority), preserve_format, return_full)
        if (cached_data := transcript_cache.get(cache_key)) is not None:
            app.logger.info(f"Cache hit for {video_id}")
            return jsonify(cached_data), 200

        start_time = time.time()
        try:
            future = executor.submit(
                fetch_transcript_with_retry,
                video_id=video_id,
                languages=language_priority,
                preserve_format=preserve_format
            )
            transcript_data = future.result(timeout=15)  # Increased timeout to 15 seconds
        except TranscriptsDisabled:
            return jsonify({'error': 'Subtitles disabled for this video'}), 404
        except NoTranscriptFound:
            return jsonify({'error': 'No transcript available in requested languages'}), 404
        except TimeoutError:
            duration = time.time() - start_time
            app.logger.error(f"Timeout fetching transcript for {video_id} after {duration:.2f} seconds")
            return jsonify({'error': 'Transcript service timeout'}), 504
        except Exception as e:
            app.logger.error(f"Proxy error for video {video_id}: {e}")
            return jsonify({'error': 'Transcript service unavailable'}), 503

        processed_data = process_transcript(transcript_data, return_full)
        response_data = {
            'status': 'success',
            'video_id': video_id,
            'detected_language': transcript_data[0].get('language', 'unknown'),
            **processed_data
        }
        
        transcript_cache[cache_key] = response_data
        return jsonify(response_data), 200

    except Exception as e:
        app.logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

# --------------------------------------------------
# Additional Endpoint: /proxy_stats
# --------------------------------------------------
@app.route('/proxy_stats', methods=['GET'])
def get_proxy_stats():
    url = "https://dashboard.smartproxy.com/subscription-api/v1/api/public/statistics/traffic"
    payload = {
        "proxyType": "residential_proxies",
        "startDate": "2024-09-01 00:00:00",
        "endDate": "2024-10-01 00:00:00",
        "groupBy": "target",
        "limit": 500,
        "page": 1,
        "sortBy": "grouping_key",
        "sortOrder": "asc"
    }
    try:
        response = session.post(url, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error fetching proxy stats: {e}")
        return jsonify({'error': 'Could not retrieve proxy stats'}), 503


# --------------------------------------------------
# Application Execution
# --------------------------------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5010))
    app.run(host='0.0.0.0', port=port, threaded=True)

# --------------------------------------------------
# Example: Testing Smartproxy Stats Separately (for debug purposes)
# --------------------------------------------------
if __name__ == '__main__' and False:
    import json
    url = "https://dashboard.smartproxy.com/subscription-api/v1/api/public/statistics/traffic"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Token {SMARTPROXY_API_TOKEN}"
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2))
