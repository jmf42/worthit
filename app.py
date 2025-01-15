from flask import Flask, request, jsonify
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import random
from random import uniform
from time import sleep
import requests
import pkg_resources
from functools import wraps
import threading
from datetime import datetime, timedelta
from cachetools import TTLCache
import hashlib

# Initialize Flask app
app = Flask(__name__)

# Smartproxy credentials
SMARTPROXY_USER = "spylrn4hi2"
SMARTPROXY_PASS = "f~6vgqT7bgqcVxYO73"
SMARTPROXY_HOST = "gate.smartproxy.com"
SMARTPROXY_PORT = "10001"
PROXY = f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{SMARTPROXY_PORT}"
proxies = {"https": PROXY}

# Initialize cache with 1-hour TTL
transcript_cache = TTLCache(maxsize=100, ttl=3600)

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/youtube_transcript.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
app.logger.addHandler(console_handler)

# User agents list
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
]

class RequestThrottler:
    def __init__(self, requests_per_minute=30):
        self.requests_per_minute = requests_per_minute
        self.requests = []
        self.lock = threading.Lock()

    def can_make_request(self):
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        with self.lock:
            self.requests = [req_time for req_time in self.requests if req_time > minute_ago]
            if len(self.requests) < self.requests_per_minute:
                self.requests.append(now)
                return True
            return False

throttler = RequestThrottler()

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def get_enhanced_headers():
    return {'User-Agent': get_random_user_agent()}

def with_exponential_backoff(max_retries=3, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    sleep_time = base_delay * (2 ** attempt) + uniform(0, 1)
                    app.logger.warning(f'Attempt {attempt + 1} failed, retrying in {sleep_time:.2f} seconds')
                    sleep(sleep_time)
            return None
        return wrapper
    return decorator

@with_exponential_backoff(max_retries=3, base_delay=1)
def check_video_availability(video_id):
    try:
        headers = get_enhanced_headers()
        response = requests.get(f'https://www.youtube.com/watch?v={video_id}', headers=headers, timeout=10)
        return response.status_code == 200
    except Exception as e:
        app.logger.warning(f'Failed to check video availability: {str(e)}')
        return False

@with_exponential_backoff(max_retries=3, base_delay=1)
def get_transcript_with_retry(video_id, language_options):
    return YouTubeTranscriptApi.get_transcript(video_id, languages=language_options, proxies=proxies)

@app.route('/transcript', methods=['GET'])
def get_transcript_endpoint():
    if not throttler.can_make_request():
        return jsonify({'error': 'Rate limit exceeded'}), 429

    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')
    cache_key = hashlib.md5(f"{video_id}:{language}".encode()).hexdigest()

    if cached_result := transcript_cache.get(cache_key):
        return jsonify(cached_result), 200

    if not check_video_availability(video_id):
        return jsonify({'error': 'Video not available'}), 404

    try:
        language_options = [language, 'en', 'en-US', 'a.en']
        transcript = get_transcript_with_retry(video_id, language_options)
        full_text = ' '.join([entry['text'] for entry in transcript])

        response_data = {'status': 'success', 'video_id': video_id, 'transcript': full_text}
        transcript_cache[cache_key] = response_data
        return jsonify(response_data), 200

    except TranscriptsDisabled:
        return jsonify({'error': 'Subtitles are disabled'}), 404
    except NoTranscriptFound:
        return jsonify({'error': 'No transcript found'}), 404
    except Exception as e:
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500

@app.route('/test', methods=['GET'])
def test_endpoint():
    return jsonify({'status': 'ok', 'message': 'Service is running'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    app.run(host='0.0.0.0', port=port)
