import os
import time
import hashlib
import logging
from random import uniform
from time import sleep

from flask import Flask, request, jsonify
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound
)
from logging.handlers import RotatingFileHandler
from cachetools import TTLCache

# -----------------------------
# Flask App Initialization
# -----------------------------
app = Flask(__name__)

# Smartproxy credentials
SMARTPROXY_USER = "spylrn4hi2"
SMARTPROXY_PASS = "f~6vgqT7bgqcVxYO73"
SMARTPROXY_HOST = "gate.smartproxy.com"
SMARTPROXY_PORT = "10001"
PROXY = f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{SMARTPROXY_PORT}"
proxies = {"https": PROXY}

# -----------------------------
# Transcript Cache (1-hour TTL)
# -----------------------------
transcript_cache = TTLCache(maxsize=100, ttl=3600)

# -----------------------------
# Logging Configuration
# -----------------------------
if not os.path.exists('logs'):
    os.mkdir('logs')

file_handler = RotatingFileHandler('logs/youtube_transcript.log',
                                   maxBytes=10240,
                                   backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
file_handler.setLevel(logging.DEBUG)

app.logger.addHandler(file_handler)
app.logger.setLevel(logging.DEBUG)

# -----------------------------
# Throttling Setup
# -----------------------------
class RequestThrottler:
    """
    Simple throttler to ensure a minimum delay between requests.
    """
    def __init__(self, delay_ms=300):
        self.delay = delay_ms / 1000.0  # convert ms to seconds
        self.last_request_time = time.time()

    def throttle(self):
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.delay:
            sleep(self.delay - elapsed)
        self.last_request_time = time.time()

throttler = RequestThrottler()

# -----------------------------
# Exponential Backoff Decorator
# -----------------------------
def with_exponential_backoff(base_delay=1, max_retries=2):
    """
    Retries a function with exponential backoff up to 'max_retries' times.
    Example backoff: base_delay * 2^attempt + uniform(0, 0.5)
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise e
                    sleep_time = base_delay * (2 ** attempt) + uniform(0, 0.5)
                    app.logger.warning(
                        f"[Direct Fetch] Attempt {attempt+1} failed ({e}). Retrying in {sleep_time:.2f}s..."
                    )
                    sleep(sleep_time)
            # If we exit the loop somehow without returning, raise last exception
            raise RuntimeError("Max retries exceeded.")
        return wrapper
    return decorator

# -----------------------------
# Fetching Functions
# -----------------------------
@with_exponential_backoff(base_delay=1, max_retries=2)
def fetch_transcript_direct(video_id, languages):
    """
    Attempt to fetch transcript directly with throttling
    and exponential backoff on errors.
    """
    throttler.throttle()
    return YouTubeTranscriptApi.get_transcript(video_id, languages=languages)

def fetch_transcript_proxy(video_id, languages):
    """
    Attempt to fetch transcript via Smartproxy.
    Throttled as well, but no exponential backoff here (only direct).
    """
    throttler.throttle()
    return YouTubeTranscriptApi.get_transcript(video_id, languages=languages, proxies=proxies)

# -----------------------------
# Routes
# -----------------------------
@app.route('/transcript', methods=['GET'])
def get_transcript():
    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')

    # 1) Validate Params
    if not video_id:
        return jsonify({'error': 'Missing videoId parameter'}), 400

    # 2) Check Cache
    cache_key = hashlib.md5(f"{video_id}:{language}".encode()).hexdigest()
    cached_result = transcript_cache.get(cache_key)
    if cached_result:
        app.logger.info(f"[Cache] HIT for {video_id} ({language})")
        app.logger.info(f"[Cache Stats] Currently storing {len(transcript_cache)}/{transcript_cache.maxsize}")
        return jsonify(cached_result), 200

    # 3) Define language fallback options
    # e.g. user-langs -> fallback to commonly used
    language_options = [language, 'es', 'fr', 'de', 'pt', 'en', 'en-US']

    # 4) Try Direct Fetch
    app.logger.info(f"[Direct Fetch] Attempting transcript for video: {video_id}")
    try:
        transcript = fetch_transcript_direct(video_id, language_options)
    except (TranscriptsDisabled, NoTranscriptFound) as specific_e:
        # Still attempt proxy if direct says no transcript (per your requirement)
        app.logger.warning(f"[Direct Fetch] {specific_e}. Trying proxy for {video_id}")
        try:
            transcript = fetch_transcript_proxy(video_id, language_options)
        except (TranscriptsDisabled, NoTranscriptFound) as final_specific_e:
            app.logger.error(f"[Proxy] {final_specific_e} for video: {video_id}")
            return jsonify({'error': str(final_specific_e)}), 404
        except Exception as proxy_err:
            # Check if it's a rate-limit or generic error
            if "Too Many Requests" in str(proxy_err) or "google.com/sorry" in str(proxy_err):
                app.logger.error(f"[Proxy] Rate-limited by YouTube: {proxy_err}")
                return jsonify({'error': 'Rate-limited by YouTube. Try again later.'}), 429
            app.logger.error(f"[Proxy] Unknown error: {proxy_err}")
            return jsonify({'error': 'Unable to fetch transcript', 'details': str(proxy_err)}), 500
    except Exception as general_err:
        # If direct fails for unknown reason, try proxy
        app.logger.warning(f"[Direct Fetch] General error for {video_id}: {general_err}. Trying proxy...")
        try:
            transcript = fetch_transcript_proxy(video_id, language_options)
        except (TranscriptsDisabled, NoTranscriptFound) as final_specific_e:
            app.logger.error(f"[Proxy] {final_specific_e} for video: {video_id}")
            return jsonify({'error': str(final_specific_e)}), 404
        except Exception as proxy_err:
            if "Too Many Requests" in str(proxy_err) or "google.com/sorry" in str(proxy_err):
                app.logger.error(f"[Proxy] Rate-limited by YouTube: {proxy_err}")
                return jsonify({'error': 'Rate-limited by YouTube. Try again later.'}), 429
            app.logger.error(f"[Proxy] Unknown error: {proxy_err}")
            return jsonify({'error': 'Unable to fetch transcript', 'details': str(proxy_err)}), 500
    # 5) We have a transcript now
    response_data = {
        'status': 'success',
        'video_id': video_id,
        'transcript': ' '.join([entry['text'] for entry in transcript]),
    }
    # 6) Store in Cache
    transcript_cache[cache_key] = response_data
    app.logger.info(f"[Cache] MISS. Storing new transcript for {video_id} ({language})")
    app.logger.info(f"[Cache Stats] Currently storing {len(transcript_cache)}/{transcript_cache.maxsize}")
    return jsonify(response_data), 200

@app.route('/test', methods=['GET'])
def test_endpoint():
    """
    Quick health check for the service.
    """
    return jsonify({'status': 'ok', 'message': 'Service is running'}), 200

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    app.run(host='0.0.0.0', port=port)
