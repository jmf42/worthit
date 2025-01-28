from flask import Flask, request, jsonify
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from logging.handlers import RotatingFileHandler
import os
import logging
import time
from cachetools import TTLCache
import hashlib
from random import uniform
from time import sleep

# Initialize Flask app
app = Flask(__name__)

# Smartproxy credentials
SMARTPROXY_USER = "spylrn4hi2"
SMARTPROXY_PASS = "f~6vgqT7bgqcVxYO73"
SMARTPROXY_HOST = "gate.smartproxy.com"
SMARTPROXY_PORT = "10001"
PROXY = f"http://{SMARTPROXY_USER}:{SMARTPROXY_PASS}@{SMARTPROXY_HOST}:{SMARTPROXY_PORT}"
proxies = {"https": PROXY}

# Cache for transcripts with 1-hour TTL
transcript_cache = TTLCache(maxsize=100, ttl=3600)

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/youtube_transcript.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
file_handler.setLevel(logging.DEBUG)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.DEBUG)

# Throttling setup
class RequestThrottler:
    def __init__(self, delay_ms=100):
        self.delay = delay_ms / 1000.0  # Convert to seconds
        self.last_request_time = time.time()

    def throttle(self):
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.delay:
            sleep(self.delay - elapsed)
        self.last_request_time = time.time()

throttler = RequestThrottler()

def with_exponential_backoff(base_delay=1, max_retries=1):
    """
    Retries a function with exponential backoff for max_retries times.
    Example: first attempt -> immediate, second attempt -> base_delay * 2^0 + random(0, 0.5).
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
                        f"[Direct Fetch] Attempt {attempt+1} failed for {args}: {e}. "
                        f"Retrying in {sleep_time:.2f}s..."
                    )
                    sleep(sleep_time)
        return wrapper
    return decorator

@with_exponential_backoff(base_delay=1, max_retries=1)
def fetch_transcript_direct(video_id, languages):
    """Attempt to fetch transcript directly with throttling."""
    throttler.throttle()
    return YouTubeTranscriptApi.get_transcript(video_id, languages=languages)

def fetch_transcript_proxy(video_id, languages):
    """Attempt to fetch transcript via Smartproxy."""
    throttler.throttle()
    return YouTubeTranscriptApi.get_transcript(video_id, languages=languages, proxies=proxies)

@app.route('/transcript', methods=['GET'])
def get_transcript_endpoint():
    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')

    if not video_id:
        return jsonify({'error': 'Missing videoId parameter'}), 400

    # Create a unique cache key
    cache_key = hashlib.md5(f"{video_id}:{language}".encode()).hexdigest()
    cached_result = transcript_cache.get(cache_key)
    if cached_result:
        app.logger.info(f"[Cache] Cache hit for video: {video_id}, language: {language}")
        return jsonify(cached_result), 200

    # Popular language fallback sequence
    language_options = [language, 'es', 'fr', 'de', 'pt', 'en', 'en-US']

    # 1) Direct Fetch (with 1 retry)
    try:
        app.logger.info(f"[Direct Fetch] Fetching transcript for video: {video_id}")
        transcript = fetch_transcript_direct(video_id, language_options)
    # If direct fetch ultimately fails, move to proxy
    except (TranscriptsDisabled, NoTranscriptFound) as specific_e:
        # Even if subtitles are disabled or transcript not found,
        # we attempt the proxy as requested: "no matter what error"
        app.logger.warning(f"[Direct Fetch] Specific error ({specific_e}), trying proxy for video: {video_id}")
        try:
            transcript = fetch_transcript_proxy(video_id, language_options)
        except (TranscriptsDisabled, NoTranscriptFound) as final_specific_e:
            app.logger.error(f"[Proxy] {final_specific_e} for video: {video_id}")
            return jsonify({'error': str(final_specific_e)}), 404
        except Exception as proxy_general_error:
            app.logger.error(f"[Proxy] Failed with unknown error: {proxy_general_error}")
            return jsonify({'error': 'Unable to fetch transcript', 'details': str(proxy_general_error)}), 500
    except Exception as general_e:
        app.logger.warning(f"[Direct Fetch] General error ({general_e}), trying proxy for video: {video_id}")
        try:
            transcript = fetch_transcript_proxy(video_id, language_options)
        except (TranscriptsDisabled, NoTranscriptFound) as final_specific_e:
            app.logger.error(f"[Proxy] {final_specific_e} for video: {video_id}")
            return jsonify({'error': str(final_specific_e)}), 404
        except Exception as proxy_general_error:
            app.logger.error(f"[Proxy] Failed with unknown error: {proxy_general_error}")
            return jsonify({'error': 'Unable to fetch transcript', 'details': str(proxy_general_error)}), 500

    # 2) If we get here, we have a transcript.
    response_data = {
        'status': 'success',
        'video_id': video_id,
        'transcript': ' '.join([entry['text'] for entry in transcript]),
    }
    transcript_cache[cache_key] = response_data
    return jsonify(response_data), 200

@app.route('/test', methods=['GET'])
def test_endpoint():
    return jsonify({'status': 'ok', 'message': 'Service is running'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    app.run(host='0.0.0.0', port=port)
