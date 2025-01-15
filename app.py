from flask import Flask, request, jsonify
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
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

# Initialize cache with 1-hour TTL
transcript_cache = TTLCache(maxsize=100, ttl=3600)

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/youtube_transcript.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

# Add console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
app.logger.addHandler(console_handler)

# Enhanced list of user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0'
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
            # Remove requests older than 1 minute
            self.requests = [req_time for req_time in self.requests if req_time > minute_ago]
            
            if len(self.requests) < self.requests_per_minute:
                self.requests.append(now)
                return True
            return False

# Initialize the throttler
throttler = RequestThrottler()

def get_random_user_agent():
    """Get a random user agent from the list."""
    return random.choice(USER_AGENTS)

def get_enhanced_headers():
    """Get enhanced headers for YouTube requests."""
    return {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'DNT': '1',  # Do Not Track
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'X-Forwarded-For': f'192.168.{random.randint(1, 254)}.{random.randint(1, 254)}'  # Randomize IP
    }

def get_cache_key(video_id, language):
    """Generate a cache key for the video and language combination."""
    return hashlib.md5(f"{video_id}:{language}".encode()).hexdigest()

def validate_video_id(video_id):
    """Validate the video ID format."""
    if not video_id or not isinstance(video_id, str):
        return False
    return 5 < len(video_id) < 15 and all(c.isalnum() or c in ['-', '_'] for c in video_id)

@with_exponential_backoff(max_retries=3, base_delay=1)
def check_video_availability(video_id):
    """Check if the video is available on YouTube."""
    try:
        headers = get_enhanced_headers()
        session = requests.Session()
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))
        response = session.get(
            f'https://www.youtube.com/watch?v={video_id}',
            headers=headers,
            timeout=10,
            allow_redirects=True
        )
        return response.status_code == 200
    except Exception as e:
        app.logger.warning(f'Failed to check video availability: {str(e)}')
        return True

def get_package_version(package_name):
    """Get the installed version of a package."""
    try:
        return pkg_resources.get_distribution(package_name).version
    except pkg_resources.DistributionNotFound:
        return "Unknown"

def with_exponential_backoff(max_retries=3, base_delay=1):
    """Decorator for exponential backoff retry logic."""
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
def get_transcript_with_retry(video_id, language_options):
    """Get transcript with enhanced retry logic."""
    try:
        # Configure the YouTube Transcript API with custom cookies and headers
        custom_headers = get_enhanced_headers()
        
        # Add these parameters to the YouTubeTranscriptApi call
        return YouTubeTranscriptApi.get_transcript(
            video_id,
            languages=language_options,
            headers=custom_headers,
            cookies={'CONSENT': 'YES+cb.20210328-17-p0.en+FX+{}'.format(random.randint(100, 999))}
        )
    except Exception as e:
        app.logger.warning(f'Transcript fetch failed: {str(e)}')
        raise

@app.route('/transcript', methods=['GET'])
def get_transcript_endpoint():
    """Main endpoint to retrieve YouTube video transcripts."""
    # Check rate limit
    if not throttler.can_make_request():
        return jsonify({
            'error': 'Rate limit exceeded. Please try again in a minute.',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 429

    start_time = time.time()
    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')

    # Check cache first
    cache_key = get_cache_key(video_id, language)
    cached_result = transcript_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result), 200

    # Enhanced request logging
    app.logger.info(f'Transcript request received from IP: {request.remote_addr}')
    app.logger.info(f'Headers: {dict(request.headers)}')
    app.logger.info(f'Video ID: {video_id}, Language: {language}')

    # Validate video ID
    if not validate_video_id(video_id):
        return jsonify({
            'error': 'Invalid video ID provided',
            'video_id': video_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 400

    # Check video availability
    if not check_video_availability(video_id):
        return jsonify({
            'error': 'Video not available or not found',
            'video_id': video_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 404

    try:
        # Get youtube_transcript_api version
        yt_version = get_package_version("youtube_transcript_api")
        app.logger.info(f'youtube_transcript_api version: {yt_version}')

        # Define language options with fallbacks
        language_options = [
            language,
            f'{language}-{language.upper()}',
            'a.' + language,
            'en',
            'en-US',
            'en-GB',
            'a.en'
        ]
        language_options = list(dict.fromkeys(language_options))

        # Get transcript with retry logic
        transcript = get_transcript_with_retry(video_id, language_options)

        # Process transcript
        full_text = ' '.join([entry['text'] for entry in transcript])
        response_time = time.time() - start_time

        response_data = {
            'status': 'success',
            'video_id': video_id,
            'transcript': full_text,
            'transcript_length': len(full_text),
            'language': language,
            'response_time': f'{response_time:.2f}s',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }

        # Cache the successful response
        transcript_cache[cache_key] = response_data

        return jsonify(response_data), 200

    except TranscriptsDisabled:
        app.logger.error(f'Subtitles are disabled for video {video_id}')
        return jsonify({
            'error': 'Subtitles are disabled for this video',
            'video_id': video_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 404
    except NoTranscriptFound:
        app.logger.error(f'No transcript found for video {video_id}')
        return jsonify({
            'error': 'No transcript found for this video',
            'video_id': video_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 404
    except Exception as e:
        app.logger.exception('Unexpected error occurred:')
        return jsonify({
            'error': 'Internal server error',
            'details': str(e),
            'video_id': video_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Test endpoint to verify service is running."""
    try:
        # Get youtube_transcript_api version
        yt_version = get_package_version("youtube_transcript_api")
    except Exception as e:
        yt_version = 'Unknown'

    return jsonify({
        'status': 'ok',
        'message': 'Service is running',
        'environment': os.environ.get('FLASK_ENV', 'production'),
        'youtube_transcript_api_version': yt_version,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
