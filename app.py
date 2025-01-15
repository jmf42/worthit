from flask import Flask, request, jsonify, abort
from youtube_transcript_api import YouTubeTranscriptApi
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import json
import random
import requests

# Initialize Flask app
app = Flask(__name__)

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

# List of common user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
]

def get_random_user_agent():
    """Get a random user agent from the list."""
    return random.choice(USER_AGENTS)

def validate_video_id(video_id):
    """Validate the video ID format."""
    if not video_id or not isinstance(video_id, str):
        return False
    return 5 < len(video_id) < 15 and all(c.isalnum() or c in ['-', '_'] for c in video_id)

def get_proxy_settings():
    """Get proxy settings from environment variables with fallback to free proxy."""
    http_proxy = os.environ.get('HTTP_PROXY')
    https_proxy = os.environ.get('HTTPS_PROXY')
    
    if http_proxy or https_proxy:
        return {
            'http': http_proxy,
            'https': https_proxy
        }
    
    # If no proxies are configured, try to get a free proxy
    try:
        proxy_response = requests.get('https://proxylist.geonode.com/api/proxy-list?limit=1&page=1&sort_by=speed&sort_type=asc&filterUpTime=90&protocols=http%2Chttps&anonymityLevel=elite')
        if proxy_response.status_code == 200:
            proxy_data = proxy_response.json()
            if proxy_data['data']:
                proxy = proxy_data['data'][0]
                proxy_url = f"http://{proxy['ip']}:{proxy['port']}"
                return {
                    'http': proxy_url,
                    'https': proxy_url
                }
    except Exception as e:
        app.logger.warning(f'Failed to fetch free proxy: {str(e)}')
    
    return None

def check_video_availability(video_id):
    """Check if the video is available on YouTube."""
    try:
        headers = {'User-Agent': get_random_user_agent()}
        response = requests.get(
            f'https://www.youtube.com/watch?v={video_id}',
            headers=headers,
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        app.logger.warning(f'Failed to check video availability: {str(e)}')
        return True  # Assume video is available if check fails

@app.route('/transcript', methods=['GET'])
def get_transcript():
    """Main endpoint to retrieve YouTube video transcripts."""
    start_time = time.time()
    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')
    
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
        # Get proxy settings
        proxies = get_proxy_settings()
        if proxies:
            app.logger.info(f'Using proxies: {proxies}')
        
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
        
        # Custom headers for the request
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        
        # Multiple attempts with different settings
        max_attempts = 3
        last_error = None
        
        for attempt in range(max_attempts):
            try:
                app.logger.info(f'Attempt {attempt + 1} of {max_attempts}')
                
                transcript = YouTubeTranscriptApi.get_transcript(
                    video_id,
                    languages=language_options,
                    proxies=proxies,
                    headers=headers
                )
                
                # Process transcript
                full_text = ' '.join([entry['text'] for entry in transcript])
                response_time = time.time() - start_time
                
                return jsonify({
                    'status': 'success',
                    'video_id': video_id,
                    'transcript': full_text,
                    'transcript_length': len(full_text),
                    'language': language,
                    'response_time': f'{response_time:.2f}s',
                    'attempt': attempt + 1,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                }), 200
                
            except Exception as e:
                last_error = str(e)
                app.logger.warning(f'Attempt {attempt + 1} failed: {last_error}')
                if proxies:
                    # Try different proxy for next attempt
                    proxies = get_proxy_settings()
                headers['User-Agent'] = get_random_user_agent()
        
        # If all attempts failed, return error
        app.logger.error(f'All attempts failed for video {video_id}')
        error_response = {
            'error': 'Failed to retrieve transcript',
            'video_id': video_id,
            'details': last_error,
            'response_time': f'{(time.time() - start_time):.2f}s',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if 'subtitles are disabled' in last_error.lower():
            error_response['error_type'] = 'SUBTITLES_DISABLED'
            return jsonify(error_response), 404
        elif 'no transcript' in last_error.lower():
            error_response['error_type'] = 'NO_TRANSCRIPT'
            return jsonify(error_response), 404
        else:
            error_response['error_type'] = 'UNKNOWN_ERROR'
            return jsonify(error_response), 500
            
    except Exception as e:
        app.logger.exception('Unexpected error occurred:')
        return jsonify({
            'error': 'Internal server error',
            'details': str(e),
            'video_id': video_id,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 500

@app.route('/test', methods=['GET'])
def test():
    """Test endpoint to verify service is running."""
    return jsonify({
        'status': 'ok',
        'message': 'Service is running',
        'environment': os.environ.get('FLASK_ENV', 'production'),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
