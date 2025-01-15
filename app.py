from flask import Flask, request, jsonify, abort
from youtube_transcript_api import YouTubeTranscriptApi
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import json

# Initialize Flask app
app = Flask(__name__)

# Configure logging
if not app.debug:
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.mkdir('logs')
    file_handler = RotatingFileHandler('logs/youtube_transcript.log', maxBytes=10240, backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.info('YouTube Transcript Service startup')

# Add console handler for development
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
app.logger.addHandler(console_handler)

def validate_video_id(video_id):
    """
    Validate the video ID format.
    Args:
        video_id (str): YouTube video ID to validate
    Returns:
        bool: True if valid, False otherwise
    """
    if not video_id or not isinstance(video_id, str):
        return False
    # Basic validation for YouTube ID format
    # YouTube IDs are usually 11 characters long and contain alphanumeric chars, underscores and hyphens
    return 5 < len(video_id) < 15 and all(c.isalnum() or c in ['-', '_'] for c in video_id)

def get_proxy_settings():
    """
    Get proxy settings from environment variables
    Returns:
        dict: Proxy configuration or None
    """
    http_proxy = os.environ.get('HTTP_PROXY')
    https_proxy = os.environ.get('HTTPS_PROXY')
    
    if http_proxy or https_proxy:
        return {
            'http': http_proxy,
            'https': https_proxy
        }
    return None

@app.route('/test', methods=['GET'])
def test():
    """Test endpoint to verify service is running."""
    return jsonify({
        'status': 'ok',
        'message': 'Service is running',
        'environment': os.environ.get('FLASK_ENV', 'production'),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring."""
    return jsonify({
        'status': 'healthy',
        'service': 'youtube-transcript-api',
        'version': '1.0.0',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'environment': os.environ.get('FLASK_ENV', 'production')
    }), 200

@app.route('/debug', methods=['GET'])
def debug_info():
    """Debug endpoint to check configuration and environment."""
    if os.environ.get('FLASK_ENV') != 'development':
        abort(403)  # Forbidden in production
    
    return jsonify({
        'environment': os.environ.get('FLASK_ENV'),
        'debug_mode': app.debug,
        'python_version': os.sys.version,
        'proxy_settings': get_proxy_settings(),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 200

@app.route('/transcript', methods=['GET'])
def get_transcript():
    """
    Main endpoint to retrieve YouTube video transcripts.
    Query Parameters:
        videoId (str): YouTube video ID
        language (str, optional): Preferred language code (default: en)
    Returns:
        JSON response with transcript or error details
    """
    start_time = time.time()
    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')
    
    # Log the incoming request
    app.logger.info(f'Transcript request received for video ID: {video_id}')
    app.logger.info(f'Request headers: {dict(request.headers)}')
    
    # Validate video ID
    if not validate_video_id(video_id):
        app.logger.error(f'Invalid video ID provided: {video_id}')
        return jsonify({
            'error': 'Invalid video ID provided',
            'video_id': video_id,
            'request_id': request.headers.get('X-Request-ID'),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 400
    
    try:
        # Get proxy settings
        proxies = get_proxy_settings()
        app.logger.info(f'Using proxy settings: {proxies}')
        
        # Attempt to fetch transcript with multiple language options
        app.logger.info(f'Attempting to fetch transcript for video: {video_id}')
        
        # Define language options with fallbacks
        language_options = [
            language,
            f'{language}-{language.upper()}',  # e.g., en-US
            'a.' + language,  # auto-generated
            'en',  # English fallback
            'en-US',
            'en-GB',
            'a.en'  # Auto-generated English
        ]
        
        # Remove duplicates while preserving order
        language_options = list(dict.fromkeys(language_options))
        
        app.logger.info(f'Trying languages in order: {language_options}')
        
        transcript = YouTubeTranscriptApi.get_transcript(
            video_id,
            languages=language_options,
            preserve_formatting=True,
            proxies=proxies
        )
        
        # Process and join the transcript text
        full_text = ' '.join([entry['text'] for entry in transcript])
        
        # Calculate response time
        response_time = time.time() - start_time
        
        # Log success
        app.logger.info(f'Successfully retrieved transcript for video: {video_id}')
        app.logger.info(f'Response time: {response_time:.2f} seconds')
        
        return jsonify({
            'status': 'success',
            'video_id': video_id,
            'transcript': full_text,
            'transcript_length': len(full_text),
            'language': language,
            'response_time': f'{response_time:.2f}s',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }), 200
    
    except Exception as e:
        # Log the error with full details
        app.logger.error(f'Error retrieving transcript for video {video_id}: {str(e)}')
        app.logger.exception('Full traceback:')
        
        # Calculate response time for error case
        response_time = time.time() - start_time
        
        # Build detailed error response
        error_response = {
            'error': 'Failed to retrieve transcript',
            'video_id': video_id,
            'details': str(e),
            'request_id': request.headers.get('X-Request-ID'),
            'response_time': f'{response_time:.2f}s',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Handle specific error cases
        error_message = str(e).lower()
        if 'subtitles are disabled' in error_message:
            error_response.update({
                'error': 'Subtitles are disabled for this video',
                'error_type': 'SUBTITLES_DISABLED'
            })
            return jsonify(error_response), 404
        elif 'no transcript' in error_message:
            error_response.update({
                'error': 'No transcript available for this video',
                'error_type': 'NO_TRANSCRIPT'
            })
            return jsonify(error_response), 404
        elif 'could not retrieve' in error_message:
            error_response.update({
                'error': 'Could not retrieve transcript',
                'error_type': 'RETRIEVAL_ERROR',
                'possible_cause': 'YouTube API access may be restricted'
            })
            return jsonify(error_response), 503
        else:
            error_response.update({
                'error_type': 'UNKNOWN_ERROR'
            })
            return jsonify(error_response), 500

@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors."""
    app.logger.error(f'404 error: {str(error)}')
    return jsonify({
        'error': 'Resource not found',
        'details': str(error),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    app.logger.error(f'Server Error: {str(error)}')
    return jsonify({
        'error': 'Internal server error',
        'details': str(error),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 500

@app.errorhandler(Exception)
def handle_exception(error):
    """Handle all unhandled exceptions."""
    app.logger.error(f'Unhandled exception: {str(error)}')
    app.logger.exception('Full traceback:')
    return jsonify({
        'error': 'Internal server error',
        'details': str(error),
        'error_type': 'UNHANDLED_EXCEPTION',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }), 500

if __name__ == '__main__':
    # Get port from environment variable or default to 5005
    port = int(os.environ.get('PORT', 5005))
    
    # Set debug mode from environment variable
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Set host to '0.0.0.0' to make it externally visible
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug_mode
    )
