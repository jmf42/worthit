from flask import Flask, request, jsonify, abort
from youtube_transcript_api import YouTubeTranscriptApi
import os
import logging
from logging.handlers import RotatingFileHandler

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

def validate_video_id(video_id):
    """Validate the video ID format."""
    if not video_id or not isinstance(video_id, str):
        return False
    # Basic validation - you might want to add more specific YouTube ID validation
    return len(video_id) > 0 and len(video_id) < 50

@app.route('/test', methods=['GET'])
def test():
    """Test endpoint to verify service is running."""
    return jsonify({
        'status': 'ok',
        'message': 'Service is running',
        'environment': os.environ.get('FLASK_ENV', 'production')
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring."""
    return jsonify({
        'status': 'healthy',
        'service': 'youtube-transcript-api',
        'version': '1.0.0'
    }), 200

@app.route('/transcript', methods=['GET'])
def get_transcript():
    """Main endpoint to retrieve YouTube video transcripts."""
    video_id = request.args.get('videoId')
    
    # Log the incoming request
    app.logger.info(f'Transcript request received for video ID: {video_id}')
    
    # Validate video ID
    if not validate_video_id(video_id):
        app.logger.error(f'Invalid video ID provided: {video_id}')
        return jsonify({
            'error': 'Invalid video ID provided',
            'video_id': video_id
        }), 400
    
    try:
        # Attempt to fetch transcript with multiple language options
        app.logger.info(f'Attempting to fetch transcript for video: {video_id}')
        transcript = YouTubeTranscriptApi.get_transcript(
            video_id,
            languages=['en', 'en-US', 'en-GB'],
            preserve_formatting=True
        )
        
        # Process and join the transcript text
        full_text = ' '.join([entry['text'] for entry in transcript])
        
        # Log success
        app.logger.info(f'Successfully retrieved transcript for video: {video_id}')
        
        return jsonify({
            'status': 'success',
            'video_id': video_id,
            'transcript': full_text,
            'transcript_length': len(full_text)
        }), 200
    
    except Exception as e:
        # Log the error with full details
        app.logger.error(f'Error retrieving transcript for video {video_id}: {str(e)}')
        
        # Return a more informative error response
        error_message = str(e)
        if 'Subtitles are disabled' in error_message:
            return jsonify({
                'error': 'Subtitles are disabled for this video',
                'video_id': video_id,
                'details': error_message
            }), 404
        elif 'No transcript' in error_message:
            return jsonify({
                'error': 'No transcript available for this video',
                'video_id': video_id,
                'details': error_message
            }), 404
        else:
            return jsonify({
                'error': 'Failed to retrieve transcript',
                'video_id': video_id,
                'details': error_message
            }), 500

@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors."""
    app.logger.error(f'404 error: {str(error)}')
    return jsonify({
        'error': 'Resource not found',
        'details': str(error)
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    app.logger.error(f'Server Error: {str(error)}')
    return jsonify({
        'error': 'Internal server error',
        'details': str(error)
    }), 500

if __name__ == '__main__':
    # Get port from environment variable or default to 5005
    port = int(os.environ.get('PORT', 5005))
    
    # Set host to '0.0.0.0' to make it externally visible
    app.run(
        host='0.0.0.0',
        port=port,
        debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    )