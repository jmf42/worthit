from flask import Flask, request, jsonify
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from youtube_transcript_api.formatters import JSONFormatter
from logging.handlers import RotatingFileHandler
import logging
import hashlib
from cachetools import TTLCache
import os

# Initialize Flask app
app = Flask(__name__)

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

# Debug helper
def log_debug_info(video_id, message, error=None):
    if error:
        app.logger.error(f"[{video_id}] {message}: {str(error)}")
    else:
        app.logger.info(f"[{video_id}] {message}")

@app.route('/transcript', methods=['GET'])
def get_transcript():
    video_id = request.args.get('videoId')
    language = request.args.get('language', 'en')

    if not video_id:
        log_debug_info("N/A", "Missing videoId parameter")
        return jsonify({'error': 'Missing videoId parameter'}), 400

    # Create a unique cache key based on videoId and language
    cache_key = hashlib.md5(f"{video_id}:{language}".encode()).hexdigest()

    # Check the cache first
    if cached_result := transcript_cache.get(cache_key):
        log_debug_info(video_id, "Returning cached transcript")
        return jsonify(cached_result), 200

    try:
        log_debug_info(video_id, "Fetching transcript")
        
        # Fetch the transcript
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[language, 'en', 'en-US'])
        
        # Format the transcript as JSON
        formatter = JSONFormatter()
        formatted_transcript = formatter.format_transcript(transcript)

        # Prepare the response data
        response_data = {
            'status': 'success',
            'video_id': video_id,
            'transcript': formatted_transcript
        }

        # Cache the result
        transcript_cache[cache_key] = response_data
        log_debug_info(video_id, "Transcript fetched and cached successfully")
        
        return jsonify(response_data), 200

    except TranscriptsDisabled:
        log_debug_info(video_id, "Subtitles are disabled for this video", error="TranscriptsDisabled")
        return jsonify({'error': 'Subtitles are disabled for this video'}), 404
    except NoTranscriptFound:
        log_debug_info(video_id, "No transcript found for this video", error="NoTranscriptFound")
        return jsonify({'error': 'No transcript found for this video'}), 404
    except Exception as e:
        log_debug_info(video_id, "Internal server error occurred", error=e)
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500

@app.route('/test', methods=['GET'])
def test_endpoint():
    log_debug_info("N/A", "Health check - service is running")
    return jsonify({'status': 'ok', 'message': 'Service is running'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5005))
    app.run(host='0.0.0.0', port=port)
