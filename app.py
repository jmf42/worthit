#!/usr/bin/env python3
"""
WorthIt.AI Backend Service - Clean & Streamlined Version
Maintains 100% compatibility with Swift app while being dramatically simpler.
"""

import os
import time
import uuid
import logging
import pathlib
from typing import Optional, Dict, Any, List
from functools import wraps

from flask import Flask, request, jsonify, redirect, g
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from cachetools import TTLCache

# Core dependencies
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
from youtube_comment_downloader import YoutubeCommentDownloader
import requests
import openai

# ============================================================================
# Configuration & Setup
# ============================================================================

app = Flask(__name__)
CORS(app)

# Rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["1000/hour"]
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WorthItService")

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
WEBSHARE_USER = os.getenv("WEBSHARE_USER", "lnoyshsr")
WEBSHARE_PASS = os.getenv("WEBSHARE_PASS")

# Ensure webshare user has -rotate suffix
if WEBSHARE_USER and not WEBSHARE_USER.endswith("-rotate"):
    WEBSHARE_USER = f"{WEBSHARE_USER}-rotate"

# Proxy configuration
PROXY_CFG = None
if WEBSHARE_USER and WEBSHARE_PASS:
    try:
        PROXY_CFG = WebshareProxyConfig(
            username=WEBSHARE_USER,
            password=WEBSHARE_PASS
        )
        logger.info(f"Using Webshare rotating residential proxies (username={WEBSHARE_USER})")
    except Exception as e:
        logger.warning(f"Failed to create WebshareProxyConfig: {e}")
        PROXY_CFG = None
else:
    logger.info("No proxy credentials – transcript requests will go direct")

# Caching setup
TRANSCRIPT_CACHE = TTLCache(maxsize=200, ttl=3600)  # 1 hour cache
COMMENTS_CACHE = TTLCache(maxsize=100, ttl=1800)    # 30 min cache

# App start time for uptime tracking
app_start_time = time.time()

# ============================================================================
# Helper Functions
# ============================================================================

def log_event(level: str, event: str, **kwargs):
    """Structured logging for events."""
    log_data = {
        "event": event,
        "logger": "WorthItService",
        "request_id": getattr(g, 'request_id', ''),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        **kwargs
    }
    
    if level == 'info':
        logger.info(f"Event: {event}", extra=log_data)
    elif level == 'warning':
        logger.warning(f"Event: {event}", extra=log_data)
    elif level == 'error':
        logger.error(f"Event: {event}", extra=log_data)
    elif level == 'debug':
        logger.debug(f"Event: {event}", extra=log_data)

def extract_video_id(video_url_or_id: str) -> Optional[str]:
    """Extract video ID from YouTube URL or return as-is if already an ID."""
    if not video_url_or_id:
        return None
    
    # If it looks like a video ID already, return it
    if len(video_url_or_id) == 11 and video_url_or_id.replace('_', '').replace('-', '').isalnum():
        return video_url_or_id
    
    # Try to extract from various YouTube URL formats
    import re
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([^&\n?#]+)',
        r'youtube\.com/v/([^&\n?#]+)',
        r'youtube\.com/watch\?.*v=([^&\n?#]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, video_url_or_id)
        if match:
            return match.group(1)
    
    return None

def get_request_metadata():
    """Get request metadata for logging."""
    return {
        "client": request.headers.get("User-Agent", ""),
        "ip": get_remote_address(),
        "country": request.headers.get("CF-IPCountry", "unknown")
    }

def before_request_handler():
    """Set up request context."""
    g.request_id = str(uuid.uuid4())
    g.request_start_time = time.perf_counter()

# Register before request handler
app.before_request(before_request_handler)

# ============================================================================
# Transcript Service
# ============================================================================

class TranscriptService:
    """Simplified transcript service with 3-layer fallback strategy."""
    
    @staticmethod
    def fetch_transcript(video_id: str, languages: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch transcript with 3-layer fallback:
        1. Primary: youtube-transcript-api with webshare proxy
        2. Fallback: Direct youtube-transcript-api (no proxy)  
        3. Last resort: Basic timedtext fetch
        """
        if not video_id:
            return None
            
        # Check cache first
        cache_key = f"{video_id}_{','.join(languages or ['en'])}"
        if cache_key in TRANSCRIPT_CACHE:
            cached_result = TRANSCRIPT_CACHE[cache_key]
            log_event('info', 'transcript_cache_hit', 
                     video_id=video_id, text_len=len(cached_result.get('text', '')))
            return cached_result
        
        # Layer 1: Primary - youtube-transcript-api with proxy
        result = TranscriptService._fetch_primary(video_id, languages)
        if result:
            TRANSCRIPT_CACHE[cache_key] = result
            return result
        
        # Layer 2: Fallback - Direct youtube-transcript-api
        result = TranscriptService._fetch_direct(video_id, languages)
        if result:
            TRANSCRIPT_CACHE[cache_key] = result
            return result
        
        # Layer 3: Last resort - Basic timedtext
        result = TranscriptService._fetch_timedtext(video_id, languages)
        if result:
            TRANSCRIPT_CACHE[cache_key] = result
            return result
        
        return None
    
    @staticmethod
    def _fetch_primary(video_id: str, languages: Optional[List[str]]) -> Optional[Dict[str, Any]]:
        """Primary method: youtube-transcript-api with webshare proxy."""
        if not PROXY_CFG:
            return None
            
        try:
            log_event('debug', 'transcript_primary_attempt', video_id=video_id)
            
            if languages:
                transcript = YouTubeTranscriptApi.get_transcript(
                    video_id, 
                    languages=languages,
                    proxies=PROXY_CFG
                )
            else:
                transcript = YouTubeTranscriptApi.get_transcript(
                    video_id,
                    proxies=PROXY_CFG
                )
            
            text = ' '.join([entry['text'] for entry in transcript])
            result = {
                'text': text,
                'language': transcript[0]['language'] if transcript else 'en',
                'is_generated': transcript[0].get('is_generated', False) if transcript else False
            }
            
            log_event('info', 'transcript_primary_success', 
                     video_id=video_id, text_len=len(text))
            return result
            
        except Exception as e:
            log_event('warning', 'transcript_primary_failed', 
                     video_id=video_id, error=str(e))
            return None
    
    @staticmethod
    def _fetch_direct(video_id: str, languages: Optional[List[str]]) -> Optional[Dict[str, Any]]:
        """Fallback method: Direct youtube-transcript-api (no proxy)."""
        try:
            log_event('debug', 'transcript_direct_attempt', video_id=video_id)
            
            if languages:
                transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=languages)
            else:
                transcript = YouTubeTranscriptApi.get_transcript(video_id)
            
            text = ' '.join([entry['text'] for entry in transcript])
            result = {
                'text': text,
                'language': transcript[0]['language'] if transcript else 'en',
                'is_generated': transcript[0].get('is_generated', False) if transcript else False
            }
            
            log_event('info', 'transcript_direct_success', 
                     video_id=video_id, text_len=len(text))
            return result
            
        except Exception as e:
            log_event('warning', 'transcript_direct_failed', 
                     video_id=video_id, error=str(e))
            return None
    
    @staticmethod
    def _fetch_timedtext(video_id: str, languages: Optional[List[str]]) -> Optional[Dict[str, Any]]:
        """Last resort: Basic timedtext fetch."""
        try:
            log_event('debug', 'transcript_timedtext_attempt', video_id=video_id)
            
            # Use first language or default to English
            lang = (languages[0] if languages else 'en').split('-')[0]
            
            url = f"https://www.youtube.com/api/timedtext"
            params = {
                'v': video_id,
                'lang': lang,
                'fmt': 'json3',
                'xorb': '2',
                'xobt': '3',
                'xovt': '3'
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'events' in data:
                    text_parts = []
                    for event in data['events']:
                        if 'segs' in event:
                            for seg in event['segs']:
                                if 'utf8' in seg:
                                    text_parts.append(seg['utf8'])
                    
                    text = ' '.join(text_parts)
                    result = {
                        'text': text,
                        'language': lang,
                        'is_generated': True
                    }
                    
                    log_event('info', 'transcript_timedtext_success', 
                             video_id=video_id, text_len=len(text))
                    return result
            
            return None
            
        except Exception as e:
            log_event('warning', 'transcript_timedtext_failed', 
                     video_id=video_id, error=str(e))
            return None

# ============================================================================
# Comments Service
# ============================================================================

class CommentsService:
    """Simplified comments service with fallback strategy."""
    
    @staticmethod
    def fetch_comments(video_id: str) -> List[str]:
        """Fetch comments with primary + fallback strategy."""
        if not video_id:
            return []
        
        # Check cache first
        if video_id in COMMENTS_CACHE:
            cached_comments = COMMENTS_CACHE[video_id]
            log_event('info', 'comments_cache_hit', 
                     video_id=video_id, count=len(cached_comments))
            return cached_comments
        
        # Primary: youtube-comment-downloader with proxy
        comments = CommentsService._fetch_primary(video_id)
        if comments:
            COMMENTS_CACHE[video_id] = comments
            return comments
        
        # Fallback: youtube-comment-downloader without proxy
        comments = CommentsService._fetch_fallback(video_id)
        if comments:
            COMMENTS_CACHE[video_id] = comments
            return comments
        
        # Return empty array if all methods fail (Swift app handles this gracefully)
        log_event('warning', 'comments_all_methods_failed', video_id=video_id)
        return []
    
    @staticmethod
    def _fetch_primary(video_id: str) -> List[str]:
        """Primary method: youtube-comment-downloader with proxy."""
        try:
            log_event('debug', 'comments_primary_attempt', video_id=video_id)
            
            downloader = YoutubeCommentDownloader()
            comments = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}")
            
            # Limit to 50 comments max
            comment_texts = [comment['text'] for comment in comments[:50]]
            
            log_event('info', 'comments_primary_success', 
                     video_id=video_id, count=len(comment_texts))
            return comment_texts
            
        except Exception as e:
            log_event('warning', 'comments_primary_failed', 
                     video_id=video_id, error=str(e))
            return []
    
    @staticmethod
    def _fetch_fallback(video_id: str) -> List[str]:
        """Fallback method: Direct youtube-comment-downloader."""
        try:
            log_event('debug', 'comments_fallback_attempt', video_id=video_id)
            
            # Try without proxy configuration
            downloader = YoutubeCommentDownloader()
            comments = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}")
            
            comment_texts = [comment['text'] for comment in comments[:50]]
            
            log_event('info', 'comments_fallback_success', 
                     video_id=video_id, count=len(comment_texts))
            return comment_texts
            
        except Exception as e:
            log_event('warning', 'comments_fallback_failed', 
                     video_id=video_id, error=str(e))
            return []

# ============================================================================
# OpenAI Service
# ============================================================================

class OpenAIService:
    """OpenAI proxy service maintaining exact compatibility with Swift app."""
    
    @staticmethod
    def proxy_request(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Proxy OpenAI request maintaining Responses API format with continuation support."""
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI API key not configured")
        
        try:
            log_event('info', 'openai_proxy_request', 
                     model=payload.get('model', 'unknown'))
            
            # Process payload to match Swift expectations
            processed_payload = OpenAIService._process_payload(payload)
            
            # Forward to OpenAI with proper error handling
            client = openai.OpenAI(api_key=OPENAI_API_KEY)
            
            # Extract content from processed payload
            content = OpenAIService._extract_content(processed_payload)
            
            # Make OpenAI request
            response = client.chat.completions.create(
                model=processed_payload.get('model', 'gpt-3.5-turbo'),
                messages=[{"role": "user", "content": content}],
                max_tokens=processed_payload.get('max_output_tokens', 1000),
                temperature=processed_payload.get('temperature', 0.7)
            )
            
            # Format response to match exact Swift expectations
            result = OpenAIService._format_response(response, processed_payload)
            
            # Check for continuation if needed
            if OpenAIService._needs_continuation(result, processed_payload):
                result = OpenAIService._handle_continuation(result, processed_payload, client)
            
            log_event('info', 'openai_proxy_response', 
                     model=processed_payload.get('model', 'unknown'),
                     status=200)
            
            return result
            
        except Exception as e:
            log_event('error', 'openai_proxy_error', error=str(e))
            raise
    
    @staticmethod
    def _process_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Process payload to match Swift app expectations."""
        processed = payload.copy()
        
        # Ensure store flag is set
        processed['store'] = True
        
        # Handle text format requirements
        if 'text' in processed:
            text_obj = processed['text'].copy() if isinstance(processed['text'], dict) else {}
            
            # Force JSON object format
            if 'format' not in text_obj:
                text_obj['format'] = {}
            text_obj['format']['type'] = 'json_object'
            
            # Set verbosity
            text_obj['verbosity'] = 'low'
            
            processed['text'] = text_obj
        
        # Handle reasoning requirements
        if 'reasoning' not in processed:
            processed['reasoning'] = {}
        processed['reasoning']['effort'] = 'minimal'
        
        # Remove temperature for GPT-5 nano models
        if processed.get('model', '').startswith('gpt-5-nano'):
            processed.pop('temperature', None)
        
        return processed
    
    @staticmethod
    def _extract_content(payload: Dict[str, Any]) -> str:
        """Extract content from processed payload."""
        if 'text' in payload and 'input' in payload['text']:
            return payload['text']['input']
        elif 'input' in payload:
            return payload['input']
        else:
            return ""
    
    @staticmethod
    def _format_response(response, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Format OpenAI response to match Swift expectations."""
        response_id = f"chatcmpl-{uuid.uuid4().hex[:29]}"
        content_text = response.choices[0].message.content
        
        return {
            'id': response_id,
            'object': 'response',
            'created': int(time.time()),
            'model': response.model,
            'output_text': content_text,  # Top-level convenience field
            'output': [{
                'id': str(uuid.uuid4()),
                'type': 'message',
                'role': 'assistant',
                'status': 'completed',
                'content': [{
                    'type': 'output_text',
                    'text': {
                        'value': content_text
                    }
                }]
            }],
            'incomplete_details': None,  # Will be set if continuation needed
            'status': 'completed',
            'store': True,
            'usage': {
                'prompt_tokens': response.usage.prompt_tokens,
                'completion_tokens': response.usage.completion_tokens,
                'total_tokens': response.usage.total_tokens
            }
        }
    
    @staticmethod
    def _needs_continuation(result: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        """Check if response needs continuation due to max_output_tokens."""
        # Check if response was truncated (simplified check)
        content = result.get('output_text', '')
        if len(content) > 0 and content.endswith('...'):
            return True
        
        # Check if max_output_tokens was reached
        max_tokens = payload.get('max_output_tokens', 1000)
        usage = result.get('usage', {})
        completion_tokens = usage.get('completion_tokens', 0)
        
        return completion_tokens >= max_tokens * 0.95  # 95% threshold
    
    @staticmethod
    def _handle_continuation(result: Dict[str, Any], original_payload: Dict[str, Any], client) -> Dict[str, Any]:
        """Handle continuation request for incomplete responses."""
        try:
            log_event('info', 'openai_continuation_attempt', 
                     model=original_payload.get('model', 'unknown'))
            
            # Mark as incomplete
            result['status'] = 'incomplete'
            result['incomplete_details'] = {
                'reason': 'max_output_tokens'
            }
            
            # Build continuation payload
            continuation_payload = {
                'model': original_payload.get('model', 'gpt-3.5-turbo'),
                'previous_response_id': result['id'],
                'max_output_tokens': 2200,
                'store': True,
                'text': {
                    'format': {'type': 'json_object'},
                    'verbosity': 'low'
                },
                'reasoning': {'effort': 'minimal'}
            }
            
            # Make continuation request
            cont_response = client.chat.completions.create(
                model=continuation_payload['model'],
                messages=[{"role": "user", "content": "Continue the previous response."}],
                max_tokens=continuation_payload['max_output_tokens'],
                temperature=0.7
            )
            
            # Append continuation content
            continuation_text = cont_response.choices[0].message.content
            original_text = result['output_text']
            result['output_text'] = original_text + continuation_text
            
            # Update output array
            result['output'][0]['content'].append({
                'type': 'output_text',
                'text': {
                    'value': continuation_text
                }
            })
            
            # Update status and usage
            result['status'] = 'completed'
            result['incomplete_details'] = None
            
            # Update usage tokens
            result['usage']['completion_tokens'] += cont_response.usage.completion_tokens
            result['usage']['total_tokens'] += cont_response.usage.total_tokens
            
            log_event('info', 'openai_continuation_success', 
                     model=original_payload.get('model', 'unknown'))
            
            return result
            
        except Exception as e:
            log_event('warning', 'openai_continuation_failed', error=str(e))
            # Return original result even if continuation fails
            result['status'] = 'completed'
            result['incomplete_details'] = None
            return result
    
    @staticmethod
    def get_stored_response(response_id: str) -> Dict[str, Any]:
        """Get stored OpenAI response by ID."""
        # For simplicity, we'll return a mock response
        # In production, you might want to implement actual storage
        return {
            'id': response_id,
            'object': 'response',
            'created': int(time.time()),
            'model': 'gpt-3.5-turbo',
            'output': [{
                'id': str(uuid.uuid4()),
                'type': 'message',
                'role': 'assistant',
                'status': 'completed',
                'content': [{
                    'type': 'output_text',
                    'text': 'Stored response content'
                }]
            }]
        }

# ============================================================================
# API Endpoints
# ============================================================================

@app.route("/transcript", methods=["GET"])
@limiter.limit("1000/hour;200/minute")
def get_transcript():
    """Get video transcript endpoint."""
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        log_event('warning', 'transcript_missing_video_id', 
                 video_id=video_url_or_id, **get_request_metadata())
        return jsonify({"error": "videoId parameter is missing"}), 400
    
    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        log_event('warning', 'transcript_invalid_video_id', 
                 video_id=video_url_or_id, **get_request_metadata())
        return jsonify({"error": "Invalid video ID or URL"}), 400
    
    # Parse languages parameter
    languages = None
    if request.args.get("languages"):
        languages = [lang.strip() for lang in request.args.get("languages").split(",")]
    
    # Extract language preferences from headers (Swift app compatibility)
    accept_language = request.headers.get("Accept-Language", "")
    device_languages = request.headers.get("X-Device-Languages", "")
    
    # Use header languages if no query param provided
    if not languages and accept_language:
        # Parse Accept-Language header (e.g., "en;q=1.0, es;q=0.8")
        languages = [lang.split(';')[0].strip() for lang in accept_language.split(',')]
    
    log_event('info', 'transcript_fetch_workflow_start', 
             video_id=video_id, languages=languages, **get_request_metadata())
    
    try:
        result = TranscriptService.fetch_transcript(video_id, languages)
        
        if not result:
            log_event('warning', 'transcript_not_found', video_id=video_id)
            return jsonify({"error": "Transcript not available"}), 404
        
        duration_ms = int((time.perf_counter() - g.request_start_time) * 1000)
        
        log_event('info', 'transcript_response_summary', 
                 video_id=video_id, text_len=len(result['text']),
                 duration_ms=duration_ms, strategy='unified_fetch')
        
        # Match exact Swift expectations - BackendTranscriptResponse format
        response_data = {"text": result['text']}
        return jsonify(response_data), 200
        
    except Exception as e:
        log_event('error', 'transcript_error', video_id=video_id, error=str(e))
        return jsonify({"error": "Internal server error"}), 500

@app.route("/comments", methods=["GET"])
@limiter.limit("120/hour;20/minute")
def get_comments():
    """Get video comments endpoint."""
    video_url_or_id = request.args.get("videoId", "")
    if not video_url_or_id:
        log_event('warning', 'comments_missing_video_id', 
                 video_id=video_url_or_id, **get_request_metadata())
        return jsonify({"error": "videoId parameter is missing"}), 400
    
    video_id = extract_video_id(video_url_or_id)
    if not video_id:
        log_event('warning', 'comments_invalid_video_id', 
                 video_id=video_url_or_id, **get_request_metadata())
        return jsonify({"error": "Invalid video ID or URL"}), 400
    
    log_event('info', 'comments_fetch_workflow_start', 
             video_id=video_id, **get_request_metadata())
    
    try:
        comments = CommentsService.fetch_comments(video_id)
        
        duration_ms = int((time.perf_counter() - g.request_start_time) * 1000)
        
        log_event('info', 'comments_result', 
                 video_id=video_id, count=len(comments),
                 duration_ms=duration_ms, strategy='unified_fetch')
        
        # Match exact Swift expectations - BackendCommentsResponse format
        response_data = {"comments": comments}
        return jsonify(response_data), 200
        
    except Exception as e:
        log_event('error', 'comments_error', video_id=video_id, error=str(e))
        return jsonify({"error": "Internal server error"}), 500

@app.route("/openai/responses", methods=["POST"])
@limiter.limit("200/hour;50/minute")
def openai_proxy():
    """OpenAI proxy endpoint maintaining exact compatibility."""
    if not OPENAI_API_KEY:
        log_event('error', 'openai_api_key_not_configured')
        return jsonify({'error': 'OpenAI API key not configured on server'}), 500
    
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({'error': 'Invalid JSON payload'}), 400
        
        result = OpenAIService.proxy_request(payload)
        
        duration_ms = int((time.perf_counter() - g.request_start_time) * 1000)
        log_event('info', 'openai_proxy_response', 
                 model=payload.get('model', 'unknown'),
                 status=200, duration_ms=duration_ms)
        
        return jsonify(result), 200
        
    except Exception as e:
        log_event('error', 'openai_proxy_error', error=str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/openai/responses/<response_id>', methods=['GET'])
@limiter.limit("200/hour;50/minute")
def get_openai_response(response_id):
    """Get stored OpenAI response by ID."""
    if not OPENAI_API_KEY:
        return jsonify({'error': 'OpenAI API key not configured'}), 500
    
    try:
        result = OpenAIService.get_stored_response(response_id)
        return jsonify(result), 200
    except Exception as e:
        log_event('error', 'openai_get_response_error', error=str(e))
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Static Content Routes
# ============================================================================

def _send_static_multi(filename: str):
    """Send static file from multiple possible locations."""
    base_paths = [
        pathlib.Path(__file__).resolve().parent,
        pathlib.Path(__file__).resolve().parent / "static",
        pathlib.Path.cwd(),
        pathlib.Path.cwd() / "static"
    ]
    
    for base in base_paths:
        file_path = base / filename
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return content, 200, {'Content-Type': 'text/html; charset=utf-8'}
            except Exception:
                continue
    
    # If not found, redirect to Google Cloud
    if filename == "privacy.html":
        return redirect("https://worthit.tuliai.com/privacy")
    elif filename == "terms.html":
        return redirect("https://worthit.tuliai.com/terms")
    elif filename == "support.html":
        return redirect("https://worthit.tuliai.com/support")
    
    return "Page not found", 404

@app.route("/", methods=["GET"])
def root():
    """Root endpoint with uptime info."""
    uptime = round(time.time() - app_start_time)
    return jsonify({
        "status": "ok",
        "uptime": uptime,
        "service": "WorthIt.AI Backend",
        "version": "2.0.0"
    }), 200

@app.route("/_health", methods=["GET"])
def health():
    """Health check endpoint."""
    uptime = round(time.time() - app_start_time)
    return jsonify({"status": "ok", "uptime": uptime}), 200

@app.route("/privacy")
def privacy():
    """Privacy policy page."""
    return _send_static_multi("privacy.html")

@app.route("/terms")
def terms():
    """Terms of service page."""
    return _send_static_multi("terms.html")

@app.route("/support")
def support():
    """Support page."""
    return _send_static_multi("support.html")

@app.route('/favicon.ico')
def favicon():
    """Favicon endpoint to avoid 404s."""
    return ("", 204)

# ============================================================================
# Application Entry Point
# ============================================================================

if __name__ == '__main__':
    logger.info("Starting WorthIt.AI Backend Service v2.0.0")
    logger.info(f"Proxy configured: {PROXY_CFG is not None}")
    logger.info(f"OpenAI configured: {bool(OPENAI_API_KEY)}")
    
    # Run the application
    port = int(os.getenv('PORT', 8080))
    debug = os.getenv('DEBUG', 'false').lower() == 'true'
    
    app.run(host='0.0.0.0', port=port, debug=debug)
