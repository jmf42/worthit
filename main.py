"""
Entry point bridge for Gunicorn setups expecting 'main:app'.
This exposes the Flask app from app.py as 'app'.
"""
from app import app as app  # noqa: F401

