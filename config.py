"""
Configuration settings for the agent report scraper.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Target website configuration
BASE_URL = "http://188.126.10.151:7080/public/report/"

# Browser configuration
BROWSER_CONFIG = {
    "headless": os.getenv("BROWSER_HEADLESS", "true").lower() == "true",
    "timeout": 60000,  # Increased timeout for cloud deployment
    "viewport": {"width": 1920, "height": 1080},
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Render-optimized browser args
    "args": [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-accelerated-2d-canvas",
        "--no-first-run",
        "--no-zygote",
        "--single-process",
        "--disable-extensions",
        "--disable-plugins",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=TranslateUI,VizDisplayCompositor",
        "--disable-ipc-flooding-protection",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-features=Translate",
        "--hide-scrollbars",
        "--mute-audio",
        "--disable-component-extensions-with-background-pages",
        "--disable-domain-reliability",
        "--disable-features=AudioServiceOutOfProcess",
        "--disable-features=CalculateNativeWinOcclusion"
    ]
}

# Scraping configuration
SCRAPING_CONFIG = {
    "wait_for_load": 3,  # seconds to wait for page load
    "retry_attempts": 3,
    "delay_between_requests": 1,  # seconds
}

# Data export configuration (for on-demand downloads only)
OUTPUT_CONFIG = {
    "export_formats": ["csv", "json"],
    "filename_prefix": "agent_report",
    # Note: No longer saving files automatically - only MongoDB storage
    # Temporary files are created only for downloads
}

# Authentication (if needed)
# You can set these as environment variables in a .env file
AUTH_CONFIG = {
    "username": os.getenv("SCRAPER_USERNAME"),
    "password": os.getenv("SCRAPER_PASSWORD"),
}

# MongoDB configuration
MONGODB_CONFIG = {
    "connection_string": os.getenv("MONGODB_CONNECTION_STRING", "mongodb://localhost:27017/"),
    "database_name": os.getenv("MONGODB_DATABASE", "agent_reports"),
    "collection_name": os.getenv("MONGODB_COLLECTION", "reports"),
    "agents_collection": os.getenv("MONGODB_AGENTS_COLLECTION", "agents"),
    "connection_timeout": 5000,  # milliseconds
    "max_pool_size": 10
}