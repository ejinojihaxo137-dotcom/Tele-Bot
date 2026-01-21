#!/usr/bin/env python3
"""
Telegram Video Downloader Bot - 24/7 Server Deployment
Optimized for Railway.app, Render.com, and other free hosting
"""

import os
import sys
import logging
import tempfile
import asyncio
import json
import math
import time
import signal
import psutil
import traceback
import re
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
import shutil

import yt_dlp
from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    InputFile,
    Message,
    User
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler
)
from telegram.error import TelegramError, NetworkError, TimedOut
import aiohttp
from aiohttp import web
import cachetools
from cachetools import TTLCache

# ============ CONFIGURATION ============
# Get environment variables - WARNING: You've hardcoded your token!
BOT_TOKEN = os.environ.get('BOT_TOKEN')  # Fixed: Use environment variable name
if not BOT_TOKEN:
    print("❌ ERROR: BOT_TOKEN environment variable is not set!")
    print("Please set it in your server environment variables.")
    sys.exit(1)

# Server settings
PORT = int(os.environ.get('PORT', 8080))
HOST = os.environ.get('HOST', '0.0.0.0')

# Telegram limits (improved constants)
MAX_FILE_SIZE = 1.8 * 1024 * 1024 * 1024  # 1.8GB (safe margin)
MAX_VIDEO_DURATION = 3600  # 1 hour in seconds
MAX_REQUESTS_PER_USER = 10  # Per hour
MAX_CONCURRENT_DOWNLOADS = 5  # Max simultaneous downloads

# Configure logging with rotation
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO if os.environ.get('DEBUG') != '1' else logging.DEBUG,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            'bot.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
    ]
)
logger = logging.getLogger(__name__)
# Reduce noise from dependencies
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)

# ============ DATA MODELS ============
@dataclass
class DownloadRequest:
    user_id: int
    url: str
    platform: str
    timestamp: float
    status: str = "pending"
    file_size: int = 0
    duration: int = 0
    quality: str = "best"
    message_id: Optional[int] = None
    
    @property
    def age(self) -> float:
        return time.time() - self.timestamp

@dataclass
class UserStats:
    user_id: int
    username: Optional[str] = None
    downloads: int = 0
    last_active: float = field(default_factory=time.time)
    total_size: int = 0
    
    def update_activity(self):
        self.last_active = time.time()

# ============ BOT MANAGER ============
class VideoDownloaderBot:
    def __init__(self):
        self.requests: Dict[int, List[DownloadRequest]] = defaultdict(list)
        self.user_stats: Dict[int, UserStats] = {}
        self.active_downloads: Dict[int, asyncio.Task] = {}
        self.rate_limit_cache = TTLCache(maxsize=1000, ttl=3600)
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        
        # Platform detection with regex patterns (more accurate)
        self.supported_platforms = {
            'youtube': [r'youtube\.com', r'youtu\.be', r'y2u\.be'],
            'tiktok': [r'tiktok\.com', r'vm\.tiktok\.com', r'vt\.tiktok\.com'],
            'instagram': [r'instagram\.com', r'instagr\.am'],
            'twitter': [r'twitter\.com', r'x\.com', r't\.co'],
            'facebook': [r'facebook\.com', r'fb\.watch', r'fb\.com'],
            'reddit': [r'reddit\.com', r'redd\.it'],
            'twitch': [r'twitch\.tv', r'clips\.twitch\.tv'],
            'dailymotion': [r'dailymotion\.com', r'dai\.ly'],
            'rumble': [r'rumble\.com'],
            'bilibili': [r'bilibili\.com', r'b23\.tv'],
            'likee': [r'likee\.video', r'like-video\.com'],
            'vk': [r'vk\.com', r'vk\.ru'],
            'ok': [r'ok\.ru'],
            'snapchat': [r'snapchat\.com'],
            'pinterest': [r'pinterest\.com'],
            'linkedin': [r'linkedin\.com'],
            'ted': [r'ted\.com'],
            'vimeo': [r'vimeo\.com'],
            'soundcloud': [r'soundcloud\.com'],
            'spotify': [r'spotify\.com'],
            'podcasts': [r'podcasts\.google\.com', r'apple\.co'],
        }
        
        # Compile regex patterns once
        self.platform_patterns = {}
        for platform, patterns in self.supported_platforms.items():
            compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
            self.platform_patterns[platform] = compiled_patterns
        
        # Initialize yt-dlp options
        self.ydl_opts = self._get_ydl_options()
        
        # Bot statistics
        self.stats = {
            'total_downloads': 0,
            'total_size_downloaded': 0,
            'start_time': time.time(),
            'errors': 0,
            'unique_users': set()
        }
        
        # Create temp directory for downloads
        self.temp_dir = Path(tempfile.mkdtemp(prefix="video_bot_"))
        logger.info(f"Created temp directory: {self.temp_dir}")
        
        # Health check endpoint
        self.health_check_running = False
        
        # Task tracking
        self.cleanup_task: Optional[asyncio.Task] = None

    def _get_ydl_options(self) -> dict:
        """Get optimized yt-dlp options for server environment."""
        return {
            'format': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'merge_output_format': 'mp4',
            'outtmpl': '%(title).100s.%(ext)s',
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
            'nooverwrites': True,
            'continuedl': True,
            'retries': 3,  # Reduced from 10 to avoid hanging
            'fragment_retries': 3,
            'skip_unavailable_fragments': True,
            'socket_timeout': 15,  # Reduced timeout
            'extract_flat': False,
            'force_generic_extractor': False,
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
            },
            'concurrent_fragment_downloads': 2,  # Reduced for server stability
            'throttledratelimit': 2097152,  # 2 MB/s
            'buffersize': 1024 * 1024,  # 1 MB buffer
            'sleep_interval_requests': 1,  # Be nice to servers
        }

    def _progress_hook(self, d: dict) -> None:
        """Progress hook for yt-dlp."""
        if d['status'] == 'downloading':
            percent = d.get('_percent_str', '0%').strip()
            speed = d.get('_speed_str', 'N/A')
            eta = d.get('_eta_str', 'N/A')
            
            # Only log every 10% progress to reduce noise
            try:
                percent_num = float(percent.strip('%'))
                if percent_num % 10 == 0:
                    logger.debug(f"Downloading: {percent} - {speed} - ETA: {eta}")
            except:
                pass
                
        elif d['status'] == 'finished':
            logger.info(f"Download completed successfully")

    def _check_rate_limit(self, user_id: int) -> Tuple[bool, Optional[int]]:
        """Check if user has exceeded rate limit."""
        key = f"rate_limit_{user_id}"
        current_time = time.time()
        
        # Get existing count
        if key in self.rate_limit_cache:
            count, last_reset = self.rate_limit_cache[key]
            
            # Check if we need to reset (more than 1 hour passed)
            if current_time - last_reset > 3600:
                count = 1
                self.rate_limit_cache[key] = (count, current_time)
                return True, MAX_REQUESTS_PER_USER - count
            elif count >= MAX_REQUESTS_PER_USER:
                return False, 0
            else:
                self.rate_limit_cache[key] = (count + 1, last_reset)
                return True, MAX_REQUESTS_PER_USER - (count + 1)
        else:
            self.rate_limit_cache[key] = (1, current_time)
            return True, MAX_REQUESTS_PER_USER - 1

    def _detect_platform(self, url: str) -> Optional[str]:
        """Detect which platform the URL belongs to using regex."""
        try:
            domain = urlparse(url).netloc.lower()
            for platform, patterns in self.platform_patterns.items():
                for pattern in patterns:
                    if pattern.search(domain):
                        return platform
            return None
        except Exception as e:
            logger.debug(f"Error detecting platform: {e}")
            return None

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size in human readable format."""
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"

    @staticmethod
    def _format_duration(seconds: int) -> str:
        """Format duration in human readable format."""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}m {secs}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            secs = seconds % 60
            return f"{hours}h {minutes}m {secs}s"

    def _get_quality_options(self, info: dict) -> List[Tuple[str, str, str]]:
        """Get available quality options for video."""
        formats = info.get('formats', [])
        qualities = []
        
        # Track seen heights to avoid duplicates
        seen_heights = set()
        
        for f in formats:
            # Check if it has both video and audio
            if f.get('vcodec') != 'none' and f.get('acodec') != 'none':
                height = f.get('height')
                ext = f.get('ext', 'mp4')
                filesize = f.get('filesize') or f.get('filesize_approx')
                
                if height and height >= 144 and ext in ['mp4', 'mkv', 'webm']:
                    # Avoid duplicates
                    if height in seen_heights:
                        continue
                    seen_heights.add(height)
                    
                    quality_key = f"{height}p"
                    quality_text = f"{height}p"
                    
                    if filesize:
                        size_str = self._format_size(filesize)
                        quality_text += f" ({size_str})"
                    
                    # Get format ID
                    format_id = f['format_id']
                    
                    # Add bitrate info if available
                    if f.get('vbr'):
                        quality_text += f" ~{int(f['vbr']/1000)}kbps"
                    
                    qualities.append((quality_key, quality_text, format_id))
        
        # Sort by quality (highest first)
        qualities.sort(key=lambda x: int(x[0].replace('p', '')) if x[0].replace('p', '').isdigit() else 0, 
                      reverse=True)
        
        return qualities[:6]  # Return top 6 qualities

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command."""
        user = update.effective_user
        user_id = user.id
        
        # Update user stats
        self.stats['unique_users'].add(user_id)
        
        # Get user stats or create new
        if user_id not in self.user_stats:
            self.user_stats[user_id] = UserStats(
                user_id=user_id,
                username=user.username
            )
        
        welcome_text = f"""
👋 *Welcome {user.first_name}!*

🎬 *Video Downloader Bot*
🌐 *Running 24/7 on Server*

📥 *Supported Platforms:*
• YouTube (Full HD & 4K)
• TikTok (With/Without watermark)
• Instagram Reels/Stories
• Twitter/X Videos
• Facebook/Reddit/Twitch
• And 30+ more sites!

⚡ *Features:*
• Up to 2GB file size
• Multiple quality options
• Fast server downloads
• No watermark (where possible)
• Audio extraction option

📊 *Your Statistics:*
• Downloads: {self.user_stats[user_id].downloads}
• Total Size: {self._format_size(self.user_stats[user_id].total_size)}

🚀 *How to use:*
Just send me any video link!
For YouTube, I'll show quality options.

Made with ❤️ - Always online!
        """
        
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
        
        # Log user start
        logger.info(f"User {user_id} ({user.username}) started the bot")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command."""
        help_text = """
🤖 *Bot Commands:*

/start - Start the bot
/help - Show this help message
/stats - Show bot statistics
/status - Check server status
/formats - Supported formats info
/cancel - Cancel current download

🎯 *YouTube Commands:*
Add these after the YouTube link:
• `-best` - Best quality available
• `-720` - 720p HD quality
• `-1080` - 1080p Full HD
• `-audio` - Audio only (MP3)
• `-fast` - Faster download (lower quality)

📱 *Mobile Tips:*
• Long press video to save
• Use landscape for better viewing
• Download via Wi-Fi for large files

⚠️ *Limits:*
• Max file: 2GB (Telegram limit)
• Max duration: 1 hour
• Rate limit: 10 downloads/hour
• Temporary storage only

💡 *Pro Tip:* For long videos, use `-fast` flag!
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel command."""
        user_id = update.effective_user.id
        
        if user_id in self.active_downloads:
            task = self.active_downloads[user_id]
            if not task.done():
                task.cancel()
                await update.message.reply_text("✅ Download cancelled successfully!")
                logger.info(f"User {user_id} cancelled download")
            else:
                await update.message.reply_text("⚠️ No active download to cancel.")
        else:
            await update.message.reply_text("⚠️ No active download to cancel.")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command."""
        # Calculate server metrics
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=1)
        disk = psutil.disk_usage('/')
        uptime = time.time() - self.stats['start_time']
        
        # Calculate average download size
        avg_size = (self.stats['total_size_downloaded'] / self.stats['total_downloads'] 
                   if self.stats['total_downloads'] > 0 else 0)
        
        stats_text = f"""
📊 *Bot Statistics*

👥 *Users:*
• Unique: {len(self.stats['unique_users'])}
• Active now: {len(self.active_downloads)}

📥 *Downloads:*
• Total: {self.stats['total_downloads']}
• Size: {self._format_size(self.stats['total_size_downloaded'])}
• Average: {self._format_size(int(avg_size))}
• Errors: {self.stats['errors']}

⚙️ *Server Status:*
• CPU: {cpu:.1f}%
• Memory: {memory.percent:.1f}% ({self._format_size(memory.used)}/{self._format_size(memory.total)})
• Disk: {disk.percent:.1f}% ({self._format_size(disk.used)}/{self._format_size(disk.total)})
• Uptime: {self._format_duration(int(uptime))}

🎯 *Performance:*
• Active downloads: {len(self.active_downloads)}
• Rate limited users: {len([k for k in self.rate_limit_cache.keys() if 'rate_limit' in k])}
• Cache hit rate: {self.rate_limit_cache.currsize/1000*100:.1f}%

🏓 *Health:* ✅ Online 24/7
        """
        
        await update.message.reply_text(stats_text, parse_mode='Markdown')

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming messages."""
        user = update.effective_user
        user_id = user.id
        message_text = update.message.text.strip()
        
        # Update user activity
        if user_id not in self.user_stats:
            self.user_stats[user_id] = UserStats(
                user_id=user_id,
                username=user.username
            )
        self.user_stats[user_id].update_activity()
        
        # Check rate limit
        allowed, remaining = self._check_rate_limit(user_id)
        if not allowed:
            await update.message.reply_text(
                f"⚠️ *Rate limit exceeded!*\n"
                f"You've used all {MAX_REQUESTS_PER_USER} downloads this hour.\n"
                f"Please wait before sending more requests.",
                parse_mode='Markdown'
            )
            return
        
        # Check if message contains URL
        if not any(proto in message_text.lower() for proto in ['http://', 'https://']):
            await update.message.reply_text(
                "Please send me a valid video URL starting with http:// or https://"
            )
            return
        
        # Extract URL from message
        url = self._extract_url(message_text)
        if not url:
            await update.message.reply_text("Could not find a valid URL in your message.")
            return
        
        # Validate URL format
        if not self._validate_url(url):
            await update.message.reply_text("Invalid URL format. Please check and try again.")
            return
        
        # Detect platform
        platform = self._detect_platform(url)
        if not platform:
            await update.message.reply_text(
                "❌ *Unsupported platform!*\n"
                "I support: YouTube, TikTok, Instagram, Twitter, Facebook, Reddit, and 30+ more sites.\n"
                "Make sure the URL is correct and the video is publicly accessible.",
                parse_mode='Markdown'
            )
            return
        
        # Check for quality flags in message
        quality = "best"
        message_lower = message_text.lower()
        if "-fast" in message_lower:
            quality = "fast"
        elif "-720" in message_lower:
            quality = "720"
        elif "-1080" in message_lower:
            quality = "1080"
        elif "-audio" in message_lower:
            quality = "audio"
        elif "-best" in message_lower:
            quality = "best"
        
        # Check if user already has an active download
        if user_id in self.active_downloads:
            task = self.active_downloads[user_id]
            if not task.done():
                await update.message.reply_text(
                    "⚠️ You already have a download in progress!\n"
                    "Use /cancel to cancel it first."
                )
                return
        
        # Process the download request with semaphore
        async with self.download_semaphore:
            await self._process_download_request(update, context, url, platform, quality)

    @staticmethod
    def _extract_url(text: str) -> Optional[str]:
        """Extract URL from text using regex."""
        url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w\.\-?=&%#+]*'
        matches = re.findall(url_pattern, text)
        return matches[0] if matches else None

    @staticmethod
    def _validate_url(url: str) -> bool:
        """Validate URL format."""
        try:
            result = urlparse(url)
            return all([result.scheme in ['http', 'https'], result.netloc])
        except:
            return False

    async def _process_download_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                       url: str, platform: str, quality: str):
        """Process download request."""
        user = update.effective_user
        user_id = user.id
        
        # Create status message
        status_msg = await update.message.reply_text(
            f"🔍 *Processing {platform.capitalize()} link...*\n"
            f"⏳ Please wait while I fetch video information...",
            parse_mode='Markdown'
        )
        
        # Create download request
        request = DownloadRequest(
            user_id=user_id,
            url=url,
            platform=platform,
            timestamp=time.time(),
            quality=quality,
            message_id=status_msg.message_id
        )
        
        try:
            # Get video info
            info = await self._get_video_info(url)
            if not info:
                await status_msg.edit_text(
                    "❌ Could not fetch video information.\n"
                    "URL might be invalid, private, or region restricted."
                )
                return
            
            # Check video duration
            duration = info.get('duration', 0)
            if duration > MAX_VIDEO_DURATION:
                await status_msg.edit_text(
                    f"❌ Video too long!\n"
                    f"Duration: {self._format_duration(duration)}\n"
                    f"Maximum allowed: {self._format_duration(MAX_VIDEO_DURATION)}\n"
                    f"Try using `-fast` flag for compressed version."
                )
                return
            
            # For YouTube, show quality options if not specified
            if platform == 'youtube' and quality == 'best':
                await self._show_youtube_quality_options(update, context, url, info, status_msg)
                return
            
            # Start download as a task
            task = asyncio.create_task(
                self._download_and_send_video(update, context, url, platform, quality, status_msg, info)
            )
            self.active_downloads[user_id] = task
            
            # Add cleanup callback
            def cleanup_task(fut):
                if user_id in self.active_downloads:
                    del self.active_downloads[user_id]
                # Log any errors
                if fut.exception():
                    logger.error(f"Download task failed: {fut.exception()}")
            
            task.add_done_callback(cleanup_task)
            
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            self.stats['errors'] += 1
            
            error_msg = "❌ An error occurred while processing your request."
            if "private" in str(e).lower():
                error_msg += "\nVideo might be private or requires login."
            elif "unavailable" in str(e).lower():
                error_msg += "\nVideo might be deleted or region restricted."
            
            try:
                await status_msg.edit_text(error_msg)
            except:
                await update.message.reply_text(error_msg)

    async def _get_video_info(self, url: str) -> Optional[dict]:
        """Get video information using yt-dlp with timeout."""
        try:
            # Use a separate event loop for yt-dlp (which is synchronous)
            loop = asyncio.get_event_loop()
            
            # Run yt-dlp with timeout
            ydl_opts = self.ydl_opts.copy()
            ydl_opts['quiet'] = True
            
            def extract_info():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(url, download=False)
            
            # Add timeout to avoid hanging
            info = await asyncio.wait_for(loop.run_in_executor(None, extract_info), timeout=30)
            
            if not info:
                return None
                
            # Log successful info extraction
            logger.info(f"Extracted info for {url}: title='{info.get('title')[:50]}...', duration={info.get('duration')}")
            return info
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout extracting info from {url}")
            return None
        except Exception as e:
            logger.error(f"Error extracting info from {url}: {e}")
            return None

    async def _download_and_send_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                      url: str, platform: str, quality: str, 
                                      status_msg: Message, info: dict = None):
        """Download and send video to user."""
        user = update.effective_user
        user_id = user.id
        
        try:
            # Update status message
            title = info.get('title', 'Video')[:50] if info else 'Video'
            await status_msg.edit_text(
                f"⬇️ *Downloading video...*\n"
                f"📹 *{title}*\n"
                f"🎯 Quality: {quality}\n"
                f"🌐 Platform: {platform.capitalize()}\n"
                f"⏳ This may take a moment...",
                parse_mode='Markdown'
            )
            
            # Create temporary directory for this download
            with tempfile.TemporaryDirectory(dir=self.temp_dir) as temp_dir:
                temp_path = Path(temp_dir)
                
                # Configure yt-dlp options based on quality
                ydl_opts = self.ydl_opts.copy()
                ydl_opts['progress_hooks'] = [self._progress_hook]
                
                if quality == 'audio':
                    ydl_opts.update({
                        'format': 'bestaudio/best',
                        'postprocessors': [{
                            'key': 'FFmpegExtractAudio',
                            'preferredcodec': 'mp3',
                            'preferredquality': '192',
                        }],
                        'outtmpl': str(temp_path / '%(title)s.%(ext)s'),
                    })
                elif quality == 'fast':
                    ydl_opts.update({
                        'format': 'best[height<=480]/worst',
                        'outtmpl': str(temp_path / '%(title)s.%(ext)s'),
                    })
                elif quality.isdigit():
                    ydl_opts.update({
                        'format': f'bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/worst',
                        'outtmpl': str(temp_path / '%(title)s.%(ext)s'),
                    })
                else:
                    ydl_opts['outtmpl'] = str(temp_path / '%(title)s.%(ext)s')
                
                # Download the video
                def download_video():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.download([url])
                
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, download_video)
                
                # Find the downloaded file
                files = list(temp_path.glob('*'))
                if not files:
                    raise Exception("No file downloaded")
                
                # Get the most recently modified file
                file_path = max(files, key=lambda f: f.stat().st_mtime)
                file_size = file_path.stat().st_size
                
                # Check file size
                if file_size > MAX_FILE_SIZE:
                    await status_msg.edit_text(
                        f"❌ File too large!\n"
                        f"Size: {self._format_size(file_size)}\n"
                        f"Maximum: {self._format_size(MAX_FILE_SIZE)}\n"
                        f"Try lower quality or `-fast` flag."
                    )
                    return
                
                # Update stats
                self.stats['total_downloads'] += 1
                self.stats['total_size_downloaded'] += file_size
                
                if user_id in self.user_stats:
                    self.user_stats[user_id].downloads += 1
                    self.user_stats[user_id].total_size += file_size
                
                # Update status message
                await status_msg.edit_text(
                    f"📤 *Uploading to Telegram...*\n"
                    f"📊 Size: {self._format_size(file_size)}\n"
                    f"⏳ Almost done!",
                    parse_mode='Markdown'
                )
                
                # Send the file with timeout
                with open(file_path, 'rb') as file:
                    if quality == 'audio':
                        await update.message.reply_audio(
                            audio=InputFile(file, filename=file_path.name),
                            caption=f"✅ Audio downloaded!\n"
                                   f"Size: {self._format_size(file_size)}",
                            timeout=300,
                            read_timeout=300,
                            write_timeout=300
                        )
                    elif file_path.suffix in ['.mp4', '.mkv', '.webm']:
                        await update.message.reply_video(
                            video=InputFile(file, filename=file_path.name),
                            caption=f"✅ Video downloaded!\n"
                                   f"Quality: {quality}\n"
                                   f"Size: {self._format_size(file_size)}",
                            supports_streaming=True,
                            timeout=300,
                            read_timeout=300,
                            write_timeout=300
                        )
                    else:
                        await update.message.reply_document(
                            document=InputFile(file, filename=file_path.name),
                            caption=f"✅ File downloaded!\n"
                                   f"Size: {self._format_size(file_size)}",
                            timeout=300,
                            read_timeout=300,
                            write_timeout=300
                        )
                
                # Clean up
                try:
                    await status_msg.delete()
                except:
                    pass
                
                logger.info(f"Successfully sent {self._format_size(file_size)} to user {user_id}")
                
        except asyncio.CancelledError:
            await status_msg.edit_text("❌ Download cancelled.")
            logger.info(f"Download cancelled for user {user_id}")
            
        except Exception as e:
            logger.error(f"Error in download process for user {user_id}: {e}")
            self.stats['errors'] += 1
            
            error_msg = "❌ Error downloading video."
            if "File too large" in str(e):
                error_msg = "❌ File is too large for Telegram (max 2GB)."
            elif "timeout" in str(e).lower():
                error_msg = "❌ Operation timed out. Try again with lower quality."
            elif "Too Many Requests" in str(e):
                error_msg = "❌ Too many requests to video platform. Try again later."
            
            try:
                await status_msg.edit_text(error_msg)
            except:
                await update.message.reply_text(error_msg)
        finally:
            # Clean up user from active downloads
            if user_id in self.active_downloads:
                del self.active_downloads[user_id]

    async def cleanup_temp_files(self):
        """Clean up temporary files periodically."""
        while True:
            try:
                # Delete files older than 1 hour
                now = time.time()
                deleted_count = 0
                
                for item in self.temp_dir.glob('*'):
                    if item.is_file():
                        file_age = now - item.stat().st_mtime
                        if file_age > 3600:  # 1 hour
                            item.unlink(missing_ok=True)
                            deleted_count += 1
                    elif item.is_dir():
                        dir_age = now - item.stat().st_mtime
                        if dir_age > 3600:  # 1 hour
                            shutil.rmtree(item, ignore_errors=True)
                            deleted_count += 1
                
                if deleted_count > 0:
                    logger.debug(f"Cleaned up {deleted_count} old temp items")
                
                # Log cache statistics periodically
                logger.debug(f"Cache size: {self.rate_limit_cache.currsize}/{self.rate_limit_cache.maxsize}")
                
            except Exception as e:
                logger.error(f"Error in cleanup: {e}")
            
            # Run every 30 minutes
            await asyncio.sleep(1800)

    async def shutdown(self):
        """Clean shutdown of the bot."""
        logger.info("Shutting down bot...")
        
        # Cancel all active downloads
        for user_id, task in self.active_downloads.items():
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.active_downloads:
            await asyncio.gather(*self.active_downloads.values(), return_exceptions=True)
        
        # Clean up temp directory
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        logger.info("Bot shutdown complete")

# ============ APPLICATION SETUP ============
def setup_application() -> Tuple[Application, VideoDownloaderBot]:
    """Set up and configure the Telegram bot application."""
    # Create bot instance
    bot_manager = VideoDownloaderBot()
    
    # Create application with connection pooling
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connection_pool_size(8)
        .pool_timeout(30)
        .get_updates_connection_pool_size(8)
        .build()
    )
    
    # Add handlers
    application.add_handler(CommandHandler("start", bot_manager.start_command))
    application.add_handler(CommandHandler("help", bot_manager.help_command))
    application.add_handler(CommandHandler("stats", bot_manager.stats_command))
    application.add_handler(CommandHandler("status", bot_manager.status_command))
    application.add_handler(CommandHandler("cancel", bot_manager.cancel_command))
    
    # Add message handler for URLs
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        bot_manager.handle_message
    ))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(bot_manager.handle_callback))
    
    # Add error handler
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        """Log errors."""
        logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
        
        # Try to notify user
        try:
            if isinstance(update, Update) and update.effective_message:
                await update.effective_message.reply_text(
                    "❌ An internal error occurred. Please try again later."
                )
        except:
            pass
    
    application.add_error_handler(error_handler)
    
    # Store bot manager in application context
    application.bot_data['manager'] = bot_manager
    
    return application, bot_manager

async def main():
    """Main entry point."""
    print("=" * 60)
    print("🎬 TELEGRAM VIDEO DOWNLOADER BOT - SERVER DEPLOYMENT")
    print("=" * 60)
    print(f"🤖 Bot Token: {'✅ Set' if BOT_TOKEN else '❌ Not Set'}")
    print(f"🌐 Host: {HOST}")
    print(f"🚪 Port: {PORT}")
    print(f"💾 Temp Dir: {tempfile.gettempdir()}")
    print(f"📊 Log Level: {'DEBUG' if os.environ.get('DEBUG') == '1' else 'INFO'}")
    print("=" * 60)
    
    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    
    application = None
    bot_manager = None
    cleanup_task = None
    
    def signal_handler():
        """Handle shutdown signals."""
        print("\n🛑 Received shutdown signal. Cleaning up...")
        if cleanup_task and not cleanup_task.done():
            cleanup_task.cancel()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        # Setup application
        application, bot_manager = setup_application()
        
        # Start cleanup task
        cleanup_task = asyncio.create_task(bot_manager.cleanup_temp_files())
        
        # Start the bot
        print("🚀 Starting bot...")
        await application.initialize()
        await application.start()
        print("✅ Bot started successfully!")
        print("📡 Listening for messages...")
        
        # Start polling with error handling
        await application.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            timeout=30,
            read_timeout=30,
            write_timeout=30,
            connect_timeout=30,
            pool_timeout=30
        )
        
        print("🤖 Bot is now running. Press Ctrl+C to stop.")
        
        # Keep running until shutdown
        await asyncio.Event().wait()
        
    except asyncio.CancelledError:
        print("🔴 Bot shutting down gracefully...")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        print(f"❌ Fatal error: {e}")
        traceback.print_exc()
    finally:
        # Cleanup
        print("🧹 Cleaning up resources...")
        try:
            if application:
                await application.stop()
                await application.shutdown()
            
            if bot_manager:
                await bot_manager.shutdown()
            
            if cleanup_task and not cleanup_task.done():
                cleanup_task.cancel()
                try:
                    await cleanup_task
                except asyncio.CancelledError:
                    pass
            
            print("✅ Cleanup complete. Goodbye!")
        except Exception as e:
            print(f"Error during cleanup: {e}")

if __name__ == '__main__':
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required!")
        sys.exit(1)
    
    # Check dependencies
    try:
        import yt_dlp
        from telegram import __version__ as telegram_version
        print(f"✅ yt-dlp: {yt_dlp.version.__version__}")
        print(f"✅ python-telegram-bot: {telegram_version}")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        sys.exit(1)
    
    # Run the bot
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)