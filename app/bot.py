#!/usr/bin/env python3
"""
ClipNova Telegram Bot - Professional YouTube Downloader
"""

import os
import re
import logging
import asyncio
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("No BOT_TOKEN found in environment variables")

# Constants
MAX_CONCURRENT_DOWNLOADS = 3
DOWNLOAD_TIMEOUT = 3600  # 1 hour
USER_RATE_LIMIT = 5  # downloads per minute
MAX_RETRIES = 3  # Max retries for file operations
FILE_CLOSE_WAIT = 2  # Seconds to wait before retrying file operations

# Data structures
@dataclass
class DownloadOption:
    format_id: str
    resolution: str
    extension: str
    label: str
    filesize: str = "N/A"

# User session management
user_sessions: Dict[int, Dict] = {}
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Global application reference
app = None

# Messages
START_MESSAGE = """
🎬 *Welcome to ClipNova Bot* 🎬

The most advanced YouTube downloader on Telegram!

✨ *Features*:
- Download videos in multiple resolutions
- Real-time progress with speed and ETA
- File size information before download
- Fast and reliable downloads

Send me a YouTube link to get started!
"""

HELP_MESSAGE = """
🛠️ *ClipNova Bot Help* 🛠️

*Available Commands*:
/start - Show welcome message
/help - Display this help message
/cancel - Cancel current operation

*How to Use*:
1. Send a YouTube URL
2. Choose your preferred format
3. Wait for download to complete

For support, contact @your_support_handle
"""

async def safe_delete_file(file_path: str, max_retries: int = MAX_RETRIES) -> bool:
    """Safely delete a file with retries and delays."""
    for attempt in range(max_retries):
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
            return False
        except PermissionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"File delete failed (attempt {attempt + 1}), retrying...")
                await asyncio.sleep(FILE_CLOSE_WAIT)
            else:
                logger.error(f"Failed to delete file after {max_retries} attempts: {e}")
                return False
    return False

async def extract_video_id(url: str) -> Optional[str]:
    """Extract YouTube video ID from URL."""
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"youtu.be\/([0-9A-Za-z_-]{11})",
        r"youtube.com\/shorts\/([0-9A-Za-z_-]{11})"
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

async def get_video_info(url: str) -> Dict:
    """Get video information using yt-dlp."""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
    }
    with YoutubeDL(ydl_opts) as ydl:
        try:
            return await asyncio.to_thread(ydl.extract_info, url, download=False)
        except DownloadError as e:
            logger.error(f"Failed to get video info: {e}")
            raise

async def get_available_formats(url: str) -> List[DownloadOption]:
    """Get available video formats for the URL."""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
    }
    with YoutubeDL(ydl_opts) as ydl:
        try:
            info = await asyncio.to_thread(ydl.extract_info, url, download=False)
            formats = info.get('formats', [])
            
            available_options = []
            seen_resolutions = set()
            
            # Get video formats
            for f in formats:
                if f.get('vcodec') != 'none' and f.get('acodec') != 'none':  # Video+audio formats
                    height = f.get('height')
                    if height and height >= 144:  # Minimum quality we'll show
                        resolution = f"{height}p"
                        if resolution not in seen_resolutions:
                            filesize = f.get('filesize') or f.get('filesize_approx')
                            filesize_str = f"{filesize / (1024 * 1024):.1f}MB" if filesize else "N/A"
                            
                            available_options.append(
                                DownloadOption(
                                    format_id=f.get('format_id'),
                                    resolution=resolution,
                                    extension=f.get('ext', 'mp4'),
                                    label=f"🎬 {resolution} ({filesize_str})",
                                    filesize=filesize_str
                                )
                            )
                            seen_resolutions.add(resolution)
            
            # Sort by resolution (high to low)
            available_options.sort(key=lambda x: int(x.resolution[:-1]), reverse=True)
            
            return available_options
            
        except Exception as e:
            logger.error(f"Error getting available formats: {e}")
            raise

async def send_thumbnail(update: Update, info: Dict):
    """Send video thumbnail to user."""
    try:
        thumbnail_url = info.get('thumbnail') or info.get('thumbnails', [{}])[0].get('url')
        if thumbnail_url:
            await update.message.reply_photo(
                photo=thumbnail_url,
                caption=f"📌 *{info['title']}*\n🕒 Duration: {info.get('duration_string', 'N/A')}",
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"Failed to send thumbnail: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    user = update.effective_user
    logger.info(f"User {user.full_name} (ID: {user.id}) started the bot")
    await update.message.reply_text(
        START_MESSAGE,
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command."""
    await update.message.reply_text(
        HELP_MESSAGE,
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

async def handle_youtube_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle YouTube URL input."""
    url = update.message.text
    user_id = update.effective_user.id
    
    try:
        video_id = await extract_video_id(url)
        if not video_id:
            await update.message.reply_text("❌ Invalid YouTube URL. Please try again.")
            return
        
        await update.message.reply_text("🔍 Fetching video information...")
        info = await get_video_info(url)
        available_formats = await get_available_formats(url)
        
        if not available_formats:
            await update.message.reply_text("❌ No downloadable formats found for this video.")
            return
        
        user_sessions[user_id] = {
            'url': url,
            'info': info,
            'available_formats': available_formats,
            'current_step': 'format_selection'
        }
        
        await send_thumbnail(update, info)
        await send_format_selection(update, available_formats)
        
    except Exception as e:
        logger.error(f"Error processing URL: {e}")
        await update.message.reply_text("❌ Failed to process this video. Please try another URL.")

async def send_format_selection(update: Update, formats: List[DownloadOption]):
    """Send format selection keyboard with available formats."""
    buttons = []
    
    # Group formats 2 per row for better layout
    for i in range(0, len(formats), 2):
        row = []
        if i < len(formats):
            row.append(InlineKeyboardButton(formats[i].label, callback_data=f"format_{formats[i].format_id}"))
        if i+1 < len(formats):
            row.append(InlineKeyboardButton(formats[i+1].label, callback_data=f"format_{formats[i+1].format_id}"))
        if row:
            buttons.append(row)
    
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    
    await update.message.reply_text(
        "📌 Available formats (with approximate sizes):\n"
        "Please select your preferred quality:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    await query.answer()
    
    try:
        if query.data.startswith("format_"):
            await handle_format_selection(query, query.data)
        elif query.data == "confirm_download":
            await start_download(query)
        elif query.data == "cancel":
            await cancel_operation(query)
    except Exception as e:
        logger.error(f"Button handler error: {e}")
        await query.edit_message_text("waiting for downloading...")

async def handle_format_selection(query, format_id: str):
    """Handle format selection."""
    user_id = query.from_user.id
    if user_id not in user_sessions:
        await query.edit_message_text("❌ Session expired. Please send the URL again.")
        return
    
    clean_format_id = format_id.replace("format_", "")
    available_formats = user_sessions[user_id]['available_formats']
    
    selected_format = next(
        (opt for opt in available_formats if opt.format_id == clean_format_id),
        None
    )
    
    if not selected_format:
        await query.edit_message_text("❌ Invalid selection. Please try again.")
        return
    
    user_sessions[user_id]['selected_format'] = selected_format
    info = user_sessions[user_id]['info']
    
    await query.edit_message_text(
        f"📌 *{info.get('title', 'Untitled')}*\n\n"
        f"🔹 Quality: {selected_format.resolution}\n"
        f"🔹 Size: {selected_format.filesize}\n"
        f"🔹 Format: {selected_format.extension.upper()}\n\n"
        "Would you like to start the download?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Download Now", callback_data="confirm_download")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
        ])
    )

async def download_progress_hook(d, query):
    """Update download progress with detailed information."""
    if d['status'] == 'downloading':
        try:
            # Calculate downloaded percentage
            percent = d.get('_percent_str', '0%')
            
            # Get download speed
            speed = d.get('_speed_str', 'N/A')
            
            # Get ETA
            eta = d.get('_eta_str', 'N/A')
            
            # Get downloaded and total size if available
            downloaded = d.get('_downloaded_bytes_str', '0B')
            total = d.get('_total_bytes_str') or d.get('_total_bytes_estimate_str', '?')
            
            progress_message = (
                f"📥 *Downloading...*\n\n"
                f"├ Progress: `{percent}`\n"
                f"├ Speed: `{speed}`\n"
                f"├ Downloaded: `{downloaded}` of `{total}`\n"
                f"└ ETA: `{eta}`"
            )
            
            # Use the application's update queue to properly schedule the edit
            await query.edit_message_text(
                progress_message,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Progress update error: {e}")

async def start_download(query):
    """Handle the download process."""
    user_id = query.from_user.id
    if user_id not in user_sessions or 'selected_format' not in user_sessions[user_id]:
        await query.edit_message_text("❌ Session expired. Please send the URL again.")
        return

    url = user_sessions[user_id]['url']
    selected_format = user_sessions[user_id]['selected_format']
    video_title = user_sessions[user_id]['info'].get('title', 'video')
    
    try:
        await query.edit_message_text("⏳ Preparing download...")
        
        # Clean up filename to remove special characters
        clean_title = re.sub(r'[^\w\-_\. ]', '', video_title)
        output_template = f"{clean_title}.%(ext)s"
        
        # Create a synchronous progress hook that schedules the coroutine
        def create_sync_progress_hook(query):
            def sync_progress_hook(d):
                """Synchronous wrapper for the progress hook."""
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(download_progress_hook(d, query))
                except RuntimeError:
                    pass
            return sync_progress_hook
        
        ydl_opts = {
            'format': selected_format.format_id,
            'quiet': True,
            'progress_hooks': [create_sync_progress_hook(query)],
            'outtmpl': output_template,
            'noplaylist': True,
            'merge_output_format': 'mp4',
            'socket_timeout': 30,
            'retries': 3,
        }

        await query.edit_message_text("🚀 Starting download...")
        
        with YoutubeDL(ydl_opts) as ydl:
            async with download_semaphore:
                try:
                    # Use asyncio.to_thread for synchronous yt-dlp operations
                    info_dict = await asyncio.wait_for(
                        asyncio.to_thread(ydl.extract_info, url, download=True),
                        timeout=DOWNLOAD_TIMEOUT
                    )
                    
                    temp_file = ydl.prepare_filename(info_dict)
                    
                    if os.path.exists(temp_file):
                        resolution = selected_format.resolution
                        filesize = os.path.getsize(temp_file) / (1024 * 1024)  # in MB
                        
                        # Send the video file
                        try:
                            with open(temp_file, 'rb') as video_file:
                                await query.message.reply_video(
                                    video=video_file,
                                    caption=f"✅ *{clean_title}*\n"
                                           f"🔹 Resolution: {resolution}\n"
                                           f"🔹 Size: {filesize:.1f}MB",
                                    filename=f"{clean_title}.mp4",
                                    supports_streaming=True,
                                    parse_mode="Markdown"
                                )
                        finally:
                            # Ensure file is closed before attempting to delete
                            await safe_delete_file(temp_file)
                        
                        if user_id in user_sessions:
                            del user_sessions[user_id]
                    else:
                        await query.message.reply_text("❌ Failed to download the file.")
                
            
                except Exception as e:
                    raise
    
    except DownloadError as e:
        error_msg = str(e)
        if "Requested format is not available" in error_msg:
            await query.message.reply_text(
                f"❌ The selected quality isn't available for this video.\n"
                "Please try a different quality."
            )
 
    finally:
        if 'temp_file' in locals() and temp_file and os.path.exists(temp_file):
            await safe_delete_file(temp_file)

async def cancel_operation(query):
    """Cancel the current operation."""
    user_id = query.from_user.id
    if user_id in user_sessions:
        del user_sessions[user_id]
    await query.edit_message_text("❌ Operation cancelled.")

def main() -> None:
    """Start the bot."""
    global app  # Make app global so progress hook can access it
    
    try:
        logger.info("Starting ClipNova Bot...")
        
        app = ApplicationBuilder() \
            .token(BOT_TOKEN) \
            .concurrent_updates(True) \
            .build()

        # Command handlers
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("cancel", cancel_operation))
        
        # Message handlers
        app.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(r'(youtube\.com|youtu\.be)'),
            handle_youtube_url
        ))
        
        # Button handlers
        app.add_handler(CallbackQueryHandler(button_handler))
        
        logger.info("Bot is running...")
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
    except Exception as e:
        logger.error(f"Bot failed: {e}")
        raise

if __name__ == "__main__":
    main()