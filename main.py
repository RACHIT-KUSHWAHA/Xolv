"""
Instagram Reels Downloader Bot
Highly optimized, asynchronous, and memory-efficient.
"""
import os
import re
import glob
import time
import html
import signal
import shutil
import asyncio
import logging
import psutil  # For server stats
import requests
from datetime import datetime
from dotenv import load_dotenv

# --- Asynchronous monkey-patch for Pyrogram on Python 3.11+ ---
import asyncio
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Pyrogram for high-speed Telegram interaction
import pyrogram
from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    InlineQuery, InlineQueryResultVideo, InputTextMessageContent
)
from pyrogram.enums import ParseMode
import yt_dlp

# --- FastAPI Web Dashboard Engine ---
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import uvicorn
import contextlib

# ------------------------------------------------------------------
# Initialization & Configuration
# ------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]  # Ensures output to stdout
)
logger = logging.getLogger(__name__)

# Credentials securely loaded from Environment Variables
API_ID = os.environ.get("API_ID", "").strip()
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
OWNER_ID = int(os.environ.get("OWNER_ID", "0").strip())
SUPPORT_GRP = os.environ.get("SUPPORT_GRP", "t.me/TheTimepassSquad")
OWNER_USER = os.environ.get("OWNER_USER", "@BeyondRachit")
XOLV_BRAND = "Xolv"

if not all([API_ID, API_HASH, BOT_TOKEN]):
    logger.error("Missing credentials in environment variables. Please check your .env file.")
    exit(1)

# Global states for Graceful Shutdown and Analytics
is_shutting_down = False
active_tasks = set()
BOT_START_TIME = time.time()
DOWNLOAD_SEMAPHORE = None  # Instantiated safely inside the main AsyncIO loop

# FastAPI Core Application
web_app = FastAPI(title="Media Downloader Dash")
templates = Jinja2Templates(directory="web_templates")

# User Tracking Storage
USERS_FILE = "users.txt"
tracked_users = set()

# Regex to detect links from various platforms (Instagram, TikTok, YouTube, Twitter/X, Pinterest, Facebook)
SUPPORTED_LINKS_REGEX = (
    r"https?://(?:www\.)?(?:instagram\.com|instagr\.am)/(?:p|reels?|tv|[\w.-]+)/[a-zA-Z0-9_-]+/?.*|"
    r"https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com)/.*|"
    r"https?://(?:www\.)?(?:youtube\.com|youtu\.be|m\.youtube\.com)/.*|"
    r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[a-zA-Z0-9_]+/status/[0-9]+/?.*|"
    r"https?://(?:www\.)?(?:pinterest\.com|pin\.it)/.*|"
    r"https?://(?:www\.)?(?:facebook\.com|fb\.watch)/.*"
)

# Shared dict to throttle progress bar edits preventing flood waits
last_update_times = {}

# ------------------------------------------------------------------
# Core Utilities
# ------------------------------------------------------------------

# Force sub removed for frictionless experience
# check_force_sub function deleted

def load_users():
    """Loads existing unique users from disk into the fast-access set."""
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            for line in f:
                try:
                    tracked_users.add(int(line.strip()))
                except ValueError:
                    pass
    logger.info(f"Loaded {len(tracked_users)} distinct users from database.")

def track_user(user_id: int):
    """Silently tracks a user id if they are new, appending to disk permanently."""
    if user_id not in tracked_users:
        tracked_users.add(user_id)
        with open(USERS_FILE, "a") as f:
            f.write(f"{user_id}\n")

# ------------------------------------------------------------------
# Core Utilities
# ------------------------------------------------------------------

def extract_video_info(url: str, message_id: int) -> dict:
    """
    Blocking yt-dlp network call to extract metadata, check sizes, and download/merge to disk.
    Runs inside a thread pool to preserve async event loop responsiveness.
    """
    os.makedirs("downloads", exist_ok=True)
    
    # --- Cobalt API Fallback for YouTube ---
    if "youtube.com" in url or "youtu.be" in url:
        cobalt_nodes = [
            'https://co.wuk.sh/api/json',
            'https://cobalt.q0.wtf/api/json',
            'https://api.cobalt.tools/api/json'
        ]
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://cobalt.tools',
            'Referer': 'https://cobalt.tools/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        payload = {'url': url}
        
        for cobalt_url in cobalt_nodes:
            try:
                response = requests.post(cobalt_url, headers=headers, json=payload, timeout=7)
                if response.status_code == 200:
                    data = response.json()
                    if 'url' in data:
                        return {
                            'url': data['url'],
                            'title': 'YouTube Video',
                            'extractor_key': 'youtube',
                            'filesize_approx': 0,
                            'duration': 0
                        }
            except Exception as e:
                logger.warning(f"Cobalt API node {cobalt_url} failed: {e}")
                continue
                
        raise ValueError("All backup download servers are currently blocked by YouTube. Please try again later.")
    # ---------------------------------------
    
    # Dynamically locate FFmpeg to bypass Windows terminal Path un-refreshing
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path and os.name == "nt":
        localappdata = os.environ.get("LOCALAPPDATA", "")
        if localappdata:
            matches = glob.glob(os.path.join(localappdata, "Microsoft", "WinGet", "Packages", "*", "*", "bin", "ffmpeg.exe"))
            if matches:
                ffmpeg_path = os.path.dirname(matches[0])

    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
        'outtmpl': f"downloads/{message_id}_%(id)s.%(ext)s",
        'merge_output_format': 'mp4',
        'http_chunk_size': 10485760,
        'concurrent_fragment_downloads': 1,
        'postprocessor_args': ['-threads', '1', '-preset', 'ultrafast'],
        'cookiefile': 'cookies.txt'
    }
    
    if ffmpeg_path:
        ydl_opts['ffmpeg_location'] = ffmpeg_path
        
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        # First extraction to check size limits
        info = ydl.extract_info(url, download=False)
        if not info:
            raise ValueError("Could not extract video info.")
            
        # Hardened Thumbnail Logic: Prefer high-quality static images
        if 'thumbnails' in info and info['thumbnails']:
            # Filter out non-traditional thumbnail formats if possible and sort by size
            static_thumbs = [t for t in info['thumbnails'] if t.get('url') and not t.get('url', '').endswith(('.webp', '.m3u8'))]
            if static_thumbs:
                info['thumbnail'] = sorted(static_thumbs, key=lambda x: (x.get('width', 0) or 0), reverse=True)[0]['url']
            
        return info

def download_video_to_disk(url: str, message_id: int, opts: dict, extract_info: dict = None) -> str:
    """Distinctly downloads the file to disk using yt-dlp or direct requests if fetched via Cobalt."""
    os.makedirs("downloads", exist_ok=True)
    
    if extract_info and extract_info.get('extractor_key') == 'youtube' and extract_info.get('url'):
        # Cobalt API returned a direct download link, use requests to download it
        direct_url = extract_info['url']
        final_file = f"downloads/{message_id}_cobalt.mp4"
        try:
            response = requests.get(direct_url, stream=True, timeout=60)
            response.raise_for_status()
            with open(final_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return final_file
        except Exception as e:
            logger.warning(f"Direct Cobalt download failed: {e}")
            raise ValueError("YouTube downloads are temporarily overloaded. Please try again later.")
    
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])
        
        # Locate the downloaded and merged file on disk
        files = glob.glob(f"downloads/{message_id}_*.*")
        final_file = None
        for f in files:
            if not f.endswith(('.part', '.ytdl', '.webm', '.m4a')):
                final_file = f
                break
        
        if not final_file and files:
            final_file = files[0]
            
        return final_file

async def progress_callback(current: int, total: int, message: Message, start_time: float, action_text: str):
    """
    Dynamic progress bar logic used for both aiohttp memory download and Pyrogram API upload.
    Respects Telegram's rate limit by updating at most once every 2 seconds.
    """
    now = time.time()
    
    # Throttle updates to 2 seconds
    if current < total and (now - last_update_times.get(message.id, 0)) < 2.0:
        return
        
    last_update_times[message.id] = now
    
    # Calculate percentage and UI components
    percentage = current * 100 / total if total > 0 else 0
    filled = int(percentage / 10)
    bar = "█" * filled + "░" * (10 - filled)
    
    # Calculate dynamic speed and size strings
    elapsed = now - start_time
    speed = current / elapsed if elapsed > 0 else 0
    speed_mb = speed / (1024 * 1024)
    current_mb = current / (1024 * 1024)
    total_mb = total / (1024 * 1024) if total > 0 else 0
    
    texto = (
        f"{action_text}\n\n"
        f"<code>[{bar}]</code> <b>{percentage:.1f}%</b>\n"
        f"⚡ <b>Speed:</b> {speed_mb:.2f} MB/s\n"
        f"📦 <b>Size:</b> {current_mb:.2f} / {total_mb:.2f} MB"
    )
    
    try:
        await message.edit_text(texto, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        # Ignore errors if message is not modified or flood-waited momentarily
        pass

# ------------------------------------------------------------------
# Message Handlers
# ------------------------------------------------------------------

async def handle_start_command(client: Client, message: Message):
    """
    Welcomes the user to Xolv with a premium, funky, minimal UI.
    """
    logger.info(f"Start command received from {message.from_user.id}")
    track_user(message.from_user.id)
    
    welcome_text = (
        f"💎 <b>{XOLV_BRAND} Boutique</b>\n"
        "〰〰〰〰〰〰〰〰〰〰\n"
        "The fastest way to catch media from Instagram, TikTok, YouTube & more.\n\n"
        "✨ <b>Features:</b>\n"
        "• No Ads, No Friction\n"
        "• Highest Quality\n"
        "• OLED Aesthetic\n\n"
        "🚀 <b>Just send me a link to catch it!</b>\n\n"
        f"👨‍💻 <b>Owner:</b> {OWNER_USER}"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Download via Browser", url="https://xolv.beyondrachit.me")],
        [InlineKeyboardButton("➕ Support Squad", url=f"https://{SUPPORT_GRP}")],
        [InlineKeyboardButton("👨‍💻 Admin", url=f"https://t.me/{OWNER_USER.replace('@', '')}")]
    ])
    
    await message.reply_text(
        text=welcome_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

async def handle_media_links(client: Client, message: Message):
    """
    Intercepts any message containing supported media links. 
    Enforces channel subscription, then extracts and uploads.
    """
    logger.info(f"Media link received from {message.from_user.id}: {message.text[:50]}...")
    global is_shutting_down
    if is_shutting_down:
        await message.reply_text("💤 <b>Bot is shutting down or restarting. Please try again later.</b>", parse_mode=ParseMode.HTML)
        return

    track_user(message.from_user.id)

    # Force sub logic removed

    # Track task for graceful shutdown
    current_task = asyncio.current_task()
    active_tasks.add(current_task)
    
    try:
        # Regex captures complete valid URLs directly
        raw_urls = re.findall(SUPPORTED_LINKS_REGEX, message.text)
        urls = list(set(raw_urls))
        
        # Process multiple links sequentially to avoid UI overlay collisions
        for url in urls:
            status_msg = await message.reply_text("⏳ <b>Analyzing Link & Queueing...</b>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            try:
                # --------------------- STRICT 1GB RAM CONCURRENCY LOCK ---------------------
                # We acquire the global lock. If 2 videos are already downloading, we wait gracefully.
                async with DOWNLOAD_SEMAPHORE:
                    await status_msg.edit_text("⏳ <b>Extracting internal manifests...</b>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                    try:
                        info = await asyncio.wait_for(
                            asyncio.to_thread(extract_video_info, url, message.id), 
                            timeout=300.0
                        )
                    except (asyncio.TimeoutError, TimeoutError):
                        await status_msg.edit_text("❌ <b>Extraction timed out.</b> The server network might be unstable.", parse_mode=ParseMode.HTML)
                        continue
                    except ValueError as ve:
                        raise ve
                        
                    await status_msg.edit_text("📥 <b>Downloading to Server Disk...</b>", parse_mode=ParseMode.HTML)
                    
                    # Re-build the options exactly as extract_video_info did to funnel the download
                    ffmpeg_path = shutil.which("ffmpeg")
                    ydl_opts = {
                        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
                        'quiet': True, 'no_warnings': True, 'geo_bypass': True,
                        'outtmpl': f"downloads/{message.id}_%(id)s.%(ext)s",
                        'merge_output_format': 'mp4',
                        'http_chunk_size': 10485760,
                        'concurrent_fragment_downloads': 1,
                        'postprocessor_args': ['-threads', '1', '-preset', 'ultrafast'],
                        'cookiefile': 'cookies.txt'
                    }
                    if ffmpeg_path: ydl_opts['ffmpeg_location'] = ffmpeg_path
                    
                    filepath = await asyncio.to_thread(download_video_to_disk, url, message.id, ydl_opts, info)
                # --------------------- END OF CONCURRENCY LOCK -----------------------------
                
                if not filepath or not os.path.exists(filepath):
                    raise ValueError("Download failed, file not found on disk.")
                
                file_size = os.path.getsize(filepath)
                if file_size > 50 * 1024 * 1024:
                    dest_dir = "/var/www/xolv/downloads/"
                    os.makedirs(dest_dir, exist_ok=True)
                    filename = os.path.basename(filepath)
                    dest_path = os.path.join(dest_dir, filename)
                    shutil.move(filepath, dest_path)
                    
                    public_url = f"https://xolv.beyondrachit.me/dl/{filename}"
                    title = info.get('title', 'Media Result')
                    platform = info.get('extractor_key', 'Link')
                    
                    caption_html = (
                        f"✨ <b>{html.escape(title[:60])}</b>\n"
                        f"🏷️ <code>{platform}</code>\n\n"
                        f"📦 <b>File too large for Telegram ({file_size // (1024*1024)}MB).</b>\n"
                        f"🔗 <b>Download Link:</b> <a href='{public_url}'>Click Here to Download</a>\n\n"
                        f"💎 <b>{XOLV_BRAND} Elite</b>"
                    )
                    
                    inline_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("🌐 Direct Download", url=public_url)],
                        [InlineKeyboardButton("🔗 Origin", url=url), InlineKeyboardButton("➕ Support", url=f"https://{SUPPORT_GRP}")]
                    ])
                    
                    await status_msg.edit_text(caption_html, parse_mode=ParseMode.HTML, reply_markup=inline_kb)
                    
                    try:
                        await message.delete()
                    except Exception:
                        pass
                else:
                    # Format Funky Minimal Metadata
                    title = info.get('title', 'Media Result')
                    platform = info.get('extractor_key', 'Link')
                    duration = info.get('duration', 0)
                    
                    # Funky caption
                    caption_html = (
                        f"✨ <b>{html.escape(title[:60])}</b>\n"
                        f"🏷️ <code>{platform}</code>\n\n"
                        f"💎 <b>{XOLV_BRAND} Elite</b>"
                    )
                    
                    inline_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("🌐 Download via Browser", url="https://xolv.beyondrachit.me")],
                        [
                            InlineKeyboardButton("🔗 Origin", url=url),
                            InlineKeyboardButton("➕ Support", url=f"https://{SUPPORT_GRP}")
                        ]
                    ])
                    
                    # Stream from Disk to Telegram API
                    await status_msg.edit_text("🚀 <b>Uploading to Telegram...</b>", parse_mode=ParseMode.HTML)
                    start_time = time.time()
                    
                    await client.send_video(
                        chat_id=message.chat.id,
                        video=filepath,
                        caption=caption_html,
                        duration=int(duration) if duration else 0,
                        reply_markup=inline_kb,
                        reply_to_message_id=message.id,
                        parse_mode=ParseMode.HTML,
                        progress=progress_callback,
                        progress_args=(status_msg, start_time, "🚀 <b>Uploading to Telegram...</b>")
                    )
                    
                    # Auto-Cleanup User's Link Message for aesthetics (Silently ignore if unauthorized)
                    try:
                        await message.delete()
                    except Exception:
                        pass
                    
                    # Cleanup Progress UI
                    await status_msg.delete()
                
            except Exception as e:
                logger.error(f"Error processing {url}: {e}", exc_info=True)
                await status_msg.edit_text(f"❌ <b>Error processing link:</b> <code>{html.escape(str(e)[:200])}</code>", parse_mode=ParseMode.HTML)
            finally:
                # Clear tracking
                last_update_times.pop(status_msg.id, None)
                # CRITICAL: Disk cleanup for this specific message.id
                lingering_files = glob.glob(f"downloads/{message.id}_*.*")
                for f in lingering_files:
                    try:
                        os.remove(f)
                    except Exception as cleanup_err:
                        logger.warning(f"Failed to delete {f}: {cleanup_err}")
                
    finally:
        active_tasks.remove(current_task)

async def handle_check_join(client: Client, query: CallbackQuery):
    """Callback for join check. Not needed anymore but kept for compatibility."""
    await query.answer("✅ Verification no longer required! Enjoy Xolv.", show_alert=True)
    await query.message.delete()

async def handle_stats_command(client: Client, message: Message):
    """Admin command displaying server details, users, and health."""
    track_user(message.from_user.id)
    
    if OWNER_ID and message.from_user.id != OWNER_ID:
        return # Stats restricted to owner
        
    # Server Metrics
    cpu_usage = psutil.cpu_percent()
    ram = psutil.virtual_memory()
    uptime_seconds = int(time.time() - BOT_START_TIME)
    
    stats_text = (
        f"📊 <b>{XOLV_BRAND} Server Metrics</b>\n"
        "〰〰〰〰〰〰〰〰〰〰\n"
        f"👥 <b>Total Users:</b> <code>{len(tracked_users)}</code>\n"
        f"⚡ <b>Active Tasks:</b> <code>{len(active_tasks)}</code>\n\n"
        f"🖥️ <b>CPU Usage:</b> <code>{cpu_usage}%</code>\n"
        f"🧠 <b>RAM Usage:</b> <code>{ram.percent}%</code>\n"
        f"⏱️ <b>Uptime:</b> <code>{uptime_seconds // 3600}h {(uptime_seconds % 3600) // 60}m</code>\n"
        f"💾 <b>Disk:</b> <code>{psutil.disk_usage('/').percent}%</code>"
    )
    await message.reply_text(stats_text, parse_mode=ParseMode.HTML)

async def handle_broadcast_command(client: Client, message: Message):
    """Owner restricted command to send a message to all tracked users."""
    if not OWNER_ID or message.from_user.id != OWNER_ID:
        # Silently ignore unauthorized attempts
        return
        
    broadcast_text = message.text.replace("/broadcast", "").strip()
    if not broadcast_text:
        await message.reply_text("⚠️ <b>Usage:</b> <code>/broadcast [your message]</code>", parse_mode=ParseMode.HTML)
        return
        
    status_msg = await message.reply_text(f"🚀 <b>Starting Broadcast to {len(tracked_users)} users...</b>", parse_mode=ParseMode.HTML)
    
    success = 0
    failed = 0
    start_time = time.time()
    
    # Isolate targets (creates a static copy to iterate safely)
    targets = list(tracked_users)
    
    for user_id in targets:
        try:
            await client.send_message(
                chat_id=user_id, 
                text=broadcast_text,
                disable_web_page_preview=True
            )
            success += 1
            # Respect Telegram rate limits of 30 msgs/second globally
            await asyncio.sleep(0.05)
        except pyrogram.errors.FloodWait as e:
            # Important: Absolute bypass against Telegram server-side IP blocks
            await asyncio.sleep(e.value + 1)
            try:
                await client.send_message(chat_id=user_id, text=broadcast_text)
                success += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
            
    elapsed = f"{(time.time() - start_time):.2f}"
    report = (
        "📣 <b>Broadcast Complete!</b>\n\n"
        f"✅ <b>Success:</b> <code>{success}</code> users\n"
        f"❌ <b>Failed/Blocked:</b> <code>{failed}</code> users\n"
        f"⏱️ <b>Time Taken:</b> <code>{elapsed}s</code>"
    )
    await status_msg.edit_text(report, parse_mode=ParseMode.HTML)

# ------------------------------------------------------------------
# Phase 2: Inline Query Mode (Zero-Bandwidth Telegram Feature)
# ------------------------------------------------------------------

async def handle_inline_query(client: Client, query: InlineQuery):
    """
    Triggers when a user types `@bot_username <link>` in ANY chat.
    We extract the target streaming URL directly without downloading, and serve it instantly!
    """
    query_text = query.query.strip()
    
    # Use re.search and handle None explicitly to prevent crashes
    url_match = re.search(SUPPORTED_LINKS_REGEX, query_text)
    if not url_match:
        return
        
    url = url_match.group(0)
    
    try:
        # Notice we extract info with Semaphore lock to prevent rate-limit flooding in case of spam
        async with DOWNLOAD_SEMAPHORE:
            # We pass a placeholder id since we aren't saving to disk
            info = await asyncio.wait_for(
                asyncio.to_thread(extract_video_info, url, int(time.time())), 
                timeout=20.0
            )
            
        filesize = info.get('filesize') or info.get('filesize_approx') or 0
        if filesize > 50 * 1024 * 1024:
            return # Can't inline stream >50MB cleanly
            
        title = info.get('title', 'Media Result')
        thumb_url = info.get('thumbnail', '')
        
        # Priority: Direct MP4 format (highest quality but strictly MP4)
        stream_url = None
        if 'formats' in info:
            # Filter for mp4 formats with both video and audio
            valid_mp4s = [
                f for f in info['formats'] 
                if f.get('ext') == 'mp4' 
                and f.get('url') 
                and f.get('vcodec') != 'none' 
                and f.get('acodec') != 'none'
            ]
            if valid_mp4s:
                stream_url = valid_mp4s[-1]['url']
        
        # Fallback to general url if no perfect mp4 found
        if not stream_url:
            stream_url = info.get('url')
            
        if not stream_url:
            return
            
        results = [
            InlineQueryResultVideo(
                video_url=stream_url,
                thumb_url=thumb_url or stream_url,
                title=title[:60],
                mime_type="video/mp4",
                video_duration=int(info.get('duration', 0))
            )
        ]
        # set is_personal=True to avoid server-side caching issues for these expiring URLs
        await query.answer(results, cache_time=0, is_personal=True)
    except Exception as e:
        logger.warning(f"Inline Query Extraction failed: {e}")

# ------------------------------------------------------------------
# Phase 2: Web Dashboard Routing (FastAPI)
# ------------------------------------------------------------------

class ExtractionRequest(BaseModel):
    url: str

@web_app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@web_app.post("/api/extract")
async def api_extract_media(req: ExtractionRequest):
    """
    Boutique Extraction API.
    Provides a primary playback URL and a curated list of download options.
    """
    url = req.url.strip()
    if not url or not re.match(SUPPORTED_LINKS_REGEX, url):
         raise HTTPException(status_code=400, detail="Invalid Support Link format.")
         
    try:
        async with DOWNLOAD_SEMAPHORE:
            info = await asyncio.wait_for(
                asyncio.to_thread(extract_video_info, url, int(time.time())), 
                timeout=45.0
            )

        filesize = info.get('filesize') or info.get('filesize_approx') or 0
        if filesize > 50 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File too large for direct web extraction (>50MB).")

        # Sort and filter for the best playable combined format
        formats_list = []
        playback_url = info.get('url') # Default
        
        # Prefer progressive MP4s with both audio and video for the web player
        if 'formats' in info:
            all_fmts = info['formats']
            progressive = [f for f in all_fmts if f.get('vcodec') != 'none' and f.get('acodec') != 'none' and f.get('url') and f.get('ext') == 'mp4']
            if progressive:
                # Pick the highest resolution progressive MP4
                best_prog = sorted(progressive, key=lambda x: (x.get('width', 0) or 0), reverse=True)[0]
                playback_url = best_prog['url']

        # Construct download format cards
        if playback_url:
            formats_list.append({
                "label": "High Quality Video",
                "url": playback_url,
                "ext": "mp4",
                "type": "video",
                "quality": info.get('resolution', 'HD')
            })

        # Add Audio Only fallback
        if 'formats' in info:
            audio_only = [f for f in info['formats'] if f.get('vcodec') == 'none' and f.get('acodec') != 'none' and f.get('url')]
            if audio_only:
                best_audio = sorted(audio_only, key=lambda x: x.get('abr', 0) or 0)[-1]
                formats_list.append({
                    "label": "Music / Audio Only",
                    "url": best_audio['url'],
                    "ext": best_audio.get('ext', 'm4a'),
                    "type": "audio",
                    "quality": f"{int(best_audio.get('abr', 0) or 128)}kbps"
                })

        return {
            "success": True,
            "title": info.get('title', 'Extracted Media'),
            "thumbnail": info.get('thumbnail', ''),
            "duration": info.get('duration', 0),
            "uploader": info.get('uploader', 'Unknown'),
            "playback_url": playback_url,
            "formats": formats_list,
            "original_url": url
        }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Extraction timed out. Try again.")
    except Exception as e:
        logger.error(f"MediaBox API Error: {e}")
        raise HTTPException(status_code=500, detail="Extraction Engine Error. The link might be expired or restricted.")

# ------------------------------------------------------------------
# Main Loop and Graceful Shutdown
# ------------------------------------------------------------------

async def cleanup_task():
    """Background task to delete files older than 24 hours from public downloads."""
    dest_dir = "/var/www/xolv/downloads/"
    while True:
        try:
            if os.path.exists(dest_dir):
                now = time.time()
                for filename in os.listdir(dest_dir):
                    file_path = os.path.join(dest_dir, filename)
                    if os.path.isfile(file_path):
                        if os.stat(file_path).st_mtime < now - 24 * 3600:
                            os.remove(file_path)
                            logger.info(f"Deleted old file: {file_path}")
        except Exception as e:
            logger.error(f"Cleanup task error: {e}")
        await asyncio.sleep(3600)

async def main():
    global is_shutting_down, DOWNLOAD_SEMAPHORE
    
    # 1. Initialize Concurrency Control (Max 2 simultaneous tasks for 1GB RAM)
    DOWNLOAD_SEMAPHORE = asyncio.Semaphore(2)
    
    # 2. Setup Bot Client
    from pyrogram.handlers import MessageHandler, CallbackQueryHandler, InlineQueryHandler
    app = Client(
        "xolv_production",
        api_id=int(API_ID),
        api_hash=API_HASH,
        bot_token=BOT_TOKEN
    )
    
    # Register Core Bot Handlers
    app.add_handler(MessageHandler(handle_start_command, filters.command("start") & filters.private))
    app.add_handler(MessageHandler(handle_stats_command, filters.command("stats") & filters.private))
    app.add_handler(MessageHandler(handle_broadcast_command, filters.command("broadcast") & filters.private))
    app.add_handler(MessageHandler(handle_media_links, filters.regex(SUPPORTED_LINKS_REGEX) & filters.private))
    app.add_handler(CallbackQueryHandler(handle_check_join, filters.regex("^check_join$")))
    app.add_handler(InlineQueryHandler(handle_inline_query))
    
    # Hydrate tracked users at boot
    load_users()

    # Start the Bot
    try:
        await app.start()
        bot_info = await app.get_me()
        logger.info(f"✅ Bot success: @{bot_info.username} (ID: {bot_info.id})")
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}", exc_info=True)
        return

    # -----------------------------------------------------
    # Spin up Uvicorn (FastAPI) inside the Pyrogram Event Loop
    # -----------------------------------------------------
    loop = asyncio.get_running_loop()
    
    config = uvicorn.Config(app=web_app, host="0.0.0.0", port=8000, loop="none")
    server = uvicorn.Server(config)
    
    # Deploy fastapi server into a concurrent task
    fastapi_task = loop.create_task(server.serve())
    logger.info("⚡ Web Dashboard launched on port 8000.")

    # Start cleanup task
    cleanup_bg_task = loop.create_task(cleanup_task())

    # Use pyrogram.idle() to gracefully block and handle signals while dispatching updates
    await idle()

    # Graceful Shutdown Sequence
    if active_tasks:
        logger.info(f"Waiting for {len(active_tasks)} active task(s) to finalize...")
        await asyncio.gather(*active_tasks, return_exceptions=True)

    server.should_exit = True
    await fastapi_task
    cleanup_bg_task.cancel()
    await app.stop()
    logger.info("Cleanup complete. Container exiting.")

if __name__ == "__main__":
    asyncio.run(main())
