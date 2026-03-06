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
from dotenv import load_dotenv

# --- Asynchronous monkey-patch for Pyrogram on Python 3.11+ ---
import asyncio
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Pyrogram for high-speed Telegram interaction
import pyrogram
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
import yt_dlp

# ------------------------------------------------------------------
# Initialization & Configuration
# ------------------------------------------------------------------
load_dotenv()

# Setup structured stdout logging for container managers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]  # Ensures output to stdout
)
logger = logging.getLogger(__name__)

# Credentials securely loaded from Environment Variables
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "stuffsroom") # Without the @

if not all([API_ID, API_HASH, BOT_TOKEN]):
    logger.error("Missing credentials in environment variables. Please check your .env file.")
    exit(1)

# Global states for Graceful Shutdown
is_shutting_down = False
active_tasks = set()

# Regex to detect links from various platforms (Instagram, TikTok, YouTube, Twitter/X, Pinterest, Facebook)
SUPPORTED_LINKS_REGEX = (
    r"https?://(?:www\.)?(?:instagram\.com|instagr\.am)/[a-zA-Z0-9_.-]+/?.*|"
    r"https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com)/.*|"
    r"https?://(?:www\.)?(?:youtube\.com/shorts/|youtu\.be/|youtube\.com/watch\?v=)[a-zA-Z0-9_-]+|"
    r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[a-zA-Z0-9_]+/status/[0-9]+|"
    r"https?://(?:www\.)?(?:pinterest\.com|pin\.it)/.*|"
    r"https?://(?:www\.)?(?:facebook\.com|fb\.watch)/.*"
)

# Shared dict to throttle progress bar edits preventing flood waits
last_update_times = {}

# ------------------------------------------------------------------
# Core Utilities
# ------------------------------------------------------------------

async def check_force_sub(client: Client, user_id: int) -> bool | str:
    """
    Checks if the user is a member of the mandatory Updates Channel.
    Returns True if subscribed, False if not, and a string error if the bot is misconfigured.
    """
    if not FORCE_SUB_CHANNEL:
        return True
    try:
        member = await client.get_chat_member(f"@{FORCE_SUB_CHANNEL}", user_id)
        if member.status in [
            pyrogram.enums.ChatMemberStatus.MEMBER,
            pyrogram.enums.ChatMemberStatus.ADMINISTRATOR,
            pyrogram.enums.ChatMemberStatus.OWNER
        ]:
            return True
        return False
    except pyrogram.errors.UserNotParticipant:
        return False
    except Exception as e:
        logger.warning(f"Error checking force sub for {user_id}: {e}")
        # To strictly enforce, if we can't verify (e.g. bot not admin in channel), block them.
        # The bot MUST be an admin in @stuffsroom for this to work.
        if "CHAT_ADMIN_REQUIRED" in str(e).upper():
            return "admin_required"
        return False

# ------------------------------------------------------------------
# Core Utilities
# ------------------------------------------------------------------

def extract_video_info(url: str, message_id: int) -> dict:
    """
    Blocking yt-dlp network call to extract metadata, check sizes, and download/merge to disk.
    Runs inside a thread pool to preserve async event loop responsiveness.
    """
    os.makedirs("downloads", exist_ok=True)
    
    # Dynamically locate FFmpeg to bypass Windows terminal Path un-refreshing
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path and os.name == "nt":
        localappdata = os.environ.get("LOCALAPPDATA", "")
        if localappdata:
            matches = glob.glob(os.path.join(localappdata, "Microsoft", "WinGet", "Packages", "*", "*", "bin", "ffmpeg.exe"))
            if matches:
                ffmpeg_path = os.path.dirname(matches[0])

    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'quiet': True,
        'no_warnings': True,
        'geo_bypass': True,
        'outtmpl': f"downloads/{message_id}_%(id)s.%(ext)s",
        'merge_output_format': 'mp4'
    }
    
    if ffmpeg_path:
        ydl_opts['ffmpeg_location'] = ffmpeg_path
        
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        # First extraction to check size limits
        info = ydl.extract_info(url, download=False)
        if not info:
            raise ValueError("Could not extract video info.")
            
        filesize = info.get('filesize') or info.get('filesize_approx') or 0
        if filesize > 50 * 1024 * 1024:
            raise ValueError("TooLarge")
            
        # Download and allow FFmpeg to merge video + audio
        ydl.download([url])
        
        # Locate the downloaded and merged file on disk
        files = glob.glob(f"downloads/{message_id}_*.*")
        final_file = None
        for f in files:
            # We want to ignore partial parts or unmerged streams
            if not f.endswith(('.part', '.ytdl', '.webm', '.m4a')):
                final_file = f
                break
        
        # Fallback if only one file exists
        if not final_file and files:
            final_file = files[0]
            
        info['filepath'] = final_file
        return info

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
    Welcomes the user, explains features, and provides links to the Owner.
    """
    welcome_text = (
        "👋 <b>Welcome to the Ultimate Media Downloader Bot!</b>\n\n"
        "I can download videos from multiple platforms in <b>Highest Quality</b> straight to Telegram!\n\n"
        "✨ <b>Supported Platforms:</b>\n"
        "• � Instagram (Reels, Posts, IGTV, Stories)\n"
        "• 🎵 TikTok (No Watermark)\n"
        "• � YouTube (Shorts & Regular Videos)\n"
        "• 🐦 Twitter / X\n"
        "• � Pinterest\n"
        "• 📘 Facebook Videos\n\n"
        "👇 <b>How to use:</b>\n"
        "Just send me <b>any link</b> and I will do the rest automatically!\n\n"
        "👨‍💻 <b>Owner & Developer:</b> <a href='https://t.me/beyondrachit'>@beyondrachit</a>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("� Join Updates Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")],
        [InlineKeyboardButton("👨‍� Contact Owner", url="https://t.me/beyondrachit")]
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
    global is_shutting_down
    if is_shutting_down:
        await message.reply_text("💤 <b>Bot is shutting down or restarting. Please try again later.</b>", parse_mode=ParseMode.HTML)
        return

    # 1. Force Subscribe Check
    is_subscribed = await check_force_sub(client, message.from_user.id)
    if is_subscribed == "admin_required":
        await message.reply_text("⚠️ <b>Bot Configuration Error:</b> The bot must be made an Administrator in the Updates Channel before it can verify members.", parse_mode=ParseMode.HTML)
        return
    elif not is_subscribed:
        join_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📣 Join Channel to Use Bot", url=f"https://t.me/{FORCE_SUB_CHANNEL}")],
            [InlineKeyboardButton("🔄 I have joined! Try again.", callback_data="check_join")]
        ])
        await message.reply_text(
            "🔒 <b>You must join our Updates Channel first!</b>\n\n"
            "This bot is completely free, but to keep using it, please join the channel below.",
            reply_markup=join_kb,
            parse_mode=ParseMode.HTML,
            reply_to_message_id=message.id
        )
        return

    # Track task for graceful shutdown
    current_task = asyncio.current_task()
    active_tasks.add(current_task)
    
    try:
        # Regex captures complete valid URLs directly
        # Use set() to deduplicate in case regex catches multiple variations of the same link
        raw_urls = re.findall(SUPPORTED_LINKS_REGEX, message.text)
        urls = list(set(raw_urls))
        
        # Process multiple links sequentially to avoid UI overlay collisions
        for url in urls:
            status_msg = await message.reply_text("⏳ <b>Analyzing Link...</b>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            try:
                # Threaded yt-dlp metadata extraction and disk download with 5-minute timeout netting
                try:
                    info = await asyncio.wait_for(
                        asyncio.to_thread(extract_video_info, url, message.id), 
                        timeout=300.0
                    )
                except (asyncio.TimeoutError, TimeoutError):
                    await status_msg.edit_text("❌ <b>Extraction timed out.</b> The server network might be unstable or the file is too large.", parse_mode=ParseMode.HTML)
                    continue
                except ValueError as ve:
                    if str(ve) == "TooLarge":
                        await status_msg.edit_text("❌ File too large! Please keep links under 50MB to prevent server crashes.", parse_mode=ParseMode.HTML)
                        continue
                    raise ve
                    
                filepath = info.get('filepath')
                if not filepath or not os.path.exists(filepath):
                    raise ValueError("Download failed, file not found on disk.")
                
                # Format Premium Metadata
                title = info.get('title', 'Media Video') or 'Media Video'
                uploader = info.get('uploader', 'Unknown User')
                caption_text = info.get('description', '') or title
                duration = info.get('duration', 0)
                width = info.get('width', 0)
                height = info.get('height', 0)
                resolution = f"{width}x{height}" if width and height else "Unknown"
                platform = info.get('extractor_key', 'Unknown')
                
                # Build rich HTML caption
                caption_html = (
                    f"🎥 <b>{html.escape(title[:60])}</b>\n"
                    f"👤 <b>By:</b> <code>{html.escape(uploader)}</code>\n"
                    f"📐 <b>Res:</b> {resolution} | <b>Platform:</b> {platform}\n\n"
                    f"📝 {html.escape(caption_text[:150])}... \n\n"
                    f"✨ <i>Downloaded via <a href='https://t.me/{FORCE_SUB_CHANNEL}'>@{FORCE_SUB_CHANNEL}</a></i>"
                )
                
                inline_kb = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("🔗 Original Post", url=url),
                        InlineKeyboardButton("📣 Updates Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")
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
    """
    Handles the Callback Query from the Force Join prompt's 'Try again' button.
    If joined, it re-triggers the media extraction synchronously.
    """
    is_subscribed = await check_force_sub(client, query.from_user.id)
    if is_subscribed == "admin_required":
        await query.answer("⚠️ The bot must be made an Administrator in the channel to verify members!", show_alert=True)
        return
    elif not is_subscribed:
        await query.answer("❌ You still haven't joined the channel!", show_alert=True)
        return
        
    await query.answer("✅ Verified! Processing your link...", show_alert=False)
    await query.message.delete()
    
    # query.message.reply_to_message contains the original user's message with the link
    original_message = query.message.reply_to_message
    if original_message and original_message.text:
        await handle_media_links(client, original_message)
    else:
        await client.send_message(query.message.chat.id, "✅ Verified! Please send your link again.")

# ------------------------------------------------------------------
# Main Loop and Graceful Shutdown
# ------------------------------------------------------------------

async def main():
    global is_shutting_down
    
    # Initialize Pyrogram App safely inside the active asyncio loop
    from pyrogram.handlers import MessageHandler, CallbackQueryHandler
    app = Client(
        "ig_reels_bot",
        api_id=int(API_ID),
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True
    )
    
    # Register the handlers dynamically instead of using decorators
    app.add_handler(MessageHandler(
        handle_start_command, 
        filters.command("start") & filters.private
    ))
    app.add_handler(MessageHandler(
        handle_media_links, 
        filters.regex(SUPPORTED_LINKS_REGEX) & filters.private
    ))
    app.add_handler(CallbackQueryHandler(
        handle_check_join,
        filters.regex("^check_join$")
    ))
    
    logger.info("Starting Telegram Bot...")
    await app.start()
    logger.info("Bot is active and polling. Ready for links!")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # Trap container interruption signals (Kubernetes/Docker/Ctrl+C)
    def handle_signal(sig):
        logger.info(f"Signal {sig.name} detected! Initiating graceful shutdown...")
        global is_shutting_down
        is_shutting_down = True
        loop.call_soon_threadsafe(stop_event.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))
        except NotImplementedError:
            # add_signal_handler is not fully supported on Windows.
            # Fallback to default keyboard interrupt handling.
            pass

    # Wait until a stop signal is fired
    await stop_event.wait()

    # Graceful Shutdown Sequence Guaranteeing No Corruption
    if active_tasks:
        logger.info(f"Waiting for {len(active_tasks)} active task(s) to finalize...")
        await asyncio.gather(*active_tasks, return_exceptions=True)

    await app.stop()
    logger.info("Cleanup complete. Container exiting.")

if __name__ == "__main__":
    asyncio.run(main())
