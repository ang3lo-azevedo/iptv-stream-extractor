import re
import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import sys
import signal
import logging
import warnings

# Suppress timeout error messages from IPTV_checker and urllib3
logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
warnings.filterwarnings('ignore')

# Disable urllib3 warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import functions from IPTV_checker
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'IPTVChecker-BitRate'))
try:
    from IPTV_checker import check_channel_status, get_detailed_stream_info, get_video_bitrate, get_audio_bitrate
    IPTV_CHECKER_AVAILABLE = True
except ImportError:
    print("Warning: IPTV_checker.py not found. Stream checking will be limited.")
    IPTV_CHECKER_AVAILABLE = False

# ANSI color codes
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'

# Logger class to write to both console and file
class Logger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.log_handle = None
        
    def open(self):
        """Open the log file for writing"""
        try:
            self.log_handle = open(self.log_file, 'w', encoding='utf-8')
            self.log(f"Log started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        except Exception as e:
            print(f"{Colors.RED}✗ Could not open log file: {e}{Colors.RESET}")
    
    def log(self, message, strip_colors=True):
        """Write message to both console and log file"""
        # Print to console with colors
        print(message, end='')
        
        # Write to file without colors
        if self.log_handle:
            try:
                if strip_colors:
                    # Remove ANSI color codes for log file
                    clean_message = re.sub(r'\033\[[0-9;]+m', '', message)
                else:
                    clean_message = message
                self.log_handle.write(clean_message)
                self.log_handle.flush()
            except:
                pass
    
    def close(self):
        """Close the log file"""
        if self.log_handle:
            try:
                self.log(f"\nLog ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                self.log_handle.close()
            except:
                pass

# Global logger instance
logger = None

# Configuration
input_file = "../middleware.sql" if os.path.exists("../middleware.sql") else "middleware.sql"
stream_progress_file = "stream_check_progress.json"
playlist_progress_file = "playlist_progress.json"
final_output_file = "IPTV.m3u8"
log_file = "LOG.log"
REPROCESS_PLAYLISTS = False  # Set to True to re-check already processed playlists

# Regex to match M3U/IPTV playlist URLs
m3u_pattern = re.compile(
    r"(https?://[^\s',\)]+(?:"
    r"type=(?:m3u[_\-]?(?:plus?|plu[ts]?|pl[a-z]*)?|ss(?:iptv)?|smart(?:_iptv)?|enigma|dreambox|ottplayer|webtvlist|gigablue|simple|ts|hls|xml|tvg_plus|adv_[a-z_]+|[a-z0-9_\-]*m3u[a-z0-9_\-]*)"
    r"|\.m3u8?"
    r")[^\s',\)]*)", 
    re.IGNORECASE
)

lock = threading.Lock()
stats_lock = threading.Lock()

# Create a session with connection pooling and aggressive timeouts
session = requests.Session()
retry_strategy = Retry(total=0, backoff_factor=0, status_forcelist=[429, 500, 502, 503, 504])  # No retries - fail fast
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100, pool_block=False)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Global statistics
global_stats = {
    # M3U file stats
    'total_m3u': 0,
    'valid_m3u': 0,
    'invalid_m3u': 0,
    'current_m3u': '',
    # Stream stats
    'total_streams': 0,
    'checked': 0,
    'working': 0,
    'failed': 0,
    'filtered': 0,
    'current_stream': '',
    'last_status': '',
    'start_time': time.time(),
    'first_display': True,
    'num_lines': 10
}

# For graceful exit
stream_progress_data = {}
working_streams_data = []
processed_playlists_data = set()

# Country code mapping
COUNTRY_CODES = {
    'AR': ['ARGENTINA', 'AR'],
    'BR': ['BRAZIL', 'BRASIL', 'BR'],
    'CA': ['CANADA', 'CA'],
    'DE': ['GERMANY', 'DEUTSCHLAND', 'DE'],
    'ES': ['SPAIN', 'ESPAÑA', 'ES'],
    'FR': ['FRANCE', 'FR'],
    'IT': ['ITALY', 'ITALIA', 'IT'],
    'MX': ['MEXICO', 'MX'],
    'PT': ['PORTUGAL', 'PT'],
    'UK': ['UNITED KINGDOM', 'UK', 'GB', 'ENGLAND', 'BRITISH'],
    'US': ['USA', 'UNITED STATES', 'US', 'AMERICA'],
    'INT': ['INTERNATIONAL', 'INT']
}

# Filters - streams to exclude (pre-compiled regex for speed)
EXCLUDE_PATTERNS = [
    # Movies
    re.compile(r'\b(movie|film|cinema|pelicula|filme|cine)\b', re.IGNORECASE),
    # Series/Shows
    re.compile(r'\b(series|tv\s*show|season|episode|episodio|temporada|capitulo)\b', re.IGNORECASE),
    # 24/7 channels
    re.compile(r'\b(24/?7|24h|24\s*h|24\s*hour|non-stop|nonstop)\b', re.IGNORECASE),
    # Adult content
    re.compile(r'\b(xxx|adult|porn|sexy|\+18|18\+|erotic|playboy|hustler)\b', re.IGNORECASE),
    # VOD/On-demand
    re.compile(r'\b(vod|on\s*demand|catch\s*up|replay)\b', re.IGNORECASE),
    # Radio (optional - uncomment if you want to exclude radio)
    # re.compile(r'\b(radio|fm)\b', re.IGNORECASE),
]

def should_filter_stream(channel_name, group_title):
    """Fast stream filtering with pre-compiled regex and early exit"""
    text = f"{channel_name} {group_title}".lower()
    for pattern in EXCLUDE_PATTERNS:
        if pattern.search(text):
            return True
    return False

def update_dual_progress(processed_playlists, total_playlists, start_time, current_status=""):
    """Display enhanced progress with two bars - one for playlists, one for streams"""
    elapsed = time.time() - start_time
    
    with stats_lock:
        valid_m3u = global_stats['valid_m3u']
        invalid_m3u = global_stats['invalid_m3u']
        total_streams = global_stats['total_streams']
        checked_streams = global_stats['checked']
        working = global_stats['working']
        failed = global_stats['failed']
        filtered = global_stats['filtered']
    
    # Playlist progress bar
    playlist_percent = (processed_playlists / total_playlists * 100) if total_playlists > 0 else 0
    bar_length = 40
    filled = int(bar_length * processed_playlists / total_playlists) if total_playlists > 0 else 0
    playlist_bar = '█' * filled + '░' * (bar_length - filled)
    
    # Stream progress bar (if we have streams to check)
    if total_streams > 0:
        stream_percent = (checked_streams / total_streams * 100)
        stream_filled = int(bar_length * checked_streams / total_streams)
        stream_bar = '█' * stream_filled + '░' * (bar_length - stream_filled)
    else:
        stream_percent = 0
        stream_bar = '░' * bar_length
    
    # Calculate rates and ETAs
    playlist_rate = processed_playlists / elapsed if elapsed > 0 else 0
    remaining_playlists = total_playlists - processed_playlists
    playlist_eta = remaining_playlists / playlist_rate if playlist_rate > 0 else 0
    
    stream_rate = checked_streams / elapsed if elapsed > 0 else 0
    remaining_streams = total_streams - checked_streams
    stream_eta = remaining_streams / stream_rate if stream_rate > 0 and total_streams > 0 else 0
    
    # Save cursor position, move up, clear from cursor down
    sys.stdout.write('\033[s')      # Save cursor position
    sys.stdout.write('\033[9A')     # Move up 9 lines
    sys.stdout.write('\033[J')      # Clear from cursor to end of screen
    
    # Print playlist progress
    print(f"{Colors.BOLD}{Colors.BLUE}┌─ Playlists ─────────────────────────────────────────────────────────────┐{Colors.RESET}")
    print(f"{Colors.BLUE}│{Colors.RESET} {Colors.CYAN}{playlist_bar}{Colors.RESET} {Colors.WHITE}{playlist_percent:>5.1f}%{Colors.RESET} {Colors.GRAY}({processed_playlists:,}/{total_playlists:,}){Colors.RESET}")
    print(f"{Colors.BLUE}│{Colors.RESET} {Colors.GREEN}✓ Valid: {valid_m3u:>6,}{Colors.RESET}  {Colors.RED}✗ Invalid: {invalid_m3u:>6,}{Colors.RESET}  {Colors.MAGENTA}⚡ {playlist_rate:>5.1f} pl/s{Colors.RESET}  {Colors.CYAN}⏱ {format_time(playlist_eta)}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}├─ Streams ───────────────────────────────────────────────────────────────┤{Colors.RESET}")
    print(f"{Colors.BLUE}│{Colors.RESET} {Colors.MAGENTA}{stream_bar}{Colors.RESET} {Colors.WHITE}{stream_percent:>5.1f}%{Colors.RESET} {Colors.GRAY}({checked_streams:,}/{total_streams:,}){Colors.RESET}")
    print(f"{Colors.BLUE}│{Colors.RESET} {Colors.GREEN}✓ Working: {working:>6,}{Colors.RESET}  {Colors.RED}✗ Failed: {failed:>6,}{Colors.RESET}  {Colors.YELLOW}⊘ Filtered: {filtered:>6,}{Colors.RESET}")
    print(f"{Colors.BLUE}│{Colors.RESET} {Colors.MAGENTA}⚡ {stream_rate:>5.1f} st/s{Colors.RESET}  {Colors.CYAN}⏱ {format_time(stream_eta)}{Colors.RESET}  {Colors.WHITE}Time: {format_time(elapsed)}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}└──────────────────────────────────────────────────────────────────────────┘{Colors.RESET}")
    
    # Print current status line
    if current_status:
        print(current_status[:76])  # Limit to terminal width
    else:
        print("")  # Empty line for status
    
    sys.stdout.write('\033[u')      # Restore cursor position
    sys.stdout.flush()

def format_time(seconds):
    if seconds < 0:
        return "0:00:00"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"

def extract_country_code(group_title, channel_name):
    """Fast country extraction with priority for specific matches"""
    text = f"{group_title} {channel_name}".upper()
    
    # Priority order: check for full country names and common patterns first
    # This prevents false matches like "AR" in "PARAMOUNT" or "FR" in "FREEFORM"
    priority_checks = [
        ('US', ['USA', 'UNITED STATES', 'AMERICA']),
        ('UK', ['UNITED KINGDOM', 'UK', 'GB', 'ENGLAND', 'BRITISH']),
        ('INT', ['INTERNATIONAL', 'INT']),
    ]
    
    # Check priority patterns first (USA, UK, etc.)
    for code, keywords in priority_checks:
        for keyword in keywords:
            # Use word boundary check for short codes
            if len(keyword) <= 3:
                # Check if it's a standalone word (not part of another word)
                if f' {keyword} ' in f' {text} ' or text.startswith(keyword + ' ') or text.endswith(' ' + keyword):
                    return code
            else:
                # For longer keywords, simple substring match is fine
                if keyword in text:
                    return code
    
    # Then check other countries
    other_countries = {
        'AR': ['ARGENTINA', 'AR'],
        'BR': ['BRAZIL', 'BRASIL', 'BR'],
        'CA': ['CANADA', 'CA'],
        'DE': ['GERMANY', 'DEUTSCHLAND', 'DE'],
        'ES': ['SPAIN', 'ESPAÑA', 'ES'],
        'FR': ['FRANCE', 'FR'],
        'IT': ['ITALY', 'ITALIA', 'IT'],
        'MX': ['MEXICO', 'MX'],
        'PT': ['PORTUGAL', 'PT'],
    }
    
    for code, keywords in other_countries.items():
        for keyword in keywords:
            # Use word boundary check for short codes (2-3 chars)
            if len(keyword) <= 3:
                if f' {keyword} ' in f' {text} ' or text.startswith(keyword + ' ') or text.endswith(' ' + keyword):
                    return code
            else:
                if keyword in text:
                    return code
    
    return 'Unknown'

def parse_channel_info(extinf_line):
    info = {
        'tvg_id': '',
        'tvg_name': '',
        'tvg_logo': '',
        'group_title': '',
        'channel_name': ''
    }
    tvg_id_match = re.search(r'tvg-id="([^"]*)"', extinf_line)
    if tvg_id_match:
        info['tvg_id'] = tvg_id_match.group(1)
    tvg_name_match = re.search(r'tvg-name="([^"]*)"', extinf_line)
    if tvg_name_match:
        info['tvg_name'] = tvg_name_match.group(1)
    tvg_logo_match = re.search(r'tvg-logo="([^"]*)"', extinf_line)
    if tvg_logo_match:
        info['tvg_logo'] = tvg_logo_match.group(1)
    group_match = re.search(r'group-title="([^"]*)"', extinf_line)
    if group_match:
        info['group_title'] = group_match.group(1)
    if ',' in extinf_line:
        info['channel_name'] = extinf_line.rsplit(',', 1)[1].strip()
    return info

def extract_bitrate_value(bitrate_str):
    if not bitrate_str or bitrate_str == 'Unknown' or bitrate_str == 'N/A':
        return 0
    match = re.search(r'(\d+)', bitrate_str)
    return int(match.group(1)) if match else 0

def update_playlist_progress(current, total, start_time, streams_found):
    """Display progress bar for playlist parsing"""
    with stats_lock:
        valid_m3u = global_stats['valid_m3u']
        invalid_m3u = global_stats['invalid_m3u']
        current_m3u = global_stats['current_m3u']
    
    percent = (current / total * 100) if total > 0 else 0
    bar_length = 50
    filled = int(bar_length * current / total) if total > 0 else 0
    bar = '█' * filled + '░' * (bar_length - filled)
    elapsed = time.time() - start_time
    rate = current / elapsed if elapsed > 0 else 0
    remaining = total - current
    eta = remaining / rate if rate > 0 else 0
    
    # Clear previous lines and print new status
    sys.stdout.write('\033[2K\r')  # Clear current line
    print(f"{Colors.BOLD}{Colors.BLUE}Downloading M3U Files:{Colors.RESET}")
    print(f"{Colors.CYAN}{bar}{Colors.RESET} {Colors.WHITE}{percent:.1f}%{Colors.RESET} ({current}/{total})")
    print(f"{Colors.GREEN}✓ Valid: {valid_m3u}{Colors.RESET}  {Colors.RED}✗ Invalid: {invalid_m3u}{Colors.RESET}  {Colors.BLUE}Streams Found:{Colors.RESET} {Colors.GREEN}{streams_found:,}{Colors.RESET}")
    print(f"{Colors.BLUE}Current M3U:{Colors.RESET} {Colors.GRAY}{current_m3u[:60]}{Colors.RESET}")
    print(f"{Colors.BLUE}ETA:{Colors.RESET} {Colors.CYAN}{format_time(eta)}{Colors.RESET}")
    # Move cursor up 5 lines for next update
    if current < total:
        sys.stdout.write('\033[5A')
    sys.stdout.flush()

def update_stream_progress_display():
    with stats_lock:
        total = global_stats['total_streams']
        checked = global_stats['checked']
        working = global_stats['working']
        failed = global_stats['failed']
        filtered = global_stats['filtered']
        current_stream = global_stats['current_stream']
        last_status = global_stats['last_status']
        elapsed = time.time() - global_stats['start_time']
        first_display = global_stats['first_display']
        num_lines = global_stats['num_lines']
    if total == 0:
        return
    percent = (checked / total * 100) if total > 0 else 0
    rate = checked / elapsed if elapsed > 0 else 0
    remaining = total - checked
    eta = remaining / rate if rate > 0 else 0
    bar_length = 50
    filled = int(bar_length * checked / total) if total > 0 else 0
    bar = '█' * filled + '░' * (bar_length - filled)
    if not first_display:
        sys.stdout.write(f'\033[{num_lines}A')
        sys.stdout.write('\033[J')
    status_color = Colors.GREEN if last_status == 'working' else Colors.RED if last_status == 'failed' else Colors.YELLOW
    print(f"{Colors.BOLD}{Colors.BLUE}Checking Streams:{Colors.RESET}")
    print(f"{Colors.CYAN}{bar}{Colors.RESET} {Colors.WHITE}{percent:.1f}%{Colors.RESET} ({checked:,}/{total:,})")
    print(f"{Colors.GREEN}✓ Working: {working:,}{Colors.RESET}  {Colors.RED}✗ Failed: {failed:,}{Colors.RESET}  {Colors.YELLOW}⊘ Filtered: {filtered:,}{Colors.RESET}")
    print(f"{Colors.BLUE}Speed:{Colors.RESET} {Colors.MAGENTA}{rate:.1f} streams/s{Colors.RESET}  {Colors.BLUE}ETA:{Colors.RESET} {Colors.CYAN}{format_time(eta)}{Colors.RESET}")
    print(f"{Colors.BLUE}Time:{Colors.RESET} {Colors.CYAN}{format_time(elapsed)}{Colors.RESET}")
    print(f"{Colors.BLUE}Current:{Colors.RESET} {status_color}{current_stream[:65]}{Colors.RESET}")
    print()
    sys.stdout.flush()
    with stats_lock:
        global_stats['first_display'] = False

def extract_urls_from_sql():
    urls = []
    stats = {'total_matches': 0, 'by_type': {}}
    print(f"{Colors.BOLD}{Colors.BLUE}→ Extracting M3U URLs from SQL database...{Colors.RESET}")
    
    line_count = 0
    last_update = time.time()
    
    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line_count += 1
            
            # Update progress every 0.5 seconds
            current_time = time.time()
            if current_time - last_update >= 0.5:
                sys.stdout.write(f'\r{Colors.CYAN}  Processing... Lines: {line_count:,}  URLs found: {len(urls):,}{Colors.RESET}')
                sys.stdout.flush()
                last_update = current_time
            
            matches = m3u_pattern.findall(line)
            for m in matches:
                stats['total_matches'] += 1
                type_match = re.search(r'type=([^&\s\'"]+)', m, re.IGNORECASE)
                if type_match:
                    ptype = type_match.group(1).lower()
                    stats['by_type'][ptype] = stats['by_type'].get(ptype, 0) + 1
                elif '.m3u' in m.lower():
                    stats['by_type']['direct_m3u'] = stats['by_type'].get('direct_m3u', 0) + 1
                urls.append(m)
    
    # Clear progress line
    sys.stdout.write('\r' + ' ' * 80 + '\r')
    sys.stdout.flush()
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    print(f"\n{Colors.BOLD}{Colors.CYAN}=== URL Extraction Statistics ==={Colors.RESET}")
    print(f"{Colors.GREEN}✓ Total URLs found: {stats['total_matches']}{Colors.RESET}")
    print(f"{Colors.GREEN}✓ Unique URLs: {len(uniq)}{Colors.RESET}")
    print(f"\n{Colors.BOLD}Top 10 playlist types:{Colors.RESET}")
    sorted_types = sorted(stats['by_type'].items(), key=lambda x: x[1], reverse=True)[:10]
    for ptype, count in sorted_types:
        print(f"  {Colors.CYAN}▸{Colors.RESET} {ptype}: {count}")
    print(f"{Colors.CYAN}{'═' * 35}{Colors.RESET}\n")
    return uniq

def download_and_parse_playlist(url, timeout=2, progress_callback=None):
    try:
        headers = {"User-Agent": "VLC/3.0.14 LibVLC/3.0.14"}
        
        # Download with progress tracking
        response = session.get(url, timeout=timeout, headers=headers, stream=True)
        if response.status_code != 200:
            return []
        
        # Get total size if available
        total_size = int(response.headers.get('content-length', 0))
        
        # Download content in chunks
        content_parts = []
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                content_parts.append(chunk)
                downloaded += len(chunk)
                
                # Call progress callback if provided
                if progress_callback and total_size > 0:
                    progress = (downloaded / total_size) * 100
                    progress_callback(progress, downloaded, total_size)
        
        # Combine all chunks
        content = b''.join(content_parts).decode('utf-8', errors='ignore')
        
        # Parse the M3U content - optimized parsing
        lines = content.split('\n')
        streams = []
        i = 0
        line_count = len(lines)
        
        while i < line_count:
            line = lines[i].strip()
            # Fast check: does line start with #EXTINF?
            if line.startswith('#EXTINF'):
                i += 1
                # Check if next line exists and is not a comment
                if i < line_count:
                    stream_url = lines[i].strip()
                    if stream_url and not stream_url.startswith('#'):
                        info = parse_channel_info(line)
                        streams.append({'extinf': line, 'url': stream_url, 'info': info})
                i += 1
            else:
                i += 1
        
        return streams
    except requests.exceptions.Timeout:
        # Timeout - skip this playlist
        return []
    except Exception:
        # Silently ignore other errors
        return []

def process_playlist_worker(url, idx, total, stream_progress):
    """Download a playlist and return its streams for checking"""
    # Update current M3U
    with stats_lock:
        global_stats['current_m3u'] = url
    
    # Download and parse playlist
    try:
        streams = download_and_parse_playlist(url)
    except Exception:
        streams = []
    
    # Update M3U stats
    with stats_lock:
        global_stats['total_streams'] += len(streams)
        if len(streams) > 0:
            global_stats['valid_m3u'] += 1
        else:
            global_stats['invalid_m3u'] += 1
    
    return streams  # Return streams for parallel checking

def download_playlist_wrapper(url, idx, total):
    """Wrapper function for downloading and parsing a playlist in parallel"""
    download_start = time.time()
    
    try:
        # Download and parse the playlist
        streams = download_and_parse_playlist(url, progress_callback=None)
        download_time = time.time() - download_start
        
        return streams, download_time
        
    except Exception:
        download_time = time.time() - download_start
        return [], download_time

def check_stream_worker(stream, stream_progress):
    """Worker function to check a single stream - assumes already filtered"""
    if not IPTV_CHECKER_AVAILABLE:
        return None
    
    channel_name = stream['info']['channel_name']
    group_title = stream['info']['group_title']
    stream_url = stream['url']
    stream_key = f"{channel_name}_{stream_url}"
    
    # Check if already processed (thread-safe)
    with lock:
        if stream_key in stream_progress:
            result = stream_progress[stream_key]
            with stats_lock:
                global_stats['checked'] += 1
                if result['status'] == 'working':
                    global_stats['working'] += 1
                else:
                    global_stats['failed'] += 1
            return result
    
    # Update status for display
    with stats_lock:
        global_stats['current_stream'] = channel_name
        global_stats['last_status'] = 'checking'
    
    status = check_channel_status(stream_url, timeout=10, extended_timeout=15)
    if status == 'Alive':
        codec_name, video_bitrate, resolution, fps = get_detailed_stream_info(stream_url)
        audio_info = get_audio_bitrate(stream_url)
        country = extract_country_code(group_title, channel_name)
        result = {
            'status': 'working',
            'extinf': stream['extinf'],
            'url': stream_url,
            'info': stream['info'],
            'codec': codec_name,
            'video_bitrate': video_bitrate,
            'resolution': resolution,
            'fps': fps,
            'audio_info': audio_info,
            'country': country
        }
        with stats_lock:
            global_stats['checked'] += 1
            global_stats['working'] += 1
            global_stats['last_status'] = 'working'
    else:
        result = {'status': 'failed', 'reason': 'Stream not working'}
        with stats_lock:
            global_stats['checked'] += 1
            global_stats['failed'] += 1
            global_stats['last_status'] = 'failed'
    
    with lock:
        stream_progress[stream_key] = result
    
    return result

def organize_streams_by_country_and_bitrate(working_streams):
    by_country = defaultdict(list)
    for stream in working_streams:
        country = stream['country']
        by_country[country].append(stream)
    organized = {}
    for country, streams in by_country.items():
        by_name = defaultdict(list)
        for stream in streams:
            channel_name = stream['info']['channel_name']
            base_name = re.sub(r'\s*\(.*?\)\s*', '', channel_name)
            base_name = re.sub(r'\s*(HD|FHD|4K|UHD|SD)\s*', '', base_name, flags=re.IGNORECASE)
            base_name = base_name.strip()
            by_name[base_name].append(stream)
        sorted_channels = []
        for base_name in sorted(by_name.keys()):
            channel_streams = by_name[base_name]
            channel_streams.sort(key=lambda s: extract_bitrate_value(s.get('video_bitrate', '0')), reverse=True)
            for idx, stream in enumerate(channel_streams):
                if idx == 0:
                    stream['final_name'] = base_name
                else:
                    stream['final_name'] = f"{base_name} backup {idx}"
            sorted_channels.extend(channel_streams)
        organized[country] = sorted_channels
    return organized

def write_m3u_output(organized_streams, output_file, expiry_date=None, incremental=False):
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("# Organized by country, alphabetically, and by bitrate\n")
            if expiry_date:
                f.write(f"# Subscription Expires: {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("\n")
            for country in sorted(organized_streams.keys()):
                streams = organized_streams[country]
                f.write(f"\n# ===== {country} ({len(streams)} streams) =====\n")
                for stream in streams:
                    info = stream['info']
                    final_name = stream['final_name']
                    extinf = f"#EXTINF:-1"
                    if info.get('tvg_id'):
                        extinf += f' tvg-id="{info["tvg_id"]}"'
                    if info.get('tvg_name'):
                        extinf += f' tvg-name="{info["tvg_name"]}"'
                    if info.get('tvg_logo'):
                        extinf += f' tvg-logo="{info["tvg_logo"]}"'
                    extinf += f' group-title="{country}"'
                    resolution = stream.get('resolution', 'Unknown')
                    video_bitrate = stream.get('video_bitrate', 'Unknown')
                    name_with_info = f"{final_name} [{resolution} {video_bitrate}]"
                    if expiry_date:
                        expiry_str = expiry_date.strftime('%Y-%m-%d')
                        name_with_info += f" [Expires: {expiry_str}]"
                    extinf += f",{name_with_info}\n"
                    f.write(extinf)
                    f.write(stream['url'] + "\n")
        if not incremental:
            print(f"\n{Colors.GREEN}✓ Output written to: {output_file}{Colors.RESET}")
    except Exception as e:
        if not incremental:
            print(f"\n{Colors.RED}✗ Error writing output: {e}{Colors.RESET}")

def save_stream_progress(data):
    with lock:
        temp_file = stream_progress_file + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_file, stream_progress_file)
        except Exception as e:
            print(f"\n{Colors.RED}✗ Error saving stream progress: {e}{Colors.RESET}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

def load_stream_progress():
    if not os.path.exists(stream_progress_file):
        return {}
    try:
        with open(stream_progress_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"{Colors.YELLOW}⚠ Could not load stream progress: {e}{Colors.RESET}")
        return {}

def load_playlist_progress():
    """Load the list of already processed playlist URLs"""
    if not os.path.exists(playlist_progress_file):
        return set()
    try:
        with open(playlist_progress_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('processed_playlists', []))
    except Exception as e:
        print(f"{Colors.YELLOW}⚠ Could not load playlist progress: {e}{Colors.RESET}")
        return set()

def save_playlist_progress(processed_playlists):
    """Save the list of processed playlist URLs"""
    try:
        with open(playlist_progress_file, 'w', encoding='utf-8') as f:
            json.dump({
                'processed_playlists': list(processed_playlists),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }, f, indent=2)
    except Exception as e:
        print(f"\n{Colors.RED}✗ Error saving playlist progress: {e}{Colors.RESET}")

def graceful_exit(signum=None, frame=None):
    """Handle graceful exit - save progress and write output"""
    print(f"\n\n{Colors.YELLOW}⚠ Interrupted! Saving progress...{Colors.RESET}")
    
    # Save stream progress
    global stream_progress_data, working_streams_data, processed_playlists_data
    if stream_progress_data:
        # Save directly without using the lock to avoid deadlock
        temp_file = stream_progress_file + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(stream_progress_data, f, indent=2)
            os.replace(temp_file, stream_progress_file)
            print(f"{Colors.GREEN}✓ Stream progress saved ({len(stream_progress_data):,} streams){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}✗ Error saving: {e}{Colors.RESET}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
    
    # Save playlist progress
    if processed_playlists_data:
        try:
            save_playlist_progress(processed_playlists_data)
            print(f"{Colors.GREEN}✓ Playlist progress saved ({len(processed_playlists_data):,} playlists){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}✗ Error saving playlist progress: {e}{Colors.RESET}")
    
    # Write partial output if we have working streams
    if working_streams_data:
        print(f"{Colors.CYAN}→ Writing partial results to {final_output_file}...{Colors.RESET}")
        try:
            organized = organize_streams_by_country_and_bitrate(working_streams_data)
            write_m3u_output(organized, final_output_file, None)
            print(f"{Colors.GREEN}✓ Partial results saved ({len(working_streams_data)} working streams){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}✗ Error writing output: {e}{Colors.RESET}")
    
    print(f"{Colors.YELLOW}→ Exiting now{Colors.RESET}\n")
    os._exit(0)  # Force exit immediately without waiting for threads

if __name__ == '__main__':
    # Register signal handlers for graceful exit
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)
    
    # Initialize logger
    logger = Logger(log_file)
    logger.open()
    
    logger.log(f"\n{Colors.BOLD}{Colors.CYAN}{'═' * 78}{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.CYAN}  IPTV M3U Stream Extractor & Checker{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.CYAN}{'═' * 78}{Colors.RESET}\n\n")
    if not IPTV_CHECKER_AVAILABLE:
        logger.log(f"{Colors.RED}✗ IPTV_checker.py not available. Cannot check streams.{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    if not os.path.exists(input_file):
        logger.log(f"{Colors.RED}✗ SQL database file not found: {input_file}{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    playlist_urls = extract_urls_from_sql()
    if not playlist_urls:
        logger.log(f"{Colors.RED}✗ No M3U URLs found in database{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    logger.log(f"{Colors.GREEN}✓ Found {len(playlist_urls)} unique M3U URLs{Colors.RESET}\n\n")
    
    # Load stream progress
    stream_progress = load_stream_progress()
    stream_progress_data = stream_progress  # For graceful exit
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Loading previous stream progress...{Colors.RESET}\n")
    logger.log(f"{Colors.CYAN}  Loaded {len(stream_progress)} previously checked streams{Colors.RESET}\n\n")
    
    # Load playlist progress
    processed_playlists = load_playlist_progress()
    processed_playlists_data = processed_playlists  # For graceful exit
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Loading previous playlist progress...{Colors.RESET}\n")
    logger.log(f"{Colors.CYAN}  Loaded {len(processed_playlists)} previously processed playlists{Colors.RESET}\n")
    
    # Filter out already processed playlists if not reprocessing
    if not REPROCESS_PLAYLISTS and processed_playlists:
        original_count = len(playlist_urls)
        playlist_urls = [url for url in playlist_urls if url not in processed_playlists]
        skipped = original_count - len(playlist_urls)
        logger.log(f"{Colors.YELLOW}  Skipping {skipped:,} already processed playlists{Colors.RESET}\n")
        logger.log(f"{Colors.GREEN}  Remaining to process: {len(playlist_urls):,} playlists{Colors.RESET}\n\n")
    elif REPROCESS_PLAYLISTS:
        logger.log(f"{Colors.YELLOW}  REPROCESS_PLAYLISTS=True: Re-checking all playlists{Colors.RESET}\n\n")
        processed_playlists = set()  # Clear the set to track fresh
        processed_playlists_data = processed_playlists
    else:
        logger.log(f"{Colors.GREEN}  All {len(playlist_urls):,} playlists need processing{Colors.RESET}\n\n")
    
    if not playlist_urls:
        logger.log(f"{Colors.GREEN}✓ All playlists already processed!{Colors.RESET}\n")
        logger.log(f"{Colors.CYAN}  Set REPROCESS_PLAYLISTS=True to re-check them{Colors.RESET}\n\n")
        logger.close()
        sys.exit(0)
    
    # Save initial progress state
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Saving initial progress state...{Colors.RESET}\n")
    save_stream_progress(stream_progress)
    logger.log(f"{Colors.GREEN}✓ Progress saved{Colors.RESET}\n\n")
    
    # Initialize stats
    with stats_lock:
        global_stats['start_time'] = time.time()
        global_stats['total_m3u'] = len(playlist_urls)
    
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Downloading playlists and checking streams...{Colors.RESET}\n\n")
    
    parse_start_time = time.time()
    working_streams = []
    all_streams_count = 0
    save_counter = 0
    max_playlist_workers = 10  # Process 10 playlists concurrently
    max_stream_workers = 30    # Check 30 streams concurrently
    processed_count = 0
    
    print(f"{Colors.CYAN}Parallel processing: {max_playlist_workers} playlist workers + {max_stream_workers} stream workers{Colors.RESET}\n")
    
    # Reserve space for progress bars (9 lines: 8 for bars + 1 for status)
    for _ in range(9):
        print()
    
    # Display initial progress
    update_dual_progress(0, len(playlist_urls), parse_start_time, "")
    
    # Process playlists with BOTH parallel downloading AND sequential stream checking
    with ThreadPoolExecutor(max_workers=max_playlist_workers) as playlist_executor, \
         ThreadPoolExecutor(max_workers=max_stream_workers) as stream_executor:
        
        last_update_time = time.time()
        
        # Submit playlists in batches for parallel downloading
        playlist_batch_size = max_playlist_workers * 2  # Download 2x workers at a time
        playlist_index = 0
        
        while playlist_index < len(playlist_urls):
            # Submit a batch of playlists for downloading
            batch_end = min(playlist_index + playlist_batch_size, len(playlist_urls))
            batch_futures = {}
            
            for idx in range(playlist_index, batch_end):
                url = playlist_urls[idx]
                future = playlist_executor.submit(download_playlist_wrapper, url, idx + 1, len(playlist_urls))
                batch_futures[future] = (url, idx + 1)
            
            # Process each downloaded playlist as it completes
            for future in as_completed(batch_futures.keys()):
                url, idx = batch_futures[future]
                
                try:
                    streams, download_time = future.result()
                    
                    # FILTER STREAMS BEFORE CHECKING
                    original_count = len(streams)
                    filtered_streams = []
                    filtered_out_count = 0
                    
                    for stream in streams:
                        channel_name = stream['info']['channel_name']
                        group_title = stream['info']['group_title']
                        
                        if should_filter_stream(channel_name, group_title):
                            filtered_out_count += 1
                            with stats_lock:
                                global_stats['filtered'] += 1
                        else:
                            filtered_streams.append(stream)
                    
                    # Update M3U stats
                    with stats_lock:
                        global_stats['total_streams'] += original_count
                        if original_count > 0:
                            global_stats['valid_m3u'] += 1
                        else:
                            global_stats['invalid_m3u'] += 1
                    
                    # Show result with filtering info
                    if filtered_streams:
                        status_msg = f"{Colors.GREEN}✓ [{idx}/{len(playlist_urls)}] Found {len(filtered_streams)} streams (filtered {filtered_out_count}/{original_count}) ({download_time:.1f}s){Colors.RESET}"
                    elif original_count > 0:
                        status_msg = f"{Colors.YELLOW}⚠ [{idx}/{len(playlist_urls)}] All {original_count} streams filtered out ({download_time:.1f}s){Colors.RESET}"
                    else:
                        status_msg = f"{Colors.RED}✗ [{idx}/{len(playlist_urls)}] Empty or timeout ({download_time:.1f}s){Colors.RESET}"
                    update_dual_progress(processed_count, len(playlist_urls), parse_start_time, status_msg)
                    
                    # If playlist has streams AFTER filtering, CHECK THEM ALL before continuing
                    if filtered_streams:
                        # Submit only non-filtered streams for checking
                        stream_futures = {}
                        for stream in filtered_streams:
                            stream_future = stream_executor.submit(check_stream_worker, stream, stream_progress)
                            stream_futures[stream_future] = stream
                        
                        # Wait for all streams from this playlist to be checked
                        stream_count = 0
                        last_save_time = time.time()  # Track when we last saved
                        
                        for sf in as_completed(stream_futures.keys()):
                            try:
                                result = sf.result()
                                if result and result['status'] == 'working':
                                    working_streams.append(result)
                                    working_streams_data.append(result)
                            except Exception:
                                pass
                            
                            stream_count += 1
                            
                            # Update progress display every 10 streams OR every 0.5 seconds
                            current_time = time.time()
                            if stream_count % 10 == 0 or stream_count == len(stream_futures) or (current_time - last_update_time) >= 0.5:
                                checking_msg = f"{Colors.CYAN}⚙ Checking streams... {stream_count}/{len(stream_futures)} from playlist {idx}{Colors.RESET}"
                                update_dual_progress(processed_count, len(playlist_urls), parse_start_time, checking_msg)
                                last_update_time = current_time
                            
                            # Auto-save progress every 30 seconds during stream checking
                            if current_time - last_save_time >= 30.0:
                                save_stream_progress(stream_progress)
                                # Also write incremental M3U if we have working streams
                                if working_streams:
                                    try:
                                        organized_temp = organize_streams_by_country_and_bitrate(working_streams)
                                        write_m3u_output(organized_temp, final_output_file, None, incremental=True)
                                    except Exception:
                                        pass
                                last_save_time = current_time
                    
                    # Increment processed count after all streams are done
                    processed_count += 1
                    
                    # Mark this playlist as processed
                    processed_playlists.add(url)
                    processed_playlists_data.add(url)
                    
                    # Final update after all streams from this playlist are done
                    done_msg = f"{Colors.GREEN}✓ Playlist {processed_count}/{len(playlist_urls)} complete{Colors.RESET}"
                    update_dual_progress(processed_count, len(playlist_urls), parse_start_time, done_msg)
                    
                    # Save progress after EVERY playlist
                    save_stream_progress(stream_progress)
                    save_playlist_progress(processed_playlists)
                    
                    # Write M3U file whenever we have working streams (every 1 playlist if streams found)
                    # or at least every 5 playlists to show progress
                    should_write = (len(working_streams) > 0 and processed_count % 1 == 0) or processed_count % 5 == 0
                    
                    if should_write and working_streams:
                        try:
                            organized_temp = organize_streams_by_country_and_bitrate(working_streams)
                            write_m3u_output(organized_temp, final_output_file, None, incremental=True)
                            # Show notification every 10 playlists
                            if processed_count % 10 == 0:
                                save_msg = f"{Colors.GREEN}💾 M3U updated: {len(working_streams)} working streams{Colors.RESET}"
                                update_dual_progress(processed_count, len(playlist_urls), parse_start_time, save_msg)
                        except Exception as e:
                            pass
                    
                except KeyboardInterrupt:
                    print(f"\n\n{Colors.YELLOW}⚠ Interrupted by user{Colors.RESET}")
                    graceful_exit()
                except Exception as e:
                    processed_count += 1
                    pass  # Continue with next playlist
            
            # Move to next batch
            playlist_index = batch_end
    
    # Clear the progress display and move to bottom
    sys.stdout.write('\033[9B')  # Move down past progress bars
    print("\n")  # Add some space
    
    logger.log(f"\n\n{Colors.CYAN}→ All playlists processed and streams checked!{Colors.RESET}\n\n")
    logger.log(f"\n{Colors.CYAN}→ Saving final progress...{Colors.RESET}\n")
    save_stream_progress(stream_progress)
    save_playlist_progress(processed_playlists)
    logger.log(f"{Colors.GREEN}✓ Progress saved{Colors.RESET}\n\n")
    
    if not working_streams:
        logger.log(f"\n{Colors.RED}✗ No working streams found{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}→ Organizing streams by country and bitrate...{Colors.RESET}\n")
    organized = organize_streams_by_country_and_bitrate(working_streams)
    total_organized = sum(len(streams) for streams in organized.values())
    logger.log(f"{Colors.GREEN}✓ Organized {total_organized} working streams across {len(organized)} countries{Colors.RESET}\n\n")
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Writing output file...{Colors.RESET}\n")
    write_m3u_output(organized, final_output_file, None)
    elapsed = time.time() - global_stats['start_time']
    logger.log(f"\n{Colors.BOLD}{Colors.GREEN}{'═' * 78}{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.GREEN}{'  ' * 15}✓ PROCESSING COMPLETE ✓{'  ' * 15}{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.GREEN}{'═' * 78}{Colors.RESET}\n\n")
    logger.log(f"{Colors.BOLD}{Colors.BLUE}M3U Files:{Colors.RESET}\n")
    logger.log(f"  {Colors.WHITE}Total:{Colors.RESET} {global_stats['total_m3u']}  {Colors.GREEN}✓ Valid:{Colors.RESET} {global_stats['valid_m3u']}  {Colors.RED}✗ Invalid:{Colors.RESET} {global_stats['invalid_m3u']}\n")
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}Streams:{Colors.RESET}\n")
    logger.log(f"  {Colors.WHITE}Total Found:{Colors.RESET}    {global_stats['total_streams']:,}\n")
    logger.log(f"  {Colors.WHITE}Checked:{Colors.RESET}        {global_stats['checked']:,}\n")
    logger.log(f"  {Colors.GREEN}✓ Working:{Colors.RESET}      {global_stats['working']:,}\n")
    logger.log(f"  {Colors.RED}✗ Failed:{Colors.RESET}       {global_stats['failed']:,}\n")
    logger.log(f"  {Colors.YELLOW}⊘ Filtered:{Colors.RESET}     {global_stats['filtered']:,}\n")
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}Performance:{Colors.RESET}\n")
    logger.log(f"  {Colors.WHITE}Time Elapsed:{Colors.RESET}   {Colors.CYAN}{format_time(elapsed)}{Colors.RESET}\n")
    if elapsed > 0:
        rate = global_stats['checked'] / elapsed
        logger.log(f"  {Colors.WHITE}Average Speed:{Colors.RESET}  {Colors.MAGENTA}{rate:.1f} streams/s{Colors.RESET}\n")
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}Output Files:{Colors.RESET}\n")
    logger.log(f"  {Colors.CYAN}▸{Colors.RESET} Playlist:            {Colors.WHITE}{final_output_file}{Colors.RESET}\n")
    logger.log(f"  {Colors.CYAN}▸{Colors.RESET} Stream Progress:     {Colors.WHITE}{stream_progress_file}{Colors.RESET}\n")
    logger.log(f"  {Colors.CYAN}▸{Colors.RESET} Playlist Progress:   {Colors.WHITE}{playlist_progress_file}{Colors.RESET}\n")
    logger.log(f"  {Colors.CYAN}▸{Colors.RESET} Log File:            {Colors.WHITE}{log_file}{Colors.RESET}\n")
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}Note:{Colors.RESET}\n")
    logger.log(f"  {Colors.GRAY}Set {Colors.WHITE}REPROCESS_PLAYLISTS=True{Colors.GRAY} to re-check already processed playlists{Colors.RESET}\n")
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}Streams by Country:{Colors.RESET}\n")
    for country in sorted(organized.keys()):
        count = len(organized[country])
        logger.log(f"  {Colors.CYAN}▸{Colors.RESET} {country}: {Colors.WHITE}{count}{Colors.RESET} streams\n")
    logger.log(f"\n{Colors.BOLD}{Colors.GREEN}{'═' * 78}{Colors.RESET}\n\n")
    
    # Close the logger
    logger.close()
