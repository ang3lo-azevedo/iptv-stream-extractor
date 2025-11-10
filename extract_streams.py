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
import argparse
import shutil  # For getting terminal size

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

# Common message prefixes (for consistency and easy refactoring)
MSG_PREFIX_SUCCESS = f"{Colors.GREEN}[+]{Colors.RESET}"
MSG_PREFIX_ERROR = f"{Colors.RED}[-]{Colors.RESET}"
MSG_PREFIX_WARNING = f"{Colors.YELLOW}[!]{Colors.RESET}"
MSG_PREFIX_INFO = f"{Colors.CYAN}[>]{Colors.RESET}"
MSG_PREFIX_M3U = f"{Colors.GRAY}[M3U]{Colors.RESET}"
MSG_PREFIX_CHK = f"{Colors.GRAY}[CHK]{Colors.RESET}"

# Status markers for display
STATUS_WORKING = f"{Colors.GREEN}[+] Working:{Colors.RESET}"
STATUS_FAILED = f"{Colors.RED}[-] Failed:{Colors.RESET}"
STATUS_FILTERED = f"{Colors.YELLOW}[x] Filtered:{Colors.RESET}"
STATUS_VALID = f"{Colors.GREEN}[+] Valid:{Colors.RESET}"
STATUS_INVALID = f"{Colors.RED}[-] Invalid:{Colors.RESET}"

# Logger class to write to both console and file
class Logger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.log_handle = None
        self.console_enabled = True  # Can be disabled during progress display
        
    def open(self):
        """Open the log file for appending"""
        try:
            self.log_handle = open(self.log_file, 'a', encoding='utf-8')
            self.log(f"Log started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        except Exception as e:
            print(f"{Colors.RED}[-] Could not open log file: {e}{Colors.RESET}")
    
    def log(self, message, strip_colors=True, file_only=False):
        """Write message to both console and log file
        
        Args:
            message: The message to log
            strip_colors: Whether to strip color codes from file output
            file_only: If True, only write to file (not console)
        """
        # Print to console with colors (unless file_only or console disabled)
        if not file_only and self.console_enabled:
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

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='IPTV M3U Stream Extractor & Checker - Extract and validate IPTV streams from playlists',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run with default settings
  %(prog)s --reprocess-playlists              # Re-check all playlists
  %(prog)s --reprocess-streams                # Re-check all streams
  %(prog)s --workers 20 50                    # Use 20 playlist workers and 50 stream workers
  %(prog)s --input custom.sql --output out.m3u8
  %(prog)s --clear-progress                   # Start fresh (clears all progress)
  %(prog)s --no-filters                       # Don't filter movies/series/VOD
        """
    )
    
    parser.add_argument('-i', '--input', type=str, default=None,
                        help='Input SQL database file (default: middleware.sql)')
    parser.add_argument('-o', '--output', type=str, default='IPTV.m3u8',
                        help='Output M3U8 file (default: IPTV.m3u8)')
    parser.add_argument('--log', type=str, default='LOG.log',
                        help='Log file path (default: LOG.log)')
    
    # Progress control
    parser.add_argument('--reprocess-playlists', action='store_true',
                        help='Re-download and re-check all playlists (ignores playlist progress)')
    parser.add_argument('--reprocess-streams', action='store_true',
                        help='Re-check all streams (ignores stream progress)')
    parser.add_argument('--clear-progress', action='store_true',
                        help='Clear all progress files and start fresh')
    
    # Performance tuning
    parser.add_argument('-w', '--workers', type=int, nargs=2, metavar=('PLAYLIST', 'STREAM'),
                        default=[10, 30],
                        help='Number of workers: playlist_workers stream_workers (default: 10 30)')
    parser.add_argument('--timeout', type=int, default=10,
                        help='Stream check timeout in seconds (default: 10)')
    parser.add_argument('--save-interval', type=int, default=30,
                        help='Auto-save interval in seconds during stream checking (default: 30)')
    
    # Filtering options
    parser.add_argument('--no-filters', action='store_true',
                        help='Disable all content filters (include movies, series, VOD, etc.)')
    parser.add_argument('--include-radio', action='store_true',
                        help='Include radio streams (disabled by default)')
    parser.add_argument('--include-adult', action='store_true',
                        help='Include adult content (disabled by default)')
    
    # Output options
    parser.add_argument('--quiet', action='store_true',
                        help='Minimal output (only errors and final summary)')
    parser.add_argument('--no-colors', action='store_true',
                        help='Disable colored output')
    
    return parser.parse_args()

# Configuration (will be updated by command-line args)
input_file = "../middleware.sql" if os.path.exists("../middleware.sql") else "middleware.sql"
stream_progress_file = "stream_check_progress.json"
playlist_progress_file = "playlist_progress.json"
final_output_file = "IPTV.m3u8"
log_file = "LOG.log"
REPROCESS_PLAYLISTS = False
REPROCESS_STREAMS = False
STREAM_TIMEOUT = 10
SAVE_INTERVAL = 30
MAX_PLAYLIST_WORKERS = 10
MAX_STREAM_WORKERS = 30
ENABLE_FILTERS = True
INCLUDE_RADIO = False
INCLUDE_ADULT = False

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

# Cache terminal width at startup to prevent display jumping
try:
    TERMINAL_WIDTH = shutil.get_terminal_size().columns
except:
    TERMINAL_WIDTH = 80
# Ensure width is between 78 and 120 (78 is minimum for content to fit)
TERMINAL_WIDTH = max(78, min(120, TERMINAL_WIDTH))

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
# These will be built dynamically based on command-line flags
EXCLUDE_PATTERNS = []

def build_filter_patterns():
    """Build filter patterns based on configuration flags"""
    global EXCLUDE_PATTERNS
    
    if not ENABLE_FILTERS:
        EXCLUDE_PATTERNS = []
        return
    
    patterns = [
        # Movies
        re.compile(r'\b(movie|film|cinema|pelicula|filme|cine)\b', re.IGNORECASE),
        # Series/Shows
        re.compile(r'\b(series|tv\s*show|season|episode|episodio|temporada|capitulo)\b', re.IGNORECASE),
        # 24/7 channels
        re.compile(r'\b(24/?7|24h|24hs|24\s*h|24\s*hs|24\s*hour|non-stop|nonstop)\b', re.IGNORECASE),
        # VOD/On-demand
        re.compile(r'\b(vod|on\s*demand|catch\s*up|replay)\b', re.IGNORECASE),
    ]
    
    # Adult content (unless --include-adult flag is set)
    if not INCLUDE_ADULT:
        patterns.append(re.compile(r'\b(xxx|adult|porn|sexy|\+18|18\+|erotic|playboy|hustler)\b', re.IGNORECASE))
    
    # Radio (unless --include-radio flag is set)
    if not INCLUDE_RADIO:
        patterns.append(re.compile(r'\b(radio|fm)\b', re.IGNORECASE))
    
    EXCLUDE_PATTERNS = patterns

def should_filter_stream(channel_name, group_title):
    """Fast stream filtering with pre-compiled regex and early exit"""
    if not ENABLE_FILTERS:
        return False
    text = f"{channel_name} {group_title}".lower()
    for pattern in EXCLUDE_PATTERNS:
        if pattern.search(text):
            return True
    return False

def truncate_line(line, max_width):
    """Truncate a line to max_width visible characters, preserving ANSI color codes"""
    # Pattern to match ANSI escape sequences
    ansi_pattern = re.compile(r'\033\[[0-9;]+m')
    
    # Split line into parts: text and ANSI codes
    parts = ansi_pattern.split(line)
    codes = ansi_pattern.findall(line)
    
    # Calculate visible length
    visible_len = sum(len(part) for part in parts)
    
    if visible_len <= max_width:
        return line
    
    # Truncate by removing characters from visible parts
    result = []
    current_len = 0
    part_idx = 0
    
    for i, part in enumerate(parts):
        if current_len + len(part) <= max_width - 3:  # Leave room for "..."
            result.append(part)
            current_len += len(part)
            # Add corresponding ANSI code if exists
            if i < len(codes):
                result.append(codes[i])
        else:
            # Truncate this part
            remaining = max_width - 3 - current_len
            if remaining > 0:
                result.append(part[:remaining])
            result.append("...")
            break
        
    return ''.join(result) + Colors.RESET  # Ensure colors are reset

def update_dual_progress(processed_playlists, total_playlists, start_time, current_status="", current_m3u_url=""):
    """Display enhanced progress with two bars - one for playlists, one for streams"""
    elapsed = time.time() - start_time
    
    # Use cached terminal width to prevent display jumping
    term_width = TERMINAL_WIDTH
    bar_length = max(20, min(40, term_width - 40))  # Dynamic bar length
    
    with stats_lock:
        valid_m3u = global_stats['valid_m3u']
        invalid_m3u = global_stats['invalid_m3u']
        total_streams = global_stats['total_streams']
        checked_streams = global_stats['checked']
        working = global_stats['working']
        failed = global_stats['failed']
        filtered = global_stats['filtered']
        current_stream = global_stats.get('current_stream', '')
        current_m3u = global_stats.get('current_m3u', current_m3u_url)
    
    # Playlist progress bar
    playlist_percent = (processed_playlists / total_playlists * 100) if total_playlists > 0 else 0
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
    
    # Always print exactly 11 lines (8 for bars + 3 for M3U/CHK/status)
    # Move cursor up 11 lines
    sys.stdout.write('\033[11A')  # Move up 11 lines
    
    # Create border lines with exact character counts
    # Format: ┌─ Label ────...────┐  (total width = term_width)
    # Components: ┌(1) + label + fill + ┐(1) = term_width
    
    top_label = "─ Playlists "
    mid_label = "─ Streams "
    
    # Calculate fill needed: term_width - 2 (corners) - label_length
    top_fill_len = term_width - 2 - len(top_label)
    mid_fill_len = term_width - 2 - len(mid_label)
    bot_fill_len = term_width - 2  # Just corners
    
    # Ensure fills are non-negative
    top_fill = "─" * max(0, top_fill_len)
    mid_fill = "─" * max(0, mid_fill_len)
    bot_fill = "─" * max(0, bot_fill_len)
    
    top_border = f"{Colors.BOLD}{Colors.BLUE}┌{top_label}{top_fill}┐{Colors.RESET}"
    mid_border = f"{Colors.BOLD}{Colors.BLUE}├{mid_label}{mid_fill}┤{Colors.RESET}"
    bot_border = f"{Colors.BOLD}{Colors.BLUE}└{bot_fill}┘{Colors.RESET}"
    
    # Build content lines
    line1 = f"{Colors.BLUE}│{Colors.RESET} {Colors.CYAN}{playlist_bar}{Colors.RESET} {Colors.WHITE}{playlist_percent:>5.1f}%{Colors.RESET} {Colors.GRAY}({processed_playlists:,}/{total_playlists:,}){Colors.RESET}"
    line2 = f"{Colors.BLUE}│{Colors.RESET} {Colors.GREEN}[+] Valid: {valid_m3u:>6,}{Colors.RESET}  {Colors.RED}[-] Invalid: {invalid_m3u:>6,}{Colors.RESET}  {Colors.MAGENTA}Rate: {playlist_rate:>5.1f} pl/s{Colors.RESET}  {Colors.CYAN}ETA: {format_time(playlist_eta)}{Colors.RESET}"
    line3 = f"{Colors.BLUE}│{Colors.RESET} {Colors.MAGENTA}{stream_bar}{Colors.RESET} {Colors.WHITE}{stream_percent:>5.1f}%{Colors.RESET} {Colors.GRAY}({checked_streams:,}/{total_streams:,}){Colors.RESET}"
    line4 = f"{Colors.BLUE}│{Colors.RESET} {Colors.GREEN}[+] Working: {working:>6,}{Colors.RESET}  {Colors.RED}[-] Failed: {failed:>6,}{Colors.RESET}  {Colors.YELLOW}[x] Filtered: {filtered:>6,}{Colors.RESET}"
    line5 = f"{Colors.BLUE}│{Colors.RESET} {Colors.MAGENTA}Rate: {stream_rate:>5.1f} st/s{Colors.RESET}  {Colors.CYAN}ETA: {format_time(stream_eta)}{Colors.RESET}  {Colors.WHITE}Time: {format_time(elapsed)}{Colors.RESET}"
    
    # Truncate lines if terminal is too narrow (accounting for ANSI codes)
    # Visible length limit is term_width
    max_visible = term_width
    
    # Print playlist progress (clear each line to handle terminal resize)
    print(f"\033[2K\033[0G{top_border[:term_width]}")
    print(f"\033[2K\033[0G{truncate_line(line1, max_visible)}")
    print(f"\033[2K\033[0G{truncate_line(line2, max_visible)}")
    print(f"\033[2K\033[0G{mid_border[:term_width]}")
    print(f"\033[2K\033[0G{truncate_line(line3, max_visible)}")
    print(f"\033[2K\033[0G{truncate_line(line4, max_visible)}")
    print(f"\033[2K\033[0G{truncate_line(line5, max_visible)}")
    print(f"\033[2K\033[0G{bot_border[:term_width]}")
    
    # Always print 3 more lines (M3U, CHK, status) - use empty lines if not available
    # Adapt URL display to terminal width
    max_url_len = term_width - 8  # Account for "[M3U] " prefix
    
    if current_m3u:
        # Extract domain/filename from URL for display
        m3u_display = current_m3u
        if len(m3u_display) > max_url_len:
            # Show beginning and end of URL
            half = max_url_len // 2 - 2
            m3u_display = current_m3u[:half] + "..." + current_m3u[-half:]
        print(f"\033[2K\033[0G{Colors.GRAY}[M3U] {m3u_display}{Colors.RESET}")
    else:
        print("\033[2K\033[0G")  # Clear line and print empty
    
    if current_stream:
        stream_display = current_stream
        if len(stream_display) > max_url_len:
            half = max_url_len // 2 - 2
            stream_display = current_stream[:half] + "..." + current_stream[-half:]
        print(f"\033[2K\033[0G{Colors.GRAY}[CHK] {stream_display}{Colors.RESET}")
    else:
        print("\033[2K\033[0G")  # Clear line and print empty
    
    if current_status:
        print(f"\033[2K\033[0G{current_status[:term_width-2]}")  # Limit to terminal width
    else:
        print("\033[2K\033[0G")  # Clear line and print empty
    
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

def extract_country_from_tvg_id(tvg_id):
    """Extract country code from tvg_id like 'CNNBrasil.br' -> 'BR'"""
    if not tvg_id:
        return None
    
    tvg_id_lower = tvg_id.lower()
    
    # Check for country code after a dot (e.g., "channel.br", "channel.us")
    if '.' in tvg_id:
        parts = tvg_id.split('.')
        potential_country = parts[-1].upper()
        
        # Map common TLDs to country codes
        country_map = {
            'BR': 'BR', 'US': 'US', 'UK': 'UK', 'CA': 'CA',
            'AR': 'AR', 'MX': 'MX', 'ES': 'ES', 'FR': 'FR',
            'DE': 'DE', 'IT': 'IT', 'PT': 'PT', 'CL': 'CL',
            'CO': 'CO', 'PE': 'PE', 'VE': 'VE', 'EC': 'EC',
        }
        
        if potential_country in country_map:
            return country_map[potential_country]
    
    # Check for country code prefix pattern (e.g., "br#channel-name", "br-channel")
    country_prefixes = {
        'br': 'BR', 'us': 'US', 'uk': 'UK', 'ca': 'CA',
        'ar': 'AR', 'mx': 'MX', 'es': 'ES', 'fr': 'FR',
        'de': 'DE', 'it': 'IT', 'pt': 'PT', 'cl': 'CL',
    }
    
    for prefix, country_code in country_prefixes.items():
        if tvg_id_lower.startswith(f'{prefix}#') or tvg_id_lower.startswith(f'{prefix}-') or tvg_id_lower.startswith(f'{prefix}_'):
            return country_code
    
    return None

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
    print(f"{Colors.GREEN}[+] Valid: {valid_m3u}{Colors.RESET}  {Colors.RED}[-] Invalid: {invalid_m3u}{Colors.RESET}  {Colors.BLUE}Streams Found:{Colors.RESET} {Colors.GREEN}{streams_found:,}{Colors.RESET}")
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
    print(f"{Colors.GREEN}[+] Working: {working:,}{Colors.RESET}  {Colors.RED}[-] Failed: {failed:,}{Colors.RESET}  {Colors.YELLOW}[x] Filtered: {filtered:,}{Colors.RESET}")
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
    print(f"{Colors.GREEN}[+] Total URLs found: {stats['total_matches']}{Colors.RESET}")
    print(f"{Colors.GREEN}[+] Unique URLs: {len(uniq)}{Colors.RESET}")
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
    
    status = check_channel_status(stream_url, timeout=STREAM_TIMEOUT, extended_timeout=STREAM_TIMEOUT + 5)
    if status == 'Alive':
        codec_name, video_bitrate, resolution, fps = get_detailed_stream_info(stream_url)
        audio_info = get_audio_bitrate(stream_url)
        
        # Try to extract country from tvg_id first, then fallback to group_title/channel_name
        tvg_id = stream['info'].get('tvg_id', '')
        country = extract_country_from_tvg_id(tvg_id)
        if not country:
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
            'country': country,
            'channel_name': channel_name,
            'group_title': group_title,
            'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        with stats_lock:
            global_stats['checked'] += 1
            global_stats['working'] += 1
            global_stats['last_status'] = 'working'
    else:
        result = {
            'status': 'failed', 
            'reason': 'Stream not working',
            'channel_name': channel_name,
            'url': stream_url,
            'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
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
            print(f"\n{Colors.GREEN}[+] Output written to: {output_file}{Colors.RESET}")
    except Exception as e:
        if not incremental:
            print(f"\n{Colors.RED}[-] Error writing output: {e}{Colors.RESET}")

def save_stream_progress(data):
    with lock:
        temp_file = stream_progress_file + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_file, stream_progress_file)
        except Exception as e:
            print(f"\n{Colors.RED}[-] Error saving stream progress: {e}{Colors.RESET}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

def load_stream_progress():
    if not os.path.exists(stream_progress_file) or REPROCESS_STREAMS:
        return {}
    try:
        with open(stream_progress_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"{Colors.YELLOW}[!] Could not load stream progress: {e}{Colors.RESET}")
        return {}

def load_playlist_progress():
    """Load the list of already processed playlist URLs"""
    if not os.path.exists(playlist_progress_file) or REPROCESS_PLAYLISTS:
        return {}
    try:
        with open(playlist_progress_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Support both old format (list) and new format (dict)
            if 'playlists' in data:
                return data['playlists']
            elif 'processed_playlists' in data:
                # Old format - convert to new format
                return {url: {'status': 'processed', 'timestamp': data.get('last_updated', '')} 
                        for url in data['processed_playlists']}
            return {}
    except Exception as e:
        print(f"{Colors.YELLOW}[!] Could not load playlist progress: {e}{Colors.RESET}")
        return {}

def save_playlist_progress(processed_playlists_info):
    """Save detailed playlist progress information"""
    try:
        with open(playlist_progress_file, 'w', encoding='utf-8') as f:
            json.dump({
                'version': '2.0',
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': len(processed_playlists_info),
                'playlists': processed_playlists_info
            }, f, indent=2)
    except Exception as e:
        print(f"\n{Colors.RED}[-] Error saving playlist progress: {e}{Colors.RESET}")

def graceful_exit(signum=None, frame=None):
    """Handle graceful exit - save progress and write output"""
    print(f"\n\n{Colors.YELLOW}[!] Interrupted! Saving progress...{Colors.RESET}")
    
    # Save stream progress
    global stream_progress_data, working_streams_data, processed_playlists_data
    if stream_progress_data:
        # Save directly without using the lock to avoid deadlock
        temp_file = stream_progress_file + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(stream_progress_data, f, indent=2)
            os.replace(temp_file, stream_progress_file)
            print(f"{Colors.GREEN}[+] Stream progress saved ({len(stream_progress_data):,} streams){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[-] Error saving: {e}{Colors.RESET}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
    
    # Save playlist progress
    if processed_playlists_data:
        try:
            save_playlist_progress(processed_playlists_data)
            print(f"{Colors.GREEN}[+] Playlist progress saved ({len(processed_playlists_data):,} playlists){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[-] Error saving playlist progress: {e}{Colors.RESET}")
    
    # Write partial output if we have working streams
    if working_streams_data:
        print(f"{Colors.CYAN}→ Writing partial results to {final_output_file}...{Colors.RESET}")
        try:
            organized = organize_streams_by_country_and_bitrate(working_streams_data)
            write_m3u_output(organized, final_output_file, None)
            print(f"{Colors.GREEN}[+] Partial results saved ({len(working_streams_data)} working streams){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[-] Error writing output: {e}{Colors.RESET}")
    
    print(f"{Colors.YELLOW}→ Exiting now{Colors.RESET}\n")
    os._exit(0)  # Force exit immediately without waiting for threads

if __name__ == '__main__':
    # Parse command-line arguments
    args = parse_arguments()
    
    # Update configuration from arguments
    if args.input:
        input_file = args.input
    final_output_file = args.output
    log_file = args.log
    REPROCESS_PLAYLISTS = args.reprocess_playlists
    REPROCESS_STREAMS = args.reprocess_streams
    STREAM_TIMEOUT = args.timeout
    SAVE_INTERVAL = args.save_interval
    MAX_PLAYLIST_WORKERS, MAX_STREAM_WORKERS = args.workers
    ENABLE_FILTERS = not args.no_filters
    INCLUDE_RADIO = args.include_radio
    INCLUDE_ADULT = args.include_adult
    
    # Build filter patterns based on flags
    build_filter_patterns()
    
    # Clear progress if requested
    if args.clear_progress:
        for progress_file in [stream_progress_file, playlist_progress_file]:
            if os.path.exists(progress_file):
                os.remove(progress_file)
                print(f"{Colors.YELLOW}[+] Cleared {progress_file}{Colors.RESET}")
    
    # Register signal handlers for graceful exit
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)
    
    # Initialize logger
    logger = Logger(log_file)
    logger.open()
    
    logger.log(f"\n{Colors.BOLD}{Colors.CYAN}{'═' * 78}{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.CYAN}  IPTV M3U Stream Extractor & Checker{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.CYAN}{'═' * 78}{Colors.RESET}\n\n")
    
    # Show configuration
    if not args.quiet:
        logger.log(f"{Colors.BOLD}{Colors.BLUE}Configuration:{Colors.RESET}\n")
        logger.log(f"  {Colors.CYAN}Input:{Colors.RESET} {input_file}\n")
        logger.log(f"  {Colors.CYAN}Output:{Colors.RESET} {final_output_file}\n")
        logger.log(f"  {Colors.CYAN}Workers:{Colors.RESET} {MAX_PLAYLIST_WORKERS} playlist, {MAX_STREAM_WORKERS} stream\n")
        logger.log(f"  {Colors.CYAN}Stream Timeout:{Colors.RESET} {STREAM_TIMEOUT}s\n")
        logger.log(f"  {Colors.CYAN}Filters:{Colors.RESET} {'Disabled' if not ENABLE_FILTERS else 'Enabled'}\n")
        if ENABLE_FILTERS:
            filters = []
            if not INCLUDE_ADULT:
                filters.append("Adult")
            if not INCLUDE_RADIO:
                filters.append("Radio")
            if filters:
                logger.log(f"  {Colors.CYAN}Excluding:{Colors.RESET} Movies, Series, VOD, 24/7, {', '.join(filters)}\n")
        logger.log(f"  {Colors.CYAN}Reprocess Playlists:{Colors.RESET} {'Yes' if REPROCESS_PLAYLISTS else 'No'}\n")
        logger.log(f"  {Colors.CYAN}Reprocess Streams:{Colors.RESET} {'Yes' if REPROCESS_STREAMS else 'No'}\n\n")
    
    if not IPTV_CHECKER_AVAILABLE:
        logger.log(f"{Colors.RED}[-] IPTV_checker.py not available. Cannot check streams.{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    if not os.path.exists(input_file):
        logger.log(f"{Colors.RED}[-] SQL database file not found: {input_file}{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    playlist_urls = extract_urls_from_sql()
    if not playlist_urls:
        logger.log(f"{Colors.RED}[-] No M3U URLs found in database{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    logger.log(f"{Colors.GREEN}[+] Found {len(playlist_urls)} unique M3U URLs{Colors.RESET}\n\n")
    
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
        processed_playlists = {}  # Clear the dict to track fresh
        processed_playlists_data = processed_playlists
    else:
        logger.log(f"{Colors.GREEN}  All {len(playlist_urls):,} playlists need processing{Colors.RESET}\n\n")
    
    if not playlist_urls:
        logger.log(f"{Colors.GREEN}[+] All playlists already processed!{Colors.RESET}\n")
        logger.log(f"{Colors.CYAN}  Set REPROCESS_PLAYLISTS=True to re-check them{Colors.RESET}\n\n")
        logger.close()
        sys.exit(0)
    
    # Save initial progress state
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Saving initial progress state...{Colors.RESET}\n")
    save_stream_progress(stream_progress)
    logger.log(f"{Colors.GREEN}[+] Progress saved{Colors.RESET}\n\n")
    
    # Initialize stats
    with stats_lock:
        global_stats['start_time'] = time.time()
        global_stats['total_m3u'] = len(playlist_urls)
    
    logger.log(f"{Colors.BOLD}{Colors.BLUE}→ Starting playlist extraction...{Colors.RESET}\n")
    logger.log(f"  Playlists to process: {len(playlist_urls):,}\n")
    logger.log(f"  Playlist workers: {MAX_PLAYLIST_WORKERS}\n")
    logger.log(f"  Stream workers: {MAX_STREAM_WORKERS}\n")
    logger.log(f"  Stream timeout: {STREAM_TIMEOUT}s\n")
    logger.log(f"  Filters enabled: {ENABLE_FILTERS}\n\n")
    
    parse_start_time = time.time()
    
    # Rebuild working_streams from previously checked streams
    working_streams = []
    if stream_progress:
        logger.log(f"{Colors.CYAN}→ Rebuilding working streams from progress...{Colors.RESET}\n")
        updated_count = 0
        for stream_key, stream_data in stream_progress.items():
            if isinstance(stream_data, dict) and stream_data.get('status') == 'working':
                # Update country code if it was incorrectly detected before
                tvg_id = stream_data.get('info', {}).get('tvg_id', '')
                if tvg_id:
                    new_country = extract_country_from_tvg_id(tvg_id)
                    if new_country and new_country != stream_data.get('country'):
                        stream_data['country'] = new_country
                        stream_progress[stream_key] = stream_data  # Update in progress dict
                        updated_count += 1
                
                working_streams.append(stream_data)
        logger.log(f"{Colors.GREEN}  Loaded {len(working_streams):,} previously working streams{Colors.RESET}\n")
        logger.log(f"{Colors.GRAY}  (Stream keys sample: {list(stream_progress.keys())[:3]}...){Colors.RESET}\n", file_only=True)
        if updated_count > 0:
            logger.log(f"{Colors.YELLOW}  Updated country codes for {updated_count} streams{Colors.RESET}\n")
        logger.log("\n")
        
        # Sync working_streams_data with loaded streams for graceful_exit
        working_streams_data = working_streams.copy()
        logger.log(f"{Colors.GRAY}  Synced working_streams_data: {len(working_streams_data)} streams{Colors.RESET}\n", file_only=True)
    else:
        logger.log(f"{Colors.YELLOW}  No previous stream progress found{Colors.RESET}\n\n")
    
    all_streams_count = 0
    save_counter = 0
    processed_count = 0
    
    # Reserve space for progress bars (11 lines: 8 for bars + 3 for M3U/CHK/status)
    for _ in range(11):
        print()
    
    # Display initial progress (this will move cursor back up and print)
    update_dual_progress(0, len(playlist_urls), parse_start_time, "")
    
    # Process playlists with BOTH parallel downloading AND sequential stream checking
    with ThreadPoolExecutor(max_workers=MAX_PLAYLIST_WORKERS) as playlist_executor, \
         ThreadPoolExecutor(max_workers=MAX_STREAM_WORKERS) as stream_executor:
        
        last_update_time = time.time()
        
        # Submit playlists in batches for parallel downloading
        playlist_batch_size = MAX_PLAYLIST_WORKERS * 2  # Download 2x workers at a time
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
                
                # Update current M3U URL being processed
                with stats_lock:
                    global_stats['current_m3u'] = url
                
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
                        status_msg = f"{Colors.GREEN}[+] [{idx}/{len(playlist_urls)}] Found {len(filtered_streams)} streams (filtered {filtered_out_count}/{original_count}) ({download_time:.1f}s){Colors.RESET}"
                        logger.log(f"[INFO] Playlist {idx}/{len(playlist_urls)}: {url}\n", file_only=True)
                        logger.log(f"       Streams: {len(filtered_streams)} valid, {filtered_out_count} filtered, {original_count} total\n", file_only=True)
                    elif original_count > 0:
                        status_msg = f"{Colors.YELLOW}[!] [{idx}/{len(playlist_urls)}] All {original_count} streams filtered out ({download_time:.1f}s){Colors.RESET}"
                        logger.log(f"[WARN] Playlist {idx}/{len(playlist_urls)}: All {original_count} streams filtered - {url}\n", file_only=True)
                        # Mark as processed even if all streams filtered
                        processed_playlists[url] = {
                            'status': 'all_filtered',
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'streams_found': original_count,
                            'streams_filtered': original_count
                        }
                        processed_playlists_data[url] = processed_playlists[url]
                        processed_count += 1
                        # Save progress for filtered playlists immediately
                        save_playlist_progress(processed_playlists)
                    else:
                        status_msg = f"{Colors.RED}[-] [{idx}/{len(playlist_urls)}] Empty or timeout ({download_time:.1f}s){Colors.RESET}"
                        logger.log(f"[ERROR] Playlist {idx}/{len(playlist_urls)}: Empty or timeout - {url}\n", file_only=True)
                        # Mark invalid/empty playlists as processed so they won't be retried
                        processed_playlists[url] = {
                            'status': 'invalid',
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'streams_found': 0,
                            'reason': 'empty_or_timeout'
                        }
                        processed_playlists_data[url] = processed_playlists[url]
                        processed_count += 1
                        # Save progress for invalid playlists immediately
                        save_playlist_progress(processed_playlists)
                    update_dual_progress(processed_count, len(playlist_urls), parse_start_time, status_msg, url)
                    
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
                                    logger.log(f"{Colors.GRAY}  DEBUG: Added working stream '{result.get('channel_name', 'Unknown')[:40]}' (total now: {len(working_streams)}){Colors.RESET}\n", file_only=True)
                                    # Log working stream details
                                    if stream_count % 50 == 0:  # Log every 50 working streams
                                        logger.log(f"[WORK] {result.get('channel_name', 'Unknown')} - {result.get('resolution', 'N/A')} @ {result.get('video_bitrate', 'N/A')}\n", file_only=True)
                            except Exception as e:
                                pass
                            
                            stream_count += 1
                            
                            # Update progress display every second
                            current_time = time.time()
                            if stream_count % 10 == 0 or stream_count == len(stream_futures) or (current_time - last_update_time) >= 1.0:
                                checking_msg = f"{Colors.CYAN}[>] Checking streams... {stream_count}/{len(stream_futures)} from playlist {idx}{Colors.RESET}"
                                update_dual_progress(processed_count, len(playlist_urls), parse_start_time, checking_msg, url)
                                last_update_time = current_time
                            
                            # Auto-save progress every SAVE_INTERVAL seconds during stream checking
                            if current_time - last_save_time >= SAVE_INTERVAL:
                                save_stream_progress(stream_progress)
                                save_playlist_progress(processed_playlists)
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
                    
                    # Count working streams from this playlist
                    working_from_playlist = len([s for s in working_streams if s['url'].startswith(url[:30])])
                    
                    # Mark this playlist as processed with details
                    processed_playlists[url] = {
                        'status': 'completed',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'streams_found': original_count,
                        'streams_filtered': filtered_out_count,
                        'streams_checked': len(filtered_streams),
                        'working_streams': working_from_playlist,
                        'url': url
                    }
                    processed_playlists_data[url] = processed_playlists[url]
                    
                    # Log completion details
                    logger.log(f"[DONE] Playlist {processed_count}/{len(playlist_urls)} completed\n", file_only=True)
                    logger.log(f"       Working: {working_from_playlist}/{len(filtered_streams)} streams\n", file_only=True)
                    
                    # Final update after all streams from this playlist are done
                    done_msg = f"{Colors.GREEN}[+] Playlist {processed_count}/{len(playlist_urls)} complete - {working_from_playlist} working{Colors.RESET}"
                    update_dual_progress(processed_count, len(playlist_urls), parse_start_time, done_msg, url)
                    
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
                                save_msg = f"{Colors.GREEN}[S] M3U updated: {len(working_streams)} working streams{Colors.RESET}"
                                update_dual_progress(processed_count, len(playlist_urls), parse_start_time, save_msg, url)
                                logger.log(f"[SAVE] M3U file updated with {len(working_streams)} streams\n", file_only=True)
                        except Exception as e:
                            logger.log(f"[ERROR] Failed to write M3U: {e}\n", file_only=True)
                    
                except KeyboardInterrupt:
                    print(f"\n\n{Colors.YELLOW}[!] Interrupted by user{Colors.RESET}")
                    graceful_exit()
                except Exception as e:
                    # Mark failed playlists as processed so they won't be retried
                    processed_playlists[url] = {
                        'status': 'error',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'error': str(e) if e else 'Unknown error'
                    }
                    processed_playlists_data[url] = processed_playlists[url]
                    processed_count += 1
                    # Save progress for failed playlists
                    save_playlist_progress(processed_playlists)
                    with stats_lock:
                        global_stats['invalid_m3u'] += 1
                    pass  # Continue with next playlist
            
            # Move to next batch
            playlist_index = batch_end
    
    # Clear the progress display and move to bottom
    sys.stdout.write('\033[9B')  # Move down past progress bars
    print("\n")  # Add some space
    
    # Calculate final stats
    elapsed_total = time.time() - parse_start_time
    
    logger.log(f"\n\n{Colors.CYAN}[=] All playlists processed and streams checked!{Colors.RESET}\n\n")
    logger.log(f"  Total time: {format_time(elapsed_total)}\n")
    logger.log(f"  Playlists processed: {len(processed_playlists):,}\n")
    logger.log(f"  Valid playlists: {global_stats['valid_m3u']:,}\n")
    logger.log(f"  Invalid playlists: {global_stats['invalid_m3u']:,}\n")
    logger.log(f"  Total streams found: {global_stats['total_streams']:,}\n")
    logger.log(f"  Streams checked: {global_stats['checked']:,}\n")
    logger.log(f"  Working streams: {global_stats['working']:,}\n")
    logger.log(f"  Failed streams: {global_stats['failed']:,}\n")
    logger.log(f"  Filtered streams: {global_stats['filtered']:,}\n\n")
    
    logger.log(f"\n{Colors.CYAN}[>] Saving final progress...{Colors.RESET}\n")
    save_stream_progress(stream_progress)
    save_playlist_progress(processed_playlists)
    logger.log(f"{Colors.GREEN}[+] Progress saved{Colors.RESET}\n\n")
    
    if not working_streams:
        logger.log(f"\n{Colors.RED}[-] No working streams found{Colors.RESET}\n")
        logger.close()
        sys.exit(1)
    
    logger.log(f"{Colors.GRAY}  DEBUG: working_streams count before organize: {len(working_streams)}{Colors.RESET}\n", file_only=True)
    logger.log(f"{Colors.GRAY}  DEBUG: Sample channel names: {[s.get('channel_name', 'Unknown')[:30] for s in working_streams[:5]]}{Colors.RESET}\n", file_only=True)
    
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}[>] Organizing streams by country and bitrate...{Colors.RESET}\n")
    organized = organize_streams_by_country_and_bitrate(working_streams)
    total_organized = sum(len(streams) for streams in organized.values())
    logger.log(f"{Colors.GREEN}[+] Organized {total_organized} working streams across {len(organized)} countries{Colors.RESET}\n\n")
    logger.log(f"{Colors.BOLD}{Colors.BLUE}[>] Writing output file...{Colors.RESET}\n")
    write_m3u_output(organized, final_output_file, None)
    elapsed = time.time() - global_stats['start_time']
    logger.log(f"\n{Colors.BOLD}{Colors.GREEN}{'═' * 78}{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.GREEN}{'  ' * 15}[+] PROCESSING COMPLETE [+]{'  ' * 15}{Colors.RESET}\n")
    logger.log(f"{Colors.BOLD}{Colors.GREEN}{'═' * 78}{Colors.RESET}\n\n")
    logger.log(f"{Colors.BOLD}{Colors.BLUE}M3U Files:{Colors.RESET}\n")
    logger.log(f"  {Colors.WHITE}Total:{Colors.RESET} {global_stats['total_m3u']}  {Colors.GREEN}[+] Valid:{Colors.RESET} {global_stats['valid_m3u']}  {Colors.RED}[-] Invalid:{Colors.RESET} {global_stats['invalid_m3u']}\n")
    logger.log(f"\n{Colors.BOLD}{Colors.BLUE}Streams:{Colors.RESET}\n")
    logger.log(f"  {Colors.WHITE}Total Found:{Colors.RESET}    {global_stats['total_streams']:,}\n")
    logger.log(f"  {Colors.WHITE}Checked:{Colors.RESET}        {global_stats['checked']:,}\n")
    logger.log(f"  {Colors.GREEN}[+] Working:{Colors.RESET}     {global_stats['working']:,}\n")
    logger.log(f"  {Colors.RED}[-] Failed:{Colors.RESET}      {global_stats['failed']:,}\n")
    logger.log(f"  {Colors.YELLOW}[x] Filtered:{Colors.RESET}    {global_stats['filtered']:,}\n")
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
