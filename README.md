# IPTV Stream Extractor

A high-performance Python script that extracts, validates, and organizes IPTV streams from M3U playlists stored in SQL databases. Features intelligent filtering, expiry date tracking, concurrent processing, and resume capability for large-scale stream validation.

## Features

### Core Functionality
- **Automatic SQL Detection**: Auto-detects any `.sql` file in the directory
- **M3U Playlist Extraction**: Extracts M3U playlist URLs from SQL database dumps  
- **Concurrent Processing**: Parallel download and validation of streams
- **Stream Validation**: Checks if streams are alive using video analysis
- **Quality Detection**: Extracts codec, resolution, bitrate, FPS, and audio info
- **Country Organization**: Groups streams by country code
- **Bitrate Sorting**: Sorts channels by video quality with backup streams

### Advanced Features
- **Expiry Date Tracking**: Extracts subscription expiry dates from IPTV panel APIs
- **Intelligent Filtering**: Auto-filters movies, series, VOD, adult content, 24/7 streams
- **Expiry Filtering**: Filters streams expiring in less than 30 days  
- **Resume Capability**: Saves progress and resumes from interruption
- **Duplicate Detection**: Eliminates duplicate streams and playlists
- **Real-time Progress**: Live ETA tracking for both playlists and streams
- **Graceful Exit**: Ctrl+C saves current progress before exiting

### Output Features
- **Organized M3U8**: Clean, country-grouped M3U8 playlist with quality info
- **Comprehensive Logging**: Detailed logs of all operations
- **Progress Persistence**: JSON-based progress tracking
- **Expiry Display**: Shows subscription expiry dates in channel names

## Requirements

### Python Version
- Python 3.7 or higher

### Dependencies
```bash
pip install requests
```

### Optional (for stream validation)
The script uses the `IPTVChecker-BitRate` module for stream validation. If not available:
```bash
git clone https://github.com/BashethUDeveloper/IPTVChecker-BitRate.git
```
Or the script will automatically skip stream validation.

## Installation

1. **Clone the repository**:
```bash
git clone https://github.com/ang3lo-azevedo/iptv-stream-extractor.git
cd iptv-stream-extractor
```

2. **Install dependencies**:
```bash
pip install requests
```

3. **Clone IPTVChecker-BitRate** (optional but recommended):
```bash
git clone https://github.com/BashethUDeveloper/IPTVChecker-BitRate.git
```

## Usage

### Basic Usage

The script automatically detects SQL files in the current or parent directory:

```bash
python3 extract_streams.py
```

### Specify Custom SQL File

```bash
python3 extract_streams.py --input custom.sql
```

### Specify Output File

```bash
python3 extract_streams.py --output mystreams.m3u8
```

### Reprocess Options

```bash
# Reprocess all playlists (skip existing playlist progress)
python3 extract_streams.py --reprocess-playlists

# Recheck all streams (skip existing stream progress)
python3 extract_streams.py --reprocess-streams

# Reprocess everything from scratch
python3 extract_streams.py --reprocess-playlists --reprocess-streams
```

### Advanced Options

```bash
# Adjust stream timeout (default: 10 seconds)
python3 extract_streams.py --stream-timeout 15

# Change progress save interval (default: 30 seconds)
python3 extract_streams.py --save-interval 60

# Minimal output mode
python3 extract_streams.py --quiet

# Disable colors
python3 extract_streams.py --no-colors
```

### Full Example

```bash
python3 extract_streams.py \
    --input middleware.sql \
    --output IPTV.m3u8 \
    --stream-timeout 15 \
    --save-interval 60
```

## How It Works

### 1. SQL File Detection
- Automatically searches for `.sql` files in current and parent directories
- Prioritizes `middleware.sql` if multiple SQL files exist
- Falls back to command-line specified file

### 2. URL Extraction
- Parses SQL dump file line by line
- Extracts M3U playlist URLs using regex patterns
- Removes duplicates and categorizes by playlist type
- Displays statistics (total URLs, unique URLs, types)

### 3. Playlist Processing
- Downloads M3U playlists concurrently (configurable workers)
- Parses EXTINF metadata (channel name, logo, group, etc.)
- **Expiry Extraction**: Queries IPTV panel API once per playlist
- Applies playlist-wide expiry date to all streams
- Saves playlist progress incrementally

### 4. Stream Filtering
Content filtering removes:
- **Movies**: Matches patterns like `(2024)`, `[2023]`, `FULL HD MOVIE`
- **Series**: Matches `S01E01`, `1x01`, `E01` patterns  
- **VOD**: Filters `VOD`, `On Demand`, `Series` groups
- **24/7**: Filters constant replay channels
- **Adult Content**: Filters explicit channel names/groups
- **Radio**: Filters audio-only streams
- **Expiring Soon**: Filters streams expiring in < 30 days

### 5. Stream Validation
- Checks if streams are alive using `IPTVChecker-BitRate`
- Extracts technical info:
  - Video codec (H264, H265, etc.)
  - Resolution (SD, 720p, 1080p, 4K)
  - Video bitrate
  - Frame rate (FPS)
  - Audio codec and bitrate
- Determines country from TVG-ID, group title, or channel name
- Saves validated stream data with expiry dates

### 6. Organization
- Groups streams by country
- Sorts channels alphabetically
- Ranks streams by bitrate (highest first)
- Creates primary + backup stream hierarchy
- Preserves metadata (logos, IDs, names)

### 7. Output Generation
- Creates organized M3U8 file
- Format: `#EXTINF:-1 [metadata],Channel Name [Resolution Bitrate] [Expires: YYYY-MM-DD]`
- Groups by country with headers
- Shows stream count per country

## File Structure

```
iptv-stream-extractor/
├── extract_streams.py          # Main script
├── middleware.sql              # SQL database (auto-detected)
├── IPTV.m3u8                  # Output playlist
├── LOG.txt                    # Detailed log file
├── stream_check_progress.json # Stream validation progress
├── playlist_progress.json     # Playlist processing progress
├── IPTVChecker-BitRate/       # Stream checker module
│   ├── IPTV_checker.py
│   └── requirements.txt
├── update_expiry_dates.py     # Utility to update existing progress
├── regenerate_m3u8.py         # Utility to regenerate M3U8 from progress
└── README.md                  # This file
```

## Progress Files

### stream_check_progress.json
Stores validation results for each stream:
```json
{
  "Channel_Name_http://url": {
    "status": "working",
    "extinf": "#EXTINF:...",
    "url": "http://...",
    "info": {...},
    "codec": "H264",
    "video_bitrate": "2000 kbps",
    "resolution": "1080p",
    "fps": 30,
    "audio_info": "128 kbps AAC",
    "country": "US",
    "expiry_date": "2025-12-31",
    "checked_at": "2025-11-10 22:00:00"
  }
}
```

### playlist_progress.json
Tracks playlist processing status:
```json
{
  "version": "2.0",
  "playlists": {
    "http://example.com/playlist.m3u": {
      "status": "completed",
      "timestamp": "2025-11-10 22:00:00",
      "streams_found": 1000,
      "streams_filtered": 200,
      "streams_checked": 800,
      "working_streams": 650
    }
  }
}
```

## Configuration

### Stream Filtering Patterns

Edit `build_filter_patterns()` function in `extract_streams.py` to customize:

```python
# Movie patterns
r'\(\d{4}\)',           # (2024)
r'\[\d{4}\]',           # [2024]
r'MOVIE.*\d{4}',        # MOVIE 2024

# Series patterns
r'S\d{2}E\d{2}',        # S01E01
r'\d+x\d+',             # 1x01
r'E\d{2,3}',            # E01

# Content types
r'VOD|On Demand',
r'24/7|24HS',
r'ADULT|XXX|PORN',
r'RADIO'
```

### Expiry Date Filtering

The script filters out streams that expire in less than 30 days. Streams without expiry dates or expiring in 30+ days are kept.

To modify this behavior, edit the filter logic in `extract_streams.py` around line 1407.

### Worker Configuration

Adjust concurrent processing in the script:
```python
MAX_PLAYLIST_WORKERS = 5   # Concurrent playlist downloads
MAX_STREAM_WORKERS = 8     # Concurrent stream checks
```

## Utilities

### Update Expiry Dates for Existing Progress

```bash
python3 update_expiry_dates.py
```

This script:
- Reads existing `stream_check_progress.json`
- Queries IPTV panel APIs for expiry dates
- Updates streams with expiry information
- Creates a backup before modifying

### Regenerate M3U8 from Progress

```bash
python3 regenerate_m3u8.py
```

This script:
- Reads `stream_check_progress.json`
- Regenerates `IPTV.m3u8` without rechecking streams
- Useful after manual progress file edits

## Expiry Date Extraction

The script automatically extracts subscription expiry dates using the same method as m3u4u:

1. **URL Parsing**: Extracts server, username, password from stream URLs
   ```
   http://server.com:8080/username/password/12345.ts
   ```

2. **API Query**: Calls IPTV panel's `player_api.php`:
   ```
   http://server.com:8080/player_api.php?username=xxx&password=yyy
   ```

3. **Expiry Extraction**: Reads `user_info.exp_date` from JSON response

4. **Caching**: Caches expiry per panel to avoid repeated API calls

5. **Application**: Applies expiry to all streams from the same playlist

## Performance

### Typical Performance
- **URL Extraction**: ~50,000 lines/second
- **Playlist Download**: 5 concurrent downloads
- **Stream Validation**: 8 concurrent checks
- **Progress Saving**: Every 30 seconds (configurable)

### Large Dataset Example
- 445,000 playlists
- 1.5 million streams
- Estimated time: ~24 hours (with 8 concurrent stream workers)
- Resume capability: Yes, can stop/start anytime

### Optimization Tips
1. **Increase workers** for faster processing (uses more bandwidth/CPU)
2. **Adjust timeouts** based on network speed
3. **Use SSD** for faster JSON file operations
4. **Filter aggressively** to reduce streams to check

## Display Features

### Real-time Progress Bars
```
Playlists  [████████░░░░░░░░░░░░] 8,234/445,892 (2%) | ETA: 3526h 48m
Streams    [██████████████████░░] 12,545/62,891 (20%) | ETA: 24h 15m
Valid M3U  [████████░░░░░░░░░░░░] 6,234 (76%)
Invalid    [█░░░░░░░░░░░░░░░░░░░] 2,000 (24%)
Filtered   [███████████░░░░░░░░░] 38,234 (61%)
Checking   [███████████████████░] 45,634 (73%)
Working    [████████████████░░░░] 12,545 (27%)
Failed     [█████████████░░░░░░░] 33,089 (73%)

[>] Checking streams... 245/800 from playlist 8234
[!] Current M3U: http://example.com/playlist.m3u
[>] Last: Channel XYZ - working
```

### Statistics Display
- Total playlists processed
- Streams found and filtered
- Working vs failed streams
- Country-wise distribution
- Processing speeds and ETAs

## Error Handling

### Graceful Exit (Ctrl+C)
- Saves current progress
- Writes partial results
- Displays summary statistics
- Safe to resume later

### Automatic Recovery
- Handles network timeouts
- Skips corrupted playlists
- Continues on stream validation errors
- Logs all errors to `LOG.txt`

### Common Issues

**Issue**: No SQL file found
```
Solution: Place any .sql file in the directory or use --input parameter
```

**Issue**: IPTVChecker not available
```
Solution: Clone IPTVChecker-BitRate repository or install it
```

**Issue**: Streams fail validation
```
Solution: Increase --stream-timeout or check network connectivity
```

**Issue**: Progress file corruption
```
Solution: Delete .json files and restart (or restore from .backup)
```

## Examples

### Example 1: Basic Extraction
```bash
python3 extract_streams.py
```
Output: Extracts from auto-detected SQL, creates `IPTV.m3u8`

### Example 2: Fresh Start
```bash
rm stream_check_progress.json playlist_progress.json
python3 extract_streams.py
```
Output: Processes everything from scratch

### Example 3: Resume After Interrupt
```bash
python3 extract_streams.py
# Press Ctrl+C to stop
python3 extract_streams.py
```
Output: Resumes from last saved progress

### Example 4: Update Expiry Dates
```bash
python3 update_expiry_dates.py
python3 regenerate_m3u8.py
```
Output: Updates existing streams with expiry dates, regenerates M3U8

## Output Format

### M3U8 File Structure
```m3u
#EXTM3U
# Generated: 2025-11-10 22:00:00
# Organized by country, alphabetically, and by bitrate

# ===== US (1,234 streams) =====
#EXTINF:-1 tvg-id="ABC" tvg-name="ABC HD" tvg-logo="http://..." group-title="US",ABC HD [1080p 5000 kbps] [Expires: 2025-12-31]
http://server.com/stream/1234.ts
#EXTINF:-1 tvg-id="ABC" tvg-name="ABC SD" tvg-logo="http://..." group-title="US",ABC HD backup 1 [720p 2500 kbps] [Expires: 2025-12-31]
http://server.com/stream/5678.ts

# ===== UK (856 streams) =====
...
```

### Channel Naming Convention
- Primary stream: `Channel Name [Resolution Bitrate] [Expires: Date]`
- Backup streams: `Channel Name backup N [Resolution Bitrate] [Expires: Date]`

## Troubleshooting

### Memory Issues
- Process smaller batches by limiting playlists
- Increase save interval to reduce I/O
- Use `--quiet` mode to reduce console output

### Slow Performance
- Check network speed
- Reduce concurrent workers
- Increase timeouts for slow streams
- Filter more aggressively

### Display Issues
- Use `--no-colors` if terminal doesn't support ANSI
- Check terminal width (script auto-detects)
- Redirect output if running in background
