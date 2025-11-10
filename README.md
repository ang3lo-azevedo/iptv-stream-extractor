# IPTV Stream Extractor - Usage Guide

## Basic Usage

```bash
python3 extract_streams.py
```

This will run with default settings and resume from previous progress.

## Command-Line Options

### Input/Output

- `-i, --input FILE` - Specify input SQL database file (default: middleware.sql)
- `-o, --output FILE` - Specify output M3U8 file (default: IPTV.m3u8)
- `--log FILE` - Specify log file path (default: LOG.log)

### Progress Control

- `--reprocess-playlists` - Re-download and re-check ALL playlists (ignores saved playlist progress)
- `--reprocess-streams` - Re-check ALL streams (ignores saved stream check progress)
- `--clear-progress` - Delete all progress files and start completely fresh

### Performance Tuning

- `-w, --workers PLAYLIST STREAM` - Set number of concurrent workers
  - Example: `-w 20 50` = 20 playlist downloaders + 50 stream checkers
  - Default: 10 playlist workers, 30 stream workers
  - Higher values = faster but more CPU/network usage

- `--timeout SECONDS` - Stream check timeout in seconds (default: 10)
  - Lower = faster but may miss slow streams
  - Higher = more thorough but slower

- `--save-interval SECONDS` - How often to auto-save during stream checking (default: 30)
  - Lower = less lost progress on interruption but more disk I/O
  - Higher = better performance but more progress lost if interrupted

### Content Filtering

- `--no-filters` - Disable ALL content filters (include everything)
- `--include-radio` - Include radio/FM streams (excluded by default)
- `--include-adult` - Include adult/XXX content (excluded by default)

**Default filters** (when not using `--no-filters`):
- Movies and films
- TV series and shows
- VOD/On-demand content
- 24/7 channels
- Radio streams (unless `--include-radio`)
- Adult content (unless `--include-adult`)

### Display Options

- `--quiet` - Minimal output (only errors and final summary)
- `--no-colors` - Disable colored terminal output

## Usage Examples

### Start Fresh
```bash
# Delete all progress and start from scratch
python3 extract_streams.py --clear-progress
```

### Resume After Interruption
```bash
# Simply run again - it will resume automatically
python3 extract_streams.py
```

### Re-check Everything
```bash
# Re-download all playlists and re-check all streams
python3 extract_streams.py --reprocess-playlists --reprocess-streams
```

### Performance Tuning
```bash
# Use more workers for faster processing
python3 extract_streams.py --workers 20 50

# Use faster timeout for quick checking
python3 extract_streams.py --timeout 5 --workers 15 60
```

### Custom Filtering
```bash
# Include everything (no filters)
python3 extract_streams.py --no-filters

# Include radio but filter everything else
python3 extract_streams.py --include-radio

# Include adult content
python3 extract_streams.py --include-adult
```

### Custom Files
```bash
# Use custom input/output files
python3 extract_streams.py --input my_database.sql --output my_playlist.m3u8 --log my_log.txt
```

### Combined Options
```bash
# Fast checking with more workers and no VOD/movies
python3 extract_streams.py --workers 25 75 --timeout 5 --save-interval 60

# Complete re-scan with everything included
python3 extract_streams.py --clear-progress --no-filters --workers 20 50
```

## Progress Files

The script saves progress in two JSON files:

- `stream_check_progress.json` - Results of checked streams (working/failed)
- `playlist_progress.json` - List of processed playlists

When you stop and restart:
- Already-checked streams are skipped
- Already-processed playlists are skipped
- Working streams found so far are preserved in IPTV.m3u8

To start fresh, use `--clear-progress` or manually delete these files.

## Tips

1. **For huge databases**: Use higher worker counts and lower timeout
   ```bash
   python3 extract_streams.py --workers 30 100 --timeout 5
   ```

2. **For quality over speed**: Use lower worker counts and higher timeout
   ```bash
   python3 extract_streams.py --workers 5 15 --timeout 20
   ```

3. **To only re-check specific playlists**: Delete `playlist_progress.json` and run with `--reprocess-playlists`

4. **To only re-check streams from existing playlists**: Keep `playlist_progress.json` but delete `stream_check_progress.json` or use `--reprocess-streams`

5. **Monitor progress**: The script auto-saves every 30 seconds (configurable), so you can safely stop/start anytime
