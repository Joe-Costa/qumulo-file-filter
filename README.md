# grumpwalk.py

High-performance async file search tool for Qumulo storage systems.

## Features

- **Fast async operations** - Direct REST API calls with concurrent requests
- **Name pattern matching** - Glob wildcards and regex support
- **Time-based filtering** - Search by creation, modification, access, or change time
- **Size-based filtering** - Find files by size with smart directory skipping
- **Owner filtering** - Filter by user/group with identity expansion
- **Type filtering** - Search files, directories, or symlinks
- **Symlink resolution** - Display symlink targets as absolute paths
- **Progress tracking** - Real-time statistics with smart skip counters
- **Multiple output formats** - Plain text, JSON, or CSV
- **Directory scope preview** - Shows total subdirs/files before search
- **Owner reports** - Generate storage capacity breakdowns by owner
- **Permissions reports** - Retrieve permissions ACLs of objects in tree
- **Duplicate detection** - Find duplicate files using adaptive sampling (advisory only)

## Requirements

- Python 3.8+
- `aiohttp` - Install with: `pip install aiohttp`
- `qumulo_api` - Install with: `pip install qumulo_api`
- `ujson` (optional) - For faster JSON parsing: `pip install ujson`
- `xxhash` (optional) - For 10x faster duplicate detection: `pip install xxhash`
- Qumulo cluster credentials (use `qq login`)

## Installation

```bash
git clone https://github.com/Joe-Costa/grumpwalk.git
cd grumpwalk
chmod +x grumpwalk.py
pip install -r requirements.txt
```

## Quick Examples

```bash
# List all files in a directory
./grumpwalk.py --host cluster.example.com --path /home

# Find files older than 30 days
./grumpwalk.py --host cluster.example.com --path /home --older-than 30

# Find large log files with progress
./grumpwalk.py --host cluster.example.com --path /var --name '*.log' --larger-than 100MB --progress

# Search for Python test files
./grumpwalk.py --host cluster.example.com --path /code --name 'test_*.py' --type file

# Find symlinks and show their targets
./grumpwalk.py --host cluster.example.com --path /home --type symlink --resolve-links

# Generate owner capacity report
./grumpwalk.py --host cluster.example.com --path /data --owner-report --csv-out report.csv
```

## Command Reference

### Required
- `--host` - Qumulo cluster hostname or IP
- `--path` - Path to search

### Name/Type Filters
- `--name PATTERN` - Match by name (glob/regex, OR logic, repeatable)
- `--name-and PATTERN` - Match by name (AND logic, repeatable)
- `--name-case-sensitive` - Case-sensitive name matching
- `--type {file,directory,symlink}` - Filter by object type
- `--file-only` - Search files only (deprecated, use `--type file`)

### Time Filters
- `--older-than N` - Files older than N days
- `--newer-than N` - Files newer than N days
- `--time-field {creation_time,modification_time,access_time,change_time}` - Time field to use (default: modification_time)

**Field-specific time filters (AND logic):**
- `--accessed-older-than N` / `--accessed-newer-than N`
- `--modified-older-than N` / `--modified-newer-than N`
- `--created-older-than N` / `--created-newer-than N`
- `--changed-older-than N` / `--changed-newer-than N`

### Size Filters
- `--larger-than SIZE` - Larger than size (e.g., `100MB`, `1.5GiB`)
- `--smaller-than SIZE` - Smaller than size
- `--include-metadata` - Include metadata blocks in size calculations

**Supported units:** B, KB, MB, GB, TB, PB, KiB, MiB, GiB, TiB, PiB

### Owner Filters
- `--owner NAME` - Filter by owner (OR logic, repeatable)
- `--ad` - Owner is Active Directory user
- `--local` - Owner is local user
- `--uid` - Owner is UID number
- `--expand-identity` - Match equivalent identities (AD user + NFS UID)
- `--show-owner` - Display owner information in output
- `--owner-report` - Generate capacity report by owner

### Directory Options
- `--max-depth N` - Maximum directory depth
- `--omit-subdirs PATTERN` - Skip directories (supports glob and paths, repeatable)
- `--omit-path PATH` - Skip specific absolute path (must start with `/`, repeatable)
- `--max-entries-per-dir N` - Skip directories exceeding N entries

### Symlink Options
- `--resolve-links` - Show symlink targets as absolute paths

### ACL Options
- `--acl-report` - Generate ACL inventory report
- `--acl-csv FILE` - Export per-file ACL data to CSV (requires `--acl-report`)
- `--acl-resolve-names` - Resolve IDs to names in ACL output
- `--show-owner` - Include owner column in ACL reports (requires `--acl-report`)
- `--show-group` - Include group column in ACL reports (requires `--acl-report`)
- ACLs are returned in NFSv4 shorthand for brevity and compactness (`rwaxdDtTnNcCoy`).
- These rights map directly to the 14 NTFS rights in an ACE
- Refer to [The nfs4_acl man page](https://www.man7.org/linux//man-pages/man5/nfs4_acl.5.html) for details

### Duplicate Detection Options
- `--find-duplicates` - Find duplicate files (ADVISORY ONLY - verify before deletion)
- `--by-size` - Use size-only detection (fast, may have false positives)
- `--sample-points N` - Override adaptive sample count (default: 3-11 based on file size)

**How it works:**
1. Groups files by size and data blocks (metadata fingerprint)
2. For each group, computes content hash of samples in parallel
3. Uses adaptive sampling strategy:
   - Sample count: 3-11 points based on file size
   - Sample size: 64KB-1MB per sample based on file size
   - Coverage: ~0.5-1% of file content
   - Hash algorithm: xxHash3 (if installed, 10-20x faster) or BLAKE2b (fallback, 2-3x faster than SHA-256)
4. Reports files with matching fingerprints

**Limitations:**
- Uses sampling, not full file hashing
- May miss small differences in large files
- Results are advisory only - always verify before deletion
- Not suitable as sole basis for data deduplication

### Output Options
- `--json` - JSON output to stdout
- `--json-out FILE` - JSON output to file
- `--csv-out FILE` - CSV output to file
- `--all-attributes` - Include all file attributes in output
- `--limit N` - Stop after N matches
- `--progress` - Show real-time progress
- `--verbose` - Detailed logging

### Performance Options
- `--max-concurrent N` - Concurrent operations (default: 100)
- `--connector-limit N` - HTTP connection pool size (default: 100)
- `--profile` - Performance profiling

### Connection Options
- `--port PORT` - API port (default: 8000)
- `--credentials-store PATH` - Credentials file path

## Pattern Matching

### Glob Patterns (shell-style)
```bash
--name '*.log'           # All log files
--name 'test_*'          # Files starting with test_
--name 'file?.txt'       # file1.txt, fileA.txt, etc.
```

### Regex Patterns
```bash
--name '^test_.*\.py$'   # Python test files (anchored)
--name '.*\.(jpg|png)$'  # Image files
```

**Auto-detection:** Patterns with `/`, `^`, `$`, or regex chars are treated as regex. Others as glob.

### Combining Patterns (--name vs --name-and)
```bash
# OR logic: Match files containing 'backup' OR '2024'
--name '*backup*' --name '*2024*'

# AND logic: Match files containing 'backup' AND '2024'
--name-and '*backup*' --name-and '*2024*'

# Mixed logic: (report OR summary) AND 2024 AND .pdf
--name '*report*' --name '*summary*' --name-and '*2024*' --name-and '*.pdf'
```

### Path Filtering

**--omit-subdirs** - Pattern-based filtering (supports wildcards):
```bash
--omit-subdirs temp             # Skip any directory named "temp"
--omit-subdirs /home/bob        # Skip directories matching this pattern at a point
# past the value provided via --path.  See --omit-path for absolute path matching
--omit-subdirs '/home/*/backup' # Skip backup dirs in all home directories
```

**--omit-path** - Exact absolute path filtering (no wildcards):
```bash
--omit-path /home/joe/100k      # Skip this exact path only
--omit-path /data/archive       # Must start with / for filter to work
--omit-path /tmp/cache          # Can specify multiple paths
```

**Key differences:**
- `--omit-subdirs` uses pattern matching (wildcards like `*` and `?`)
- `--omit-path` requires exact absolute paths starting with `/`
- Both flags can be used multiple times
- Both increment the Smart Skip counter for progress tracking

## Advanced Examples

### Complex time range query
```bash
./grumpwalk.py --host cluster.example.com --path /data \
  --accessed-newer-than 30 --accessed-older-than 90 \
  --modified-older-than 180 \
  --larger-than 1GB --progress
```

### Find stale backups, exclude specific paths
```bash
./grumpwalk.py --host cluster.example.com --path /backups \
  --name '*backup*' --name-and '*2024*' \
  --older-than 365 --larger-than 100MB \
  --omit-path /backups/alice --omit-path /backups/bob \
  --csv-out stale-backups.csv
```

### Capacity report for specific owners
```bash
./grumpwalk.py --host cluster.example.com --path /projects \
  --owner joe --owner jane --expand-identity \
  --owner-report --csv-out capacity.csv
```

### Find all symlinks and their targets
```bash
./grumpwalk.py --host cluster.example.com --path /home \
  --type symlink --resolve-links --max-depth 2 \
  --csv-out symlinks.csv
```

### Generate ACL report with name resolution
```bash
./grumpwalk.py --host cluster.example.com --path /shared \
  --acl-report --acl-csv permissions.csv \
  --acl-resolve-names --show-owner --show-group --progress
```

### Find duplicate files (content-based sampling)
```bash
./grumpwalk.py --host cluster.example.com --path /data \
  --find-duplicates --progress --csv-out duplicates.csv
```

### Find duplicates by size only (fast, advisory)
```bash
./grumpwalk.py --host cluster.example.com --path /backups \
  --find-duplicates --by-size --larger-than 100MB --progress
```

## Performance Tips

1. **Use --max-depth** to limit search scope
2. **Enable --progress** to monitor large searches
3. **Use --omit-subdirs** to skip directories by pattern, or **--omit-path** for exact paths
4. **Combine filters** - More filters = fewer results to process
5. **Use --limit** for testing before full runs
6. **Smart skipping** automatically avoids directories that can't match your filters

## Output Formats

### Plain Text (default)
```
/home/joe/file1.txt
/home/jane/file2.log
```

### With --show-owner
```
/home/joe/file1.txt    joe (UID 1000)
/home/jane/file2.log   AD\jane
```

### With --resolve-links
```
/home/joe/link_to_docs → /shared/documentation
```

### CSV
```csv
path,modification_time,size
/home/joe/file1.txt,2024-01-15T10:30:00Z,1024
```

### JSON
```json
{"path":"/home/joe/file1.txt","modification_time":"2024-01-15T10:30:00Z"}
```

### ACL Reports
ACL reports export per-file permissions in CSV or JSON format:

**CSV format** (one row per file):
```csv
path,owner,group,ace_count,inherited_count,explicit_count,trustee_1,trustee_2
/shared/file.txt,AD\joe,AD\Domain Users,2,0,2,Allow::admin:rwx,Allow:g:users:rx
```

**JSON format** (one object per file):
```json
{"path":"/shared/file.txt","owner":"AD\\joe","group":"AD\\Domain Users","ace_count":2,"inherited_count":0,"explicit_count":2,"trustees":["Allow::admin:rwx","Allow:g:users:rx"]}
```

Use `--acl-resolve-names` to convert auth IDs to readable names (e.g., `joe` instead of `auth_id:1234`).
Use `--show-owner` and `--show-group` to include owner and group columns in the output.

## Architecture

- **Async I/O** - aiohttp for concurrent HTTP requests
- **Connection pooling** - Reuses connections for efficiency
- **Smart directory skipping** - Uses aggregates API to skip entire directory trees
- **Streaming output** - Results output as found (non-blocking)
- **Adaptive concurrency** - Automatically adjusts based on directory size
- **Identity caching** - Caches owner name resolutions

## License

MIT License

## Author

Joe Costa

## Contributing

Pull requests welcome at https://github.com/Joe-Costa/grumpwalk
