#!/usr/bin/env python3

"""
Qumulo File Filter - Async Python version with aiohttp

High-performance async implementation using direct REST API calls instead of qq CLI.
Applies lessons learned from benchmark_async_aiohttp.py for 6-7x performance improvement.

Usage:
    ./qumulo_file_filter_async.py --host <cluster> --path <path> [OPTIONS]

Key improvements over bash version:
- Direct REST API calls via aiohttp (no subprocess overhead)
- Concurrent HTTP requests with asyncio
- Connection pooling for efficiency
- Progress reporting in real-time
- Multi-node support for load distribution
"""

import argparse
import asyncio
import fnmatch
import json
import re
import ssl
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Set
from urllib.parse import quote, urlparse, parse_qs
from datetime import datetime, timedelta, timezone

# Add the Qumulo CLI directory to path to import auth modules
CLI_PATH = Path(__file__).parent / "cli" / "cli"
sys.path.insert(0, str(CLI_PATH))

try:
    from qumulo.lib.auth import get_credentials, credential_store_filename
except ImportError as e:
    print(f"[ERROR] Failed to import Qumulo API modules: {e}", file=sys.stderr)
    print(f"[ERROR] Make sure the CLI directory exists at: {CLI_PATH}", file=sys.stderr)
    sys.exit(1)

try:
    import aiohttp
except ImportError:
    print(
        "[ERROR] aiohttp not installed. Install with: pip install aiohttp",
        file=sys.stderr,
    )
    sys.exit(1)

# Try to use ujson for faster parsing
try:
    import ujson as json_parser

    JSON_PARSER_NAME = "ujson"
except ImportError:
    import json as json_parser

    JSON_PARSER_NAME = "json"

# Identity cache configuration
IDENTITY_CACHE_FILE = "file_filter_resolved_identities"
IDENTITY_CACHE_TTL = 15 * 60  # 15 minutes in seconds
import os


def load_identity_cache(verbose: bool = False) -> Dict:
    """Load identity cache from file, removing expired entries."""
    cache = {}
    cache_timestamp = int(time.time())

    try:
        if os.path.exists(IDENTITY_CACHE_FILE):
            with open(IDENTITY_CACHE_FILE, "r") as f:
                cache_data = json_parser.load(f)

                # Remove expired entries
                expired_count = 0
                for auth_id, entry in list(cache_data.items()):
                    if cache_timestamp - entry.get("timestamp", 0) > IDENTITY_CACHE_TTL:
                        expired_count += 1
                        del cache_data[auth_id]
                    else:
                        # Store full identity data, not just name
                        cache[auth_id] = entry.get("identity", {})

                # Write cleaned cache back to file if entries were expired
                if expired_count > 0:
                    save_identity_cache(cache, verbose=False)

            if verbose and cache:
                print(
                    f"[INFO] Loaded {len(cache)} cached identities from {IDENTITY_CACHE_FILE}",
                    file=sys.stderr,
                )
                if expired_count > 0:
                    print(
                        f"[INFO] Removed {expired_count} expired cache entries",
                        file=sys.stderr,
                    )
    except Exception as e:
        if verbose:
            print(f"[WARN] Failed to load identity cache: {e}", file=sys.stderr)

    return cache


def save_identity_cache(identity_cache: Dict, verbose: bool = False):
    """Save identity cache to file."""
    try:
        cache_data = {}
        cache_timestamp = int(time.time())

        for auth_id, identity in identity_cache.items():
            cache_data[auth_id] = {"identity": identity, "timestamp": cache_timestamp}

        with open(IDENTITY_CACHE_FILE, "w") as f:
            json_parser.dump(cache_data, f, indent=2)

        if verbose:
            print(
                f"[INFO] Saved {len(identity_cache)} identities to cache file",
                file=sys.stderr,
            )
    except Exception as e:
        if verbose:
            print(f"[WARN] Failed to save identity cache: {e}", file=sys.stderr)


class OwnerStats:
    """Track file ownership statistics for --owner-report."""

    def __init__(self, use_capacity: bool = False):
        """
        Initialize owner statistics tracker.

        Args:
            use_capacity: If True, use actual disk usage (datablocks + metablocks).
                         If False, use logical file size. Set to True to handle sparse files correctly.
        """
        self.owner_data = {}  # auth_id -> {'bytes': int, 'files': int, 'dirs': int}
        self.lock = asyncio.Lock()
        self.use_capacity = use_capacity

    async def add_file(self, owner_auth_id: str, size: int, is_dir: bool = False):
        """Add a file to the owner statistics."""
        async with self.lock:
            if owner_auth_id not in self.owner_data:
                self.owner_data[owner_auth_id] = {"bytes": 0, "files": 0, "dirs": 0}

            self.owner_data[owner_auth_id]["bytes"] += size
            if is_dir:
                self.owner_data[owner_auth_id]["dirs"] += 1
            else:
                self.owner_data[owner_auth_id]["files"] += 1

    def get_all_owners(self) -> List[str]:
        """Get list of all unique owner auth_ids."""
        return list(self.owner_data.keys())

    def get_stats(self, owner_auth_id: str) -> Dict:
        """Get statistics for a specific owner."""
        return self.owner_data.get(owner_auth_id, {"bytes": 0, "files": 0, "dirs": 0})


class ProgressTracker:
    """Track progress of async tree walking with real-time updates."""

    def __init__(self, verbose: bool = False, limit: Optional[int] = None):
        self.total_objects = 0
        self.total_dirs = 0
        self.matches = 0
        self.skipped_dirs = 0  # Count of directories skipped via smart skipping
        self.skipped_files = 0  # Count of files avoided by smart skipping
        self.skipped_subdirs = 0  # Count of subdirectories avoided by smart skipping
        self.start_time = time.time()
        self.verbose = verbose
        self.last_update = time.time()
        self.lock = asyncio.Lock()
        self.limit = limit
        self.limit_reached = False

    async def update(self, objects: int, dirs: int = 0, matches: int = 0):
        """Update progress counters and check if limit reached."""
        async with self.lock:
            self.total_objects += objects
            self.total_dirs += dirs
            self.matches += matches

            # Check if limit reached
            if self.limit and self.matches >= self.limit and not self.limit_reached:
                self.limit_reached = True
                if self.verbose:
                    print(
                        f"\r[INFO] Limit reached: {self.matches} matches (limit: {self.limit})",
                        file=sys.stderr,
                        flush=True,
                    )

            # Print progress every 0.5 seconds
            if self.verbose and time.time() - self.last_update > 0.5:
                elapsed = time.time() - self.start_time
                rate = self.total_objects / elapsed if elapsed > 0 else 0
                time_str = format_time(elapsed)
                print(
                    f"\r[PROGRESS] {self.total_objects:,} objects processed | "
                    f"{self.matches:,} matches | "
                    f"Smart Skip: {self.skipped_dirs:,} dirs ({self.skipped_files:,} files, {self.skipped_subdirs:,} subdirs) | "
                    f"{rate:.1f} obj/sec | "
                    f"Run time: {time_str}",
                    end="",
                    file=sys.stderr,
                    flush=True,
                )
                self.last_update = time.time()

    async def increment_skipped(self, files_skipped: int = 0, subdirs_skipped: int = 0):
        """
        Increment the skipped directory counter.

        Args:
            files_skipped: Number of files in the skipped directory
            subdirs_skipped: Number of subdirectories in the skipped directory
        """
        async with self.lock:
            self.skipped_dirs += 1
            self.skipped_files += files_skipped
            self.skipped_subdirs += subdirs_skipped

    def should_stop(self) -> bool:
        """Check if processing should stop due to limit."""
        return self.limit_reached

    def final_report(self):
        """Print final progress report."""
        if self.verbose:
            elapsed = time.time() - self.start_time
            rate = self.total_objects / elapsed if elapsed > 0 else 0
            time_str = format_time(elapsed)
            print(
                f"\r[PROGRESS] FINAL: {self.total_objects:,} objects processed | "
                f"{self.matches:,} matches | "
                f"Smart Skip: {self.skipped_dirs:,} dirs ({self.skipped_files:,} files, {self.skipped_subdirs:,} subdirs) | "
                f"{rate:.1f} obj/sec | "
                f"Run time: {time_str}",
                file=sys.stderr,
            )


class BatchedOutputHandler:
    """Handle batched output with identity resolution for --show-owner streaming."""

    def __init__(
        self,
        client: "AsyncQumuloClient",
        batch_size: int = 100,
        show_owner: bool = False,
        output_format: str = "text",
    ):
        self.client = client
        self.batch_size = batch_size
        self.show_owner = show_owner
        self.output_format = output_format  # 'text' or 'json'
        self.batch = []
        self.lock = asyncio.Lock()

    async def add_entry(self, entry: dict):
        """Add entry to batch and flush if batch is full."""
        async with self.lock:
            self.batch.append(entry)

            if len(self.batch) >= self.batch_size:
                await self._flush_batch()

    async def _flush_batch(self):
        """Resolve identities for current batch and output."""
        if not self.batch:
            return

        identity_cache = {}

        if self.show_owner:
            # Collect unique owner auth_ids from batch
            unique_owners = set()
            for entry in self.batch:
                owner_details = entry.get("owner_details", {})
                owner_auth_id = owner_details.get("auth_id") or entry.get("owner")
                if owner_auth_id:
                    unique_owners.add(owner_auth_id)

            # Resolve all owners in parallel
            if unique_owners:
                async with self.client.create_session() as session:
                    identity_cache = await self.client.resolve_multiple_identities(
                        session, list(unique_owners)
                    )

        # Output batch
        for entry in self.batch:
            if self.output_format == "json":
                print(json_parser.dumps(entry))
            else:
                # Plain text
                output_line = entry["path"]
                if self.show_owner:
                    owner_details = entry.get("owner_details", {})
                    owner_auth_id = owner_details.get("auth_id") or entry.get("owner")
                    if owner_auth_id and owner_auth_id in identity_cache:
                        identity = identity_cache[owner_auth_id]
                        owner_name = format_owner_name(identity)
                        output_line = f"{output_line}\t{owner_name}"
                    else:
                        output_line = f"{output_line}\tUnknown"
                print(output_line)
            sys.stdout.flush()

        # Clear batch
        self.batch = []

    async def flush(self):
        """Flush any remaining entries in batch."""
        async with self.lock:
            await self._flush_batch()


class Profiler:
    """Track detailed performance metrics for profiling."""

    def __init__(self):
        self.timings = {}  # operation -> total time
        self.counts = {}  # operation -> call count
        self.lock = asyncio.Lock()

    async def record(self, operation: str, duration: float):
        """Record timing for an operation."""
        async with self.lock:
            if operation not in self.timings:
                self.timings[operation] = 0.0
                self.counts[operation] = 0
            self.timings[operation] += duration
            self.counts[operation] += 1

    def record_sync(self, operation: str, duration: float):
        """Record timing for an operation (synchronous version)."""
        if operation not in self.timings:
            self.timings[operation] = 0.0
            self.counts[operation] = 0
        self.timings[operation] += duration
        self.counts[operation] += 1

    def print_report(self, total_elapsed: float):
        """Print profiling report."""
        print("\n" + "=" * 80, file=sys.stderr)
        print("PROFILING REPORT", file=sys.stderr)
        print("=" * 80, file=sys.stderr)

        # Calculate total accounted time
        total_accounted = sum(self.timings.values())

        # Sort by total time descending
        sorted_ops = sorted(self.timings.items(), key=lambda x: x[1], reverse=True)

        print(
            f"\n{'Operation':<30} {'Total Time':>12} {'Calls':>10} {'Avg Time':>12} {'% Total':>8}",
            file=sys.stderr,
        )
        print("-" * 80, file=sys.stderr)

        for operation, total_time in sorted_ops:
            count = self.counts[operation]
            avg_time = total_time / count if count > 0 else 0
            pct_total = (total_time / total_elapsed * 100) if total_elapsed > 0 else 0

            print(
                f"{operation:<30} {total_time:>11.3f}s {count:>10,} {avg_time*1000:>11.2f}ms {pct_total:>7.1f}%",
                file=sys.stderr,
            )

        print("-" * 80, file=sys.stderr)
        print(f"{'Total Accounted':<30} {total_accounted:>11.3f}s", file=sys.stderr)
        print(f"{'Total Elapsed':<30} {total_elapsed:>11.3f}s", file=sys.stderr)

        unaccounted = total_elapsed - total_accounted
        if unaccounted > 0.01:
            pct_unaccounted = (
                (unaccounted / total_elapsed * 100) if total_elapsed > 0 else 0
            )
            print(
                f"{'Unaccounted (overhead)':<30} {unaccounted:>11.3f}s {pct_unaccounted:>7.1f}%",
                file=sys.stderr,
            )

        # Identify bottlenecks
        print(f"\nTop 3 Bottlenecks:", file=sys.stderr)
        for i, (operation, total_time) in enumerate(sorted_ops[:3]):
            pct = (total_time / total_elapsed * 100) if total_elapsed > 0 else 0
            print(f"  {i+1}. {operation}: {pct:.1f}% of total time", file=sys.stderr)

        print("=" * 80, file=sys.stderr)


def extract_pagination_token(api_response: dict) -> Optional[str]:
    """Extract the pagination token from Qumulo API response."""
    if "paging" not in api_response:
        return None

    next_url = api_response["paging"].get("next")
    if not next_url:
        return None

    try:
        parsed = urlparse(next_url)
        query_params = parse_qs(parsed.query)

        if "after" in query_params:
            return query_params["after"][0]
        else:
            return None

    except Exception:
        return None


class AsyncQumuloClient:
    """Async Qumulo API client using aiohttp with optimized connection pooling."""

    def __init__(
        self,
        host: str,
        port: int,
        bearer_token: str,
        max_concurrent: int = 100,
        connector_limit: int = 100,
        identity_cache: Optional[Dict] = None,
        verbose: bool = False,
    ):
        self.host = host
        self.port = port
        self.base_url = f"https://{host}:{port}"
        self.bearer_token = bearer_token
        self.max_concurrent = max_concurrent
        self.verbose = verbose

        # Create SSL context that doesn't verify certificates (for self-signed certs)
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

        # Configure connection pooling
        self.connector_limit = connector_limit

        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {bearer_token}",
        }

        # Semaphore to limit concurrent operations
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Persistent identity cache for performance
        self.persistent_identity_cache = (
            identity_cache if identity_cache is not None else {}
        )
        self.cache_hits = 0
        self.cache_misses = 0

    def create_session(self) -> aiohttp.ClientSession:
        """Create optimized ClientSession with connection pooling."""
        connector = aiohttp.TCPConnector(
            limit=self.connector_limit,
            limit_per_host=self.connector_limit,
            ttl_dns_cache=300,
            ssl=self.ssl_context,
        )
        return aiohttp.ClientSession(connector=connector, headers=self.headers)

    async def get_directory_page(
        self,
        session: aiohttp.ClientSession,
        path: str,
        limit: int = 1000,
        after_token: Optional[str] = None,
    ) -> dict:
        """
        Fetch a single page of directory contents from Qumulo API.

        Args:
            session: aiohttp ClientSession
            path: Directory path (must start with '/')
            limit: Maximum entries per page
            after_token: Pagination token from previous response

        Returns:
            Dictionary containing 'files' and 'paging' metadata
        """
        async with self.semaphore:
            if not path.startswith("/"):
                path = "/" + path

            encoded_path = quote(path, safe="")
            url = f"{self.base_url}/v1/files/{encoded_path}/entries/"

            params = {"limit": limit}
            if after_token:
                params["after"] = after_token

            async with session.get(
                url, params=params, ssl=self.ssl_context
            ) as response:
                response.raise_for_status()
                return await response.json()

    async def enumerate_directory(
        self,
        session: aiohttp.ClientSession,
        path: str,
        max_entries: Optional[int] = None,
    ) -> List[dict]:
        """
        Enumerate all entries in a directory, following pagination.

        Args:
            session: aiohttp ClientSession
            path: Directory path
            max_entries: Optional limit on total entries to fetch

        Returns:
            List of all file/directory entries
        """
        all_entries = []
        after_token = None

        while True:
            response = await self.get_directory_page(
                session, path, limit=1000, after_token=after_token
            )

            files = response.get("files", [])
            all_entries.extend(files)

            # Check if we've reached the max entries limit
            if max_entries and len(all_entries) >= max_entries:
                all_entries = all_entries[:max_entries]
                break

            # Get next token
            after_token = extract_pagination_token(response)
            if not after_token:
                break

        return all_entries

    async def enumerate_directory_streaming(
        self, session: aiohttp.ClientSession, path: str, callback
    ) -> int:
        """
        Stream directory entries without accumulating in memory.
        Calls callback with each page of results for processing.

        This is more memory-efficient for large directories (50k+ entries)
        as it processes entries page-by-page instead of accumulating them all.

        Args:
            session: aiohttp ClientSession
            path: Directory path
            callback: Async function that receives list of entries per page
                     Returns: (matching_entries, subdirs) tuple

        Returns:
            Total number of entries processed
        """
        total_entries = 0
        after_token = None

        while True:
            response = await self.get_directory_page(
                session, path, limit=1000, after_token=after_token
            )

            files = response.get("files", [])
            total_entries += len(files)

            # Process this page immediately via callback
            if callback and files:
                await callback(files)

            # Get next page token
            after_token = extract_pagination_token(response)
            if not after_token:
                break

        return total_entries

    async def get_directory_aggregates(
        self, session: aiohttp.ClientSession, path: str
    ) -> dict:
        """
        Get directory aggregate statistics.

        Returns statistics for immediate children only (non-recursive).
        All count fields are returned as strings - use int() to convert.

        Args:
            session: aiohttp ClientSession
            path: Directory path

        Returns:
            Dictionary with aggregates data including total_files, total_directories, etc.
            Falls back to safe defaults if API call fails.
        """
        async with self.semaphore:
            if not path.startswith("/"):
                path = "/" + path

            encoded_path = quote(path, safe="")
            url = f"{self.base_url}/v1/files/{encoded_path}/aggregates/"

            try:
                async with session.get(url, ssl=self.ssl_context) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                # Fall back gracefully if aggregates unavailable
                return {"total_files": "0", "total_directories": "0", "error": str(e)}

    async def get_directory_capacity(
        self, session: aiohttp.ClientSession, path: str
    ) -> dict:
        """
        Get directory capacity breakdown by owner.

        PHASE 3.3: Used for owner filter smart skipping to check if target owner
        has any files in the directory before enumeration.

        Args:
            session: aiohttp ClientSession
            path: Directory path

        Returns:
            Dictionary with capacity_by_owner data, or empty dict on error.
            Example: {"capacity_by_owner": [{"id": "500", "capacity_usage": 1073741824}]}
        """
        async with self.semaphore:
            if not path.startswith("/"):
                path = "/" + path

            encoded_path = quote(path, safe="")
            url = f"{self.base_url}/v1/files/data/capacity/"

            params = {"path": path, "by_owner": "true"}

            try:
                async with session.get(
                    url, params=params, ssl=self.ssl_context
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        # API may not be available or path may not exist
                        return {}
            except aiohttp.ClientError:
                # Fall back gracefully if capacity API unavailable
                return {}

    async def read_symlink(self, session: aiohttp.ClientSession, path: str) -> Optional[str]:
        """
        Read the target of a symlink.

        Args:
            session: aiohttp ClientSession
            path: Path to the symlink

        Returns:
            The target path that the symlink points to, or None if read fails
        """
        async with self.semaphore:
            if not path.startswith('/'):
                path = '/' + path

            encoded_path = quote(path, safe='')
            url = f"{self.base_url}/v1/files/{encoded_path}/data"

            try:
                async with session.get(url, ssl=self.ssl_context) as response:
                    if response.status == 200:
                        # Read the symlink target (returns as plain text)
                        target = await response.text()
                        return target.strip() if target else None
                    else:
                        return None
            except aiohttp.ClientError:
                return None

    async def read_file_chunk(
        self,
        session: aiohttp.ClientSession,
        path: str,
        offset: int,
        length: int
    ) -> Optional[bytes]:
        """
        Read a chunk of a file at a specific offset.

        Args:
            session: aiohttp ClientSession
            path: Path to the file
            offset: Byte offset to start reading from
            length: Number of bytes to read

        Returns:
            Bytes read from the file, or None if read fails
        """
        async with self.semaphore:
            if not path.startswith('/'):
                path = '/' + path

            encoded_path = quote(path, safe='')
            url = f"{self.base_url}/v1/files/{encoded_path}/data?offset={offset}&length={length}"

            try:
                async with session.get(url, ssl=self.ssl_context) as response:
                    if response.status == 200:
                        data = await response.read()
                        return data
                    else:
                        if self.verbose:
                            print(f"[WARN] Failed to read chunk from {path} at offset {offset}: HTTP {response.status}", file=sys.stderr)
                        return None
            except aiohttp.ClientError as e:
                if self.verbose:
                    print(f"[WARN] Error reading chunk from {path}: {e}", file=sys.stderr)
                return None

    def calculate_adaptive_concurrency(self, total_entries: int) -> int:
        """
        Calculate adaptive concurrency based on directory size.

        PHASE 3.2: Adaptive concurrency reduces concurrent operations for very large
        directories to prevent overwhelming the cluster and consuming excessive memory.

        Thresholds:
        - < 10k entries: Use full concurrency
        - 10k-50k entries: Reduce to 50% of base
        - 50k-100k entries: Reduce to 25% of base
        - > 100k entries: Reduce to 10% of base (min 5)

        Args:
            total_entries: Total entries in directory (files + directories)

        Returns:
            Adjusted concurrency level
        """
        if total_entries < 10000:
            return self.max_concurrent
        elif total_entries < 50000:
            return max(5, self.max_concurrent // 2)
        elif total_entries < 100000:
            return max(5, self.max_concurrent // 4)
        else:
            return max(5, self.max_concurrent // 10)

    async def enumerate_directory_adaptive(
        self,
        session: aiohttp.ClientSession,
        path: str,
        aggregates: dict,
        file_filter=None,
        owner_stats: Optional[OwnerStats] = None,
        collect_results: bool = True,
        verbose: bool = False,
        progress: Optional["ProgressTracker"] = None,
    ) -> tuple:
        """
        Automatically choose between batch mode and streaming mode based on directory size.

        PHASE 3.2: Progressive streaming automatically switches to streaming mode for
        directories with 50k+ entries when collecting results to reduce memory usage.

        Decision logic:
        - < 50k entries: Use batch mode (existing behavior)
        - >= 50k entries AND collect_results=True: Use streaming mode
        - >= 50k entries AND collect_results=False: Use batch mode (already memory-efficient)

        Args:
            session: aiohttp ClientSession
            path: Directory path
            aggregates: Pre-fetched aggregates data with total_files/total_directories
            file_filter: Optional filter function
            owner_stats: Optional OwnerStats for collecting ownership data
            collect_results: If False, don't accumulate matching entries
            verbose: If True, log mode selection

        Returns:
            Tuple of (matching_entries, subdirs, match_count, total_entries_processed)
        """
        # Parse aggregates to determine directory size
        try:
            total_files = int(aggregates.get("total_files", 0))
            total_dirs = int(aggregates.get("total_directories", 0))
            total_entries = total_files + total_dirs
        except (ValueError, TypeError):
            total_entries = 0

        # Decide enumeration strategy
        use_streaming = total_entries >= 50000 and collect_results

        if verbose and use_streaming:
            print(
                f"\r[INFO] Progressive streaming: Using streaming mode for {path} ({total_entries:,} entries)",
                file=sys.stderr,
            )

        matching_entries = []
        subdirs = []
        match_count = 0
        total_processed = 0  # Track total entries processed (all files + dirs)

        if use_streaming:
            # Use streaming mode - process pages as they arrive
            async def process_page(page_entries):
                nonlocal match_count, total_processed
                page_size = len(page_entries)
                page_matches = 0

                total_processed += page_size
                for entry in page_entries:
                    # Collect owner statistics if enabled
                    if owner_stats:
                        owner_details = entry.get("owner_details", {})
                        owner_auth_id = owner_details.get("auth_id") or entry.get(
                            "owner"
                        )
                        if owner_auth_id:
                            try:
                                if owner_stats.use_capacity:
                                    datablocks = int(entry.get("datablocks", 0))
                                    metablocks = int(entry.get("metablocks", 0))
                                    file_size = (datablocks + metablocks) * 4096
                                else:
                                    file_size = int(entry.get("size", 0))
                            except (ValueError, TypeError):
                                file_size = 0
                            is_dir = entry.get("type") == "FS_FILE_TYPE_DIRECTORY"
                            await owner_stats.add_file(owner_auth_id, file_size, is_dir)

                    # Track subdirectories
                    if entry.get("type") == "FS_FILE_TYPE_DIRECTORY":
                        subdirs.append(entry["path"])

                    # Apply filter and collect results
                    if file_filter:
                        if file_filter(entry):
                            match_count += 1
                            page_matches += 1
                            if collect_results:
                                matching_entries.append(entry)
                    else:
                        match_count += 1
                        page_matches += 1
                        if collect_results:
                            matching_entries.append(entry)

                # Update progress after each page in streaming mode
                if progress:
                    await progress.update(page_size, 0, page_matches)

            # Stream directory entries
            await self.enumerate_directory_streaming(session, path, process_page)
        else:
            # Use batch mode - existing behavior
            entries = await self.enumerate_directory(session, path)
            total_processed = len(entries)

            for entry in entries:
                # Collect owner statistics if enabled
                if owner_stats:
                    owner_details = entry.get("owner_details", {})
                    owner_auth_id = owner_details.get("auth_id") or entry.get("owner")
                    if owner_auth_id:
                        try:
                            if owner_stats.use_capacity:
                                datablocks = int(entry.get("datablocks", 0))
                                metablocks = int(entry.get("metablocks", 0))
                                file_size = (datablocks + metablocks) * 4096
                            else:
                                file_size = int(entry.get("size", 0))
                        except (ValueError, TypeError):
                            file_size = 0
                        is_dir = entry.get("type") == "FS_FILE_TYPE_DIRECTORY"
                        await owner_stats.add_file(owner_auth_id, file_size, is_dir)

                # Track subdirectories
                if entry.get("type") == "FS_FILE_TYPE_DIRECTORY":
                    subdirs.append(entry["path"])

                # Apply filter and collect results
                if file_filter:
                    if file_filter(entry):
                        match_count += 1
                        if collect_results:
                            matching_entries.append(entry)
                else:
                    match_count += 1
                    if collect_results:
                        matching_entries.append(entry)

        return (matching_entries, subdirs, match_count, total_processed)

    async def walk_tree_async(
        self,
        session: aiohttp.ClientSession,
        path: str,
        max_depth: Optional[int] = None,
        _current_depth: int = 0,
        progress: Optional[ProgressTracker] = None,
        file_filter=None,
        owner_stats: Optional[OwnerStats] = None,
        omit_subdirs: Optional[List[str]] = None,
        collect_results: bool = True,
        verbose: bool = False,
        max_entries_per_dir: Optional[int] = None,
        time_filter_info: Optional[Dict] = None,
        size_filter_info: Optional[Dict] = None,
        owner_filter_info: Optional[Dict] = None,
        output_callback=None,
    ) -> List[dict]:
        """
        Recursively walk directory tree with concurrent directory enumeration.

        Args:
            session: aiohttp ClientSession
            path: Directory path to walk
            max_depth: Maximum depth to traverse (-1 or None for unlimited)
            _current_depth: Internal tracking of current depth
            progress: Optional ProgressTracker for reporting progress
            file_filter: Optional function to filter files
            owner_stats: Optional OwnerStats for collecting ownership data
            omit_subdirs: Optional list of wildcard patterns for directories to skip
            collect_results: If False, don't accumulate matching entries (saves memory for reports)
            verbose: If True, emit warnings to stderr for large directories
            max_entries_per_dir: If set, skip directories with more entries than this limit
            time_filter_info: Optional dict with time filter thresholds for smart skipping
            size_filter_info: Optional dict with size filter thresholds for smart skipping
            owner_filter_info: Optional dict with owner filter auth_ids for smart skipping

        Returns:
            List of matching file entries (empty if collect_results=False)
        """
        # Check depth limit
        if max_depth is not None and max_depth >= 0 and _current_depth >= max_depth:
            return []

        # Early exit: Check if limit reached
        if progress and progress.should_stop():
            return []

        # Get directory aggregates for pre-flight intelligence
        aggregates = await self.get_directory_aggregates(session, path)

        # Check for errors in aggregates response (API may not be available)
        has_aggregates_error = "error" in aggregates

        if not has_aggregates_error:
            # Parse aggregate statistics (API returns strings, convert to ints)
            try:
                total_files = int(aggregates.get("total_files", 0))
                total_directories = int(aggregates.get("total_directories", 0))
                total_entries = total_files + total_directories

                # Warn on large directories (100k+ entries)
                if verbose and total_entries > 100_000:
                    print(
                        f"\r[WARN] Large directory: {path} ({total_entries:,} entries: "
                        f"{total_files:,} files, {total_directories:,} dirs)",
                        file=sys.stderr,
                    )

                # Safety valve: skip directories exceeding max_entries_per_dir
                if max_entries_per_dir and total_entries > max_entries_per_dir:
                    # if verbose:
                    #     print(f"\r[SKIP] Directory exceeds limit: {path} "
                    #           f"({total_entries:,} entries > {max_entries_per_dir:,} limit)",
                    #           file=sys.stderr)
                    pass
                    if progress:
                        await progress.increment_skipped(total_files, total_directories)
                    return []

                # PHASE 2: Smart skipping - skip directories that can't possibly match filters
                # This saves API calls by not enumerating directories we know won't have matches
                if file_filter:
                    # Check if file_only filter is active (file_filter rejects all directories)
                    # We detect this by checking if args has file_only attribute
                    # Since we don't have access to args here, we check by testing the filter
                    # with a mock directory entry
                    test_dir = {
                        "type": "FS_FILE_TYPE_DIRECTORY",
                        "path": "/test",
                        "name": "test",
                    }
                    test_file = {
                        "type": "FS_FILE_TYPE_FILE",
                        "path": "/test",
                        "name": "test",
                    }

                    # If filter rejects directories but might accept files, check file count
                    if not file_filter(test_dir) and file_filter(test_file):
                        # This looks like --file-only filter
                        if total_files == 0:
                            # if verbose:
                            #     print(f"\r[SKIP] Smart skip: {path} (0 files, --file-only active)",
                            #           file=sys.stderr)
                            pass
                            if progress:
                                await progress.increment_skipped(
                                    total_files, total_directories
                                )
                            return []

                # PHASE 3: Enhanced smart skipping for time and size filters
                # Use aggregates data to skip directories that cannot possibly contain matching files

                # Time-based smart skipping
                if time_filter_info:
                    oldest_mod = aggregates.get("oldest_modification_time")
                    newest_mod = aggregates.get("newest_modification_time")

                    # Parse time filter info
                    older_than_threshold = time_filter_info.get("older_than")
                    newer_than_threshold = time_filter_info.get("newer_than")
                    time_field = time_filter_info.get("time_field", "modification_time")

                    # Only apply smart skipping for modification_time (since aggregates provides mod times)
                    if time_field == "modification_time" and (
                        oldest_mod and newest_mod
                    ):
                        try:
                            # Parse aggregates times
                            oldest_time = datetime.fromisoformat(
                                oldest_mod.rstrip("Z").split(".")[0]
                            )
                            newest_time = datetime.fromisoformat(
                                newest_mod.rstrip("Z").split(".")[0]
                            )

                            # Check --older-than filter
                            if older_than_threshold:
                                # If the NEWEST file is younger than threshold, NO files match
                                if newest_time >= older_than_threshold:
                                    # if verbose:
                                    #     print(f"\r[SKIP] Smart skip: {path} (all files newer than threshold)",
                                    #           file=sys.stderr)
                                    if progress:
                                        await progress.increment_skipped(
                                            total_files, total_directories
                                        )
                                    return []

                            # Check --newer-than filter
                            if newer_than_threshold:
                                # If the OLDEST file is older than threshold, NO files match
                                if oldest_time <= newer_than_threshold:
                                    # if verbose:
                                    #     print(f"\r[SKIP] Smart skip: {path} (all files older than threshold)",
                                    #           file=sys.stderr)
                                    if progress:
                                        await progress.increment_skipped(
                                            total_files, total_directories
                                        )
                                    return []

                        except (ValueError, AttributeError):
                            # If time parsing fails, continue without smart skipping
                            pass

                # Size-based smart skipping
                if size_filter_info:
                    total_capacity = aggregates.get("total_capacity")
                    min_size = size_filter_info.get("min_size")

                    # Check --larger-than filter (min_size)
                    if min_size and total_capacity:
                        try:
                            total_cap_bytes = int(total_capacity)
                            # If total directory capacity is less than min_size,
                            # NO individual files can be >= min_size
                            if total_cap_bytes < min_size:
                                # if verbose:
                                #     print(f"\r[SKIP] Smart skip: {path} "
                                #           f"(total capacity {total_cap_bytes} < min {min_size})",
                                #           file=sys.stderr)
                                if progress:
                                    await progress.increment_skipped(
                                        total_files, total_directories
                                    )
                                return []
                        except (ValueError, TypeError):
                            # If capacity parsing fails, continue without smart skipping
                            pass

                # PHASE 3.3: Owner-based smart skipping
                # Use capacity API to check if target owner has any files in this directory
                if owner_filter_info:
                    owner_auth_ids = owner_filter_info.get("auth_ids")
                    if owner_auth_ids:
                        # Get capacity breakdown by owner
                        capacity_data = await self.get_directory_capacity(session, path)
                        if capacity_data and "capacity_by_owner" in capacity_data:
                            # Extract owner IDs from capacity data
                            owners_with_files = set()
                            for entry in capacity_data.get("capacity_by_owner", []):
                                owner_id = entry.get("id")
                                if owner_id:
                                    owners_with_files.add(owner_id)

                            # Check if any of our target owners have files here
                            has_matching_owner = any(
                                auth_id in owners_with_files
                                for auth_id in owner_auth_ids
                            )

                            if not has_matching_owner:
                                # if verbose:
                                #     print(f"\r[SKIP] Smart skip: {path} (no files owned by target owner(s))",
                                #           file=sys.stderr)
                                if progress:
                                    await progress.increment_skipped(
                                        total_files, total_directories
                                    )
                                return []

            except (ValueError, TypeError):
                # If we can't parse aggregates, continue without the check
                pass

        # PHASE 3.2: Use adaptive enumeration (automatically chooses streaming vs batch mode)
        # Pass progress tracker for per-page updates in streaming mode
        matching_entries, subdirs, match_count, total_processed = (
            await self.enumerate_directory_adaptive(
                session,
                path,
                aggregates,
                file_filter,
                owner_stats,
                collect_results,
                verbose,
                progress,
            )
        )

        # Filter subdirectories based on omit patterns
        if omit_subdirs:
            filtered_subdirs = []
            for subdir_path in subdirs:
                # Extract directory name (last component, handling trailing slashes)
                subdir_name = (
                    subdir_path.rstrip("/").split("/")[-1]
                    if "/" in subdir_path
                    else subdir_path
                )

                # Check if this directory should be omitted
                should_omit = False
                matched_pattern = None
                for pattern in omit_subdirs:
                    if fnmatch.fnmatch(subdir_name, pattern):
                        should_omit = True
                        matched_pattern = pattern
                        break

                if should_omit:
                    # if verbose:
                    #     print(f"\r[SKIP] Omitting subdirectory: {subdir_path} (matched pattern: {matched_pattern})",
                    #           file=sys.stderr)
                    pass
                else:
                    filtered_subdirs.append(subdir_path)

            subdirs = filtered_subdirs

        # Output matches immediately if callback provided
        if output_callback and matching_entries:
            for entry in matching_entries:
                await output_callback(entry)

        # Update progress tracker for batch mode
        # In streaming mode, progress is already updated per-page inside enumerate_directory_adaptive
        # In batch mode, we update once with the total for this directory
        # We determine the mode by checking if total_entries >= 50000 (streaming threshold)
        try:
            total_files = int(aggregates.get("total_files", 0))
            total_dirs = int(aggregates.get("total_directories", 0))
            total_entries = total_files + total_dirs
            used_streaming = total_entries >= 50000 and collect_results
        except (ValueError, TypeError):
            used_streaming = False

        if progress and not used_streaming:
            # Batch mode: update progress once for this directory
            # Count ACTUAL entries processed (not recursive aggregates)
            await progress.update(total_processed, 1, match_count)

        # Recursively process subdirectories concurrently
        if subdirs and (
            max_depth is None or max_depth < 0 or _current_depth + 1 < max_depth
        ):
            # PHASE 3.2: Adaptive concurrency - process subdirectories in batches
            # Calculate batch size based on number of subdirectories
            num_subdirs = len(subdirs)
            batch_size = self.calculate_adaptive_concurrency(num_subdirs)

            if verbose and batch_size < self.max_concurrent and num_subdirs > 0:
                print(
                    f"\r[INFO] Adaptive concurrency: Processing {num_subdirs} subdirs with batch size {batch_size} "
                    f"(reduced from {self.max_concurrent})",
                    file=sys.stderr,
                )

            # Process subdirectories in batches
            all_results = []

            # Track progress for large subdirectory sets
            show_batch_progress = progress and num_subdirs > 10000
            last_progress_time = time.time()
            batch_progress_interval = 10.0  # Show progress every 10 seconds

            for i in range(0, len(subdirs), batch_size):
                # Early exit: Check if limit reached before processing next batch
                if progress and progress.should_stop():
                    if verbose:
                        print(
                            f"\r[INFO] Early exit: Limit reached, skipping remaining {len(subdirs) - i} subdirectories",
                            file=sys.stderr,
                        )
                    break

                batch = subdirs[i : i + batch_size]
                tasks = [
                    self.walk_tree_async(
                        session,
                        subdir,
                        max_depth,
                        _current_depth + 1,
                        progress,
                        file_filter,
                        owner_stats,
                        omit_subdirs,
                        collect_results,
                        verbose,
                        max_entries_per_dir,
                        time_filter_info,
                        size_filter_info,
                        owner_filter_info,
                        output_callback,
                    )
                    for subdir in batch
                ]

                # Process this batch concurrently
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                all_results.extend(batch_results)

                # Show batch progress for large directories
                if show_batch_progress:
                    current_time = time.time()
                    elapsed_since_last = current_time - last_progress_time
                    batch_num = (i // batch_size) + 1
                    total_batches = (num_subdirs + batch_size - 1) // batch_size

                    # Show progress every 100 batches or every 10 seconds
                    if (
                        batch_num % 100 == 0
                        or elapsed_since_last >= batch_progress_interval
                        or batch_num == total_batches
                    ):
                        percent = (i + batch_size) / num_subdirs * 100
                        subdirs_processed = min(i + batch_size, num_subdirs)

                        # Shorten path for display
                        display_path = path if len(path) <= 50 else "..." + path[-47:]

                        elapsed_total = current_time - progress.start_time
                        time_str = format_time(elapsed_total)

                        print(
                            f"\r[PROGRESS] Scanning subdirs in {display_path}: "
                            f"{subdirs_processed:,}/{num_subdirs:,} ({percent:.1f}%) | "
                            f"Run time: {time_str}",
                            end="",
                            file=sys.stderr,
                        )

                        last_progress_time = current_time

            # Clear the batch progress line if we showed it
            if show_batch_progress:
                print(file=sys.stderr)  # Newline to finish the progress line

            # Collect results from subdirectories (only if collect_results=True)
            if collect_results:
                # Optimize: At the top level, use efficient concatenation for large result sets
                # At deeper levels, use simple extend to avoid memory overhead
                if _current_depth == 0 and len(all_results) > 1000:
                    if verbose:
                        print(
                            f"\r[INFO] Collecting results from {len(all_results)} subdirectory batches...",
                            file=sys.stderr,
                        )

                    # Use itertools.chain for efficient concatenation
                    import itertools

                    result_lists = [
                        result for result in all_results if isinstance(result, list)
                    ]
                    if result_lists:
                        matching_entries.extend(
                            itertools.chain.from_iterable(result_lists)
                        )

                    if verbose:
                        print(
                            f"\r[INFO] Collection complete, {len(matching_entries)} total matches          ",
                            file=sys.stderr,
                        )
                else:
                    # For smaller result sets or nested levels, simple extend is fine
                    for result in all_results:
                        if isinstance(result, list):
                            matching_entries.extend(result)

        return matching_entries

    async def resolve_identity(
        self, session: aiohttp.ClientSession, identifier: str, id_type: str = "auth_id"
    ) -> Dict:
        """
        Resolve an identity using various identifier types.

        Args:
            session: aiohttp ClientSession
            identifier: The identifier value (auth_id, SID, UID, GID, or name)
            id_type: Type of identifier - "auth_id", "sid", "uid", "gid", or "name"

        Returns:
            Dictionary containing complete identity information
        """
        url = f"{self.base_url}/v1/identity/find"

        # Build the appropriate payload based on identifier type
        if id_type == "auth_id":
            payload = {"auth_id": str(identifier)}
        elif id_type == "sid":
            payload = {"sid": str(identifier)}
        elif id_type == "uid":
            # UID must be an integer
            try:
                payload = {"uid": int(identifier)}
            except (ValueError, TypeError):
                raise ValueError(f"UID must be a valid integer: {identifier}")
        elif id_type == "gid":
            # GID must be an integer
            try:
                payload = {"gid": int(identifier)}
            except (ValueError, TypeError):
                raise ValueError(f"GID must be a valid integer: {identifier}")
        elif id_type == "name":
            payload = {"name": str(identifier)}
        else:
            raise ValueError(
                f"Unknown id_type: {id_type}. Must be auth_id, sid, uid, gid, or name"
            )

        try:
            async with session.post(
                url, json=payload, ssl=self.ssl_context
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    result["resolved"] = True
                    return result
                elif response.status == 404:
                    # Identity not found - return fallback
                    return {
                        "domain": "UNKNOWN",
                        id_type: identifier,
                        "name": f"Unknown ({id_type}: {identifier})",
                        "resolved": False,
                    }
                else:
                    response.raise_for_status()
        except Exception as e:
            # Return fallback on error
            return {
                "domain": "ERROR",
                id_type: identifier,
                "name": f"Error resolving {id_type}: {identifier}",
                "error": str(e),
                "resolved": False,
            }

    async def resolve_multiple_identities(
        self,
        session: aiohttp.ClientSession,
        auth_ids: List[str],
        show_progress: bool = False,
    ) -> Dict[str, Dict]:
        """
        Resolve multiple identities in parallel, using identity expansion to find
        the best name for POSIX UIDs that are linked to AD users.

        Args:
            session: aiohttp ClientSession
            auth_ids: List of auth_id values to resolve
            show_progress: If True, display progress updates during resolution

        Returns:
            Dictionary mapping auth_id to resolved identity info
        """
        # Remove duplicates
        unique_ids = list(set(auth_ids))

        if not unique_ids:
            return {}

        # Track how many are already cached
        cached_count = sum(
            1 for auth_id in unique_ids if auth_id in self.persistent_identity_cache
        )
        to_resolve_count = len(unique_ids) - cached_count

        if show_progress:
            print(
                f"[INFO] Resolving {len(unique_ids)} unique owner identities ({cached_count} cached, {to_resolve_count} to fetch)...",
                file=sys.stderr,
            )

        start_time = time.time()

        # Create tasks for parallel resolution with expansion
        tasks = [
            self._resolve_identity_with_expansion(session, auth_id)
            for auth_id in unique_ids
        ]

        # Execute all resolutions in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        if show_progress:
            elapsed = time.time() - start_time
            avg_time = elapsed / len(unique_ids) if len(unique_ids) > 0 else 0
            print(
                f"[INFO] Identity resolution completed in {elapsed:.1f}s ({avg_time*1000:.1f}ms per identity)",
                file=sys.stderr,
            )
            print(
                f"[INFO] Cache stats - Hits: {self.cache_hits}, Misses: {self.cache_misses}, Hit rate: {self.cache_hits/(self.cache_hits+self.cache_misses)*100:.1f}%",
                file=sys.stderr,
            )

        # Build cache mapping auth_id to result
        identity_cache = {}
        for auth_id, result in zip(unique_ids, results):
            if isinstance(result, Exception):
                identity_cache[auth_id] = {
                    "domain": "ERROR",
                    "auth_id": auth_id,
                    "name": f"Error: {auth_id}",
                    "error": str(result),
                    "resolved": False,
                }
            else:
                identity_cache[auth_id] = result

        return identity_cache

    async def _resolve_identity_with_expansion(
        self, session: aiohttp.ClientSession, auth_id: str
    ) -> Dict:
        """
        Resolve an identity using expansion to find the best displayable name.

        This handles cases where a POSIX UID (like 2005) is linked to an AD user
        (like "mark") through POSIX extensions. We want to show the AD name.

        Checks persistent cache first to avoid redundant API calls.

        Args:
            session: aiohttp ClientSession
            auth_id: The auth_id to resolve

        Returns:
            Dictionary containing identity info with the best available name
        """
        # Check persistent cache first
        if auth_id in self.persistent_identity_cache:
            self.cache_hits += 1
            return self.persistent_identity_cache[auth_id]

        self.cache_misses += 1

        url = f"{self.base_url}/v1/identity/expand"

        # Build identity dict for the expand API
        # The expand API expects: {"id": {"auth_id": "12884903893"}}
        payload = {"id": {"auth_id": str(auth_id)}}

        try:
            async with session.post(
                url, json=payload, ssl=self.ssl_context
            ) as response:
                if response.status == 200:
                    expand_result = await response.json()

                    # Extract the primary identity (the one we queried for)
                    primary_identity = expand_result.get("id", {})

                    # Check if we got a name from the primary identity
                    best_identity = primary_identity.copy()
                    best_identity["auth_id"] = auth_id
                    best_identity["resolved"] = True

                    # If the primary identity doesn't have a name, check equivalent identities
                    if not primary_identity.get("name"):
                        # Look through equivalent identities to find one with a name
                        # Prefer AD identities over POSIX identities
                        equivalent_ids = expand_result.get("equivalent_ids", [])

                        # Sort equivalent identities by preference: AD > LOCAL > POSIX
                        def identity_preference(identity):
                            domain = identity.get("domain", "")
                            if domain == "ACTIVE_DIRECTORY":
                                return 0
                            elif domain == "LOCAL":
                                return 1
                            elif domain in ["POSIX_USER", "POSIX_GROUP"]:
                                return 2
                            else:
                                return 3

                        sorted_identities = sorted(
                            equivalent_ids, key=identity_preference
                        )

                        # Find the first equivalent identity with a name
                        for equiv_identity in sorted_identities:
                            if equiv_identity.get("name"):
                                # Found a better name - use this identity info
                                # But keep the original auth_id and domain info
                                best_identity["name"] = equiv_identity["name"]
                                # Keep track of both domains for display
                                if primary_identity.get("domain"):
                                    best_identity["domain"] = primary_identity["domain"]
                                best_identity["display_domain"] = equiv_identity.get(
                                    "domain"
                                )
                                break

                    # Store in cache for future use
                    self.persistent_identity_cache[auth_id] = best_identity

                    return best_identity

                elif response.status == 404:
                    # Identity not found - fall back to basic resolution
                    result = await self.resolve_identity(session, auth_id, "auth_id")
                    # Cache the result
                    self.persistent_identity_cache[auth_id] = result
                    return result
                else:
                    response.raise_for_status()

        except Exception:
            # Fall back to basic resolution on error
            result = await self.resolve_identity(session, auth_id, "auth_id")
            # Cache the fallback result
            self.persistent_identity_cache[auth_id] = result
            return result

    async def show_directory_stats(
        self,
        session: aiohttp.ClientSession,
        path: str,
        max_depth: int = 1,
        current_depth: int = 0,
    ) -> None:
        """
        Display directory statistics without enumerating entries.
        Uses aggregates API for fast exploration.

        Args:
            session: aiohttp ClientSession
            path: Directory path
            max_depth: Maximum depth to display (default: 1)
            current_depth: Current recursion depth
        """
        # Get aggregates
        aggregates = await self.get_directory_aggregates(session, path)

        if "error" in aggregates:
            print(f"\nDirectory: {path}")
            print("  (Unable to retrieve statistics)")
            return

        # Display statistics
        total_files = int(aggregates.get("total_files", 0))
        total_dirs = int(aggregates.get("total_directories", 0))
        total_entries = total_files + total_dirs
        total_capacity = int(aggregates.get("total_capacity", 0))
        oldest_time = aggregates.get("oldest_modification_time", "Unknown")
        newest_time = aggregates.get("newest_modification_time", "Unknown")

        # Format times
        if oldest_time != "Unknown":
            try:
                oldest_dt = datetime.fromisoformat(
                    oldest_time.rstrip("Z").split(".")[0]
                )
                oldest_time = oldest_dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass

        if newest_time != "Unknown":
            try:
                newest_dt = datetime.fromisoformat(
                    newest_time.rstrip("Z").split(".")[0]
                )
                newest_time = newest_dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass

        avg_size = total_capacity / total_files if total_files > 0 else 0

        # Print with indentation based on depth
        indent = "  " * current_depth
        print(f"\n{indent}Directory: {path}")
        print(
            f"{indent}  Total entries: {total_entries:,} ({total_files:,} files, {total_dirs:,} directories)"
        )
        print(f"{indent}  Total size: {format_bytes(total_capacity)}")
        print(f"{indent}  Modification time range: {oldest_time} to {newest_time}")
        if total_files > 0:
            print(f"{indent}  Average file size: {format_bytes(avg_size)}")

        # Recurse to subdirectories if depth permits
        if current_depth < max_depth:
            # Enumerate immediate children only (just to get directory names)
            try:
                entries = await self.enumerate_directory(session, path)
                subdirs = [
                    e for e in entries if e.get("type") == "FS_FILE_TYPE_DIRECTORY"
                ]

                for subdir in subdirs:
                    subdir_path = subdir["path"]
                    await self.show_directory_stats(
                        session, subdir_path, max_depth, current_depth + 1
                    )
            except Exception as e:
                if self.verbose:
                    print(
                        f"{indent}  [ERROR] Failed to enumerate subdirectories: {e}",
                        file=sys.stderr,
                    )

    async def expand_identity(
        self, session: aiohttp.ClientSession, auth_id: str
    ) -> List[str]:
        """
        Expand an identity to all equivalent auth_ids.

        Args:
            session: aiohttp ClientSession
            auth_id: The auth_id to expand

        Returns:
            List of all equivalent auth_ids (including the original)
        """
        url = f"{self.base_url}/v1/identity/expand"

        payload = {"auth_id": auth_id}

        try:
            async with session.post(
                url, json=payload, ssl=self.ssl_context
            ) as response:
                if response.status == 200:
                    result = await response.json()

                    # Extract all equivalent auth_ids
                    equivalent_ids = [auth_id]  # Include original

                    # Add from equivalent_ids array
                    for equiv in result.get("equivalent_ids", []):
                        equiv_auth_id = equiv.get("auth_id")
                        if equiv_auth_id and equiv_auth_id not in equivalent_ids:
                            equivalent_ids.append(equiv_auth_id)

                    # Add from nfs_id
                    nfs_auth_id = result.get("nfs_id", {}).get("auth_id")
                    if nfs_auth_id and nfs_auth_id not in equivalent_ids:
                        equivalent_ids.append(nfs_auth_id)

                    # Add from smb_id
                    smb_auth_id = result.get("smb_id", {}).get("auth_id")
                    if smb_auth_id and smb_auth_id not in equivalent_ids:
                        equivalent_ids.append(smb_auth_id)

                    # Add from id
                    id_auth_id = result.get("id", {}).get("auth_id")
                    if id_auth_id and id_auth_id not in equivalent_ids:
                        equivalent_ids.append(id_auth_id)

                    return equivalent_ids
                else:
                    # If expansion fails, return just the original
                    return [auth_id]
        except Exception:
            # If expansion fails, return just the original
            return [auth_id]


def parse_trustee(trustee_input: str) -> Dict:
    """
    Parse various trustee formats into an API payload.

    Supported formats:
        - SID: S-1-5-21-...
        - Auth ID: auth_id:500
        - UID: uid:1000 or just 1000
        - GID: gid:1001
        - Domain\\User: DOMAIN\\username
        - Email: user@domain.com
        - Plain name: username

    Returns:
        Dictionary suitable for /v1/identity/find endpoint with (payload, detected_type)
    """
    trustee = trustee_input.strip()

    # Windows SID format
    if trustee.startswith("S-") and len(trustee.split("-")) >= 3:
        return {"payload": {"sid": trustee}, "type": "sid"}

    # Explicit type prefixes
    if trustee.startswith("auth_id:"):
        return {"payload": {"auth_id": trustee[8:]}, "type": "auth_id"}

    if trustee.startswith("uid:"):
        try:
            return {"payload": {"uid": int(trustee[4:])}, "type": "uid"}
        except ValueError:
            return {"payload": {"name": trustee}, "type": "name"}

    if trustee.startswith("gid:"):
        try:
            return {"payload": {"gid": int(trustee[4:])}, "type": "gid"}
        except ValueError:
            return {"payload": {"name": trustee}, "type": "name"}

    # Pure numeric - assume UID
    if trustee.isdigit():
        return {"payload": {"uid": int(trustee)}, "type": "uid"}

    # NetBIOS domain format (DOMAIN\username)
    if "\\" in trustee:
        # Need to escape the backslash for JSON
        domain, username = trustee.split("\\", 1)
        return {"payload": {"name": f"{domain}\\\\{username}"}, "type": "name"}

    # Email or LDAP DN format
    if "@" in trustee or trustee.startswith("CN="):
        return {"payload": {"name": trustee}, "type": "name"}

    # Domain prefix formats (ad:user, local:user)
    if ":" in trustee and not trustee.startswith("S-"):
        prefix, name = trustee.split(":", 1)
        prefix = prefix.lower()

        if prefix in ["ad", "active_directory"]:
            return {
                "payload": {"name": name, "domain": "ACTIVE_DIRECTORY"},
                "type": "name",
            }
        elif prefix == "local":
            return {"payload": {"name": name, "domain": "LOCAL"}, "type": "name"}
        else:
            # Unknown prefix, treat as name
            return {"payload": {"name": trustee}, "type": "name"}

    # Default to name lookup
    return {"payload": {"name": trustee}, "type": "name"}


def parse_size_to_bytes(size_str: str) -> int:
    """Parse size string (e.g., '100MB', '1.5GiB') to bytes."""
    import re

    match = re.match(r"^([0-9]+\.?[0-9]*)([A-Za-z]*)$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    size_num = float(match.group(1))
    size_unit = match.group(2).lower()

    multipliers = {
        "": 1,
        "b": 1,
        "kb": 1000,
        "mb": 1000000,
        "gb": 1000000000,
        "tb": 1000000000000,
        "pb": 1000000000000000,
        "kib": 1024,
        "mib": 1048576,
        "gib": 1073741824,
        "tib": 1099511627776,
        "pib": 1125899906842624,
    }

    if size_unit not in multipliers:
        raise ValueError(f"Unknown size unit: {size_unit}")

    return int(size_num * multipliers[size_unit])


def calculate_confidence_percentage(num_sample_points: int) -> str:
    """
    Calculate confidence percentage for duplicate detection based on sample points.

    Args:
        num_sample_points: Number of sample points used

    Returns:
        Human-readable confidence string
    """
    # Each 64KB sample with SHA-256 has 2^256 possible values
    # Collision probability for independent samples: 1 / (2^256)^n
    # For practical purposes, anything beyond 10^-40 is effectively 100%

    # Rough approximation of confidence:
    # 3 points: 99.999999999999999999999999999999999999999999999999% (10^-50)
    # 5 points: effectively 100% (10^-80)
    # 7+ points: effectively 100% (10^-110+)

    if num_sample_points >= 5:
        return ">99.9999999999999999999999999999%"
    elif num_sample_points >= 3:
        return ">99.99999999999999999999999%"
    else:
        return ">99.999%"


def calculate_sample_points(file_size: int, sample_points: Optional[int] = None) -> List[int]:
    """
    Calculate adaptive sample points based on file size.

    Args:
        file_size: Size of the file in bytes
        sample_points: Override number of sample points (3-11), or None for adaptive

    Returns:
        List of byte offsets to sample from
    """
    SAMPLE_CHUNK_SIZE = 65536  # 64KB per sample

    # Special case: empty files
    if file_size == 0:
        return [0]  # Single sample at offset 0 (will read 0 bytes)

    # User override
    if sample_points is not None:
        num_points = max(3, min(11, sample_points))  # Clamp to 3-11
    else:
        # Adaptive based on file size
        if file_size < 1_000_000:  # < 1MB
            num_points = 3
        elif file_size < 100_000_000:  # < 100MB
            num_points = 5
        elif file_size < 1_000_000_000:  # < 1GB
            num_points = 7
        elif file_size < 10_000_000_000:  # < 10GB
            num_points = 9
        else:  # >= 10GB
            num_points = 11

    # Calculate evenly distributed offsets
    offsets = []
    for i in range(num_points):
        # Distribute points evenly: 0%, ~14%, ~28%, ..., ~85%, ~100%
        position = i / (num_points - 1) if num_points > 1 else 0
        offset = int(position * file_size)

        # Ensure we don't read past end of file
        if offset + SAMPLE_CHUNK_SIZE > file_size:
            offset = max(0, file_size - SAMPLE_CHUNK_SIZE)

        offsets.append(offset)

    # Remove duplicates (can happen with very small files) and sort
    return sorted(set(offsets))


async def compute_sample_hash(
    client: AsyncQumuloClient,
    session: aiohttp.ClientSession,
    file_path: str,
    file_size: int,
    sample_points: Optional[int] = None
) -> Optional[str]:
    """
    Compute a hash from multiple sample points in a file.

    Args:
        client: AsyncQumuloClient instance
        session: aiohttp ClientSession
        file_path: Path to the file
        file_size: Size of the file in bytes
        sample_points: Optional override for number of sample points

    Returns:
        SHA-256 hash of concatenated samples, or None if failed
    """
    import hashlib

    SAMPLE_CHUNK_SIZE = 65536  # 64KB

    offsets = calculate_sample_points(file_size, sample_points)

    # Read all sample points concurrently
    tasks = []
    for offset in offsets:
        task = client.read_file_chunk(session, file_path, offset, SAMPLE_CHUNK_SIZE)
        tasks.append(task)

    chunks = await asyncio.gather(*tasks)

    # Check if any reads failed
    if None in chunks:
        return None

    # Concatenate all chunks and hash
    combined = b''.join(chunks)
    hash_digest = hashlib.sha256(combined).hexdigest()

    return hash_digest


async def find_duplicates(
    client: AsyncQumuloClient,
    files: List[Dict],
    by_size_only: bool = False,
    sample_points: Optional[int] = None,
    progress: Optional['ProgressTracker'] = None
) -> Dict[str, List[Dict]]:
    """
    Find duplicate files using metadata filtering and sample hashing.

    Phase 1: Group by size + datablocks + sparse_file (instant)
    Phase 2: Compute sample hashes for potential duplicates (fast)
    Phase 3: Return groups of duplicates

    Args:
        client: AsyncQumuloClient instance
        files: List of file entries with metadata
        by_size_only: If True, only use size for duplicate detection (no hashing)
        sample_points: Optional override for number of sample points
        progress: Optional ProgressTracker for status updates

    Returns:
        Dictionary mapping fingerprint -> list of duplicate files
    """
    from collections import defaultdict

    if progress and progress.verbose:
        print(f"[DUPLICATE DETECTION] Phase 1: Metadata pre-filtering {len(files):,} files", file=sys.stderr)

    # Phase 1: Group by metadata (size, datablocks, sparse_file)
    metadata_groups = defaultdict(list)

    for entry in files:
        size = int(entry.get('size', 0))
        datablocks = entry.get('datablocks', 'unknown')
        sparse = entry.get('extended_attributes', {}).get('sparse_file', False)

        # Create metadata fingerprint
        fingerprint = f"{size}:{datablocks}:{sparse}"
        metadata_groups[fingerprint].append(entry)

    # Filter to only groups with 2+ files
    potential_duplicates = {k: v for k, v in metadata_groups.items() if len(v) >= 2}

    if progress and progress.verbose:
        total_potential = sum(len(v) for v in potential_duplicates.values())
        print(f"[DUPLICATE DETECTION] Found {total_potential:,} potential duplicates in {len(potential_duplicates):,} groups", file=sys.stderr)

    # If size-only mode, return now
    if by_size_only:
        return potential_duplicates

    # Phase 2: Compute sample hashes for potential duplicates
    if progress and progress.verbose:
        print(f"[DUPLICATE DETECTION] Phase 2: Computing sample hashes", file=sys.stderr)

    hash_groups = defaultdict(list)
    BATCH_SIZE = 1000  # Process files in batches to avoid overwhelming the system

    # Track progress across all groups
    total_files_to_hash = sum(len(group) for group in potential_duplicates.values())
    files_hashed = 0
    hash_start_time = time.time()
    last_progress_update = time.time()
    is_tty = sys.stderr.isatty() if progress else False

    async with client.create_session() as session:
        for fingerprint, group in potential_duplicates.items():
            # Extract size from fingerprint
            size_str = fingerprint.split(':')[0]
            file_size = int(size_str)

            # Process files in batches
            for i in range(0, len(group), BATCH_SIZE):
                batch = group[i:i + BATCH_SIZE]

                # Compute hashes for this batch concurrently
                tasks = []
                for entry in batch:
                    file_path = entry['path']
                    task = compute_sample_hash(client, session, file_path, file_size, sample_points)
                    tasks.append((entry, task))

                # Wait for all hashes in this batch to complete
                results = await asyncio.gather(*[task for _, task in tasks])

                # Group by hash
                for (entry, _), sample_hash in zip(tasks, results):
                    if sample_hash:
                        # Create combined fingerprint: metadata + hash
                        combined_fingerprint = f"{fingerprint}:{sample_hash}"
                        hash_groups[combined_fingerprint].append(entry)

                # Update progress
                files_hashed += len(batch)

                if progress:
                    current_time = time.time()
                    elapsed = current_time - hash_start_time
                    rate = files_hashed / elapsed if elapsed > 0 else 0
                    remaining = total_files_to_hash - files_hashed

                    progress_msg = (f"[DUPLICATE DETECTION] {files_hashed:,} / {total_files_to_hash:,} hashed | "
                                  f"{remaining:,} remaining | {rate:.1f} files/sec")

                    # Only update progress display every 0.5 seconds
                    if is_tty:
                        # Always overwrite in TTY mode
                        print(f"\r{progress_msg}", end='', file=sys.stderr, flush=True)
                    elif progress.verbose and (current_time - last_progress_update) > 0.5:
                        # Only print on new line when redirected AND enough time has passed
                        print(progress_msg, file=sys.stderr, flush=True)
                        last_progress_update = current_time

    # Print final summary
    if progress and is_tty:
        elapsed = time.time() - hash_start_time
        rate = files_hashed / elapsed if elapsed > 0 else 0
        print(f"\r[DUPLICATE DETECTION] FINAL: {files_hashed:,} files hashed | {rate:.1f} files/sec | {elapsed:.1f}s", file=sys.stderr)
    elif progress and progress.verbose:
        elapsed = time.time() - hash_start_time
        rate = files_hashed / elapsed if elapsed > 0 else 0
        print(f"[DUPLICATE DETECTION] FINAL: {files_hashed:,} files hashed | {rate:.1f} files/sec | {elapsed:.1f}s", file=sys.stderr)

    # Filter to only groups with 2+ files (actual duplicates)
    duplicates = {k: v for k, v in hash_groups.items() if len(v) >= 2}

    if progress and progress.verbose:
        total_duplicates = sum(len(v) for v in duplicates.values())
        print(f"[DUPLICATE DETECTION] Found {total_duplicates:,} confirmed duplicates in {len(duplicates):,} groups", file=sys.stderr)

    return duplicates


async def resolve_owner_filters(
    client: AsyncQumuloClient, session: aiohttp.ClientSession, args
) -> Optional[Set[str]]:
    """
    Resolve owner filter arguments to a set of auth_ids to match.

    Args:
        client: AsyncQumuloClient instance
        session: aiohttp ClientSession
        args: Command-line arguments

    Returns:
        Set of auth_ids to match, or None if no owner filter specified
    """
    if not args.owners:
        return None

    # Determine owner type
    owner_type = "auto"
    if args.ad:
        owner_type = "ad"
    elif args.local:
        owner_type = "local"
    elif args.uid:
        owner_type = "uid"

    all_auth_ids = set()

    for owner in args.owners:
        # Parse the owner input based on type
        if owner_type == "uid":
            # UID - resolve by UID
            try:
                identity = await client.resolve_identity(session, owner, "uid")
                if identity.get("resolved") and identity.get("auth_id"):
                    all_auth_ids.add(identity["auth_id"])
            except Exception as e:
                print(f"[WARN] Failed to resolve UID {owner}: {e}", file=sys.stderr)
        elif owner_type == "ad":
            # Active Directory - resolve by name with AD domain
            payload_info = parse_trustee(f"ad:{owner}")
            payload = payload_info["payload"]

            url = f"{client.base_url}/v1/identity/find"
            try:
                async with session.post(
                    url, json=payload, ssl=client.ssl_context
                ) as response:
                    if response.status == 200:
                        identity = await response.json()
                        if identity.get("auth_id"):
                            all_auth_ids.add(identity["auth_id"])
            except Exception as e:
                print(f"[WARN] Failed to resolve AD user {owner}: {e}", file=sys.stderr)
        elif owner_type == "local":
            # Local - resolve by name with LOCAL domain
            payload_info = parse_trustee(f"local:{owner}")
            payload = payload_info["payload"]

            url = f"{client.base_url}/v1/identity/find"
            try:
                async with session.post(
                    url, json=payload, ssl=client.ssl_context
                ) as response:
                    if response.status == 200:
                        identity = await response.json()
                        if identity.get("auth_id"):
                            all_auth_ids.add(identity["auth_id"])
            except Exception as e:
                print(
                    f"[WARN] Failed to resolve local user {owner}: {e}", file=sys.stderr
                )
        else:
            # Auto-detect - parse and resolve
            payload_info = parse_trustee(owner)
            payload = payload_info["payload"]

            url = f"{client.base_url}/v1/identity/find"
            try:
                async with session.post(
                    url, json=payload, ssl=client.ssl_context
                ) as response:
                    if response.status == 200:
                        identity = await response.json()
                        if identity.get("auth_id"):
                            all_auth_ids.add(identity["auth_id"])
            except Exception as e:
                print(f"[WARN] Failed to resolve owner {owner}: {e}", file=sys.stderr)

    # If expand-identity is enabled, expand all auth_ids
    if args.expand_identity and all_auth_ids:
        expanded_ids = set()
        for auth_id in all_auth_ids:
            equivalent_ids = await client.expand_identity(session, auth_id)
            expanded_ids.update(equivalent_ids)
        return expanded_ids

    return all_auth_ids if all_auth_ids else None


def glob_to_regex(pattern: str) -> str:
    """
    Convert a glob pattern to a regex pattern.
    Supports common glob wildcards: *, ?, [seq], [!seq]

    If the pattern is already a valid regex (contains regex special chars
    that aren't glob chars), return it as-is.

    Args:
        pattern: Glob or regex pattern

    Returns:
        Regex pattern string
    """
    # Check if this looks like a regex pattern (contains regex-specific chars)
    # that aren't also glob chars. If so, assume it's already regex.
    regex_specific_chars = {'^', '$', '.', '+', '(', ')', '|', '{', '}', '\\'}

    # If pattern starts with common regex anchors or contains regex-specific syntax,
    # treat it as regex
    if pattern.startswith('^') or pattern.endswith('$'):
        return pattern

    # Check for regex-specific characters (excluding those used in globs)
    has_regex_chars = any(char in pattern for char in regex_specific_chars)

    # If it has regex chars, try to compile it as regex first
    if has_regex_chars:
        try:
            re.compile(pattern)
            # If it compiles successfully, it's likely a regex pattern
            return pattern
        except re.error:
            # If it fails, fall through to glob conversion
            pass

    # Convert glob to regex using fnmatch
    return fnmatch.translate(pattern)


def create_file_filter(args, owner_auth_ids: Optional[Set[str]] = None):
    """Create a file filter function based on command-line arguments."""

    # Calculate time thresholds (using current UTC time)
    now_utc = datetime.now(timezone.utc).replace(
        tzinfo=None
    )  # Convert to timezone-naive for comparison
    time_threshold_older = None
    time_threshold_newer = None

    if args.older_than:
        time_threshold_older = now_utc - timedelta(days=args.older_than)
    if args.newer_than:
        time_threshold_newer = now_utc - timedelta(days=args.newer_than)

    # Calculate field-specific time thresholds
    field_time_filters = {}

    if args.accessed_older_than or args.accessed_newer_than:
        field_time_filters["access_time"] = {
            "older": (
                now_utc - timedelta(days=args.accessed_older_than)
                if args.accessed_older_than
                else None
            ),
            "newer": (
                now_utc - timedelta(days=args.accessed_newer_than)
                if args.accessed_newer_than
                else None
            ),
        }

    if args.modified_older_than or args.modified_newer_than:
        field_time_filters["modification_time"] = {
            "older": (
                now_utc - timedelta(days=args.modified_older_than)
                if args.modified_older_than
                else None
            ),
            "newer": (
                now_utc - timedelta(days=args.modified_newer_than)
                if args.modified_newer_than
                else None
            ),
        }

    if args.created_older_than or args.created_newer_than:
        field_time_filters["creation_time"] = {
            "older": (
                now_utc - timedelta(days=args.created_older_than)
                if args.created_older_than
                else None
            ),
            "newer": (
                now_utc - timedelta(days=args.created_newer_than)
                if args.created_newer_than
                else None
            ),
        }

    if args.changed_older_than or args.changed_newer_than:
        field_time_filters["change_time"] = {
            "older": (
                now_utc - timedelta(days=args.changed_older_than)
                if args.changed_older_than
                else None
            ),
            "newer": (
                now_utc - timedelta(days=args.changed_newer_than)
                if args.changed_newer_than
                else None
            ),
        }

    # Parse size filters
    size_larger = None
    size_smaller = None
    include_metadata = args.include_metadata

    if args.larger_than:
        size_larger = parse_size_to_bytes(args.larger_than)
    if args.smaller_than:
        size_smaller = parse_size_to_bytes(args.smaller_than)

    # Determine time field
    time_field = args.time_field

    # Compile name patterns (OR logic)
    name_patterns_or = []
    if args.name_patterns:
        regex_flags = 0 if args.name_case_sensitive else re.IGNORECASE
        for pattern in args.name_patterns:
            try:
                # Convert glob to regex if needed
                regex_pattern = glob_to_regex(pattern)
                name_patterns_or.append(re.compile(regex_pattern, regex_flags))
            except re.error as e:
                print(f"[ERROR] Invalid pattern '{pattern}': {e}", file=sys.stderr)
                sys.exit(1)

    # Compile name patterns (AND logic)
    name_patterns_and = []
    if args.name_patterns_and:
        regex_flags = 0 if args.name_case_sensitive else re.IGNORECASE
        for pattern in args.name_patterns_and:
            try:
                # Convert glob to regex if needed
                regex_pattern = glob_to_regex(pattern)
                name_patterns_and.append(re.compile(regex_pattern, regex_flags))
            except re.error as e:
                print(f"[ERROR] Invalid pattern '{pattern}': {e}", file=sys.stderr)
                sys.exit(1)

    # Map type argument to Qumulo API type
    target_type = None
    if args.type:
        type_mapping = {
            'file': 'FS_FILE_TYPE_FILE',
            'f': 'FS_FILE_TYPE_FILE',
            'directory': 'FS_FILE_TYPE_DIRECTORY',
            'dir': 'FS_FILE_TYPE_DIRECTORY',
            'd': 'FS_FILE_TYPE_DIRECTORY',
            'symlink': 'FS_FILE_TYPE_SYMLINK',
            'link': 'FS_FILE_TYPE_SYMLINK',
            'l': 'FS_FILE_TYPE_SYMLINK',
        }
        target_type = type_mapping.get(args.type)

    def file_filter(entry: dict) -> bool:
        """Filter function that returns True if entry matches all criteria."""

        # Type filter
        if target_type and entry.get("type") != target_type:
            return False

        # Name pattern filters (OR logic - any pattern can match)
        if name_patterns_or:
            # Extract basename from path
            path = entry.get("path", "")
            name = path.rstrip('/').split('/')[-1] if '/' in path else path

            # Check if any pattern matches
            if not any(pattern.search(name) for pattern in name_patterns_or):
                return False

        # Name pattern filters (AND logic - all patterns must match)
        if name_patterns_and:
            # Extract basename from path
            path = entry.get("path", "")
            name = path.rstrip('/').split('/')[-1] if '/' in path else path

            # Check if all patterns match
            if not all(pattern.search(name) for pattern in name_patterns_and):
                return False

        # File-only filter (deprecated - use --type file instead)
        if args.file_only and entry.get("type") == "FS_FILE_TYPE_DIRECTORY":
            return False

        # Owner filter
        if owner_auth_ids is not None:
            # Get owner from entry - try owner_details first, then owner
            owner_details = entry.get("owner_details", {})
            file_owner_auth_id = owner_details.get("auth_id") or entry.get("owner")

            if not file_owner_auth_id:
                # No owner info, skip this file
                return False

            # Check if file owner matches any of our target auth_ids
            if file_owner_auth_id not in owner_auth_ids:
                return False

        # Size filters
        if size_larger is not None or size_smaller is not None:
            file_size = entry.get("size")
            if file_size is None:
                return False

            try:
                size_bytes = int(file_size)

                # Add metadata size if requested
                if include_metadata:
                    metablocks = entry.get("metablocks")
                    if metablocks:
                        try:
                            metadata_bytes = int(metablocks) * 4096
                            size_bytes += metadata_bytes
                        except (ValueError, TypeError):
                            pass  # If metablocks is invalid, just use file size

                if size_larger is not None and size_bytes <= size_larger:
                    return False
                if size_smaller is not None and size_bytes >= size_smaller:
                    return False
            except (ValueError, TypeError):
                return False

        # Time filters
        if time_threshold_older is not None or time_threshold_newer is not None:
            time_value = entry.get(time_field)
            if not time_value:
                return False

            try:
                # Parse Qumulo timestamp format: "2023-01-15T10:30:45.123456789Z"
                # Remove 'Z' and parse as timezone-naive for comparison
                file_time = datetime.fromisoformat(time_value.rstrip("Z").split(".")[0])

                if (
                    time_threshold_older is not None
                    and file_time >= time_threshold_older
                ):
                    return False
                if (
                    time_threshold_newer is not None
                    and file_time <= time_threshold_newer
                ):
                    return False
            except (ValueError, AttributeError):
                return False

        # Field-specific time filters (AND logic - all must match)
        for field_name, thresholds in field_time_filters.items():
            time_value = entry.get(field_name)
            if not time_value:
                return False  # If field is missing, reject the file

            try:
                # Parse Qumulo timestamp format
                file_time = datetime.fromisoformat(time_value.rstrip("Z").split(".")[0])

                # Check both older and newer thresholds if specified
                if thresholds["older"] is not None and file_time >= thresholds["older"]:
                    return False
                if thresholds["newer"] is not None and file_time <= thresholds["newer"]:
                    return False
            except (ValueError, AttributeError):
                return False  # If parsing fails, reject the file

        return True

    return file_filter


def format_bytes(bytes_value: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} EB"


def format_time(seconds: float) -> str:
    """
    Format elapsed time in human-friendly format with total seconds.

    Examples:
        5.2s (5.2s)
        72.3s -> 1m 12s (72.3s)
        3665.7s -> 1h 1m 5s (3665.7s)
    """
    total_seconds = seconds

    if seconds < 60:
        # Less than a minute - just show seconds
        return f"{seconds:.1f}s"

    hours = int(seconds // 3600)
    seconds = seconds % 3600
    minutes = int(seconds // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    friendly = " ".join(parts)
    return f"{friendly} ({total_seconds:.1f}s)"


def format_owner_name(identity: Dict) -> str:
    """Format owner name from resolved identity."""
    if not identity:
        return "Unknown"

    owner_name = identity.get("name", "Unknown")
    domain = identity.get("domain", "UNKNOWN")

    # For POSIX_USER domain, show UID if available
    if domain == "POSIX_USER" and "uid" in identity:
        uid = identity.get("uid")
        if owner_name and owner_name.startswith("Unknown"):
            return f"UID {uid}"
        elif owner_name:
            return f"{owner_name} (UID {uid})"
        else:
            return f"UID {uid}"

    # For POSIX_GROUP domain, show GID if available
    elif domain == "POSIX_GROUP" and "gid" in identity:
        gid = identity.get("gid")
        if owner_name and owner_name.startswith("Unknown"):
            return f"GID {gid}"
        elif owner_name:
            return f"{owner_name} (GID {gid})"
        else:
            return f"GID {gid}"

    return owner_name


async def generate_owner_report(
    client: AsyncQumuloClient, owner_stats: OwnerStats, args, elapsed_time: float
):
    """Generate and display ownership report."""
    print("\n" + "=" * 80, file=sys.stderr)
    print("OWNER REPORT", file=sys.stderr)
    print("=" * 80, file=sys.stderr)

    # Get all unique owners
    all_owners = owner_stats.get_all_owners()

    if not all_owners:
        print("No files found", file=sys.stderr)
        return

    # Resolve all owners in parallel
    async with client.create_session() as session:
        identity_cache = await client.resolve_multiple_identities(
            session, all_owners, show_progress=True
        )

    # Build report data
    report_rows = []
    total_bytes = 0
    total_files = 0
    total_dirs = 0

    for owner_auth_id in all_owners:
        stats = owner_stats.get_stats(owner_auth_id)
        identity = identity_cache.get(owner_auth_id, {})

        # Extract owner name, including UID/GID for POSIX users
        owner_name = identity.get("name", f"Unknown ({owner_auth_id})")
        domain = identity.get("domain", "UNKNOWN")

        # For POSIX_USER domain, show UID if available
        if domain == "POSIX_USER" and "uid" in identity:
            uid = identity.get("uid")
            # If name is generic "Unknown", replace with UID
            if owner_name and owner_name.startswith("Unknown"):
                owner_name = f"UID {uid}"
            elif owner_name:
                # Append UID to name
                owner_name = f"{owner_name} (UID {uid})"
            else:
                # No name at all, use UID
                owner_name = f"UID {uid}"

        # For POSIX_GROUP domain, show GID if available
        elif domain == "POSIX_GROUP" and "gid" in identity:
            gid = identity.get("gid")
            if owner_name and owner_name.startswith("Unknown"):
                owner_name = f"GID {gid}"
            elif owner_name:
                owner_name = f"{owner_name} (GID {gid})"
            else:
                # No name at all, use GID
                owner_name = f"GID {gid}"

        report_rows.append(
            {
                "owner": owner_name,
                "domain": domain,
                "auth_id": owner_auth_id,
                "bytes": stats["bytes"],
                "files": stats["files"],
                "dirs": stats["dirs"],
            }
        )

        total_bytes += stats["bytes"]
        total_files += stats["files"]
        total_dirs += stats["dirs"]

    # Sort by bytes descending
    report_rows.sort(key=lambda x: x["bytes"], reverse=True)

    # Print report
    print(
        f"\n{'Owner':<30} {'Domain':<20} {'Files':>10} {'Dirs':>8} {'Total Size':>15}",
        file=sys.stderr,
    )
    print("-" * 90, file=sys.stderr)

    for row in report_rows:
        owner = row["owner"] or "Unknown"
        domain = row["domain"] or "UNKNOWN"
        print(
            f"{owner:<30} {domain:<20} {row['files']:>10,} {row['dirs']:>8,} {format_bytes(row['bytes']):>15}",
            file=sys.stderr,
        )

    print("-" * 90, file=sys.stderr)
    print(
        f"{'TOTAL':<30} {'':<20} {total_files:>10,} {total_dirs:>8,} {format_bytes(total_bytes):>15}",
        file=sys.stderr,
    )

    print(f"\nProcessing time: {elapsed_time:.2f}s", file=sys.stderr)
    rate = (total_files + total_dirs) / elapsed_time if elapsed_time > 0 else 0
    print(f"Processing rate: {rate:.1f} obj/sec", file=sys.stderr)

    # Print cache statistics
    total_lookups = client.cache_hits + client.cache_misses
    if total_lookups > 0:
        hit_rate = (client.cache_hits / total_lookups) * 100
        print(
            f"\nIdentity cache: {client.cache_hits} hits, {client.cache_misses} misses ({hit_rate:.1f}% hit rate)",
            file=sys.stderr,
        )

    print("=" * 80, file=sys.stderr)


async def main_async(args):
    """Main async function."""
    print("=" * 70, file=sys.stderr)
    print("Qumulo File Filter - Async Python (aiohttp)", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"Cluster:          {args.host}", file=sys.stderr)
    print(f"Path:             {args.path}", file=sys.stderr)
    print(f"JSON parser:      {JSON_PARSER_NAME}", file=sys.stderr)
    print(f"Max concurrent:   {args.max_concurrent}", file=sys.stderr)
    print(f"Connection pool:  {args.connector_limit}", file=sys.stderr)
    if args.max_depth:
        print(f"Max depth:        {args.max_depth}", file=sys.stderr)
    if args.progress:
        print(f"Progress:         Enabled", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # Load credentials
    if args.credentials_store:
        creds = get_credentials(args.credentials_store)
    else:
        creds = get_credentials(credential_store_filename())

    if not creds:
        print(
            "\n[ERROR] No credentials found. Please run 'qq --host <cluster> login' first.",
            file=sys.stderr,
        )
        sys.exit(1)

    bearer_token = creds.bearer_token

    # Load persistent identity cache
    identity_cache = load_identity_cache(verbose=args.verbose)

    # Create client with identity cache
    client = AsyncQumuloClient(
        args.host,
        args.port,
        bearer_token,
        args.max_concurrent,
        args.connector_limit,
        identity_cache=identity_cache,
        verbose=args.verbose,
    )

    # PHASE 3: Directory statistics exploration mode
    if args.show_dir_stats:
        print("\n[INFO] Directory statistics mode (exploration)", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        start_time = time.time()

        # Use max_depth from args, default to 1 if not specified
        depth = args.max_depth if args.max_depth else 1

        async with client.create_session() as session:
            await client.show_directory_stats(session, args.path, max_depth=depth)

        elapsed = time.time() - start_time
        print(f"\n{'=' * 70}", file=sys.stderr)
        print(f"Exploration completed in {elapsed:.2f}s", file=sys.stderr)
        return

    # Resolve owner filters if specified
    owner_auth_ids = None
    profiler = Profiler() if args.profile else None

    if args.owners:
        print("\nResolving owner identities...", file=sys.stderr)
        if profiler:
            resolve_start = time.time()

        async with client.create_session() as session:
            owner_auth_ids = await resolve_owner_filters(client, session, args)

        if profiler:
            profiler.record_sync(
                "owner_identity_resolution", time.time() - resolve_start
            )

        if owner_auth_ids:
            print(
                f"Filtering by {len(owner_auth_ids)} owner auth_id(s)", file=sys.stderr
            )
            if args.verbose:
                print(f"Owner auth_ids: {', '.join(owner_auth_ids)}", file=sys.stderr)
        else:
            print(
                "[WARN] No valid owners resolved - no files will match!",
                file=sys.stderr,
            )

    # Create file filter
    file_filter = create_file_filter(args, owner_auth_ids)

    # PHASE 3: Prepare filter info for smart skipping
    # Build time filter info for aggregates-based smart skipping
    time_filter_info = None
    if args.older_than or args.newer_than:
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        time_filter_info = {
            "time_field": args.time_field,
            "older_than": (
                now_utc - timedelta(days=args.older_than) if args.older_than else None
            ),
            "newer_than": (
                now_utc - timedelta(days=args.newer_than) if args.newer_than else None
            ),
        }

    # Build size filter info for aggregates-based smart skipping
    size_filter_info = None
    if args.larger_than:
        # Only support --larger-than for smart skipping (min size threshold)
        size_filter_info = {"min_size": parse_size_to_bytes(args.larger_than)}

    # PHASE 3.3: Build owner filter info for aggregates-based smart skipping
    owner_filter_info = None
    if owner_auth_ids:
        owner_filter_info = {"auth_ids": owner_auth_ids}

    # Create progress tracker with optional limit for early exit
    progress = (
        ProgressTracker(verbose=args.progress, limit=args.limit)
        if args.progress
        else None
    )

    # Fetch and display directory aggregates to inform user of search scope
    async with client.create_session() as session:
        try:
            aggregates = await client.get_directory_aggregates(session, args.path)
            total_files = aggregates.get('total_files', 'unknown')
            total_dirs = aggregates.get('total_directories', 'unknown')

            # Format numbers with commas
            if isinstance(total_files, str):
                files_str = total_files
            else:
                files_str = f"{int(total_files):,}"

            if isinstance(total_dirs, str):
                dirs_str = total_dirs
            else:
                dirs_str = f"{int(total_dirs):,}"

            print(f"Searching directory {args.path} ({dirs_str} subdirectories, {files_str} files)",
                  file=sys.stderr)
        except Exception as e:
            # If aggregates fail, just continue without displaying them
            if args.verbose:
                print(f"[WARN] Could not fetch directory aggregates: {e}", file=sys.stderr)

    # Create owner stats tracker if owner-report enabled
    # Use capacity-based calculation (actual disk usage) by default to handle sparse files correctly
    owner_stats = (
        OwnerStats(use_capacity=args.use_capacity) if args.owner_report else None
    )

    # Walk tree and collect matches
    start_time = time.time()

    if profiler:
        tree_walk_start = time.time()

    # For owner reports, don't collect matching files to save memory
    # Also collect results if we need to resolve symlinks or find duplicates
    collect_results = not args.owner_report or args.resolve_links or args.find_duplicates

    # Create output callback for streaming results to stdout (plain text mode only)
    # Disable streaming if --resolve-links is enabled (need to resolve after collection)
    output_callback = None
    batched_handler = None

    if not args.owner_report and not args.csv_out and not args.json_out and not args.resolve_links and not args.find_duplicates:
        if args.show_owner:
            # Use batched output handler for streaming with owner resolution
            output_format = "json" if args.json else "text"
            batched_handler = BatchedOutputHandler(
                client, batch_size=100, show_owner=True, output_format=output_format
            )

            async def output_callback(entry):
                await batched_handler.add_entry(entry)

        else:
            # Direct streaming output (no owner resolution needed)
            if args.json:
                # JSON to stdout
                async def output_callback(entry):
                    print(json_parser.dumps(entry))
                    sys.stdout.flush()

            else:
                # Plain text to stdout
                async def output_callback(entry):
                    print(entry["path"])
                    sys.stdout.flush()

    async with client.create_session() as session:
        matching_files = await client.walk_tree_async(
            session,
            args.path,
            args.max_depth,
            progress=progress,
            file_filter=file_filter,
            owner_stats=owner_stats,
            omit_subdirs=args.omit_subdirs,
            collect_results=collect_results,
            verbose=args.verbose,
            max_entries_per_dir=args.max_entries_per_dir,
            time_filter_info=time_filter_info,
            size_filter_info=size_filter_info,
            owner_filter_info=owner_filter_info,
            output_callback=output_callback,
        )

    if profiler:
        tree_walk_time = time.time() - tree_walk_start
        profiler.record_sync("tree_walking", tree_walk_time)

    elapsed = time.time() - start_time

    # Final progress report
    if progress:
        progress.final_report()

    # Add diagnostic timing
    if args.progress or args.verbose:
        print(
            f"[INFO] Tree walk completed, collected {len(matching_files)} matching files",
            file=sys.stderr,
        )

    # Flush any remaining batched output
    if batched_handler:
        await batched_handler.flush()

    # Resolve owner identities if --show-owner is enabled (for non-streaming modes only)
    # Skip if batched_handler was used (streaming mode)
    identity_cache_for_output = {}
    if args.show_owner and matching_files and not batched_handler:
        # Collect unique owner auth_ids from matching files
        unique_owners = set()
        for entry in matching_files:
            owner_details = entry.get("owner_details", {})
            owner_auth_id = owner_details.get("auth_id") or entry.get("owner")
            if owner_auth_id:
                unique_owners.add(owner_auth_id)

        if unique_owners:
            async with client.create_session() as session:
                identity_cache_for_output = await client.resolve_multiple_identities(
                    session,
                    list(unique_owners),
                    show_progress=args.verbose or args.progress,
                )

    # Resolve symlinks if --resolve-links is enabled
    if args.resolve_links and matching_files and not batched_handler:
        async with client.create_session() as session:
            for entry in matching_files:
                if entry.get("type") == "FS_FILE_TYPE_SYMLINK":
                    target = await client.read_symlink(session, entry["path"])
                    if target:
                        # Convert relative paths to absolute paths
                        if not target.startswith('/'):
                            # Relative path - resolve relative to symlink's directory
                            import os.path
                            symlink_dir = os.path.dirname(entry["path"])
                            # Normalize path to handle .. and . components
                            absolute_target = os.path.normpath(os.path.join(symlink_dir, target))
                            entry["symlink_target"] = absolute_target
                        else:
                            # Already absolute
                            entry["symlink_target"] = target
                    else:
                        entry["symlink_target"] = "(unreadable)"

    # Generate owner report if requested
    if args.owner_report and owner_stats:
        if profiler:
            report_start = time.time()
        await generate_owner_report(client, owner_stats, args, elapsed)
        if profiler:
            profiler.record_sync("owner_report_generation", time.time() - report_start)
            profiler.print_report(elapsed)

        # Save identity cache before exiting
        save_identity_cache(client.persistent_identity_cache, verbose=args.verbose)
        return  # Exit after report, don't output file list

    # Find duplicates if requested
    if args.find_duplicates:
        if profiler:
            dup_start = time.time()

        print(f"\n{'=' * 70}", file=sys.stderr)
        print(f"DUPLICATE DETECTION", file=sys.stderr)
        print(f"{'=' * 70}", file=sys.stderr)

        duplicates = await find_duplicates(
            client,
            matching_files,
            by_size_only=args.by_size,
            sample_points=args.sample_points,
            progress=progress
        )

        if profiler:
            profiler.record_sync("duplicate_detection", time.time() - dup_start)

        # Report results
        if not duplicates:
            print("\nNo duplicates found.", file=sys.stderr)
        else:
            total_groups = len(duplicates)
            total_dupes = sum(len(group) for group in duplicates.values())

            # Calculate confidence based on detection method
            if args.by_size:
                confidence_msg = "Detection method: Size+metadata only (may have false positives)"
            else:
                # Get sample point count from first group (representative)
                first_file = next(iter(duplicates.values()))[0]
                file_size = int(first_file.get('size', 0))
                sample_offsets = calculate_sample_points(file_size, args.sample_points)
                num_points = len(sample_offsets)
                confidence = calculate_confidence_percentage(num_points)
                confidence_msg = f"Detection method: {num_points}-point sampling (confidence: {confidence})"

            print(f"\nFound {total_dupes:,} duplicate files in {total_groups:,} groups", file=sys.stderr)
            print(f"{confidence_msg}", file=sys.stderr)
            print(f"{'=' * 70}\n", file=sys.stderr)

            # Output duplicate groups
            for group_id, (fingerprint, files) in enumerate(duplicates.items(), 1):
                # Extract size from fingerprint
                size_str = fingerprint.split(':')[0]
                file_size = int(size_str)

                print(f"Group {group_id}: {len(files)} files ({file_size:,} bytes each)", file=sys.stderr)
                for f in files:
                    print(f"  {f['path']}", file=sys.stderr)
                print(file=sys.stderr)

        if profiler:
            profiler.print_report(elapsed)

        # Save identity cache before exiting
        save_identity_cache(client.persistent_identity_cache, verbose=args.verbose)
        return  # Exit after duplicate detection

    # Apply limit if specified
    if args.limit and len(matching_files) > args.limit:
        if args.verbose:
            print(
                f"\n[INFO] Limiting results to {args.limit} files (found {len(matching_files)})",
                file=sys.stderr,
            )
        matching_files = matching_files[: args.limit]

    # Output results
    if profiler:
        output_start = time.time()

    if args.csv_out:
        # CSV output
        import csv

        with open(args.csv_out, "w", newline="") as csv_file:
            if not matching_files:
                if args.verbose:
                    print(
                        f"[INFO] No matching files found, CSV file will be empty",
                        file=sys.stderr,
                    )
                return

            if args.all_attributes:
                # Add resolved owner name to entries if --show-owner is enabled
                if args.show_owner:
                    for entry in matching_files:
                        owner_details = entry.get("owner_details", {})
                        owner_auth_id = owner_details.get("auth_id") or entry.get(
                            "owner"
                        )
                        if owner_auth_id and owner_auth_id in identity_cache_for_output:
                            identity = identity_cache_for_output[owner_auth_id]
                            entry["owner_name"] = format_owner_name(identity)
                        else:
                            entry["owner_name"] = "Unknown"

                # Write all attributes
                fieldnames = sorted(matching_files[0].keys())
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                for entry in matching_files:
                    writer.writerow(entry)
            else:
                # Write selective fields
                fieldnames = ["path"]

                # Add time field if time filter was used
                if args.older_than or args.newer_than:
                    fieldnames.append(args.time_field)

                # Add size if size filter was used
                if args.larger_than or args.smaller_than:
                    fieldnames.append("size")

                # Add owner if --show-owner is enabled
                if args.show_owner:
                    fieldnames.append("owner")

                # Add symlink_target if --resolve-links is enabled
                if args.resolve_links:
                    fieldnames.append("symlink_target")

                writer = csv.DictWriter(
                    csv_file, fieldnames=fieldnames, extrasaction="ignore"
                )
                writer.writeheader()
                for entry in matching_files:
                    row = {"path": entry["path"]}
                    if args.older_than or args.newer_than:
                        row[args.time_field] = entry.get(args.time_field)
                    if args.larger_than or args.smaller_than:
                        row["size"] = entry.get("size")
                    if args.show_owner:
                        owner_details = entry.get("owner_details", {})
                        owner_auth_id = owner_details.get("auth_id") or entry.get(
                            "owner"
                        )
                        if owner_auth_id and owner_auth_id in identity_cache_for_output:
                            identity = identity_cache_for_output[owner_auth_id]
                            row["owner"] = format_owner_name(identity)
                        else:
                            row["owner"] = "Unknown"
                    if args.resolve_links and "symlink_target" in entry:
                        row["symlink_target"] = entry["symlink_target"]
                    writer.writerow(row)

        if args.verbose:
            print(
                f"\n[INFO] Wrote {len(matching_files)} results to {args.csv_out}",
                file=sys.stderr,
            )
    elif args.json or args.json_out:
        # JSON output
        # Skip if batched_handler was used (already output via streaming)
        if batched_handler:
            pass  # Already handled by batched streaming
        else:
            output_handle = sys.stdout
            if args.json_out:
                output_handle = open(args.json_out, "w")

            for entry in matching_files:
                if args.all_attributes:
                    # Add resolved owner name to entry if --show-owner is enabled
                    if args.show_owner:
                        owner_details = entry.get("owner_details", {})
                        owner_auth_id = owner_details.get("auth_id") or entry.get(
                            "owner"
                        )
                        if owner_auth_id and owner_auth_id in identity_cache_for_output:
                            identity = identity_cache_for_output[owner_auth_id]
                            entry["owner_name"] = format_owner_name(identity)
                        else:
                            entry["owner_name"] = "Unknown"
                    output_handle.write(json_parser.dumps(entry) + "\n")
                else:
                    # Minimal output: path and filtered fields
                    minimal_entry = {"path": entry["path"]}
                    if args.older_than or args.newer_than:
                        minimal_entry[args.time_field] = entry.get(args.time_field)
                    if args.larger_than or args.smaller_than:
                        minimal_entry["size"] = entry.get("size")
                    if args.show_owner:
                        owner_details = entry.get("owner_details", {})
                        owner_auth_id = owner_details.get("auth_id") or entry.get(
                            "owner"
                        )
                        if owner_auth_id and owner_auth_id in identity_cache_for_output:
                            identity = identity_cache_for_output[owner_auth_id]
                            minimal_entry["owner"] = format_owner_name(identity)
                        else:
                            minimal_entry["owner"] = "Unknown"
                    if args.resolve_links and "symlink_target" in entry:
                        minimal_entry["symlink_target"] = entry["symlink_target"]
                    output_handle.write(json_parser.dumps(minimal_entry) + "\n")

            if args.json_out:
                output_handle.close()
                print(f"\n[INFO] Results written to {args.json_out}", file=sys.stderr)
    else:
        # Plain text output
        # Only output if we didn't use streaming callback (which already printed results)
        if output_callback is None:
            for entry in matching_files:
                output_line = entry["path"]

                # Add symlink target if --resolve-links is enabled and this is a symlink
                if args.resolve_links and "symlink_target" in entry:
                    output_line = f"{output_line} → {entry['symlink_target']}"

                # Add owner information if --show-owner is enabled
                if args.show_owner:
                    owner_details = entry.get("owner_details", {})
                    owner_auth_id = owner_details.get("auth_id") or entry.get("owner")
                    if owner_auth_id and owner_auth_id in identity_cache_for_output:
                        identity = identity_cache_for_output[owner_auth_id]
                        owner_name = format_owner_name(identity)
                        output_line = f"{output_line}\t{owner_name}"
                    else:
                        output_line = f"{output_line}\tUnknown"

                print(output_line)

    # Record output timing
    if profiler:
        output_time = time.time() - output_start
        profiler.record_sync("output_generation", output_time)

    # Summary
    if args.verbose:
        print(
            f"\n[INFO] Processed {progress.total_objects if progress else 'N/A'} objects in {elapsed:.2f}s",
            file=sys.stderr,
        )
        print(f"[INFO] Found {len(matching_files)} matching files", file=sys.stderr)
        rate = (
            (progress.total_objects if progress else len(matching_files)) / elapsed
            if elapsed > 0
            else 0
        )
        print(f"[INFO] Processing rate: {rate:.1f} obj/sec", file=sys.stderr)

    # Print profiling report
    if profiler:
        profiler.print_report(elapsed)

    # Save identity cache before exiting
    save_identity_cache(client.persistent_identity_cache, verbose=args.verbose)


def main():
    parser = argparse.ArgumentParser(
        description="Qumulo File Filter - Async Python implementation with aiohttp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find files older than 30 days
  ./qumulo_file_filter_async.py --host cluster.example.com --path /home --older-than 30

  # Find large files with progress tracking
  ./qumulo_file_filter_async.py --host cluster.example.com --path /data --larger-than 1GB --progress

  # Search for log files or temporary files (OR logic, glob wildcards)
  ./qumulo_file_filter_async.py --host cluster.example.com --path /var --name '*.log' --name '*.tmp'

  # Search for backup files from 2024 (AND logic)
  ./qumulo_file_filter_async.py --host cluster.example.com --path /backups --name-and '*backup*' --name-and '*2024*'

  # Find all Python test files (glob pattern)
  ./qumulo_file_filter_async.py --host cluster.example.com --path /code --name 'test_*.py' --type file

  # Find all directories starting with "temp" (regex pattern)
  ./qumulo_file_filter_async.py --host cluster.example.com --path /data --name '^temp.*' --type directory

  # Case-sensitive search for README files
  ./qumulo_file_filter_async.py --host cluster.example.com --path /docs --name '^README$' --name-case-sensitive

  # High-performance mode with increased concurrency
  ./qumulo_file_filter_async.py --host cluster.example.com --path /home --older-than 90 --max-concurrent 200 --connector-limit 200

  # Output to JSON file
  ./qumulo_file_filter_async.py --host cluster.example.com --path /home --older-than 30 --json-out results.json --all-attributes
        """,
    )

    # Required arguments
    parser.add_argument("--host", required=True, help="Qumulo cluster hostname or IP")
    parser.add_argument("--path", required=True, help="Path to search")

    # Time filters
    parser.add_argument("--older-than", type=int, help="Find files older than N days")
    parser.add_argument("--newer-than", type=int, help="Find files newer than N days")

    # Time field selection
    parser.add_argument(
        "--time-field",
        default="creation_time",
        choices=["creation_time", "modification_time", "access_time", "change_time"],
        help="Time field to filter on (default: creation_time)",
    )
    parser.add_argument(
        "--created",
        action="store_const",
        const="creation_time",
        dest="time_field",
        help="Filter by creation time",
    )
    parser.add_argument(
        "--modified",
        action="store_const",
        const="modification_time",
        dest="time_field",
        help="Filter by modification time",
    )
    parser.add_argument(
        "--accessed",
        action="store_const",
        const="access_time",
        dest="time_field",
        help="Filter by access time",
    )
    parser.add_argument(
        "--changed",
        action="store_const",
        const="change_time",
        dest="time_field",
        help="Filter by change time",
    )

    # Field-specific time filters (all use AND logic)
    parser.add_argument(
        "--accessed-older-than",
        type=int,
        help="Find files with access time older than N days",
    )
    parser.add_argument(
        "--accessed-newer-than",
        type=int,
        help="Find files with access time newer than N days",
    )
    parser.add_argument(
        "--modified-older-than",
        type=int,
        help="Find files with modification time older than N days",
    )
    parser.add_argument(
        "--modified-newer-than",
        type=int,
        help="Find files with modification time newer than N days",
    )
    parser.add_argument(
        "--created-older-than",
        type=int,
        help="Find files with creation time older than N days",
    )
    parser.add_argument(
        "--created-newer-than",
        type=int,
        help="Find files with creation time newer than N days",
    )
    parser.add_argument(
        "--changed-older-than",
        type=int,
        help="Find files with change time older than N days",
    )
    parser.add_argument(
        "--changed-newer-than",
        type=int,
        help="Find files with change time newer than N days",
    )

    # Size filters
    parser.add_argument(
        "--larger-than",
        help="Find files larger than specified size (e.g., 100MB, 1.5GiB)",
    )
    parser.add_argument("--smaller-than", help="Find files smaller than specified size")
    parser.add_argument(
        "--include-metadata",
        action="store_true",
        help="Include metadata blocks in size calculations (metablocks * 4KB)",
    )

    # Owner filters
    parser.add_argument(
        "--owner",
        action="append",
        dest="owners",
        help="Filter by file owner (can be specified multiple times for OR logic)",
    )
    parser.add_argument(
        "--ad", action="store_true", help="Owner(s) are Active Directory users"
    )
    parser.add_argument("--local", action="store_true", help="Owner(s) are local users")
    parser.add_argument(
        "--uid", action="store_true", help="Owner(s) are specified as UID numbers"
    )
    parser.add_argument(
        "--expand-identity",
        action="store_true",
        help="Match all equivalent identities (e.g., AD user + NFS UID)",
    )
    parser.add_argument(
        "--show-owner",
        action="store_true",
        help="Display owner information for matching files",
    )
    parser.add_argument(
        "--owner-report",
        action="store_true",
        help="Generate ownership report (file count and total bytes by owner)",
    )

    # Duplicate detection options
    parser.add_argument(
        "--find-duplicates",
        action="store_true",
        help="Find duplicate files using adaptive sampling strategy. "
             "Groups files by size+datablocks, then computes sample hashes.",
    )
    parser.add_argument(
        "--by-size",
        action="store_true",
        help="Use size-only duplicate detection (fast, may have false positives). "
             "Only valid with --find-duplicates.",
    )
    parser.add_argument(
        "--sample-points",
        type=int,
        choices=range(3, 12),
        metavar="N",
        help="Override number of sample points for duplicate detection (3-11). "
             "Default is adaptive based on file size.",
    )

    # Name search filters
    parser.add_argument(
        "--name",
        action="append",
        dest="name_patterns",
        help="Filter by name pattern (supports glob wildcards and regex, can be specified multiple times for OR logic). "
             "Glob examples: --name '*.log' --name 'test_*'. "
             "Regex examples: --name '.*\\.log$' --name '^test_.*'",
    )
    parser.add_argument(
        "--name-and",
        action="append",
        dest="name_patterns_and",
        help="Filter by name pattern using AND logic (all patterns must match, supports glob and regex). "
             "Examples: --name-and '*backup*' --name-and '*2024*'",
    )
    parser.add_argument(
        "--name-case-sensitive",
        action="store_true",
        help="Make name pattern matching case-sensitive (default: case-insensitive)",
    )
    parser.add_argument(
        "--type",
        choices=["file", "f", "directory", "dir", "d", "symlink", "link", "l"],
        help="Filter by object type: file/f, directory/dir/d, or symlink/link/l",
    )
    parser.add_argument(
        "--resolve-links",
        action="store_true",
        help="Resolve and display symlink targets (shows 'link → target' in output). "
             "Does not follow symlinks during traversal.",
    )

    parser.add_argument(
        "--use-capacity",
        action="store_true",
        default=True,
        help="Use actual disk capacity (datablocks + metablocks) instead of logical file size for owner reports (default: True). Handles sparse files correctly.",
    )
    parser.add_argument(
        "--report-logical-size",
        dest="use_capacity",
        action="store_false",
        help="Report logical file size instead of actual disk capacity in owner reports",
    )

    # Search options
    parser.add_argument(
        "--max-depth", type=int, help="Maximum directory depth to search"
    )
    parser.add_argument(
        "--file-only",
        action="store_true",
        help="Search files only (exclude directories)",
    )
    parser.add_argument(
        "--omit-subdirs",
        action="append",
        help="Omit subdirectories matching pattern (supports wildcards, can be specified multiple times)",
    )
    parser.add_argument(
        "--max-entries-per-dir",
        type=int,
        help="Skip directories with more than N entries (safety valve for large directories)",
    )
    parser.add_argument(
        "--show-dir-stats",
        action="store_true",
        help="Show directory statistics without enumerating files (exploration mode)",
    )

    # Output options
    parser.add_argument(
        "--json", action="store_true", help="Output results as JSON to stdout"
    )
    parser.add_argument("--json-out", help="Write JSON results to file")
    parser.add_argument(
        "--csv-out",
        help="Write results to CSV file (mutually exclusive with --json/--json-out)",
    )
    parser.add_argument(
        "--all-attributes",
        action="store_true",
        help="Include all file attributes in JSON output",
    )
    parser.add_argument("--verbose", action="store_true", help="Show detailed logging")
    parser.add_argument(
        "--progress", action="store_true", help="Show real-time progress stats"
    )
    parser.add_argument(
        "--limit", type=int, help="Stop after finding N matching results"
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable detailed performance profiling and timing metrics",
    )

    # Connection options
    parser.add_argument(
        "--port", type=int, default=8000, help="Qumulo API port (default: 8000)"
    )
    parser.add_argument(
        "--credentials-store", help="Path to credentials file (default: ~/.qfsd_cred)"
    )

    # Performance tuning
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=100,
        help="Maximum concurrent operations (default: 100)",
    )
    parser.add_argument(
        "--connector-limit",
        type=int,
        default=100,
        help="Maximum HTTP connections in pool (default: 100)",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.older_than and args.newer_than and args.newer_than >= args.older_than:
        print(
            "Error: --newer-than must be less than --older-than for a valid time range",
            file=sys.stderr,
        )
        sys.exit(1)

    # Check for mutually exclusive CSV and JSON output
    if args.csv_out and (args.json or args.json_out):
        print(
            "Error: --csv-out cannot be used with --json or --json-out", file=sys.stderr
        )
        print("Please choose either CSV or JSON output format", file=sys.stderr)
        sys.exit(1)

    # Run async main
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
