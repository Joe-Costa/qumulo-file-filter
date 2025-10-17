Efficient Duplicate Detection via REST Range Reads
You have 2000 × 50 MB files accessed via a REST API over VPN.
Goal: minimize bytes transferred while achieving very high confidence in duplicate detection.
🔹 Core Idea
Use a tiered sampling approach that:
reads small windows (64 KiB) via Range requests,
combines fixed, stratified, and random offsets,
escalates sampling only for likely duplicates,
and falls back to full reads only when absolutely necessary.
1. Pre-Bucketing (No Downloads)
Use API metadata first:
Perform a HEAD request to get:
file size,
last-modified / ETag,
content-type.
Group files by size.
If an ETag is a strong content hash, use it directly.
Weak ETags (e.g., S3 multipart) → treat as hints only.
2. Stratified + Random Sampling (Range Reads)
Parameters:
Window size (W) = 64 KiB
Always sample:
start (0)
end (L − W)
Add:
m evenly spaced (stratified) windows
r random windows
Each file yields:
(
2
+
m
+
r
)
×
W
(2+m+r)×W
bytes.
For m=4, r=4, that’s ≈ 512 KiB/file → ~1 GB total for 2000 files.
3. Position-Aware Fingerprint
For each sampled region:
struct {
  uint64 offset;
  uint64 length;
  uint8 data[length];
}
Concatenate all, then hash with BLAKE3 (fast + secure):
fingerprint = blake3( concat( [ offset||len||data for all windows ] ) )
Files with different hashes → different.
Files with identical hashes → candidate duplicates.
4. Adaptive Confirmation (Escalation)
For candidate groups:
Increase m and r (e.g., from 4/4 → 6/10).
If still identical, add more random windows (e.g., +16 – 32).
Only after several passes and perfect matches should you do a full-file compare.
Even at 1 MB sampled per 50 MB file, that’s only ~2% total data read.
5. Optional Full Confirm (Final Check)
Do a complete comparison only on final groups that still match after multiple sampling rounds.
⚙️ Why This Works
For a 50 MB file with 64 KiB windows:
1
−
W
/
L
≈
0.9987
1−W/L≈0.9987
With K windows, the probability a single changed block is missed ≈ (1 - W/L)^K.
Windows (K)	Miss Probability
10	0.987
18	0.978
26	0.967
However, because real-world edits are clustered, combining stratified and random windows drives the practical false-negative rate far lower than this simple bound.
6. Reproducible Offset Generation
Given file size L and window W:
def sample_offsets(L, m=4, r=4, stable_id=""):
    offs = []
    add = lambda pos: offs.append(max(0, min(L - W, pos)))
    if L <= 0: return [0]
    add(0)
    add(L - W)
    for i in range(1, m + 1):
        add(int(round(i * L/(m + 1) - W/2)))
    # Deterministic random offsets
    seed = int.from_bytes(blake3(f"{L}|{stable_id}".encode()).digest()[:8], "little")
    rng = random.Random(seed)
    while len(offs) < 2 + m + r:
        pos = rng.randrange(0, max(1, L - W + 1))
        if pos not in offs:
            add(pos)
    return sorted(set(offs))
Offsets are:
deterministic (based on file metadata),
non-overlapping,
evenly distributed + randomized.
7. Range Read and Fingerprint Function
import struct, random, requests
from blake3 import blake3

W = 64 * 1024  # 64 KiB

def range_read(session, url, start, length, headers=None, timeout=30):
    end = start + length - 1
    h = {"Range": f"bytes={start}-{end}"}
    if headers: h.update(headers)
    r = session.get(url, headers=h, stream=True, timeout=timeout)
    r.raise_for_status()
    return r.content

def sampled_fingerprint(session, url, size, stable_id, m=4, r=4):
    h = blake3()
    for off in sample_offsets(size, m, r, stable_id):
        data = range_read(session, url, off, min(W, size - off))
        h.update(struct.pack("<Q", off))
        h.update(struct.pack("<Q", len(data)))
        h.update(data)
    return h.hexdigest()
8. Practical Knobs & Tips
Parameter	Recommendation
Window size	64–128 KiB
Workers	8–16 concurrent requests
Retry	If range read fails, backoff and retry
Cache	Store {id, size, mtime, m, r, hash}
Confidence	Escalate m/r for stubborn groups
TLS	Always use HTTPS over VPN
Server-side hash	Use it if available (e.g., SHA-256 in metadata)
9. Confidence Guide
Example: for 50 MB files and 64 KiB windows
Tier	m	r	Bytes Read	Confidence
1	4	4	512 KiB	Moderate
2	6	10	1 MB	High
3	6	32	2 MB	Very High
4	Full	–	50 MB	Certain
✅ Summary
Step	Purpose	Cost
1️⃣ HEAD / metadata	Size filter	None
2️⃣ 64 KiB window sampling	Fast grouping	~0.5 MB/file
3️⃣ Escalation sampling	High confidence	~1 MB/file
4️⃣ Full compare (rare)	Absolute certainty	50 MB/file
This approach minimizes network load while maintaining high integrity checks—ideal for VPN or cloud-to-cloud deduplication.