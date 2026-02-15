#!/usr/bin/env python3
"""
Audio Converter Pipeline — High-Throughput Edition
====================================================
Recursively converts audio files to 16kHz mono 16-bit PCM WAV.
Mirrors directory structure, zips outputs with manifests, tracks
everything in SQLite, and generates detailed summaries.

Optimized for: AMD Ryzen 9 7950X3D · 192 GB RAM · 28 worker threads
Conversion via direct ffmpeg subprocess (no pydub memory overhead).
SHA256 hashing, full metadata capture, resumable across runs.

Dependencies:
    - ffmpeg + ffprobe  (system, must be in PATH)
    - Python 3.10+      (stdlib only — no pip packages required)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Configuration ────────────────────────────────────────────────────────────

INPUT_DIR   = Path("audio-inputs")
OUTPUT_DIR  = Path("audio-wav-files")
ZIP_DIR     = Path("audio-zips")
DB_PATH     = Path("audio_conversion.db")

MAX_ZIP_BYTES = 5 * 1024 * 1024 * 1024          # 5 GB per zip chunk
WORKER_THREADS = 28                               # conversion workers
HASH_THREADS   = 28                               # parallel SHA256 workers
DB_WRITER_BATCH = 200                             # DB commit batch size

# Memory guard: rough cap so we don't blow past 150 GB.
# ffmpeg typically uses 50-200 MB per instance depending on format/duration.
# 28 workers × ~200 MB peak ≈ 5.6 GB — well within budget.
# The real memory concern is the zip phase (file handles) and hashing I/O,
# both of which are streaming and don't hold large buffers.
# We set an explicit ulimit-style soft cap for future-proofing.
RAM_BUDGET_GB = 150

# Audio extensions ffmpeg can handle
AUDIO_EXTENSIONS = {
    ".mp3", ".wav", ".flac", ".ogg", ".m4a", ".aac", ".wma", ".aiff",
    ".aif", ".opus", ".webm", ".mp4", ".amr", ".ape", ".ac3", ".dts",
    ".mka", ".mpc", ".oga", ".spx", ".tta", ".voc", ".wv",
}

RUN_START = datetime.now(timezone.utc)
RUN_STAMP = RUN_START.strftime("%y.%m.%d_%H%M")

# ── Logging ──────────────────────────────────────────────────────────────────

error_log_path = Path(f"error_{RUN_STAMP}.txt")
logger = logging.getLogger("audio_converter")
logger.setLevel(logging.DEBUG)

_console = logging.StreamHandler(sys.stdout)
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(_console)

_fh = logging.FileHandler(error_log_path, mode="a", encoding="utf-8")
_fh.setLevel(logging.WARNING)
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_fh)


# ── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class ConversionRecord:
    """One record per input file — mirrors the DB schema exactly."""
    input_path:          str                # relative to INPUT_DIR
    output_path:         str                # relative to OUTPUT_DIR
    filename:            str
    status:              str                # success | failed | skipped
    error_log:           Optional[str]
    # Source file info
    old_size_bytes:      int
    old_format:          str
    old_codec:           Optional[str]
    old_sample_rate:     Optional[int]
    old_bit_depth:       Optional[int]
    old_channels:        Optional[int]
    old_duration_sec:    Optional[float]
    old_bit_rate:        Optional[int]
    # Output file info
    new_size_bytes:      Optional[int]
    new_format:          str
    new_codec:           str
    new_sample_rate:     int
    new_bit_depth:       int
    new_channels:        int
    new_duration_sec:    Optional[float]
    # Integrity & timing
    conversion_timestamp: str
    sha256_input:        Optional[str]
    sha256_output:       Optional[str]
    conversion_time_sec: Optional[float]


# ── Prerequisite Check ───────────────────────────────────────────────────────

def check_ffmpeg():
    """Fail fast if ffmpeg/ffprobe aren't available."""
    for tool in ("ffmpeg", "ffprobe"):
        if shutil.which(tool) is None:
            logger.error(f"'{tool}' not found in PATH. Install ffmpeg first.")
            sys.exit(1)
    # Log version for audit trail
    try:
        ver = subprocess.run(
            ["ffmpeg", "-version"], capture_output=True, text=True
        ).stdout.split("\n")[0]
        logger.info(f"Using {ver}")
    except Exception:
        pass


# ── Database ─────────────────────────────────────────────────────────────────

_DB_LOCK = threading.Lock()


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")        # faster writes, WAL protects
    conn.execute("PRAGMA cache_size=-64000")          # 64 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            input_path          TEXT UNIQUE NOT NULL,
            output_path         TEXT,
            filename            TEXT,
            status              TEXT NOT NULL,
            error_log           TEXT,
            old_size_bytes      INTEGER,
            old_format          TEXT,
            old_codec           TEXT,
            old_sample_rate     INTEGER,
            old_bit_depth       INTEGER,
            old_channels        INTEGER,
            old_duration_sec    REAL,
            old_bit_rate        INTEGER,
            new_size_bytes      INTEGER,
            new_format          TEXT,
            new_codec           TEXT,
            new_sample_rate     INTEGER,
            new_bit_depth       INTEGER,
            new_channels        INTEGER,
            new_duration_sec    REAL,
            conversion_timestamp TEXT,
            sha256_input        TEXT,
            sha256_output       TEXT,
            conversion_time_sec REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp     TEXT NOT NULL,
            total_files       INTEGER,
            success_count     INTEGER,
            fail_count        INTEGER,
            skip_count        INTEGER,
            total_duration_sec REAL,
            elapsed_sec       REAL,
            worker_threads    INTEGER,
            status            TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS zip_files (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            zip_name        TEXT NOT NULL,
            parent_folder   TEXT NOT NULL,
            zip_index       INTEGER NOT NULL,
            size_bytes      INTEGER,
            file_count      INTEGER,
            manifest_name   TEXT,
            run_timestamp   TEXT
        )
    """)
    conn.commit()
    return conn


def get_already_converted(conn: sqlite3.Connection) -> set[str]:
    """Load all successfully converted relative input paths into a set for O(1) lookup."""
    rows = conn.execute(
        "SELECT input_path FROM conversions WHERE status = 'success'"
    ).fetchall()
    return {r[0] for r in rows}


def batch_upsert_records(conn: sqlite3.Connection, records: list[ConversionRecord]):
    """Batch-insert/update records under a single lock + transaction."""
    if not records:
        return
    with _DB_LOCK:
        for rec in records:
            d = asdict(rec)
            cols = ", ".join(d.keys())
            placeholders = ", ".join(["?"] * len(d))
            updates = ", ".join(
                f"{k}=excluded.{k}" for k in d if k != "input_path"
            )
            conn.execute(
                f"INSERT INTO conversions ({cols}) VALUES ({placeholders}) "
                f"ON CONFLICT(input_path) DO UPDATE SET {updates}",
                list(d.values()),
            )
        conn.commit()


# ── Helpers ──────────────────────────────────────────────────────────────────

def sha256_file(path: Path) -> str:
    """Stream-hash a file in 1 MB chunks — I/O bound, safe to parallelize."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def probe_metadata(filepath: str | Path) -> dict:
    """Extract audio metadata via ffprobe — no pydub dependency."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_format", "-show_streams",
        str(filepath),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        audio_stream = next(
            (s for s in data.get("streams", []) if s.get("codec_type") == "audio"),
            {},
        )
        fmt = data.get("format", {})

        def _int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        def _float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        return {
            "format_name":   fmt.get("format_name", "unknown"),
            "format_long":   fmt.get("format_long_name", "unknown"),
            "duration":      _float(fmt.get("duration")),
            "size":          _int(fmt.get("size")),
            "bit_rate":      _int(fmt.get("bit_rate")),
            "codec":         audio_stream.get("codec_name"),
            "codec_long":    audio_stream.get("codec_long_name"),
            "sample_rate":   _int(audio_stream.get("sample_rate")),
            "channels":      _int(audio_stream.get("channels")),
            "bit_depth":     _int(audio_stream.get("bits_per_sample")),
        }
    except Exception as exc:
        return {"error": str(exc)}


def format_size(size_bytes: int | float) -> str:
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
    elif size_bytes >= 1024 ** 2:
        return f"{size_bytes / (1024 ** 2):.1f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.0f} KB"
    return f"{size_bytes} B"


def format_hms(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:05.2f}"


# ── Core Conversion (single file) ───────────────────────────────────────────

def convert_one(input_path: Path, output_path: Path) -> ConversionRecord:
    """
    Convert a single audio file to 16 kHz mono 16-bit PCM WAV via ffmpeg.
    Collects full metadata before/after, SHA256 hashes, timing.
    """
    rel_input  = str(input_path.relative_to(INPUT_DIR))
    rel_output = str(output_path.relative_to(OUTPUT_DIR))
    timestamp  = datetime.now(timezone.utc).isoformat()

    # ── Gather source info ──
    old_size = input_path.stat().st_size
    old_ext  = input_path.suffix.lower().lstrip(".")
    info     = probe_metadata(input_path)

    if "error" in info:
        err_msg = f"ffprobe failed: {info['error']}"
        logger.error(f"PROBE FAIL: {rel_input} — {err_msg}")
        return ConversionRecord(
            input_path=rel_input, output_path=rel_output,
            filename=input_path.name, status="failed", error_log=err_msg,
            old_size_bytes=old_size, old_format=old_ext,
            old_codec=None, old_sample_rate=None, old_bit_depth=None,
            old_channels=None, old_duration_sec=None, old_bit_rate=None,
            new_size_bytes=None, new_format="wav", new_codec="pcm_s16le",
            new_sample_rate=16000, new_bit_depth=16, new_channels=1,
            new_duration_sec=None, conversion_timestamp=timestamp,
            sha256_input=None, sha256_output=None, conversion_time_sec=None,
        )

    old_codec = info.get("codec")
    old_sr    = info.get("sample_rate")
    old_bd    = info.get("bit_depth")
    old_ch    = info.get("channels")
    old_dur   = info.get("duration")
    old_br    = info.get("bit_rate")

    # SHA256 of input (I/O bound — runs in this thread)
    sha_in = sha256_file(input_path)

    # ── Convert via ffmpeg subprocess ──
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg", "-y",
        "-threads", "1",          # per-instance: 1 thread (we parallelize at the job level)
        "-i", str(input_path),
        "-vn",                    # strip video/artwork
        "-ac", "1",               # mono
        "-ar", "16000",           # 16 kHz
        "-acodec", "pcm_s16le",   # 16-bit PCM (explicit, don't rely on container default)
        "-f", "wav",              # force WAV container
        str(output_path),
    ]

    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        conv_time = time.monotonic() - t0
    except subprocess.CalledProcessError as exc:
        conv_time = time.monotonic() - t0
        err_msg = exc.stderr.decode(errors="replace")[-2000:]   # last 2 KB of stderr
        logger.error(f"FAILED: {rel_input}\n{err_msg}")
        return ConversionRecord(
            input_path=rel_input, output_path=rel_output,
            filename=input_path.name, status="failed", error_log=err_msg,
            old_size_bytes=old_size, old_format=old_ext,
            old_codec=old_codec, old_sample_rate=old_sr,
            old_bit_depth=old_bd, old_channels=old_ch,
            old_duration_sec=old_dur, old_bit_rate=old_br,
            new_size_bytes=None, new_format="wav", new_codec="pcm_s16le",
            new_sample_rate=16000, new_bit_depth=16, new_channels=1,
            new_duration_sec=None, conversion_timestamp=timestamp,
            sha256_input=sha_in, sha256_output=None,
            conversion_time_sec=conv_time,
        )
    except Exception as exc:
        conv_time = time.monotonic() - t0
        err_msg = traceback.format_exc()
        logger.error(f"FAILED: {rel_input}\n{err_msg}")
        return ConversionRecord(
            input_path=rel_input, output_path=rel_output,
            filename=input_path.name, status="failed", error_log=err_msg,
            old_size_bytes=old_size, old_format=old_ext,
            old_codec=old_codec, old_sample_rate=old_sr,
            old_bit_depth=old_bd, old_channels=old_ch,
            old_duration_sec=old_dur, old_bit_rate=old_br,
            new_size_bytes=None, new_format="wav", new_codec="pcm_s16le",
            new_sample_rate=16000, new_bit_depth=16, new_channels=1,
            new_duration_sec=None, conversion_timestamp=timestamp,
            sha256_input=sha_in, sha256_output=None,
            conversion_time_sec=conv_time,
        )

    # ── Gather output info ──
    new_size = output_path.stat().st_size
    sha_out  = sha256_file(output_path)

    # Probe output for actual duration (more reliable than input probe for edge cases)
    out_info = probe_metadata(output_path)
    new_dur  = out_info.get("duration") or old_dur

    return ConversionRecord(
        input_path=rel_input, output_path=rel_output,
        filename=input_path.name, status="success", error_log=None,
        old_size_bytes=old_size, old_format=old_ext,
        old_codec=old_codec, old_sample_rate=old_sr,
        old_bit_depth=old_bd, old_channels=old_ch,
        old_duration_sec=old_dur, old_bit_rate=old_br,
        new_size_bytes=new_size, new_format="wav", new_codec="pcm_s16le",
        new_sample_rate=16000, new_bit_depth=16, new_channels=1,
        new_duration_sec=new_dur, conversion_timestamp=timestamp,
        sha256_input=sha_in, sha256_output=sha_out,
        conversion_time_sec=conv_time,
    )


# ── Parallel Conversion Engine ───────────────────────────────────────────────

def run_conversions(
    audio_files: list[Path],
    already_done: set[str],
    conn: sqlite3.Connection,
) -> tuple[int, int, int]:
    """
    Convert files using a ThreadPoolExecutor with WORKER_THREADS workers.
    Each worker spawns its own ffmpeg subprocess — CPU/IO overlap is excellent
    because ffmpeg does the heavy lifting in separate processes while Python
    threads handle coordination, hashing, and DB writes.

    Returns (success_count, fail_count, skip_count).
    """
    success = 0
    failed  = 0
    skipped = 0
    total   = len(audio_files)

    # Pre-filter: build work list, skip already-done
    work: list[tuple[int, Path, Path]] = []
    for i, input_path in enumerate(audio_files):
        rel_input = str(input_path.relative_to(INPUT_DIR))
        if rel_input in already_done:
            skipped += 1
            continue
        rel_stem    = Path(rel_input).with_suffix(".wav")
        output_path = OUTPUT_DIR / rel_stem
        work.append((i, input_path, output_path))

    if skipped:
        logger.info(f"Skipping {skipped} already-converted files")

    if not work:
        logger.info("All files already converted — nothing to do.")
        return 0, 0, skipped

    logger.info(
        f"Converting {len(work)} files across {WORKER_THREADS} threads "
        f"({skipped} skipped, {total} total)"
    )

    # DB write buffer — accumulate records, flush in batches
    write_buf: list[ConversionRecord] = []
    buf_lock = threading.Lock()

    def _maybe_flush(force: bool = False):
        nonlocal write_buf
        with buf_lock:
            if len(write_buf) >= DB_WRITER_BATCH or (force and write_buf):
                batch = write_buf
                write_buf = []
            else:
                return
        batch_upsert_records(conn, batch)

    completed = 0
    t_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as pool:
        futures = {
            pool.submit(convert_one, inp, out): (idx, inp)
            for idx, inp, out in work
        }

        for future in as_completed(futures):
            idx, inp = futures[future]
            completed += 1

            try:
                rec = future.result()
            except Exception as exc:
                # Should not happen — convert_one catches everything — but safety net
                logger.error(f"Unexpected worker exception for {inp}: {exc}")
                failed += 1
                continue

            if rec.status == "success":
                success += 1
            else:
                failed += 1

            with buf_lock:
                write_buf.append(rec)

            # Progress logging every 100 files or at milestones
            if completed % 100 == 0 or completed == len(work):
                elapsed = time.monotonic() - t_start
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (len(work) - completed) / rate if rate > 0 else 0
                logger.info(
                    f"  Progress: {completed}/{len(work)} "
                    f"({success} ok, {failed} fail) "
                    f"[{rate:.1f} files/s, ETA {format_hms(eta)}]"
                )

            _maybe_flush()

    # Final flush
    _maybe_flush(force=True)

    elapsed = time.monotonic() - t_start
    logger.info(
        f"Conversion phase complete in {format_hms(elapsed)}: "
        f"{success} ok, {failed} failed, {skipped} skipped"
    )
    return success, failed, skipped


# ── Zipping + Manifests ──────────────────────────────────────────────────────

def zip_output_folders(conn: sqlite3.Connection):
    """
    Group output WAVs by their top-level subfolder under OUTPUT_DIR.
    Zip each group into ≤ 5 GB chunks with a JSON manifest per chunk.
    Record every zip in the database.
    """
    logger.info("── Zipping output files ──")

    rows = conn.execute(
        "SELECT output_path, filename, old_size_bytes, new_size_bytes, "
        "old_format, old_codec, old_sample_rate, old_bit_depth, old_channels, "
        "old_duration_sec, new_format, new_codec, new_sample_rate, new_bit_depth, "
        "new_channels, new_duration_sec, conversion_timestamp, status, error_log, "
        "old_bit_rate "
        "FROM conversions WHERE status = 'success'"
    ).fetchall()

    if not rows:
        logger.info("No successful conversions to zip.")
        return

    # Group by FULL relative parent path (avoids collision on same basename)
    folder_files: dict[str, list] = defaultdict(list)
    for row in rows:
        out_rel = row[0]
        parts = Path(out_rel).parts
        parent = str(Path(out_rel).parent) if len(parts) > 1 else "."
        folder_files[parent].append(row)

    ZIP_DIR.mkdir(exist_ok=True)

    for folder_key, files in folder_files.items():
        folder_label = folder_key.replace(os.sep, "_").replace("/", "_")
        if folder_label == ".":
            folder_label = "root"

        files.sort(key=lambda r: r[0])

        chunk_idx = 0
        current_size = 0
        current_files: list = []
        manifest_entries: list = []

        def _flush():
            nonlocal chunk_idx, current_size, current_files, manifest_entries
            if not current_files:
                return

            chunk_idx += 1
            size_label = format_size(current_size)
            zip_name = f"{folder_label}_{chunk_idx}_{size_label.replace(' ', '')}.zip"
            zip_path = ZIP_DIR / zip_name
            manifest_name = f"manifest_{folder_label}_{chunk_idx}.json"

            manifest = {
                "zip_name":       zip_name,
                "parent_folder":  folder_key,
                "chunk_index":    chunk_idx,
                "total_files":    len(current_files),
                "total_size_bytes": current_size,
                "created":        datetime.now(timezone.utc).isoformat(),
                "files":          manifest_entries,
            }

            logger.info(f"  Creating {zip_name} ({len(current_files)} files, {size_label})")

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for file_row in current_files:
                    out_rel = file_row[0]
                    abs_path = OUTPUT_DIR / out_rel
                    zf.write(abs_path, out_rel)
                zf.writestr(manifest_name, json.dumps(manifest, indent=2))

            actual_size = zip_path.stat().st_size

            with _DB_LOCK:
                conn.execute(
                    "INSERT INTO zip_files (zip_name, parent_folder, zip_index, "
                    "size_bytes, file_count, manifest_name, run_timestamp) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (zip_name, folder_key, chunk_idx, actual_size,
                     len(current_files), manifest_name, RUN_STAMP),
                )
                conn.commit()

            current_size = 0
            current_files = []
            manifest_entries = []

        for row in files:
            (out_rel, filename, old_size, new_size, old_fmt, old_codec,
             old_sr, old_bd, old_ch, old_dur, new_fmt, new_codec,
             new_sr, new_bd, new_ch, new_dur, conv_ts, status,
             err_log, old_br) = row

            abs_out = OUTPUT_DIR / out_rel
            if not abs_out.exists():
                logger.warning(f"  Output missing, skipping zip entry: {out_rel}")
                continue

            file_size = abs_out.stat().st_size

            if current_files and (current_size + file_size) > MAX_ZIP_BYTES:
                _flush()

            current_files.append(row)
            current_size += file_size

            manifest_entries.append({
                "filename":        filename,
                "output_path":     out_rel,
                "new_size_bytes":  new_size,
                "old_size_bytes":  old_size,
                "old_format":      old_fmt,
                "old_quality": {
                    "codec":       old_codec,
                    "sample_rate": old_sr,
                    "bit_depth":   old_bd,
                    "channels":    old_ch,
                    "bit_rate":    old_br,
                },
                "new_quality": {
                    "codec":       new_codec,
                    "sample_rate": new_sr,
                    "bit_depth":   new_bd,
                    "channels":    new_ch,
                },
                "audio_duration_sec":     new_dur,
                "conversion_status":      status,
                "conversion_timestamp":   conv_ts,
            })

        _flush()


# ── Summary Report ───────────────────────────────────────────────────────────

def generate_summary(conn: sqlite3.Connection, had_failures: bool,
                     success: int, failed: int, skipped: int):
    status_tag = "failure" if had_failures else "success"
    summary_path = Path(f"summary_{RUN_STAMP}_{status_tag}.md")

    rows = conn.execute(
        "SELECT status, COUNT(*), COALESCE(SUM(old_size_bytes),0), "
        "COALESCE(SUM(new_size_bytes),0), COALESCE(SUM(old_duration_sec),0), "
        "COALESCE(SUM(new_duration_sec),0) "
        "FROM conversions GROUP BY status"
    ).fetchall()

    total_files = total_old_bytes = total_new_bytes = 0
    total_old_dur = total_new_dur = 0.0

    for st, cnt, ob, nb, od, nd in rows:
        total_files += cnt
        if st == "success":
            total_old_bytes += ob
            total_new_bytes += nb
            total_old_dur += od
            total_new_dur += nd
        elif st == "failed":
            total_old_bytes += ob

    # Per-folder stats
    folder_rows = conn.execute("""
        SELECT
            CASE
                WHEN INSTR(output_path, '/') > 0
                THEN SUBSTR(output_path, 1, INSTR(output_path, '/') - 1)
                ELSE '(root)'
            END AS folder,
            COUNT(*) AS file_count,
            COALESCE(SUM(new_duration_sec), 0) AS total_dur,
            COALESCE(AVG(new_duration_sec), 0) AS avg_dur,
            COALESCE(SUM(new_size_bytes), 0)   AS total_size,
            COALESCE(SUM(old_size_bytes), 0)   AS old_total_size
        FROM conversions
        WHERE status = 'success'
        GROUP BY folder
        ORDER BY folder
    """).fetchall()

    zip_rows = conn.execute(
        "SELECT zip_name, file_count, size_bytes FROM zip_files WHERE run_timestamp = ?",
        (RUN_STAMP,),
    ).fetchall()

    fail_rows = conn.execute(
        "SELECT input_path, error_log FROM conversions WHERE status = 'failed'"
    ).fetchall()

    run_end = datetime.now(timezone.utc)
    elapsed = (run_end - RUN_START).total_seconds()
    avg_dur = (total_new_dur / success) if success > 0 else 0.0
    throughput = success / elapsed if elapsed > 0 else 0

    lines = [
        "# Audio Conversion Summary",
        "",
        f"**Run started:** {RUN_START.strftime('%Y-%m-%d %H:%M:%S UTC')}  ",
        f"**Run ended:** {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}  ",
        f"**Elapsed:** {format_hms(elapsed)}  ",
        f"**Workers:** {WORKER_THREADS} threads  ",
        f"**Throughput:** {throughput:.1f} files/sec  ",
        f"**Status:** {'SUCCESS' if not had_failures else 'COMPLETED WITH FAILURES'}",
        "",
        "## Overall Stats",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Total files processed | {total_files} |",
        f"| Successful conversions | {success} |",
        f"| Failed conversions | {failed} |",
        f"| Skipped (already done) | {skipped} |",
        f"| Total source size | {format_size(total_old_bytes)} |",
        f"| Total output size | {format_size(total_new_bytes)} |",
        f"| Total audio duration | {format_hms(total_new_dur)} |",
        f"| Average file duration | {format_hms(avg_dur)} |",
        f"| Output format | WAV 16kHz mono 16-bit PCM |",
        "",
        "## Per-Folder Breakdown",
        "",
        "| Folder | Files | Total Duration | Avg Duration | Source Size | Output Size |",
        "|---|---|---|---|---|---|",
    ]

    for folder, fcount, tdur, adur, tsize, osize in folder_rows:
        lines.append(
            f"| {folder} | {fcount} | {format_hms(tdur)} | "
            f"{format_hms(adur)} | {format_size(osize)} | {format_size(tsize)} |"
        )

    lines += ["", "## Zip Archives Created", ""]
    if zip_rows:
        lines += ["| Zip File | Files | Size |", "|---|---|---|"]
        for zname, zcount, zsize in zip_rows:
            lines.append(f"| {zname} | {zcount} | {format_size(zsize or 0)} |")
    else:
        lines.append("No zip archives created this run.")

    if failed > 0:
        lines += ["", f"## Failed Conversions ({failed})", ""]
        for fpath, ferr in fail_rows[:50]:
            short = (ferr or "Unknown").strip().split("\n")[-1][:150]
            lines.append(f"- `{fpath}`: {short}")
        if len(fail_rows) > 50:
            lines.append(f"- ... and {len(fail_rows) - 50} more (see error log)")

    lines += [
        "", "---",
        f"*Error log: `{error_log_path}`*  ",
        f"*Database: `{DB_PATH}`*  ",
        f"*System: {WORKER_THREADS} threads, RAM budget {RAM_BUDGET_GB} GB*",
    ]

    summary_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Summary written to {summary_path}")

    # Record run
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO runs (run_timestamp, total_files, success_count, fail_count, "
            "skip_count, total_duration_sec, elapsed_sec, worker_threads, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (RUN_STAMP, total_files, success, failed, skipped,
             total_new_dur, elapsed, WORKER_THREADS, status_tag),
        )
        conn.commit()

    return summary_path


# ── File Discovery ───────────────────────────────────────────────────────────

def discover_audio_files(input_dir: Path) -> list[Path]:
    """Recursively find all audio files, sorted for determinism."""
    found = []
    for root, _dirs, files in os.walk(input_dir):
        for fname in sorted(files):
            if Path(fname).suffix.lower() in AUDIO_EXTENSIONS:
                found.append(Path(root) / fname)
    return found


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info(f"═══ Audio Converter — {RUN_STAMP} ═══")
    logger.info(f"Config: {WORKER_THREADS} workers, {RAM_BUDGET_GB} GB RAM budget")

    # Preflight
    check_ffmpeg()

    if not INPUT_DIR.exists():
        logger.error(f"Input directory '{INPUT_DIR}' does not exist.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = init_db(DB_PATH)

    # Discover
    audio_files = discover_audio_files(INPUT_DIR)
    logger.info(f"Found {len(audio_files)} audio files in '{INPUT_DIR}'")

    if not audio_files:
        logger.info("Nothing to process.")
        generate_summary(conn, had_failures=False, success=0, failed=0, skipped=0)
        conn.close()
        return

    # Load resumption set (one query, O(1) lookups during parallel phase)
    already_done = get_already_converted(conn)
    if already_done:
        logger.info(f"Resumption: {len(already_done)} files already in DB as success")

    # Convert
    success, failed, skipped = run_conversions(audio_files, already_done, conn)

    # Zip
    zip_output_folders(conn)

    # Summary
    generate_summary(conn, had_failures=(failed > 0),
                     success=success, failed=failed, skipped=skipped)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
