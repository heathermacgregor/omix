"""
Cache management for ENA metadata with a multi-level, batched-write SQLite backend.

Features:
- Per-accession caching (samples, runs, experiments)
- Study-level caching (projects, studies)
- Bulk query results caching
- Comprehensive metrics tracking
- Batched writes to reduce SQLite lock contention
"""

import asyncio
import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, cast

try:
    import pandas as pd
except ImportError:
    pd = None

from omix.logging_utils import get_logger

JSONPrimitive = str | int | float | bool | None
JSONValue = JSONPrimitive | Dict[str, "JSONValue"] | List["JSONValue"]


@dataclass
class WriteOp:
    """Represents a single cache write operation."""
    table: str          # 'cache', 'study_cache', or 'bulk_query_cache'
    key: str
    data_json: str
    timestamp: float
    query_hash: Optional[str] = None      # For bulk_query_cache
    result_count: Optional[int] = None    # For bulk_query_cache


class WriteQueue:
    """Accumulates cache writes and flushes them in batched transactions."""

    def __init__(self, batch_size: int = 100, flush_interval_seconds: float = 5.0):
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.queue: List[WriteOp] = []
        self.lock = asyncio.Lock()
        self.last_flush_time = time.time()
        self.metrics = {"writes_queued": 0, "flushes": 0, "writes_committed": 0}

    async def enqueue(self, op: WriteOp) -> None:
        """Add a write operation to the queue."""
        async with self.lock:
            self.queue.append(op)
            self.metrics["writes_queued"] += 1

    async def should_flush(self) -> bool:
        """Check if queue should be flushed (size or time threshold exceeded)."""
        async with self.lock:
            return (len(self.queue) >= self.batch_size
                    or time.time() - self.last_flush_time > self.flush_interval_seconds)

    async def get_pending_count(self) -> int:
        """Return number of pending writes."""
        async with self.lock:
            return len(self.queue)

    async def flush(self, conn: sqlite3.Connection) -> int:
        """Flush all pending writes to the database in a single transaction.

        Returns:
            Number of writes committed.
        """
        async with self.lock:
            if not self.queue:
                return 0

            num_writes = len(self.queue)
            ops_by_table: Dict[str, List[WriteOp]] = {
                "cache": [], "study_cache": [], "bulk_query_cache": []
            }
            for op in self.queue:
                ops_by_table[op.table].append(op)

            try:
                with conn:
                    for op in ops_by_table["cache"]:
                        conn.execute(
                            "INSERT OR REPLACE INTO cache (key, data, timestamp) VALUES (?, ?, ?)",
                            (op.key, op.data_json, op.timestamp)
                        )
                    for op in ops_by_table["study_cache"]:
                        conn.execute(
                            "INSERT OR REPLACE INTO study_cache (study_accession, data, timestamp) VALUES (?, ?, ?)",
                            (op.key, op.data_json, op.timestamp)
                        )
                    for op in ops_by_table["bulk_query_cache"]:
                        conn.execute(
                            "INSERT OR REPLACE INTO bulk_query_cache (query_hash, result_count, data, timestamp) VALUES (?, ?, ?, ?)",
                            (op.query_hash, op.result_count, op.data_json, op.timestamp)
                        )

                self.queue.clear()
                self.last_flush_time = time.time()
                self.metrics["flushes"] += 1
                self.metrics["writes_committed"] += num_writes
                return num_writes
            except Exception as e:
                raise Exception(f"Failed to flush {num_writes} writes: {e}") from e

    def get_metrics(self) -> Dict[str, int]:
        """Return queue metrics."""
        return self.metrics.copy()


class SQLiteCacheManager:
    """Multi-level cache manager backed by SQLite with batched writes."""

    def __init__(
        self,
        cache_dir: Path,
        db_name: str = "ena_cache.db",
        ttl_seconds: int = 604800,               # 7 days default
        db_write_batch_size: int = 100,
        db_write_interval_seconds: float = 5.0,
    ):
        self.cache_dir = cache_dir
        self.db_path = self.cache_dir / db_name
        self.logger = get_logger("omix.ena.cache")
        self.ttl = ttl_seconds
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._write_lock = asyncio.Lock()
        self.write_queue = WriteQueue(
            batch_size=db_write_batch_size,
            flush_interval_seconds=db_write_interval_seconds
        )

        # Per-thread SQLite connections (thread-safe)
        self._thread_local = threading.local()

        # Metrics
        self.metrics = {
            "accession_hits": 0, "accession_misses": 0,
            "study_hits": 0, "study_misses": 0,
            "bulk_hits": 0, "bulk_misses": 0,
            "total_queries": 0,
        }
        self._init_db()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create a per-thread SQLite connection."""
        if not hasattr(self._thread_local, 'conn') or self._thread_local.conn is None:
            self._thread_local.conn = sqlite3.connect(
                self.db_path, timeout=60.0, isolation_level=None,
                check_same_thread=False
            )
            self._thread_local.conn.execute("PRAGMA journal_mode=WAL")
            self._thread_local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._thread_local.conn

    def _init_db(self) -> None:
        """Create cache tables if they don't exist."""
        conn = self._get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                data TEXT,
                timestamp REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS study_cache (
                study_accession TEXT PRIMARY KEY,
                data TEXT,
                timestamp REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bulk_query_cache (
                query_hash TEXT PRIMARY KEY,
                result_count INTEGER,
                data TEXT,
                timestamp REAL
            )
        """)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

    # ------------------------------------------------------------------
    # Core get/set (accession-level)
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[JSONValue]:
        """Retrieve a cached value by key, respecting TTL."""
        if await self.write_queue.get_pending_count() > 0:
            await self.bulk_commit()

        def _get_sync() -> Optional[JSONValue]:
            conn = self._get_connection()
            row = conn.execute(
                "SELECT data, timestamp FROM cache WHERE key = ?", (key,)
            ).fetchone()
            if row:
                data_str, timestamp = row
                if time.time() - timestamp > self.ttl:
                    return None
                self.metrics["accession_hits"] += 1
                return cast(JSONValue, json.loads(data_str))
            self.metrics["accession_misses"] += 1
            return None

        self.metrics["total_queries"] += 1
        return await asyncio.to_thread(_get_sync)

    async def get_bulk(self, keys: List[str]) -> Dict[str, JSONValue]:
        """Fetch multiple keys at once (batched SQL query)."""
        if await self.write_queue.get_pending_count() > 0:
            await self.bulk_commit()

        def _get_bulk_sync() -> Dict[str, JSONValue]:
            if not keys:
                return {}
            conn = self._get_connection()
            current_time = time.time()
            result = {}
            # Process in chunks to respect SQLite parameter limits
            batch_size = 500
            for i in range(0, len(keys), batch_size):
                batch = keys[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                query = f"SELECT key, data, timestamp FROM cache WHERE key IN ({placeholders})"
                for key, data_str, timestamp in conn.execute(query, batch).fetchall():
                    if current_time - timestamp > self.ttl:
                        continue
                    try:
                        result[key] = cast(JSONValue, json.loads(data_str))
                    except (json.JSONDecodeError, ValueError):
                        continue
            return result

        return await asyncio.to_thread(_get_bulk_sync)

    async def set(self, key: str, data: JSONValue) -> None:
        """Enqueue a write (does not commit immediately)."""
        if data is None:
            return

        # Convert pandas objects to serializable types
        serializable = data
        try:
            if pd is not None:
                if isinstance(data, pd.DataFrame):
                    serializable = data.to_dict(orient='records')
                elif isinstance(data, pd.Series):
                    serializable = data.to_dict()
        except Exception:
            serializable = data

        try:
            data_json = json.dumps(serializable)
        except TypeError:
            try:
                data_json = json.dumps(str(serializable))
            except Exception:
                data_json = json.dumps("null")

        now = time.time()
        op = WriteOp(table="cache", key=key, data_json=data_json, timestamp=now)
        await self.write_queue.enqueue(op)

        if await self.write_queue.should_flush():
            await self.bulk_commit()

    async def bulk_commit(self) -> int:
        """Flush all pending writes in one transaction."""
        async with self._write_lock:
            conn = self._get_connection()
            try:
                return await self.write_queue.flush(conn)
            except Exception as e:
                self.logger.error(f"Failed to flush pending writes: {e}")
                raise

    async def get_write_queue_depth(self) -> int:
        """Return number of pending writes."""
        return await self.write_queue.get_pending_count()

    async def close(self) -> None:
        """Flush remaining writes and close the per-thread connection."""
        pending = await self.write_queue.get_pending_count()
        if pending > 0:
            committed = await self.bulk_commit()
            self.logger.info(f"Flushed {committed} pending writes on close")
        if hasattr(self._thread_local, 'conn') and self._thread_local.conn is not None:
            self._thread_local.conn.close()
            self._thread_local.conn = None

    # ------------------------------------------------------------------
    # Study-level caching
    # ------------------------------------------------------------------

    async def get_study(self, study_accession: str) -> Optional[JSONValue]:
        """Fetch cached study metadata."""
        if await self.write_queue.get_pending_count() > 0:
            await self.bulk_commit()

        def _get_study_sync() -> Optional[JSONValue]:
            conn = self._get_connection()
            row = conn.execute(
                "SELECT data, timestamp FROM study_cache WHERE study_accession = ?",
                (study_accession,)
            ).fetchone()
            if row:
                data_str, timestamp = row
                if time.time() - timestamp > self.ttl:
                    return None
                self.metrics["study_hits"] += 1
                return cast(JSONValue, json.loads(data_str))
            self.metrics["study_misses"] += 1
            return None

        result = await asyncio.to_thread(_get_study_sync)
        if result:
            self.metrics["study_hits"] += 1
        return result

    async def set_study(self, study_accession: str, data: JSONValue) -> None:
        """Enqueue a study cache write."""
        if data is None:
            return
        serializable = data
        try:
            if pd is not None:
                if isinstance(data, pd.DataFrame):
                    serializable = data.to_dict(orient='records')
                elif isinstance(data, pd.Series):
                    serializable = data.to_dict()
        except Exception:
            serializable = data
        try:
            data_json = json.dumps(serializable)
        except TypeError:
            data_json = json.dumps(str(serializable))
        now = time.time()
        op = WriteOp(table="study_cache", key=study_accession, data_json=data_json, timestamp=now)
        await self.write_queue.enqueue(op)
        if await self.write_queue.should_flush():
            await self.bulk_commit()

    # ------------------------------------------------------------------
    # Bulk query caching
    # ------------------------------------------------------------------

    def _hash_query(self, query_params: Mapping[str, JSONValue]) -> str:
        """Generate a deterministic hash for bulk query parameters."""
        query_str = json.dumps(query_params, sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()

    async def get_bulk_query(self, query_params: Mapping[str, JSONValue]) -> Optional[JSONValue]:
        """Fetch cached bulk query results."""
        if await self.write_queue.get_pending_count() > 0:
            await self.bulk_commit()

        query_hash = self._hash_query(query_params)

        def _get_bulk_query_sync() -> Optional[JSONValue]:
            conn = self._get_connection()
            row = conn.execute(
                "SELECT data, timestamp FROM bulk_query_cache WHERE query_hash = ?",
                (query_hash,)
            ).fetchone()
            if row:
                data_str, timestamp = row
                if time.time() - timestamp > self.ttl:
                    return None
                self.metrics["bulk_hits"] += 1
                return cast(JSONValue, json.loads(data_str))
            self.metrics["bulk_misses"] += 1
            return None

        result = await asyncio.to_thread(_get_bulk_query_sync)
        if result:
            self.metrics["bulk_hits"] += 1
        return result

    async def set_bulk_query(
        self,
        query_params: Mapping[str, JSONValue],
        data: JSONValue,
        result_count: int = 0,
    ) -> None:
        """Enqueue a bulk query cache write."""
        if data is None:
            return

        query_hash = self._hash_query(query_params)
        serializable = data
        try:
            if pd is not None:
                if isinstance(data, pd.DataFrame):
                    serializable = data.to_dict(orient='records')
                elif isinstance(data, pd.Series):
                    serializable = data.to_dict()
        except Exception:
            serializable = data
        try:
            data_json = json.dumps(serializable)
        except TypeError:
            data_json = json.dumps(str(serializable))

        now = time.time()
        op = WriteOp(
            table="bulk_query_cache",
            key=query_hash,
            data_json=data_json,
            timestamp=now,
            query_hash=query_hash,
            result_count=result_count
        )
        await self.write_queue.enqueue(op)
        if await self.write_queue.should_flush():
            await self.bulk_commit()

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def get_metrics(self) -> Dict[str, object]:
        """Return a compact dictionary of hit/miss metrics."""
        total_acc = self.metrics["accession_hits"] + self.metrics["accession_misses"]
        total_study = self.metrics["study_hits"] + self.metrics["study_misses"]
        total_bulk = self.metrics["bulk_hits"] + self.metrics["bulk_misses"]
        return {
            "accession": {
                "hits": self.metrics["accession_hits"],
                "misses": self.metrics["accession_misses"],
                "hit_rate": self.metrics["accession_hits"] / max(1, total_acc),
            },
            "study": {
                "hits": self.metrics["study_hits"],
                "misses": self.metrics["study_misses"],
                "hit_rate": self.metrics["study_hits"] / max(1, total_study),
            },
            "bulk": {
                "hits": self.metrics["bulk_hits"],
                "misses": self.metrics["bulk_misses"],
                "hit_rate": self.metrics["bulk_hits"] / max(1, total_bulk),
            },
            "total_queries": self.metrics["total_queries"],
        }

    def reset_metrics(self) -> None:
        """Reset all hit/miss counters."""
        for key in self.metrics:
            self.metrics[key] = 0