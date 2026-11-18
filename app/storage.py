"""Durable, resumable chunk storage backed by SQLite and a local directory.

Layout under ``DATA_DIR``::

    service.sqlite3          upload/chunk/object metadata
    uploads/<session>/chunks/<index>.part
    uploads/<session>/tmp/   in-flight request bodies
    staging/                 hidden, fully merged but unregistered files
    objects/<aa>/<sha256>    verified, content-addressed objects
    locks/                   per-session cross-process locks

Invariants:

* A chunk row is written only after its file has been fsynced and atomically
  renamed into place.
* A merged object is written to ``staging``, fsynced, verified, and only then
  atomically renamed to its content-addressed final path.
* A session is marked completed only in the same transaction that registers
  the object.  Therefore a crash can leave a complete but hidden file, never a
  downloadable half-file.  Retrying completion verifies and reuses that file.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import re
import shutil
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

CHUNK_SIZE = 8 * 1024 * 1024
STALE_TMP_SECONDS = 24 * 60 * 60
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    id          TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL UNIQUE,
    size        INTEGER NOT NULL CHECK (size >= 0),
    rel_path    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS uploads (
    id              TEXT PRIMARY KEY,
    filename        TEXT NOT NULL,
    file_size       INTEGER NOT NULL CHECK (file_size >= 0),
    chunk_size      INTEGER NOT NULL CHECK (chunk_size > 0),
    total_chunks    INTEGER NOT NULL CHECK (total_chunks >= 0),
    expected_sha256 TEXT NOT NULL CHECK (length(expected_sha256) = 64),
    status          TEXT NOT NULL DEFAULT 'uploading'
                    CHECK (status IN ('uploading', 'completed')),
    object_id       TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    completed_at    TEXT,
    FOREIGN KEY (object_id) REFERENCES objects(id)
);

CREATE TABLE IF NOT EXISTS chunks (
    upload_id  TEXT NOT NULL,
    idx        INTEGER NOT NULL CHECK (idx >= 0),
    size       INTEGER NOT NULL CHECK (size >= 0),
    sha256     TEXT NOT NULL CHECK (length(sha256) = 64),
    created_at TEXT NOT NULL,
    PRIMARY KEY (upload_id, idx),
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_uploads_lookup
    ON uploads(expected_sha256, file_size, filename);
CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status);

-- At most one in-progress session per logical file.  Completed sessions are
-- outside the index so the same content may be uploaded again later (and
-- will reuse the content-addressed object).
CREATE UNIQUE INDEX IF NOT EXISTS uq_uploads_active
    ON uploads(expected_sha256, file_size, filename)
    WHERE status = 'uploading';
"""


class ApiError(Exception):
    """An error that maps directly to a structured HTTP response."""

    def __init__(self, status_code: int, code: str, message: str, **extra: Any):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra

    def payload(self) -> dict[str, Any]:
        detail = {"code": self.code, "message": self.message}
        detail.update(self.extra)
        return {"detail": detail}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def normalize_sha256(value: str) -> str:
    digest = (value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise ApiError(400, "invalid_sha256", "expected_sha256 must be 64 hexadecimal characters")
    return digest


def expected_chunk_size(file_size: int, chunk_size: int, idx: int) -> int:
    start = idx * chunk_size
    if start >= file_size:
        return 0
    return min(chunk_size, file_size - start)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class UploadService:
    STALE_TMP_SECONDS = STALE_TMP_SECONDS

    def __init__(self, data_dir: str | Path, chunk_size: int = CHUNK_SIZE):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.root = Path(data_dir).expanduser().resolve()
        self.chunk_size = int(chunk_size)
        self.uploads_dir = self.root / "uploads"
        self.objects_dir = self.root / "objects"
        self.staging_dir = self.root / "staging"
        self.locks_dir = self.root / "locks"
        self.db_path = self.root / "service.sqlite3"
        for directory in (
            self.root,
            self.uploads_dir,
            self.objects_dir,
            self.staging_dir,
            self.locks_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self._thread_locks: dict[str, threading.Lock] = {}
        self._thread_locks_guard = threading.Lock()
        self._init_db()

    # ------------------------------------------------------------------ setup

    def _raw_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        # synchronous is per connection, unlike journal_mode.
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    @contextlib.contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._raw_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._raw_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connection() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
        # Remove stale scratch files from an interrupted merge or upload.
        # Staging files are never registered and cannot be downloaded, so
        # discarding them is safe; the next completion rebuilds them from the
        # confirmed chunks.  Only old files are touched so that a second
        # worker process starting up cannot delete a merge in progress.
        cutoff = time.time() - self.STALE_TMP_SECONDS
        for leftover in self.staging_dir.glob("*"):
            self._remove_if_stale(leftover, cutoff)
        for session_dir in self.uploads_dir.glob("*/tmp"):
            for leftover in session_dir.glob("*"):
                self._remove_if_stale(leftover, cutoff)

    @staticmethod
    def _remove_if_stale(path: Path, cutoff: float) -> None:
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            pass

    def _session_lock(self, upload_id: str) -> contextlib.AbstractContextManager[None]:
        """Serialise completion/chunk registration per session.

        A thread lock handles concurrent requests in this process, while an
        advisory file lock also protects multiple worker processes that share
        the volume.  Locks are per session, so one slow completion never blocks
        uploads for other sessions.
        """

        with self._thread_locks_guard:
            thread_lock = self._thread_locks.setdefault(upload_id, threading.Lock())

        @contextlib.contextmanager
        def locked() -> Iterator[None]:
            thread_lock.acquire()
            fd = -1
            try:
                lock_path = self.locks_dir / f"{upload_id}.lock"
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield
            finally:
                if fd >= 0:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                    finally:
                        os.close(fd)
                thread_lock.release()

        return locked()

    # ------------------------------------------------------------------ paths

    @staticmethod
    def canonical_id(upload_id: str) -> str:
        try:
            return str(uuid.UUID(str(upload_id)))
        except (ValueError, AttributeError, TypeError):
            raise ApiError(404, "upload_not_found", "upload session was not found")

    def _session_dir(self, upload_id: str) -> Path:
        return self.uploads_dir / upload_id

    def _chunks_dir(self, upload_id: str) -> Path:
        return self._session_dir(upload_id) / "chunks"

    def _tmp_dir(self, upload_id: str) -> Path:
        return self._session_dir(upload_id) / "tmp"

    def session_tmp_dir(self, upload_id: str) -> Path:
        """Public accessor used by the HTTP layer for streaming request bodies."""
        return self._tmp_dir(self.canonical_id(upload_id))

    def _chunk_path(self, upload_id: str, idx: int) -> Path:
        return self._chunks_dir(upload_id) / f"{idx:012d}.part"

    def _object_rel_path(self, sha256: str) -> str:
        return f"objects/{sha256[:2]}/{sha256}"

    def _object_path(self, sha256: str) -> Path:
        return self.root / self._object_rel_path(sha256)

    # ---------------------------------------------------------------- helpers

    def _fetch_upload(self, conn: sqlite3.Connection, upload_id: str) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()
        if row is None:
            raise ApiError(404, "upload_not_found", "upload session was not found")
        return dict(row)

    def _fetch_chunks(
        self, conn: sqlite3.Connection, upload_id: str
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT idx, size, sha256, created_at FROM chunks"
            " WHERE upload_id = ? ORDER BY idx",
            (upload_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _reconcile_chunks(self, upload_id: str) -> list[int]:
        """Drop metadata for confirmed chunks whose file vanished or shrank.

        The caller must hold the session lock so this cannot race with a
        concurrent re-upload of the same index.  This keeps a resumed session
        honest after an external volume problem: the affected indexes are
        reported as missing and can be re-uploaded.  Completed sessions are
        left alone because their chunk files are intentionally removed after
        registration.
        """

        with self._connection() as conn:
            upload = self._fetch_upload(conn, upload_id)
            if upload["status"] == "completed":
                return []
            rows = self._fetch_chunks(conn, upload_id)

        bad: list[int] = []
        for row in rows:
            idx = int(row["idx"])
            path = self._chunk_path(upload_id, idx)
            expected_size = expected_chunk_size(
                upload["file_size"], upload["chunk_size"], idx
            )
            try:
                actual_size = path.stat().st_size
            except OSError:
                bad.append(idx)
                continue
            if (
                not path.is_file()
                or actual_size != row["size"]
                or row["size"] != expected_size
                or idx < 0
                or idx >= upload["total_chunks"]
            ):
                bad.append(idx)

        if bad:
            with self._transaction() as conn:
                for idx in bad:
                    conn.execute(
                        "DELETE FROM chunks WHERE upload_id = ? AND idx = ?",
                        (upload_id, idx),
                    )
                conn.execute(
                    "UPDATE uploads SET updated_at = ? WHERE id = ?",
                    (utc_now(), upload_id),
                )
        return sorted(set(bad))

    def _summary(
        self,
        upload: dict[str, Any],
        chunks: list[dict[str, Any]],
        *,
        include_chunks: bool = True,
    ) -> dict[str, Any]:
        total = int(upload["total_chunks"])
        received_indexes = {int(row["idx"]) for row in chunks}
        missing = [idx for idx in range(total) if idx not in received_indexes]
        completed = upload["status"] == "completed"
        result: dict[str, Any] = {
            "session_id": upload["id"],
            "filename": upload["filename"],
            "file_size": upload["file_size"],
            "chunk_size": upload["chunk_size"],
            "total_chunks": total,
            "expected_sha256": upload["expected_sha256"],
            "status": upload["status"],
            "received_count": len(received_indexes),
            "missing_chunks": [] if completed else missing,
            "created_at": upload["created_at"],
            "updated_at": upload["updated_at"],
            "completed_at": upload["completed_at"],
            "object_id": upload["object_id"],
            "download_url": f"/api/uploads/{upload['id']}/download" if completed else None,
        }
        if include_chunks:
            result["received_chunks"] = chunks
        return result

    # ---------------------------------------------------------------- create

    def create_upload(
        self, filename: str, file_size: int, expected_sha256: str
    ) -> dict[str, Any]:
        if not isinstance(file_size, int) or isinstance(file_size, bool) or file_size < 0:
            raise ApiError(400, "invalid_file_size", "file_size must be a non-negative integer")
        digest = normalize_sha256(expected_sha256)
        clean_name = os.path.basename(str(filename or "").replace("\\", "/")).strip()
        if not clean_name:
            raise ApiError(400, "invalid_filename", "filename must not be empty")
        if len(clean_name) > 255:
            clean_name = clean_name[:255]

        now = utc_now()
        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size

        # Re-select the same file after a disconnect: an identical session that
        # is still in progress is returned with its received/missing chunk
        # list, so the client can continue instead of restarting.
        upload_id: str | None = None
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT id FROM uploads WHERE expected_sha256 = ?"
                " AND file_size = ? AND filename = ? AND status = 'uploading'",
                (digest, file_size, clean_name),
            ).fetchone()
            if row is not None:
                upload_id = row["id"]

            if upload_id is None:
                candidate = str(uuid.uuid4())
                self._chunks_dir(candidate).mkdir(parents=True, exist_ok=True)
                self._tmp_dir(candidate).mkdir(parents=True, exist_ok=True)
                try:
                    conn.execute(
                        "INSERT INTO uploads"
                        " (id, filename, file_size, chunk_size, total_chunks,"
                        "  expected_sha256, status, created_at, updated_at)"
                        " VALUES (?, ?, ?, ?, ?, ?, 'uploading', ?, ?)",
                        (
                            candidate,
                            clean_name,
                            file_size,
                            self.chunk_size,
                            total_chunks,
                            digest,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError:
                    # Lost a race with a concurrent identical request (possible
                    # when several processes share the database).
                    shutil.rmtree(self._session_dir(candidate), ignore_errors=True)
                    row = conn.execute(
                        "SELECT id FROM uploads WHERE expected_sha256 = ?"
                        " AND file_size = ? AND filename = ? AND status = 'uploading'",
                        (digest, file_size, clean_name),
                    ).fetchone()
                    if row is None:
                        raise
                    upload_id = row["id"]
                else:
                    upload_id = candidate

        # Fetch outside the write transaction: reconciliation may need to
        # write, and a second connection would block on our RESERVED lock.
        return self.get_upload(upload_id)

    def get_upload(self, upload_id: str) -> dict[str, Any]:
        canonical = self.canonical_id(upload_id)
        with self._session_lock(canonical):
            self._reconcile_chunks(canonical)
        with self._connection() as conn:
            upload = self._fetch_upload(conn, canonical)
            chunks = self._fetch_chunks(conn, canonical)
        return self._summary(upload, chunks)

    def find_uploads(
        self,
        *,
        expected_sha256: str | None = None,
        file_size: int | None = None,
        filename: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if expected_sha256 is not None:
            clauses.append("expected_sha256 = ?")
            params.append(normalize_sha256(expected_sha256))
        if file_size is not None:
            if file_size < 0:
                raise ApiError(400, "invalid_file_size", "file_size must be non-negative")
            clauses.append("file_size = ?")
            params.append(file_size)
        if filename is not None:
            clauses.append("filename = ?")
            params.append(filename)
        if status is not None:
            if status not in {"uploading", "completed"}:
                raise ApiError(400, "invalid_status", "status must be uploading or completed")
            clauses.append("status = ?")
            params.append(status)
        limit = max(1, min(int(limit), 200))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM uploads" + where + " ORDER BY created_at DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
            results = []
            for row in rows:
                chunks = self._fetch_chunks(conn, row["id"])
                results.append(self._summary(dict(row), chunks, include_chunks=False))
        return results

    # ----------------------------------------------------------- chunk upload

    def chunk_requirement(self, upload_id: str, idx: int) -> dict[str, Any]:
        canonical = self.canonical_id(upload_id)
        with self._connection() as conn:
            upload = self._fetch_upload(conn, canonical)
        if upload["status"] == "completed":
            raise ApiError(409, "upload_completed", "upload is already completed")
        total = int(upload["total_chunks"])
        if idx < 0 or idx >= total:
            raise ApiError(
                400,
                "invalid_chunk_index",
                f"chunk index must be between 0 and {total - 1}",
            )
        return {
            "upload_id": canonical,
            "index": idx,
            "expected_size": expected_chunk_size(
                upload["file_size"], upload["chunk_size"], idx
            ),
            "chunk_size": upload["chunk_size"],
        }

    def commit_chunk(
        self,
        upload_id: str,
        idx: int,
        sha256: str,
        size: int,
        temp_path: Path,
    ) -> dict[str, Any]:
        """Atomically confirm an already-hashed temporary chunk file.

        Re-uploading an identical index returns the same acknowledgement.  A
        different digest for a confirmed index is rejected without changing
        the stored chunk.
        """

        canonical = self.canonical_id(upload_id)
        digest = normalize_sha256(sha256)
        try:
            temp_size = temp_path.stat().st_size
        except OSError:
            raise ApiError(500, "chunk_staging_missing", "staged chunk is unavailable")

        with self._session_lock(canonical):
            with self._connection() as conn:
                upload = self._fetch_upload(conn, canonical)
            if upload["status"] == "completed":
                raise ApiError(409, "upload_completed", "upload is already completed")
            total = int(upload["total_chunks"])
            if idx < 0 or idx >= total:
                raise ApiError(400, "invalid_chunk_index", "chunk index is out of range")
            wanted_size = expected_chunk_size(
                upload["file_size"], upload["chunk_size"], idx
            )
            if size != wanted_size or temp_size != wanted_size:
                raise ApiError(
                    400,
                    "chunk_size_mismatch",
                    f"chunk {idx} must be exactly {wanted_size} bytes",
                    expected_size=wanted_size,
                    uploaded_size=size,
                )

            chunk_path = self._chunk_path(canonical, idx)
            chunk_path.parent.mkdir(parents=True, exist_ok=True)

            with self._connection() as conn:
                existing = conn.execute(
                    "SELECT idx, size, sha256 FROM chunks"
                    " WHERE upload_id = ? AND idx = ?",
                    (canonical, idx),
                ).fetchone()

            if existing is not None and existing["sha256"] != digest:
                raise ApiError(
                    409,
                    "chunk_digest_conflict",
                    f"chunk {idx} already exists with a different SHA-256",
                    index=idx,
                    stored_sha256=existing["sha256"],
                    uploaded_sha256=digest,
                )

            # Same digest (or a first upload): publish the verified bytes with
            # an atomic rename, then persist the acknowledgement.  The rename
            # is idempotent, so a re-upload returns the original ack rather
            # than creating a second copy.
            os.replace(temp_path, chunk_path)
            _fsync_dir(chunk_path.parent)
            now = utc_now()
            with self._transaction() as conn:
                conn.execute(
                    "INSERT INTO chunks"
                    " (upload_id, idx, size, sha256, created_at)"
                    " VALUES (?, ?, ?, ?, ?)"
                    " ON CONFLICT(upload_id, idx) DO UPDATE SET"
                    "  size = excluded.size,"
                    "  sha256 = excluded.sha256"
                    " WHERE chunks.sha256 = excluded.sha256",
                    (canonical, idx, size, digest, now),
                )
                if conn.total_changes == 0:
                    raise ApiError(
                        409,
                        "chunk_digest_conflict",
                        f"chunk {idx} already exists with a different SHA-256",
                        index=idx,
                        uploaded_sha256=digest,
                    )
                conn.execute(
                    "UPDATE uploads SET updated_at = ? WHERE id = ?",
                    (now, canonical),
                )

            return {
                "session_id": canonical,
                "index": idx,
                "sha256": digest,
                "size": size,
                "received": True,
            }

    # -------------------------------------------------------------- completion

    def _verified_existing_object(self, sha256: str, file_size: int) -> Path | None:
        path = self._object_path(sha256)
        try:
            if not path.is_file() or path.stat().st_size != file_size:
                return None
        except OSError:
            return None
        return path if _sha256_file(path) == sha256 else None

    def _merge_chunks(self, upload: dict[str, Any], staging: Path) -> str:
        """Merge in index order, verifying each chunk and the whole digest."""

        upload_id = upload["id"]
        overall = hashlib.sha256()
        total_written = 0
        expected_rows = [
            (idx, expected_chunk_size(upload["file_size"], upload["chunk_size"], idx))
            for idx in range(int(upload["total_chunks"]))
        ]
        rows = {
            int(row["idx"]): row
            for row in self._fetch_chunks_standalone(upload_id)
        }
        staging.parent.mkdir(parents=True, exist_ok=True)

        with staging.open("wb") as out:
            for idx, wanted_size in expected_rows:
                problem = self._copy_verified_chunk(
                    out, upload_id, idx, wanted_size, rows, overall
                )
                if problem is not None:
                    # A duplicate index cannot occur (primary key), so the
                    # added size is exact for a verified chunk.
                    raise problem
                total_written += wanted_size
            out.flush()
            os.fsync(out.fileno())

        if total_written != upload["file_size"]:
            raise ApiError(
                409,
                "size_mismatch",
                "merged size does not match the declared file size",
            )
        return overall.hexdigest()

    def _copy_verified_chunk(
        self,
        out: Any,
        upload_id: str,
        idx: int,
        wanted_size: int,
        rows: dict[int, dict[str, Any]],
        overall: Any,
    ) -> ApiError | None:
        """Copy one chunk to ``out`` after checking its size and digest."""

        row = rows.get(idx)
        if row is None:
            return ApiError(
                409,
                "chunks_missing",
                "not all chunks are available",
                missing_chunks=[idx],
            )
        chunk_path = self._chunk_path(upload_id, idx)
        try:
            actual_size = chunk_path.stat().st_size
        except OSError:
            return ApiError(
                409,
                "chunks_missing",
                f"chunk {idx} is missing from storage",
                missing_chunks=[idx],
            )
        if actual_size != wanted_size or row["size"] != wanted_size:
            return ApiError(
                409,
                "chunk_size_mismatch",
                f"chunk {idx} has an invalid size",
                missing_chunks=[idx],
            )
        chunk_digest = hashlib.sha256()
        read_size = 0
        with chunk_path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                chunk_digest.update(block)
                overall.update(block)
                out.write(block)
                read_size += len(block)
        if read_size != wanted_size or chunk_digest.hexdigest() != row["sha256"]:
            return ApiError(
                409,
                "chunk_corrupt",
                f"chunk {idx} does not match its confirmed SHA-256",
                missing_chunks=[idx],
            )
        return None

    def _fetch_chunks_standalone(self, upload_id: str) -> list[dict[str, Any]]:
        with self._connection() as conn:
            return self._fetch_chunks(conn, upload_id)

    def _register_object(
        self, upload_id: str, sha256: str, size: int, rel_path: str
    ) -> str:
        """Register an object and complete the session atomically.

        Object id is the verified content digest, and ``sha256`` is UNIQUE, so
        simultaneous completions (same or different sessions) converge on one
        object row.
        """

        now = utc_now()
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO objects (id, sha256, size, rel_path, created_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(sha256) DO NOTHING",
                (sha256, sha256, size, rel_path, now),
            )
            obj = conn.execute(
                "SELECT id, size, rel_path FROM objects WHERE sha256 = ?", (sha256,)
            ).fetchone()
            if obj is None:
                raise RuntimeError("object registration failed")
            conn.execute(
                "UPDATE uploads SET status = 'completed', object_id = ?,"
                " completed_at = ?, updated_at = ? WHERE id = ?",
                (obj["id"], now, now, upload_id),
            )
        return str(obj["id"])

    def _cleanup_chunks(self, upload_id: str) -> None:
        # Metadata remains for the audit/status response; bytes are no longer
        # needed once the object is registered.  Errors here must not undo a
        # successful completion.
        for directory in (self._chunks_dir(upload_id), self._tmp_dir(upload_id)):
            try:
                shutil.rmtree(directory)
            except OSError:
                pass

    def complete_upload(self, upload_id: str) -> dict[str, Any]:
        canonical = self.canonical_id(upload_id)

        with self._session_lock(canonical):
            with self._connection() as conn:
                upload = self._fetch_upload(conn, canonical)
                chunks = self._fetch_chunks(conn, canonical)

            if upload["status"] == "completed":
                if not upload["object_id"]:
                    raise ApiError(500, "object_inconsistent", "completed upload has no object")
                with self._connection() as conn:
                    obj = conn.execute(
                        "SELECT * FROM objects WHERE id = ?", (upload["object_id"],)
                    ).fetchone()
                if obj is None or not (self.root / obj["rel_path"]).is_file():
                    raise ApiError(
                        409,
                        "object_unavailable",
                        "the registered object file is unavailable",
                    )
                result = self._summary(upload, chunks)
                result.update(object_id=upload["object_id"], already_completed=True)
                return result

            digest = upload["expected_sha256"]

            # Completion is only legal once *this session* has received every
            # chunk, even if identical content is already stored for another
            # session.  Reconcile drops rows whose files vanished, so the
            # response names exactly the indexes that must be re-uploaded.
            self._reconcile_chunks(canonical)
            with self._connection() as conn:
                upload = self._fetch_upload(conn, canonical)
                chunks = self._fetch_chunks(conn, canonical)
            rows_by_idx = {int(row["idx"]): row for row in chunks}
            missing = [
                idx
                for idx in range(int(upload["total_chunks"]))
                if idx not in rows_by_idx
            ]
            if missing:
                raise ApiError(
                    409,
                    "chunks_missing",
                    "not all chunks have been received",
                    missing_chunks=missing,
                )
            # Explicit sizes protect against a malformed DB row.
            for idx, row in rows_by_idx.items():
                wanted = expected_chunk_size(
                    upload["file_size"], upload["chunk_size"], idx
                )
                if row["size"] != wanted:
                    raise ApiError(
                        409,
                        "chunks_missing",
                        f"chunk {idx} is invalid and must be re-uploaded",
                        missing_chunks=[idx],
                    )

            final_path = self._object_path(digest)
            final_path.parent.mkdir(parents=True, exist_ok=True)

            # Always merge and verify this session's own chunks: that proves
            # continuity, sizes and the whole-file digest for *this* session
            # rather than assuming identical content registered earlier.
            staging = self.staging_dir / f"{canonical}.{uuid.uuid4().hex}.part"
            try:
                actual_digest = self._merge_chunks(upload, staging)
                if actual_digest != digest:
                    raise ApiError(
                        422,
                        "digest_mismatch",
                        "the merged file does not match expected_sha256",
                        expected_sha256=digest,
                        actual_sha256=actual_digest,
                    )
                # If a previous attempt (or another session) already produced
                # this exact content and the file is intact, keep that single
                # object and discard the duplicate.  Otherwise publish the
                # staging file with one atomic rename.  In both cases the file
                # stays unregistered and unreachable until the transaction.
                if self._verified_existing_object(digest, int(upload["file_size"])):
                    staging.unlink()
                else:
                    os.replace(staging, final_path)
                    _fsync_dir(final_path.parent)
            finally:
                with contextlib.suppress(OSError):
                    staging.unlink()

            object_id = self._register_object(
                canonical, digest, int(upload["file_size"]), self._object_rel_path(digest)
            )
            self._cleanup_chunks(canonical)

            with self._connection() as conn:
                upload = self._fetch_upload(conn, canonical)
                chunks = self._fetch_chunks(conn, canonical)
            result = self._summary(upload, chunks)
            result.update(object_id=object_id, already_completed=False)
            return result

    # ---------------------------------------------------------------- download

    def download_target(self, upload_id: str) -> tuple[Path, str]:
        canonical = self.canonical_id(upload_id)
        with self._connection() as conn:
            upload = self._fetch_upload(conn, canonical)
            if upload["status"] != "completed" or not upload["object_id"]:
                raise ApiError(
                    409,
                    "upload_not_completed",
                    "the upload must be completed and verified before download",
                )
            obj = conn.execute(
                "SELECT * FROM objects WHERE id = ?", (upload["object_id"],)
            ).fetchone()
        if obj is None:
            raise ApiError(409, "object_unavailable", "the object is not registered")
        path = self.root / obj["rel_path"]
        try:
            if not path.is_file() or path.stat().st_size != obj["size"]:
                raise ApiError(
                    409,
                    "object_unavailable",
                    "the object file is missing or has an unexpected size",
                )
        except OSError:
            raise ApiError(409, "object_unavailable", "the object file is unavailable")
        return path, upload["filename"]

    def object_count(self) -> int:
        with self._connection() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0])

    def list_objects(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT id, sha256, size, rel_path, created_at FROM objects"
                " ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
