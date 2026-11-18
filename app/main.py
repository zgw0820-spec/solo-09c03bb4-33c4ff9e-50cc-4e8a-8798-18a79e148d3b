"""FastAPI application for resumable offline artifact uploads."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, File, Form, Header, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .storage import ApiError, UploadService

MAX_CHUNK_BYTES = 64 * 1024 * 1024


def create_app(data_dir: str | Path | None = None, chunk_size: int | None = None) -> FastAPI:
    root = data_dir or os.environ.get("DATA_DIR", "./data")
    size = chunk_size or int(os.environ.get("CHUNK_SIZE", str(8 * 1024 * 1024)))
    service = UploadService(root, chunk_size=size)
    static_dir = Path(__file__).parent / "static"

    app = FastAPI(
        title="离线制品上传服务",
        version="1.0.0",
        description="Resumable, content-verified offline artifact upload service.",
    )
    app.state.service = service
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.payload())

    @app.exception_handler(RequestValidationError)
    async def _validation_error(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        problems = []
        for error in exc.errors():
            problems.append(
                {
                    "field": ".".join(str(part) for part in error.get("loc", ())),
                    "message": error.get("msg", "invalid value"),
                }
            )
        return JSONResponse(
            status_code=422,
            content={
                "detail": {
                    "code": "invalid_request",
                    "message": "one or more request fields are invalid",
                    "problems": problems,
                }
            },
        )

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "detail": {
                    "code": "internal_error",
                    "message": "unexpected server error",
                }
            },
        )

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        return HTMLResponse((static_dir / "index.html").read_text(encoding="utf-8"))

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/config")
    async def config() -> dict[str, int]:
        return {"chunk_size": service.chunk_size, "max_chunk_bytes": MAX_CHUNK_BYTES}

    # ------------------------------------------------------------- sessions

    @app.post("/api/uploads", status_code=201)
    async def create_upload(
        filename: Annotated[str, Form()],
        file_size: Annotated[int, Form(ge=0)],
        expected_sha256: Annotated[str, Form()],
    ) -> JSONResponse:
        result = await run_in_threadpool(
            service.create_upload, filename, file_size, expected_sha256
        )
        return JSONResponse(status_code=201, content=result)

    @app.get("/api/uploads")
    async def list_uploads(
        expected_sha256: str | None = Query(default=None),
        file_size: int | None = Query(default=None, ge=0),
        filename: str | None = Query(default=None),
        status: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> dict[str, object]:
        results = await run_in_threadpool(
            service.find_uploads,
            expected_sha256=expected_sha256,
            file_size=file_size,
            filename=filename,
            status=status,
            limit=limit,
        )
        return {"sessions": results}

    @app.get("/api/uploads/{upload_id}")
    async def get_upload(upload_id: str) -> dict[str, object]:
        return await run_in_threadpool(service.get_upload, upload_id)

    @app.get("/api/uploads/{upload_id}/chunks/{idx}")
    async def chunk_requirement(upload_id: str, idx: int) -> dict[str, object]:
        return await run_in_threadpool(service.chunk_requirement, upload_id, idx)

    @app.put("/api/uploads/{upload_id}/chunks/{idx}")
    async def put_chunk(
        upload_id: str,
        idx: int,
        chunk: Annotated[UploadFile, File()],
        x_chunk_sha256: Annotated[str | None, Header(alias="X-Chunk-SHA256")] = None,
    ) -> dict[str, object]:
        canonical = service.canonical_id(upload_id)
        requirement = service.chunk_requirement(canonical, idx)
        expected_size = int(requirement["expected_size"])
        if expected_size > MAX_CHUNK_BYTES:
            raise ApiError(
                413,
                "chunk_too_large",
                f"chunk exceeds the {MAX_CHUNK_BYTES}-byte request limit",
            )

        declared = (x_chunk_sha256 or "").strip().lower()
        temp_path = await _spool_chunk(chunk, service, canonical)
        try:
            size = temp_path.stat().st_size
            if size != expected_size:
                raise ApiError(
                    400,
                    "chunk_size_mismatch",
                    f"chunk {idx} must be exactly {expected_size} bytes",
                    expected_size=expected_size,
                    uploaded_size=size,
                )
            digest = await run_in_threadpool(_hash_file, temp_path)
            if declared and declared != digest:
                raise ApiError(
                    400,
                    "chunk_digest_mismatch",
                    "X-Chunk-SHA256 does not match the uploaded bytes",
                    declared_sha256=declared,
                    actual_sha256=digest,
                )
            result = await run_in_threadpool(
                service.commit_chunk, canonical, idx, digest, size, temp_path
            )
            return {**result, "session_id": canonical}
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

    @app.post("/api/uploads/{upload_id}/complete")
    async def complete_upload(upload_id: str) -> dict[str, object]:
        # Runs off the event loop: merging and verifying a large artifact must
        # not delay chunk uploads belonging to other sessions.
        return await run_in_threadpool(service.complete_upload, upload_id)

    @app.get("/api/uploads/{upload_id}/download")
    async def download(upload_id: str) -> FileResponse:
        path, filename = await run_in_threadpool(service.download_target, upload_id)
        return FileResponse(
            path,
            media_type="application/octet-stream",
            filename=filename,
            headers={
                "X-Content-SHA256": path.name,
                "Cache-Control": "no-store",
            },
        )

    @app.get("/api/objects")
    async def list_objects(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, object]:
        objects = await run_in_threadpool(service.list_objects, limit)
        return {"objects": objects}

    return app


async def _spool_chunk(chunk: UploadFile, service: UploadService, upload_id: str) -> Path:
    """Stream the request body to the session's temp dir while hashing."""

    tmp_dir = service.session_tmp_dir(upload_id)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=tmp_dir, prefix="incoming-", suffix=".part")
    path = Path(name)
    written = 0
    try:
        with os.fdopen(fd, "wb") as out:
            while True:
                block = await chunk.read(1024 * 1024)
                if not block:
                    break
                written += len(block)
                if written > MAX_CHUNK_BYTES:
                    raise ApiError(413, "chunk_too_large", "request body is too large")
                out.write(block)
            out.flush()
            os.fsync(out.fileno())
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise
    return path


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


app = create_app()
