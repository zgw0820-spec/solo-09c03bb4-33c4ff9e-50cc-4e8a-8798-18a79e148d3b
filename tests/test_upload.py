"""Automated coverage of interrupted uploads, retries, restarts, and completion."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.storage import ApiError, UploadService


def digest_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_client(tmp_path: Path, chunk_size: int = 1024) -> TestClient:
    return TestClient(create_app(tmp_path / "data", chunk_size=chunk_size))


def create_session(client: TestClient, name: str, data: bytes, sha: str | None = None):
    response = client.post(
        "/api/uploads",
        data={
            "filename": name,
            "file_size": str(len(data)),
            "expected_sha256": sha or digest_of(data),
        },
    )
    return response


def cut(data: bytes, chunk_size: int) -> list[bytes]:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def put_chunk(client: TestClient, session_id: str, idx: int, payload: bytes):
    return client.put(
        f"/api/uploads/{session_id}/chunks/{idx}",
        files={"chunk": (f"chunk-{idx}", payload, "application/octet-stream")},
        headers={"X-Chunk-SHA256": digest_of(payload)},
    )


def test_interrupted_upload_resumes_with_only_missing_chunks(tmp_path: Path):
    data = os.urandom(5 * 1024 + 37)
    client = make_client(tmp_path)
    created = create_session(client, "image.tar", data)
    assert created.status_code == 201, created.text
    session = created.json()
    assert session["total_chunks"] == 6
    assert session["missing_chunks"] == list(range(6))
    assert session["status"] == "uploading"
    assert session["session_id"]

    parts = cut(data, 1024)
    for idx in (0, 1, 3, 5):
        assert put_chunk(client, session["session_id"], idx, parts[idx]).status_code == 200

    # "Disconnect": re-select the same file.  A new session is not created and
    # the server reports the acknowledged chunks.
    resumed = create_session(client, "image.tar", data).json()
    assert resumed["session_id"] == session["session_id"]
    assert resumed["received_count"] == 4
    assert resumed["missing_chunks"] == [2, 4]
    assert {c["idx"] for c in resumed["received_chunks"]} == {0, 1, 3, 5}

    # Completion before all chunks arrive is refused.
    early = client.post(f"/api/uploads/{session['session_id']}/complete")
    assert early.status_code == 409
    assert early.json()["detail"]["code"] == "chunks_missing"
    assert early.json()["detail"]["missing_chunks"] == [2, 4]

    for idx in (2, 4):
        assert put_chunk(client, session["session_id"], idx, parts[idx]).status_code == 200

    done = client.post(f"/api/uploads/{session['session_id']}/complete")
    assert done.status_code == 200, done.text
    body = done.json()
    assert body["status"] == "completed"
    assert body["already_completed"] is False
    assert body["object_id"] == digest_of(data)

    download = client.get(f"/api/uploads/{session['session_id']}/download")
    assert download.status_code == 200
    assert download.content == data

    # Only one object exists for the content.
    assert client.app.state.service.object_count() == 1


def test_same_index_retry_is_idempotent_but_conflict_is_rejected(tmp_path: Path):
    data = os.urandom(3 * 1024)
    client = make_client(tmp_path)
    session = create_session(client, "a.bin", data).json()
    parts = cut(data, 1024)

    first = put_chunk(client, session["session_id"], 1, parts[1])
    assert first.status_code == 200
    ack = first.json()
    assert ack["sha256"] == digest_of(parts[1])

    again = put_chunk(client, session["session_id"], 1, parts[1])
    assert again.status_code == 200
    assert again.json() == ack

    conflict = put_chunk(client, session["session_id"], 1, b"\x00" * 1024)
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "chunk_digest_conflict"

    # The original chunk was not overwritten.
    status = client.get(f"/api/uploads/{session['session_id']}").json()
    stored = [c for c in status["received_chunks"] if c["idx"] == 1][0]
    assert stored["sha256"] == digest_of(parts[1])

    assert put_chunk(client, session["session_id"], 0, parts[0]).status_code == 200
    assert put_chunk(client, session["session_id"], 2, parts[2]).status_code == 200
    assert client.post(f"/api/uploads/{session['session_id']}/complete").status_code == 200


def test_chunk_size_and_whole_file_digest_are_enforced(tmp_path: Path):
    data = os.urandom(2500)
    client = make_client(tmp_path, chunk_size=1024)
    session = create_session(client, "b.bin", data).json()

    short = put_chunk(client, session["session_id"], 0, data[:100])
    assert short.status_code == 400
    assert short.json()["detail"]["code"] == "chunk_size_mismatch"

    out_of_range = client.put(
        f"/api/uploads/{session['session_id']}/chunks/3",
        files={"chunk": ("c", b"x", "application/octet-stream")},
    )
    assert out_of_range.status_code == 400

    # Wrong declared whole-file digest: every chunk is accepted, but
    # completion refuses to register the object.
    bad = create_session(client, "b.bin", data, sha="0" * 64)
    assert bad.status_code == 201
    bad_session = bad.json()
    parts = cut(data, 1024)
    for idx, part in enumerate(parts):
        assert put_chunk(client, bad_session["session_id"], idx, part).status_code == 200
    failed = client.post(f"/api/uploads/{bad_session['session_id']}/complete")
    assert failed.status_code == 422
    assert failed.json()["detail"]["code"] == "digest_mismatch"
    assert client.app.state.service.object_count() == 0
    assert client.get(f"/api/uploads/{bad_session['session_id']}/download").status_code == 409


def test_chunk_loss_is_reported_and_reuploadable(tmp_path: Path):
    data = os.urandom(4096)
    client = make_client(tmp_path)
    session = create_session(client, "c.bin", data).json()
    parts = cut(data, 1024)
    for idx, part in enumerate(parts):
        assert put_chunk(client, session["session_id"], idx, part).status_code == 200

    service = client.app.state.service
    (service._chunk_path(session["session_id"], 2)).unlink()

    status = client.get(f"/api/uploads/{session['session_id']}").json()
    assert status["missing_chunks"] == [2]
    assert put_chunk(client, session["session_id"], 2, parts[2]).status_code == 200
    assert client.post(f"/api/uploads/{session['session_id']}/complete").status_code == 200


def test_confirmed_chunks_survive_restart(tmp_path: Path):
    data = os.urandom(6 * 1024)
    parts = cut(data, 1024)
    data_dir = tmp_path / "data"

    client = TestClient(create_app(data_dir, chunk_size=1024))
    session = create_session(client, "d.bin", data).json()
    for idx in (0, 2, 5):
        assert put_chunk(client, session["session_id"], idx, parts[idx]).status_code == 200

    # A brand new application instance, as after a container restart.
    restarted = TestClient(create_app(data_dir, chunk_size=1024))
    resumed = restarted.get(f"/api/uploads/{session['session_id']}").json()
    assert resumed["received_count"] == 3
    assert resumed["missing_chunks"] == [1, 3, 4]

    for idx in (1, 3, 4):
        assert put_chunk(restarted, session["session_id"], idx, parts[idx]).status_code == 200
    assert restarted.post(f"/api/uploads/{session['session_id']}/complete").status_code == 200
    assert restarted.get(f"/api/uploads/{session['session_id']}/download").content == data


def test_merged_but_unregistered_file_is_hidden_and_reused(tmp_path: Path):
    data = os.urandom(4096)
    client = make_client(tmp_path)
    session = create_session(client, "e.bin", data).json()
    for idx, part in enumerate(cut(data, 1024)):
        assert put_chunk(client, session["session_id"], idx, part).status_code == 200

    service = client.app.state.service
    # Simulate a crash between the verified rename and the registration
    # transaction: the merged object sits at its final path, no object row
    # exists, and the session (with all its chunks) is still 'uploading'.
    final_path = service._object_path(digest_of(data))
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(data)

    hidden = client.get(f"/api/uploads/{session['session_id']}/download")
    assert hidden.status_code == 409
    assert hidden.json()["detail"]["code"] == "upload_not_completed"

    # Restarting the service must not expose the unregistered file either.
    restarted = TestClient(create_app(tmp_path / "data", chunk_size=1024))
    assert restarted.get(f"/api/uploads/{session['session_id']}/download").status_code == 409
    listed = restarted.get("/api/objects").json()["objects"]
    assert listed == []
    # The unregistered object must not be reachable through any completed
    # session either (it is addressed only via session status).
    assert restarted.get("/api/uploads").json()["sessions"][0]["status"] == "uploading"

    # Retrying completion re-verifies and reuses the merged file and registers
    # exactly one object (no second copy, no chunk re-upload needed).
    retried = restarted.post(f"/api/uploads/{session['session_id']}/complete")
    assert retried.status_code == 200, retried.text
    assert retried.json()["object_id"] == digest_of(data)
    assert restarted.get(f"/api/uploads/{session['session_id']}/download").content == data
    assert restarted.app.state.service.object_count() == 1


def test_concurrent_completion_yields_one_object_and_other_sessions_progress(
    tmp_path: Path,
):
    data = os.urandom(8 * 1024)
    other_data = os.urandom(3 * 1024)
    client = make_client(tmp_path)
    session = create_session(client, "f.bin", data).json()
    for idx, part in enumerate(cut(data, 1024)):
        assert put_chunk(client, session["session_id"], idx, part).status_code == 200

    other = create_session(client, "g.bin", other_data).json()
    results: list[int] = []
    started = threading.Barrier(4)

    def complete():
        started.wait()
        response = client.post(f"/api/uploads/{session['session_id']}/complete")
        results.append(response.status_code)

    def upload_other():
        started.wait()
        for idx, part in enumerate(cut(other_data, 1024)):
            response = put_chunk(client, other["session_id"], idx, part)
            results.append(response.status_code)

    threads = [threading.Thread(target=complete) for _ in range(3)]
    threads.append(threading.Thread(target=upload_other))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert results.count(200) == 3 + 3  # 3 completions + 3 chunks of the other file
    assert client.app.state.service.object_count() == 1
    assert client.get(f"/api/uploads/{session['session_id']}/download").content == data
    assert client.post(f"/api/uploads/{other['session_id']}/complete").status_code == 200
    assert client.app.state.service.object_count() == 2


def test_two_sessions_with_identical_content_share_one_object(tmp_path: Path):
    data = os.urandom(2048)
    client = make_client(tmp_path, chunk_size=1024)
    first = create_session(client, "h1.bin", data).json()
    for idx, part in enumerate(cut(data, 1024)):
        assert put_chunk(client, first["session_id"], idx, part).status_code == 200
    first_done = client.post(f"/api/uploads/{first['session_id']}/complete")
    assert first_done.status_code == 200

    # Different filename => distinct, completed session reusing the object.
    second = create_session(client, "h2.bin", data).json()
    assert second["session_id"] != first["session_id"]
    for idx, part in enumerate(cut(data, 1024)):
        assert put_chunk(client, second["session_id"], idx, part).status_code == 200
    done = client.post(f"/api/uploads/{second['session_id']}/complete")
    assert done.status_code == 200
    assert done.json()["object_id"] == first_done.json()["object_id"]
    assert client.app.state.service.object_count() == 1


def test_completion_requires_all_chunks_even_if_object_already_exists(tmp_path: Path):
    data = os.urandom(2048)
    client = make_client(tmp_path, chunk_size=1024)

    first = create_session(client, "k1.bin", data).json()
    for idx, part in enumerate(cut(data, 1024)):
        assert put_chunk(client, first["session_id"], idx, part).status_code == 200
    assert client.post(f"/api/uploads/{first['session_id']}/complete").status_code == 200

    # A second session for the same content cannot skip its own chunks just
    # because the object is already stored.
    second = create_session(client, "k2.bin", data).json()
    incomplete = client.post(f"/api/uploads/{second['session_id']}/complete")
    assert incomplete.status_code == 409
    assert incomplete.json()["detail"]["missing_chunks"] == [0, 1]
    assert client.get(f"/api/uploads/{second['session_id']}/download").status_code == 409


def test_corrupted_chunk_bytes_block_completion(tmp_path: Path):
    data = os.urandom(3072)
    client = make_client(tmp_path)
    session = create_session(client, "l.bin", data).json()
    for idx, part in enumerate(cut(data, 1024)):
        assert put_chunk(client, session["session_id"], idx, part).status_code == 200

    service = client.app.state.service
    # Same length, different bytes: only the digest check can catch this.
    service._chunk_path(session["session_id"], 2).write_bytes(b"\x00" * 1024)

    failed = client.post(f"/api/uploads/{session['session_id']}/complete")
    assert failed.status_code == 409
    assert failed.json()["detail"]["code"] == "chunk_corrupt"
    assert failed.json()["detail"]["missing_chunks"] == [2]
    assert service.object_count() == 0
    assert client.get(f"/api/uploads/{session['session_id']}").json()["status"] == "uploading"
    # No staging leftovers from the aborted merge.
    assert list(service.staging_dir.iterdir()) == []


def test_empty_file_roundtrip(tmp_path: Path):
    client = make_client(tmp_path)
    empty_digest = hashlib.sha256(b"").hexdigest()
    session = create_session(client, "empty.bin", b"").json()
    assert session["total_chunks"] == 0
    assert session["missing_chunks"] == []
    assert client.post(f"/api/uploads/{session['session_id']}/complete").status_code == 200
    response = client.get(f"/api/uploads/{session['session_id']}/download")
    assert response.status_code == 200
    assert response.content == b""
    assert empty_digest == session["expected_sha256"]


def test_validation_and_unknown_sessions(tmp_path: Path):
    client = make_client(tmp_path)
    bad = client.post(
        "/api/uploads",
        data={"filename": "x", "file_size": "10", "expected_sha256": "not-a-digest"},
    )
    assert bad.status_code == 400
    assert bad.json()["detail"]["code"] == "invalid_sha256"

    missing = client.get("/api/uploads/not-a-uuid")
    assert missing.status_code == 404
    missing_chunk = client.put(
        "/api/uploads/not-a-uuid/chunks/0",
        files={"chunk": ("c", b"x", "application/octet-stream")},
    )
    assert missing_chunk.status_code == 404
    assert client.get("/api/health").json() == {"status": "ok"}
    assert client.get("/").status_code == 200


def test_header_digest_mismatch_is_rejected(tmp_path: Path):
    data = os.urandom(1024)
    client = make_client(tmp_path)
    session = create_session(client, "i.bin", data).json()
    response = client.put(
        f"/api/uploads/{session['session_id']}/chunks/0",
        files={"chunk": ("c", data, "application/octet-stream")},
        headers={"X-Chunk-SHA256": "0" * 64},
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "chunk_digest_mismatch"


def test_service_rejects_invalid_chunk_without_state_change(tmp_path: Path):
    with pytest.raises(ValueError):
        UploadService(tmp_path / "data", chunk_size=0)

    service = UploadService(tmp_path / "data", chunk_size=1024)
    upload = service.create_upload("j.bin", 2048, digest_of(b"0" * 2048))
    temp = tmp_path / "temp.bin"
    temp.write_bytes(b"a" * 512)
    with pytest.raises(ApiError) as excinfo:
        service.commit_chunk(upload["session_id"], 0, digest_of(b"a" * 512), 512, temp)
    assert excinfo.value.code == "chunk_size_mismatch"
    assert service.get_upload(upload["session_id"])["received_count"] == 0
