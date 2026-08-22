import json

import pandas as pd
import pytest
import responses

from toolviper.utils.data import cloudflare


@pytest.fixture
def mock_metadata(tmp_path):
    metadata = {
        "version": "1.0.0",
        "metadata": {
            "test_file.zip": {
                "file": "test_file.zip",
                "path": "test",
                "dtype": "ZIP",
                "telescope": "ALMA",
                "size": "100",
                "mode": "test",
            }
        },
    }
    meta_dir = tmp_path / ".cloudflare"
    meta_dir.mkdir()
    meta_file = meta_dir / "file.download.json"
    with open(meta_file, "w") as f:
        json.dump(metadata, f)
    return meta_file


def test_version(mock_metadata, monkeypatch, caplog):
    # Mock __file__ in cloudflare to point to our temp directory
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )

    # The toolviper logger doesn't propagate to the root logger (it has its
    # own handler), but caplog captures via the root logger — re-enable
    # propagation for the duration of the test.
    import toolviper.utils.logger

    monkeypatch.setattr(toolviper.utils.logger.get_logger(), "propagate", True)

    with caplog.at_level("INFO"):
        cloudflare.version()
    assert "1.0.0" in caplog.text


@responses.activate
def test_download(mock_metadata, monkeypatch, tmp_path):
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )

    url = "https://downloadnrao.org/test/test_file.zip"
    responses.add(
        responses.GET,
        url,
        body=b"test data",
        status=200,
        headers={"Content-Length": "9"},
    )

    dest_folder = tmp_path / "dest"
    cloudflare.download("test_file.zip", folder=str(dest_folder), decompress=False)

    assert (dest_folder / "test_file.zip").exists()
    with open(dest_folder / "test_file.zip", "rb") as f:
        assert f.read() == b"test data"


def test_get_files(mock_metadata, monkeypatch):
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )
    files = cloudflare.get_files()
    assert "test_file.zip" in files


def test_get_file_size(tmp_path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")

    sizes = cloudflare.get_file_size(str(tmp_path))
    assert sizes["test.txt"] == 5


@responses.activate
def test_update(mock_metadata, monkeypatch, tmp_path):
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )

    url = "https://downloadnrao.org/file.download.json"
    new_metadata = {"version": "1.1.0", "metadata": {}}
    responses.add(responses.GET, url, json=new_metadata, status=200)

    update_path = tmp_path / "update_dir"
    update_path.mkdir()
    cloudflare.update(path=str(update_path))

    assert (update_path / "file.download.json").exists()


def test_list_files(mock_metadata, monkeypatch):
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )
    # list_files returns pd.DataFrame or None (if it prints)
    # By default it might try to use itables or tabulate
    df = cloudflare.list_files()
    if df is not None:
        assert isinstance(df, pd.DataFrame)
        assert "test_file.zip" in df["file"].values


def _make_task(tmp_path, filename, size=0):
    return {
        "metadata": {"file": filename, "path": "test"},
        "folder": str(tmp_path),
        "visible": True,
        "size": size,
    }


class _FakeResponse:
    def __init__(self, chunks, content_length=None):
        self._chunks = chunks
        self.headers = (
            {"Content-Length": str(content_length)}
            if content_length is not None
            else {}
        )

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        _ = chunk_size
        yield from self._chunks

    def close(self):
        return None


def test_worker_uses_configured_timeouts(monkeypatch, tmp_path):
    captured = {}

    def fake_get(url, stream, headers, timeout):
        captured["url"] = url
        captured["stream"] = stream
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse([b"x"], content_length=1)

    monkeypatch.setattr(cloudflare.requests, "get", fake_get)

    task = _make_task(tmp_path, "timeout_test.zip", size=1)
    cloudflare.worker(task_id=0, task=task, progress=None, decompress=False)

    assert captured["url"] == "https://downloadnrao.org/test/timeout_test.zip"
    assert captured["stream"] is True
    assert captured["headers"] == {"user-agent": cloudflare.USER_AGENT}
    assert captured["timeout"] == (
        cloudflare.DOWNLOAD_CONNECT_TIMEOUT,
        cloudflare.DOWNLOAD_READ_TIMEOUT,
    )
    assert "error" not in task
    assert (tmp_path / "timeout_test.zip").exists()


def test_worker_aborts_stalled_download(monkeypatch, tmp_path):
    """A trickling connection (slow but never idle) must be detected and retried."""
    clock = [0.0]
    attempts = {"n": 0}

    class TrickleResponse(_FakeResponse):
        def iter_content(self, chunk_size):
            # A few bytes arrive just past the grace period: far below the
            # minimum average rate, but never idle long enough for the
            # requests read timeout to fire.
            clock[0] += cloudflare.DOWNLOAD_STALL_GRACE_PERIOD + 1
            yield b"x" * 10
            clock[0] += 1
            yield b"x" * 10

    def fake_get(url, stream, headers, timeout):
        attempts["n"] += 1
        return TrickleResponse([], content_length=10 * 1024 * 1024)

    monkeypatch.setattr(cloudflare.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cloudflare.time, "sleep", lambda _: None)
    monkeypatch.setattr(cloudflare.requests, "get", fake_get)

    task = _make_task(tmp_path, "stall_test.zip")
    cloudflare.worker(task_id=0, task=task, progress=None, decompress=False)

    assert attempts["n"] == cloudflare.DOWNLOAD_MAX_ATTEMPTS
    assert "fell below" in task["error"]
    assert not (tmp_path / "stall_test.zip").exists()


def test_worker_detects_truncated_stream(monkeypatch, tmp_path):
    attempts = {"n": 0}

    def fake_get(url, stream, headers, timeout):
        attempts["n"] += 1
        return _FakeResponse([b"1234"], content_length=9)

    monkeypatch.setattr(cloudflare.time, "sleep", lambda _: None)
    monkeypatch.setattr(cloudflare.requests, "get", fake_get)

    task = _make_task(tmp_path, "truncated_test.zip")
    cloudflare.worker(task_id=0, task=task, progress=None, decompress=False)

    assert attempts["n"] == cloudflare.DOWNLOAD_MAX_ATTEMPTS
    assert "incomplete" in task["error"]
    assert not (tmp_path / "truncated_test.zip").exists()


def test_worker_retries_then_succeeds(monkeypatch, tmp_path):
    attempts = {"n": 0}

    def fake_get(url, stream, headers, timeout):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise cloudflare.requests.ConnectionError("connection reset")
        return _FakeResponse([b"test data"], content_length=9)

    monkeypatch.setattr(cloudflare.time, "sleep", lambda _: None)
    monkeypatch.setattr(cloudflare.requests, "get", fake_get)

    task = _make_task(tmp_path, "retry_test.zip")
    cloudflare.worker(task_id=0, task=task, progress=None, decompress=False)

    assert attempts["n"] == 2
    assert "error" not in task
    assert (tmp_path / "retry_test.zip").read_bytes() == b"test data"


@responses.activate
def test_download_raises_on_failure(mock_metadata, monkeypatch, tmp_path):
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )
    monkeypatch.setattr(cloudflare.time, "sleep", lambda _: None)

    url = "https://downloadnrao.org/test/test_file.zip"
    responses.add(responses.GET, url, status=500)

    with pytest.raises(RuntimeError, match="Download failed"):
        cloudflare.download(
            "test_file.zip", folder=str(tmp_path / "dest"), decompress=False
        )


def test_download_raises_on_unknown_file(mock_metadata, monkeypatch, tmp_path):
    monkeypatch.setattr(
        cloudflare, "__file__", str(mock_metadata.parent.parent / "cloudflare.py")
    )

    with pytest.raises(RuntimeError, match="manifest"):
        cloudflare.download("no_such_file.zip", folder=str(tmp_path))
