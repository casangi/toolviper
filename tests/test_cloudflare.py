import os
import pathlib
import json
import pytest
import responses
from toolviper.utils.data import cloudflare
import pandas as pd


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
