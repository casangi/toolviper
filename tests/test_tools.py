import pytest
import json
import pathlib
import hashlib
import os
from unittest.mock import MagicMock, patch
from toolviper.utils.tools import (
    open_json,
    calculate_checksum,
    iter_files_,
    update_hash,
    verify,
    process_entry_,
    add_entry,
    update_version,
    ChecksumError,
)


@pytest.fixture
def temp_json_file(tmp_path):
    data = {"version": "v1.0.0", "metadata": {}}
    file_path = tmp_path / "test.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    return file_path


@pytest.fixture
def temp_data_file(tmp_path):
    file_path = tmp_path / "test_file.txt"
    content = b"hello world"
    file_path.write_bytes(content)
    # sha256 of "hello world"
    expected_hash = hashlib.sha256(content).hexdigest()
    return file_path, expected_hash


def test_open_json_success(temp_json_file):
    data = open_json(str(temp_json_file))
    assert data["version"] == "v1.0.0"


def test_open_json_not_found():
    with pytest.raises(FileNotFoundError):
        open_json("non_existent_file.json")


def test_calculate_checksum(temp_data_file):
    file_path, expected_hash = temp_data_file
    assert calculate_checksum(str(file_path)) == expected_hash


def test_iter_files_(tmp_path):
    (tmp_path / "file1.txt").write_text("1")
    (tmp_path / "file2.txt").write_text("2")
    files = list(iter_files_(str(tmp_path)))
    assert set(files) == {"file1.txt", "file2.txt"}


def test_iter_files_not_found():
    with pytest.raises(FileNotFoundError):
        list(iter_files_("non_existent_path"))


def test_update_hash(tmp_path):
    # Setup manifest
    manifest_path = tmp_path / "manifest.json"
    manifest_data = {"metadata": {"test_file": {"hash": "old_hash"}}}
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)

    # Setup data file
    data_file = tmp_path / "test_file"
    data_file.write_text("new content")
    new_hash = hashlib.sha256(b"new content").hexdigest()

    update_hash(str(manifest_path), str(tmp_path))

    updated_manifest = open_json(str(manifest_path))
    assert updated_manifest["metadata"]["test_file"]["hash"] == new_hash


def test_verify_success(tmp_path, monkeypatch):
    # Setup manifest in a place where verify can find it (mocking toolviper.__file__)
    manifest_dir = tmp_path / "utils/data/.cloudflare"
    manifest_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "file.download.json"

    data_file = tmp_path / "test.zip"
    data_file.write_text("zip content")
    expected_hash = hashlib.sha256(b"zip content").hexdigest()

    manifest_data = {"metadata": {"test": {"hash": expected_hash}}}
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)

    import toolviper

    monkeypatch.setattr(toolviper, "__file__", str(tmp_path / "__init__.py"))
    (tmp_path / "__init__.py").touch()

    # verify(filename, folder)
    # verify handles .zip extension by stripping it
    verify("test.zip", str(tmp_path))


def test_verify_checksum_error(tmp_path, monkeypatch):
    manifest_dir = tmp_path / "utils/data/.cloudflare"
    manifest_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "file.download.json"

    data_file = tmp_path / "test.zip"
    data_file.write_text("wrong content")

    manifest_data = {"metadata": {"test": {"hash": "expected_but_different_hash"}}}
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)

    import toolviper

    monkeypatch.setattr(toolviper, "__file__", str(tmp_path / "__init__.py"))
    (tmp_path / "__init__.py").touch()

    with pytest.raises(ChecksumError):
        verify("test.zip", str(tmp_path))


def test_update_version():
    with (
        patch("toolviper.utils.tools.open_json") as mock_open,
        patch("pathlib.Path.exists", return_value=True),
    ):

        mock_open.return_value = {"version": "v1.2.3"}

        # current implementation doesn't reset other parts
        assert update_version("major") == "v2.2.3"
        assert update_version("minor") == "v1.3.3"
        assert update_version("patch") == "v1.2.4"
        assert update_version("unknown") is None


def test_process_entry_(tmp_path):
    json_file = {"metadata": {}}
    test_file = tmp_path / "test.zip"
    test_file.write_text("content")
    file_hash = hashlib.sha256(b"content").hexdigest()

    with patch("toolviper.utils.data.get_file_size", return_value={"test": 123}):
        process_entry_(
            file=str(test_file),
            path="verification",
            dtype="int",
            telescope="VLA",
            mode="test",
            json_file=json_file,
        )

    assert "test" in json_file["metadata"]
    assert json_file["metadata"]["test"]["hash"] == file_hash
    assert json_file["metadata"]["test"]["size"] == "123"


def test_add_entry(tmp_path, monkeypatch):
    manifest_dir = tmp_path / "utils/data/.cloudflare"
    manifest_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "file.download.json"
    manifest_data = {"version": "v1.0.0", "metadata": {}}
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)

    import toolviper

    monkeypatch.setattr(toolviper, "__path__", [str(tmp_path)])

    test_file = tmp_path / "new_file.zip"
    test_file.write_text("content")

    entry = {
        "file": str(test_file),
        "path": "verification",
        "dtype": "int",
        "telescope": "VLA",
        "mode": "test",
    }

    with patch("toolviper.utils.data.get_file_size", return_value={"new_file": 456}):
        result = add_entry(entries=[entry], manifest=str(manifest_path))

    assert result["version"] == "v1.0.1"
    assert "new_file" in result["metadata"]



def test_checksum_error_str():
    error = ChecksumError("msg", "file.txt", "/folder", 10)
    assert "[10]: There was an error verifying the checksum of /folder/file.txt" in str(
        error
    )
