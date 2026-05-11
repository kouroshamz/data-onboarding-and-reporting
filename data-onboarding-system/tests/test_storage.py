"""Tests for local storage module."""

import json
import pytest
from pathlib import Path

from app.storage.local import LocalStorage


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(root=tmp_path)


def test_save_and_load_json(storage):
    data = {"tables": 5, "score": 87.3}
    path = storage.save_json("run001", "report", data)
    assert path.exists()

    loaded = storage.load_json("run001", "report")
    assert loaded == data


def test_save_text(storage):
    path = storage.save_text("run001", "summary", "Hello world", ext="txt")
    assert path.exists()
    assert path.read_text() == "Hello world"


def test_save_bytes(storage):
    path = storage.save_bytes("run001", "report", b"PDF_CONTENT", ext="pdf")
    assert path.exists()
    assert path.read_bytes() == b"PDF_CONTENT"


def test_list_runs(storage):
    storage.save_json("run_a", "x", {})
    storage.save_json("run_b", "y", {})
    runs = storage.list_runs()
    assert "run_a" in runs
    assert "run_b" in runs


def test_list_artifacts(storage):
    storage.save_json("run01", "schema", {})
    storage.save_text("run01", "report", "hi", ext="txt")
    artifacts = storage.list_artifacts("run01")
    assert "schema.json" in artifacts
    assert "report.txt" in artifacts


def test_load_missing_raises(storage):
    with pytest.raises(FileNotFoundError):
        storage.load_json("nonexistent", "data")


def test_get_path(storage):
    p = storage.get_path("run01", "file.json")
    assert p.name == "file.json"
    assert "run01" in str(p)
