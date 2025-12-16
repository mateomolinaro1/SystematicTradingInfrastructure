from SystematicTradingInfra.utils.files_utils import FileUtils
from pathlib import Path
import pytest

def create_files(base_path: Path, filenames):
    for name in filenames:
        (base_path / name).touch()


def test_delete_all_files_except_gitkeep(tmp_path):
    create_files(tmp_path, ["a.txt", "b.csv", ".gitkeep"])

    FileUtils.delete_all_files(tmp_path)

    remaining_files = {f.name for f in tmp_path.iterdir()}
    assert remaining_files == {".gitkeep"}


def test_delete_all_files_including_gitkeep(tmp_path):
    create_files(tmp_path, ["a.txt", ".gitkeep"])

    FileUtils.delete_all_files(tmp_path, except_git_keep=False)

    assert list(tmp_path.iterdir()) == []


def test_does_not_delete_directories(tmp_path):
    create_files(tmp_path, ["a.txt"])
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "inside.txt").touch()

    FileUtils.delete_all_files(tmp_path)

    remaining = {p.name for p in tmp_path.iterdir()}
    assert remaining == {"subdir"}

    assert (subdir / "inside.txt").exists()


def test_raises_error_if_not_directory(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.touch()

    with pytest.raises(ValueError, match="is not a valid directory"):
        FileUtils.delete_all_files(file_path)
