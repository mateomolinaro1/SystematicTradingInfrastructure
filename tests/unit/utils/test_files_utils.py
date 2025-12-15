from packages.files_utils import FileUtils


def test_delete_all_files(tmp_path):
    f1 = tmp_path / "a.txt"
    f1.write_text("x")

    FileUtils.delete_all_files(tmp_path)

    assert not any(tmp_path.iterdir())
