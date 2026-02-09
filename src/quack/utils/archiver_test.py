import os

from quack.utils.archiver import Archiver


class TestArchiver:
    def test_archiver(self, tmp_path):
        # 基本归档和解压测试
        tmp_file = tmp_path / "test.txt"
        tmp_file.write_text("test")

        tmp_archive = tmp_path / "test.tar.zst"
        Archiver.archive([str(tmp_file)], str(tmp_archive))
        assert tmp_archive.exists()

        tmp_file.unlink()
        Archiver.extract(str(tmp_archive), "/")
        assert tmp_file.exists()

    def test_extract_behavior(self, tmp_path):
        # 创建文件和归档
        tmp_file = tmp_path / "test.txt"
        tmp_file.write_text("original content")
        origin_timestamp = os.path.getmtime(tmp_file)

        tmp_archive = tmp_path / "test.tar.zst"
        Archiver.archive([str(tmp_file)], str(tmp_archive))

        # 测试1: 内容相同时保留时间戳
        # 修改文件时间戳但保持相同内容
        tmp_file.write_text("original content")
        modified_time = os.path.getmtime(tmp_file)
        assert modified_time > origin_timestamp

        # 提取归档，内容相同不应覆盖时间戳
        Archiver.extract(str(tmp_archive), "/")
        assert os.path.getmtime(tmp_file) == modified_time

        # 测试2: 不同内容时覆盖文件并更新时间戳
        # 修改文件内容
        before_content_change = os.path.getmtime(tmp_file)
        tmp_file.write_text("modified content")

        # 提取归档，不同内容应被覆盖
        Archiver.extract(str(tmp_archive), "/")

        # 覆盖后时间戳应该是最新的，以防止 CMake 之类的工具将其视为未修改
        assert tmp_file.read_text() == "original content"
        assert os.path.getmtime(tmp_file) > before_content_change
