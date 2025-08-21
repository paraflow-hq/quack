from quack.utils.archiver import Archiver


class TestArchiver:
    def test_archiver(self, tmp_path):
        tmp_file = tmp_path / "test.txt"
        tmp_file.write_text("test")

        tmp_archive = tmp_path / "test.tar.gz"
        Archiver.archive([str(tmp_file)], str(tmp_archive))
        assert tmp_archive.exists()

        tmp_file.unlink()
        Archiver.extract(str(tmp_archive), "/")
        assert tmp_file.exists()
