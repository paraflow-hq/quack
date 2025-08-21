from pathlib import Path

from quack.utils.checksummer import Checksummer


class TestChecksummer:
    def test_checksumer(self, tmp_path: Path):
        tmp_file = tmp_path / "test.txt"
        _ = tmp_file.write_text("test")

        tmp_checksum = tmp_path / "sha256sums.txt"
        Checksummer.generate(str(tmp_file), str(tmp_checksum))
        assert tmp_checksum.exists()

        Checksummer.verify(str(tmp_file), str(tmp_checksum))
