import math

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from target_postgres.arrow_batch import ArrowValueError, build_adbc_uri, read_manifest_tables, strip_file_uri


class TestStripFileUri:
    def test_strips_file_prefix(self):
        assert strip_file_uri(["file:///tmp/a.arrow"]) == ["/tmp/a.arrow"]

    def test_leaves_plain_paths_unchanged(self):
        assert strip_file_uri(["/tmp/a.arrow"]) == ["/tmp/a.arrow"]

    def test_multiple_entries(self):
        assert strip_file_uri(["file:///a", "/b", "file:///c"]) == ["/a", "/b", "/c"]


class TestReadManifestTables:
    def _write_arrow_file(self, path, data: dict):
        table = pa.table(data)
        with ipc.new_file(str(path), table.schema) as writer:
            writer.write_table(table)
        return path

    def test_reads_single_file(self, tmp_path):
        path = self._write_arrow_file(tmp_path / "a.arrow", {"id": [1, 2], "name": ["a", "b"]})
        table = read_manifest_tables([str(path)])
        assert table.num_rows == 2
        assert table.column_names == ["id", "name"]

    def test_concatenates_multiple_files(self, tmp_path):
        path1 = self._write_arrow_file(tmp_path / "a.arrow", {"id": [1], "name": ["a"]})
        path2 = self._write_arrow_file(tmp_path / "b.arrow", {"id": [2], "name": ["b"]})
        table = read_manifest_tables([str(path1), str(path2)])
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]

    def test_list_column_is_json_encoded_as_string(self, tmp_path):
        lists = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int32()))
        path = self._write_arrow_file(tmp_path / "a.arrow", {"lists": lists})
        table = read_manifest_tables([str(path)])
        assert table.schema.field("lists").type == pa.string()
        assert table.column("lists").to_pylist() == ["[1, 2]", "[3, 4]"]

    def test_struct_column_is_json_encoded_as_string(self, tmp_path):
        structs = pa.array(
            [{"database": "db", "collection": "a"}, None],
            type=pa.struct([("database", pa.string()), ("collection", pa.string())]),
        )
        path = self._write_arrow_file(tmp_path / "a.arrow", {"id": [1, 2], "namespace": structs})
        table = read_manifest_tables([str(path)])
        assert table.schema.field("namespace").type == pa.string()
        assert table.column("namespace").to_pylist() == ['{"database": "db", "collection": "a"}', None]
        assert table.column("id").to_pylist() == [1, 2]

    def test_nested_struct_with_timestamp_is_json_encoded(self, tmp_path):
        from datetime import datetime

        structs = pa.array(
            [{"inner": {"at": datetime(2020, 1, 2, 3, 4, 5)}}],
            type=pa.struct([("inner", pa.struct([("at", pa.timestamp("us"))]))]),
        )
        path = self._write_arrow_file(tmp_path / "a.arrow", {"obj": structs})
        table = read_manifest_tables([str(path)])
        assert table.column("obj").to_pylist() == ['{"inner": {"at": "2020-01-02 03:04:05"}}']

    def test_struct_with_nan_raises(self, tmp_path):
        structs = pa.array(
            [{"amount": math.pi}, {"amount": math.nan}, {"amount": math.inf}],
            type=pa.struct([("amount", pa.float64())]),
        )
        path = self._write_arrow_file(tmp_path / "a.arrow", {"obj": structs})
        with pytest.raises(
            ArrowValueError,
            match="Column 'obj' of type `struct<amount: double>` has an unsupported value",
        ) as exc_info:
            _ = read_manifest_tables([str(path)])

        assert isinstance(exc_info.value.__cause__, ValueError)
        assert str(exc_info.value.__cause__).startswith("Out of range float values are not JSON compliant")

    def test_does_not_delete_source_files(self, tmp_path):
        path = self._write_arrow_file(tmp_path / "a.arrow", {"id": [1]})
        read_manifest_tables([str(path)])
        assert path.exists()


class TestBuildAdbcUri:
    def test_basic_uri(self):
        config = {"host": "localhost", "port": 5432, "user": "u", "password": "p", "dbname": "db"}
        assert build_adbc_uri(config) == "postgresql://u:p@localhost:5432/db"

    def test_url_encodes_user_and_password(self):
        config = {"host": "h", "port": 5432, "user": "u@x", "password": "p@ss:word", "dbname": "db"}
        uri = build_adbc_uri(config)
        assert "u%40x" in uri
        assert "p%40ss%3Aword" in uri

    def test_ssl_appends_sslmode(self):
        config = {
            "host": "h",
            "port": 5432,
            "user": "u",
            "password": "p",
            "dbname": "db",
            "ssl": True,
        }
        assert build_adbc_uri(config).endswith("?sslmode=require")

    def test_ssl_string_true_is_not_treated_as_boolean(self):
        config = {
            "host": "h",
            "port": 5432,
            "user": "u",
            "password": "p",
            "dbname": "db",
            "ssl": "true",
        }
        assert "sslmode" not in build_adbc_uri(config)
