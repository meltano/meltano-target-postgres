import json

from target_postgres.cli import main


class TestAbout:
    def test_about_prints_json_with_batch_capability(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["target-postgres", "--about"])
        main()

        output = json.loads(capsys.readouterr().out)
        assert output["name"] == "target-postgres"
        assert "batch" in output["capabilities"]

    def test_about_exits_before_reading_stdin(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["target-postgres", "--about"])

        def _fail_persist_lines(*args, **kwargs):
            raise AssertionError("persist_lines should not be called with --about")

        monkeypatch.setattr("target_postgres.cli.persist_lines", _fail_persist_lines)
        main()  # should not raise
        assert "batch" in json.loads(capsys.readouterr().out)["capabilities"]
