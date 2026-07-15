from target_postgres.csv_writer import record_to_csv_row


class TestRecordToCsvRow:
    def test_plain_string_json_encoded(self):
        row = record_to_csv_row({"name": "Ada"}, ["name"])
        # json.dumps("Ada") -> '"Ada"', which then needs CSV wrapping+escaping
        # since it contains the quote character.
        assert row == '"\\"Ada\\""\n'

    def test_none_becomes_empty_field(self):
        row = record_to_csv_row({"name": None}, ["name"])
        assert row == "\n"

    def test_empty_string_becomes_empty_field(self):
        row = record_to_csv_row({"name": ""}, ["name"])
        assert row == "\n"

    def test_zero_is_not_omitted(self):
        row = record_to_csv_row({"count": 0}, ["count"])
        assert row == "0\n"

    def test_false_is_omitted(self):
        row = record_to_csv_row({"flag": False}, ["flag"])
        assert row == "\n"

    def test_true_json_encoded(self):
        row = record_to_csv_row({"flag": True}, ["flag"])
        assert row == "true\n"

    def test_missing_column_becomes_empty_field(self):
        row = record_to_csv_row({}, ["name"])
        assert row == "\n"

    def test_embedded_quote_escaped_with_backslash(self):
        row = record_to_csv_row({"name": 'it\'s "quoted"'}, ["name"])
        # json.dumps escapes the embedded quotes itself first: "it's \"quoted\"".
        # The CSV layer then wraps the whole field in quotes and backslash-escapes
        # every literal quote character (never doubling), giving a mix of single
        # (JSON-added) and now-doubled-up (CSV-added) backslashes before each ".
        assert row == '"\\"it\'s \\\\"quoted\\\\"\\""\n'

    def test_embedded_comma_and_newline_quoted(self):
        row = record_to_csv_row({"name": "a,b\nc"}, ["name"])
        assert row.startswith('"')
        assert "a,b" in row
        assert row == '"\\"a,b\\nc\\""\n'

    def test_nested_dict_value_json_encoded(self):
        row = record_to_csv_row({"data": {"a": 1}}, ["data"])
        assert row == '"{\\"a\\": 1}"\n'

    def test_multiple_columns_order_preserved(self):
        row = record_to_csv_row({"b": 2, "a": 1}, ["a", "b"])
        assert row == "1,2\n"
