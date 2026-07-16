from target_postgres.schema import column_type, flatten_record, flatten_schema


class TestColumnType:
    def test_object_is_jsonb(self):
        assert column_type({"type": ["object"]}) == "jsonb"

    def test_array_is_jsonb(self):
        assert column_type({"type": ["array"]}) == "jsonb"

    def test_date_time_format(self):
        assert column_type({"type": ["string"], "format": "date-time"}) == "timestamp without time zone"

    def test_time_format(self):
        assert column_type({"type": ["string"], "format": "time"}) == "time without time zone"

    def test_number(self):
        assert column_type({"type": ["number", "null"]}) == "double precision"

    def test_integer_and_string_union_is_ambiguous_varchar(self):
        assert column_type({"type": ["integer", "string"]}) == "character varying"

    def test_integer_small_max(self):
        assert column_type({"type": ["integer"], "maximum": 32767}) == "smallint"

    def test_integer_medium_max(self):
        assert column_type({"type": ["integer"], "maximum": 2147483647}) == "integer"

    def test_integer_large_max(self):
        assert column_type({"type": ["integer"], "maximum": 9223372036854775807}) == "bigint"

    def test_integer_no_max_is_unbounded_numeric(self):
        assert column_type({"type": ["integer"]}) == "numeric"

    def test_integer_max_exceeding_all_bounds_falls_back_to_numeric(self):
        # SPEC.md §4 point 6: original has a fallthrough bug here (leaves
        # character varying); this reimplementation fixes it to numeric.
        assert column_type({"type": ["integer"], "maximum": 10**30}) == "numeric"

    def test_boolean(self):
        assert column_type({"type": ["boolean", "null"]}) == "boolean"

    def test_default_string_no_format(self):
        assert column_type({"type": ["string", "null"]}) == "character varying"

    def test_plain_string_type_not_list(self):
        assert column_type({"type": "string"}) == "character varying"

    def test_no_type_key(self):
        assert column_type({}) == "character varying"


class TestFlattenSchema:
    def test_flat_properties(self):
        properties = {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}
        columns = flatten_schema(properties, {})
        names = {c["name"] for c in columns}
        assert names == {"id", "name"}

    def test_nested_object_flattens_within_max_level(self):
        properties = {
            "address": {
                "type": ["object"],
                "properties": {
                    "city": {"type": ["string"]},
                    "zip": {"type": ["string"]},
                },
            }
        }
        columns = flatten_schema(properties, {"data_flattening_max_level": 1})
        names = {c["name"] for c in columns}
        assert names == {"address__city", "address__zip"}

    def test_nested_object_falls_back_to_jsonb_past_max_level(self):
        properties = {
            "address": {
                "type": ["object"],
                "properties": {"city": {"type": ["string"]}},
            }
        }
        columns = flatten_schema(properties, {"data_flattening_max_level": 0})
        assert len(columns) == 1
        assert columns[0]["name"] == "address"
        assert column_type(columns[0]["schema"]) == "jsonb"

    def test_array_never_recursed_into(self):
        properties = {
            "tags": {
                "type": ["array"],
                "items": {"type": "object", "properties": {"x": {"type": "string"}}},
            }
        }
        columns = flatten_schema(properties, {"data_flattening_max_level": 5})
        assert len(columns) == 1
        assert columns[0]["name"] == "tags"

    def test_underscore_camel_case_fields(self):
        properties = {"HTTPHeader_Value": {"type": ["string"]}}
        columns = flatten_schema(properties, {"underscore_camel_case_fields": True})
        assert columns[0]["name"] == "http_header__value"

    def test_dedup_suffix_on_name_collision(self):
        properties = {
            "a__b": {"type": ["string"]},
            "a": {
                "type": ["object"],
                "properties": {"b": {"type": ["string"]}},
            },
        }
        columns = flatten_schema(properties, {"data_flattening_max_level": 1})
        names = sorted(c["name"] for c in columns)
        assert names == ["a__b", "a__b__1"]


class TestFlattenRecord:
    def test_extracts_flat_values(self):
        properties = {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}
        columns = flatten_schema(properties, {})
        record = {"id": 1, "name": "Ada"}
        assert flatten_record(record, columns) == {"id": 1, "name": "Ada"}

    def test_missing_keys_are_omitted(self):
        properties = {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}
        columns = flatten_schema(properties, {})
        record = {"id": 1}
        assert flatten_record(record, columns) == {"id": 1}

    def test_nested_values_extracted_by_path(self):
        properties = {
            "address": {
                "type": ["object"],
                "properties": {"city": {"type": ["string"]}},
            }
        }
        columns = flatten_schema(properties, {"data_flattening_max_level": 1})
        record = {"address": {"city": "Berlin"}}
        assert flatten_record(record, columns) == {"address__city": "Berlin"}

    def test_object_past_max_level_passed_through_raw(self):
        properties = {
            "address": {
                "type": ["object"],
                "properties": {"city": {"type": ["string"]}},
            }
        }
        columns = flatten_schema(properties, {"data_flattening_max_level": 0})
        record = {"address": {"city": "Berlin"}}
        assert flatten_record(record, columns) == {"address": {"city": "Berlin"}}
