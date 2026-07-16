import pytest

from target_postgres.exceptions import TargetSchemaNotFoundException
from target_postgres.naming import (
    deduplicate_name,
    flatten_key,
    inflect_name,
    resolve_grantees,
    resolve_indices,
    resolve_target_schema,
    safe_table_name,
    stream_name_to_dict,
    temp_table_name,
    underscore,
)


class TestStreamNameToDict:
    def test_single_segment(self):
        assert stream_name_to_dict("orders") == {
            "catalog_name": None,
            "schema_name": None,
            "table_name": "orders",
        }

    def test_two_segments(self):
        assert stream_name_to_dict("public-orders") == {
            "catalog_name": None,
            "schema_name": "public",
            "table_name": "orders",
        }

    def test_three_segments(self):
        assert stream_name_to_dict("mydb-public-orders") == {
            "catalog_name": "mydb",
            "schema_name": "public",
            "table_name": "orders",
        }

    def test_four_plus_segments_rejoin_with_underscore(self):
        assert stream_name_to_dict("mydb-public-orders-detail") == {
            "catalog_name": "mydb",
            "schema_name": "public",
            "table_name": "orders_detail",
        }


class TestTargetSchemaResolution:
    def test_uses_schema_mapping_when_present(self):
        config = {
            "schema_mapping": {"public": {"target_schema": "mapped_schema"}},
            "default_target_schema": "default_schema",
        }
        assert resolve_target_schema("public", config) == "mapped_schema"

    def test_falls_back_to_default(self):
        config = {"default_target_schema": "default_schema"}
        assert resolve_target_schema("public", config) == "default_schema"
        assert resolve_target_schema(None, config) == "default_schema"

    def test_raises_when_unresolvable(self):
        with pytest.raises(TargetSchemaNotFoundException):
            resolve_target_schema("public", {})

    def test_grantees_prefer_schema_mapping(self):
        config = {
            "schema_mapping": {
                "public": {
                    "target_schema": "s",
                    "target_schema_select_permissions": ["role_a"],
                }
            },
            "default_target_schema_select_permissions": "role_b",
        }
        assert resolve_grantees("public", config) == ["role_a"]

    def test_grantees_fall_back_to_default(self):
        config = {"default_target_schema_select_permissions": "role_b"}
        assert resolve_grantees("public", config) == "role_b"
        assert resolve_grantees(None, config) == "role_b"

    def test_indices_from_schema_mapping(self):
        config = {"schema_mapping": {"public": {"target_schema": "s", "indices": {"orders": ["customer_id"]}}}}
        assert resolve_indices("public", "orders", config) == ["customer_id"]
        assert resolve_indices("public", "other_table", config) == []
        assert resolve_indices(None, "orders", config) == []


class TestIdentifierNaming:
    def test_safe_table_name_replaces_dots_and_dashes(self):
        assert safe_table_name("My.Table-Name") == "my_table_name"

    def test_temp_table_name_shape(self):
        name = temp_table_name()
        assert name.startswith("tmp_")
        assert "-" not in name

    def test_underscore_basic(self):
        assert underscore("SomeValue") == "some_value"
        assert underscore("some-value") == "some_value"

    def test_inflect_name_collapses_capital_runs(self):
        # SPEC.md §5 worked example.
        assert inflect_name("HTTPHeader_Value") == "http_header__value"

    def test_flatten_key_short_name_unaffected(self):
        assert flatten_key(["parent", "child"]) == "parent__child"

    def test_flatten_key_truncates_long_name(self):
        parts = [
            "a_very_long_parent_segment_name_indeed",
            "another_long_child_segment_value",
            "leaf",
        ]
        name = flatten_key(parts)
        assert len(name) <= 63

    def test_deduplicate_name_appends_suffix_on_collision(self):
        seen: dict = {}
        first = deduplicate_name("col", seen)
        second = deduplicate_name("col", seen)
        third = deduplicate_name("col", seen)
        assert first == "col"
        assert second == "col__1"
        assert third == "col__2"

    def test_deduplicate_name_no_collision(self):
        seen: dict = {}
        assert deduplicate_name("a", seen) == "a"
        assert deduplicate_name("b", seen) == "b"
