import pytest
import yaml
from app.sql_builder import SQLBuilder, ValidationError
from app.schemas import GetDataParams, MeasureModel, FilterModel, SortModel

def test_yaml_fixture(sample_table_config_yaml):
    parsed_yaml = yaml.safe_load(sample_table_config_yaml)
    assert parsed_yaml is not None
    assert "SCHEMAS" in parsed_yaml
    assert "newwpnl" in parsed_yaml["SCHEMAS"]

def test_sql_builder_init(mock_yaml_load):
    builder = SQLBuilder()
    assert "newwpnl" in builder.table_configs, f"table_configs is empty: {builder.table_configs}"
    assert builder.table_configs["newwpnl"].priority == 1
    assert "uid" in builder.table_configs["newwpnl"].restricted_attributes

def test_validate_columns_valid(mock_yaml_load):
    builder = SQLBuilder()
    errors = builder._validate_columns(["uidtype", "newpnl"])
    assert not errors, f"Unexpected errors: {errors}"

def test_validate_columns_invalid(mock_yaml_load):
    builder = SQLBuilder()
    errors = builder._validate_columns(["col"])
    assert len(errors) == 1
    assert "Column 'col' not found in any table" in errors[0]["message"]

def test_build_query_aggregated(mock_yaml_load):
    builder = SQLBuilder(count_strategy="simple")
    params = GetDataParams(
        measures=[MeasureModel(field="newpnl", function="SUM")],
        groupBy=["uidtype"],
        filterBy=[FilterModel(field="businessdate", operator="EQUAL", values=20230101)],
        sortBy=[SortModel(field="newpnl", order="DESC")],
        page=1,
        page_size=10
    )
    main_query, main_params, count_query, count_params = builder.build_query(params)
    assert "SELECT uidtype, businessdate, uidtype" in main_query
    assert "SUM(newpnl) AS sum_newpnl" in main_query
    assert "FROM newwpnl" in main_query
    assert "WHERE (businessdate = ?)" in main_query
    assert "GROUP BY uidtype, businessdate" in main_query
    assert "ORDER BY newpnl DESC" in main_query
    assert "LIMIT 10" in main_query
    assert main_params == [20230101]
    assert "SELECT COUNT(*)" in count_query
    assert "FROM newwpnl" in count_query
    assert count_params == [20230101]

def test_build_query_validation_error(mock_yaml_load):
    builder = SQLBuilder()
    params = GetDataParams(measures=[MeasureModel(field="col", function="SUM")])
    with pytest.raises(ValidationError) as exc:
        builder.build_query(params)
    assert len(exc.value.errors) == 2
    assert "col" in exc.value.errors[0]["message"]

def test_build_distinct_values_query_restricted(mock_yaml_load):
    builder = SQLBuilder()
    queries = builder.build_distinct_values_query(["uid"])
    assert len(queries) == 1
    assert queries[0][0] == "uid"
    assert queries[0][2] is None  # Restricted column, no query
    assert queries[0][3] == []

def test_build_distinct_values_query_valid(mock_yaml_load):
    builder = SQLBuilder()
    queries = builder.build_distinct_values_query(["uidtype"])
    assert len(queries) == 1
    assert queries[0][0] == "uidtype"
    assert queries[0][1] == "VARCHAR"
    assert "SELECT DISTINCT uidtype FROM newwpnl WHERE uidtype IS NOT NULL" in queries[0][2]
    assert queries[0][3] == []

def test_build_count_query_distinct(mock_yaml_load):
    builder = SQLBuilder(count_strategy="distinct")
    params = GetDataParams(groupBy=["uidtype"])
    main_query, main_params, count_query, count_params = builder.build_query(params)
    assert "SELECT DISTINCT uidtype" in main_query
    assert "COUNT(DISTINCT uidtype)" in count_query