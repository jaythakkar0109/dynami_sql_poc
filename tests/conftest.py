from unittest.mock import patch, mock_open

import pytest
import yaml

from app.schemas import GetDataParams, FilterModel, MeasureModel, SortModel

@pytest.fixture
def sample_get_data_params():
    return GetDataParams(
        measures=[MeasureModel(field="newpnl", function="SUM")],
        groupBy=["uidtype"],
        filterBy=[FilterModel(field="businessdate", operator="EQUAL", values=20230101)],
        sortBy=[SortModel(field="newpnl", order="DESC")],
        page=1,
        page_size=10
    )

@pytest.fixture
def sample_table_config_yaml():
    return """
    SCHEMAS:
      newwpnl:
        schema_name: newwpnl
        schema_fields:
          uidtype:
            field_aliases: [ uidtype ]
            field_type: VARCHAR
          businessdate:
            field_aliases: [ businessdate ]
            field_type: INTEGER
          newpnl:
            field_aliases: [ newpnl ]
            field_type: DOUBLE PRECISION
          uid:
            field_aliases: [ uid ]
            field_type: VARCHAR
        priority: 1
        mandatory_fields: [uidtype, businessdate]
        aggregation:
          - field: newpnl
            function: SUM
            alias: total_newpnl
        relations: []
        restricted_attributes: [uid]
    """


@pytest.fixture
def mock_yaml_load(sample_table_config_yaml):
    parsed_yaml = yaml.safe_load(sample_table_config_yaml)
    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data=sample_table_config_yaml)), \
         patch("yaml.safe_load", return_value=parsed_yaml):
        yield

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("API_URL", "http://api")
    monkeypatch.setenv("API_PORT", "8080")
    yield