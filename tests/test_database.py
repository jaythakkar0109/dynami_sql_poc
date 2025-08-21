import pytest
from unittest.mock import patch
import requests
from app.database import execute_query
from app.settings import settings

# Mock settings to avoid real config dependencies
@pytest.fixture
def mock_settings():
    with patch("app.database.settings") as mock_settings:
        mock_settings.API_URL = "http://fake-api"
        mock_settings.API_PORT = "8080"
        yield mock_settings

# Test successful query execution
@patch("requests.post")
def test_execute_query_success(mock_post, mock_settings):
    mock_post.return_value.json.return_value = {
        "resultTable": {
            "dataSchema": {"columnNames": ["col1"]},
            "rows": [["value1"]]
        }
    }
    mock_post.return_value.raise_for_status = lambda: None
    result = execute_query("SELECT col1 FROM fake_table")
    assert result == [{"col1": "value1"}]
    mock_post.assert_called_once_with(
        "http://fake-api:8080/sql",
        json={"sql": "SELECT col1 FROM fake_table", "trace": False, "queryOptions": ""},
        timeout=30
    )

# Test count query
@patch("requests.post")
def test_execute_query_count(mock_post, mock_settings):
    mock_post.return_value.json.return_value = {"rows": [[5]]}
    mock_post.return_value.raise_for_status = lambda: None
    result = execute_query("SELECT COUNT(*) FROM fake_table", is_count_query=True)
    assert result == [{"count": 5}]

# Test empty count query
@patch("requests.post")
def test_execute_query_count_empty(mock_post, mock_settings):
    mock_post.return_value.json.return_value = {"rows": []}
    mock_post.return_value.raise_for_status = lambda: None
    result = execute_query("SELECT COUNT(*) FROM fake_table", is_count_query=True)
    assert result == [{"count": 0}]

# Test API request failure
@patch("requests.post")
def test_execute_query_request_error(mock_post, mock_settings):
    mock_post.side_effect = requests.exceptions.RequestException("API error")
    with pytest.raises(requests.exceptions.RequestException):
        execute_query("SELECT col1 FROM fake_table")

# Test invalid response format
@patch("requests.post")
def test_execute_query_invalid_response(mock_post, mock_settings):
    mock_post.return_value.json.return_value = {}  # Missing resultTable
    mock_post.return_value.raise_for_status = lambda: None
    with pytest.raises(ValueError, match="Invalid response format from API"):
        execute_query("SELECT col1 FROM fake_table")

# Test query with parameters
@patch("app.database._format_query_with_params")
@patch("requests.post")
def test_execute_query_with_params(mock_post, mock_format_query, mock_settings):
    mock_format_query.return_value = "SELECT col1 FROM fake_table WHERE id = %s"
    mock_post.return_value.json.return_value = {
        "resultTable": {
            "dataSchema": {"columnNames": ["col1"]},
            "rows": [["value1"]]
        }
    }
    mock_post.return_value.raise_for_status = lambda: None
    result = execute_query("SELECT col1 FROM fake_table WHERE id = %s", params=[42])
    assert result == [{"col1": "value1"}]
    mock_format_query.assert_called_once_with("SELECT col1 FROM fake_table WHERE id = %s", [42])