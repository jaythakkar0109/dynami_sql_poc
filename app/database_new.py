import trino
import trino.auth
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
import logging
from app.settings import settings
import psycopg2

logger = logging.getLogger(__name__)

def execute_query(query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Execute a query via API and return results as list of dictionaries"""
    try:
        if params:
            formatted_query = _format_query_with_params(query, params)
        else:
            formatted_query = query

        payload = {
            "sql": formatted_query,
            "trace": False,
            "queryOptions": ""
        }

        api_url = f"{settings.API_URL}:{settings.API_PORT}/sql"

        response = requests.post(api_url, json=payload, timeout=30)
        response.raise_for_status()

        result_data = response.json()
        if "resultTable" not in result_data:
            logger.error("Invalid response format: 'resultTable' key missing")
            raise ValueError("Invalid response format from API")

        result_table = result_data["resultTable"]
        column_names = result_table.get("dataSchema", {}).get("columnNames", [])
        rows = result_table.get("rows", [])

        if not column_names:
            logger.warning("No column names in API response, returning empty result")
            return []

        result = [dict(zip(column_names, row)) for row in rows]
        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"API request error: {e}")
        raise
    except ValueError as e:
        logger.error(f"Response parsing error: {e}")
        raise
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise