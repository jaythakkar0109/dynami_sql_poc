from typing import List

from fastapi import APIRouter, HTTPException, status
from app.schemas import GetDataParams, QueryResponse, AttributeResponse, GetAttributesRequest
from app.sql_builder import SQLBuilder, ValidationError
from app.database import execute_query
import logging
import uuid

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/rates/risk/get-data", response_model=QueryResponse)
async def execute_dynamic_query(params: GetDataParams):
    query_id = str(uuid.uuid4())
    try:
        sql_builder = SQLBuilder()
        main_query, main_params, count_query, count_params = sql_builder.build_query(params)
        total_count = 0
        if count_query:
            count_result = execute_query(count_query, count_params)
            total_count = count_result[0]["count"] if count_result else 0
        results = execute_query(main_query, main_params)
        return QueryResponse(
            query_id=query_id,
            data=results,
            page=params.page,
            page_size=params.page_size,
            total_count=total_count,
            query=main_query
        )
    except ValidationError as e:
        logger.error(f"Query ID: {query_id} - Validation error: {e.errors}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"query_id": query_id, "errors": e.errors}
        )
    except Exception as e:
        logger.error(f"Query ID: {query_id} - Query execution error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"query_id": query_id, "errors": [{"message": f"Internal server error: {str(e)}"}]}
        )

@router.post("/rates/risk/get-attributes", response_model=List[AttributeResponse])
async def get_attributes(params: GetAttributesRequest):
    query_id = str(uuid.uuid4())
    try:
        sql_builder = SQLBuilder()

        # Get queries for distinct values
        queries = sql_builder.build_distinct_values_query(params.columns)

        response = []
        for column, data_type, query, query_params in queries:
            values = []
            if query:
                results = execute_query(query, query_params)
                values = [str(row[column]) for row in results if row[column] is not None]

            response.append(AttributeResponse(
                field=column,
                type=data_type,
                values=values
            ))

        return response

    except ValidationError as e:
        logger.error(f"Query ID: {query_id} - Validation error: {e.errors}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"query_id": query_id, "errors": e.errors}
        )
    except Exception as e:
        logger.error(f"Query ID: {query_id} - Query execution error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"query_id": query_id, "errors": [{"message": f"Internal server error: {str(e)}"}]}
        )