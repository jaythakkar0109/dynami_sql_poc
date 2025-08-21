from fastapi import APIRouter, HTTPException, status
from app.schemas import GetDataParams, QueryResponse, AttributeResponse, GetAttributesRequest, GetAttributesResponse
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

@router.post("/rates/risk/get-attributes", response_model=GetAttributesResponse)
async def get_attributes(params: GetAttributesRequest):
    query_id = str(uuid.uuid4())
    try:
        sql_builder = SQLBuilder()

        # Validate columns and filters
        all_errors = sql_builder._validate_columns(params.columns)
        if params.filterBy:
            _, column_to_table_map = sql_builder._get_explicitly_requested_tables(GetDataParams(groupBy=params.columns, filterBy=params.filterBy))
            filter_errors = sql_builder._validate_filter_data_types(params.filterBy, column_to_table_map)
            all_errors.extend(filter_errors)
        if all_errors:
            raise ValidationError(all_errors)

        # Get queries for distinct values
        queries = sql_builder.build_distinct_values_query(params.columns)

        response_data = []
        query_strings = []

        for column, data_type, query, query_params in queries:
            values = []
            final_params = query_params.copy()
            final_query = query

            if query:
                if params.filterBy:
                    where_conditions = []
                    for filter_obj in params.filterBy:
                        condition, filter_values = sql_builder._build_filter_condition(filter_obj, column_to_table_map)
                        if condition:
                            where_conditions.append(condition)
                            final_params.extend(filter_values)
                    if where_conditions:
                        final_query = f"{query} AND {' AND '.join(where_conditions)}"

                results = execute_query(final_query, final_params)
                values = [str(row[column.split('.')[-1]]) for row in results if row[column.split('.')[-1]] is not None]

            query_strings.append(final_query if final_query else f"-- Restricted column: {column}")

            response_data.append(AttributeResponse(
                field=column,
                type=data_type,
                values=values
            ))

        # Combine all queries for response
        final_query_string = "; ".join([q for q in query_strings if q])

        return GetAttributesResponse(
            query_id=query_id,
            data=response_data,
            query=final_query_string
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