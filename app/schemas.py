from pydantic import BaseModel, validator
from typing import List, Optional, Any, Dict
from enum import Enum

class AggregationEnum(str, Enum):
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"

class MeasureModel(BaseModel):
    field: str
    function: AggregationEnum

class FilterModel(BaseModel):
    field: str
    operator: str
    values: Any

    @validator('operator')
    def valid_operator(cls, v):
        valid_operators = {'EQUAL', 'IN', 'BETWEEN', 'INLIST'}
        if v.upper() not in valid_operators:
            raise ValueError(f"Operator must be one of {valid_operators}")
        return v.upper()

    @validator('values')
    def validate_values(cls, v, values):
        operator = values.get('operator', '').upper()
        if operator == 'BETWEEN':
            if not isinstance(v, list) or len(v) != 2:
                raise ValueError("BETWEEN requires a list of exactly 2 values")
        elif operator in ['IN', 'INLIST']:
            if not isinstance(v, list) or not v:
                raise ValueError("IN/INLIST requires a non-empty list")
        elif operator == 'EQUAL':
            if v is None or isinstance(v, list):
                raise ValueError("EQUAL requires a single value")
        return v

class SortModel(BaseModel):
    field: str
    order: str = "ASC"

    @validator('order')
    def valid_order(cls, v):
        if v.upper() not in ["ASC", "DESC"]:
            raise ValueError("Order must be ASC or DESC")
        return v.upper()

class GetDataParams(BaseModel):
    measures: Optional[List[MeasureModel]] = []
    groupBy: Optional[List[str]] = []
    filterBy: Optional[List[FilterModel]] = []
    sortBy: Optional[List[SortModel]] = []
    page: Optional[int] = 1
    
    # Internal fields for backward compatibility and processing
    page_size: Optional[int] = 10
    complete_result: bool = False

    @validator('page')
    def page_gte_one(cls, v):
        if v is None or v >= 1:
            return v
        raise ValueError("Page must be greater than or equal to 1")

    @validator('page_size')
    def page_size_gte_one(cls, v):
        if v is None or v >= 1:
            return v
        raise ValueError("Page size must be greater than or equal to 1")

    @validator('groupBy')
    def groupBy_check(cls, v):
        # groupBy can be empty for non-aggregated queries
        return v or []

    @validator('measures')
    def measures_check(cls, v):
        # measures can be empty for non-aggregated queries
        return v or []

    @validator('filterBy')
    def filterBy_check(cls, v):
        # filterBy can be empty
        return v or []

    @validator('sortBy')
    def sortBy_check(cls, v):
        # sortBy can be empty
        return v or []

    def has_aggregations(self) -> bool:
        """Check if this request has any aggregations/measures"""
        return bool(self.measures)

    def get_all_columns(self) -> List[str]:
        """Get all columns referenced in the request"""
        columns = []
        
        # Add groupBy columns
        columns.extend(self.groupBy or [])
        
        # Add measure fields
        if self.measures:
            columns.extend([measure.field for measure in self.measures])
        
        # Add filter fields
        if self.filterBy:
            columns.extend([filter_obj.field for filter_obj in self.filterBy])
        
        # Add sort fields
        if self.sortBy:
            columns.extend([sort_obj.field for sort_obj in self.sortBy])
        
        return list(set(columns))  # Remove duplicates

class GetDataRecordsParams(BaseModel):
    """For non-aggregated record retrieval"""
    columns: List[str]
    filterBy: Optional[List[FilterModel]] = []
    sortBy: Optional[List[SortModel]] = []
    page: Optional[int] = 1
    
    # Internal fields
    page_size: Optional[int] = 10
    complete_result: bool = False

    @validator('page')
    def page_gte_one(cls, v):
        if v is None or v >= 1:
            return v
        raise ValueError("Page must be greater than or equal to 1")

    @validator('page_size')
    def page_size_gte_one(cls, v):
        if v is None or v >= 1:
            return v
        raise ValueError("Page size must be greater than or equal to 1")

    @validator('columns')
    def columns_check(cls, v):
        if not v:
            raise ValueError("Columns must be a non-empty list for record retrieval")
        return v

    @validator('filterBy')
    def filterBy_check(cls, v):
        return v or []

    @validator('sortBy')
    def sortBy_check(cls, v):
        return v or []

    def get_all_columns(self) -> List[str]:
        """Get all columns referenced in the request"""
        columns = list(self.columns)
        
        # Add filter fields
        if self.filterBy:
            columns.extend([filter_obj.field for filter_obj in self.filterBy])
        
        # Add sort fields
        if self.sortBy:
            columns.extend([sort_obj.field for sort_obj in self.sortBy])
        
        return list(set(columns))  # Remove duplicates

class QueryResponse(BaseModel):
    data: List[Dict[str, Any]]
    page: int
    page_size: int
    total_count: int = 0
    query: str

# Legacy support - FilterCondition for backward compatibility
class FilterCondition(BaseModel):
    field: str
    operator: str
    values: Any

    @validator('operator')
    def valid_operator(cls, v):
        valid_operators = {'EQUAL', 'IN', 'BETWEEN'}
        if v.upper() not in valid_operators:
            raise ValueError(f"Operator must be one of {valid_operators}")
        return v.upper()

    @validator('values')
    def validate_values(cls, v, values):
        operator = values.get('operator', '').upper()
        if operator == 'BETWEEN':
            if not isinstance(v, list) or len(v) != 2:
                raise ValueError("BETWEEN requires a list of exactly 2 values")
        elif operator == 'IN':
            if not isinstance(v, list) or not v:
                raise ValueError("IN requires a non-empty list")
        elif operator == 'EQUAL':
            if v is None or isinstance(v, list):
                raise ValueError("EQUAL requires a single value")
        return v

# Legacy support - AggregationModel for backward compatibility
class AggregationModel(BaseModel):
    function: AggregationEnum
    column: str
    alias: str