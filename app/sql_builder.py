import os
import yaml
from typing import List, Any, Dict, Tuple, Union
from app.schemas import GetDataParams, FilterModel, MeasureModel, SortModel


class ValidationError(Exception):
    """Custom exception for structured validation errors."""

    def __init__(self, errors: List[Dict]):
        self.errors = errors
        super().__init__("Validation failed")


class TableConfig:
    def __init__(self, name: str, priority: int, columns: List[Dict], relations: List[Dict] = None,
                 mandatory_fields: List[str] = None, aggregations: List[Dict] = None):
        self.name = name
        self.priority = priority
        self.columns = columns or []
        self.relations = relations or []
        self.mandatory_fields = mandatory_fields or []
        self.aggregations = aggregations or []


class JoinRelation:
    def __init__(self, target_table: str, join_type: str, join_columns: List[Dict]):
        self.target_table = target_table
        self.join_type = join_type
        self.join_columns = join_columns


class SQLBuilder:
    def __init__(self, config_path: str = "config/table_config.yaml", count_strategy: str = "simple"):
        """
        Initialize SQLBuilder with configurable count strategy.

        Args:
            config_path: Path to table configuration YAML file
            count_strategy: Strategy for counting records. Options:
                - "simple": Use simple COUNT(*) without nested queries
                - "distinct": Use COUNT(DISTINCT ...) for grouped queries
                - "separate": Execute separate query to get count (requires post-processing)
                - "estimate": Return estimated count based on result set size
        """
        self.query_parts = {
            'select': [],
            'from': '',
            'joins': [],
            'where': [],
            'group_by': [],
            'order_by': '',
        }
        self.parameters = []
        self.table_configs = {}
        self.config_path = config_path
        self.count_strategy = count_strategy
        self._load_table_configurations()

    def _load_table_configurations(self):
        """Load table configurations from YAML file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as file:
                    config_data = yaml.safe_load(file)
                    self._parse_table_configs(config_data)
            else:
                print(f"Warning: Configuration file {self.config_path} not found. Joins will be disabled.")
        except Exception as e:
            print(f"Error loading configuration: {e}")

    def _parse_table_configs(self, config_data: Dict):
        """Parse YAML configuration into table config objects"""
        if 'SCHEMAS' in config_data:
            for schema_name, table_data in config_data['SCHEMAS'].items():
                name = table_data.get('schema_name')
                priority = table_data.get('priority', 999)
                columns = [
                    {'name': field_name, 'field_aliases': field_data.get('field_aliases', []),
                     'field_type': field_data.get('field_type')}
                    for field_name, field_data in table_data.get('schema_fields', {}).items()
                ]
                relations = table_data.get('relations', [])
                mandatory_fields = table_data.get('mandatory_fields', [])
                aggregations = table_data.get('aggregation', [])

                if name:
                    config = TableConfig(name, priority, columns, relations, mandatory_fields, aggregations)
                    self.table_configs[name] = config

    def _validate_columns(self, columns: List[str]) -> List[Dict]:
        """Validate that the given columns exist in the table configurations."""
        errors = []

        for column in columns:
            if '.' in column:
                table_name, col_name = column.split('.', 1)
                if table_name in self.table_configs:
                    table_config = self.table_configs[table_name]
                    if not any(
                            col_def.get('name') == col_name or col_name in col_def.get('field_aliases', []) for col_def
                            in table_config.columns):
                        errors.append({
                            "field": "columns",
                            "message": f"Column '{column}' does not exist in table '{table_name}'"
                        })
                else:
                    errors.append({
                        "field": "columns",
                        "message": f"Table '{table_name}' in column '{column}' not found in configurations"
                    })
            else:
                col_name = column
                found = False
                for table_config in self.table_configs.values():
                    if any(col_def.get('name') == col_name or col_name in col_def.get('field_aliases', []) for col_def
                           in table_config.columns):
                        found = True
                        break
                if not found:
                    errors.append({
                        "field": "columns",
                        "message": f"Column '{column}' not found in any table"
                    })

        return errors

    def _get_column_data_type(self, column: str, column_to_table_map: Dict[str, TableConfig]) -> str:
        """Get the expected data type of a column from the table configuration."""
        table_name = None
        col_name = column

        if '.' in column:
            table_name, col_name = column.split('.', 1)
        elif col_name in column_to_table_map:
            table_config = column_to_table_map[col_name]
            table_name = table_config.name

        if table_name and table_name in self.table_configs:
            table_config = self.table_configs[table_name]
            for col_def in table_config.columns:
                if col_def.get('name') == col_name or col_name in col_def.get('field_aliases', []):
                    return col_def.get('field_type')
        return None

    def _validate_filter_data_types(self, filters: List[FilterModel], column_to_table_map: Dict[str, TableConfig]) -> \
            List[Dict]:
        """Validate the data types of values in filters."""
        errors = []
        type_mapping = {
            'varchar': (str,),
            'integer': (int,),
            'double precision': (float, int),
            'boolean': (bool,),
        }

        for i, filter_obj in enumerate(filters):
            if not isinstance(filter_obj, FilterModel):
                errors.append({
                    "field": "filters",
                    "message": f"Filter at index {i} is not a valid FilterModel: {filter_obj}"
                })
                continue

            column = filter_obj.field
            operator = filter_obj.operator.upper()
            values = filter_obj.values

            expected_type = self._get_column_data_type(column, column_to_table_map)
            if not expected_type:
                continue

            expected_python_types = type_mapping.get(expected_type.lower(), ())
            if not expected_python_types:
                continue

            if operator == 'BETWEEN':
                if not isinstance(values, list) or len(values) != 2:
                    errors.append({
                        "field": "filters",
                        "message": f"Invalid BETWEEN range for column '{column}' at index {i}: must be a list of two values"
                    })
                    continue
                for val in values:
                    if val is None:
                        continue
                    if not isinstance(val, expected_python_types):
                        errors.append({
                            "field": "filters",
                            "message": f"Invalid data type for column '{column}' in BETWEEN range at index {i}",
                            "expected": expected_type,
                            "actual": type(val).__name__,
                            "value": str(val)
                        })
            elif operator in ['IN', 'INLIST']:
                if not isinstance(values, list) or not values:
                    errors.append({
                        "field": "filters",
                        "message": f"Invalid IN/INLIST values for column '{column}' at index {i}: must be a non-empty list"
                    })
                    continue
                for val in values:
                    if val is None:
                        continue
                    if not isinstance(val, expected_python_types):
                        errors.append({
                            "field": "filters",
                            "message": f"Invalid data type for column '{column}' in IN/INLIST at index {i}",
                            "expected": expected_type,
                            "actual": type(val).__name__,
                            "value": str(val)
                        })
            elif operator == 'EQUAL':
                if values is None:
                    continue
                if not isinstance(values, (int, float, str, bool)):
                    errors.append({
                        "field": "filters",
                        "message": f"Invalid value for column '{column}' with EQUAL operator at index {i}: must be a single value",
                        "expected": expected_type,
                        "actual": type(values).__name__,
                        "value": str(values)
                    })
            else:
                errors.append({
                    "field": "filters",
                    "message": f"Unsupported operator '{operator}' for column '{column}' at index {i}. Supported operators: EQUAL, IN, INLIST, BETWEEN"
                })

        return errors

    def _validate_measures(self, measures: List[MeasureModel]) -> List[Dict]:
        """Validate measure fields and functions."""
        errors = []
        valid_functions = {'SUM', 'COUNT', 'AVG', 'MAX', 'MIN'}

        for i, measure in enumerate(measures):
            if not isinstance(measure, MeasureModel):
                errors.append({
                    "field": "measures",
                    "message": f"Measure at index {i} is not a valid MeasureModel: {measure}"
                })
                continue

            column_errors = self._validate_columns([measure.field])
            if column_errors:
                errors.extend([
                    {**error, "field": "measures", "message": error["message"].replace("columns", "measures")}
                    for error in column_errors
                ])

            if measure.function.upper() not in valid_functions:
                errors.append({
                    "field": "measures",
                    "message": f"Invalid aggregation function '{measure.function}' for field '{measure.field}' at index {i}. Valid functions: {', '.join(valid_functions)}"
                })

        return errors

    def _get_relations_for_table(self, table_config: TableConfig) -> List[Dict]:
        """Get all relations for a table"""
        return table_config.relations

    def _get_reverse_relations_for_table(self, table_config: TableConfig) -> List[Dict]:
        """Get relations where this table is the target (reverse relations)"""
        reverse_relations = []

        for other_name, other_config in self.table_configs.items():
            if other_name == table_config.name:
                continue

            for relation in other_config.relations:
                relation_target_name = relation.get('name')

                if relation_target_name == table_config.name:
                    reverse_relation = {
                        'source_table': other_config.name,
                        'target_table': table_config.name,
                        'type': relation.get('type', 'LEFT'),
                        'joinColumns': relation.get('joinColumns', []),
                        'original_relation': relation
                    }
                    reverse_relations.append(reverse_relation)

        return reverse_relations

    def build_query(self, params: GetDataParams) -> Tuple[str, List[Any], str, List[Any]]:
        """Build complete SQL query from parameters."""
        all_errors = []

        all_columns = params.get_all_columns()
        column_errors = self._validate_columns(all_columns)
        if column_errors:
            all_errors.extend(column_errors)

        if params.filterBy:
            set_a_tables, column_to_table_map = self._get_explicitly_requested_tables(params)
            filter_errors = self._validate_filter_data_types(params.filterBy, column_to_table_map)
            all_errors.extend(filter_errors)

        if params.measures:
            measure_errors = self._validate_measures(params.measures)
            all_errors.extend(measure_errors)

        if all_errors:
            raise ValidationError(all_errors)

        self._reset()

        set_a_tables, column_to_table_map = self._get_explicitly_requested_tables(params)
        join_tables = self._find_tables_to_join(set_a_tables)
        main_table = self._determine_main_table(set_a_tables)

        is_aggregated = params.is_aggregated()
        self._build_select_clause(params, column_to_table_map, join_tables, is_aggregated)
        self._build_from_clause(main_table)
        self._build_join_clauses(main_table, join_tables)
        self._build_where_clause(params, column_to_table_map)
        self._build_group_by_clause(params, column_to_table_map, join_tables, is_aggregated)
        self._build_order_by_clause(params, column_to_table_map)
        self._build_pagination(params)

        main_query = self._construct_final_query()
        main_parameters = self.parameters.copy()

        count_query, count_parameters = self._build_count_query(main_table, join_tables, column_to_table_map, params)

        return main_query, main_parameters, count_query, count_parameters

    def _build_count_query(self, main_table: TableConfig, join_tables: Dict[str, TableConfig],
                              column_to_table_map: Dict[str, TableConfig], params: GetDataParams) -> Tuple[
        str, List[Any]]:
        """
        Build count query using different strategies to avoid nested queries.
        """
        count_parameters = []

        # Copy WHERE clause parameters for count query
        if params.filterBy:
            for filter_obj in params.filterBy:
                _, params_values = self._build_filter_condition(filter_obj, column_to_table_map)
                count_parameters.extend(params_values)

        is_distinct_only = params.is_distinct_only()
        is_aggregated = params.is_aggregated()

        if self.count_strategy == "simple":
            return self._build_simple_count_query(main_table, join_tables, params, count_parameters)
        elif self.count_strategy == "distinct":
            return self._build_distinct_count_query(main_table, join_tables, params, count_parameters, is_distinct_only)
        elif self.count_strategy == "separate":
            return self._build_separate_count_query(main_table, join_tables, params, count_parameters)
        elif self.count_strategy == "estimate":
            return self._build_estimate_count_query(main_table, join_tables, params, count_parameters)
        else:
            # Default to simple strategy
            return self._build_simple_count_query(main_table, join_tables, params, count_parameters)

    def _build_simple_count_query(self, main_table: TableConfig, join_tables: Dict[str, TableConfig],
                                  params: GetDataParams, count_parameters: List[Any]) -> Tuple[str, List[Any]]:
        """
        Simple count strategy: Just count all matching rows, ignoring GROUP BY.
        This gives an approximate count but avoids nested queries.
        """
        count_query_parts = ["SELECT COUNT(*)"]
        count_query_parts.append(f"FROM {main_table.name}")

        # Add joins
        for join in self.query_parts['joins']:
            count_query_parts.append(join)

        # Add WHERE clause
        if self.query_parts['where']:
            count_query_parts.append(f"WHERE {' AND '.join(self.query_parts['where'])}")

        count_query = ' '.join(count_query_parts)
        return count_query, count_parameters

    def _build_distinct_count_query(self, main_table: TableConfig, join_tables: Dict[str, TableConfig],
                                    params: GetDataParams, count_parameters: List[Any],
                                    is_distinct_only: bool) -> Tuple[str, List[Any]]:
        """
        Distinct count strategy: Use COUNT(DISTINCT ...) for grouped queries.
        This works if your third-party API supports COUNT(DISTINCT).
        """
        if is_distinct_only and params.groupBy:
            # For distinct-only queries, count distinct combinations
            group_cols = []
            for col in params.groupBy:
                if '.' in col:
                    table_name, col_name = col.split('.', 1)
                    group_cols.append(col_name)
                else:
                    group_cols.append(col)

            if len(group_cols) == 1:
                count_select = f"COUNT(DISTINCT {group_cols[0]})"
            else:
                # For multiple columns, concatenate them
                concat_cols = " || '|' || ".join(group_cols)
                count_select = f"COUNT(DISTINCT ({concat_cols}))"
        else:
            count_select = "COUNT(*)"

        count_query_parts = [f"SELECT {count_select}"]
        count_query_parts.append(f"FROM {main_table.name}")

        # Add joins
        for join in self.query_parts['joins']:
            count_query_parts.append(join)

        # Add WHERE clause
        if self.query_parts['where']:
            count_query_parts.append(f"WHERE {' AND '.join(self.query_parts['where'])}")

        count_query = ' '.join(count_query_parts)
        return count_query, count_parameters

    def _build_separate_count_query(self, main_table: TableConfig, join_tables: Dict[str, TableConfig],
                                    params: GetDataParams, count_parameters: List[Any]) -> Tuple[str, List[Any]]:
        """
        Separate count strategy: Return a query that can be executed separately.
        The application logic will need to handle this by executing the query
        and counting the returned rows.
        """
        if params.groupBy:
            # Return a query that selects the groupBy columns
            # The application will count the returned rows
            select_items = []
            for col in params.groupBy:
                if '.' in col:
                    table_name, col_name = col.split('.', 1)
                    select_items.append(col_name)
                else:
                    select_items.append(col)

            count_query_parts = [f"SELECT DISTINCT {', '.join(select_items)}"]
            count_query_parts.append(f"FROM {main_table.name}")

            # Add joins
            for join in self.query_parts['joins']:
                count_query_parts.append(join)

            # Add WHERE clause
            if self.query_parts['where']:
                count_query_parts.append(f"WHERE {' AND '.join(self.query_parts['where'])}")

            count_query = ' '.join(count_query_parts)
            return count_query, count_parameters
        else:
            # For non-grouped queries, use simple count
            return self._build_simple_count_query(main_table, join_tables, params, count_parameters)

    def _build_estimate_count_query(self, main_table: TableConfig, join_tables: Dict[str, TableConfig],
                                    params: GetDataParams, count_parameters: List[Any]) -> Tuple[str, List[Any]]:
        """
        Estimate count strategy: Return a special marker query.
        The application logic should detect this and provide an estimated count
        based on the actual result set size.
        """
        # Return a special query that the application can detect
        count_query = "SELECT -1 AS estimated_count"
        return count_query, []

    def get_count_from_results(self, count_result: List[Dict], main_results: List[Dict],
                               params: GetDataParams) -> int:
        """
        Helper method to extract count from count query results based on strategy.

        Args:
            count_result: Result from count query
            main_results: Result from main query (used for estimation)
            params: Original query parameters

        Returns:
            Total count as integer
        """
        if not count_result:
            return 0

        if self.count_strategy == "separate":
            # For separate strategy, count the rows returned
            return len(count_result)
        elif self.count_strategy == "estimate":
            # For estimate strategy, use the main result set size as basis
            if count_result[0].get('estimated_count') == -1:
                # Provide estimated count based on page size and current results
                result_count = len(main_results)
                if params.page and params.page_size:
                    if result_count == params.page_size:
                        # Estimate there might be more pages
                        return params.page * params.page_size + params.page_size
                    else:
                        # This is likely the last page
                        return (params.page - 1) * params.page_size + result_count
                return result_count
        else:
            # For simple and distinct strategies, return the count value
            count_key = next(iter(count_result[0].keys()))
            return count_result[0][count_key]

    # ... [Rest of the methods remain the same] ...

    def _reset(self):
        """Reset query builder state"""
        self.query_parts = {
            'select': [],
            'from': '',
            'joins': [],
            'where': [],
            'group_by': [],
            'order_by': '',
        }
        self.parameters = []

    def _get_explicitly_requested_tables(self, params: GetDataParams) -> Tuple[
        Dict[str, TableConfig], Dict[str, TableConfig]]:
        """Get tables explicitly requested in the query parameters."""
        set_a = {}
        column_to_table_map = {}
        all_columns = params.get_all_columns()

        for column in all_columns:
            if '.' in column:
                table_name, col_name = column.split('.', 1)

                if table_name in self.table_configs:
                    config = self.table_configs[table_name]
                    set_a[table_name] = config
                    column_to_table_map[col_name] = config
                else:
                    found_config = None
                    for name, config in self.table_configs.items():
                        if config.name.lower() == table_name.lower():
                            found_config = config
                            break

                    if found_config:
                        set_a[table_name] = found_config
                        column_to_table_map[col_name] = found_config
                    else:
                        temp_config = TableConfig(table_name, 999, [])
                        set_a[table_name] = temp_config
                        column_to_table_map[col_name] = temp_config
            else:
                column_name = column
                best_table = None
                highest_priority = float('inf')

                for config in self.table_configs.values():
                    for col_def in config.columns:
                        if col_def.get('name') == column_name or column_name in col_def.get('field_aliases', []):
                            if config.priority < highest_priority:
                                highest_priority = config.priority
                                best_table = config
                            break

                if best_table:
                    set_a[best_table.name] = best_table
                    column_to_table_map[column_name] = best_table

        return set_a, column_to_table_map

    def _find_tables_to_join(self, set_a_tables: Dict[str, TableConfig]) -> Dict[str, TableConfig]:
        """Find all tables that need to be joined based on relations."""
        join_tables = dict(set_a_tables)

        for set_a_name, set_a_config in set_a_tables.items():
            all_relations = self._get_relations_for_table(set_a_config)

            for relation in all_relations:
                target_name = relation.get('name')

                target_config = None
                if target_name and target_name in self.table_configs:
                    target_config = self.table_configs[target_name]

                if target_config and target_config.name not in join_tables:
                    join_tables[target_config.name] = target_config

        for table_name, table_config in self.table_configs.items():
            if table_name in join_tables:
                continue

            has_relation_to_set_a = False
            all_relations = self._get_relations_for_table(table_config)

            for relation in all_relations:
                relation_target_name = relation.get('name')

                for set_a_name, set_a_config in set_a_tables.items():
                    if relation_target_name == set_a_config.name:
                        has_relation_to_set_a = True
                        break

                if has_relation_to_set_a:
                    break

            if has_relation_to_set_a:
                join_tables[table_name] = table_config

        return join_tables

    def _determine_main_table(self, set_a_tables: Dict[str, TableConfig]) -> TableConfig:
        """Determine the main table based on priority."""
        if not set_a_tables:
            raise ValidationError([{
                "field": "columns",
                "message": "No tables found for requested columns"
            }])

        main_table = min(set_a_tables.values(), key=lambda x: x.priority)
        return main_table

    def _build_from_clause(self, table_config: TableConfig):
        """Build FROM clause without alias."""
        self.query_parts['from'] = f"{table_config.name}"

    def _build_join_clauses(self, main_table: TableConfig, join_tables: Dict[str, TableConfig]):
        """Build JOIN clauses for all necessary tables without aliases."""
        if not join_tables:
            return

        joined_tables = {main_table.name: main_table}

        sorted_join_tables = sorted(join_tables.values(), key=lambda x: x.priority)

        for target_table in sorted_join_tables:
            if target_table.name in joined_tables:
                continue

            join_clause = self._find_join_path(target_table, joined_tables)
            if join_clause:
                self.query_parts['joins'].append(join_clause)
                joined_tables[target_table.name] = target_table
            else:
                print(
                    f"Warning: No join path found for table {target_table.name}. This may cause ambiguity in column names.")

    def _find_join_path(self, target_table: TableConfig, joined_tables: Dict[str, TableConfig]) -> str:
        """Find a valid join path to the target table."""
        for joined_name, joined_config in joined_tables.items():
            all_relations = self._get_relations_for_table(joined_config)
            for relation in all_relations:
                relation_target_name = relation.get('name')

                if relation_target_name == target_table.name:
                    return self._build_join_clause(joined_config, target_table, relation)

            reverse_relations = self._get_reverse_relations_for_table(target_table)
            for reverse_relation in reverse_relations:
                source_name = reverse_relation.get('source_table')

                if source_name == joined_name:
                    return self._build_join_clause_from_reverse(target_table, joined_config, reverse_relation)

        return None

    def _build_join_clause(self, source_config: TableConfig, target_config: TableConfig, relation: Dict) -> str:
        """Build a JOIN clause from a relation without aliases."""
        join_conditions = []

        join_columns = relation.get('joinColumns', [])

        for join_col in join_columns:
            if isinstance(join_col, dict):
                if 'source' in join_col and 'target' in join_col:
                    source_col = join_col['source']
                    target_col = join_col['target']
                    condition = f"{source_config.name}.{source_col} = {target_config.name}.{target_col}"
                    join_conditions.append(condition)
                elif 'name' in join_col:
                    col_name = join_col['name']
                    condition = f"{source_config.name}.{col_name} = {target_config.name}.{col_name}"
                    join_conditions.append(condition)
            else:
                col_name = join_col
                condition = f"{source_config.name}.{col_name} = {target_config.name}.{col_name}"
                join_conditions.append(condition)

        if join_conditions:
            join_type = self._convert_join_type(relation.get('type', 'LEFT'))
            join_clause = (
                f"{join_type} JOIN {target_config.name} ON "
                f"{' AND '.join(join_conditions)}"
            )
            return join_clause

        return None

    def _build_join_clause_from_reverse(self, target_config: TableConfig, source_config: TableConfig,
                                        reverse_relation: Dict) -> str:
        """Build a JOIN clause from a reverse relation without aliases."""
        join_conditions = []

        original_relation = reverse_relation.get('original_relation', {})
        join_columns = original_relation.get('joinColumns', [])

        for join_col in join_columns:
            if isinstance(join_col, dict):
                if 'source' in join_col and 'target' in join_col:
                    source_col = join_col['target']
                    target_col = join_col['source']
                    condition = f"{source_config.name}.{source_col} = {target_config.name}.{target_col}"
                    join_conditions.append(condition)
                elif 'name' in join_col:
                    col_name = join_col['name']
                    condition = f"{source_config.name}.{col_name} = {target_config.name}.{col_name}"
                    join_conditions.append(condition)
            else:
                col_name = join_col
                condition = f"{source_config.name}.{col_name} = {target_config.name}.{col_name}"
                join_conditions.append(condition)

        if join_conditions:
            join_type = self._convert_join_type(original_relation.get('type', 'LEFT'))
            join_clause = (
                f"{join_type} JOIN {target_config.name} ON "
                f"{' AND '.join(join_conditions)}"
            )
            return join_clause

        return None

    def _convert_join_type(self, relation_type: str) -> str:
        """Convert relation type to SQL JOIN type."""
        type_mapping = {
            'ONE_TO_ONE': 'LEFT',
            'ONE_TO_MANY': 'LEFT',
            'MANY_TO_ONE': 'LEFT',
            'MANY_TO_MANY': 'LEFT',
            'LEFT': 'LEFT',
            'RIGHT': 'RIGHT',
            'INNER': 'INNER',
            'OUTER': 'FULL OUTER'
        }
        return type_mapping.get(relation_type.upper(), 'LEFT')

    def _build_select_clause(self, params: GetDataParams, column_to_table_map: Dict[str, TableConfig],
                             join_tables: Dict[str, TableConfig], is_aggregated: bool):
        """Build SELECT clause without table aliases."""
        select_items = []
        is_distinct_only = params.is_distinct_only()
        main_table = self._determine_main_table(join_tables)

        if is_distinct_only:
            # Distinct-only case: select groupBy columns without table prefixes
            for col in params.groupBy or []:
                if '.' in col:
                    table_name, col_name = col.split('.', 1)
                    select_items.append(col_name)
                else:
                    select_items.append(col)
        elif is_aggregated:
            # Aggregated query: include mandatory fields and measures
            for field in main_table.mandatory_fields:
                select_items.append(f"{field}")

            for table_name, table_config in join_tables.items():
                if table_name != main_table.name:
                    for field in table_config.mandatory_fields:
                        select_items.append(f"{field}")

            # Add groupBy columns
            for col in params.groupBy or []:
                if '.' in col:
                    table_name, col_name = col.split('.', 1)
                    select_items.append(col_name)
                else:
                    select_items.append(col)

            # Add measures
            for measure in params.measures or []:
                col = measure.field
                if '.' in col:
                    table_name, col_name = col.split('.', 1)
                    col = col_name
                alias = f"{measure.function.lower()}_{col.replace('.', '_')}"
                select_items.append(f"{measure.function.upper()}({col}) AS {alias}")

            # Add table aggregations from config
            for table_name, table_config in join_tables.items():
                for agg in table_config.aggregations:
                    agg_field = agg.get('field')
                    agg_function = agg.get('function', 'SUM').upper()
                    agg_alias = agg.get('alias', f"{agg_function.lower()}_{agg_field}")
                    if any(col_def.get('name') == agg_field for col_def in table_config.columns):
                        select_items.append(f"{agg_function}({agg_field}) AS {agg_alias}")
        else:
            # Non-aggregated: select groupBy columns without table prefixes
            for col in params.groupBy or []:
                if '.' in col:
                    table_name, col_name = col.split('.', 1)
                    select_items.append(col_name)
                else:
                    select_items.append(col)

        if not select_items:
            select_items = ["*"]

        # Apply DISTINCT for distinct-only case
        self.query_parts['select'] = ['DISTINCT ' + ', '.join(select_items)] if is_distinct_only else select_items

    def _build_where_clause(self, params: GetDataParams, column_to_table_map: Dict[str, TableConfig]):
        """Build WHERE clause without table aliases."""
        if not params.filterBy:
            return

        conditions = []
        for filter_obj in params.filterBy:
            condition, params_values = self._build_filter_condition(filter_obj, column_to_table_map)
            if condition:
                conditions.append(condition)
                self.parameters.extend(params_values)

        if conditions:
            self.query_parts['where'] = [f"({' AND '.join(conditions)})"]

    def _build_filter_condition(self, filter_obj: FilterModel, column_to_table_map: Dict[str, TableConfig]) -> Tuple[
        str, List[Any]]:
        """Build individual filter condition without table aliases."""
        column = filter_obj.field
        if '.' in column:
            table_name, col_name = column.split('.', 1)
            column = col_name
        elif column in column_to_table_map:
            column = column  # Already the column name without prefix

        operator = filter_obj.operator.upper()  # Already normalized by schemas.py
        values = filter_obj.values

        if operator == 'BETWEEN':
            if isinstance(values, list) and len(values) == 2:
                start, end = values
                condition = f"{column} BETWEEN ? AND ?"
                return condition, [start, end]
        elif operator in ['IN', 'INLIST']:
            if isinstance(values, list) and values:
                placeholders = ','.join(['?' for _ in values])
                condition = f"{column} IN ({placeholders})"
                return condition, values
        elif operator == 'EQUAL':
            if values is not None:
                condition = f"{column} = ?"
                return condition, [values]

        return None, []

    def _build_group_by_clause(self, params: GetDataParams, column_to_table_map: Dict[str, TableConfig],
                               join_tables: Dict[str, TableConfig], is_aggregated: bool):
        """Build GROUP BY clause without table aliases."""
        if not is_aggregated:
            return

        has_aggregates = any(
            '(' in item and item.split('(')[0].upper() in {'COUNT', 'SUM', 'AVG', 'MAX', 'MIN'} for item in
            self.query_parts['select'])
        if not has_aggregates:
            return

        group_by_items = []
        main_table = self._determine_main_table(join_tables)

        for field in main_table.mandatory_fields:
            group_by_items.append(f"{field}")

        for table_name, table_config in join_tables.items():
            if table_name != main_table.name:
                for field in table_config.mandatory_fields:
                    group_by_items.append(f"{field}")

        for col in params.groupBy or []:
            if '.' in col:
                table_name, col_name = col.split('.', 1)
                if col_name not in group_by_items:
                    group_by_items.append(col_name)
            else:
                if col not in group_by_items:
                    group_by_items.append(col)

        if group_by_items:
            self.query_parts['group_by'] = group_by_items

    def _build_order_by_clause(self, params: GetDataParams, column_to_table_map: Dict[str, TableConfig]):
        """Build ORDER BY clause without table aliases using normalized order."""
        if not params.sortBy:
            return

        order_items = []
        for sort_obj in params.sortBy:
            column = sort_obj.field
            if '.' in column:
                table_name, col_name = column.split('.', 1)
                column = col_name
            order_items.append(f"{column} {sort_obj.order}")  # Uses normalized ASC/DESC

        if order_items:
            self.query_parts['order_by'] = f"ORDER BY {', '.join(order_items)}"

    def _build_pagination(self, params: GetDataParams):
        """Build LIMIT and OFFSET clauses."""
        if params.page and params.page_size:
            limit = params.page_size
            offset = (params.page - 1) * params.page_size
            self.query_parts['limit'] = str(limit)
            if offset > 0:
                self.query_parts['offset'] = str(offset)

    def _construct_final_query(self) -> str:
        """Construct the final SQL query."""
        query_parts = []

        query_parts.append(f"SELECT {', '.join(self.query_parts['select'])}")
        query_parts.append(f"FROM {self.query_parts['from']}")

        for join in self.query_parts['joins']:
            query_parts.append(join)

        if self.query_parts['where']:
            query_parts.append(f"WHERE {' AND '.join(self.query_parts['where'])}")

        if self.query_parts['group_by']:
            query_parts.append(f"GROUP BY {', '.join(self.query_parts['group_by'])}")

        if self.query_parts['order_by']:
            query_parts.append(self.query_parts['order_by'])

        if 'limit' in self.query_parts and self.query_parts['limit']:
            query_parts.append(f"LIMIT {self.query_parts['limit']}")

        if 'offset' in self.query_parts and self.query_parts['offset']:
            query_parts.append(f"OFFSET {self.query_parts['offset']}")

        return ' '.join(query_parts)