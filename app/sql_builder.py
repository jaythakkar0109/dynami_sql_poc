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
    def __init__(self, name: str, alias: str, priority: int, columns: List[Dict], relations: List[Dict] = None,
                 mandatory_fields: List[str] = None, aggregations: List[Dict] = None):
        self.name = name
        self.alias = alias
        self.priority = priority
        self.columns = columns or []
        self.relations = relations or []
        self.mandatory_fields = mandatory_fields or []
        self.aggregations = aggregations or []


class JoinRelation:
    def __init__(self, target_table: str, target_alias: str, join_type: str, join_columns: List[Dict]):
        self.target_table = target_table
        self.target_alias = target_alias
        self.join_type = join_type
        self.join_columns = join_columns


class SQLBuilder:
    def __init__(self, config_path: str = "config/table_config.yaml"):
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
                alias = table_data.get('schema_name')
                priority = table_data.get('priority', 999)
                columns = [
                    {'name': field_name, 'field_aliases': field_data.get('field_aliases', []),
                     'field_type': field_data.get('field_type')}
                    for field_name, field_data in table_data.get('schema_fields', {}).items()
                ]
                relations = table_data.get('relations', [])
                mandatory_fields = table_data.get('mandatory_fields', [])
                aggregations = table_data.get('aggregation', [])

                if name and alias:
                    config = TableConfig(name, alias, priority, columns, relations, mandatory_fields, aggregations)
                    self.table_configs[alias] = config

    def _validate_columns(self, columns: List[str]) -> List[Dict]:
        """Validate that the given columns exist in the table configurations."""
        errors = []

        for column in columns:
            if '.' in column:
                table_alias, col_name = column.split('.', 1)
                if table_alias in self.table_configs:
                    table_config = self.table_configs[table_alias]
                    if not any(
                            col_def.get('name') == col_name or col_name in col_def.get('field_aliases', []) for col_def
                            in table_config.columns):
                        errors.append({
                            "field": "columns",
                            "message": f"Column '{column}' does not exist in table '{table_alias}'"
                        })
                else:
                    errors.append({
                        "field": "columns",
                        "message": f"Table alias '{table_alias}' in column '{column}' not found in configurations"
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
        table_alias = None
        col_name = column

        if '.' in column:
            table_alias, col_name = column.split('.', 1)
        elif col_name in column_to_table_map:
            table_config = column_to_table_map[col_name]
            table_alias = table_config.alias

        if table_alias and table_alias in self.table_configs:
            table_config = self.table_configs[table_alias]
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

        for other_alias, other_config in self.table_configs.items():
            if other_alias == table_config.alias:
                continue

            for relation in other_config.relations:
                relation_target_name = relation.get('name')
                relation_target_alias = relation.get('alias')

                if (relation_target_name == table_config.name or
                        relation_target_alias == table_config.alias):
                    reverse_relation = {
                        'source_table': other_config.name,
                        'source_alias': other_config.alias,
                        'target_table': table_config.name,
                        'target_alias': table_config.alias,
                        'type': relation.get('type', 'LEFT'),
                        'joinColumns': relation.get('joinColumns', []),
                        'original_relation': relation
                    }
                    reverse_relations.append(reverse_relation)

        return reverse_relations

    def build_query(self, params: GetDataParams) -> Tuple[str, List[Any], str, List[Any]]:
        """Build complete SQL query from parameters."""
        all_errors = []

        is_aggregated = params.is_aggregated()
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

        self._build_select_clause(params, column_to_table_map, join_tables, is_aggregated)
        self._build_from_clause_with_alias(main_table)
        self._build_join_clauses_new(main_table, join_tables)
        self._build_where_clause(params, column_to_table_map)
        self._build_group_by_clause(params, column_to_table_map, join_tables, is_aggregated)
        self._build_order_by_clause(params, column_to_table_map)
        self._build_pagination(params)

        main_query = self._construct_final_query()
        main_parameters = self.parameters.copy()

        count_query, count_parameters = self._build_count_query(main_table, join_tables, column_to_table_map, params)

        return main_query, main_parameters, count_query, count_parameters

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
                table_alias, col_name = column.split('.', 1)

                if table_alias in self.table_configs:
                    config = self.table_configs[table_alias]
                    set_a[table_alias] = config
                    column_to_table_map[col_name] = config
                else:
                    found_config = None
                    for alias, config in self.table_configs.items():
                        if config.name.lower() == table_alias.lower() or alias.lower() == table_alias.lower():
                            found_config = config
                            break

                    if found_config:
                        set_a[table_alias] = found_config
                        column_to_table_map[col_name] = found_config
                    else:
                        temp_config = TableConfig(table_alias, table_alias, 999, [])
                        set_a[table_alias] = temp_config
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
                    set_a[best_table.alias] = best_table
                    column_to_table_map[column_name] = best_table

        return set_a, column_to_table_map

    def _find_tables_to_join(self, set_a_tables: Dict[str, TableConfig]) -> Dict[str, TableConfig]:
        """Find all tables that need to be joined based on relations."""
        join_tables = dict(set_a_tables)

        for set_a_alias, set_a_config in set_a_tables.items():
            all_relations = self._get_relations_for_table(set_a_config)

            for relation in all_relations:
                target_alias = relation.get('alias')
                target_name = relation.get('name')

                target_config = None
                if target_alias and target_alias in self.table_configs:
                    target_config = self.table_configs[target_alias]
                else:
                    for config in self.table_configs.values():
                        if (config.name == target_name or
                                config.alias == target_alias or
                                config.name == target_alias):
                            target_config = config
                            break

                if target_config and target_config.alias not in join_tables:
                    join_tables[target_config.alias] = target_config

        for table_alias, table_config in self.table_configs.items():
            if table_alias in join_tables:
                continue

            has_relation_to_set_a = False
            all_relations = self._get_relations_for_table(table_config)

            for relation in all_relations:
                relation_target_alias = relation.get('alias')
                relation_target_name = relation.get('name')

                for set_a_alias, set_a_config in set_a_tables.items():
                    if (relation_target_alias == set_a_alias or
                            relation_target_name == set_a_config.name or
                            relation_target_alias == set_a_config.alias):
                        has_relation_to_set_a = True
                        break

                if has_relation_to_set_a:
                    break

            if has_relation_to_set_a:
                join_tables[table_alias] = table_config

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

    def _build_from_clause_with_alias(self, table_config: TableConfig):
        """Build FROM clause with table alias."""
        self.query_parts['from'] = f"{table_config.name} AS {table_config.alias}"

    def _build_join_clauses_new(self, main_table: TableConfig, join_tables: Dict[str, TableConfig]):
        """Build JOIN clauses for all necessary tables."""
        if not join_tables:
            return

        joined_tables = {main_table.alias: main_table}

        sorted_join_tables = sorted(join_tables.values(), key=lambda x: x.priority)

        for target_table in sorted_join_tables:
            if target_table.alias in joined_tables:
                continue

            join_clause = self._find_join_path(target_table, joined_tables)
            if join_clause:
                self.query_parts['joins'].append(join_clause)
                joined_tables[target_table.alias] = target_table
            else:
                print(f"Warning: No join path found for table {target_table.alias}")

    def _find_join_path(self, target_table: TableConfig, joined_tables: Dict[str, TableConfig]) -> str:
        """Find a valid join path to the target table."""
        for joined_alias, joined_config in joined_tables.items():
            all_relations = self._get_relations_for_table(joined_config)
            for relation in all_relations:
                relation_target_alias = relation.get('alias')
                relation_target_name = relation.get('name')

                if (relation_target_alias == target_table.alias or
                        relation_target_name == target_table.name):
                    return self._build_join_clause_new(joined_config, target_table, relation)

            reverse_relations = self._get_reverse_relations_for_table(target_table)
            for reverse_relation in reverse_relations:
                source_alias = reverse_relation.get('source_alias')
                source_name = reverse_relation.get('source_table')

                if (source_alias == joined_alias or
                        source_name == joined_config.name):
                    return self._build_join_clause_from_reverse(target_table, joined_config, reverse_relation)

        return None

    def _build_join_clause_new(self, source_config: TableConfig, target_config: TableConfig, relation: Dict) -> str:
        """Build a JOIN clause from a relation."""
        join_conditions = []

        join_columns = relation.get('joinColumns', [])

        for join_col in join_columns:
            if isinstance(join_col, dict):
                if 'source' in join_col and 'target' in join_col:
                    source_col = join_col['source']
                    target_col = join_col['target']
                    condition = f"{source_config.alias}.{source_col} = {target_config.alias}.{target_col}"
                    join_conditions.append(condition)
                elif 'name' in join_col:
                    col_name = join_col['name']
                    condition = f"{source_config.alias}.{col_name} = {target_config.alias}.{col_name}"
                    join_conditions.append(condition)
            else:
                col_name = join_col
                condition = f"{source_config.alias}.{col_name} = {target_config.alias}.{col_name}"
                join_conditions.append(condition)

        if join_conditions:
            join_type = self._convert_join_type(relation.get('type', 'LEFT'))
            join_clause = (
                f"{join_type} JOIN {target_config.name} AS {target_config.alias} ON "
                f"{' AND '.join(join_conditions)}"
            )
            return join_clause

        return None

    def _build_join_clause_from_reverse(self, target_config: TableConfig, source_config: TableConfig,
                                        reverse_relation: Dict) -> str:
        """Build a JOIN clause from a reverse relation."""
        join_conditions = []

        original_relation = reverse_relation.get('original_relation', {})
        join_columns = original_relation.get('joinColumns', [])

        for join_col in join_columns:
            if isinstance(join_col, dict):
                if 'source' in join_col and 'target' in join_col:
                    source_col = join_col['target']
                    target_col = join_col['source']
                    condition = f"{source_config.alias}.{source_col} = {target_config.alias}.{target_col}"
                    join_conditions.append(condition)
                elif 'name' in join_col:
                    col_name = join_col['name']
                    condition = f"{source_config.alias}.{col_name} = {target_config.alias}.{col_name}"
                    join_conditions.append(condition)
            else:
                col_name = join_col
                condition = f"{source_config.alias}.{col_name} = {target_config.alias}.{col_name}"
                join_conditions.append(condition)

        if join_conditions:
            join_type = self._convert_join_type(original_relation.get('type', 'LEFT'))
            join_clause = (
                f"{join_type} JOIN {target_config.name} AS {target_config.alias} ON "
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
        """Build SELECT clause based on the payload structure."""
        select_items = []
        is_distinct_only = params.is_distinct_only()
        main_table = self._determine_main_table(join_tables)

        if is_distinct_only:
            # Distinct-only case: select groupBy columns with DISTINCT
            for col in params.groupBy or []:
                if '.' in col:
                    select_items.append(col)
                else:
                    select_items.append(col)
        elif is_aggregated:
            # Aggregated query: include mandatory fields and measures
            for field in main_table.mandatory_fields:
                select_items.append(f"{main_table.alias}.{field}")

            for table_alias, table_config in join_tables.items():
                if table_alias != main_table.alias:
                    for field in table_config.mandatory_fields:
                        select_items.append(f"{table_alias}.{field}")

            # Add groupBy columns
            for col in params.groupBy or []:
                if '.' in col:
                    select_items.append(col)
                else:
                    if col in column_to_table_map:
                        table_config = column_to_table_map[col]
                        select_items.append(f"{table_config.alias}.{col}")
                    else:
                        select_items.append(f"{main_table.alias}.{col}")

            # Add measures
            for measure in params.measures or []:
                col = measure.field
                if '.' not in col and col in column_to_table_map:
                    table_config = column_to_table_map[col]
                    col = f"{table_config.alias}.{col}"

                alias = f"{measure.function.lower()}_{measure.field.replace('.', '_')}"
                select_items.append(f"{measure.function.upper()}({col}) AS {alias}")

            # Add table aggregations from config
            for table_alias, table_config in join_tables.items():
                for agg in table_config.aggregations:
                    agg_field = agg.get('field')
                    agg_function = agg.get('function', 'SUM').upper()
                    agg_alias = agg.get('alias', f"{agg_function.lower()}_{agg_field}")
                    if any(col_def.get('name') == agg_field for col_def in table_config.columns):
                        select_items.append(f"{agg_function}({table_alias}.{agg_field}) AS {agg_alias}")
        else:
            # Non-aggregated: select groupBy columns without table aliases
            for col in params.groupBy or []:
                if '.' in col:
                    select_items.append(col)
                else:
                    select_items.append(col)

        if not select_items:
            select_items = [f"{main_table.alias}.*"]

        # Apply DISTINCT for distinct-only case
        self.query_parts['select'] = ['DISTINCT ' + ', '.join(select_items)] if is_distinct_only else select_items

    def _build_where_clause(self, params: GetDataParams, column_to_table_map: Dict[str, TableConfig]):
        """Build WHERE clause from filters."""
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
        """Build individual filter condition."""
        column = filter_obj.field
        if '.' not in column and column in column_to_table_map:
            table_config = column_to_table_map[column]
            column = f"{table_config.alias}.{column}"

        operator = filter_obj.operator.upper()
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
        """Build GROUP BY clause for aggregated queries."""
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
            group_by_items.append(f"{main_table.alias}.{field}")

        for table_alias, table_config in join_tables.items():
            if table_alias != main_table.alias:
                for field in table_config.mandatory_fields:
                    group_by_items.append(f"{table_alias}.{field}")

        for col in params.groupBy or []:
            if '.' in col:
                if col not in group_by_items:
                    group_by_items.append(col)
            else:
                if col in column_to_table_map:
                    table_config = column_to_table_map[col]
                    full_col = f"{table_config.alias}.{col}"
                    if full_col not in group_by_items:
                        group_by_items.append(full_col)

        if group_by_items:
            self.query_parts['group_by'] = group_by_items

    def _build_order_by_clause(self, params: GetDataParams, column_to_table_map: Dict[str, TableConfig]):
        """Build ORDER BY clause from sortBy."""
        if not params.sortBy:
            return

        order_items = []
        for sort_obj in params.sortBy:
            column = sort_obj.field
            if '.' not in column and column in column_to_table_map:
                table_config = column_to_table_map[column]
                column = f"{table_config.alias}.{column}"

            order_items.append(f"{column} {sort_obj.order}")

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

    def _build_count_query(self, main_table: TableConfig, join_tables: Dict[str, TableConfig],
                           column_to_table_map: Dict[str, TableConfig], params: GetDataParams) -> Tuple[str, List[Any]]:
        """Build a query to compute the total count of matching rows."""
        count_parameters = self.parameters.copy()

        count_query_parts = []
        is_distinct_only = params.is_distinct_only()

        if is_distinct_only:
            # Count distinct rows for groupBy-only case
            select_items = []
            for col in params.groupBy or []:
                if '.' in col:
                    select_items.append(col)
                else:
                    select_items.append(col)
            count_query_parts.append(f"SELECT COUNT(*) FROM (SELECT DISTINCT {', '.join(select_items)}")
            count_query_parts.append(f"FROM {self.query_parts['from']}")
            for join in self.query_parts['joins']:
                count_query_parts.append(join)
            count_query_parts.append(") AS subquery")
        else:
            # Standard count query
            count_query_parts.append("SELECT COUNT(*)")
            count_query_parts.append(f"FROM {self.query_parts['from']}")

            for join in self.query_parts['joins']:
                count_query_parts.append(join)

            if self.query_parts['where']:
                count_query_parts.append(f"WHERE {' AND '.join(self.query_parts['where'])}")

            if self.query_parts['group_by']:
                count_query_parts = [
                    "SELECT COUNT(*) FROM (",
                    ' '.join(count_query_parts),
                    f"GROUP BY {', '.join(self.query_parts['group_by'])}",
                    ") AS subquery"
                ]

        count_query = ' '.join(count_query_parts)
        return count_query, count_parameters

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