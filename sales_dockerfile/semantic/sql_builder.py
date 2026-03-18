# # # # app/semantic/sql_builder.py
# # # from typing import List, Optional, Tuple
# # # from semantic.registry import SemanticRegistry
# # # from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# # # class SQLBuilder:
# # #     """
# # #     Builds SQL from semantic intent.
    
# # #     ENHANCEMENTS:
# # #     1. Better handling of multi-value filters with OR logic
# # #     2. Improved partial matching for text filters
# # #     3. Proper handling of type_desc and sector filters
# # #     """

# # #     def __init__(self, registry: SemanticRegistry):
# # #         self.registry = registry

# # #     def build(
# # #         self,
# # #         metric: str,
# # #         dimensions: List[str],
# # #         date_filter: Optional[str],
# # #         filters: Optional[dict] = None,
# # #         order_by: Optional[str] = None,
# # #         order_direction: str = "DESC",
# # #         limit: Optional[int] = None,
# # #         time_grain: Optional[str] = None
# # #     ) -> Tuple[str, List[str]]:
# # #         """
# # #         Build SQL query from semantic components.
        
# # #         Returns:
# # #             Tuple of (sql_string, warnings_list)
# # #         """
# # #         warnings = []
        
# # #         # Get metric definition
# # #         if metric not in self.registry.metrics:
# # #             raise ValueError(f"Unknown metric: {metric}")
        
# # #         metric_def = self.registry.metrics[metric]
# # #         fact_table = self.registry.fact.table
        
# # #         # Build SELECT clause
# # #         select_items = []
# # #         group_by_items = []
        
# # #         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
# # #         if time_grain:
# # #             date_col = self.registry.fact.primary_date
# # #             parsed_date = _date_sql(date_col)

# # #             time_expressions = {
# # #                 "day": f"CAST({parsed_date} AS DATE)",
# # #                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
# # #                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
# # #                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
# # #             }
            
# # #             if time_grain in time_expressions:
# # #                 expr = time_expressions[time_grain]
# # #                 select_items.append(f"{expr} AS {time_grain}")
# # #                 group_by_items.append(f"{time_grain}")
# # #             else:
# # #                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

# # #         # 2. Add dimensions
# # #         # for dim in dimensions:
# # #         #     if dim not in self.registry.dimensions:
# # #         #         raise ValueError(f"Unknown dimension: {dim}")
# # #         #     dim_def = self.registry.dimensions[dim]
# # #         #     select_items.append(f'"{dim_def.column}" AS {dim}')
# # #         #     group_by_items.append(f'"{dim_def.column}"')
# # #         # 2. Add dimensions
# # #         for dim in dimensions:
# # #             if dim not in self.registry.dimensions:
# # #                 raise ValueError(f"Unknown dimension: {dim}")
# # #             dim_def = self.registry.dimensions[dim]
            
# # #             # Check if this is a CASE expression (contains "CASE" keyword)
# # #             if "CASE" in dim_def.column.upper():
# # #                 # For CASE expressions, use as-is without quotes
# # #                 select_items.append(f'({dim_def.column}) AS {dim}')
# # #                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
# # #             else:
# # #                 # For regular columns, use quotes
# # #                 select_items.append(f'"{dim_def.column}" AS {dim}')
# # #                 group_by_items.append(f'"{dim_def.column}"')
        
# # #         # 3. Add metric expression
# # #         select_items.append(f"{metric_def.expression} AS {metric}")
        
# # #         select_clause = "SELECT " + ", ".join(select_items)
        
# # #         # Build FROM clause
# # #         from_clause = f"FROM {fact_table}"
        
# # #         # Build WHERE clause
# # #         where_conditions = []
        
# # #         # Add date filter
# # #         if date_filter:
# # #             where_conditions.append(date_filter)
        
# # #         # Add metric-level mandatory filters
# # #         if metric_def.filters:
# # #             for filter_expr in metric_def.filters:
# # #                 where_conditions.append(filter_expr)
        
# # #         # ========================================
# # #         # ENHANCED: Better dimension filter handling
# # #         # ========================================
# # #         if filters:
# # #             for dim_name, filter_value in filters.items():
# # #                 if dim_name in self.registry.dimensions:
# # #                     dim_def = self.registry.dimensions[dim_name]
                    
# # #                     # Handle list of values (multi-value filter)
# # #                     if isinstance(filter_value, list) and len(filter_value) > 0:
# # #                         filter_condition = self._build_multi_value_filter(
# # #                             dim_def.column, 
# # #                             filter_value,
# # #                             dim_name
# # #                         )
# # #                         where_conditions.append(filter_condition)
                    
# # #                     # Handle single value
# # #                     elif isinstance(filter_value, str):
# # #                         filter_condition = self._build_single_value_filter(
# # #                             dim_def.column,
# # #                             filter_value,
# # #                             dim_name
# # #                         )
# # #                         where_conditions.append(filter_condition)
        
# # #         where_clause = ""
# # #         if where_conditions:
# # #             where_clause = "WHERE " + " AND ".join(where_conditions)
        
# # #         # Build GROUP BY clause
# # #         group_by_clause = ""
# # #         if group_by_items:
# # #             indices = [str(i+1) for i in range(len(group_by_items))]
# # #             group_by_clause = "GROUP BY " + ", ".join(indices)
        
# # #         # Build ORDER BY clause
# # #         order_by_clause = ""
# # #         if order_by:
# # #             direction = order_direction.upper() if order_direction else "DESC"
# # #             order_by_clause = f"ORDER BY {order_by} {direction}"
        
# # #         # Build LIMIT clause
# # #         limit_clause = ""
# # #         if limit:
# # #             limit_clause = f"LIMIT {limit}"
        
# # #         # Assemble final SQL
# # #         sql_parts = [
# # #             select_clause,
# # #             from_clause,
# # #             where_clause,
# # #             group_by_clause,
# # #             order_by_clause,
# # #             limit_clause
# # #         ]
        
# # #         sql = "\n".join([part for part in sql_parts if part])
        
# # #         return sql, warnings

# # #     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
# # #         """
# # #         Build filter for multiple values using OR logic.
        
# # #         Examples:
# # #             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
# # #             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
# # #         For specific dimensions like tower and floor_desc, we use exact matching.
# # #         For type_desc and sector, we use partial matching.
# # #         """
# # #         # Dimensions that need exact matching
# # #         exact_match_dims = ["tower", "floor_desc"]
        
# # #         if dim_name in exact_match_dims:
# # #             # Use exact matching with OR
# # #             conditions = []
# # #             for value in values:
# # #                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
# # #             return f"({' OR '.join(conditions)})"
# # #         else:
# # #             # Use partial matching with OR (for type_desc, sector, etc.)
# # #             conditions = []
# # #             for value in values:
# # #                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
# # #             return f"({' OR '.join(conditions)})"

# # #     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
# # #         """
# # #         Build filter for a single value.
# # #         Uses partial matching (LIKE) by default.
# # #         """
# # #         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'




# # # app/semantic/sql_builder.py
# # from typing import List, Optional, Tuple
# # from semantic.registry import SemanticRegistry
# # from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# # class SQLBuilder:
# #     """
# #     Builds SQL from semantic intent.
    
# #     ENHANCEMENTS:
# #     1. Better handling of multi-value filters with OR logic
# #     2. Improved partial matching for text filters
# #     3. Proper handling of type_desc and sector filters
# #     """

# #     def __init__(self, registry: SemanticRegistry):
# #         self.registry = registry

# #     def build(
# #         self,
# #         metric: str,
# #         dimensions: List[str],
# #         date_filter: Optional[str],
# #         filters: Optional[dict] = None,
# #         order_by: Optional[str] = None,
# #         order_direction: str = "DESC",
# #         limit: Optional[int] = None,
# #         time_grain: Optional[str] = None,
# #         metrics: Optional[List[str]] = None
# #     ) -> Tuple[str, List[str]]:
# #         """
# #         Build SQL query from semantic components.
# #         Supports multiple metrics via the `metrics` parameter.
        
# #         Returns:
# #             Tuple of (sql_string, warnings_list)
# #         """
# #         warnings = []

# #         # Resolve full list of metrics to emit
# #         all_metrics = metrics if metrics else [metric]

# #         # Validate all metrics
# #         for m in all_metrics:
# #             if m not in self.registry.metrics:
# #                 raise ValueError(f"Unknown metric: {m}")

# #         # Primary metric_def for backward compat
# #         metric_def = self.registry.metrics[all_metrics[0]]
# #         fact_table = self.registry.fact.table
        
# #         # Build SELECT clause
# #         select_items = []
# #         group_by_items = []
        
# #         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
# #         if time_grain:
# #             date_col = self.registry.fact.primary_date
# #             parsed_date = _date_sql(date_col)

# #             time_expressions = {
# #                 "day": f"CAST({parsed_date} AS DATE)",
# #                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
# #                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
# #                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
# #             }
            
# #             if time_grain in time_expressions:
# #                 expr = time_expressions[time_grain]
# #                 select_items.append(f"{expr} AS {time_grain}")
# #                 group_by_items.append(f"{time_grain}")
# #             else:
# #                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

# #         # 2. Add dimensions
# #         # for dim in dimensions:
# #         #     if dim not in self.registry.dimensions:
# #         #         raise ValueError(f"Unknown dimension: {dim}")
# #         #     dim_def = self.registry.dimensions[dim]
# #         #     select_items.append(f'"{dim_def.column}" AS {dim}')
# #         #     group_by_items.append(f'"{dim_def.column}"')
# #         # 2. Add dimensions
# #         for dim in dimensions:
# #             if dim not in self.registry.dimensions:
# #                 raise ValueError(f"Unknown dimension: {dim}")
# #             dim_def = self.registry.dimensions[dim]
            
# #             # Check if this is a CASE expression (contains "CASE" keyword)
# #             if "CASE" in dim_def.column.upper():
# #                 # For CASE expressions, use as-is without quotes
# #                 select_items.append(f'({dim_def.column}) AS {dim}')
# #                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
# #             else:
# #                 # For regular columns, use quotes
# #                 select_items.append(f'"{dim_def.column}" AS {dim}')
# #                 group_by_items.append(f'"{dim_def.column}"')
        
# #         # 3. Add metric expression(s) — collect mandatory filters from all metrics
# #         all_mandatory_filters = []
# #         seen_mfilters = set()
# #         for m in all_metrics:
# #             m_def = self.registry.metrics[m]
# #             if m_def.filters:
# #                 for f in m_def.filters:
# #                     if f not in seen_mfilters:
# #                         all_mandatory_filters.append(f)
# #                         seen_mfilters.add(f)
# #             select_items.append(f"{m_def.expression} AS {m}")
        
# #         select_clause = "SELECT " + ", ".join(select_items)
        
# #         # Build FROM clause
# #         from_clause = f"FROM {fact_table}"
        
# #         # Build WHERE clause
# #         where_conditions = []
        
# #         # Add date filter
# #         if date_filter:
# #             where_conditions.append(date_filter)
        
# #         # Add metric-level mandatory filters (unioned across all metrics)
# #         for filter_expr in all_mandatory_filters:
# #             where_conditions.append(filter_expr)
        
# #         # ========================================
# #         # ENHANCED: Better dimension filter handling
# #         # ========================================
# #         if filters:
# #             for dim_name, filter_value in filters.items():
# #                 if dim_name in self.registry.dimensions:
# #                     dim_def = self.registry.dimensions[dim_name]
                    
# #                     # Handle list of values (multi-value filter)
# #                     if isinstance(filter_value, list) and len(filter_value) > 0:
# #                         filter_condition = self._build_multi_value_filter(
# #                             dim_def.column, 
# #                             filter_value,
# #                             dim_name
# #                         )
# #                         where_conditions.append(filter_condition)
                    
# #                     # Handle single value
# #                     elif isinstance(filter_value, str):
# #                         filter_condition = self._build_single_value_filter(
# #                             dim_def.column,
# #                             filter_value,
# #                             dim_name
# #                         )
# #                         where_conditions.append(filter_condition)
        
# #         where_clause = ""
# #         if where_conditions:
# #             where_clause = "WHERE " + " AND ".join(where_conditions)
        
# #         # Build GROUP BY clause
# #         group_by_clause = ""
# #         if group_by_items:
# #             indices = [str(i+1) for i in range(len(group_by_items))]
# #             group_by_clause = "GROUP BY " + ", ".join(indices)
        
# #         # Build ORDER BY clause
# #         order_by_clause = ""
# #         if order_by:
# #             direction = order_direction.upper() if order_direction else "DESC"
# #             order_by_clause = f"ORDER BY {order_by} {direction}"
        
# #         # Build LIMIT clause
# #         limit_clause = ""
# #         if limit:
# #             limit_clause = f"LIMIT {limit}"
        
# #         # Assemble final SQL
# #         sql_parts = [
# #             select_clause,
# #             from_clause,
# #             where_clause,
# #             group_by_clause,
# #             order_by_clause,
# #             limit_clause
# #         ]
        
# #         sql = "\n".join([part for part in sql_parts if part])
        
# #         return sql, warnings

# #     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
# #         """
# #         Build filter for multiple values using OR logic.
        
# #         Examples:
# #             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
# #             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
# #         For specific dimensions like tower and floor_desc, we use exact matching.
# #         For type_desc and sector, we use partial matching.
# #         """
# #         # Dimensions that need exact matching
# #         exact_match_dims = ["tower", "floor_desc"]
        
# #         if dim_name in exact_match_dims:
# #             # Use exact matching with OR
# #             conditions = []
# #             for value in values:
# #                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
# #             return f"({' OR '.join(conditions)})"
# #         else:
# #             # Use partial matching with OR (for type_desc, sector, etc.)
# #             conditions = []
# #             for value in values:
# #                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
# #             return f"({' OR '.join(conditions)})"

# #     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
# #         """
# #         Build filter for a single value.
# #         Uses partial matching (LIKE) by default.
# #         """
# #         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'


















# # app/semantic/sql_builder.py
# from typing import List, Optional, Tuple
# from semantic.registry import SemanticRegistry
# from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# class SQLBuilder:
#     """
#     Builds SQL from semantic intent.
    
#     ENHANCEMENTS:
#     1. Better handling of multi-value filters with OR logic
#     2. Improved partial matching for text filters
#     3. Proper handling of type_desc and sector filters
#     """

#     def __init__(self, registry: SemanticRegistry):
#         self.registry = registry

#     # Cancelled filter expression — used to detect & strip it when needed
#     CANCELLED_FILTER = "LOWER(\"Customer_Type\") != 'cancelled'"

#     def build(
#         self,
#         metric: str,
#         dimensions: List[str],
#         date_filter: Optional[str],
#         filters: Optional[dict] = None,
#         order_by: Optional[str] = None,
#         order_direction: str = "DESC",
#         limit: Optional[int] = None,
#         time_grain: Optional[str] = None,
#         metrics: Optional[List[str]] = None
#     ) -> Tuple[str, List[str]]:
#         """
#         Build SQL query from semantic components.
#         Supports multiple metrics via the `metrics` parameter.

#         Special behaviour:
#           - When `customer_type` is in dimensions, the cancelled exclusion filter
#             is automatically removed so all customer types (including cancelled)
#             appear in the GROUP BY result.

#         Returns:
#             Tuple of (sql_string, warnings_list)
#         """
#         warnings = []

#         # Resolve full list of metrics to emit
#         all_metrics = metrics if metrics else [metric]

#         # Validate all metrics
#         for m in all_metrics:
#             if m not in self.registry.metrics:
#                 raise ValueError(f"Unknown metric: {m}")

#         # Primary metric_def for backward compat
#         metric_def = self.registry.metrics[all_metrics[0]]
#         fact_table = self.registry.fact.table

#         # Detect if we are grouping by customer_type — if so, include cancelled rows
#         grouping_by_customer_type = "customer_type" in dimensions
        
#         # Build SELECT clause
#         select_items = []
#         group_by_items = []
        
#         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
#         if time_grain:
#             date_col = self.registry.fact.primary_date
#             parsed_date = _date_sql(date_col)

#             time_expressions = {
#                 "day": f"CAST({parsed_date} AS DATE)",
#                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
#                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
#                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
#             }
            
#             if time_grain in time_expressions:
#                 expr = time_expressions[time_grain]
#                 select_items.append(f"{expr} AS {time_grain}")
#                 group_by_items.append(f"{time_grain}")
#             else:
#                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

#         # 2. Add dimensions
#         # for dim in dimensions:
#         #     if dim not in self.registry.dimensions:
#         #         raise ValueError(f"Unknown dimension: {dim}")
#         #     dim_def = self.registry.dimensions[dim]
#         #     select_items.append(f'"{dim_def.column}" AS {dim}')
#         #     group_by_items.append(f'"{dim_def.column}"')
#         # 2. Add dimensions
#         for dim in dimensions:
#             if dim not in self.registry.dimensions:
#                 raise ValueError(f"Unknown dimension: {dim}")
#             dim_def = self.registry.dimensions[dim]
            
#             # Check if this is a CASE expression (contains "CASE" keyword)
#             if "CASE" in dim_def.column.upper():
#                 # For CASE expressions, use as-is without quotes
#                 select_items.append(f'({dim_def.column}) AS {dim}')
#                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
#             else:
#                 # For regular columns, use quotes
#                 select_items.append(f'"{dim_def.column}" AS {dim}')
#                 group_by_items.append(f'"{dim_def.column}"')
        
#         # 3. Add metric expression(s) — collect mandatory filters from all metrics
#         all_mandatory_filters = []
#         seen_mfilters = set()
#         for m in all_metrics:
#             m_def = self.registry.metrics[m]
#             if m_def.filters:
#                 for f in m_def.filters:
#                     if f not in seen_mfilters:
#                         all_mandatory_filters.append(f)
#                         seen_mfilters.add(f)
#             select_items.append(f"{m_def.expression} AS {m}")

#         select_clause = "SELECT " + ", ".join(select_items)

#         # Build FROM clause
#         from_clause = f"FROM {fact_table}"

#         # Build WHERE clause
#         where_conditions = []

#         # Add date filter
#         if date_filter:
#             where_conditions.append(date_filter)

#         # Add metric-level mandatory filters.
#         # SPECIAL RULE: When grouping by customer_type, drop the != 'cancelled'
#         # exclusion so that cancelled rows appear as their own group in results.
#         for filter_expr in all_mandatory_filters:
#             if grouping_by_customer_type and "!= 'cancelled'" in filter_expr:
#                 print(f"[SQL_BUILDER] Dropping cancelled exclusion filter for customer_type breakdown: {filter_expr}")
#                 continue
#             where_conditions.append(filter_expr)
        
#         # ========================================
#         # ENHANCED: Better dimension filter handling
#         # ========================================
#         if filters:
#             for dim_name, filter_value in filters.items():
#                 if dim_name in self.registry.dimensions:
#                     dim_def = self.registry.dimensions[dim_name]
                    
#                     # Handle list of values (multi-value filter)
#                     if isinstance(filter_value, list) and len(filter_value) > 0:
#                         filter_condition = self._build_multi_value_filter(
#                             dim_def.column, 
#                             filter_value,
#                             dim_name
#                         )
#                         where_conditions.append(filter_condition)
                    
#                     # Handle single value
#                     elif isinstance(filter_value, str):
#                         filter_condition = self._build_single_value_filter(
#                             dim_def.column,
#                             filter_value,
#                             dim_name
#                         )
#                         where_conditions.append(filter_condition)
        
#         where_clause = ""
#         if where_conditions:
#             where_clause = "WHERE " + " AND ".join(where_conditions)
        
#         # Build GROUP BY clause
#         group_by_clause = ""
#         if group_by_items:
#             indices = [str(i+1) for i in range(len(group_by_items))]
#             group_by_clause = "GROUP BY " + ", ".join(indices)
        
#         # Build ORDER BY clause
#         order_by_clause = ""
#         if order_by:
#             direction = order_direction.upper() if order_direction else "DESC"
#             order_by_clause = f"ORDER BY {order_by} {direction}"
        
#         # Build LIMIT clause
#         limit_clause = ""
#         if limit:
#             limit_clause = f"LIMIT {limit}"
        
#         # Assemble final SQL
#         sql_parts = [
#             select_clause,
#             from_clause,
#             where_clause,
#             group_by_clause,
#             order_by_clause,
#             limit_clause
#         ]
        
#         sql = "\n".join([part for part in sql_parts if part])
        
#         return sql, warnings

#     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
#         """
#         Build filter for multiple values using OR logic.
        
#         Examples:
#             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
#             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
#         For specific dimensions like tower and floor_desc, we use exact matching.
#         For type_desc and sector, we use partial matching.
#         """
#         # Dimensions that need exact matching
#         exact_match_dims = ["tower", "floor_desc"]
        
#         if dim_name in exact_match_dims:
#             # Use exact matching with OR
#             conditions = []
#             for value in values:
#                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
#             return f"({' OR '.join(conditions)})"
#         else:
#             # Use partial matching with OR (for type_desc, sector, etc.)
#             conditions = []
#             for value in values:
#                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
#             return f"({' OR '.join(conditions)})"

#     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
#         """
#         Build filter for a single value.
#         Uses partial matching (LIKE) by default.
#         """
#         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'



















# # # app/semantic/sql_builder.py
# # from typing import List, Optional, Tuple
# # from semantic.registry import SemanticRegistry
# # from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# # class SQLBuilder:
# #     """
# #     Builds SQL from semantic intent.
    
# #     ENHANCEMENTS:
# #     1. Better handling of multi-value filters with OR logic
# #     2. Improved partial matching for text filters
# #     3. Proper handling of type_desc and sector filters
# #     """

# #     def __init__(self, registry: SemanticRegistry):
# #         self.registry = registry

# #     def build(
# #         self,
# #         metric: str,
# #         dimensions: List[str],
# #         date_filter: Optional[str],
# #         filters: Optional[dict] = None,
# #         order_by: Optional[str] = None,
# #         order_direction: str = "DESC",
# #         limit: Optional[int] = None,
# #         time_grain: Optional[str] = None
# #     ) -> Tuple[str, List[str]]:
# #         """
# #         Build SQL query from semantic components.
        
# #         Returns:
# #             Tuple of (sql_string, warnings_list)
# #         """
# #         warnings = []
        
# #         # Get metric definition
# #         if metric not in self.registry.metrics:
# #             raise ValueError(f"Unknown metric: {metric}")
        
# #         metric_def = self.registry.metrics[metric]
# #         fact_table = self.registry.fact.table
        
# #         # Build SELECT clause
# #         select_items = []
# #         group_by_items = []
        
# #         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
# #         if time_grain:
# #             date_col = self.registry.fact.primary_date
# #             parsed_date = _date_sql(date_col)

# #             time_expressions = {
# #                 "day": f"CAST({parsed_date} AS DATE)",
# #                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
# #                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
# #                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
# #             }
            
# #             if time_grain in time_expressions:
# #                 expr = time_expressions[time_grain]
# #                 select_items.append(f"{expr} AS {time_grain}")
# #                 group_by_items.append(f"{time_grain}")
# #             else:
# #                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

# #         # 2. Add dimensions
# #         # for dim in dimensions:
# #         #     if dim not in self.registry.dimensions:
# #         #         raise ValueError(f"Unknown dimension: {dim}")
# #         #     dim_def = self.registry.dimensions[dim]
# #         #     select_items.append(f'"{dim_def.column}" AS {dim}')
# #         #     group_by_items.append(f'"{dim_def.column}"')
# #         # 2. Add dimensions
# #         for dim in dimensions:
# #             if dim not in self.registry.dimensions:
# #                 raise ValueError(f"Unknown dimension: {dim}")
# #             dim_def = self.registry.dimensions[dim]
            
# #             # Check if this is a CASE expression (contains "CASE" keyword)
# #             if "CASE" in dim_def.column.upper():
# #                 # For CASE expressions, use as-is without quotes
# #                 select_items.append(f'({dim_def.column}) AS {dim}')
# #                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
# #             else:
# #                 # For regular columns, use quotes
# #                 select_items.append(f'"{dim_def.column}" AS {dim}')
# #                 group_by_items.append(f'"{dim_def.column}"')
        
# #         # 3. Add metric expression
# #         select_items.append(f"{metric_def.expression} AS {metric}")
        
# #         select_clause = "SELECT " + ", ".join(select_items)
        
# #         # Build FROM clause
# #         from_clause = f"FROM {fact_table}"
        
# #         # Build WHERE clause
# #         where_conditions = []
        
# #         # Add date filter
# #         if date_filter:
# #             where_conditions.append(date_filter)
        
# #         # Add metric-level mandatory filters
# #         if metric_def.filters:
# #             for filter_expr in metric_def.filters:
# #                 where_conditions.append(filter_expr)
        
# #         # ========================================
# #         # ENHANCED: Better dimension filter handling
# #         # ========================================
# #         if filters:
# #             for dim_name, filter_value in filters.items():
# #                 if dim_name in self.registry.dimensions:
# #                     dim_def = self.registry.dimensions[dim_name]
                    
# #                     # Handle list of values (multi-value filter)
# #                     if isinstance(filter_value, list) and len(filter_value) > 0:
# #                         filter_condition = self._build_multi_value_filter(
# #                             dim_def.column, 
# #                             filter_value,
# #                             dim_name
# #                         )
# #                         where_conditions.append(filter_condition)
                    
# #                     # Handle single value
# #                     elif isinstance(filter_value, str):
# #                         filter_condition = self._build_single_value_filter(
# #                             dim_def.column,
# #                             filter_value,
# #                             dim_name
# #                         )
# #                         where_conditions.append(filter_condition)
        
# #         where_clause = ""
# #         if where_conditions:
# #             where_clause = "WHERE " + " AND ".join(where_conditions)
        
# #         # Build GROUP BY clause
# #         group_by_clause = ""
# #         if group_by_items:
# #             indices = [str(i+1) for i in range(len(group_by_items))]
# #             group_by_clause = "GROUP BY " + ", ".join(indices)
        
# #         # Build ORDER BY clause
# #         order_by_clause = ""
# #         if order_by:
# #             direction = order_direction.upper() if order_direction else "DESC"
# #             order_by_clause = f"ORDER BY {order_by} {direction}"
        
# #         # Build LIMIT clause
# #         limit_clause = ""
# #         if limit:
# #             limit_clause = f"LIMIT {limit}"
        
# #         # Assemble final SQL
# #         sql_parts = [
# #             select_clause,
# #             from_clause,
# #             where_clause,
# #             group_by_clause,
# #             order_by_clause,
# #             limit_clause
# #         ]
        
# #         sql = "\n".join([part for part in sql_parts if part])
        
# #         return sql, warnings

# #     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
# #         """
# #         Build filter for multiple values using OR logic.
        
# #         Examples:
# #             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
# #             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
# #         For specific dimensions like tower and floor_desc, we use exact matching.
# #         For type_desc and sector, we use partial matching.
# #         """
# #         # Dimensions that need exact matching
# #         exact_match_dims = ["tower", "floor_desc"]
        
# #         if dim_name in exact_match_dims:
# #             # Use exact matching with OR
# #             conditions = []
# #             for value in values:
# #                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
# #             return f"({' OR '.join(conditions)})"
# #         else:
# #             # Use partial matching with OR (for type_desc, sector, etc.)
# #             conditions = []
# #             for value in values:
# #                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
# #             return f"({' OR '.join(conditions)})"

# #     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
# #         """
# #         Build filter for a single value.
# #         Uses partial matching (LIKE) by default.
# #         """
# #         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'




# # app/semantic/sql_builder.py
# from typing import List, Optional, Tuple
# from semantic.registry import SemanticRegistry
# from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# class SQLBuilder:
#     """
#     Builds SQL from semantic intent.
    
#     ENHANCEMENTS:
#     1. Better handling of multi-value filters with OR logic
#     2. Improved partial matching for text filters
#     3. Proper handling of type_desc and sector filters
#     """

#     def __init__(self, registry: SemanticRegistry):
#         self.registry = registry

#     def build(
#         self,
#         metric: str,
#         dimensions: List[str],
#         date_filter: Optional[str],
#         filters: Optional[dict] = None,
#         order_by: Optional[str] = None,
#         order_direction: str = "DESC",
#         limit: Optional[int] = None,
#         time_grain: Optional[str] = None,
#         metrics: Optional[List[str]] = None
#     ) -> Tuple[str, List[str]]:
#         """
#         Build SQL query from semantic components.
#         Supports multiple metrics via the `metrics` parameter.
        
#         Returns:
#             Tuple of (sql_string, warnings_list)
#         """
#         warnings = []

#         # Resolve full list of metrics to emit
#         all_metrics = metrics if metrics else [metric]

#         # Validate all metrics
#         for m in all_metrics:
#             if m not in self.registry.metrics:
#                 raise ValueError(f"Unknown metric: {m}")

#         # Primary metric_def for backward compat
#         metric_def = self.registry.metrics[all_metrics[0]]
#         fact_table = self.registry.fact.table
        
#         # Build SELECT clause
#         select_items = []
#         group_by_items = []
        
#         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
#         if time_grain:
#             date_col = self.registry.fact.primary_date
#             parsed_date = _date_sql(date_col)

#             time_expressions = {
#                 "day": f"CAST({parsed_date} AS DATE)",
#                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
#                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
#                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
#             }
            
#             if time_grain in time_expressions:
#                 expr = time_expressions[time_grain]
#                 select_items.append(f"{expr} AS {time_grain}")
#                 group_by_items.append(f"{time_grain}")
#             else:
#                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

#         # 2. Add dimensions
#         # for dim in dimensions:
#         #     if dim not in self.registry.dimensions:
#         #         raise ValueError(f"Unknown dimension: {dim}")
#         #     dim_def = self.registry.dimensions[dim]
#         #     select_items.append(f'"{dim_def.column}" AS {dim}')
#         #     group_by_items.append(f'"{dim_def.column}"')
#         # 2. Add dimensions
#         for dim in dimensions:
#             if dim not in self.registry.dimensions:
#                 raise ValueError(f"Unknown dimension: {dim}")
#             dim_def = self.registry.dimensions[dim]
            
#             # Check if this is a CASE expression (contains "CASE" keyword)
#             if "CASE" in dim_def.column.upper():
#                 # For CASE expressions, use as-is without quotes
#                 select_items.append(f'({dim_def.column}) AS {dim}')
#                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
#             else:
#                 # For regular columns, use quotes
#                 select_items.append(f'"{dim_def.column}" AS {dim}')
#                 group_by_items.append(f'"{dim_def.column}"')
        
#         # 3. Add metric expression(s) — collect mandatory filters from all metrics
#         all_mandatory_filters = []
#         seen_mfilters = set()
#         for m in all_metrics:
#             m_def = self.registry.metrics[m]
#             if m_def.filters:
#                 for f in m_def.filters:
#                     if f not in seen_mfilters:
#                         all_mandatory_filters.append(f)
#                         seen_mfilters.add(f)
#             select_items.append(f"{m_def.expression} AS {m}")
        
#         select_clause = "SELECT " + ", ".join(select_items)
        
#         # Build FROM clause
#         from_clause = f"FROM {fact_table}"
        
#         # Build WHERE clause
#         where_conditions = []
        
#         # Add date filter
#         if date_filter:
#             where_conditions.append(date_filter)
        
#         # Add metric-level mandatory filters (unioned across all metrics)
#         for filter_expr in all_mandatory_filters:
#             where_conditions.append(filter_expr)
        
#         # ========================================
#         # ENHANCED: Better dimension filter handling
#         # ========================================
#         if filters:
#             for dim_name, filter_value in filters.items():
#                 if dim_name in self.registry.dimensions:
#                     dim_def = self.registry.dimensions[dim_name]
                    
#                     # Handle list of values (multi-value filter)
#                     if isinstance(filter_value, list) and len(filter_value) > 0:
#                         filter_condition = self._build_multi_value_filter(
#                             dim_def.column, 
#                             filter_value,
#                             dim_name
#                         )
#                         where_conditions.append(filter_condition)
                    
#                     # Handle single value
#                     elif isinstance(filter_value, str):
#                         filter_condition = self._build_single_value_filter(
#                             dim_def.column,
#                             filter_value,
#                             dim_name
#                         )
#                         where_conditions.append(filter_condition)
        
#         where_clause = ""
#         if where_conditions:
#             where_clause = "WHERE " + " AND ".join(where_conditions)
        
#         # Build GROUP BY clause
#         group_by_clause = ""
#         if group_by_items:
#             indices = [str(i+1) for i in range(len(group_by_items))]
#             group_by_clause = "GROUP BY " + ", ".join(indices)
        
#         # Build ORDER BY clause
#         order_by_clause = ""
#         if order_by:
#             direction = order_direction.upper() if order_direction else "DESC"
#             order_by_clause = f"ORDER BY {order_by} {direction}"
        
#         # Build LIMIT clause
#         limit_clause = ""
#         if limit:
#             limit_clause = f"LIMIT {limit}"
        
#         # Assemble final SQL
#         sql_parts = [
#             select_clause,
#             from_clause,
#             where_clause,
#             group_by_clause,
#             order_by_clause,
#             limit_clause
#         ]
        
#         sql = "\n".join([part for part in sql_parts if part])
        
#         return sql, warnings

#     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
#         """
#         Build filter for multiple values using OR logic.
        
#         Examples:
#             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
#             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
#         For specific dimensions like tower and floor_desc, we use exact matching.
#         For type_desc and sector, we use partial matching.
#         """
#         # Dimensions that need exact matching
#         exact_match_dims = ["tower", "floor_desc"]
        
#         if dim_name in exact_match_dims:
#             # Use exact matching with OR
#             conditions = []
#             for value in values:
#                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
#             return f"({' OR '.join(conditions)})"
#         else:
#             # Use partial matching with OR (for type_desc, sector, etc.)
#             conditions = []
#             for value in values:
#                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
#             return f"({' OR '.join(conditions)})"

#     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
#         """
#         Build filter for a single value.
#         Uses partial matching (LIKE) by default.
#         """
#         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'


















# # # app/semantic/sql_builder.py
# # from typing import List, Optional, Tuple
# # from semantic.registry import SemanticRegistry
# # from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# # class SQLBuilder:
# #     """
# #     Builds SQL from semantic intent.
    
# #     ENHANCEMENTS:
# #     1. Better handling of multi-value filters with OR logic
# #     2. Improved partial matching for text filters
# #     3. Proper handling of type_desc and sector filters
# #     """

# #     def __init__(self, registry: SemanticRegistry):
# #         self.registry = registry

# #     def build(
# #         self,
# #         metric: str,
# #         dimensions: List[str],
# #         date_filter: Optional[str],
# #         filters: Optional[dict] = None,
# #         order_by: Optional[str] = None,
# #         order_direction: str = "DESC",
# #         limit: Optional[int] = None,
# #         time_grain: Optional[str] = None
# #     ) -> Tuple[str, List[str]]:
# #         """
# #         Build SQL query from semantic components.
        
# #         Returns:
# #             Tuple of (sql_string, warnings_list)
# #         """
# #         warnings = []
        
# #         # Get metric definition
# #         if metric not in self.registry.metrics:
# #             raise ValueError(f"Unknown metric: {metric}")
        
# #         metric_def = self.registry.metrics[metric]
# #         fact_table = self.registry.fact.table
        
# #         # Build SELECT clause
# #         select_items = []
# #         group_by_items = []
        
# #         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
# #         if time_grain:
# #             date_col = self.registry.fact.primary_date
# #             parsed_date = _date_sql(date_col)

# #             time_expressions = {
# #                 "day": f"CAST({parsed_date} AS DATE)",
# #                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
# #                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
# #                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
# #             }
            
# #             if time_grain in time_expressions:
# #                 expr = time_expressions[time_grain]
# #                 select_items.append(f"{expr} AS {time_grain}")
# #                 group_by_items.append(f"{time_grain}")
# #             else:
# #                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

# #         # 2. Add dimensions
# #         # for dim in dimensions:
# #         #     if dim not in self.registry.dimensions:
# #         #         raise ValueError(f"Unknown dimension: {dim}")
# #         #     dim_def = self.registry.dimensions[dim]
# #         #     select_items.append(f'"{dim_def.column}" AS {dim}')
# #         #     group_by_items.append(f'"{dim_def.column}"')
# #         # 2. Add dimensions
# #         for dim in dimensions:
# #             if dim not in self.registry.dimensions:
# #                 raise ValueError(f"Unknown dimension: {dim}")
# #             dim_def = self.registry.dimensions[dim]
            
# #             # Check if this is a CASE expression (contains "CASE" keyword)
# #             if "CASE" in dim_def.column.upper():
# #                 # For CASE expressions, use as-is without quotes
# #                 select_items.append(f'({dim_def.column}) AS {dim}')
# #                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
# #             else:
# #                 # For regular columns, use quotes
# #                 select_items.append(f'"{dim_def.column}" AS {dim}')
# #                 group_by_items.append(f'"{dim_def.column}"')
        
# #         # 3. Add metric expression
# #         select_items.append(f"{metric_def.expression} AS {metric}")
        
# #         select_clause = "SELECT " + ", ".join(select_items)
        
# #         # Build FROM clause
# #         from_clause = f"FROM {fact_table}"
        
# #         # Build WHERE clause
# #         where_conditions = []
        
# #         # Add date filter
# #         if date_filter:
# #             where_conditions.append(date_filter)
        
# #         # Add metric-level mandatory filters
# #         if metric_def.filters:
# #             for filter_expr in metric_def.filters:
# #                 where_conditions.append(filter_expr)
        
# #         # ========================================
# #         # ENHANCED: Better dimension filter handling
# #         # ========================================
# #         if filters:
# #             for dim_name, filter_value in filters.items():
# #                 if dim_name in self.registry.dimensions:
# #                     dim_def = self.registry.dimensions[dim_name]
                    
# #                     # Handle list of values (multi-value filter)
# #                     if isinstance(filter_value, list) and len(filter_value) > 0:
# #                         filter_condition = self._build_multi_value_filter(
# #                             dim_def.column, 
# #                             filter_value,
# #                             dim_name
# #                         )
# #                         where_conditions.append(filter_condition)
                    
# #                     # Handle single value
# #                     elif isinstance(filter_value, str):
# #                         filter_condition = self._build_single_value_filter(
# #                             dim_def.column,
# #                             filter_value,
# #                             dim_name
# #                         )
# #                         where_conditions.append(filter_condition)
        
# #         where_clause = ""
# #         if where_conditions:
# #             where_clause = "WHERE " + " AND ".join(where_conditions)
        
# #         # Build GROUP BY clause
# #         group_by_clause = ""
# #         if group_by_items:
# #             indices = [str(i+1) for i in range(len(group_by_items))]
# #             group_by_clause = "GROUP BY " + ", ".join(indices)
        
# #         # Build ORDER BY clause
# #         order_by_clause = ""
# #         if order_by:
# #             direction = order_direction.upper() if order_direction else "DESC"
# #             order_by_clause = f"ORDER BY {order_by} {direction}"
        
# #         # Build LIMIT clause
# #         limit_clause = ""
# #         if limit:
# #             limit_clause = f"LIMIT {limit}"
        
# #         # Assemble final SQL
# #         sql_parts = [
# #             select_clause,
# #             from_clause,
# #             where_clause,
# #             group_by_clause,
# #             order_by_clause,
# #             limit_clause
# #         ]
        
# #         sql = "\n".join([part for part in sql_parts if part])
        
# #         return sql, warnings

# #     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
# #         """
# #         Build filter for multiple values using OR logic.
        
# #         Examples:
# #             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
# #             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
# #         For specific dimensions like tower and floor_desc, we use exact matching.
# #         For type_desc and sector, we use partial matching.
# #         """
# #         # Dimensions that need exact matching
# #         exact_match_dims = ["tower", "floor_desc"]
        
# #         if dim_name in exact_match_dims:
# #             # Use exact matching with OR
# #             conditions = []
# #             for value in values:
# #                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
# #             return f"({' OR '.join(conditions)})"
# #         else:
# #             # Use partial matching with OR (for type_desc, sector, etc.)
# #             conditions = []
# #             for value in values:
# #                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
# #             return f"({' OR '.join(conditions)})"

# #     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
# #         """
# #         Build filter for a single value.
# #         Uses partial matching (LIKE) by default.
# #         """
# #         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'




# # app/semantic/sql_builder.py
# from typing import List, Optional, Tuple
# from semantic.registry import SemanticRegistry
# from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


# class SQLBuilder:
#     """
#     Builds SQL from semantic intent.
    
#     ENHANCEMENTS:
#     1. Better handling of multi-value filters with OR logic
#     2. Improved partial matching for text filters
#     3. Proper handling of type_desc and sector filters
#     """

#     def __init__(self, registry: SemanticRegistry):
#         self.registry = registry

#     def build(
#         self,
#         metric: str,
#         dimensions: List[str],
#         date_filter: Optional[str],
#         filters: Optional[dict] = None,
#         order_by: Optional[str] = None,
#         order_direction: str = "DESC",
#         limit: Optional[int] = None,
#         time_grain: Optional[str] = None,
#         metrics: Optional[List[str]] = None
#     ) -> Tuple[str, List[str]]:
#         """
#         Build SQL query from semantic components.
#         Supports multiple metrics via the `metrics` parameter.
        
#         Returns:
#             Tuple of (sql_string, warnings_list)
#         """
#         warnings = []

#         # Resolve full list of metrics to emit
#         all_metrics = metrics if metrics else [metric]

#         # Validate all metrics
#         for m in all_metrics:
#             if m not in self.registry.metrics:
#                 raise ValueError(f"Unknown metric: {m}")

#         # Primary metric_def for backward compat
#         metric_def = self.registry.metrics[all_metrics[0]]
#         fact_table = self.registry.fact.table
        
#         # Build SELECT clause
#         select_items = []
#         group_by_items = []
        
#         # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
#         if time_grain:
#             date_col = self.registry.fact.primary_date
#             parsed_date = _date_sql(date_col)

#             time_expressions = {
#                 "day": f"CAST({parsed_date} AS DATE)",
#                 "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
#                 "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
#                 "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
#             }
            
#             if time_grain in time_expressions:
#                 expr = time_expressions[time_grain]
#                 select_items.append(f"{expr} AS {time_grain}")
#                 group_by_items.append(f"{time_grain}")
#             else:
#                 warnings.append(f"Unknown time grain: {time_grain} - ignored")

#         # 2. Add dimensions
#         # for dim in dimensions:
#         #     if dim not in self.registry.dimensions:
#         #         raise ValueError(f"Unknown dimension: {dim}")
#         #     dim_def = self.registry.dimensions[dim]
#         #     select_items.append(f'"{dim_def.column}" AS {dim}')
#         #     group_by_items.append(f'"{dim_def.column}"')
#         # 2. Add dimensions
#         for dim in dimensions:
#             if dim not in self.registry.dimensions:
#                 raise ValueError(f"Unknown dimension: {dim}")
#             dim_def = self.registry.dimensions[dim]
            
#             # Check if this is a CASE expression (contains "CASE" keyword)
#             if "CASE" in dim_def.column.upper():
#                 # For CASE expressions, use as-is without quotes
#                 select_items.append(f'({dim_def.column}) AS {dim}')
#                 group_by_items.append(f'{dim}')  # Use alias in GROUP BY
#             else:
#                 # For regular columns, use quotes
#                 select_items.append(f'"{dim_def.column}" AS {dim}')
#                 group_by_items.append(f'"{dim_def.column}"')
        
#         # 3. Add metric expression(s) — collect mandatory filters from all metrics
#         all_mandatory_filters = []
#         seen_mfilters = set()
#         for m in all_metrics:
#             m_def = self.registry.metrics[m]
#             if m_def.filters:
#                 for f in m_def.filters:
#                     if f not in seen_mfilters:
#                         all_mandatory_filters.append(f)
#                         seen_mfilters.add(f)
#             select_items.append(f"{m_def.expression} AS {m}")
        
#         select_clause = "SELECT " + ", ".join(select_items)
        
#         # Build FROM clause
#         from_clause = f"FROM {fact_table}"
        
#         # Build WHERE clause
#         where_conditions = []
        
#         # Add date filter
#         if date_filter:
#             where_conditions.append(date_filter)
        
#         # Add metric-level mandatory filters (unioned across all metrics)
#         for filter_expr in all_mandatory_filters:
#             where_conditions.append(filter_expr)
        
#         # ========================================
#         # ENHANCED: Better dimension filter handling
#         # ========================================
#         if filters:
#             for dim_name, filter_value in filters.items():
#                 if dim_name in self.registry.dimensions:
#                     dim_def = self.registry.dimensions[dim_name]
                    
#                     # Handle list of values (multi-value filter)
#                     if isinstance(filter_value, list) and len(filter_value) > 0:
#                         filter_condition = self._build_multi_value_filter(
#                             dim_def.column, 
#                             filter_value,
#                             dim_name
#                         )
#                         where_conditions.append(filter_condition)
                    
#                     # Handle single value
#                     elif isinstance(filter_value, str):
#                         filter_condition = self._build_single_value_filter(
#                             dim_def.column,
#                             filter_value,
#                             dim_name
#                         )
#                         where_conditions.append(filter_condition)
        
#         where_clause = ""
#         if where_conditions:
#             where_clause = "WHERE " + " AND ".join(where_conditions)
        
#         # Build GROUP BY clause
#         group_by_clause = ""
#         if group_by_items:
#             indices = [str(i+1) for i in range(len(group_by_items))]
#             group_by_clause = "GROUP BY " + ", ".join(indices)
        
#         # Build ORDER BY clause
#         order_by_clause = ""
#         if order_by:
#             direction = order_direction.upper() if order_direction else "DESC"
#             order_by_clause = f"ORDER BY {order_by} {direction}"
        
#         # Build LIMIT clause
#         limit_clause = ""
#         if limit:
#             limit_clause = f"LIMIT {limit}"
        
#         # Assemble final SQL
#         sql_parts = [
#             select_clause,
#             from_clause,
#             where_clause,
#             group_by_clause,
#             order_by_clause,
#             limit_clause
#         ]
        
#         sql = "\n".join([part for part in sql_parts if part])
        
#         return sql, warnings

#     def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
#         """
#         Build filter for multiple values using OR logic.
        
#         Examples:
#             ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
#             ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
#         For specific dimensions like tower and floor_desc, we use exact matching.
#         For type_desc and sector, we use partial matching.
#         """
#         # Dimensions that need exact matching
#         exact_match_dims = ["tower", "floor_desc"]
        
#         if dim_name in exact_match_dims:
#             # Use exact matching with OR
#             conditions = []
#             for value in values:
#                 conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
#             return f"({' OR '.join(conditions)})"
#         else:
#             # Use partial matching with OR (for type_desc, sector, etc.)
#             conditions = []
#             for value in values:
#                 conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
#             return f"({' OR '.join(conditions)})"

#     def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
#         """
#         Build filter for a single value.
#         Uses partial matching (LIKE) by default.
#         """
#         return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'


















# app/semantic/sql_builder.py
from typing import List, Optional, Tuple
from semantic.registry import SemanticRegistry
from semantic.date_resolver import _financial_year_sql, _financial_quarter_sql, _date_sql


class SQLBuilder:
    """
    Builds SQL from semantic intent.
    
    ENHANCEMENTS:
    1. Better handling of multi-value filters with OR logic
    2. Improved partial matching for text filters
    3. Proper handling of type_desc and sector filters
    """

    def __init__(self, registry: SemanticRegistry):
        self.registry = registry

    # Cancelled filter expression — used to detect & strip it when needed
    CANCELLED_FILTER = "LOWER(\"Customer_Type\") != 'cancelled'"

    def build(
        self,
        metric: str,
        dimensions: List[str],
        date_filter: Optional[str],
        filters: Optional[dict] = None,
        order_by: Optional[str] = None,
        order_direction: str = "DESC",
        limit: Optional[int] = None,
        time_grain: Optional[str] = None,
        metrics: Optional[List[str]] = None,
        having_filter: Optional[str] = None
    ) -> Tuple[str, List[str]]:
        """
        Build SQL query from semantic components.
        Supports multiple metrics via the `metrics` parameter.

        Special behaviour:
          - When `customer_type` is in dimensions, the cancelled exclusion filter
            is automatically removed so all customer types (including cancelled)
            appear in the GROUP BY result.

        Returns:
            Tuple of (sql_string, warnings_list)
        """
        warnings = []

        # Resolve full list of metrics to emit
        all_metrics = metrics if metrics else [metric]

        # Validate all metrics
        for m in all_metrics:
            if m not in self.registry.metrics:
                raise ValueError(f"Unknown metric: {m}")

        # Primary metric_def for backward compat
        metric_def = self.registry.metrics[all_metrics[0]]
        fact_table = self.registry.fact.table

        # Detect if we are grouping by customer_type — if so, include cancelled rows
        grouping_by_customer_type = "customer_type" in dimensions
        
        # Build SELECT clause
        select_items = []
        group_by_items = []
        
        # 1. Handle Time Grain with FINANCIAL YEAR LOGIC
        if time_grain:
            date_col = self.registry.fact.primary_date
            parsed_date = _date_sql(date_col)

            time_expressions = {
                "day": f"CAST({parsed_date} AS DATE)",
                "month": f"DATE_FORMAT({parsed_date}, '%Y-%m')",
                "quarter": f"CONCAT('FY', CAST({_financial_year_sql(date_col)} AS VARCHAR), '-Q', CAST({_financial_quarter_sql(date_col)} AS VARCHAR))",
                "year": f"CAST({_financial_year_sql(date_col)} AS VARCHAR)"
            }
            
            if time_grain in time_expressions:
                expr = time_expressions[time_grain]
                select_items.append(f"{expr} AS {time_grain}")
                group_by_items.append(f"{time_grain}")
            else:
                warnings.append(f"Unknown time grain: {time_grain} - ignored")

        # 2. Add dimensions
        # for dim in dimensions:
        #     if dim not in self.registry.dimensions:
        #         raise ValueError(f"Unknown dimension: {dim}")
        #     dim_def = self.registry.dimensions[dim]
        #     select_items.append(f'"{dim_def.column}" AS {dim}')
        #     group_by_items.append(f'"{dim_def.column}"')
        # 2. Add dimensions
        # Dimension key -> preferred output alias (snake_case column name)
        # Some registry keys like "project" are shorthand but the column alias
        # shown in results should reflect the actual column (e.g. sales_org_desc)
        DIM_ALIAS_MAP = {
            "project":    "sales_org_desc",
            "product":    "sales_group_desc",
            "product_desc": "sales_group_desc",
            "tower_desc": "tower",
            "type":       "type_desc",
            "broker":     "broker_name",
            "sub_broker": "sub_broker_name",
            "payer":      "payer_name",
            "sold_to":    "sold_to_name",
            "sales_executive": "sales_executive_name",
            "back_office_executive": "back_office_executive_name",
            "distribution_channel": "dist_channel_desc",
            "sale_organization": "sales_org_desc",
            "sales_org":  "sales_org_desc",
            "sales_group": "sales_group_desc",
            "sales_office": "sales_office_desc",
            "division":   "division_desc",
            "material_pricing_group": "material_pricing_group_desc",
            "material_group": "material_pricing_group_desc",
            "consortium": "consortium_name",
        }

        for dim in dimensions:
            if dim not in self.registry.dimensions:
                raise ValueError(f"Unknown dimension: {dim}")
            dim_def = self.registry.dimensions[dim]

            # Use a clean snake_case alias: prefer DIM_ALIAS_MAP, else fall back to the dimension key
            # (NOT the raw column expression, which may contain SQL keywords like CASE)
            col_alias = DIM_ALIAS_MAP.get(dim, dim)

            # Check if this is a CASE expression (contains "CASE" keyword)
            if "CASE" in dim_def.column.upper():
                select_items.append(f'({dim_def.column}) AS {col_alias}')
                group_by_items.append(f'{col_alias}')
            else:
                select_items.append(f'"{dim_def.column}" AS {col_alias}')
                group_by_items.append(f'"{dim_def.column}"')
        
        # 3. Add metric expression(s) — collect mandatory filters from all metrics
        all_mandatory_filters = []
        seen_mfilters = set()
        for m in all_metrics:
            m_def = self.registry.metrics[m]
            if m_def.filters:
                for f in m_def.filters:
                    if f not in seen_mfilters:
                        all_mandatory_filters.append(f)
                        seen_mfilters.add(f)
            select_items.append(f"{m_def.expression} AS {m}")

        select_clause = "SELECT " + ", ".join(select_items)

        # Build FROM clause
        from_clause = f"FROM {fact_table}"

        # Build WHERE clause
        where_conditions = []

        # Add date filter
        if date_filter:
            where_conditions.append(date_filter)

        # Add metric-level mandatory filters.
        # SPECIAL RULE: When grouping by customer_type, drop the != 'cancelled'
        # exclusion so that cancelled rows appear as their own group in results.
        for filter_expr in all_mandatory_filters:
            if grouping_by_customer_type and "!= 'cancelled'" in filter_expr:
                print(f"[SQL_BUILDER] Dropping cancelled exclusion filter for customer_type breakdown: {filter_expr}")
                continue
            where_conditions.append(filter_expr)
        
        # ========================================
        # ENHANCED: Better dimension filter handling
        # ========================================
        if filters:
            for dim_name, filter_value in filters.items():
                if dim_name in self.registry.dimensions:
                    dim_def = self.registry.dimensions[dim_name]
                    
                    # Handle list of values (multi-value filter)
                    if isinstance(filter_value, list) and len(filter_value) > 0:
                        filter_condition = self._build_multi_value_filter(
                            dim_def.column, 
                            filter_value,
                            dim_name
                        )
                        where_conditions.append(filter_condition)
                    
                    # Handle single value
                    elif isinstance(filter_value, str):
                        filter_condition = self._build_single_value_filter(
                            dim_def.column,
                            filter_value,
                            dim_name
                        )
                        where_conditions.append(filter_condition)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Build GROUP BY clause
        group_by_clause = ""
        if group_by_items:
            indices = [str(i+1) for i in range(len(group_by_items))]
            group_by_clause = "GROUP BY " + ", ".join(indices)
        
        # Build HAVING clause (for aggregate filters like "value > 50000")
        having_clause = ""
        if having_filter:
            having_clause = f"HAVING {having_filter}"
        
        # Build ORDER BY clause
        order_by_clause = ""
        if order_by:
            direction = order_direction.upper() if order_direction else "DESC"
            order_by_clause = f"ORDER BY {order_by} {direction}"
        
        # Build LIMIT clause
        limit_clause = ""
        if limit:
            limit_clause = f"LIMIT {limit}"
        
        # Assemble final SQL
        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            group_by_clause,
            having_clause,
            order_by_clause,
            limit_clause
        ]
        
        sql = "\n".join([part for part in sql_parts if part])
        
        return sql, warnings

    def _build_multi_value_filter(self, column: str, values: List[str], dim_name: str) -> str:
        """
        Build filter for multiple values using OR logic.
        
        Examples:
            ["a", "7"] -> (column LIKE '%a%' OR column LIKE '%7%')
            ["apartment", "plot"] -> (column LIKE '%apartment%' OR column LIKE '%plot%')
        
        For specific dimensions like tower and floor_desc, we use exact matching.
        For type_desc and sector, we use partial matching.
        """
        # Dimensions that need exact matching
        exact_match_dims = ["tower", "floor_desc"]
        
        if dim_name in exact_match_dims:
            # Use exact matching with OR
            conditions = []
            for value in values:
                conditions.append(f'LOWER("{column}") = LOWER(\'{value}\')')
            return f"({' OR '.join(conditions)})"
        else:
            # Use partial matching with OR (for type_desc, sector, etc.)
            conditions = []
            for value in values:
                conditions.append(f'LOWER("{column}") LIKE LOWER(\'%{value}%\')')
            return f"({' OR '.join(conditions)})"

    def _build_single_value_filter(self, column: str, value: str, dim_name: str) -> str:
        """
        Build filter for a single value.
        Uses partial matching (LIKE) by default.
        """
        return f'LOWER("{column}") LIKE LOWER(\'%{value}%\')'