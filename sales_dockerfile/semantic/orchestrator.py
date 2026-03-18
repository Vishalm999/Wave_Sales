


# # from semantic.intent import SemanticIntent
# # from semantic.registry import SemanticRegistry
# # from semantic.validator import SemanticValidator
# # from semantic.sql_builder import SQLBuilder
# # from semantic.date_resolver import resolve_date_filter
# # from typing import List, Dict, Any


# # class SemanticOrchestrator:
# #     """
# #     Enhanced orchestrator that handles both single and multiple query intents.
    
# #     NEW FEATURES:
# #     1. Support for multiple separate queries (e.g., "wave city and wave estate")
# #     2. Parallel execution of multiple intents
# #     3. Combined results presentation
# #     4. FIX #6: Query splitting on "and" separator
# #     """

# #     def __init__(self, model_path: str):
# #         """
# #         Args:
# #             model_path: Path to semantic model directory
# #         """
# #         self.registry = SemanticRegistry(model_path)
# #         self.validator = SemanticValidator(self.registry)
# #         self.sql_builder = SQLBuilder(self.registry)

# #     def should_split_query(self, query: str) -> bool:
# #         """
# #         FIX #6: Detect if query contains 'and' separator indicating multiple separate queries.
        
# #         Examples that should split:
# #             "wave city and wave estate" -> True
# #             "eden and amore" -> True
# #             "tower a and tower 7" -> False (this is a multi-value filter)
        
# #         Rules:
# #             - Split if "and" separates two project names
# #             - Do NOT split if "and" is part of a multi-value filter for same dimension
# #         """
# #         query_lower = query.lower()
        
# #         # Project names that indicate separate queries when joined by "and"
# #         project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
# #         # Check if query contains "PROJECT1 and PROJECT2"
# #         for i, proj1 in enumerate(project_keywords):
# #             for proj2 in project_keywords[i+1:]:
# #                 if f"{proj1} and {proj2}" in query_lower or f"{proj2} and {proj1}" in query_lower:
# #                     return True
        
# #         # Dimension filters should NOT split
# #         dimension_filters = [
# #             "floor and",  # "15th floor and 16th floor"
# #             "tower and",  # "tower a and tower 7"
# #             "type and",   # "apartment and plot"
# #             "bhk and",    # "2 bhk and 3 bhk"
# #         ]
        
# #         if any(pattern in query_lower for pattern in dimension_filters):
# #             return False
        
# #         return False

# #     def split_query_by_and(self, query: str) -> List[str]:
# #         """
# #         FIX #6: Split query into multiple queries if appropriate.
        
# #         Example:
# #             "total sales wave city and wave estate" 
# #             -> ["total sales wave city", "total sales wave estate"]
# #         """
# #         query_lower = query.lower()
        
# #         # Find the separator
# #         if " and " not in query_lower:
# #             return [query]
        
# #         # Identify project names in query
# #         project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
# #         found_projects = []
# #         for project in project_keywords:
# #             if project in query_lower:
# #                 found_projects.append(project)
        
# #         # Need exactly 2 projects
# #         if len(found_projects) != 2:
# #             return [query]
        
# #         # Find the base query (everything before first project)
# #         first_project_idx = query_lower.index(found_projects[0])
# #         base_query = query[:first_project_idx].strip()
        
# #         # Create two queries
# #         return [
# #             f"{base_query} {found_projects[0]}",
# #             f"{base_query} {found_projects[1]}"
# #         ]

# #     def execute_intent(self, intent: SemanticIntent) -> Dict[str, Any]:
# #         """
# #         Execute semantic intent and return result.
        
# #         Args:
# #             intent: Semantic intent from adapter
        
# #         Returns:
# #             Query result dictionary with sql, data, warnings
# #         """
# #         # Build SQL from intent
# #         sql, warnings = self.build_sql_from_intent(intent)

# #         # Execute query
# #         result = self.registry.execute(sql)

# #         # Post-process results (sorting and total)
# #         processed = self._post_process_results(result["columns"], result["rows"])

# #         # Format response
# #         return {
# #             "query": intent.original_query,
# #             "sql": sql,
# #             "warnings": warnings,
# #             "columns": processed["columns"],
# #             "rows": processed["rows"]
# #         }

# #     def _post_process_results(self, columns: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
# #         """
# #         Post-process query results:
# #         1. Sort by the first numeric column in descending order
# #         2. Add a 'Total' row at the end (if not already present)
# #         """
# #         if not rows:
# #             return {"columns": columns, "rows": rows}

# #         # Check for existing total row (case-insensitive)
# #         existing_total_idx = -1
# #         for i, row in enumerate(rows):
# #             if any(isinstance(val, str) and val.upper() == "TOTAL" for val in row):
# #                 existing_total_idx = i
# #                 break

# #         # Extract total row if it exists
# #         existing_total_row = None
# #         if existing_total_idx != -1:
# #             existing_total_row = rows.pop(existing_total_idx)

# #         # Identify numeric columns (using remaining rows)
# #         numeric_indices = []
# #         if rows:
# #             for i, col in enumerate(columns):
# #                 is_numeric = True
# #                 has_numeric = False
# #                 for row in rows:
# #                     if i >= len(row): continue
# #                     val = row[i]
# #                     if val is None: continue
# #                     if isinstance(val, (int, float)):
# #                         has_numeric = True
# #                         continue
# #                     if isinstance(val, str):
# #                         try:
# #                             clean_val = val.replace(',', '').strip()
# #                             if not clean_val: continue
# #                             float(clean_val)
# #                             has_numeric = True
# #                             continue
# #                         except ValueError:
# #                             is_numeric = False
# #                             break
# #                     else:
# #                         is_numeric = False
# #                         break
# #                 if is_numeric and has_numeric:
# #                     numeric_indices.append(i)

# #         # 1. Sort by the first numeric column in descending order
# #         if numeric_indices and len(rows) > 1:
# #             sort_idx = numeric_indices[0]
# #             def sort_key(row):
# #                 if sort_idx >= len(row): return -float('inf')
# #                 val = row[sort_idx]
# #                 if val is None: return -float('inf')
# #                 if isinstance(val, str):
# #                     try: return float(val.replace(',', '').strip())
# #                     except ValueError: return -float('inf')
# #                 return float(val)
# #             rows.sort(key=sort_key, reverse=True)

# #         # 2. Addition logic for total
# #         if numeric_indices:
# #             # If we had an existing total row, we use that instead of calculating a new one
# #             # This respects the SQL-provided total if present
# #             if existing_total_row:
# #                 rows.append(existing_total_row)
# #             elif len(rows) > 1:
# #                 # Calculate new total only if multiple rows and no existing total
# #                 total_row = [None] * len(columns)
# #                 label_placed = False
# #                 for i in range(len(columns)):
# #                     if i not in numeric_indices:
# #                         total_row[i] = "Total"
# #                         label_placed = True
# #                         break
# #                 if not label_placed:
# #                     total_row[0] = "Total"

# #                 for idx in numeric_indices:
# #                     col_sum = 0
# #                     for row in rows:
# #                         if idx >= len(row): continue
# #                         val = row[idx]
# #                         if val is not None:
# #                             if isinstance(val, str):
# #                                 try: col_sum += float(val.replace(',', '').strip())
# #                                 except ValueError: pass
# #                             else: col_sum += float(val)
# #                     if isinstance(col_sum, float):
# #                         col_sum = round(col_sum, 2)
# #                     total_row[idx] = col_sum
# #                 rows.append(total_row)

# #         return {"columns": columns, "rows": rows}

# #     def execute_multiple_intents(self, intents: List[SemanticIntent]) -> List[Dict[str, Any]]:
# #         """
# #         Execute multiple intents and return separate results.
        
# #         This is used when the query contains separators like "and" that indicate
# #         the user wants multiple separate results (e.g., "wave city and wave estate").
        
# #         Args:
# #             intents: List of semantic intents
        
# #         Returns:
# #             List of query results, one for each intent
# #         """
# #         results = []
        
# #         for i, intent in enumerate(intents):
# #             print(f"\n[ORCHESTRATOR] Executing intent {i+1}/{len(intents)}")
# #             try:
# #                 result = self.execute_intent(intent)
# #                 results.append(result)
# #             except Exception as e:
# #                 print(f"[ERROR] Failed to execute intent {i+1}: {str(e)}")
# #                 results.append({
# #                     "query": intent.original_query,
# #                     "sql": None,
# #                     "warnings": [f"Execution failed: {str(e)}"],
# #                     "columns": [],
# #                     "rows": []
# #                 })
        
# #         return results

# #     def build_sql_from_intent(self, intent: SemanticIntent) -> tuple:
# #         """
# #         Build SQL from semantic intent.
        
# #         Args:
# #             intent: Semantic intent object
        
# #         Returns:
# #             Tuple of (sql_string, warnings_list)
# #         """
# #         # Validate intent
# #         warnings = self.validator.validate(intent)

# #         # Build date filter
# #         if intent.custom_dates and len(intent.custom_dates) > 0:
# #             date_filter_sql = resolve_date_filter(
# #                 intent.date_range,
# #                 "Document_Date",
# #                 custom_dates=intent.custom_dates
# #             )
# #         else:
# #             date_filter_sql = resolve_date_filter(
# #                 intent.date_range,
# #                 "Document_Date"
# #             )

# #         # Build SQL
# #         sql, sql_warnings = self.sql_builder.build(
# #             intent.metric,
# #             intent.dimensions,
# #             date_filter_sql,
# #             intent.filters,
# #             intent.order_by,
# #             intent.order_direction,
# #             intent.limit,
# #             intent.time_grain
# #         )

        

        

# #         warnings.extend(sql_warnings)

# #         return sql, warnings










# from semantic.intent import SemanticIntent
# from semantic.registry import SemanticRegistry
# from semantic.validator import SemanticValidator
# from semantic.sql_builder import SQLBuilder
# from semantic.date_resolver import resolve_date_filter
# from typing import List, Dict, Any


# class SemanticOrchestrator:
#     """
#     Enhanced orchestrator that handles both single and multiple query intents.
    
#     NEW FEATURES:
#     1. Support for multiple separate queries (e.g., "wave city and wave estate")
#     2. Parallel execution of multiple intents
#     3. Combined results presentation
#     4. FIX #6: Query splitting on "and" separator
#     """

#     def __init__(self, model_path: str):
#         """
#         Args:
#             model_path: Path to semantic model directory
#         """
#         self.registry = SemanticRegistry(model_path)
#         self.validator = SemanticValidator(self.registry)
#         self.sql_builder = SQLBuilder(self.registry)

#     def should_split_query(self, query: str) -> bool:
#         """
#         FIX #6: Detect if query contains 'and' separator indicating multiple separate queries.
        
#         Examples that should split:
#             "wave city and wave estate" -> True
#             "eden and amore" -> True
#             "tower a and tower 7" -> False (this is a multi-value filter)
        
#         Rules:
#             - Split if "and" separates two project names
#             - Do NOT split if "and" is part of a multi-value filter for same dimension
#         """
#         query_lower = query.lower()
        
#         # Project names that indicate separate queries when joined by "and"
#         project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
#         # Check if query contains "PROJECT1 and PROJECT2"
#         for i, proj1 in enumerate(project_keywords):
#             for proj2 in project_keywords[i+1:]:
#                 if f"{proj1} and {proj2}" in query_lower or f"{proj2} and {proj1}" in query_lower:
#                     return True
        
#         # Dimension filters should NOT split
#         dimension_filters = [
#             "floor and",  # "15th floor and 16th floor"
#             "tower and",  # "tower a and tower 7"
#             "type and",   # "apartment and plot"
#             "bhk and",    # "2 bhk and 3 bhk"
#         ]
        
#         if any(pattern in query_lower for pattern in dimension_filters):
#             return False
        
#         return False

#     def split_query_by_and(self, query: str) -> List[str]:
#         """
#         FIX #6: Split query into multiple queries if appropriate.
        
#         Example:
#             "total sales wave city and wave estate" 
#             -> ["total sales wave city", "total sales wave estate"]
#         """
#         query_lower = query.lower()
        
#         # Find the separator
#         if " and " not in query_lower:
#             return [query]
        
#         # Identify project names in query
#         project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
#         found_projects = []
#         for project in project_keywords:
#             if project in query_lower:
#                 found_projects.append(project)
        
#         # Need exactly 2 projects
#         if len(found_projects) != 2:
#             return [query]
        
#         # Find the base query (everything before first project)
#         first_project_idx = query_lower.index(found_projects[0])
#         base_query = query[:first_project_idx].strip()
        
#         # Create two queries
#         return [
#             f"{base_query} {found_projects[0]}",
#             f"{base_query} {found_projects[1]}"
#         ]

#     def execute_intent(self, intent: SemanticIntent) -> Dict[str, Any]:
#         """
#         Execute semantic intent and return result.
        
#         Args:
#             intent: Semantic intent from adapter
        
#         Returns:
#             Query result dictionary with sql, data, warnings
#         """
#         # Build SQL from intent
#         sql, warnings = self.build_sql_from_intent(intent)

#         # Execute query
#         result = self.registry.execute(sql)

#         # Post-process results (sorting and total)
#         processed = self._post_process_results(result["columns"], result["rows"])

#         # Format response
#         return {
#             "query": intent.original_query,
#             "sql": sql,
#             "warnings": warnings,
#             "columns": processed["columns"],
#             "rows": processed["rows"]
#         }

#     def _post_process_results(self, columns: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
#         """
#         Post-process query results:
#         1. Sort by the first numeric column in descending order
#         2. Add a 'Total' row at the end (if not already present)
#         """
#         if not rows:
#             return {"columns": columns, "rows": rows}

#         # Check for existing total row (case-insensitive)
#         existing_total_idx = -1
#         for i, row in enumerate(rows):
#             if any(isinstance(val, str) and val.upper() == "TOTAL" for val in row):
#                 existing_total_idx = i
#                 break

#         # Extract total row if it exists
#         existing_total_row = None
#         if existing_total_idx != -1:
#             existing_total_row = rows.pop(existing_total_idx)

#         # Identify numeric columns (using remaining rows)
#         numeric_indices = []
#         if rows:
#             for i, col in enumerate(columns):
#                 is_numeric = True
#                 has_numeric = False
#                 for row in rows:
#                     if i >= len(row): continue
#                     val = row[i]
#                     if val is None: continue
#                     if isinstance(val, (int, float)):
#                         has_numeric = True
#                         continue
#                     if isinstance(val, str):
#                         try:
#                             clean_val = val.replace(',', '').strip()
#                             if not clean_val: continue
#                             float(clean_val)
#                             has_numeric = True
#                             continue
#                         except ValueError:
#                             is_numeric = False
#                             break
#                     else:
#                         is_numeric = False
#                         break
#                 if is_numeric and has_numeric:
#                     numeric_indices.append(i)

#         # 1. Sort by the first numeric column in descending order
#         if numeric_indices and len(rows) > 1:
#             sort_idx = numeric_indices[0]
#             def sort_key(row):
#                 if sort_idx >= len(row): return -float('inf')
#                 val = row[sort_idx]
#                 if val is None: return -float('inf')
#                 if isinstance(val, str):
#                     try: return float(val.replace(',', '').strip())
#                     except ValueError: return -float('inf')
#                 return float(val)
#             rows.sort(key=sort_key, reverse=True)

#         # 2. Addition logic for total
#         if numeric_indices:
#             # If we had an existing total row, we use that instead of calculating a new one
#             # This respects the SQL-provided total if present
#             if existing_total_row:
#                 rows.append(existing_total_row)
#             elif len(rows) > 1:
#                 # Calculate new total only if multiple rows and no existing total
#                 total_row = [None] * len(columns)
#                 label_placed = False
#                 for i in range(len(columns)):
#                     if i not in numeric_indices:
#                         total_row[i] = "Total"
#                         label_placed = True
#                         break
#                 if not label_placed:
#                     total_row[0] = "Total"

#                 for idx in numeric_indices:
#                     col_sum = 0
#                     for row in rows:
#                         if idx >= len(row): continue
#                         val = row[idx]
#                         if val is not None:
#                             if isinstance(val, str):
#                                 try: col_sum += float(val.replace(',', '').strip())
#                                 except ValueError: pass
#                             else: col_sum += float(val)
#                     if isinstance(col_sum, float):
#                         col_sum = round(col_sum, 2)
#                     total_row[idx] = col_sum
#                 rows.append(total_row)

#         return {"columns": columns, "rows": rows}

#     def execute_multiple_intents(self, intents: List[SemanticIntent]) -> List[Dict[str, Any]]:
#         """
#         Execute multiple intents and return separate results.
        
#         This is used when the query contains separators like "and" that indicate
#         the user wants multiple separate results (e.g., "wave city and wave estate").
        
#         Args:
#             intents: List of semantic intents
        
#         Returns:
#             List of query results, one for each intent
#         """
#         results = []
        
#         for i, intent in enumerate(intents):
#             print(f"\n[ORCHESTRATOR] Executing intent {i+1}/{len(intents)}")
#             try:
#                 result = self.execute_intent(intent)
#                 results.append(result)
#             except Exception as e:
#                 print(f"[ERROR] Failed to execute intent {i+1}: {str(e)}")
#                 results.append({
#                     "query": intent.original_query,
#                     "sql": None,
#                     "warnings": [f"Execution failed: {str(e)}"],
#                     "columns": [],
#                     "rows": []
#                 })
        
#         return results

#     def build_sql_from_intent(self, intent: SemanticIntent) -> tuple:
#         """
#         Build SQL from semantic intent.
        
#         Args:
#             intent: Semantic intent object
        
#         Returns:
#             Tuple of (sql_string, warnings_list)
#         """
#         # Validate intent
#         warnings = self.validator.validate(intent)

#         # Build date filter
#         if intent.custom_dates and len(intent.custom_dates) > 0:
#             date_filter_sql = resolve_date_filter(
#                 intent.date_range,
#                 "Document_Date",
#                 custom_dates=intent.custom_dates
#             )
#         else:
#             date_filter_sql = resolve_date_filter(
#                 intent.date_range,
#                 "Document_Date"
#             )

#         # Build SQL (pass multi-metric list if present)
#         sql, sql_warnings = self.sql_builder.build(
#             intent.metric,
#             intent.dimensions,
#             date_filter_sql,
#             intent.filters,
#             intent.order_by,
#             intent.order_direction,
#             intent.limit,
#             intent.time_grain,
#             metrics=intent.metrics if intent.metrics else None
#         )

        

        

#         warnings.extend(sql_warnings)

#         return sql, warnings


















# from semantic.intent import SemanticIntent
# from semantic.registry import SemanticRegistry
# from semantic.validator import SemanticValidator
# from semantic.sql_builder import SQLBuilder
# from semantic.date_resolver import resolve_date_filter
# from typing import List, Dict, Any


# class SemanticOrchestrator:
#     """
#     Enhanced orchestrator that handles both single and multiple query intents.
    
#     NEW FEATURES:
#     1. Support for multiple separate queries (e.g., "wave city and wave estate")
#     2. Parallel execution of multiple intents
#     3. Combined results presentation
#     4. FIX #6: Query splitting on "and" separator
#     """

#     def __init__(self, model_path: str):
#         """
#         Args:
#             model_path: Path to semantic model directory
#         """
#         self.registry = SemanticRegistry(model_path)
#         self.validator = SemanticValidator(self.registry)
#         self.sql_builder = SQLBuilder(self.registry)

#     def should_split_query(self, query: str) -> bool:
#         """
#         FIX #6: Detect if query contains 'and' separator indicating multiple separate queries.
        
#         Examples that should split:
#             "wave city and wave estate" -> True
#             "eden and amore" -> True
#             "tower a and tower 7" -> False (this is a multi-value filter)
        
#         Rules:
#             - Split if "and" separates two project names
#             - Do NOT split if "and" is part of a multi-value filter for same dimension
#         """
#         query_lower = query.lower()
        
#         # Project names that indicate separate queries when joined by "and"
#         project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
#         # Check if query contains "PROJECT1 and PROJECT2"
#         for i, proj1 in enumerate(project_keywords):
#             for proj2 in project_keywords[i+1:]:
#                 if f"{proj1} and {proj2}" in query_lower or f"{proj2} and {proj1}" in query_lower:
#                     return True
        
#         # Dimension filters should NOT split
#         dimension_filters = [
#             "floor and",  # "15th floor and 16th floor"
#             "tower and",  # "tower a and tower 7"
#             "type and",   # "apartment and plot"
#             "bhk and",    # "2 bhk and 3 bhk"
#         ]
        
#         if any(pattern in query_lower for pattern in dimension_filters):
#             return False
        
#         return False

#     def split_query_by_and(self, query: str) -> List[str]:
#         """
#         FIX #6: Split query into multiple queries if appropriate.
        
#         Example:
#             "total sales wave city and wave estate" 
#             -> ["total sales wave city", "total sales wave estate"]
#         """
#         query_lower = query.lower()
        
#         # Find the separator
#         if " and " not in query_lower:
#             return [query]
        
#         # Identify project names in query
#         project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
#         found_projects = []
#         for project in project_keywords:
#             if project in query_lower:
#                 found_projects.append(project)
        
#         # Need exactly 2 projects
#         if len(found_projects) != 2:
#             return [query]
        
#         # Find the base query (everything before first project)
#         first_project_idx = query_lower.index(found_projects[0])
#         base_query = query[:first_project_idx].strip()
        
#         # Create two queries
#         return [
#             f"{base_query} {found_projects[0]}",
#             f"{base_query} {found_projects[1]}"
#         ]

#     def execute_intent(self, intent: SemanticIntent) -> Dict[str, Any]:
#         """
#         Execute semantic intent and return result.
        
#         Args:
#             intent: Semantic intent from adapter
        
#         Returns:
#             Query result dictionary with sql, data, warnings
#         """
#         # Build SQL from intent
#         sql, warnings = self.build_sql_from_intent(intent)

#         # Execute query
#         result = self.registry.execute(sql)

#         # Post-process results (sorting and total)
#         processed = self._post_process_results(result["columns"], result["rows"])

#         # Format response
#         return {
#             "query": intent.original_query,
#             "sql": sql,
#             "warnings": warnings,
#             "columns": processed["columns"],
#             "rows": processed["rows"]
#         }

#     def _post_process_results(self, columns: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
#         """
#         Post-process query results:
#         1. Sort by the first numeric column in descending order
#         2. Add a 'Total' row at the end (if not already present)
#         """
#         if not rows:
#             return {"columns": columns, "rows": rows}

#         # Check for existing total row (case-insensitive)
#         existing_total_idx = -1
#         for i, row in enumerate(rows):
#             if any(isinstance(val, str) and val.upper() == "TOTAL" for val in row):
#                 existing_total_idx = i
#                 break

#         # Extract total row if it exists
#         existing_total_row = None
#         if existing_total_idx != -1:
#             existing_total_row = rows.pop(existing_total_idx)

#         # Identify numeric columns (using remaining rows)
#         numeric_indices = []
#         if rows:
#             for i, col in enumerate(columns):
#                 is_numeric = True
#                 has_numeric = False
#                 for row in rows:
#                     if i >= len(row): continue
#                     val = row[i]
#                     if val is None: continue
#                     if isinstance(val, (int, float)):
#                         has_numeric = True
#                         continue
#                     if isinstance(val, str):
#                         try:
#                             clean_val = val.replace(',', '').strip()
#                             if not clean_val: continue
#                             float(clean_val)
#                             has_numeric = True
#                             continue
#                         except ValueError:
#                             is_numeric = False
#                             break
#                     else:
#                         is_numeric = False
#                         break
#                 if is_numeric and has_numeric:
#                     numeric_indices.append(i)

#         # 1. Sort by the first numeric column in descending order
#         if numeric_indices and len(rows) > 1:
#             sort_idx = numeric_indices[0]
#             def sort_key(row):
#                 if sort_idx >= len(row): return -float('inf')
#                 val = row[sort_idx]
#                 if val is None: return -float('inf')
#                 if isinstance(val, str):
#                     try: return float(val.replace(',', '').strip())
#                     except ValueError: return -float('inf')
#                 return float(val)
#             rows.sort(key=sort_key, reverse=True)

#         # 2. Addition logic for total
#         if numeric_indices:
#             # If we had an existing total row, we use that instead of calculating a new one
#             # This respects the SQL-provided total if present
#             if existing_total_row:
#                 rows.append(existing_total_row)
#             elif len(rows) > 1:
#                 # Calculate new total only if multiple rows and no existing total
#                 total_row = [None] * len(columns)
#                 label_placed = False
#                 for i in range(len(columns)):
#                     if i not in numeric_indices:
#                         total_row[i] = "Total"
#                         label_placed = True
#                         break
#                 if not label_placed:
#                     total_row[0] = "Total"

#                 for idx in numeric_indices:
#                     col_sum = 0
#                     for row in rows:
#                         if idx >= len(row): continue
#                         val = row[idx]
#                         if val is not None:
#                             if isinstance(val, str):
#                                 try: col_sum += float(val.replace(',', '').strip())
#                                 except ValueError: pass
#                             else: col_sum += float(val)
#                     if isinstance(col_sum, float):
#                         col_sum = round(col_sum, 2)
#                     total_row[idx] = col_sum
#                 rows.append(total_row)

#         return {"columns": columns, "rows": rows}

#     def execute_multiple_intents(self, intents: List[SemanticIntent]) -> List[Dict[str, Any]]:
#         """
#         Execute multiple intents and return separate results.
        
#         This is used when the query contains separators like "and" that indicate
#         the user wants multiple separate results (e.g., "wave city and wave estate").
        
#         Args:
#             intents: List of semantic intents
        
#         Returns:
#             List of query results, one for each intent
#         """
#         results = []
        
#         for i, intent in enumerate(intents):
#             print(f"\n[ORCHESTRATOR] Executing intent {i+1}/{len(intents)}")
#             try:
#                 result = self.execute_intent(intent)
#                 results.append(result)
#             except Exception as e:
#                 print(f"[ERROR] Failed to execute intent {i+1}: {str(e)}")
#                 results.append({
#                     "query": intent.original_query,
#                     "sql": None,
#                     "warnings": [f"Execution failed: {str(e)}"],
#                     "columns": [],
#                     "rows": []
#                 })
        
#         return results

#     def build_sql_from_intent(self, intent: SemanticIntent) -> tuple:
#         """
#         Build SQL from semantic intent.
        
#         Args:
#             intent: Semantic intent object
        
#         Returns:
#             Tuple of (sql_string, warnings_list)
#         """
#         # Validate intent
#         warnings = self.validator.validate(intent)

#         # Build date filter
#         if intent.custom_dates and len(intent.custom_dates) > 0:
#             date_filter_sql = resolve_date_filter(
#                 intent.date_range,
#                 "Document_Date",
#                 custom_dates=intent.custom_dates
#             )
#         else:
#             date_filter_sql = resolve_date_filter(
#                 intent.date_range,
#                 "Document_Date"
#             )

#         # Build SQL
#         sql, sql_warnings = self.sql_builder.build(
#             intent.metric,
#             intent.dimensions,
#             date_filter_sql,
#             intent.filters,
#             intent.order_by,
#             intent.order_direction,
#             intent.limit,
#             intent.time_grain
#         )

        

        

#         warnings.extend(sql_warnings)

#         return sql, warnings










from semantic.intent import SemanticIntent
from semantic.registry import SemanticRegistry
from semantic.validator import SemanticValidator
from semantic.sql_builder import SQLBuilder
from semantic.date_resolver import resolve_date_filter
from typing import List, Dict, Any


class SemanticOrchestrator:
    """
    Enhanced orchestrator that handles both single and multiple query intents.
    
    NEW FEATURES:
    1. Support for multiple separate queries (e.g., "wave city and wave estate")
    2. Parallel execution of multiple intents
    3. Combined results presentation
    4. FIX #6: Query splitting on "and" separator
    """

    def __init__(self, model_path: str):
        """
        Args:
            model_path: Path to semantic model directory
        """
        self.registry = SemanticRegistry(model_path)
        self.validator = SemanticValidator(self.registry)
        self.sql_builder = SQLBuilder(self.registry)

    def should_split_query(self, query: str) -> bool:
        """
        FIX #6: Detect if query contains 'and' separator indicating multiple separate queries.
        
        Examples that should split:
            "wave city and wave estate" -> True
            "eden and amore" -> True
            "tower a and tower 7" -> False (this is a multi-value filter)
        
        Rules:
            - Split if "and" separates two project names
            - Do NOT split if "and" is part of a multi-value filter for same dimension
        """
        query_lower = query.lower()
        
        # Project names that indicate separate queries when joined by "and"
        project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
        # Check if query contains "PROJECT1 and PROJECT2"
        for i, proj1 in enumerate(project_keywords):
            for proj2 in project_keywords[i+1:]:
                if f"{proj1} and {proj2}" in query_lower or f"{proj2} and {proj1}" in query_lower:
                    return True
        
        # Dimension filters should NOT split
        dimension_filters = [
            "floor and",  # "15th floor and 16th floor"
            "tower and",  # "tower a and tower 7"
            "type and",   # "apartment and plot"
            "bhk and",    # "2 bhk and 3 bhk"
        ]
        
        if any(pattern in query_lower for pattern in dimension_filters):
            return False
        
        return False

    def split_query_by_and(self, query: str) -> List[str]:
        """
        FIX #6: Split query into multiple queries if appropriate.
        
        Example:
            "total sales wave city and wave estate" 
            -> ["total sales wave city", "total sales wave estate"]
        """
        query_lower = query.lower()
        
        # Find the separator
        if " and " not in query_lower:
            return [query]
        
        # Identify project names in query
        project_keywords = ["wave city", "wave estate", "eden", "amore", "livork"]
        
        found_projects = []
        for project in project_keywords:
            if project in query_lower:
                found_projects.append(project)
        
        # Need exactly 2 projects
        if len(found_projects) != 2:
            return [query]
        
        # Find the base query (everything before first project)
        first_project_idx = query_lower.index(found_projects[0])
        base_query = query[:first_project_idx].strip()
        
        # Create two queries
        return [
            f"{base_query} {found_projects[0]}",
            f"{base_query} {found_projects[1]}"
        ]

    def execute_intent(self, intent: SemanticIntent) -> Dict[str, Any]:
        """
        Execute semantic intent and return result.
        
        Args:
            intent: Semantic intent from adapter
        
        Returns:
            Query result dictionary with sql, data, warnings
        """
        # Build SQL from intent
        sql, warnings = self.build_sql_from_intent(intent)

        # Execute query
        result = self.registry.execute(sql)

        # Post-process results (sorting and total)
        processed = self._post_process_results(result["columns"], result["rows"])

        # Format response
        return {
            "query": intent.original_query,
            "sql": sql,
            "warnings": warnings,
            "columns": processed["columns"],
            "rows": processed["rows"]
        }

    def _post_process_results(self, columns: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
        """
        Post-process query results:
        1. Sort by the first numeric column in descending order
        2. Add a 'Total' row at the end (if not already present)
        """
        if not rows:
            return {"columns": columns, "rows": rows}

        # Check for existing total row (case-insensitive)
        existing_total_idx = -1
        for i, row in enumerate(rows):
            if any(isinstance(val, str) and val.upper() == "TOTAL" for val in row):
                existing_total_idx = i
                break

        # Extract total row if it exists
        existing_total_row = None
        if existing_total_idx != -1:
            existing_total_row = rows.pop(existing_total_idx)

        # Identify numeric columns (using remaining rows)
        numeric_indices = []
        if rows:
            for i, col in enumerate(columns):
                is_numeric = True
                has_numeric = False
                for row in rows:
                    if i >= len(row): continue
                    val = row[i]
                    if val is None: continue
                    if isinstance(val, (int, float)):
                        has_numeric = True
                        continue
                    if isinstance(val, str):
                        try:
                            clean_val = val.replace(',', '').strip()
                            if not clean_val: continue
                            float(clean_val)
                            has_numeric = True
                            continue
                        except ValueError:
                            is_numeric = False
                            break
                    else:
                        is_numeric = False
                        break
                if is_numeric and has_numeric:
                    numeric_indices.append(i)

        # 1. Sort by the first numeric column in descending order
        if numeric_indices and len(rows) > 1:
            sort_idx = numeric_indices[0]
            def sort_key(row):
                if sort_idx >= len(row): return -float('inf')
                val = row[sort_idx]
                if val is None: return -float('inf')
                if isinstance(val, str):
                    try: return float(val.replace(',', '').strip())
                    except ValueError: return -float('inf')
                return float(val)
            rows.sort(key=sort_key, reverse=True)

        # 2. Addition logic for total
        if numeric_indices:
            # If we had an existing total row, we use that instead of calculating a new one
            # This respects the SQL-provided total if present
            if existing_total_row:
                rows.append(existing_total_row)
            elif len(rows) > 1:
                # Calculate new total only if multiple rows and no existing total
                total_row = [None] * len(columns)
                label_placed = False
                for i in range(len(columns)):
                    if i not in numeric_indices:
                        total_row[i] = "Total"
                        label_placed = True
                        break
                if not label_placed:
                    total_row[0] = "Total"

                for idx in numeric_indices:
                    col_sum = 0
                    for row in rows:
                        if idx >= len(row): continue
                        val = row[idx]
                        if val is not None:
                            if isinstance(val, str):
                                try: col_sum += float(val.replace(',', '').strip())
                                except ValueError: pass
                            else: col_sum += float(val)
                    if isinstance(col_sum, float):
                        col_sum = round(col_sum, 2)
                    total_row[idx] = col_sum
                rows.append(total_row)

        return {"columns": columns, "rows": rows}

    def execute_multiple_intents(self, intents: List[SemanticIntent]) -> List[Dict[str, Any]]:
        """
        Execute multiple intents and return separate results.
        
        This is used when the query contains separators like "and" that indicate
        the user wants multiple separate results (e.g., "wave city and wave estate").
        
        Args:
            intents: List of semantic intents
        
        Returns:
            List of query results, one for each intent
        """
        results = []
        
        for i, intent in enumerate(intents):
            print(f"\n[ORCHESTRATOR] Executing intent {i+1}/{len(intents)}")
            try:
                result = self.execute_intent(intent)
                results.append(result)
            except Exception as e:
                print(f"[ERROR] Failed to execute intent {i+1}: {str(e)}")
                results.append({
                    "query": intent.original_query,
                    "sql": None,
                    "warnings": [f"Execution failed: {str(e)}"],
                    "columns": [],
                    "rows": []
                })
        
        return results

    def build_sql_from_intent(self, intent: SemanticIntent) -> tuple:
        """
        Build SQL from semantic intent.
        
        Args:
            intent: Semantic intent object
        
        Returns:
            Tuple of (sql_string, warnings_list)
        """
        # Validate intent
        warnings = self.validator.validate(intent)

        # Build date filter
        if intent.custom_dates and len(intent.custom_dates) > 0:
            date_filter_sql = resolve_date_filter(
                intent.date_range,
                "Document_Date",
                custom_dates=intent.custom_dates
            )
        else:
            date_filter_sql = resolve_date_filter(
                intent.date_range,
                "Document_Date"
            )

        # Build SQL (pass multi-metric list if present)
        sql, sql_warnings = self.sql_builder.build(
            intent.metric,
            intent.dimensions,
            date_filter_sql,
            intent.filters,
            intent.order_by,
            intent.order_direction,
            intent.limit,
            intent.time_grain,
            metrics=intent.metrics if intent.metrics else None,
            having_filter=getattr(intent, 'having_filter', None)
        )

        

        

        warnings.extend(sql_warnings)

        return sql, warnings