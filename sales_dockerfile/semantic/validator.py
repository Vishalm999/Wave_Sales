# # # app/semantic/validator.py

# # from typing import List
# # from semantic.registry import SemanticRegistry
# # # from app.semantic.registry import SemanticRegistry


# # class SemanticValidationError(Exception):
# #     """Custom exception for semantic validation errors."""
# #     pass


# # class SemanticValidator:
# #     """
# #     Validates semantic intent against the registry.
    
# #     Checks:
# #     - Metric exists and is valid
# #     - Dimensions exist in registry
# #     - Date range is recognized
# #     """

# #     def __init__(self, registry: SemanticRegistry):
# #         """
# #         Args:
# #             registry: SemanticRegistry instance
# #         """
# #         self.registry = registry

# #     def validate(self, intent) -> List[str]:
# #         """
# #         Validate semantic intent.
        
# #         Args:
# #             intent: SemanticIntent object with all fields
        
# #         Returns:
# #             List of warning messages (empty if all valid)
        
# #         Raises:
# #             SemanticValidationError: If validation fails
# #         """
# #         warnings = []

# #         print(f"DEBUG: Validating intent with metric='{intent.metric}', dimensions={intent.dimensions}, date_range='{intent.date_range}'")

# #         # Extract fields from intent
# #         metric = intent.metric if hasattr(intent, 'metric') else None
# #         dimensions = intent.dimensions if hasattr(intent, 'dimensions') else []
# #         date_range = intent.date_range if hasattr(intent, 'date_range') else None

# #         # Normalize common synonyms for dimensions (map LLM variants to canonical names)
# #         synonyms = {
# #             'sales_channel_desc': 'dist_channel_desc',
# #             'sales_channel': 'dist_channel_desc',
# #             'sales_type': 'dist_channel_desc',
# #         }

# #         if dimensions:
# #             normalized = []
# #             seen = set()
# #             for d in dimensions:
# #                 if d in self.registry.dimensions:
# #                     cand = d
# #                 elif d in synonyms and synonyms[d] in self.registry.dimensions:
# #                     cand = synonyms[d]
# #                     print(f"DEBUG: Normalizing dimension '{d}' -> '{cand}'")
# #                 else:
# #                     cand = d

# #                 if cand not in seen:
# #                     normalized.append(cand)
# #                     seen.add(cand)

# #             # Update intent dimensions in-place if possible so downstream uses canonical names
# #             try:
# #                 intent.dimensions = normalized
# #             except Exception:
# #                 pass

# #             dimensions = normalized

# #         print(f"DEBUG: Validating intent with metric='{metric}', dimensions={dimensions}, date_range='{date_range}'")

# #         # Validate metric
# #         if metric:
# #             metric_warnings = self._validate_metric(metric)
# #             warnings.extend(metric_warnings)

# #         # Validate dimensions
# #         if dimensions:
# #             dim_warnings = self._validate_dimensions_exist(dimensions)
# #             warnings.extend(dim_warnings)

# #         # Validate date range (soft validation, just warnings)
# #         if date_range:
# #             date_warnings = self._validate_date_range(date_range)
# #             warnings.extend(date_warnings)

# #         return warnings

# #     def _validate_metric(self, metric: str) -> List[str]:
# #         """
# #         Validate that metric exists in registry.
# #         """
# #         warnings = []
        
# #         if not metric:
# #             raise SemanticValidationError("Metric cannot be empty")
        
# #         # CORRECT - checks metrics registry
# #         if metric not in self.registry.metrics:
# #             raise SemanticValidationError(f"Unknown metric: {metric}")
        
# #         metric_obj = self.registry.metrics[metric]
        
# #         # Check if behavior is non-additive or ratio
# #         if hasattr(metric_obj, 'behavior'):
# #             if metric_obj.behavior in ['non-additive', 'ratio']:
# #                 warnings.append(f"Metric '{metric}' is {metric_obj.behavior} - results may need special handling")
        
# #         return warnings


# #     def _validate_dimensions_exist(self, dimensions: List[str]) -> List[str]:
# #         """
# #         Validate that all dimensions exist in registry.
        
# #         Args:
# #             dimensions: List of dimension names
        
# #         Returns:
# #             List of warnings
        
# #         Raises:
# #             SemanticValidationError: If dimension is invalid
# #         """
# #         warnings = []

# #         for dim in dimensions:
# #             print(f"DEBUG: Validating dimension: {dim}")
# #             print(f"DEBUG: Available dimensions: {list(self.registry.dimensions.keys())}")
# #             if dim not in self.registry.dimensions:
# #                 raise SemanticValidationError(f"Unknown dimension: {dim}")

# #         return warnings

# #     def _validate_date_range(self, date_range: str) -> List[str]:
# #         """
# #         Validate date range (soft validation).
        
# #         Args:
# #             date_range: Date range string
        
# #         Returns:
# #             List of warnings
# #         """
# #         warnings = []

# #         # Known date ranges
# #         known_ranges = [
# #             "current_financial_year", "last_financial_year", "fytd",
# #             "today", "yesterday",
# #             "this_week", "last_week",
# #             "this_month", "last_month",
# #             "this_quarter", "last_quarter",
# #             "this_year", "last_year",
# #             "mtd", "qtd", "ytd",
# #             "rolling_7_days", "rolling_14_days", "rolling_30_days",
# #             "rolling_60_days", "rolling_90_days",
# #             "rolling_6_months", "rolling_12_months",
# #             "custom_range", "before_date", "after_date"
# #         ]

# #         if date_range not in known_ranges:
# #             warnings.append(f"Unknown date range: '{date_range}' - will use fallback")

# #         return warnings












# # app/semantic/validator.py

# from typing import List
# from semantic.registry import SemanticRegistry
# # from app.semantic.registry import SemanticRegistry


# class SemanticValidationError(Exception):
#     """Custom exception for semantic validation errors."""
#     pass


# class SemanticValidator:
#     """
#     Validates semantic intent against the registry.
    
#     Checks:
#     - Metric exists and is valid
#     - Dimensions exist in registry
#     - Date range is recognized
#     """

#     def __init__(self, registry: SemanticRegistry):
#         """
#         Args:
#             registry: SemanticRegistry instance
#         """
#         self.registry = registry

#     def validate(self, intent) -> List[str]:
#         """
#         Validate semantic intent.
        
#         Args:
#             intent: SemanticIntent object with all fields
        
#         Returns:
#             List of warning messages (empty if all valid)
        
#         Raises:
#             SemanticValidationError: If validation fails
#         """
#         warnings = []

#         print(f"DEBUG: Validating intent with metric='{intent.metric}', dimensions={intent.dimensions}, date_range='{intent.date_range}'")

#         # Extract fields from intent
#         metric = intent.metric if hasattr(intent, 'metric') else None
#         dimensions = intent.dimensions if hasattr(intent, 'dimensions') else []
#         date_range = intent.date_range if hasattr(intent, 'date_range') else None

#         # Normalize common synonyms for dimensions (map LLM variants to canonical names)
#         synonyms = {
#             'sales_channel_desc': 'dist_channel_desc',
#             'sales_channel': 'dist_channel_desc',
#             'sales_type': 'dist_channel_desc',
#         }

#         if dimensions:
#             normalized = []
#             seen = set()
#             for d in dimensions:
#                 if d in self.registry.dimensions:
#                     cand = d
#                 elif d in synonyms and synonyms[d] in self.registry.dimensions:
#                     cand = synonyms[d]
#                     print(f"DEBUG: Normalizing dimension '{d}' -> '{cand}'")
#                 else:
#                     cand = d

#                 if cand not in seen:
#                     normalized.append(cand)
#                     seen.add(cand)

#             # Update intent dimensions in-place if possible so downstream uses canonical names
#             try:
#                 intent.dimensions = normalized
#             except Exception:
#                 pass

#             dimensions = normalized

#         print(f"DEBUG: Validating intent with metric='{metric}', dimensions={dimensions}, date_range='{date_range}'")

#         # Validate metric
#         if metric:
#             metric_warnings = self._validate_metric(metric)
#             warnings.extend(metric_warnings)

#         # Validate dimensions
#         if dimensions:
#             dim_warnings = self._validate_dimensions_exist(dimensions)
#             warnings.extend(dim_warnings)

#         # Validate date range (soft validation, just warnings)
#         if date_range:
#             date_warnings = self._validate_date_range(date_range)
#             warnings.extend(date_warnings)

#         return warnings

#     def _validate_metric(self, metric: str) -> List[str]:
#         """
#         Validate that metric exists in registry.
#         """
#         warnings = []
        
#         if not metric:
#             raise SemanticValidationError("Metric cannot be empty")
        
#         # CORRECT - checks metrics registry
#         if metric not in self.registry.metrics:
#             raise SemanticValidationError(f"Unknown metric: {metric}")
        
#         metric_obj = self.registry.metrics[metric]
        
#         # Check if behavior is non-additive or ratio
#         if hasattr(metric_obj, 'behavior'):
#             if metric_obj.behavior in ['non-additive', 'ratio']:
#                 warnings.append(f"Metric '{metric}' is {metric_obj.behavior} - results may need special handling")
        
#         return warnings


#     def _validate_dimensions_exist(self, dimensions: List[str]) -> List[str]:
#         """
#         Validate that all dimensions exist in registry.
        
#         Args:
#             dimensions: List of dimension names
        
#         Returns:
#             List of warnings
        
#         Raises:
#             SemanticValidationError: If dimension is invalid
#         """
#         warnings = []

#         for dim in dimensions:
#             print(f"DEBUG: Validating dimension: {dim}")
#             print(f"DEBUG: Available dimensions: {list(self.registry.dimensions.keys())}")
#             if dim not in self.registry.dimensions:
#                 raise SemanticValidationError(f"Unknown dimension: {dim}")

#         return warnings

#     def _validate_date_range(self, date_range: str) -> List[str]:
#         """
#         Validate date range (soft validation).
        
#         Args:
#             date_range: Date range string
        
#         Returns:
#             List of warnings
#         """
#         warnings = []

#         # Known date ranges
#         known_ranges = [
#             "current_financial_year", "last_financial_year", "fytd",
#             "today", "yesterday",
#             "this_week", "last_week",
#             "this_month", "last_month",
#             "this_quarter", "last_quarter",
#             "this_year", "last_year",
#             "mtd", "qtd", "ytd",
#             "rolling_7_days", "rolling_14_days", "rolling_30_days",
#             "rolling_60_days", "rolling_90_days",
#             "rolling_6_months", "rolling_12_months",
#             "custom_range", "custom_date", "before_date", "after_date"
#         ]

#         if date_range not in known_ranges:
#             warnings.append(f"Unknown date range: '{date_range}' - will use fallback")

#         return warnings






# app/semantic/validator.py

from typing import List
from semantic.registry import SemanticRegistry
# from app.semantic.registry import SemanticRegistry


class SemanticValidationError(Exception):
    """Custom exception for semantic validation errors."""
    pass


class SemanticValidator:
    """
    Validates semantic intent against the registry.
    
    Checks:
    - Metric exists and is valid
    - Dimensions exist in registry
    - Date range is recognized
    """

    def __init__(self, registry: SemanticRegistry):
        """
        Args:
            registry: SemanticRegistry instance
        """
        self.registry = registry

    def validate(self, intent) -> List[str]:
        """
        Validate semantic intent.
        
        Args:
            intent: SemanticIntent object with all fields
        
        Returns:
            List of warning messages (empty if all valid)
        
        Raises:
            SemanticValidationError: If validation fails
        """
        warnings = []

        print(f"DEBUG: Validating intent with metric='{intent.metric}', dimensions={intent.dimensions}, date_range='{intent.date_range}'")

        # Extract fields from intent
        metric = intent.metric if hasattr(intent, 'metric') else None
        dimensions = intent.dimensions if hasattr(intent, 'dimensions') else []
        date_range = intent.date_range if hasattr(intent, 'date_range') else None

        # Normalize common synonyms for dimensions (map LLM variants to canonical names)
        synonyms = {
            'sales_channel_desc': 'dist_channel_desc',
            'sales_channel': 'dist_channel_desc',
            'sales_type': 'dist_channel_desc',
        }

        if dimensions:
            normalized = []
            seen = set()
            for d in dimensions:
                if d in self.registry.dimensions:
                    cand = d
                elif d in synonyms and synonyms[d] in self.registry.dimensions:
                    cand = synonyms[d]
                    print(f"DEBUG: Normalizing dimension '{d}' -> '{cand}'")
                else:
                    cand = d

                if cand not in seen:
                    normalized.append(cand)
                    seen.add(cand)

            # Update intent dimensions in-place if possible so downstream uses canonical names
            try:
                intent.dimensions = normalized
            except Exception:
                pass

            dimensions = normalized

        print(f"DEBUG: Validating intent with metric='{metric}', dimensions={dimensions}, date_range='{date_range}'")

        # Validate metric
        if metric:
            metric_warnings = self._validate_metric(metric)
            warnings.extend(metric_warnings)

        # Validate dimensions
        if dimensions:
            dim_warnings = self._validate_dimensions_exist(dimensions)
            warnings.extend(dim_warnings)

        # Validate date range (soft validation, just warnings)
        if date_range:
            date_warnings = self._validate_date_range(date_range)
            warnings.extend(date_warnings)

        return warnings

    def _validate_metric(self, metric: str) -> List[str]:
        """
        Validate that metric exists in registry.
        """
        warnings = []
        
        if not metric:
            raise SemanticValidationError("Metric cannot be empty")
        
        # CORRECT - checks metrics registry
        if metric not in self.registry.metrics:
            raise SemanticValidationError(f"Unknown metric: {metric}")
        
        metric_obj = self.registry.metrics[metric]
        
        # Check if behavior is non-additive or ratio
        if hasattr(metric_obj, 'behavior'):
            if metric_obj.behavior in ['non-additive', 'ratio']:
                warnings.append(f"Metric '{metric}' is {metric_obj.behavior} - results may need special handling")
        
        return warnings


    def _validate_dimensions_exist(self, dimensions: List[str]) -> List[str]:
        """
        Validate that all dimensions exist in registry.
        
        Args:
            dimensions: List of dimension names
        
        Returns:
            List of warnings
        
        Raises:
            SemanticValidationError: If dimension is invalid
        """
        warnings = []

        for dim in dimensions:
            print(f"DEBUG: Validating dimension: {dim}")
            print(f"DEBUG: Available dimensions: {list(self.registry.dimensions.keys())}")
            if dim not in self.registry.dimensions:
                raise SemanticValidationError(f"Unknown dimension: {dim}")

        return warnings

    def _validate_date_range(self, date_range: str) -> List[str]:
        """
        Validate date range (soft validation).
        
        Args:
            date_range: Date range string
        
        Returns:
            List of warnings
        """
        warnings = []

        # Known date ranges
        known_ranges = [
            "current_financial_year", "last_financial_year", "fytd",
            "today", "yesterday",
            "this_week", "last_week",
            "this_month", "last_month",
            "this_quarter", "last_quarter",
            "this_year", "last_year",
            "mtd", "qtd", "ytd",
            "rolling_7_days", "rolling_14_days", "rolling_30_days",
            "rolling_60_days", "rolling_90_days",
            "rolling_6_months", "rolling_12_months",
            "custom_range", "custom_date", "before_date", "after_date",
            "last_3_financial_years",
            "last_3_financial_years_yoy",
        ]

        # Also accept dynamic patterns like last_N_financial_years, last_N_quarters, last_N_months, fy_YYYY, qN
        import re as _re
        dynamic_patterns = [
            r'^last_\d+_financial_years$',
            r'^last_\d+_quarters$',
            r'^last_\d+_months$',
            r'^fy_\d{4}$',
            r'^q[1-4]$',
        ]
        if date_range not in known_ranges:
            if not any(_re.match(p, date_range) for p in dynamic_patterns):
                warnings.append(f"Unknown date range: '{date_range}' - will use fallback")
        return warnings