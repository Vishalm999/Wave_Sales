# # from dataclasses import dataclass, field
# # from typing import List, Dict, Optional, Any

# # @dataclass
# # class SemanticIntent:
# #     """
# #     Semantic representation of user query intent.
    
# #     Attributes:
# #         metric: The aggregation metric (e.g., "total_sales", "sales_value")
# #         dimensions: List of dimension names for GROUP BY
# #         date_range: Predefined date range identifier
# #         custom_dates: List of custom date objects for specific date queries
# #         is_trend: Whether this is a time-series trend query
# #         time_grain: Time granularity (day, month, quarter, year)
# #         compare_to: Comparison type (mom, qoq, yoy, wow)
# #         order_by: Column to order results by
# #         order_direction: Sort direction (asc/desc)
# #         limit: Number of results to return
# #         filters: Additional filter values extracted from query
# #         original_query: Original user query text
    
# #     Example:
# #         Query: "Tower wise sales for april 2024"
# #         Intent:
# #             metric="total_sales"
# #             dimensions=["tower"]
# #             custom_dates=[{"month_num": 4, "year": 2024}]
# #             date_range="custom_range"
# #     """
    
# #     metric: str = "total_sales"
# #     dimensions: List[str] = field(default_factory=list)
# #     date_range: str = "current_financial_year"
# #     custom_dates: List[Dict[str, Any]] = field(default_factory=list)  # NEW!
# #     is_trend: bool = False
# #     time_grain: Optional[str] = None
# #     compare_to: Optional[str] = None
# #     order_by: Optional[str] = None
# #     order_direction: Optional[str] = None
# #     limit: Optional[int] = None
# #     filters: Optional[Dict] = None
# #     original_query: Optional[str] = None






# from dataclasses import dataclass, field
# from typing import List, Dict, Optional, Any

# @dataclass
# class SemanticIntent:
#     """
#     Semantic representation of user query intent.
    
#     Attributes:
#         metric: The aggregation metric (e.g., "total_sales", "sales_value")
#         dimensions: List of dimension names for GROUP BY
#         date_range: Predefined date range identifier
#         custom_dates: List of custom date objects for specific date queries
#         is_trend: Whether this is a time-series trend query
#         time_grain: Time granularity (day, month, quarter, year)
#         compare_to: Comparison type (mom, qoq, yoy, wow)
#         order_by: Column to order results by
#         order_direction: Sort direction (asc/desc)
#         limit: Number of results to return
#         filters: Additional filter values extracted from query
#         original_query: Original user query text
    
#     Example:
#         Query: "Tower wise sales for april 2024"
#         Intent:
#             metric="total_sales"
#             dimensions=["tower"]
#             custom_dates=[{"month_num": 4, "year": 2024}]
#             date_range="custom_range"
#     """
    
#     metric: str = "total_sales"
#     metrics: List[str] = field(default_factory=list)  # Multi-metric support
#     dimensions: List[str] = field(default_factory=list)
#     date_range: str = "current_financial_year"
#     custom_dates: List[Dict[str, Any]] = field(default_factory=list)  # NEW!
#     is_trend: bool = False
#     time_grain: Optional[str] = None
#     compare_to: Optional[str] = None
#     order_by: Optional[str] = None
#     order_direction: Optional[str] = None
#     limit: Optional[int] = None
#     filters: Optional[Dict] = None
#     original_query: Optional[str] = None






# from dataclasses import dataclass, field
# from typing import List, Dict, Optional, Any

# @dataclass
# class SemanticIntent:
#     """
#     Semantic representation of user query intent.
    
#     Attributes:
#         metric: The aggregation metric (e.g., "total_sales", "sales_value")
#         dimensions: List of dimension names for GROUP BY
#         date_range: Predefined date range identifier
#         custom_dates: List of custom date objects for specific date queries
#         is_trend: Whether this is a time-series trend query
#         time_grain: Time granularity (day, month, quarter, year)
#         compare_to: Comparison type (mom, qoq, yoy, wow)
#         order_by: Column to order results by
#         order_direction: Sort direction (asc/desc)
#         limit: Number of results to return
#         filters: Additional filter values extracted from query
#         original_query: Original user query text
    
#     Example:
#         Query: "Tower wise sales for april 2024"
#         Intent:
#             metric="total_sales"
#             dimensions=["tower"]
#             custom_dates=[{"month_num": 4, "year": 2024}]
#             date_range="custom_range"
#     """
    
#     metric: str = "total_sales"
#     dimensions: List[str] = field(default_factory=list)
#     date_range: str = "current_financial_year"
#     custom_dates: List[Dict[str, Any]] = field(default_factory=list)  # NEW!
#     is_trend: bool = False
#     time_grain: Optional[str] = None
#     compare_to: Optional[str] = None
#     order_by: Optional[str] = None
#     order_direction: Optional[str] = None
#     limit: Optional[int] = None
#     filters: Optional[Dict] = None
#     original_query: Optional[str] = None






from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

@dataclass
class SemanticIntent:
    """
    Semantic representation of user query intent.
    
    Attributes:
        metric: The aggregation metric (e.g., "total_sales", "sales_value")
        dimensions: List of dimension names for GROUP BY
        date_range: Predefined date range identifier
        custom_dates: List of custom date objects for specific date queries
        is_trend: Whether this is a time-series trend query
        time_grain: Time granularity (day, month, quarter, year)
        compare_to: Comparison type (mom, qoq, yoy, wow)
        order_by: Column to order results by
        order_direction: Sort direction (asc/desc)
        limit: Number of results to return
        filters: Additional filter values extracted from query
        original_query: Original user query text
    
    Example:
        Query: "Tower wise sales for april 2024"
        Intent:
            metric="total_sales"
            dimensions=["tower"]
            custom_dates=[{"month_num": 4, "year": 2024}]
            date_range="custom_range"
    """
    
    metric: str = "total_sales"
    metrics: List[str] = field(default_factory=list)  # Multi-metric support
    dimensions: List[str] = field(default_factory=list)
    date_range: str = "current_financial_year"
    custom_dates: List[Dict[str, Any]] = field(default_factory=list)  # NEW!
    is_trend: bool = False
    time_grain: Optional[str] = None
    compare_to: Optional[str] = None
    order_by: Optional[str] = None
    order_direction: Optional[str] = None
    limit: Optional[int] = None
    filters: Optional[Dict] = None
    having_filter: Optional[str] = None  # e.g. "net_value > 50000"
    original_query: Optional[str] = None