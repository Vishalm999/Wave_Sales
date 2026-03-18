# from datetime import date, datetime, timedelta
# from typing import List, Dict, Optional
# import re


# # -----------------------------
# # Helpers
# # -----------------------------

# def _date_sql(column_name: str) -> str:
#     """
#     Canonical SQL expression for parsing YYYYMMDD date columns in Presto.
#     Matches dates.yaml definition.
#     """
#     return f"TRY(DATE_PARSE(CAST(\"{column_name}\" AS VARCHAR), '%Y%m%d'))"


# def _financial_year_sql(column_name: str) -> str:
#     """
#     SQL expression to extract financial year from YYYYMMDD column.
    
#     Financial year logic:
#     - If month >= 4 (Apr-Dec): FY = year
#     - If month < 4 (Jan-Mar): FY = year - 1
    
#     Example:
#     - 20240315 (Mar 15, 2024) → FY 2023
#     - 20240415 (Apr 15, 2024) → FY 2024
    
#     Args:
#         column_name: Name of the YYYYMMDD integer column
    
#     Returns:
#         SQL expression that returns the financial year as integer
#     """
#     parsed = _date_sql(column_name)
#     return f"""
#     CASE 
#         WHEN MONTH({parsed}) >= 4 THEN YEAR({parsed})
#         ELSE YEAR({parsed}) - 1
#     END
#     """.strip()


# def _financial_quarter_sql(column_name: str) -> str:
#     """
#     SQL expression to extract financial quarter from YYYYMMDD column.
    
#     Financial quarters:
#     - Q1: Apr, May, Jun (months 4, 5, 6)
#     - Q2: Jul, Aug, Sep (months 7, 8, 9)
#     - Q3: Oct, Nov, Dec (months 10, 11, 12)
#     - Q4: Jan, Feb, Mar (months 1, 2, 3)
    
#     Args:
#         column_name: Name of the YYYYMMDD integer column
    
#     Returns:
#         SQL expression that returns the quarter number (1-4)
#     """
#     parsed = _date_sql(column_name)
#     return f"""
#     CASE
#         WHEN MONTH({parsed}) IN (4, 5, 6) THEN 1
#         WHEN MONTH({parsed}) IN (7, 8, 9) THEN 2
#         WHEN MONTH({parsed}) IN (10, 11, 12) THEN 3
#         WHEN MONTH({parsed}) IN (1, 2, 3) THEN 4
#     END
#     """.strip()


# def _financial_month_sql(column_name: str) -> str:
#     """
#     SQL expression to extract financial month number from YYYYMMDD column.
    
#     Financial months (1-12):
#     - Month 1 = April
#     - Month 2 = May
#     - ...
#     - Month 12 = March
    
#     Args:
#         column_name: Name of the YYYYMMDD integer column
    
#     Returns:
#         SQL expression that returns the financial month number (1-12)
#     """
#     parsed = _date_sql(column_name)
#     return f"""
#     CASE
#         WHEN MONTH({parsed}) >= 4 THEN MONTH({parsed}) - 3
#         ELSE MONTH({parsed}) + 9
#     END
#     """.strip()


# def _first_day_of_month(year: int, month: int) -> str:
#     """Return first day of month as YYYYMMDD string"""
#     return f"{year}{month:02d}01"


# def _last_day_of_month(year: int, month: int) -> str:
#     """Return last day of month as YYYYMMDD string"""
#     if month == 12:
#         next_month = datetime(year + 1, 1, 1)
#     else:
#         next_month = datetime(year, month + 1, 1)
#     return (next_month - timedelta(days=1)).strftime("%Y%m%d")


# def _get_fy_quarter_months(quarter_index: int) -> tuple:
#     """
#     Get start and end months for a financial year quarter.
    
#     Args:
#         quarter_index: 0-3 (Q1-Q4)
    
#     Returns:
#         Tuple of (start_month, end_month)
#     """
#     mapping = {
#         0: (4, 6),   # Q1: Apr-Jun
#         1: (7, 9),   # Q2: Jul-Sep
#         2: (10, 12), # Q3: Oct-Dec
#         3: (1, 3),   # Q4: Jan-Mar
#     }
#     return mapping[quarter_index]


# def _get_current_fy_quarter(today: date) -> tuple:
#     """
#     Get current financial year quarter information.
    
#     Args:
#         today: Current date
    
#     Returns:
#         Tuple of (fy_year, quarter_index) where quarter_index is 0-3
#     """
#     year = today.year
#     month = today.month
    
#     # Determine FY year
#     if month >= 4:
#         fy_year = year
#         fy_month = month
#     else:
#         fy_year = year - 1
#         fy_month = month + 12
    
#     # Calculate quarter index (0-3)
#     quarter_index = (fy_month - 4) // 3
    
#     return (fy_year, quarter_index)


# def _canonical_date_range(dr: Optional[str]) -> str:
#     """
#     Normalize date range strings to canonical format.
#     Handles common aliases and variations.
#     """
#     if not dr:
#         return "current_financial_year"
    
#     # x = str(dr).strip().lower().replace(" ", "").replace("_", "")
#     x = str(dr).strip().lower().replace(" ", "")
    
#     alias_map = {
#         "today": "today",
#         "yesterday": "yesterday",
        
#         "thisweek": "this_week",
#         "lastweek": "last_week",
        
#         "thismonth": "this_month",
#         "lastmonth": "last_month",
        
#         "thisquarter": "this_quarter",
#         "lastquarter": "last_quarter",
        
#         "thisyear": "current_financial_year",
#         "lastyear": "last_financial_year",
        
#         "currentfinancialyear": "current_financial_year",
#         "currentfy": "current_financial_year",
        
#         "lastfinancialyear": "last_financial_year",
#         "lastfy": "last_financial_year",
        
#         "fytd": "fytd",
#         "mtd": "mtd",
#         "qtd": "qtd",
#         "ytd": "ytd",
        
#         "customrange": "custom_range",
        
#         "rolling7days": "rolling_7_days",
#         "rolling14days": "rolling_14_days",
#         "rolling15days": "rolling_15_days",
#         "rolling30days": "rolling_30_days",
#         "rolling60days": "rolling_60_days",
#         "rolling90days": "rolling_90_days",
#         "rolling6months": "rolling_6_months",
#         "rolling12months": "rolling_12_months",
        
#         "yearonyear": "last_3_financial_years",
#         "yoy": "last_3_financial_years",
        
#         "quarteronquarter": "current_financial_year",
#         "qoq": "current_financial_year",
        
#         # NEW: Dynamic "last N" mappings
#         "last2financialyears": "last_2_financial_years",
#         "last3financialyears": "last_3_financial_years",
#         "last4financialyears": "last_4_financial_years",
#         "last5financialyears": "last_5_financial_years",
        
#         "last2quarters": "last_2_quarters",
#         "last3quarters": "last_3_quarters",
#         "last4quarters": "last_4_quarters",
        
#         "last2months": "last_2_months",
#         "last3months": "last_3_months",
#         "last6months": "last_6_months",
#         "last12months": "last_12_months",
#     }
    
#     return alias_map.get(x, x)


# # -----------------------------
# # NEW: Pattern-based date parsing
# # -----------------------------

# def _parse_relative_periods(date_range: str) -> Optional[Dict]:
#     """
#     Parse patterns like:
#     - "last 2 quarters"
#     - "last 3 months"
#     - "last 2 financial years"
    
#     Returns:
#         Dict with 'type' and 'count' if matched, None otherwise
#     """
#     # Clean the input
#     clean = date_range.lower().strip().replace("_", " ")
    
#     # Pattern: "last N quarters" (excluding current quarter)
#     match = re.match(r'last\s+(\d+)\s+quarters?', clean)
#     if match:
#         count = int(match.group(1))
#         return {'type': 'quarters', 'count': count}
    
#     # Pattern: "last N months" (excluding current month)
#     match = re.match(r'last\s+(\d+)\s+months?', clean)
#     if match:
#         count = int(match.group(1))
#         return {'type': 'months', 'count': count}
    
#     # Pattern: "last N financial years" (excluding current FY)
#     match = re.match(r'last\s+(\d+)\s+(?:financial\s+)?years?', clean)
#     if match:
#         count = int(match.group(1))
#         return {'type': 'financial_years', 'count': count}
    
#     return None


# def _resolve_last_n_quarters(count: int, column_name: str, today: date) -> str:
#     """
#     Generate SQL for last N quarters (EXCLUDING current quarter).
    
#     Example: If today is Feb 12, 2026 (Q4 of FY 2025-26):
#     - "last 2 quarters" = Q2 (Jul-Sep 2025) + Q3 (Oct-Dec 2025)
#     - "last quarter" = Q3 (Oct-Dec 2025)
    
#     Args:
#         count: Number of quarters
#         column_name: Date column name
#         today: Current date
    
#     Returns:
#         SQL WHERE clause
#     """
#     year = today.year
#     month = today.month
    
#     # Get current quarter info
#     fy_year, current_q_index = _get_current_fy_quarter(today)
    
#     # Calculate the ending quarter (the one just before current)
#     end_q_index = current_q_index - 1
#     end_fy_year = fy_year
    
#     if end_q_index < 0:
#         end_q_index = 3  # Q4 of previous FY
#         end_fy_year -= 1
    
#     # Calculate the starting quarter
#     start_q_index = end_q_index - (count - 1)
#     start_fy_year = end_fy_year
    
#     while start_q_index < 0:
#         start_q_index += 4
#         start_fy_year -= 1
    
#     # Get start date
#     start_month, _ = _get_fy_quarter_months(start_q_index)
#     start_year = start_fy_year
#     if start_month > 12:
#         start_month -= 12
#         start_year += 1
#     start = _first_day_of_month(start_year, start_month)
    
#     # Get end date
#     _, end_month = _get_fy_quarter_months(end_q_index)
#     end_year = end_fy_year
#     if end_month > 12:
#         end_month -= 12
#         end_year += 1
#     end = _last_day_of_month(end_year, end_month)
    
#     col_sql = _date_sql(column_name)
    
#     result = (
#         f"{col_sql} BETWEEN "
#         f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
#     )
    
#     print(f"[LAST_N_QUARTERS] count={count}, today={today}")
#     print(f"[LAST_N_QUARTERS] Current quarter: FY{fy_year} Q{current_q_index + 1}")
#     print(f"[LAST_N_QUARTERS] Range: FY{start_fy_year} Q{start_q_index + 1} to FY{end_fy_year} Q{end_q_index + 1}")
#     print(f"[LAST_N_QUARTERS] Date range: {start} to {end}")
#     print(f"[LAST_N_QUARTERS] SQL: {result}")
    
#     return result


# def _resolve_last_n_months(count: int, column_name: str, today: date) -> str:
#     """
#     Generate SQL for last N months (EXCLUDING current month).
    
#     Example: If today is Feb 12, 2026:
#     - "last 2 months" = Dec 2025 + Jan 2026
#     - "last month" = Jan 2026
    
#     Args:
#         count: Number of months
#         column_name: Date column name
#         today: Current date
    
#     Returns:
#         SQL WHERE clause
#     """
#     # End month is the one just before current month
#     end_date = (today.replace(day=1) - timedelta(days=1))
#     end_year = end_date.year
#     end_month = end_date.month
    
#     # Calculate start month
#     start_date = end_date
#     for _ in range(count - 1):
#         start_date = (start_date.replace(day=1) - timedelta(days=1))
    
#     start_year = start_date.year
#     start_month = start_date.month
    
#     start = _first_day_of_month(start_year, start_month)
#     end = _last_day_of_month(end_year, end_month)
    
#     col_sql = _date_sql(column_name)
    
#     result = (
#         f"{col_sql} BETWEEN "
#         f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
#     )
    
#     print(f"[LAST_N_MONTHS] count={count}, today={today}")
#     print(f"[LAST_N_MONTHS] Date range: {start} to {end}")
#     print(f"[LAST_N_MONTHS] SQL: {result}")
    
#     return result


# def _resolve_last_n_financial_years(count: int, column_name: str, today: date) -> str:
#     """
#     Generate SQL for last N financial years INCLUDING the current FY (partial).
    
#     Example: If today is Mar 5, 2026 (FY 2025-26):
#     - "last 3 financial years" = FY 2022-23 + FY 2023-24 + FY 2024-25 + FY 2025-26 (to today)
#     - count=3 means 3 complete past FYs + current partial FY
#     """
#     year = today.year
#     month = today.month
    
#     # Current FY start year
#     current_fy_start = year if month >= 4 else year - 1
    
#     # Start N full FYs before current FY
#     start_fy_start = current_fy_start - count
    
#     start = f"{start_fy_start}0401"
#     end = today.strftime("%Y%m%d")  # Include current FY up to today
    
#     col_sql = _date_sql(column_name)
    
#     result = (
#         f"{col_sql} BETWEEN "
#         f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
#     )
    
#     print(f"[LAST_N_FY] count={count}, today={today}")
#     print(f"[LAST_N_FY] Current FY: {current_fy_start}-{current_fy_start + 1}")
#     print(f"[LAST_N_FY] Range: FY {start_fy_start} to today ({end})")
#     print(f"[LAST_N_FY] SQL: {result}")
    
#     return result


# # -----------------------------
# # Custom Date Handling
# # -----------------------------

# def resolve_custom_dates(custom_dates: List[Dict], column_name: str) -> str:
#     """
#     Resolve custom date specifications.
#     Custom dates are treated as ONE CONTINUOUS RANGE.
    
#     Args:
#         custom_dates: List of date dictionaries with year, month_num, and optional day
#         column_name: Name of date column
    
#     Returns:
#         SQL WHERE clause
#     """
#     if not custom_dates:
#         raise ValueError("custom_dates provided but empty")
    
#     # Normalize and sort
#     def to_key(d):
#         y = d["year"]
#         m = d.get("month_num") or d.get("month", 1)
#         day = d.get("day", 1)
#         return y * 10000 + m * 100 + day
    
#     custom_dates = sorted(custom_dates, key=to_key)
    
#     def build_date(d: Dict, is_start: bool) -> str:
#         """Build YYYYMMDD string from date dict. Uses day if present."""
#         y = d["year"]
#         m = d.get("month_num") or d.get("month", 1)
#         if "day" in d:
#             return f"{y}{m:02d}{d['day']:02d}"
#         return _first_day_of_month(y, m) if is_start else _last_day_of_month(y, m)

#     if len(custom_dates) == 1:
#         d = custom_dates[0]
#         y = d["year"]
#         m = d.get("month_num") or d.get("month")
#         if "day" in d:
#             # Single specific day — filter for that exact date
#             day_str = f"{y}{m:02d}{d['day']:02d}"
#             start = end = day_str
#         else:
#             # Single month — full month range
#             start = _first_day_of_month(y, m)
#             end = _last_day_of_month(y, m)
#     else:
#         # Multiple entries — continuous range, respecting day fields
#         start_d = custom_dates[0]
#         end_d = custom_dates[-1]
#         start = build_date(start_d, True)
#         end = build_date(end_d, False)
    
#     col_sql = _date_sql(column_name)
    
#     result = (
#         f"{col_sql} BETWEEN "
#         f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
#     )
    
#     print(f"[RESOLVE_CUSTOM_DATES] Generated SQL: {result}")
    
#     return result


# # -----------------------------
# # Main Date Filter Resolver
# # -----------------------------

# def resolve_date_filter(
#     date_range: str,
#     column_name: str = "Document_Date",
#     custom_dates: Optional[List[Dict]] = None
# ) -> str:
#     """
#     Resolve date range to SQL WHERE clause.
    
#     Args:
#         date_range: Date range identifier (e.g., "current_financial_year", "last 2 quarters")
#         column_name: Name of date column in database
#         custom_dates: List of custom date dictionaries (overrides date_range)
    
#     Returns:
#         SQL WHERE clause for date filtering
    
#     Financial Year Context:
#         - FY runs from April 1 to March 31
#         - FY 2024-25 = Apr 2024 to Mar 2025
#         - All year-based calculations use FY, not calendar year
    
#     NEW: Supports patterns like:
#         - "last 2 quarters" (excludes current quarter)
#         - "last 3 months" (excludes current month)
#         - "last 2 financial years" (excludes current FY)
#     """
    
#     today = date.today()
#     year = today.year
#     month = today.month
    
#     col_sql = _date_sql(column_name)
    
#     # ========================================
#     # 1. CUSTOM DATES OVERRIDE
#     # ========================================
#     if custom_dates:
#         return resolve_custom_dates(custom_dates, column_name)
    

    

#     # ========================================
#     # 2. CHECK FOR PATTERN-BASED DATES (PRIORITY)
#     # ========================================
#     # This handles dynamic "last N" patterns
#     parsed = _parse_relative_periods(date_range)
#     if parsed:
#         if parsed['type'] == 'quarters':
#             return _resolve_last_n_quarters(parsed['count'], column_name, today)
#         elif parsed['type'] == 'months':
#             return _resolve_last_n_months(parsed['count'], column_name, today)
#         elif parsed['type'] == 'financial_years':
#             return _resolve_last_n_financial_years(parsed['count'], column_name, today)
    
#     # ========================================
#     # 3. NORMALIZE DATE RANGE
#     # ========================================
#     date_range = _canonical_date_range(date_range)
    
#     # ========================================
#     # 4. FINANCIAL YEAR RANGES
#     # ========================================
    
#     if date_range == "current_financial_year":
#         start_year = year if month >= 4 else year - 1
#         end_year = start_year + 1
#         start = f"{start_year}0401"
#         end = f"{end_year}0331"
    
#     elif date_range == "last_financial_year":
#         start_year = (year - 1) if month >= 4 else (year - 2)
#         end_year = start_year + 1
#         start = f"{start_year}0401"
#         end = f"{end_year}0331"
    
#     elif date_range == "fytd":
#         start_year = year if month >= 4 else year - 1
#         start = f"{start_year}0401"
#         end = today.strftime("%Y%m%d")
    
#     # ========================================
#     # 5. WEEK RANGES
#     # ========================================
    
#     elif date_range == "this_week":
#         monday = today - timedelta(days=today.weekday())
#         sunday = monday + timedelta(days=6)
#         start = monday.strftime("%Y%m%d")
#         end = sunday.strftime("%Y%m%d")
    
#     elif date_range == "last_week":
#         last_monday = today - timedelta(days=today.weekday() + 7)
#         last_sunday = last_monday + timedelta(days=6)
#         start = last_monday.strftime("%Y%m%d")
#         end = last_sunday.strftime("%Y%m%d")
    
#     # ========================================
#     # 6. DAY RANGES
#     # ========================================
    
#     elif date_range == "today":
#         start = end = today.strftime("%Y%m%d")
    
#     elif date_range == "yesterday":
#         y = today - timedelta(days=1)
#         start = end = y.strftime("%Y%m%d")
    
#     # ========================================
#     # 7. MONTH RANGES
#     # ========================================
    
#     elif date_range == "this_month":
#         start = _first_day_of_month(year, month)
#         end = _last_day_of_month(year, month)
    
#     elif date_range == "last_month":
#         prev = (today.replace(day=1) - timedelta(days=1))
#         start = _first_day_of_month(prev.year, prev.month)
#         end = _last_day_of_month(prev.year, prev.month)
    
#     # ========================================
#     # 8. FINANCIAL QUARTER RANGES
#     # ========================================
    
#     elif date_range in ("this_quarter", "last_quarter", "qtd"):
#         # Calculate FY year and month
#         if month >= 4:
#             fy_year = year
#             fy_month = month
#         else:
#             fy_year = year - 1
#             fy_month = month + 12
        
#         # Calculate quarter index (0-3)
#         q_index = (fy_month - 4) // 3
        
#         if date_range == "last_quarter":
#             q_index -= 1
        
#         # Handle wrap-around
#         if q_index < 0:
#             q_index += 4
#             fy_year -= 1
        
#         # Calculate start month
#         start_month = 4 + q_index * 3
#         start_year = fy_year
#         if start_month > 12:
#             start_month -= 12
#             start_year += 1
        
#         # Calculate end month
#         end_month = start_month + 2
#         end_year = start_year
#         if end_month > 12:
#             end_month -= 12
#             end_year += 1
        
#         start = _first_day_of_month(start_year, start_month)
        
#         if date_range == "qtd":
#             end = today.strftime("%Y%m%d")
#         else:
#             end = _last_day_of_month(end_year, end_month)
    
#     # ========================================
#     # 9. TO-DATE RANGES
#     # ========================================
    
#     elif date_range == "mtd":
#         start = _first_day_of_month(year, month)
#         end = today.strftime("%Y%m%d")
    
#     elif date_range == "ytd":
#         start = f"{year}0101"
#         end = today.strftime("%Y%m%d")
    
#     # ========================================
#     # 10. ROLLING WINDOWS
#     # ========================================
    
#     elif date_range.startswith("rolling_"):
#         days_map = {
#             "rolling_7_days": 7,
#             "rolling_14_days": 14,
#             "rolling_15_days": 15,
#             "rolling_30_days": 30,
#             "rolling_60_days": 60,
#             "rolling_90_days": 90,
#             "rolling_6_months": 180,
#             "rolling_12_months": 365,
#         }
#         days = days_map.get(date_range, 30)
#         start = (today - timedelta(days=days - 1)).strftime("%Y%m%d")
#         end = today.strftime("%Y%m%d")
    
    
    

#     # ========================================
#     # 14. SPECIFIC FINANCIAL YEAR (e.g., "fy_2024")
#     # ========================================
    
#     elif date_range.startswith("fy_"):
#         try:
#             target_year = int(date_range.split("_")[1])
#         except:
#             # Fallback to current FY
#             target_year = year if month >= 4 else year - 1
        
#         start = f"{target_year}0401"
#         end = f"{target_year + 1}0331"
        
#         print(f"[DATE_RESOLVER] Specific FY {target_year}: {start} to {end}")
    
#     # ========================================
#     # 15. SPECIFIC QUARTER (e.g., "q1", "q2")
#     # ========================================
    
#     elif date_range in ("q1", "q2", "q3", "q4"):
#         # Get current financial year
#         curr_fy_start = year if month >= 4 else year - 1
        
#         # Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
#         quarter_map = {
#             "q1": (4, 6),   # Apr-Jun
#             "q2": (7, 9),   # Jul-Sep  
#             "q3": (10, 12), # Oct-Dec
#             "q4": (1, 3)    # Jan-Mar (next calendar year)
#         }
        
#         start_month, end_month = quarter_map[date_range]
        
#         if date_range == "q4":
#             # Q4 spans into next calendar year
#             start_year = curr_fy_start
#             end_year = curr_fy_start + 1
#         else:
#             start_year = curr_fy_start
#             end_year = curr_fy_start
        
#         start = _first_day_of_month(start_year, start_month)
#         end = _last_day_of_month(end_year, end_month)
        
#         print(f"[DATE_RESOLVER] {date_range.upper()}: {start} to {end}")
    

#     # ========================================
#     # 11. MULTI-YEAR COMPARISONS
#     # ========================================
    
#     elif date_range == "last_3_financial_years":
#         # Include current FY (partial) so YoY shows current year alongside past years.
#         # End = today so current FY data up to now is visible.
#         curr_fy_start = year if month >= 4 else year - 1
#         start_year = curr_fy_start - 3
#         start = f"{start_year}0401"
#         end = today.strftime("%Y%m%d")
#         print(f"[DATE_RESOLVER] last_3_financial_years (inc. current FY): {start} to {end}")

#     # ========================================
#     # 12. DYNAMIC HANDLERS (Fallback for underscore format)
#     # ========================================
#     # These handle cases like "last_2_financial_years" that weren't caught by pattern parser
    
#     elif date_range.startswith("last_") and date_range.endswith("_financial_years"):
#         try:
#             n = int(date_range.split("_")[1])
#         except:
#             n = 3
        
#         curr_fy_start = year if month >= 4 else year - 1
#         start_year = curr_fy_start - n
        
#         # End = today so current FY (partial) is included
#         start = f"{start_year}0401"
#         end = today.strftime("%Y%m%d")
        
#         print(f"[DATE_RESOLVER] {date_range} (inc. current FY): {start} to {end}")
    
#     elif date_range.startswith("last_") and date_range.endswith("_quarters"):
#         try:
#             n = int(date_range.split("_")[1])
#         except:
#             n = 2
        
#         if month >= 4:
#             fy_year = year
#             fy_month = month
#         else:
#             fy_year = year - 1
#             fy_month = month + 12
        
#         current_q = (fy_month - 4) // 3
#         start_q = current_q - n
        
#         while start_q < 0:
#             start_q += 4
#             fy_year -= 1
        
#         start_month = 4 + start_q * 3
#         start_year = fy_year
#         if start_month > 12:
#             start_month -= 12
#             start_year += 1
        
#         end_offset = n * 3 - 1
#         end_month = start_month + end_offset
#         end_year = start_year
        
#         while end_month > 12:
#             end_month -= 12
#             end_year += 1
        
#         start = _first_day_of_month(start_year, start_month)
#         end = _last_day_of_month(end_year, end_month)
        
#         print(f"[DATE_RESOLVER] {date_range}: {start} to {end}")
    
#     elif date_range.startswith("last_") and date_range.endswith("_months"):
#         try:
#             n = int(date_range.split("_")[1])
#         except:
#             n = 2
        
#         prev_month_date = today.replace(day=1) - timedelta(days=1)
#         end_month = prev_month_date.month
#         end_year = prev_month_date.year
        
#         start_date = prev_month_date
#         for _ in range(n - 1):
#             start_date = start_date.replace(day=1) - timedelta(days=1)
        
#         start_month = start_date.month
#         start_year = start_date.year
        
#         start = _first_day_of_month(start_year, start_month)
#         end = _last_day_of_month(end_year, end_month)
        
#         print(f"[DATE_RESOLVER] {date_range}: {start} to {end}")
    
    
#     # ==================================================
#     # elif date_range.startswith("fy_"):
#     #     try:
#     #         target_year = int(date_range.split("_")[1])
#     #     except:
#     #         target_year = year if month >= 4 else year - 1
        
#     #     start = f"{target_year}0401"
#     #     end = f"{target_year + 1}0331"
        
#     #     print(f"[DATE_RESOLVER] Specific FY {target_year}: {start} to {end}")
    
#     # elif date_range in ("q1", "q2", "q3", "q4"):
#     #     curr_fy_start = year if month >= 4 else year - 1
        
#     #     quarter_map = {
#     #         "q1": (4, 6),
#     #         "q2": (7, 9),
#     #         "q3": (10, 12),
#     #         "q4": (1, 3)
#     #     }
        
#     #     start_month, end_month = quarter_map[date_range]
        
#     #     if date_range == "q4":
#     #         start_year = curr_fy_start
#     #         end_year = curr_fy_start + 1
#     #     else:
#     #         start_year = curr_fy_start
#     #         end_year = curr_fy_start
        
#     #     start = _first_day_of_month(start_year, start_month)
#     #     end = _last_day_of_month(end_year, end_month)
        
#     #     print(f"[DATE_RESOLVER] {date_range.upper()}: {start} to {end}")



#     # # ========================================
#     # # 13. UNKNOWN → DEFAULT TO CURRENT FY
#     # # ========================================
    
#     # else:
#     #     print(f"[WARNING] Unknown date range '{date_range}', defaulting to current_financial_year")
#     #     start_year = year if month >= 4 else year - 1
#     #     end_year = start_year + 1
#     #     start = f"{start_year}0401"
#     #     end = f"{end_year}0331"
    
#     # ========================================
#     # 13. SPECIFIC FINANCIAL YEAR (e.g., "fy_2024")
#     # ========================================
    
#     elif date_range.startswith("fy_"):
#         try:
#             target_year = int(date_range.split("_")[1])
#         except:
#             target_year = year if month >= 4 else year - 1
        
#         start = f"{target_year}0401"
#         end = f"{target_year + 1}0331"
        
#         print(f"[DATE_RESOLVER] Specific FY {target_year}: {start} to {end}")
    
#     # ========================================
#     # 14. SPECIFIC QUARTER (e.g., "q1", "q2", "q3", "q4")
#     # ========================================
    
#     elif date_range in ("q1", "q2", "q3", "q4"):
#         # Get current financial year
#         curr_fy_start = year if month >= 4 else year - 1
        
#         # Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
#         quarter_map = {
#             "q1": (4, 6),
#             "q2": (7, 9),
#             "q3": (10, 12),
#             "q4": (1, 3)
#         }
        
#         start_month, end_month = quarter_map[date_range]
        
#         if date_range == "q4":
#             start_year = curr_fy_start
#             end_year = curr_fy_start + 1
#         else:
#             start_year = curr_fy_start
#             end_year = curr_fy_start
        
#         start = _first_day_of_month(start_year, start_month)
#         end = _last_day_of_month(end_year, end_month)
        
#         print(f"[DATE_RESOLVER] {date_range.upper()}: {start} to {end}")
    
#     # ========================================
#     # 15. UNKNOWN → DEFAULT TO CURRENT FY
#     # ========================================
    
#     else:
#         print(f"[WARNING] Unknown date range '{date_range}', defaulting to current_financial_year")
#         start_year = year if month >= 4 else year - 1
#         end_year = start_year + 1
#         start = f"{start_year}0401"
#         end = f"{end_year}0331"

#     # ========================================
#     # BUILD SQL CLAUSE
#     # ========================================
    
#     return (
#         f"{col_sql} BETWEEN "
#         f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
#     )















# app/semantic/date_resolver.py

from datetime import date, datetime, timedelta
from typing import List, Dict, Optional
import re


# -----------------------------
# Helpers
# -----------------------------

def _date_sql(column_name: str) -> str:
    """
    Canonical SQL expression for parsing YYYYMMDD date columns in Presto.
    Matches dates.yaml definition.
    """
    return f"TRY(DATE_PARSE(CAST(\"{column_name}\" AS VARCHAR), '%Y%m%d'))"


def _financial_year_sql(column_name: str) -> str:
    """
    SQL expression to extract financial year from YYYYMMDD column.
    
    Financial year logic:
    - If month >= 4 (Apr-Dec): FY = year
    - If month < 4 (Jan-Mar): FY = year - 1
    
    Example:
    - 20240315 (Mar 15, 2024) → FY 2023
    - 20240415 (Apr 15, 2024) → FY 2024
    
    Args:
        column_name: Name of the YYYYMMDD integer column
    
    Returns:
        SQL expression that returns the financial year as integer
    """
    parsed = _date_sql(column_name)
    return f"""
    CASE 
        WHEN MONTH({parsed}) >= 4 THEN YEAR({parsed})
        ELSE YEAR({parsed}) - 1
    END
    """.strip()


def _financial_quarter_sql(column_name: str) -> str:
    """
    SQL expression to extract financial quarter from YYYYMMDD column.
    
    Financial quarters:
    - Q1: Apr, May, Jun (months 4, 5, 6)
    - Q2: Jul, Aug, Sep (months 7, 8, 9)
    - Q3: Oct, Nov, Dec (months 10, 11, 12)
    - Q4: Jan, Feb, Mar (months 1, 2, 3)
    
    Args:
        column_name: Name of the YYYYMMDD integer column
    
    Returns:
        SQL expression that returns the quarter number (1-4)
    """
    parsed = _date_sql(column_name)
    return f"""
    CASE
        WHEN MONTH({parsed}) IN (4, 5, 6) THEN 1
        WHEN MONTH({parsed}) IN (7, 8, 9) THEN 2
        WHEN MONTH({parsed}) IN (10, 11, 12) THEN 3
        WHEN MONTH({parsed}) IN (1, 2, 3) THEN 4
    END
    """.strip()


def _financial_month_sql(column_name: str) -> str:
    """
    SQL expression to extract financial month number from YYYYMMDD column.
    
    Financial months (1-12):
    - Month 1 = April
    - Month 2 = May
    - ...
    - Month 12 = March
    
    Args:
        column_name: Name of the YYYYMMDD integer column
    
    Returns:
        SQL expression that returns the financial month number (1-12)
    """
    parsed = _date_sql(column_name)
    return f"""
    CASE
        WHEN MONTH({parsed}) >= 4 THEN MONTH({parsed}) - 3
        ELSE MONTH({parsed}) + 9
    END
    """.strip()


def _first_day_of_month(year: int, month: int) -> str:
    """Return first day of month as YYYYMMDD string"""
    return f"{year}{month:02d}01"


def _last_day_of_month(year: int, month: int) -> str:
    """Return last day of month as YYYYMMDD string"""
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    return (next_month - timedelta(days=1)).strftime("%Y%m%d")


def _get_fy_quarter_months(quarter_index: int) -> tuple:
    """
    Get start and end months for a financial year quarter.
    
    Args:
        quarter_index: 0-3 (Q1-Q4)
    
    Returns:
        Tuple of (start_month, end_month)
    """
    mapping = {
        0: (4, 6),   # Q1: Apr-Jun
        1: (7, 9),   # Q2: Jul-Sep
        2: (10, 12), # Q3: Oct-Dec
        3: (1, 3),   # Q4: Jan-Mar
    }
    return mapping[quarter_index]


def _get_current_fy_quarter(today: date) -> tuple:
    """
    Get current financial year quarter information.
    
    Args:
        today: Current date
    
    Returns:
        Tuple of (fy_year, quarter_index) where quarter_index is 0-3
    """
    year = today.year
    month = today.month
    
    # Determine FY year
    if month >= 4:
        fy_year = year
        fy_month = month
    else:
        fy_year = year - 1
        fy_month = month + 12
    
    # Calculate quarter index (0-3)
    quarter_index = (fy_month - 4) // 3
    
    return (fy_year, quarter_index)


def _canonical_date_range(dr: Optional[str]) -> str:
    """
    Normalize date range strings to canonical format.
    Handles common aliases and variations.
    """
    if not dr:
        return "current_financial_year"
    
    # x = str(dr).strip().lower().replace(" ", "").replace("_", "")
    x = str(dr).strip().lower().replace(" ", "")
    
    alias_map = {
        "today": "today",
        "yesterday": "yesterday",
        
        "thisweek": "this_week",
        "lastweek": "last_week",
        
        "thismonth": "this_month",
        "lastmonth": "last_month",
        
        "thisquarter": "this_quarter",
        "lastquarter": "last_quarter",
        
        "thisyear": "current_financial_year",
        "lastyear": "last_financial_year",
        
        "currentfinancialyear": "current_financial_year",
        "currentfy": "current_financial_year",
        
        "lastfinancialyear": "last_financial_year",
        "lastfy": "last_financial_year",
        
        "fytd": "fytd",
        "mtd": "mtd",
        "qtd": "qtd",
        "ytd": "ytd",
        
        "customrange": "custom_range",
        
        "rolling7days": "rolling_7_days",
        "rolling14days": "rolling_14_days",
        "rolling15days": "rolling_15_days",
        "rolling30days": "rolling_30_days",
        "rolling60days": "rolling_60_days",
        "rolling90days": "rolling_90_days",
        "rolling6months": "rolling_6_months",
        "rolling12months": "rolling_12_months",
        
        "yearonyear": "last_3_financial_years_yoy",
        "yoy": "last_3_financial_years_yoy",
        
        "quarteronquarter": "current_financial_year",
        "qoq": "current_financial_year",
        
        # NEW: Dynamic "last N" mappings
        "last2financialyears": "last_2_financial_years",
        "last3financialyears": "last_3_financial_years",
        "last4financialyears": "last_4_financial_years",
        "last5financialyears": "last_5_financial_years",
        
        "last2quarters": "last_2_quarters",
        "last3quarters": "last_3_quarters",
        "last4quarters": "last_4_quarters",
        
        "last2months": "last_2_months",
        "last3months": "last_3_months",
        "last6months": "last_6_months",
        "last12months": "last_12_months",
    }
    
    return alias_map.get(x, x)


# -----------------------------
# NEW: Pattern-based date parsing
# -----------------------------

def _parse_relative_periods(date_range: str) -> Optional[Dict]:
    """
    Parse patterns like:
    - "last 2 quarters"
    - "last 3 months"
    - "last 2 financial years"
    
    Returns:
        Dict with 'type' and 'count' if matched, None otherwise
    """
    # Clean the input
    clean = date_range.lower().strip().replace("_", " ")
    
    # Pattern: "last N quarters" (excluding current quarter)
    # NOTE: Anchored to avoid matching trailing tokens like "yoy".
    match = re.match(r'^last\s+(\d+)\s+quarters?$', clean)
    if match:
        count = int(match.group(1))
        return {'type': 'quarters', 'count': count}
    
    # Pattern: "last N months" (excluding current month)
    match = re.match(r'^last\s+(\d+)\s+months?$', clean)
    if match:
        count = int(match.group(1))
        return {'type': 'months', 'count': count}
    
    # Pattern: "last N financial years" (excluding current FY)
    match = re.match(r'^last\s+(\d+)\s+(?:financial\s+)?years?$', clean)
    if match:
        count = int(match.group(1))
        return {'type': 'financial_years', 'count': count}
    
    return None


def _resolve_last_n_quarters(count: int, column_name: str, today: date) -> str:
    """
    Generate SQL for last N quarters (EXCLUDING current quarter).
    
    Example: If today is Feb 12, 2026 (Q4 of FY 2025-26):
    - "last 2 quarters" = Q2 (Jul-Sep 2025) + Q3 (Oct-Dec 2025)
    - "last quarter" = Q3 (Oct-Dec 2025)
    
    Args:
        count: Number of quarters
        column_name: Date column name
        today: Current date
    
    Returns:
        SQL WHERE clause
    """
    year = today.year
    month = today.month
    
    # Get current quarter info
    fy_year, current_q_index = _get_current_fy_quarter(today)
    
    # Calculate the ending quarter (the one just before current)
    end_q_index = current_q_index - 1
    end_fy_year = fy_year
    
    if end_q_index < 0:
        end_q_index = 3  # Q4 of previous FY
        end_fy_year -= 1
    
    # Calculate the starting quarter
    start_q_index = end_q_index - (count - 1)
    start_fy_year = end_fy_year
    
    while start_q_index < 0:
        start_q_index += 4
        start_fy_year -= 1
    
    # Get start date
    start_month, _ = _get_fy_quarter_months(start_q_index)
    start_year = start_fy_year
    if start_month > 12:
        start_month -= 12
        start_year += 1
    start = _first_day_of_month(start_year, start_month)
    
    # Get end date
    _, end_month = _get_fy_quarter_months(end_q_index)
    end_year = end_fy_year
    if end_month > 12:
        end_month -= 12
        end_year += 1
    end = _last_day_of_month(end_year, end_month)
    
    col_sql = _date_sql(column_name)
    
    result = (
        f"{col_sql} BETWEEN "
        f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
    )
    
    print(f"[LAST_N_QUARTERS] count={count}, today={today}")
    print(f"[LAST_N_QUARTERS] Current quarter: FY{fy_year} Q{current_q_index + 1}")
    print(f"[LAST_N_QUARTERS] Range: FY{start_fy_year} Q{start_q_index + 1} to FY{end_fy_year} Q{end_q_index + 1}")
    print(f"[LAST_N_QUARTERS] Date range: {start} to {end}")
    print(f"[LAST_N_QUARTERS] SQL: {result}")
    
    return result


def _resolve_last_n_months(count: int, column_name: str, today: date) -> str:
    """
    Generate SQL for last N months (EXCLUDING current month).
    
    Example: If today is Feb 12, 2026:
    - "last 2 months" = Dec 2025 + Jan 2026
    - "last month" = Jan 2026
    
    Args:
        count: Number of months
        column_name: Date column name
        today: Current date
    
    Returns:
        SQL WHERE clause
    """
    # End month is the one just before current month
    end_date = (today.replace(day=1) - timedelta(days=1))
    end_year = end_date.year
    end_month = end_date.month
    
    # Calculate start month
    start_date = end_date
    for _ in range(count - 1):
        start_date = (start_date.replace(day=1) - timedelta(days=1))
    
    start_year = start_date.year
    start_month = start_date.month
    
    start = _first_day_of_month(start_year, start_month)
    end = _last_day_of_month(end_year, end_month)
    
    col_sql = _date_sql(column_name)
    
    result = (
        f"{col_sql} BETWEEN "
        f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
    )
    
    print(f"[LAST_N_MONTHS] count={count}, today={today}")
    print(f"[LAST_N_MONTHS] Date range: {start} to {end}")
    print(f"[LAST_N_MONTHS] SQL: {result}")
    
    return result


def _resolve_last_n_financial_years(count: int, column_name: str, today: date) -> str:
    """
    Generate SQL for last N complete financial years (excludes current partial FY).

    Example: If today is Mar 17, 2026 (FY 2025-26):
    - "last 3 financial years" = FY2022-23 + FY2023-24 + FY2024-25
    - start = Apr 1 2022, end = Mar 31 2025
    """
    year = today.year
    month = today.month

    # Current FY start year (e.g. 2025 for FY2025-26)
    current_fy_start = year if month >= 4 else year - 1

    # Start N complete FYs before current FY
    start_fy_start = current_fy_start - count

    start = f"{start_fy_start}0401"
    # End = last day of the most recently completed FY (Mar 31 of current_fy_start)
    end = f"{current_fy_start}0331"

    col_sql = _date_sql(column_name)

    result = (
        f"{col_sql} BETWEEN "
        f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
    )

    print(f"[LAST_N_FY] count={count}, today={today}")
    print(f"[LAST_N_FY] Current FY: {current_fy_start}-{current_fy_start + 1}")
    print(f"[LAST_N_FY] Range: FY {start_fy_start} to {end}")
    print(f"[LAST_N_FY] SQL: {result}")

    return result


# -----------------------------
# Custom Date Handling
# -----------------------------

def resolve_custom_dates(custom_dates: List[Dict], column_name: str) -> str:
    """
    Resolve custom date specifications.
    Custom dates are treated as ONE CONTINUOUS RANGE.
    
    Args:
        custom_dates: List of date dictionaries with year, month_num, and optional day
        column_name: Name of date column
    
    Returns:
        SQL WHERE clause
    """
    if not custom_dates:
        raise ValueError("custom_dates provided but empty")
    
    # Normalize and sort
    def to_key(d):
        y = d["year"]
        m = d.get("month_num") or d.get("month", 1)
        day = d.get("day", 1)
        return y * 10000 + m * 100 + day
    
    custom_dates = sorted(custom_dates, key=to_key)
    
    def build_date(d: Dict, is_start: bool) -> str:
        """Build YYYYMMDD string from date dict. Uses day if present."""
        y = d["year"]
        m = d.get("month_num") or d.get("month", 1)
        if "day" in d:
            return f"{y}{m:02d}{d['day']:02d}"
        return _first_day_of_month(y, m) if is_start else _last_day_of_month(y, m)

    if len(custom_dates) == 1:
        d = custom_dates[0]
        y = d["year"]
        m = d.get("month_num") or d.get("month")
        if "day" in d:
            # Single specific day — filter for that exact date
            day_str = f"{y}{m:02d}{d['day']:02d}"
            start = end = day_str
        else:
            # Single month — full month range
            start = _first_day_of_month(y, m)
            end = _last_day_of_month(y, m)
    else:
        # Multiple entries — continuous range, respecting day fields
        start_d = custom_dates[0]
        end_d = custom_dates[-1]
        start = build_date(start_d, True)
        end = build_date(end_d, False)
    
    col_sql = _date_sql(column_name)
    
    result = (
        f"{col_sql} BETWEEN "
        f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
    )
    
    print(f"[RESOLVE_CUSTOM_DATES] Generated SQL: {result}")
    
    return result


# -----------------------------
# Main Date Filter Resolver
# -----------------------------

def resolve_date_filter(
    date_range: str,
    column_name: str = "Document_Date",
    custom_dates: Optional[List[Dict]] = None
) -> str:
    """
    Resolve date range to SQL WHERE clause.
    
    Args:
        date_range: Date range identifier (e.g., "current_financial_year", "last 2 quarters")
        column_name: Name of date column in database
        custom_dates: List of custom date dictionaries (overrides date_range)
    
    Returns:
        SQL WHERE clause for date filtering
    
    Financial Year Context:
        - FY runs from April 1 to March 31
        - FY 2024-25 = Apr 2024 to Mar 2025
        - All year-based calculations use FY, not calendar year
    
    NEW: Supports patterns like:
        - "last 2 quarters" (excludes current quarter)
        - "last 3 months" (excludes current month)
        - "last 2 financial years" (excludes current FY)
    """
    
    today = date.today()
    year = today.year
    month = today.month
    
    col_sql = _date_sql(column_name)
    
    # ========================================
    # 1. CUSTOM DATES OVERRIDE
    # ========================================
    if custom_dates:
        return resolve_custom_dates(custom_dates, column_name)
    

    

    # ========================================
    # 2. CHECK FOR PATTERN-BASED DATES (PRIORITY)
    # ========================================
    # This handles dynamic "last N" patterns
    parsed = _parse_relative_periods(date_range)
    if parsed:
        if parsed['type'] == 'quarters':
            return _resolve_last_n_quarters(parsed['count'], column_name, today)
        elif parsed['type'] == 'months':
            return _resolve_last_n_months(parsed['count'], column_name, today)
        elif parsed['type'] == 'financial_years':
            return _resolve_last_n_financial_years(parsed['count'], column_name, today)
    
    # ========================================
    # 3. NORMALIZE DATE RANGE
    # ========================================
    date_range = _canonical_date_range(date_range)
    
    # ========================================
    # 4. FINANCIAL YEAR RANGES
    # ========================================
    
    if date_range == "current_financial_year":
        start_year = year if month >= 4 else year - 1
        end_year = start_year + 1
        start = f"{start_year}0401"
        end = f"{end_year}0331"
    
    elif date_range == "last_financial_year":
        start_year = (year - 1) if month >= 4 else (year - 2)
        end_year = start_year + 1
        start = f"{start_year}0401"
        end = f"{end_year}0331"
    
    elif date_range == "fytd":
        start_year = year if month >= 4 else year - 1
        start = f"{start_year}0401"
        end = today.strftime("%Y%m%d")
    
    # ========================================
    # 5. WEEK RANGES
    # ========================================
    
    elif date_range == "this_week":
        monday = today - timedelta(days=today.weekday())
        sunday = monday + timedelta(days=6)
        start = monday.strftime("%Y%m%d")
        end = sunday.strftime("%Y%m%d")
    
    elif date_range == "last_week":
        last_monday = today - timedelta(days=today.weekday() + 7)
        last_sunday = last_monday + timedelta(days=6)
        start = last_monday.strftime("%Y%m%d")
        end = last_sunday.strftime("%Y%m%d")
    
    # ========================================
    # 6. DAY RANGES
    # ========================================
    
    elif date_range == "today":
        start = end = today.strftime("%Y%m%d")
    
    elif date_range == "yesterday":
        y = today - timedelta(days=1)
        start = end = y.strftime("%Y%m%d")
    
    # ========================================
    # 7. MONTH RANGES
    # ========================================
    
    elif date_range == "this_month":
        start = _first_day_of_month(year, month)
        end = _last_day_of_month(year, month)
    
    elif date_range == "last_month":
        prev = (today.replace(day=1) - timedelta(days=1))
        start = _first_day_of_month(prev.year, prev.month)
        end = _last_day_of_month(prev.year, prev.month)
    
    # ========================================
    # 8. FINANCIAL QUARTER RANGES
    # ========================================
    
    elif date_range in ("this_quarter", "last_quarter", "qtd"):
        # Calculate FY year and month
        if month >= 4:
            fy_year = year
            fy_month = month
        else:
            fy_year = year - 1
            fy_month = month + 12
        
        # Calculate quarter index (0-3)
        q_index = (fy_month - 4) // 3
        
        if date_range == "last_quarter":
            q_index -= 1
        
        # Handle wrap-around
        if q_index < 0:
            q_index += 4
            fy_year -= 1
        
        # Calculate start month
        start_month = 4 + q_index * 3
        start_year = fy_year
        if start_month > 12:
            start_month -= 12
            start_year += 1
        
        # Calculate end month
        end_month = start_month + 2
        end_year = start_year
        if end_month > 12:
            end_month -= 12
            end_year += 1
        
        start = _first_day_of_month(start_year, start_month)
        
        if date_range == "qtd":
            end = today.strftime("%Y%m%d")
        else:
            end = _last_day_of_month(end_year, end_month)
    
    # ========================================
    # 9. TO-DATE RANGES
    # ========================================
    
    elif date_range == "mtd":
        start = _first_day_of_month(year, month)
        end = today.strftime("%Y%m%d")
    
    elif date_range == "ytd":
        start = f"{year}0101"
        end = today.strftime("%Y%m%d")
    
    # ========================================
    # 10. ROLLING WINDOWS
    # ========================================
    
    elif date_range.startswith("rolling_"):
        days_map = {
            "rolling_7_days": 7,
            "rolling_14_days": 14,
            "rolling_15_days": 15,
            "rolling_30_days": 30,
            "rolling_60_days": 60,
            "rolling_90_days": 90,
            "rolling_6_months": 180,
            "rolling_12_months": 365,
        }
        days = days_map.get(date_range, 30)
        start = (today - timedelta(days=days - 1)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
    
    
    

    # ========================================
    # 14. SPECIFIC FINANCIAL YEAR (e.g., "fy_2024")
    # ========================================
    
    elif date_range.startswith("fy_"):
        try:
            target_year = int(date_range.split("_")[1])
        except:
            # Fallback to current FY
            target_year = year if month >= 4 else year - 1
        
        start = f"{target_year}0401"
        end = f"{target_year + 1}0331"
        
        print(f"[DATE_RESOLVER] Specific FY {target_year}: {start} to {end}")
    
    # ========================================
    # 15. SPECIFIC QUARTER (e.g., "q1", "q2")
    # ========================================
    
    elif date_range in ("q1", "q2", "q3", "q4"):
        # Get current financial year
        curr_fy_start = year if month >= 4 else year - 1
        
        # Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
        quarter_map = {
            "q1": (4, 6),   # Apr-Jun
            "q2": (7, 9),   # Jul-Sep  
            "q3": (10, 12), # Oct-Dec
            "q4": (1, 3)    # Jan-Mar (next calendar year)
        }
        
        start_month, end_month = quarter_map[date_range]
        
        if date_range == "q4":
            # Q4 spans into next calendar year
            start_year = curr_fy_start
            end_year = curr_fy_start + 1
        else:
            start_year = curr_fy_start
            end_year = curr_fy_start
        
        start = _first_day_of_month(start_year, start_month)
        end = _last_day_of_month(end_year, end_month)
        
        print(f"[DATE_RESOLVER] {date_range.upper()}: {start} to {end}")
    

    # ========================================
    # 11. MULTI-YEAR COMPARISONS
    # ========================================
    
    elif date_range == "last_3_financial_years_yoy":
        # Include current FY (partial) so YoY shows current year alongside past years.
        # End = today so current FY data up to now is visible.
        # NOTE: Use 4 past FYs so "year on year" includes the full FY that began 4 years ago.
        curr_fy_start = year if month >= 4 else year - 1
        start_year = curr_fy_start - 4
        start = f"{start_year}0401"
        end = today.strftime("%Y%m%d")
        print(f"[DATE_RESOLVER] last_3_financial_years_yoy (inc. current FY): {start} to {end}")

    # ========================================
    # 12. DYNAMIC HANDLERS (Fallback for underscore format)
    # ========================================
    # These handle cases like "last_2_financial_years" that weren't caught by pattern parser
    
    elif date_range.startswith("last_") and date_range.endswith("_financial_years"):
        try:
            n = int(date_range.split("_")[1])
        except:
            n = 3

        curr_fy_start = year if month >= 4 else year - 1
        start_year = curr_fy_start - n

        # End = Mar 31 of last completed FY (excludes current partial FY)
        start = f"{start_year}0401"
        end = f"{curr_fy_start}0331"

        print(f"[DATE_RESOLVER] {date_range} (complete FYs only): {start} to {end}")
    
    elif date_range.startswith("last_") and date_range.endswith("_quarters"):
        try:
            n = int(date_range.split("_")[1])
        except:
            n = 2
        
        if month >= 4:
            fy_year = year
            fy_month = month
        else:
            fy_year = year - 1
            fy_month = month + 12
        
        current_q = (fy_month - 4) // 3
        start_q = current_q - n
        
        while start_q < 0:
            start_q += 4
            fy_year -= 1
        
        start_month = 4 + start_q * 3
        start_year = fy_year
        if start_month > 12:
            start_month -= 12
            start_year += 1
        
        end_offset = n * 3 - 1
        end_month = start_month + end_offset
        end_year = start_year
        
        while end_month > 12:
            end_month -= 12
            end_year += 1
        
        start = _first_day_of_month(start_year, start_month)
        end = _last_day_of_month(end_year, end_month)
        
        print(f"[DATE_RESOLVER] {date_range}: {start} to {end}")
    
    elif date_range.startswith("last_") and date_range.endswith("_months"):
        try:
            n = int(date_range.split("_")[1])
        except:
            n = 2
        
        prev_month_date = today.replace(day=1) - timedelta(days=1)
        end_month = prev_month_date.month
        end_year = prev_month_date.year
        
        start_date = prev_month_date
        for _ in range(n - 1):
            start_date = start_date.replace(day=1) - timedelta(days=1)
        
        start_month = start_date.month
        start_year = start_date.year
        
        start = _first_day_of_month(start_year, start_month)
        end = _last_day_of_month(end_year, end_month)
        
        print(f"[DATE_RESOLVER] {date_range}: {start} to {end}")
    
    
    # ==================================================
    # elif date_range.startswith("fy_"):
    #     try:
    #         target_year = int(date_range.split("_")[1])
    #     except:
    #         target_year = year if month >= 4 else year - 1
        
    #     start = f"{target_year}0401"
    #     end = f"{target_year + 1}0331"
        
    #     print(f"[DATE_RESOLVER] Specific FY {target_year}: {start} to {end}")
    
    # elif date_range in ("q1", "q2", "q3", "q4"):
    #     curr_fy_start = year if month >= 4 else year - 1
        
    #     quarter_map = {
    #         "q1": (4, 6),
    #         "q2": (7, 9),
    #         "q3": (10, 12),
    #         "q4": (1, 3)
    #     }
        
    #     start_month, end_month = quarter_map[date_range]
        
    #     if date_range == "q4":
    #         start_year = curr_fy_start
    #         end_year = curr_fy_start + 1
    #     else:
    #         start_year = curr_fy_start
    #         end_year = curr_fy_start
        
    #     start = _first_day_of_month(start_year, start_month)
    #     end = _last_day_of_month(end_year, end_month)
        
    #     print(f"[DATE_RESOLVER] {date_range.upper()}: {start} to {end}")



    # # ========================================
    # # 13. UNKNOWN → DEFAULT TO CURRENT FY
    # # ========================================
    
    # else:
    #     print(f"[WARNING] Unknown date range '{date_range}', defaulting to current_financial_year")
    #     start_year = year if month >= 4 else year - 1
    #     end_year = start_year + 1
    #     start = f"{start_year}0401"
    #     end = f"{end_year}0331"
    
    # ========================================
    # 13. SPECIFIC FINANCIAL YEAR (e.g., "fy_2024")
    # ========================================
    
    elif date_range.startswith("fy_"):
        try:
            target_year = int(date_range.split("_")[1])
        except:
            target_year = year if month >= 4 else year - 1
        
        start = f"{target_year}0401"
        end = f"{target_year + 1}0331"
        
        print(f"[DATE_RESOLVER] Specific FY {target_year}: {start} to {end}")
    
    # ========================================
    # 14. SPECIFIC QUARTER (e.g., "q1", "q2", "q3", "q4")
    # ========================================
    
    elif date_range in ("q1", "q2", "q3", "q4"):
        # Get current financial year
        curr_fy_start = year if month >= 4 else year - 1
        
        # Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
        quarter_map = {
            "q1": (4, 6),
            "q2": (7, 9),
            "q3": (10, 12),
            "q4": (1, 3)
        }
        
        start_month, end_month = quarter_map[date_range]
        
        if date_range == "q4":
            start_year = curr_fy_start
            end_year = curr_fy_start + 1
        else:
            start_year = curr_fy_start
            end_year = curr_fy_start
        
        start = _first_day_of_month(start_year, start_month)
        end = _last_day_of_month(end_year, end_month)
        
        print(f"[DATE_RESOLVER] {date_range.upper()}: {start} to {end}")
    
    # ========================================
    # 15. UNKNOWN → DEFAULT TO CURRENT FY
    # ========================================
    
    else:
        print(f"[WARNING] Unknown date range '{date_range}', defaulting to current_financial_year")
        start_year = year if month >= 4 else year - 1
        end_year = start_year + 1
        start = f"{start_year}0401"
        end = f"{end_year}0331"

    # ========================================
    # BUILD SQL CLAUSE
    # ========================================
    
    return (
        f"{col_sql} BETWEEN "
        f"DATE_PARSE('{start}', '%Y%m%d') AND DATE_PARSE('{end}', '%Y%m%d')"
    )