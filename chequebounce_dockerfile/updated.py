from __future__ import annotations
from fastapi import FastAPI
from pydantic import BaseModel
import prestodb
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import Credentials
import re
from datetime import date, timedelta
from calendar import monthrange
from dotenv import load_dotenv
import os
# --------------------
# Load Environment Variables
# --------------------
load_dotenv()

CATALOG = os.getenv("PRESTO_CATALOG")
SCHEMA = os.getenv("PRESTO_SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME")

username = os.getenv("PRESTO_USERNAME")
password = os.getenv("PRESTO_PASSWORD")
hostname = os.getenv("PRESTO_HOST")
portnumber = int(os.getenv("PRESTO_PORT", "30984"))

# Watsonx credentials
creds = Credentials(
    url=os.getenv("WATSONX_URL", "https://us-south.ml.cloud.ibm.com"),
    api_key=os.getenv("WATSONX_API_KEY"),
)

model = ModelInference(
    model_id=os.getenv("WATSONX_MODEL_ID", "meta-llama/llama-3-3-70b-instruct"),
    credentials=creds,
    project_id=os.getenv("WATSONX_PROJECT_ID"),
    params={"temperature": 0, "max_new_tokens": 3000}
)

# --------------------
# Helper: Financial Year
# --------------------
def get_financial_year_range(today: date = None):
    if today is None:
        today = date.today()
    year = today.year
    if today.month < 4:
        start_year = year - 1
    else:
        start_year = year
    start_date = date(start_year, 4, 1)
    end_date = date(start_year + 1, 3, 31)
    return start_date, end_date


def insert_where_before_groupby(sql: str, clause: str) -> str:
    """Safely insert WHERE/AND clause before GROUP BY, ORDER BY, or LIMIT."""
    sql_upper = sql.upper()

    if "GROUP BY" in sql_upper:
        parts = re.split(r"(?i)\bGROUP BY\b", sql, maxsplit=1)
        before, after = parts[0], parts[1]
        if "WHERE" in before.upper():
            return before + " AND " + clause + " GROUP BY " + after
        else:
            return before + " WHERE " + clause + " GROUP BY " + after

    if "ORDER BY" in sql_upper:
        parts = re.split(r"(?i)\bORDER BY\b", sql, maxsplit=1)
        before, after = parts[0], parts[1]
        if "WHERE" in before.upper():
            return before + " AND " + clause + " ORDER BY " + after
        else:
            return before + " WHERE " + clause + " ORDER BY " + after

    if "LIMIT" in sql_upper:
        parts = re.split(r"(?i)\bLIMIT\b", sql, maxsplit=1)
        before, after = parts[0], parts[1]
        if "WHERE" in before.upper():
            return before + " AND " + clause + " LIMIT " + after
        else:
            return before + " WHERE " + clause + " LIMIT " + after

    # Default → just append
    if "WHERE" in sql_upper:
        return sql + " AND " + clause
    else:
        return sql + " WHERE " + clause



def convert_word_numbers_to_digits(text: str) -> str:
    """
    Convert word numbers (one, two, three, etc.) to digits in queries.
    Examples: "last two months" → "last 2 months"
              "last six quarters" → "last 6 quarters"
    """
    word_to_num = {
        'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
        'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10',
        'eleven': '11', 'twelve': '12', 'thirteen': '13', 'fourteen': '14',
        'fifteen': '15', 'sixteen': '16', 'seventeen': '17', 'eighteen': '18',
        'nineteen': '19', 'twenty': '20'
    }
    
    # Pattern to match "last/past/previous <word_number> days/months/quarters/years"
    pattern = r'\b(last|past|previous)\s+(' + '|'.join(word_to_num.keys()) + r')\s+(days?|months?|quarters?|years?)\b'
    
    def replace_func(match):
        prefix = match.group(1)
        word = match.group(2)
        unit = match.group(3)
        digit = word_to_num[word]
        return f"{prefix} {digit} {unit}"
    
    return re.sub(pattern, replace_func, text, flags=re.IGNORECASE)




def detect_comparison_query(user_query: str) -> dict:
    """
    Detects if the query is a comparison (v/s) query.
    
    Returns:
        dict with:
        - is_comparison: bool
        - comparison_type: 'year', 'quarter', 'month', 'day', etc.
        - periods: list of period definitions
    """
    from datetime import date
    query_lower = user_query.lower()
    
    result = {
        'is_comparison': False,
        'comparison_type': None,
        'periods': []
    }
    
    # Check for v/s, vs, versus, compared to, comparison
    if not re.search(r'\b(v/s|vs|versus|compared to|comparison|compare| and)\b', query_lower):
        return result
    
    result['is_comparison'] = True
    
    # ========== CHECK FOR DIMENSIONS ==========
    # Check if query asks for project-wise/company-wise breakdown
    if re.search(r'\b(project\s*wise|company\s*wise|company\s*code\s*wise)\b', query_lower):
        result['is_project_wise'] = True
    
    # Check for specific project
    if 'wave city' in query_lower:
        result['specific_project'] = 'wave city'
    elif 'wmcc' in query_lower:
        result['specific_project'] = 'wmcc'
    elif 'wave estate' in query_lower:
        result['specific_project'] = 'wave estate'
    
    # Check for customer-wise
    if re.search(r'\b(customer\s*wise|by\s+customer)\b', query_lower):
        result['is_customer_wise'] = True
    
    # Check for sales group wise
    if re.search(r'\b(sales\s+group\s*wise|product\s*wise)\b', query_lower):
        result['is_sales_group_wise'] = True
    
    # Check for bank wise
    if re.search(r'\bbank\s*wise\b', query_lower):
        result['is_bank_wise'] = True
 
    # ========== PRIORITY 1: SPECIFIC DAYS (16 SEP V/S 18 SEP) ==========

    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
    }
    
    # ⭐⭐⭐ CRITICAL FIX: More flexible pattern matching
    day_vs_match = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*'
        r'(?:v/s|vs\.?|versus| and)\s*'
        r'(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        query_lower
    )
    
    if day_vs_match:
        d1 = int(day_vs_match.group(1))
        m1_str = day_vs_match.group(2)
        d2 = int(day_vs_match.group(3))
        m2_str = day_vs_match.group(4)
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        # Check for explicit year
        year_match = re.search(r'\b(20\d{2})\b', user_query)
        
        if year_match:
            year = int(year_match.group(1))
            y1 = year
            y2 = year
        else:
            # Infer year using FY logic
            from datetime import date
            today = date.today()
            fy_start_year = today.year if today.month >= 4 else today.year - 1
            
            y1 = fy_start_year if m1 >= 4 else fy_start_year + 1
            y2 = fy_start_year if m2 >= 4 else fy_start_year + 1
        
        try:
            date1 = date(y1, m1, d1)
            date2 = date(y2, m2, d2)
            
            result['comparison_type'] = 'day'
            result['periods'] = [
                {
                    'name': f'{d1} {m1_str.capitalize()}',
                    'start_date': date1,
                    'end_date': date1
                },
                {
                    'name': f'{d2} {m2_str.capitalize()}',
                    'start_date': date2,
                    'end_date': date2
                }
            ]
            return result
        except ValueError:
            # Invalid date, continue to other patterns
            pass


    # ========== THIS YEAR V/S LAST YEAR ==========
    # ========== THIS YEAR V/S LAST YEAR - BIDIRECTIONAL PATTERN ==========
    # ⭐⭐⭐ CRITICAL FIX: Check BOTH orders!
    if (re.search(r'\bthis year\b.*\b(v/s|vs|versus| and)\b.*\blast year\b', query_lower) or 
        re.search(r'\blast year\b.*\b(v/s|vs|versus| and)\b.*\bthis year\b', query_lower)):
        
        fy_start, fy_end = get_financial_year_range()
        
        # This year (current FY)
        this_fy_start = fy_start
        this_fy_end = fy_end
        
        # Last year (previous FY)
        last_fy_start = date(fy_start.year - 1, 4, 1)
        last_fy_end = date(fy_start.year, 3, 31)
        
        result['comparison_type'] = 'year'
        
        # ⭐⭐⭐ IMPORTANT: Always return in consistent order
        # Determine which one comes first in the query
        this_year_pos = query_lower.find('this year')
        last_year_pos = query_lower.find('last year')
        
        if this_year_pos < last_year_pos:
            # "this year v/s last year"
            result['periods'] = [
                {
                    'name': 'This Year',
                    'start_date': this_fy_start,
                    'end_date': this_fy_end
                },
                {
                    'name': 'Last Year',
                    'start_date': last_fy_start,
                    'end_date': last_fy_end
                }
            ]
        else:
            # "last year v/s this year"
            result['periods'] = [
                {
                    'name': 'Last Year',
                    'start_date': last_fy_start,
                    'end_date': last_fy_end
                },
                {
                    'name': 'This Year',
                    'start_date': this_fy_start,
                    'end_date': this_fy_end
                }
            ]
        
        return result
    
    # ========== THIS QUARTER V/S LAST QUARTER - BIDIRECTIONAL ==========
    if (re.search(r'\bthis quarter\b.*\b(v/s|vs|versus| and)\b.*\blast quarter\b', query_lower) or
        re.search(r'\blast quarter\b.*\b(v/s|vs|versus| and)\b.*\bthis quarter\b', query_lower)):
        
        today = date.today()
        
        def quarter_start_end(d):
            month = d.month
            year = d.year
            if month in [4, 5, 6]:   # Q1
                return date(year, 4, 1), date(year, 6, 30)
            elif month in [7, 8, 9]: # Q2
                return date(year, 7, 1), date(year, 9, 30)
            elif month in [10, 11, 12]: # Q3
                return date(year, 10, 1), date(year, 12, 31)
            else:  # Jan-Mar -> Q4
                return date(year, 1, 1), date(year, 3, 31)
        
        def last_quarter_start_end(d):
            month = d.month
            year = d.year
            
            if month in [4, 5, 6]:   # Currently Q1 → Last quarter was Q4
                return date(year, 1, 1), date(year, 3, 31)
            elif month in [7, 8, 9]: # Currently Q2 → Last quarter was Q1
                return date(year, 4, 1), date(year, 6, 30)
            elif month in [10, 11, 12]: # Currently Q3 → Last quarter was Q2
                return date(year, 7, 1), date(year, 9, 30)
            else:  # Currently Q4 → Last quarter was Q3
                return date(year - 1, 10, 1), date(year - 1, 12, 31)
        
        this_q_start, this_q_end = quarter_start_end(today)
        last_q_start, last_q_end = last_quarter_start_end(today)
        
        result['comparison_type'] = 'quarter'
        
        # Maintain order from query
        this_q_pos = query_lower.find('this quarter')
        last_q_pos = query_lower.find('last quarter')
        
        if this_q_pos < last_q_pos:
            result['periods'] = [
                {'name': 'This Quarter', 'start_date': this_q_start, 'end_date': this_q_end},
                {'name': 'Last Quarter', 'start_date': last_q_start, 'end_date': last_q_end}
            ]
        else:
            result['periods'] = [
                {'name': 'Last Quarter', 'start_date': last_q_start, 'end_date': last_q_end},
                {'name': 'This Quarter', 'start_date': this_q_start, 'end_date': this_q_end}
            ]
        
        return result
    
    # ========== THIS MONTH V/S LAST MONTH - BIDIRECTIONAL ==========
    if (re.search(r'\bthis month\b.*\b(v/s|vs|versus| and)\b.*\blast month\b', query_lower) or
        re.search(r'\blast month\b.*\b(v/s|vs|versus| and)\b.*\bthis month\b', query_lower)):
        
        today = date.today()
        
        # This month
        month_start = date(today.year, today.month, 1)
        month_end = (date(today.year, today.month + 1, 1) - timedelta(days=1)) if today.month != 12 else date(today.year, 12, 31)
        
        # Last month
        if today.month == 1:
            last_month_start = date(today.year - 1, 12, 1)
            last_month_end = date(today.year - 1, 12, 31)
        else:
            last_month_start = date(today.year, today.month - 1, 1)
            last_month_end = date(today.year, today.month, 1) - timedelta(days=1)
        
        result['comparison_type'] = 'month'
        
        # Maintain order
        this_m_pos = query_lower.find('this month')
        last_m_pos = query_lower.find('last month')
        
        if this_m_pos < last_m_pos:
            result['periods'] = [
                {'name': 'This Month', 'start_date': month_start, 'end_date': month_end},
                {'name': 'Last Month', 'start_date': last_month_start, 'end_date': last_month_end}
            ]
        else:
            result['periods'] = [
                {'name': 'Last Month', 'start_date': last_month_start, 'end_date': last_month_end},
                {'name': 'This Month', 'start_date': month_start, 'end_date': month_end}
            ]
        
        return result

    
    # ========== SPECIFIC QUARTERS (Q1 V/S Q2, etc.) ==========
    quarter_vs_match = re.search(
        r'\bq([1-4])\b.*\b(v/s|vs|versus| and)\b.*\bq([1-4])\b',
        query_lower
    )
    
    if quarter_vs_match:
        q1 = int(quarter_vs_match.group(1))
        q2 = int(quarter_vs_match.group(3))
        
        # Check for year specification
        fy_match = re.search(r'\bfy\s*(20\d{2})\b', query_lower, re.IGNORECASE)
        year_match = re.search(r'\b(20\d{2})\b', user_query)
        
        if fy_match:
            fy_year = int(fy_match.group(1))
        elif year_match:
            fy_year = int(year_match.group(1))
        else:
            fy_year = get_financial_year_range()[0].year
        
        def get_quarter_dates(q, fy):
            if q == 1:
                return date(fy, 4, 1), date(fy, 6, 30)
            elif q == 2:
                return date(fy, 7, 1), date(fy, 9, 30)
            elif q == 3:
                return date(fy, 10, 1), date(fy, 12, 31)
            else:  # Q4
                return date(fy + 1, 1, 1), date(fy + 1, 3, 31)
        
        q1_start, q1_end = get_quarter_dates(q1, fy_year)
        q2_start, q2_end = get_quarter_dates(q2, fy_year)
        
        result['comparison_type'] = 'quarter'
        result['periods'] = [
            {
                'name': f'Q{q1}',
                'start_date': q1_start,
                'end_date': q1_end
            },
            {
                'name': f'Q{q2}',
                'start_date': q2_start,
                'end_date': q2_end
            }
        ]
        return result
    
    # ========== SPECIFIC MONTHS (APRIL V/S MAY, etc.) ==========
    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
    }
    
    month_vs_match = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b.*\b(v/s|vs|versus)\b.*\b'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        query_lower
    )
    
    if month_vs_match:
        m1_str = month_vs_match.group(1)
        m2_str = month_vs_match.group(3)
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        # Check for year specification
        year_match = re.search(r'\b(20\d{2})\b', user_query)
        
        if year_match:
            year = int(year_match.group(1))
        else:
            # Use FY logic
            fy_start, fy_end = get_financial_year_range()
            year = fy_start.year
        
        # Month 1 dates
        m1_year = year if m1 >= 4 else year + 1
        m1_last_day = monthrange(m1_year, m1)[1]
        m1_start = date(m1_year, m1, 1)
        m1_end = date(m1_year, m1, m1_last_day)
        
        # Month 2 dates
        m2_year = year if m2 >= 4 else year + 1
        m2_last_day = monthrange(m2_year, m2)[1]
        m2_start = date(m2_year, m2, 1)
        m2_end = date(m2_year, m2, m2_last_day)
        
        result['comparison_type'] = 'month'
        result['periods'] = [
            {
                'name': m1_str.capitalize(),
                'start_date': m1_start,
                'end_date': m1_end
            },
            {
                'name': m2_str.capitalize(),
                'start_date': m2_start,
                'end_date': m2_end
            }
        ]
        return result
    
    # ========== SPECIFIC YEARS (2024 V/S 2025, etc.) ==========
    year_vs_match = re.search(
        r'\b(20\d{2})\b.*\b(v/s|vs|versus)\b.*\b(20\d{2})\b',
        user_query
    )
    
    if year_vs_match:
        y1 = int(year_vs_match.group(1))
        y2 = int(year_vs_match.group(3))
        
        y1_start = date(y1, 4, 1)
        y1_end = date(y1 + 1, 3, 31)
        
        y2_start = date(y2, 4, 1)
        y2_end = date(y2 + 1, 3, 31)
        
        result['comparison_type'] = 'year'
        result['periods'] = [
            {
                'name': f'FY {y1}',
                'start_date': y1_start,
                'end_date': y1_end
            },
            {
                'name': f'FY {y2}',
                'start_date': y2_start,
                'end_date': y2_end
            }
        ]
        return result
    
    # ========== SPECIFIC DAYS (16 SEP V/S 18 SEP, etc.) ==========
    day_vs_match = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
        r'jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b.*'
        r'\b(v/s|vs|versus)\b.*\b(\d{1,2})(?:st|nd|rd|th)?\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        query_lower
    )
    
    if day_vs_match:
        d1 = int(day_vs_match.group(1))
        m1_str = day_vs_match.group(2)
        d2 = int(day_vs_match.group(4))
        m2_str = day_vs_match.group(5)
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        # Infer year
        fy_start, fy_end = get_financial_year_range()
        y1 = fy_start.year if m1 >= 4 else fy_end.year
        y2 = fy_start.year if m2 >= 4 else fy_end.year
        
        date1 = date(y1, m1, d1)
        date2 = date(y2, m2, d2)
        
        result['comparison_type'] = 'day'
        result['periods'] = [
            {
                'name': f'{d1} {m1_str.capitalize()}',
                'start_date': date1,
                'end_date': date1
            },
            {
                'name': f'{d2} {m2_str.capitalize()}',
                'start_date': date2,
                'end_date': date2
            }
        ]
        return result
    
    return result


# def build_comparison_query(user_query: str, comparison_info: dict, CATALOG: str, SCHEMA: str, TABLE_NAME: str) -> str:
#     """
#     Builds a UNION ALL query for comparison results.
#     """
#     if not comparison_info['is_comparison']:
#         return None
    
#     # Base amount expression
#     amount_expr = """ABS(
#         SUM(
#             CAST(
#                 CASE
#                     WHEN TRIM("Gross_amount") LIKE '%-' 
#                     THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
#                     ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
#                 END AS DOUBLE
#             )
#         )
#     )"""
    
#     queries = []
    
#     # ========== CHECK FOR CUSTOMER-WISE ==========
#     is_customer_wise = re.search(r'\bcustomer\s*wise\b|\bby\s+customer\b', user_query.lower())
    
#     # ========== CHECK FOR SALES GROUP WISE ==========
#     is_sales_group_wise = re.search(r'\bsales\s+group\s*wise\b|\bproduct\s*wise\b', user_query.lower())
    
#     # ========== CHECK FOR COMPANY WISE ==========
#     is_company_wise = re.search(r'\bcompany\s*wise\b|\bproject\s*wise\b', user_query.lower())
    
#     # ========== CHECK FOR BANK WISE ==========
#     is_bank_wise = re.search(r'\bbank\s*wise\b', user_query.lower())
    
#     for period in comparison_info['periods']:
#         period_name = period['name']
#         start_date = period['start_date']
#         end_date = period['end_date']
        
#         # Build base query
#         if is_customer_wise:
#             # Customer-wise breakdown
#             query = f"""SELECT 
#     '{period_name}' AS period,
#     "Cust_no",
#     "Cust_name",
#     {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
# GROUP BY "Cust_no", "Cust_name"
# ORDER BY chequebounce DESC"""
        
#         elif is_sales_group_wise:
#             # Sales group wise breakdown
#             query = f"""SELECT 
#     '{period_name}' AS period,
#     "sales_grp_descp",
#     {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
# GROUP BY "sales_grp_descp"
# ORDER BY chequebounce DESC"""
        
#         elif is_company_wise:
#             # Company wise breakdown
#             query = f"""SELECT 
#     '{period_name}' AS period,
#     "Comp_code",
#     {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
# GROUP BY "Comp_code"
# ORDER BY chequebounce DESC"""
        
#         elif is_bank_wise:
#             # Bank wise breakdown
#             query = f"""SELECT 
#     '{period_name}' AS period,
#     "Bank_name",
#     {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
# GROUP BY "Bank_name"
# ORDER BY chequebounce DESC"""
        
#         else:
#             # Simple total
#             query = f"""SELECT 
#     '{period_name}' AS period,
#     {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
        
#         queries.append(query)
    
#     if queries:
#         return "\nUNION ALL\n".join(queries)
    
#     return None

def build_comparison_query(user_query: str, comparison_info: dict, CATALOG: str, SCHEMA: str, TABLE_NAME: str) -> str:
    """
    Builds a UNION ALL query for comparison results.
    Enhanced to support project-wise, customer-wise, sales-group-wise, and bank-wise comparisons.
    """
    if not comparison_info['is_comparison']:
        return None
    
    # Base amount expression
    amount_expr = """ABS(
        SUM(
            CAST(
                CASE
                    WHEN TRIM("Gross_amount") LIKE '%-' 
                    THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
                    ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
                END AS DOUBLE
            )
        )
    )"""
    
    queries = []
    
    # ========== DETERMINE BREAKDOWN TYPE ==========
    is_customer_wise = comparison_info.get('is_customer_wise', False) or \
                       re.search(r'\bcustomer\s*wise\b|\bby\s+customer\b', user_query.lower())
    
    is_sales_group_wise = comparison_info.get('is_sales_group_wise', False) or \
                          re.search(r'\bsales\s+group\s*wise\b|\bproduct\s*wise\b', user_query.lower())
    
    is_company_wise = comparison_info.get('is_project_wise', False) or \
                      re.search(r'\bcompany\s*wise\b|\bproject\s*wise\b', user_query.lower())
    
    is_bank_wise = comparison_info.get('is_bank_wise', False) or \
                   re.search(r'\bbank\s*wise\b', user_query.lower())
    
    specific_project = comparison_info.get('specific_project', None)
    
    # ========== BUILD QUERIES FOR EACH PERIOD ==========
    for period in comparison_info['periods']:
        period_name = period['name']
        start_date = period['start_date']
        end_date = period['end_date']
        
        # ========== CASE 1: SPECIFIC PROJECT ==========
        if specific_project:
            comp_code_map = {
                'wave city': 1000,
                'wmcc': 1100,
                'wave estate': 1300
            }
            comp_code = comp_code_map.get(specific_project)
            
            if comp_code:
                # ⭐⭐⭐ REMOVED ORDER BY
                query = f"""SELECT 
    '{period_name}' AS period,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE "Comp_code" = {comp_code}
  AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
                
                queries.append(query)
        
        # ========== CASE 2: PROJECT-WISE / COMPANY-WISE ==========
        elif is_company_wise:
            # ⭐⭐⭐ REMOVED ORDER BY
            query = f"""SELECT 
    '{period_name}' AS period,
    "Comp_code",
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY "Comp_code\""""
            
            queries.append(query)
        
        # ========== CASE 3: CUSTOMER-WISE ==========
        elif is_customer_wise:
            # ⭐⭐⭐ REMOVED ORDER BY
            query = f"""SELECT 
    '{period_name}' AS period,
    "Cust_no",
    "Cust_name",
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY "Cust_no", "Cust_name\""""
            
            queries.append(query)
        
        # ========== CASE 4: SALES GROUP-WISE ==========
        elif is_sales_group_wise:
            # ⭐⭐⭐ REMOVED ORDER BY
            query = f"""SELECT 
    '{period_name}' AS period,
    "sales_grp_descp",
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY "sales_grp_descp\""""
            
            queries.append(query)
        
        # ========== CASE 5: BANK-WISE ==========
        elif is_bank_wise:
            # ⭐⭐⭐ REMOVED ORDER BY
            query = f"""SELECT 
    '{period_name}' AS period,
    "Bank_name",
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY "Bank_name\""""
            
            queries.append(query)
        
        # ========== CASE 6: SIMPLE TOTAL ==========
        else:
            query = f"""SELECT 
    '{period_name}' AS period,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
    
    # ⭐⭐⭐ ADD SINGLE ORDER BY AT THE END
    if queries:
        full_query = "\nUNION ALL\n".join(queries)
        
        # Add ORDER BY based on query type
        if is_company_wise or is_customer_wise or is_sales_group_wise or is_bank_wise:
            # For breakdown queries, order by period first, then amount
            full_query += "\nORDER BY period, chequebounce DESC"
        else:
            # For simple total, just order by period
            full_query += "\nORDER BY period"
        
        return full_query
    
    return None

def get_financial_year_range(today: date = None):
    """Helper to get FY range."""
    if today is None:
        today = date.today()
    year = today.year
    if today.month < 4:
        start_year = year - 1
    else:
        start_year = year
    start_date = date(start_year, 4, 1)
    end_date = date(start_year + 1, 3, 31)
    return start_date, end_date


def enforce_financial_year(sql: str, user_query: str) -> str:
    """
    Apply correct financial year, quarter, month, or YoY rules to SQL query.
    Removes any existing DATE_PARSE BETWEEN clauses before inserting the new one.
    """
    
     # ⭐⭐⭐ CRITICAL FIX: Convert word numbers to digits FIRST
    user_query = convert_word_numbers_to_digits(user_query)

    today = date.today()
    # Compute current FY using your helper
    fy_start, fy_end = get_financial_year_range(today)
    
    
    # 🔹 CRITICAL: Strip ANY existing date filters from the model-generated SQL
    # This prevents duplicate/conflicting WHERE conditions
    
    # Remove BETWEEN clauses
    sql = re.sub(
        r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+BETWEEN\s+DATE\s+\'[0-9\-]+\'\s+AND\s+DATE\s+\'[0-9\-]+\'',
        r'\1 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    
    # ⭐⭐⭐ NEW: Remove <= DATE conditions (for "till" queries)
    sql = re.sub(
        r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s*<=\s*DATE\s+\'[0-9\-]+\'',
        r'\1 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    
    # ⭐⭐⭐ NEW: Remove >= DATE conditions
    sql = re.sub(
        r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s*>=\s*DATE\s+\'[0-9\-]+\'',
        r'\1 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    
    # ⭐⭐⭐ NEW: Remove = DATE conditions (for exact date queries)
    sql = re.sub(
        r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s*=\s*DATE\s+\'[0-9\-]+\'',
        r'\1 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    
    # ---------- MONTH NAMES ----------
    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
    }
    
    # ========== FIXED: "last N years" (e.g., "last 2 years") ==========
    last_n_years = re.search(
        r'\b(?:last|past|previous)\s+(\d{1,2})\s+years?\b',
        user_query,
        re.IGNORECASE
    )
    
    if last_n_years:
        n = int(last_n_years.group(1))
        if n > 0:
            # ⭐⭐⭐ CRITICAL FIX: Correct calculation for last N years
            # Today: Jan 2, 2026 → Current FY: 2025-26 (Apr 2025 - Mar 2026)
            # Last completed FY: 2024-25 (Apr 2024 - Mar 2025)
            # Last 2 years should cover: FY 2023-24 AND FY 2024-25
            
            # End date = end of LAST COMPLETED FY
            end_date = date(fy_start.year, 3, 31)
            
            # Start date = n complete FYs before the end date
            # For last 2 years: go back 2 years from end_date year
            start_date = date(end_date.year - n, 4, 1)
            
            # Example: 
            # end_date = 2025-03-31 (end of FY 2024-25)
            # n = 2
            # start_date = date(2025 - 2, 4, 1) = 2023-04-01
            # Range: 2023-04-01 to 2025-03-31 (covers FY 2023-24 and FY 2024-25)
            
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
    

    # ============= comparison queries or v/s : this year v/s last year , this quarter v/s last quarter=====


    # ========== NEW FIX 3: "till 12 december" ==========
    # Range: Start of current FY → Specified day and month
    till_day_month = re.search(
        r'\b(?:till|until|upto|up to)\s+([0-3]?\d)(?:st|nd|rd|th)?\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s+(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_day_month:
        day = int(till_day_month.group(1))
        month_str = till_day_month.group(2).lower()
        year_str = till_day_month.group(3)
        
        month_num = month_map[month_str[:3]]
        
        if year_str:
            year = int(year_str)
        else:
            # Infer year based on current FY
            year = fy_start.year if month_num >= 4 else fy_end.year
        
        try:
            end_date = date(year, month_num, day)
            
            # ⭐⭐⭐ ALWAYS start from beginning of current FY
            start_date = fy_start
            
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        except ValueError:
            pass  # Invalid date like Feb 30

    

    # ========== NEW FIX 2: "till september" (month only, no day) ==========
    till_month_only = re.search(
        r'\b(?:till|until|upto|up to)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
        r'(?!\s+\d)',  # Not followed by a day number
        user_query,
        re.IGNORECASE
    )
    
    if till_month_only:
        month_str = till_month_only.group(1).lower()
        month_num = month_map[month_str[:3]]
        
        # Infer year based on FY
        year = fy_start.year if month_num >= 4 else fy_end.year
        
        # Get last day of month
        last_day = monthrange(year, month_num)[1]
        end_date = date(year, month_num, last_day)
        start_date = fy_start
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    ########################################
    
    day_month_range = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?\s*(?:to|till|until|-)\s*'
        r'(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?',
        user_query,
        re.IGNORECASE
    )

    if day_month_range:
        d1 = int(day_month_range.group(1))
        m1 = day_month_range.group(2).lower()
        y1 = day_month_range.group(3)

        d2 = int(day_month_range.group(4))
        m2 = day_month_range.group(5).lower()
        y2 = day_month_range.group(6)

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }

        m1_num = month_map[m1[:3]]
        m2_num = month_map[m2[:3]]

        # Year inference
        if y1:
            y1 = int(y1)
        else:
            y1 = fy_start.year if m1_num >= 4 else fy_end.year

        if y2:
            y2 = int(y2)
        else:
            # If range crosses year boundary
            if (m2_num < m1_num) or (m2_num == m1_num and d2 < d1):
                y2 = y1 + 1
            else:
                y2 = y1

        try:
            start = date(y1, m1_num, d1)
            end = date(y2, m2_num, d2)

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start}' AND DATE '{end}'"
            )
            return insert_where_before_groupby(sql, clause)

        except Exception:
            pass

    


    # ========== NEW FIX 1: Single date without year (e.g., "16 sep") ==========
    # ⭐⭐⭐ UPDATED: Also handle "on 16 sep" pattern
    single_date_no_year = re.search(
        r'\b(?:on\s+)?([0-3]?\d)(?:st|nd|rd|th)?\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
        r'(?!\s+20\d{2})'  # Not followed by year
        r'(?!\s+(?:to|-|till|until))',  # ⭐ NEW: Not followed by range keywords
        user_query,
        re.IGNORECASE
)
    
    if single_date_no_year:
        day = int(single_date_no_year.group(1))
        month_str = single_date_no_year.group(2).lower()
        month_num = month_map[month_str[:3]]
        
        # Infer year based on FY
        year = fy_start.year if month_num >= 4 else fy_end.year
        
        try:
            target_date = date(year, month_num, day)
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{target_date}' AND DATE '{target_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        except ValueError:
            pass  # Invalid date, continue to other patterns

    
    

    # # ========== FIXED: "last N years" (e.g., "last 2 years") ==========
    # last_n_years = re.search(
    #     r'\b(?:last|past|previous)\s+(\d{1,2})\s+years?\b',
    #     user_query,
    #     re.IGNORECASE
    # )
    
    # if last_n_years:
    #     n = int(last_n_years.group(1))
    #     if n > 0:
    #         # ⭐⭐⭐ CRITICAL FIX: Correct calculation for last N years
    #         # Today: Jan 2, 2026 → Current FY: 2025-26 (Apr 2025 - Mar 2026)
    #         # Last completed FY: 2024-25 (Apr 2024 - Mar 2025)
    #         # Last 2 years should cover: FY 2023-24 AND FY 2024-25
            
    #         # End date = end of LAST COMPLETED FY
    #         end_date = date(fy_start.year, 3, 31)
            
    #         # Start date = n complete FYs before the end date
    #         # For last 2 years: go back 2 years from end_date year
    #         start_date = date(end_date.year - n, 4, 1)
            
    #         # Example: 
    #         # end_date = 2025-03-31 (end of FY 2024-25)
    #         # n = 2
    #         # start_date = date(2025 - 2, 4, 1) = 2023-04-01
    #         # Range: 2023-04-01 to 2025-03-31 (covers FY 2023-24 and FY 2024-25)
            
    #         clause = (
    #             f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
    #             f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    #         )
    #         return insert_where_before_groupby(sql, clause)

    

    # Pattern: "from <month> <year> to <month> <year>"
    match = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})'
        r'\s+(?:to|till|until)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )
    if match:
        m1_str = match.group(1).lower()
        y1 = int(match.group(2))
        m2_str = match.group(3).lower()
        y2 = int(match.group(4))
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        start_date = date(y1, m1, 1)
        end_date = date(y2, m2, monthrange(y2, m2)[1])
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        return insert_where_before_groupby(sql, clause)
    
    # Pattern: "from <month> to/till <month> <year>"
    match = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(?:to|till|until)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )
    if match:
        m1_str = match.group(1).lower()
        m2_str = match.group(2).lower()
        year = int(match.group(3))
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        start_date = date(year, m1, 1)
        end_date = date(year, m2, monthrange(year, m2)[1])
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        return insert_where_before_groupby(sql, clause)
    
    month_to_month = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+to\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        user_query,
        re.IGNORECASE
    )   
    if month_to_month:
        m1_str = month_to_month.group(1).lower()
        m2_str = month_to_month.group(2).lower()
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        # Infer years based on FY
        y1 = fy_start.year if m1 >= 4 else fy_end.year
        y2 = fy_start.year if m2 >= 4 else fy_end.year
        if m2 < m1:
            y2 += 1
        
        start_date = date(y1, m1, 1)
        end_day = monthrange(y2, m2)[1]
        end_date = date(y2, m2, end_day)
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    



    # Pattern: "from <month> till <day> <month> [year]"
    match = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(?:till|until|to)\s+'
        r'([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s+(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    if match:
        start_month_str = match.group(1).lower()
        end_day = int(match.group(2))
        end_month_str = match.group(3).lower()
        explicit_year = match.group(4)
        
        start_month = month_map[start_month_str[:3]]
        end_month = month_map[end_month_str[:3]]
        
        if explicit_year:
            year = int(explicit_year)
            start_year = year
            end_year = year
        else:
            start_year = fy_start.year if start_month >= 4 else fy_end.year
            end_year = fy_start.year if end_month >= 4 else fy_end.year
            if end_month < start_month:
                end_year = start_year + 1
        
        try:
            start_date = date(start_year, start_month, 1)
            end_date = date(end_year, end_month, end_day)
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            return insert_where_before_groupby(sql, clause)
        except:
            pass
    
    # Pattern: "<month> to <month> <year>" (without "from")
    match = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(?:to|till|until)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )
    if match:
        m1_str = match.group(1).lower()
        m2_str = match.group(2).lower()
        year = int(match.group(3))
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        start_date = date(year, m1, 1)
        end_date = date(year, m2, monthrange(year, m2)[1])
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        return insert_where_before_groupby(sql, clause)

    

    # ========== FIX 1: "from <month> <year>" pattern (from specific month/year till today) ==========
    from_month_year_match = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if from_month_year_match:
        month_str = from_month_year_match.group(1).lower()
        year = int(from_month_year_match.group(2))
        m = month_map[month_str[:3]]
        
        start_date = date(year, m, 1)
        
        # Check if there's "till date" or "to date"
        if re.search(r'\b(till date|to date|till today|to today)\b', user_query, re.IGNORECASE):
            end_date = today
        else:
            # Check if there's an end date specified
            to_month_year_match = re.search(
                r'\bto\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
                r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
                user_query,
                re.IGNORECASE
            )
            if to_month_year_match:
                end_month_str = to_month_year_match.group(1).lower()
                end_year = int(to_month_year_match.group(2))
                end_m = month_map[end_month_str[:3]]
                end_day = monthrange(end_year, end_m)[1]
                end_date = date(end_year, end_m, end_day)
            else:
                # Default to today
                end_date = today
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)

    # ========== FIX 2: "from <month>" (no year) pattern (till today, current FY) ==========
    from_month_match = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b(?!\s+20\d{2})',
        user_query,
        re.IGNORECASE
    )

    if from_month_match:
        month_str = from_month_match.group(1).lower()
        m = month_map[month_str[:3]]
        
        # Infer year based on FY
        y = fy_start.year if m >= 4 else fy_end.year
        
        start_date = date(y, m, 1)
        end_date = today  # Till today!
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)

    # ========== FIX 3: "from <day> <month> <year> to <day> <month> <year>" ==========
    day_month_year_range = re.search(
        r'\bfrom\s+([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})'
        r'\s*(?:to|till|until)\s*'
        r'([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if day_month_year_range:
        d1 = int(day_month_year_range.group(1))
        m1_str = day_month_year_range.group(2).lower()
        y1 = int(day_month_year_range.group(3))
        
        d2 = int(day_month_year_range.group(4))
        m2_str = day_month_year_range.group(5).lower()
        y2 = int(day_month_year_range.group(6))
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        try:
            start_date = date(y1, m1, d1)
            end_date = date(y2, m2, d2)
            
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        except Exception:
            pass

    # ========== FIX 4: "<month> <year> - <month> <year>" (e.g., "apr 2024 - oct 2025") ==========
    month_year_range = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})'
        r'\s*(?:-|to|till|until)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if month_year_range:
        m1_str = month_year_range.group(1).lower()
        y1 = int(month_year_range.group(2))
        m2_str = month_year_range.group(3).lower()
        y2 = int(month_year_range.group(4))
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        start_date = date(y1, m1, 1)
        end_day = monthrange(y2, m2)[1]
        end_date = date(y2, m2, end_day)
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)

    # ========== FIX 5: "from <year> to <year>" (e.g., "from 2024 to 2025") ==========
    year_to_year = re.search(
        r'\bfrom\s+(20\d{2})\s+to\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if year_to_year:
        y1 = int(year_to_year.group(1))
        y2 = int(year_to_year.group(2))
        
        # Indian FY: April to March
        start_date = date(y1, 4, 1)
        end_date = date(y2 + 1, 3, 31)
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)


    # 🔹 0. Strip any existing BETWEEN on POST_date added by the model
    # Handles both:
    #   WHERE DATE_PARSE(...) BETWEEN DATE 'YYYY-MM-DD' AND DATE 'YYYY-MM-DD'
    #   AND   DATE_PARSE(...) BETWEEN DATE 'YYYY-MM-DD' AND DATE 'YYYY-MM-DD'
    sql = re.sub(
        r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+BETWEEN\s+DATE\s+\'[0-9\-]+\'\s+AND\s+DATE\s+\'[0-9\-]+\'',
        r'\1 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    
    # # ---------- FY CALC ----------
    # fy_start = date(today.year if today.month >= 4 else today.year - 1, 4, 1)
    # fy_end = date(fy_start.year + 1, 3, 31)
    # ---------- MONTH NAMES ----------
    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
    }
    
    # ========== FIX 1: "from <month> <year> to <month> <year>" pattern ==========
    from_to_month_year = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})'
        r'\s+(?:to|till|until)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if from_to_month_year:
        m1_str = from_to_month_year.group(1).lower()
        y1 = int(from_to_month_year.group(2))
        m2_str = from_to_month_year.group(3).lower()
        y2 = int(from_to_month_year.group(4))
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        start_date = date(y1, m1, 1)
        end_day = monthrange(y2, m2)[1]
        end_date = date(y2, m2, end_day)
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)

    # ========== FIX 2: "from <month> till <day> <month>" (e.g., "from may till 12 dec") ==========
    from_month_till_day_month = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(?:till|until|to)\s+'
        r'([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s+(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )

    if from_month_till_day_month:
        start_month_str = from_month_till_day_month.group(1).lower()
        end_day = int(from_month_till_day_month.group(2))
        end_month_str = from_month_till_day_month.group(3).lower()
        explicit_year = from_month_till_day_month.group(4)
        
        start_month = month_map[start_month_str[:3]]
        end_month = month_map[end_month_str[:3]]
        
        # Determine year
        if explicit_year:
            year = int(explicit_year)
            start_year = year
            end_year = year
        else:
            # Use FY logic
            start_year = fy_start.year if start_month >= 4 else fy_end.year
            end_year = fy_start.year if end_month >= 4 else fy_end.year
            
            # Handle year rollover
            if end_month < start_month:
                end_year = start_year + 1
        
        try:
            start_date = date(start_year, start_month, 1)
            end_date = date(end_year, end_month, end_day)
            
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        except Exception:
            pass

    # ========== FIX 3: "from <month> to <month> <year>" (e.g., "from april to sep 2024") ==========
    from_month_to_month_year = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(?:to|till|until)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if from_month_to_month_year:
        m1_str = from_month_to_month_year.group(1).lower()
        m2_str = from_month_to_month_year.group(2).lower()
        year = int(from_month_to_month_year.group(3))
        
        m1 = month_map[m1_str[:3]]
        m2 = month_map[m2_str[:3]]
        
        start_date = date(year, m1, 1)
        end_day = monthrange(year, m2)[1]
        end_date = date(year, m2, end_day)
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    # ========== FIX 1: "from <month>" pattern (till today) ==========
    from_month_match = re.search(
        r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        user_query,
        re.IGNORECASE
    )

    if from_month_match:
        month_str = from_month_match.group(1).lower()
        m = month_map[month_str[:3]]
        
        # Infer year based on FY
        y = fy_start.year if m >= 4 else fy_end.year
        
        start_date = date(y, m, 1)
        end_date = today  # Till today!
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    

    # --- "till / until / upto <day> <month> (optional year)" ---
    till_match = re.search(
        r'\b(?:till|until|upto|up to)\s+([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )

    if till_match:
        d = int(till_match.group(1))
        month_str = till_match.group(2).lower()
        year_str = till_match.group(3)

        m = month_map[month_str[:3]]

        if year_str:
            y = int(year_str)
        else:
            y = fy_start.year if m >= 4 else fy_end.year

        try:
            end_date = date(y, m, d)
            start_date = fy_start

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

        except Exception:
            pass

    
    # ---- NEW FIXED: last N days (end at yesterday, not today) ----
    last_n_days = re.search(r'\b(?:last|past|previous)\s+(\d{1,3})\s+days?\b', user_query, re.IGNORECASE)
    if last_n_days:
        n = int(last_n_days.group(1))
        if n > 0:
            end_date = today - timedelta(days=1)           # yesterday
            start_date = end_date - timedelta(days=n-1)    # rolling days
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

    
    # ---- NEW FIXED: last N months (end at previous month, not current month) ----
    last_n_months = re.search(r'\b(?:last|past|previous)\s+(\d{1,3})\s+months?\b', user_query, re.IGNORECASE)
    if last_n_months:
        n = int(last_n_months.group(1))
        if n > 0:

            # Previous month (end)
            end_year = today.year
            end_month = today.month - 1
            if end_month == 0:
                end_month = 12
                end_year -= 1

            
            end_day = monthrange(end_year, end_month)[1]
            end_date = date(end_year, end_month, end_day)

            # Start = n months before end_date + 1st day
            def subtract_months(d, months):
                y = d.year
                m = d.month - months
                while m <= 0:
                    m += 12
                    y -= 1
                last_day = monthrange(y, m)[1]
                return date(y, m, 1)

            start_date = subtract_months(end_date, n-1)

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

    # --------------------------------------------------------------------
    # --- LAST N QUARTERS (e.g. "last 2 quarters", "past 4 quarters") ---
    last_n_quarters = re.search(
        r'\b(?:last|past|previous)\s+(\d{1,2})\s+quarters?\b',
        user_query,
        re.IGNORECASE
    )
    if last_n_quarters:
        n = int(last_n_quarters.group(1))
        if n > 0:

            # Determine last completed quarter
            cm = today.month
            cy = today.year

            # Identify current quarter boundaries
            if cm in (1, 2, 3):      # Q4 FY (previous FY)
                cq_start = date(cy - 1, 1, 1)
                cq_end   = date(cy - 1, 3, 31)
                last_q_year = cy - 1
                last_q_num = 3
            elif cm in (4, 5, 6):    # Q1
                cq_start = date(cy, 4, 1)
                cq_end   = date(cy, 6, 30)
                last_q_year = cy - 1
                last_q_num = 4
            elif cm in (7, 8, 9):    # Q2
                cq_start = date(cy, 7, 1)
                cq_end   = date(cy, 9, 30)
                last_q_year = cy
                last_q_num = 1
            else:                    # Q3
                cq_start = date(cy, 10, 1)
                cq_end   = date(cy, 12, 31)
                last_q_year = cy
                last_q_num = 2

            # Function: get start/end for given FY quarter
            def quarter_range(q, y):
                if q == 1:  return date(y, 4, 1),  date(y, 6, 30)
                if q == 2:  return date(y, 7, 1),  date(y, 9, 30)
                if q == 3:  return date(y,10, 1),  date(y,12, 31)
                if q == 4:  return date(y+1, 1, 1), date(y+1, 3, 31)

            # End date = last completed quarter end
            end_start, end_date = quarter_range(last_q_num, last_q_year)

            # Calculate start quarter (rolling N quarters backwards)
            start_q = last_q_num - (n - 1)
            start_y = last_q_year

            while start_q <= 0:
                start_q += 4
                start_y -= 1

            start_date, _ = quarter_range(start_q, start_y)

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        
    # --- Day + Month range + Year (e.g., "16 sep to 30 september 2024") ---
    day_month_range = re.search(
        r'\b([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through|until|till)\s*'
        r'([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))\b',
        user_query,
        re.IGNORECASE
    )

    if day_month_range:
        d1 = int(day_month_range.group(1))
        m1 = day_month_range.group(2).lower()
        d2 = int(day_month_range.group(3))
        m2 = day_month_range.group(4).lower()
        year = int(day_month_range.group(5))

        sm = month_map[m1[:3]]
        em = month_map[m2[:3]]

        try:
            start_date = date(year, sm, d1)
            end_date = date(year, em, d2)
        except Exception:
            pass
        else:
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

        
    # --- Two specific dates: "16th and 18th sep", "5 and 7 october", etc. ---
    two_dates_match = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:and|&|,)\s*(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        user_query,
        re.IGNORECASE
    )

    if two_dates_match:
        d1 = int(two_dates_match.group(1))
        d2 = int(two_dates_match.group(2))
        month_str = two_dates_match.group(3).lower()

        m = month_map[month_str[:3]]
        inferred_year = fy_start.year if m >= 4 else fy_end.year

        date1 = date(inferred_year, m, d1)
        date2 = date(inferred_year, m, d2)

        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"IN (DATE '{date1}', DATE '{date2}')"
        )

        return insert_where_before_groupby(sql, clause)


    # --- "till / until / upto <day> <month> (optional year)" ---
    till_match = re.search(
        r'\b(?:till|until|upto|up to)\s+([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )

    if till_match:
        d = int(till_match.group(1))
        month_str = till_match.group(2).lower()
        year_str = till_match.group(3)

        m = month_map[month_str[:3]]

        if year_str:
            y = int(year_str)
        else:
            y = fy_start.year if m >= 4 else fy_end.year

        try:
            end_date = date(y, m, d)
            start_date = fy_start

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

        except Exception:
            pass

    
    
    # --- "till / until / upto <day> <month> (optional year)" ---
    till_match = re.search(
        r'\b(?:till|until|upto|up to)\s+([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )

    if till_match:
        d = int(till_match.group(1))
        month_str = till_match.group(2).lower()
        year_str = till_match.group(3)

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10, "nov": 11, "november": 11,
            "dec": 12, "december": 12
        }

        m = month_map[month_str[:3]]

        # infer year
        if year_str:
            y = int(year_str)
        else:
            # infer using FY
            y = fy_start.year if m >= 4 else fy_end.year

        try:
            end_date = date(y, m, d)
            # start of current FY
            start_date = fy_start

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

        except Exception:
            pass

    
    
    # ---- NEW FIXED: last N days (end at yesterday, not today) ----
    last_n_days = re.search(r'\b(?:last|past|previous)\s+(\d{1,3})\s+days?\b', user_query, re.IGNORECASE)
    if last_n_days:
        n = int(last_n_days.group(1))
        if n > 0:
            end_date = today - timedelta(days=1)           # yesterday
            start_date = end_date - timedelta(days=n-1)    # rolling days
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

    # --------------------------------------------------------------------
    
    
    
    # ---- NEW FIXED: last N months (end at previous month, not current month) ----
    last_n_months = re.search(r'\b(?:last|past|previous)\s+(\d{1,3})\s+months?\b', user_query, re.IGNORECASE)
    if last_n_months:
        n = int(last_n_months.group(1))
        if n > 0:

            # Previous month (end)
            end_year = today.year
            end_month = today.month - 1
            if end_month == 0:
                end_month = 12
                end_year -= 1

            
            end_day = monthrange(end_year, end_month)[1]
            end_date = date(end_year, end_month, end_day)

            # Start = n months before end_date + 1st day
            def subtract_months(d, months):
                y = d.year
                m = d.month - months
                while m <= 0:
                    m += 12
                    y -= 1
                last_day = monthrange(y, m)[1]
                return date(y, m, 1)

            start_date = subtract_months(end_date, n-1)

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

    # --------------------------------------------------------------------
    # --- LAST N QUARTERS (e.g. "last 2 quarters", "past 4 quarters") ---
    last_n_quarters = re.search(
        r'\b(?:last|past|previous)\s+(\d{1,2})\s+quarters?\b',
        user_query,
        re.IGNORECASE
    )
    if last_n_quarters:
        n = int(last_n_quarters.group(1))
        if n > 0:

            # Determine last completed quarter
            cm = today.month
            cy = today.year

            # Identify current quarter boundaries
            if cm in (1, 2, 3):      # Q4 FY (previous FY)
                cq_start = date(cy - 1, 1, 1)
                cq_end   = date(cy - 1, 3, 31)
                # last completed quarter becomes Q3 of (cy - 1)
                last_q_year = cy - 1
                last_q_num = 3
            elif cm in (4, 5, 6):    # Q1
                cq_start = date(cy, 4, 1)
                cq_end   = date(cy, 6, 30)
                last_q_year = cy - 1
                last_q_num = 4
            elif cm in (7, 8, 9):    # Q2
                cq_start = date(cy, 7, 1)
                cq_end   = date(cy, 9, 30)
                last_q_year = cy
                last_q_num = 1
            else:                    # Q3
                cq_start = date(cy, 10, 1)
                cq_end   = date(cy, 12, 31)
                last_q_year = cy
                last_q_num = 2

            # Function: get start/end for given FY quarter
            def quarter_range(q, y):
                if q == 1:  return date(y, 4, 1),  date(y, 6, 30)
                if q == 2:  return date(y, 7, 1),  date(y, 9, 30)
                if q == 3:  return date(y,10, 1),  date(y,12, 31)
                if q == 4:  return date(y+1, 1, 1), date(y+1, 3, 31)

            # End date = last completed quarter end
            end_start, end_date = quarter_range(last_q_num, last_q_year)

            # Calculate start quarter (rolling N quarters backwards)
            start_q = last_q_num - (n - 1)
            start_y = last_q_year

            while start_q <= 0:
                start_q += 4
                start_y -= 1

            start_date, _ = quarter_range(start_q, start_y)

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        
    ########################################new###############################################
    
    
    
    
    
        
    # --- Day + Month range + Year (e.g., "16 sep to 30 september 2024") ---
    day_month_range = re.search(
        r'\b([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through|until|till)\s*'
        r'([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))\b',
        user_query,
        re.IGNORECASE
    )

    if day_month_range:
        d1 = int(day_month_range.group(1))
        m1 = day_month_range.group(2).lower()
        d2 = int(day_month_range.group(3))
        m2 = day_month_range.group(4).lower()
        year = int(day_month_range.group(5))

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }

        sm = month_map[m1[:3]]
        em = month_map[m2[:3]]

        # Validate date objects
        try:
            start_date = date(year, sm, d1)
            end_date = date(year, em, d2)
        except Exception:
            # Invalid date like 31 Feb — ignore and let other handlers process
            pass
        else:
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)

        
    # --- Two specific dates: "16th and 18th sep", "5 and 7 october", etc. ---
    two_dates_match = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:and|&|,)\s*(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        user_query,
        re.IGNORECASE
    )

    if two_dates_match:
        d1 = int(two_dates_match.group(1))
        d2 = int(two_dates_match.group(2))
        month_str = two_dates_match.group(3).lower()

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2,
            "mar": 3, "march": 3, "apr": 4, "april": 4,
            "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10, "nov": 11, "november": 11,
            "dec": 12, "december": 12
        }

        m = month_map[month_str[:3]]

        # Infer year based on Indian FY (Apr–Mar)
        inferred_year = fy_start.year if m >= 4 else fy_end.year

        date1 = date(inferred_year, m, d1)
        date2 = date(inferred_year, m, d2)

        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"IN (DATE '{date1}', DATE '{date2}')"
        )

        return insert_where_before_groupby(sql, clause)


    
    # --- Single exact date (e.g. "1st May 2022", "May 1, 2022", "2022-05-01") ---
    # Should be placed BEFORE month-range and single-month handlers.
    date_match = None

    # 1) Common day-month-year with optional ordinal: "1st May 2022", "01-May-2022"
    date_match = re.search(
        r'\b([0-3]?\d)(?:st|nd|rd|th)?[ \-\/\.]?(?:of\s+)?(?:,?\s*)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[ \-\/\.]?(?:,?\s*)?(20\d{2})\b',
        user_query, re.IGNORECASE
    )

    # 2) Month-day-year: "May 1, 2022" or "May 01 2022"
    if not date_match:
        date_match = re.search(
            r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[ ,\-\.]+([0-3]?\d)(?:st|nd|rd|th)?(?:,?\s*)?(20\d{2})\b',
            user_query, re.IGNORECASE
        )

    # 3) ISO or numeric yyyy-mm-dd or dd/mm/yyyy (prefer yyyy-mm-dd)
    if not date_match:
        date_match = re.search(r'\b(20\d{2})[ \-\/\.]([01]?\d)[ \-\/\.]([0-3]?\d)\b', user_query)
        if date_match:
            # reorder to (day, month, year) below by setting flags
            iso_year = int(date_match.group(1))
            iso_month = int(date_match.group(2))
            iso_day = int(date_match.group(3))
            try:
                start = end = date(iso_year, iso_month, iso_day)
                clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
                return insert_where_before_groupby(sql, clause)
            except Exception:
                date_match = None  # fallthrough if invalid date

    if date_match:
        # normalize month strings
        # date_match groups vary by which regex matched but we can handle both patterns:
        g1 = date_match.group(1)
        g2 = date_match.group(2)
        g3 = date_match.group(3)

        # If pattern 1 matched: group1=day, group2=month, group3=year
        # If pattern 2 matched: group1=month, group2=day, group3=year
        # Use heuristics: if g1 looks like a number -> treat it as day
        if re.match(r'^\d{1,2}$', g1):
            day = int(g1)
            month_str = g2.lower()
            year = int(g3)
        else:
            # g1 is month name
            month_str = g1.lower()
            day = int(g2)
            year = int(g3)

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }

        # map first 3 letters safely
        m = month_map.get(month_str[:3], None) if month_str else None
        if m is None:
            # fallback attempt full key
            m = month_map.get(month_str, None)

        try:
            start = end = date(year, m, day)
        except Exception:
            # invalid date (e.g., 31 Feb) — do not alter SQL; fallback to rest of handlers
            start = end = None

        if start and end:
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
            return insert_where_before_groupby(sql, clause)
        
        
    
    
    
    
    
    
    
    
    
    
    
    
        
    # --- Day–Month range with optional year (e.g. "15 sep to 30 sep", "1 jan 2024 to 15 feb 2024") ---
    day_month_range = re.search(
        r'\b(\d{1,2})\s*(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?\s*(?:to|till|until|-)\s*'
        r'(\d{1,2})\s*(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(20\d{2}))?',
        user_query,
        re.IGNORECASE
    )

    if day_month_range:
        d1 = int(day_month_range.group(1))
        m1 = day_month_range.group(2).lower()
        y1 = day_month_range.group(3)

        d2 = int(day_month_range.group(4))
        m2 = day_month_range.group(5).lower()
        y2 = day_month_range.group(6)

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }

        m1_num = month_map[m1[:3]]
        m2_num = month_map[m2[:3]]

        # Year inference
        if y1:
            y1 = int(y1)
        else:
            y1 = fy_start.year if m1_num >= 4 else fy_end.year

        if y2:
            y2 = int(y2)
        else:
            # If range crosses year boundary
            if (m2_num < m1_num) or (m2_num == m1_num and d2 < d1):
                y2 = y1 + 1
            else:
                y2 = y1

        try:
            start = date(y1, m1_num, d1)
            end = date(y2, m2_num, d2)

            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start}' AND DATE '{end}'"
            )
            return insert_where_before_groupby(sql, clause)

        except Exception:
            pass
    # --- Month range + LAST YEAR (e.g. "apr to sep last year", "from april till september last year") ---
    month_range_last_year = re.search(
        r'\b(?:from\s+)?'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|till|until|through|-)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:in\s+)?last year\b',
        user_query,
        re.IGNORECASE
    )

    if month_range_last_year:
        m1 = month_range_last_year.group(1).lower()
        m2 = month_range_last_year.group(2).lower()

        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }

        s_month = month_map[m1[:3]]
        e_month = month_map[m2[:3]]

        # last financial year start = fy_start.year - 1
        last_fy_start_year = fy_start.year - 1

        # Year mapping for each month
        s_year = last_fy_start_year if s_month >= 4 else last_fy_start_year + 1
        e_year = last_fy_start_year if e_month >= 4 else last_fy_start_year + 1

        # Fix rollover (e.g. feb < apr)
        if e_year < s_year or (e_year == s_year and e_month < s_month):
            e_year = s_year + 1

        start_date = date(s_year, s_month, 1)
        end_date = date(e_year, e_month, monthrange(e_year, e_month)[1])

        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    # --- TWO SEPARATE MONTHS e.g. "apr and aug", "april & august" ---
    two_months_match = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:and|&|,)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        user_query,
        re.IGNORECASE
    )

    if two_months_match:
        m1 = two_months_match.group(1).lower()
        m2 = two_months_match.group(2).lower()

        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }

        s_month = month_map[m1[:3]]
        e_month = month_map[m2[:3]]

        # ⭐⭐⭐ Detect explicit user year — “april and august 2024”
        explicit_year_match = re.search(r'\b(20\d{2})\b', user_query)
        explicit_year = int(explicit_year_match.group(1)) if explicit_year_match else None

        if explicit_year:
            s_year = explicit_year
            e_year = explicit_year
        else:
            # fallback to FY logic
            s_year = fy_end.year if s_month < 4 else fy_start.year
            e_year = fy_end.year if e_month < 4 else fy_start.year

        s_last_day = monthrange(s_year, s_month)[1]
        e_last_day = monthrange(e_year, e_month)[1]

        start_date_1 = date(s_year, s_month, 1)
        end_date_1 = date(s_year, s_month, s_last_day)

        start_date_2 = date(e_year, e_month, 1)
        end_date_2 = date(e_year, e_month, e_last_day)

        clause = (
            "("
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date_1}' AND DATE '{end_date_1}'"
            " OR "
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date_2}' AND DATE '{end_date_2}'"
            ")"
        )

        return insert_where_before_groupby(sql, clause)


    # --- Month range (e.g. "april to june", "apr-sep 2025", "from nov to feb") ---
    month_range_match = re.search(
        r'\b(?:from\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*(?:to|-|through|until|till)\s*(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s*(?:,|\s)\s*(20\d{2}))?\b',
        user_query, re.IGNORECASE
    )
    if month_range_match:
        start_month_str = month_range_match.group(1).lower()
        end_month_str = month_range_match.group(2).lower()
        explicit_year = month_range_match.group(3)  # may be None

        # map short/long month names to numbers
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        s_month = month_map[start_month_str[:3]]
        e_month = month_map[end_month_str[:3]]

        # Determine start/end year
        if explicit_year:
            s_year = int(explicit_year)
            # if end month is before start month, assume it rolls into next calendar year
            e_year = s_year if e_month >= s_month else s_year + 1
        else:
            # Use your FY mapping: months < 4 belong to fy_end.year, months >=4 belong to fy_start.year
            s_year = fy_end.year if s_month < 4 else fy_start.year
            e_year = fy_end.year if e_month < 4 else fy_start.year
            # If end falls before start (range crosses year boundary), bump end year
            if e_year < s_year or (e_month < s_month and e_year == s_year):
                e_year = s_year + 1

        s_last_day = monthrange(s_year, s_month)[1]
        e_last_day = monthrange(e_year, e_month)[1]

        start = date(s_year, s_month, 1)
        end = date(e_year, e_month, e_last_day)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    
    

    # --- Single month (e.g. "april", "apr", "April 2025") ---
    single_month_match = re.search(
        r'\b(?:in\s+|for\s+|of\s+|between\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s*(?:,|\s)\s*(20\d{2}))?\b',
        user_query, re.IGNORECASE
    )
    if single_month_match:
        month_str = single_month_match.group(1).lower()
        explicit_year = single_month_match.group(2)  # may be None
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        m = month_map[month_str[:3]]
        if explicit_year:
            y = int(explicit_year)
        else:
            # follow your FY mapping: months < 4 belong to fy_end.year else fy_start.year
            y = fy_end.year if m < 4 else fy_start.year

        last_day = monthrange(y, m)[1]
        start = date(y, m, 1)
        end = date(y, m, last_day)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)


    # --- Quarter helpers ---
    # def quarter_start_end(d):
    #     month = d.month
    #     year = d.year
    #     if month in [4, 5, 6]:   # Q1
    #         return date(year, 4, 1), date(year, 6, 30)
    #     elif month in [7, 8, 9]: # Q2
    #         return date(year, 7, 1), date(year, 9, 30)
    #     elif month in [10, 11, 12]: # Q3
    #         return date(year, 10, 1), date(year, 12, 31)
    #     else:  # Jan-Mar -> Q4
    #         return date(year - 1, 1, 1), date(year - 1, 3, 31)

    # def last_quarter_start_end(d):
    #     m = (d.month - 1) // 3 * 3 + 1 - 3
    #     y = d.year
    #     if m <= 0:
    #         m += 12
    #         y -= 1
    #     return quarter_start_end(date(y, m, 1))

    def quarter_start_end(d):
        """
        Returns start and end dates for the current quarter based on Indian Financial Year.
        FY runs from April 1 to March 31.
        Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
        """
        month = d.month
        year = d.year
        
        # Determine which FY quarter we're in based on current month
        if month in [4, 5, 6]:   # Q1: April-June
            return date(year, 4, 1), date(year, 6, 30)
        elif month in [7, 8, 9]: # Q2: July-September
            return date(year, 7, 1), date(year, 9, 30)
        elif month in [10, 11, 12]: # Q3: October-December
            return date(year, 10, 1), date(year, 12, 31)
        else:  # Q4: Jan-Mar (months 1, 2, 3)
            # Q4 belongs to the PREVIOUS FY year
            # Example: Jan 2026 is Q4 of FY 2025-26
            return date(year, 1, 1), date(year, 3, 31)


    def last_quarter_start_end(d):
        """
        Returns start and end dates for the last completed quarter based on Indian FY.
        """
        month = d.month
        year = d.year
        
        # Determine current quarter and go back one
        if month in [4, 5, 6]:   # Currently Q1 → Last quarter was Q4 of previous FY
            return date(year, 1, 1), date(year, 3, 31)
        elif month in [7, 8, 9]: # Currently Q2 → Last quarter was Q1
            return date(year, 4, 1), date(year, 6, 30)
        elif month in [10, 11, 12]: # Currently Q3 → Last quarter was Q2
            return date(year, 7, 1), date(year, 9, 30)
        else:  # Currently Q4 (Jan-Mar) → Last quarter was Q3 of previous calendar year
            return date(year - 1, 10, 1), date(year - 1, 12, 31)



    # ----------------------------------------
    # 16) Specific quarter Q1..Q4 optionally with FY-year (e.g., "quarter 1", "q2 of 2024")
    # ----------------------------------------
    def fiscal_quarter_start_end(fy_year, qnum):
        if qnum == 1:
            return date(fy_year, 4, 1), date(fy_year, 6, 30)
        if qnum == 2:
            return date(fy_year, 7, 1), date(fy_year, 9, 30)
        if qnum == 3:
            return date(fy_year, 10, 1), date(fy_year, 12, 31)
        # qnum == 4
        return date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)

    m = re.search(r'\b(?:q(?:uarter)?\s*[-\s]*([1-4])|quarter\s+([1-4]))\b(?:\s*(?:of\s*)?(?:fy\s*)?(\d{4}))?', user_query, re.IGNORECASE)
    if m:
        qnum = int(m.group(1) or m.group(2))
        year_tok = m.group(3)
        fy_year = int(year_tok) if year_tok else fy_start.year
        s_dt, e_dt = fiscal_quarter_start_end(fy_year, qnum)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{s_dt}' AND DATE '{e_dt}'"
        )
        return insert_where_before_groupby(sql, clause)

    

    # 1️⃣ Year-on-Year / YoY → skip date filter entirely
    if re.search(r"\b(year on year|yoy|year-over-year|by year| year wise| yearly)\b", user_query, re.I):
        return sql  # SQL generated by model already aggregates by year

    # 2️⃣ "last year" → previous financial year (Apr–Mar)
    if re.search(r"\blast year\b", user_query, re.I):
        last_fy_start = date(fy_start.year - 1, 4, 1)
        last_fy_end = date(fy_start.year, 3, 31)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{last_fy_start}' AND DATE '{last_fy_end}'"
        )
        return insert_where_before_groupby(sql, clause)

    # 3️⃣ Full date (dd/mm/yyyy) → leave SQL as is
    if re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}", user_query):
        return sql

    
    # 4️⃣ Quarter (Q1-Q4) with optional FY
    q_match = re.search(r"\bQ([1-4])(?:\s*(?:FY)?\s*(20\d{2}))?\b", user_query, re.I)
    if q_match:
        q = int(q_match.group(1))
        fy_year = int(q_match.group(2)) if q_match.group(2) else fy_start.year

        # Identify quarter date ranges (Indian FY)
        if q == 1:
            start, end = date(fy_year, 4, 1), date(fy_year, 6, 30)
        elif q == 2:
            start, end = date(fy_year, 7, 1), date(fy_year, 9, 30)
        elif q == 3:
            start, end = date(fy_year, 10, 1), date(fy_year, 12, 31)
        else:  # Q4
            start, end = date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)

        # ⭐⭐⭐ CRITICAL FIX: Remove MONTH groupings so quarter always returns ONE RESULT
        # Remove SELECT month_num
        sql = re.sub(r'\bmonth_num\b\s*,?', '', sql, flags=re.IGNORECASE)

        # Remove GROUP BY month_num
        sql = re.sub(
            r'GROUP BY\s+.*month_num.*?(?=ORDER BY|$)',
            'GROUP BY ',
            sql,
            flags=re.IGNORECASE
        )

        # Remove ORDER BY month_num
        sql = re.sub(
            r'ORDER BY\s*month_num\s*(,)?',
            'ORDER BY ',
            sql,
            flags=re.IGNORECASE
        )

        # Apply quarter date filter
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start}' AND DATE '{end}'"
        )

        return insert_where_before_groupby(sql, clause)


    # 5️⃣ "this quarter" / "last quarter"
    if re.search(r"\bthis quarter\b", user_query, re.I):
        start, end = quarter_start_end(today)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    if re.search(r"\blast quarter\b", user_query, re.I):
        start, end = last_quarter_start_end(today)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)

    # 6️⃣ Month + Year (e.g., "May 2024")
    month_year_match = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+20\d{2}", user_query, re.I)
    if month_year_match:
        month_str, year = month_year_match.group(1).lower(), int(re.search(r'\d{4}', month_year_match.group(0)).group())
        month_map = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
        }
        month_num = month_map[month_str]
        last_day = monthrange(year, month_num)[1]
        start, end = date(year, month_num, 1), date(year, month_num, last_day)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)

    # 7️⃣ "this month" / "last month"
    if re.search(r"\bthis month\b", user_query, re.I):
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{month_start}' AND DATE '{month_end}'"
        return insert_where_before_groupby(sql, clause)
    if re.search(r"\blast month\b", user_query, re.I):
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{last_month_start}' AND DATE '{last_month_end}'"
        return insert_where_before_groupby(sql, clause)
    
    # --- Multiple specific years: "2024 and 2025" ---
    year_list_match = re.search(
        r'\b(20\d{2})(?:\s*,\s*|\s+and\s+|\s*&\s*)(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )

    if year_list_match:
        y1 = int(year_list_match.group(1))
        y2 = int(year_list_match.group(2))

        # Ensure proper ordering
        years = sorted([y1, y2])

        # Build CASE expression for fiscal year
        fy_case = (
            "CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) >= 4 "
            "THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) "
            "ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) - 1 END"
        )

        clause = f"{fy_case} IN ({years[0]}, {years[1]})"

        return insert_where_before_groupby(sql, clause)

    
    # --- Year range: "2024 to 2025" OR "2024 - 2025" ---
    year_range = re.search(
        r'\b(20\d{2})\s*(?:to|-|until|through)\s*(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )
    if year_range:
        y1 = int(year_range.group(1))
        y2 = int(year_range.group(2))

        if y2 < y1:
            y1, y2 = y2, y1

        start_date = date(y1, 4, 1)
        end_date = date(y2, 3, 31)

        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)



    # 8️⃣ Year-only (e.g., 2024) → full FY
    year_match = re.search(r"\b(20\d{2})\b", user_query)
    if year_match:
        year = int(year_match.group(1))
        start, end = date(year, 4, 1), date(year + 1, 3, 31)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)

    # 🔟 Default → current FY
    clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
    return insert_where_before_groupby(sql, clause)


# --------------------
# Helper: Run SQL on Presto
# --------------------
def run_presto_query(sql: str):
    with prestodb.dbapi.connect(
        host=hostname,
        port=portnumber,
        user=username,
        catalog=CATALOG,
        schema=SCHEMA,
        http_scheme="https",
        auth=prestodb.auth.BasicAuthentication(username, password),
    ) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        result = [dict(zip(cols, row)) for row in rows]
        return result


# --------------------
# Post-processing: Enforce DESC sort on numeric columns
# --------------------
def enforce_descending_order(sql: str) -> str:
    TEMPORAL_COLS = {
        'year_num', 'month_num', 'quarter_num',
        'year', 'month', 'quarter', 'period'
    }
    sql = sql.rstrip(';').strip()

    if re.search(r'\bORDER BY\b', sql, re.IGNORECASE):
        def fix_order_clause(m):
            order_clause = m.group(1).strip()
            parts = [p.strip() for p in order_clause.split(',')]
            new_parts = []
            for part in parts:
                tokens = part.split()
                col_name = tokens[0].lower().strip('"').strip("'")
                if col_name in TEMPORAL_COLS:
                    new_parts.append(part)
                elif len(tokens) == 1:
                    new_parts.append(part + ' DESC')
                elif tokens[-1].upper() == 'ASC':
                    new_parts.append(' '.join(tokens[:-1]) + ' DESC')
                else:
                    new_parts.append(part)
            return 'ORDER BY ' + ', '.join(new_parts)

        sql = re.sub(r'ORDER BY\s+(.+?)$', fix_order_clause, sql, flags=re.IGNORECASE)
    else:
        numeric_alias = re.search(
            r'\bAS\s+(refund|basic_amount|refund_amount|amount|total_amount|'
            r'total|chequebounce|bounce_amount|count|net_amount)\b',
            sql, re.IGNORECASE
        )
        if numeric_alias:
            sql += f' ORDER BY {numeric_alias.group(1)} DESC'

    return sql


# --------------------
# Post-processing: Append Total row to multi-row results
# --------------------
def add_total_row(data: list) -> list:
    if not data or len(data) <= 1:
        return data

    # ⭐ Dimension/identifier columns that should NEVER be summed.
    # These columns hold codes, labels, or category names — not amounts.
    DIMENSION_COLS = {
        'project', 'comp_code', 'sales_group', 'sales_group_name',
        'month', 'month_name', 'year', 'quarter',
        'month_num', 'year_num', 'period', 'bank', 'bank_name',
        'customer', 'customer_name', 'cust_name',
        'posting_date', 'post_date', 'date',   'fy_year', 'quarter_num', 'quarter', 'quarter_label',
    }

    total_row = {}
    first_label_done = False

    for key in data[0].keys():
        # Check if this column is a known dimension (case-insensitive)
        if key.lower() in DIMENSION_COLS:
            if not first_label_done:
                total_row[key] = 'Total'
                first_label_done = True
            else:
                total_row[key] = '-'
            continue

        col_values = [row.get(key) for row in data]
        non_null = [v for v in col_values if v is not None]
        try:
            numeric_vals = [float(v) for v in non_null]
            total_row[key] = round(sum(numeric_vals), 2)
        except (ValueError, TypeError):
            if not first_label_done:
                total_row[key] = 'Total'
                first_label_done = True
            else:
                total_row[key] = '-'

    data.append(total_row)
    return data


######################################### new code ############################################
from datetime import date, timedelta
# Today
today = date.today()

# FY starts on April 1
fy_start = date(today.year if today.month >= 4 else today.year-1, 4, 1)
fy_end = date(fy_start.year + 1, 3, 31)

# Determine current quarter
def quarter_start_end(d):
    month = d.month
    year = d.year
    if month in [4,5,6]:   # Q1
        return date(year,4,1), date(year,6,30)
    elif month in [7,8,9]: # Q2
        return date(year,7,1), date(year,9,30)
    elif month in [10,11,12]: # Q3
        return date(year,10,1), date(year,12,31)
    else: # Jan-Mar -> Q4
        return date(year-1,1,1), date(year-1,3,31)

# This quarter
q_start, q_end = quarter_start_end(today)

# Last quarter
def last_quarter_start_end(d):
    # shift back 3 months
    m = (d.month-1)//3*3 +1 -3
    y = d.year
    if m <= 0:
        m +=12
        y -=1
    last_q_date = date(y, m, 1)
    return quarter_start_end(last_q_date)

last_q_start, last_q_end = last_quarter_start_end(today)

# --- THIS MONTH ---
month_start = date(today.year, today.month, 1)
month_end = (date(today.year, today.month + 1, 1) - timedelta(days=1)) if today.month != 12 else date(today.year, 12, 31)
month_start_str = month_start.strftime("%Y-%m-%d")
month_end_str = month_end.strftime("%Y-%m-%d")

# --- LAST MONTH ---
if today.month == 1:
    last_month_start = date(today.year - 1, 12, 1)
    last_month_end = date(today.year - 1, 12, 31)
else:
    last_month_start = date(today.year, today.month - 1, 1)
    last_month_end = date(today.year, today.month, 1) - timedelta(days=1)
last_month_start_str = last_month_start.strftime("%Y-%m-%d")
last_month_end_str = last_month_end.strftime("%Y-%m-%d")
    


# --------------------
# NL → SQL with Watsonx
# --------------------


def nl_to_sql(user_query: str) -> str:
    fy_start, fy_end = get_financial_year_range()
    fy_start_str = fy_start.strftime("%Y-%m-%d")
    fy_end_str = fy_end.strftime("%Y-%m-%d")
    
    prompt = f"""
You are an expert Presto SQL generator for Watsonx.data.

Convert the user's natural language question into a valid Presto SQL query
for the chequebounce dataset.

REQUIREMENTS:

- Always reference the table as "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}".
- Allowed columns (use exact casing): "Comp_code","fiscal_year","POST_date","Cust_no","Cust_name",
  "sales_doc_no","cust_po_no","sales_grp_descp","mat_descp","Recipt_date",
  "account_doc_no","Doc_hrd_text","cheque_dd","Doc_date_doc","gen_ledger","Bank_name",
  "Basic_amount","Service_tax","Gross_amount","Text","remarks"
  
- Company code / project mapping rules:

    Company-wise / Company code wise:
    * If the user says "company wise", treat it EXACTLY the same as "company code wise":
        - SELECT "Comp_code"
        - GROUP BY "Comp_code"
        - Do NOT filter or assume any project unless the user explicitly says wave city / wmcc / wave estate.
        - Do NOT look for any column named "Company" or "Company_Name" (these do not exist).

    Projects (for filtering):
        * "wave city"         ↔ Comp_code = 1000
        * "wmcc", "wmcc sec 32"   ↔ Comp_code = 1100
        * "wave estate"       ↔ Comp_code = 1300
    
    IMPORTANT RULES:
        * If user says "company wise" OR "company code wise":
              - DO NOT filter on Comp_code
              - Only display totals broken by Comp_code
              - Only GROUP BY "Comp_code" + whatever dimensions the query requires
############################################################################################################
  ==========================================================================
CRITICAL: HANDLING "AND" QUERIES WITH SEPARATE RESULTS
==========================================================================

When user asks for multiple items with "and" (e.g., "april and august", "q1 and q2", "2024 and 2025"):
YOU MUST return SEPARATE rows for EACH item.

DETECTION PATTERNS:
1. Two months: "april and august", "jan and dec"
2. Two quarters: "q1 and q2", "Q1 FY 2024 and Q3 FY 2024"
3. Two years: "2024 and 2025"
4. Month combinations: "april and august 2024"

FOR THESE QUERIES, USE THIS APPROACH:

**FOR MONTHS "and" QUERIES:**
- Use UNION ALL to create separate rows
- Example: "april and august 2024"
  
  SELECT 'April' AS period, ABS(SUM(CAST(...))) AS chequebounce
  FROM table
  WHERE DATE_PARSE(...) BETWEEN DATE '2024-04-01' AND DATE '2024-04-30'
  UNION ALL
  SELECT 'August' AS period, ABS(SUM(CAST(...))) AS chequebounce
  FROM table
  WHERE DATE_PARSE(...) BETWEEN DATE '2024-08-01' AND DATE '2024-08-31';

**FOR QUARTERS "and" QUERIES:**
- Use UNION ALL approach
- Example: "q1 and q2"
  
  SELECT 'Q1' AS period, ABS(SUM(CAST(...))) AS chequebounce
  FROM table
  WHERE DATE_PARSE(...) BETWEEN DATE '2024-04-01' AND DATE '2024-06-30'
  UNION ALL
  SELECT 'Q2' AS period, ABS(SUM(CAST(...))) AS chequebounce
  FROM table
  WHERE DATE_PARSE(...) BETWEEN DATE '2024-07-01' AND DATE '2024-09-30';

**FOR YEARS "and" QUERIES:**
- Use UNION ALL approach
- Example: "2024 and 2025"
  
  SELECT '2024' AS period, ABS(SUM(CAST(...))) AS chequebounce
  FROM table
  WHERE DATE_PARSE(...) BETWEEN DATE '2024-04-01' AND DATE '2025-03-31'
  UNION ALL
  SELECT '2025' AS period, ABS(SUM(CAST(...))) AS chequebounce
  FROM table
  WHERE DATE_PARSE(...) BETWEEN DATE '2025-04-01' AND DATE '2026-03-31';

**IMPORTANT RULES:**
- ALWAYS use UNION ALL (not UNION) to preserve all rows
- Each SELECT must have identical column structure
- Use descriptive period labels ('April', 'Q1', '2024', etc.)
- Each UNION segment gets its own WHERE clause with exact date ranges
- DO NOT use OR conditions - use UNION ALL instead
- Order results by the period column at the end

==========================================================================

- Chequebounce expression (Presto-compatible):
    ABS(
      SUM(
        CAST(
          CASE
            WHEN TRIM("Gross_amount") LIKE '%-' 
              THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
            ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
          END AS DOUBLE
        )
      )
    ) AS chequebounce

  CRITICAL: For month-on-month queries WITHOUT "and", the SELECT clause MUST be:
    EXTRACT(YEAR FROM DATE_PARSE(...)) AS year_num,
    EXTRACT(MONTH FROM DATE_PARSE(...)) AS month_num,
    ABS(SUM(CAST(...))) AS chequebounce
  
  NEVER write: "EXTRACT(MONTH ...) AS ABS(SUM(...)) AS chequebounce"
  The month extraction and the aggregation are SEPARATE columns.

  
##############################################################################################


  IMPORTANT RULES:
    * NEVER add any filter on "Comp_code" if the user does NOT mention any of these project names.
      - For generic questions like "total chequebounce", "overall chequebounce", etc., DO NOT reference "Comp_code" at all.
    * If the user mentions **wave city**:
        - Add: WHERE "Comp_code" = 1000
    * If the user mentions **wmcc**:
        - Add: WHERE "Comp_code" = 1100
    * If the user mentions **wave estate**:
        - Add: WHERE "Comp_code" = 1300

    * If the user mentions multiple projects (e.g. "wave city and wmcc"):
        - Use: WHERE "Comp_code" IN (1000,1100)  (include all mentioned codes)
    * Never assume a default company. Never write "Comp_code = '1000'" unless the user said "wave city".


- Chequebounce expression (Presto-compatible):
    ABS(
      SUM(
        CAST(
          CASE
            WHEN TRIM("Gross_amount") LIKE '%-' 
              THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
            ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
          END AS DOUBLE
        )
      )
    ) AS chequebounce

- Gross amount queries → treat exactly the same as chequebounce.
- Basic_amount queries → use:
    SUM(ABS("Basic_amount")) AS basic_amount

- For customer-wise or bank-wise queries:
    * Always SELECT "Cust_no", "Cust_name"
    * If a specific bank is requested → also SELECT "Bank_name"
    * Always include GROUP BY corresponding columns:
        - customer-wise: GROUP BY "Cust_no", "Cust_name"
        - customer-wise + bank: GROUP BY "Cust_no", "Cust_name", "Bank_name"
    * Always order by the amount descending:
        - chequebounce → ORDER BY chequebounce DESC
        - gross → ORDER BY chequebounce DESC
        - basic_amount → ORDER BY basic_amount DESC

- Filtering by Cust_name:
    WHERE LOWER(REGEXP_REPLACE(TRIM("Cust_name"), '[^a-zA-Z0-9 ]', '')) = '<customer name>'
    * Exact match, case-insensitive, cleaned

- Filtering by Bank_name:
    WHERE LOWER(REGEXP_REPLACE(TRIM("Bank_name"), '[^a-zA-Z ]', '')) LIKE '%<first word of bank name>%'
    * Partial match using first word ensures extra digits or text still match

- Date filtering (POST_date YYYYMMDD):
    * Always use DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')
    * Only include ONE BETWEEN clause
    * Rules:
        1. User does NOT specify year/month/quarter → default FY {fy_start_str} to {fy_end_str}
        2. User specifies a year (e.g., 2024) → financial year April–March
        3. User specifies a quarter (Q1/Q2/Q3/Q4) → calculate start/end dates of that quarter
        4. User specifies "this month" → BETWEEN DATE '{month_start_str}' AND DATE '{month_end_str}'
        5. User specifies "last month" → BETWEEN DATE '{last_month_start_str}' AND DATE '{last_month_end_str}'
        6. User specifies "this quarter" → BETWEEN DATE '{q_start}' AND DATE '{q_end}'
        7. User specifies "last quarter" → BETWEEN DATE '{last_q_start}' AND DATE '{last_q_end}'
        8. Never include default FY when month/quarter is detected



You are an expert Presto SQL generator for chequebounce data. Follow these rules for Month-on-Month (MoM) queries:

1. Always extract YEAR and MONTH from POST_date as:
   - EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS year_num
   - EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS month_num

2. For TOTAL chequebounce (no customer or sales group filter):
   - SELECT year_num, month_num, SUM(chequebounce) AS chequebounce_amount
   - Wrap as subquery if needed and ORDER BY t.year_num, t.month_num

3. For CUSTOMER-wise queries (all customers):
   - Include "Cust_no" and "Cust_name"
   - GROUP BY Cust_no, Cust_name, year_num, month_num
   - ORDER BY year_num, month_num, chequebounce_amount DESC

3. For remarks/cancellation reason queries (filter on remarks):
   - Include "Cust_no" and "Cust_name" and "bank_name"
   - GROUP BY Cust_no, Cust_name, bank_name, year_num, month_num
   

- Sales Group rules:
    1. Always SELECT "sales_grp_descp", plus the required amount expression for sales-group queries.
    2. Always GROUP BY "sales_grp_descp".
    3. If the user specifies a sales group by name (e.g., 'new plots','old plots','amore','veridia','eden','eligo','wave floors','armonia villa','dream bazaar','dream home','wave floor 85','wave floor 89','executive floors','institutional','verdia 3','verdia 4','verdia 5','verdia 6','verdia 7','wave galleria','wave garden','ews_p2','lig_p2','prime floors','swamanorath','hssc','lig_001_(310)','ews_p2','ews_001_(410)'), filter on "sales_grp_descp":
       WHERE LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), '\\s+', '')) LIKE '%<group_name>%'

    4. If the user specifies a sales group by code (e.g. 101), filter on "sales_grp_descp":
       WHERE "sales_grp_descp" = '101'

    5. If no sales group specified, return all groups (no WHERE).

-  CRITICAL Sales Group rules:
    1. Always SELECT "sales_grp_descp", plus the required amount expression for sales-group queries.
    2. Always GROUP BY "sales_grp_descp".
    
    # ========== ADD THESE ENHANCED RULES ==========
    3. CRITICAL: If query says "sales group wise/product wise" + company name:
       - Filter on Comp_code first
       - Then group by sales_grp_descp
       - Example: "wave city sales group wise" → WHERE "Comp_code" = 1000 GROUP BY "sales_grp_descp"
       - Filter on Comp_code first
       - Then group by sales_grp_descp
       - Example: "wave city sales group wise" → WHERE "Comp_code" = 1000 GROUP BY "sales_grp_descp"
    
    4. If query says "for X and Y separately":
       - Treat as separate sales groups
       - This triggers UNION ALL logic (handled automatically)
    
    5. Known sales groups: eden, veridia, amore, new plots, old plots, etc.

    
    - IMPORTANT DISAMBIGUATION:
        If the user writes "sales group wise/ product wise" anywhere in the question, then:
            • Treat ALL names mentioned after it as SALES GROUP names (not customer names).
            • Do NOT apply any filtering on "Cust_name".
            • Only apply sales group filtering:
                WHERE LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), '\\s+', '')) LIKE '%<group_name>%'
            • Example:
                "sales group wise total cheque bounce for amita gulla"
                → MUST NOT filter on sales_grp_descp LIKE '%amitagulla%'
                → MUST filter on Cust_name LIKE '%amitagulla%'.
                
        
        DETECTION RULES:
        1. If the name matches KNOWN sales groups → treat as sales group query
           Known groups: new plots, old plots, amore, veridia, eden, eligo, wave floors, 
                        armonia villa, dream bazaar, wave galleria, etc.
        
        2. If the name is NOT in known sales groups → treat as CUSTOMER query
           → Filter on Cust_name
           → Group by sales_grp_descp, Cust_no, Cust_name
        
        3. Default: If ambiguous, prefer CUSTOMER interpretation for "sales group wise"

    
5. Always handle chequebounce amounts:
   - If value ends with '-', prepend '-' and remove trailing minus
   - Remove non-numeric characters before casting to DOUBLE
   - Wrap SUM with ABS to get positive totals

6. Use aliases for year_num and month_num in GROUP BY and ORDER BY; do not repeat full EXTRACT expressions

7. Do not leave trailing commas in SELECT, GROUP BY, or ORDER BY

Return only the Presto SQL query, ending with a semicolon. Do not add explanations.



Rules:
1. Always start SQL with SELECT and return a **single-line SQL ending with semicolon**. No explanations.
# 2. Quarter-on-Quarter (QoQ) / "quarter on quarter" / "qoq" / "quarter-on-quarter":
#     * MUST ALWAYS use the EXACT pattern below:
    
#       SELECT
#         EXTRACT(YEAR FROM DATE_ADD('month', -3, DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d'))) AS year_num,
#         CEIL(EXTRACT(MONTH FROM DATE_ADD('month', -3, DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d'))) / 3.0) AS quarter_num,
#         <amount expression> AS chequebounce
#       FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
#       WHERE <date filters based on user query>
#       GROUP BY
#         EXTRACT(YEAR FROM DATE_ADD('month', -3, DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d'))),
#         CEIL(EXTRACT(MONTH FROM DATE_ADD('month', -3, DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d"))) / 3.0)
#       ORDER BY year_num, quarter_num;

2. Quarter-on-Quarter (QoQ) / "quarter on quarter" / "qoq" / "quarter-on-quarter":
    * MUST ALWAYS use the EXACT pattern below for INDIAN FINANCIAL YEAR quarters:
    * Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
    
      SELECT
        CASE 
            WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) >= 4
            THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d'))
            ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) - 1
        END AS fy_year,
        CASE
            WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 4 AND 6 THEN 1
            WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 7 AND 9 THEN 2
            WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 10 AND 12 THEN 3
            ELSE 4
        END AS quarter_num,
        <amount expression> AS chequebounce
      FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
      WHERE <date filters>
      GROUP BY
        CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(...)) >= 4 THEN EXTRACT(YEAR FROM ...) ELSE EXTRACT(YEAR FROM ...) - 1 END,
        CASE WHEN EXTRACT(MONTH FROM ...) BETWEEN 4 AND 6 THEN 1 WHEN ... BETWEEN 7 AND 9 THEN 2 WHEN ... BETWEEN 10 AND 12 THEN 3 ELSE 4 END
      ORDER BY fy_year, quarter_num;

    * Do NOT use normal quarter logic.
    * Do NOT use EXTRACT(MONTH)/3 directly.
    * Do NOT use fiscal year CASE WHEN logic.
    * Do NOT group by aliases.
    * Every QoQ query must output exactly:
         year_num, quarter_num, chequebounce
    * Date filter for a year (e.g., "QoQ 2024"):
         BETWEEN DATE '2024-04-01' AND DATE '2025-03-31'
    * If user does NOT specify a year:
         Use default financial year.

3. Do NOT use aliases in GROUP BY (do not write "GROUP BY ... AS quarter_num").
4. Always order by `year_num, quarter_num`.
5. Amount expression must handle **trailing minus values** and convert all values to positive using ABS(), for example:
   ABS(SUM(CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') AS DOUBLE))) AS chequebounce
6. Remove duplicate or redundant WHERE conditions.
7. Do NOT break parentheses. Make sure every open parenthesis has a matching close.
8. Only return valid Presto SQL with correct syntax.


   
        
    
- If the user specifies a quarter in a given FY (e.g., "Q1 FY 2024"):
    * Always map quarters using Indian FY (Apr–Mar):
        - Q1 FY YYYY → Apr 1 YYYY to Jun 30 YYYY
        - Q2 FY YYYY → Jul 1 YYYY to Sep 30 YYYY
        - Q3 FY YYYY → Oct 1 YYYY to Dec 31 YYYY
        - Q4 FY YYYY → Jan 1 (YYYY+1) to Mar 31 (YYYY+1)
    * Only generate ONE BETWEEN condition for this range.
    * Example: "Q1 FY 2024" → 
        BETWEEN DATE '2024-04-01' AND DATE '2024-06-30'
    

- For Year-on-Year (YoY) / "year on year" / "year-over-year" / "YoY" / "by year" queries:
    * DETECT these keywords. When detected:
        - DO NOT add any DATE_PARSE(...) BETWEEN ... clause.
        - DO NOT apply the default financial year filter.
        - Instead, aggregate across all available data BY FINANCIAL YEAR (April–March) using EXACTLY this pattern:
            SELECT 
                CASE 
                    WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) >= 4 
                    THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) 
                    ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) - 1 
                END AS year_num,
                <amount expression> AS chequebounce
            FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
            GROUP BY 
                CASE 
                    WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) >= 4 
                    THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) 
                    ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) - 1 
                END
            ORDER BY year_num;
        - Never write just "ORDER BY" without specifying "year_num".


==========================================================================
CRITICAL: QUARTER QUERY RULES
==========================================================================

DETECTION RULES:
1. "total cheque bounce of Q1" OR "Q1 cheque bounce" → ONE COMBINED RESULT
2. "month on month Q1" OR "Q1 MoM" → THREE SEPARATE MONTH RESULTS

SQL GENERATION RULES:

**FOR QUARTER TOTAL (ONE RESULT):**
When user asks: "total cheque bounce of Q1" or "Q1 FY 2024"

SELECT 
    'Q1' AS quarter,
    ABS(SUM(CAST(...))) AS chequebounce
FROM table
WHERE DATE_PARSE(...) BETWEEN DATE '2024-04-01' AND DATE '2024-06-30'

**DO NOT include month_num or year_num in SELECT**
**DO NOT include GROUP BY**
**Return ONLY ONE row with total**

**FOR MONTH-ON-MONTH WITHIN QUARTER (BREAKDOWN):**
When user asks: "month on month Q1" or "Q1 MoM"

SELECT 
    EXTRACT(YEAR FROM DATE_PARSE(...)) AS year_num,
    EXTRACT(MONTH FROM DATE_PARSE(...)) AS month_num,
    ABS(SUM(CAST(...))) AS chequebounce
FROM table
WHERE DATE_PARSE(...) BETWEEN DATE '2024-04-01' AND DATE '2024-06-30'
GROUP BY 
    EXTRACT(YEAR FROM DATE_PARSE(...)),
    EXTRACT(MONTH FROM DATE_PARSE(...))
ORDER BY year_num, month_num

**Return THREE rows (April, May, June)**

NEVER CONFUSE THESE TWO PATTERNS!



1. Always wrap **customer-wise Month-on-Month (MoM) or Year-on-Year (YoY) queries** in a subquery AS t.
2. Aliases:
   - MoM: year_num, month_num
   - YoY: year_num
   - Amount: chequebounce
3. Use aliases in GROUP BY and ORDER BY; never repeat full EXTRACT or CASE expressions.
4. Handle trailing minus in "Gross_amount":
    CASE WHEN TRIM("Gross_amount") LIKE '%-' THEN '-' || REGEXP_REPLACE(TRIM("Gross_amount"), '-$', '') ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') END
   Wrap SUM(...) with ABS.
5. Customer-wise queries must SELECT "Cust_no", "Cust_name".
6. Always ORDER BY chequebounce DESC for customer-wise.
7. Do NOT add default FY BETWEEN clause for YoY.
8. MoM queries: filter by financial year if specified; otherwise, wrap entire inner query as subquery and aggregate by month.
9. Non-customer-wise queries: aggregate normally by month or year, wrap as subquery AS t if needed.
10. Use DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') for all date operations.
11. Always return a single-line Presto SQL ending with semicolon; no explanations.
12. If the user mention words instead of numbers for queries like last two quarters or last three months, so always treat that word as number. For example, "last two quarters" = 2, "last three months" = 3, "last four years" = 4 etc.


User question: "{user_query}"
SQL:
"""

#################################### working#########################################################    


    raw = model.generate_text(prompt=prompt)
    return raw





# def detect_and_queries(user_query: str) -> dict:
#     """
#     Detects if user is asking for multiple separate periods with 'and'.
#     Returns dict with type and entities to query separately.
#     """
#     query_lower = user_query.lower()
    
#     result = {
#         'has_and': False,
#         'type': None,
#         'entities': []
#     }
    
#     # Month map
#     month_map = {
#         "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
#         "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
#         "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
#         "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
#     }
    
#     # Skip MoM, QoQ, YoY
#     if re.search(r'\b(month on month|mom|quarter on quarter|qoq|year on year|yoy)\b', query_lower):
#         return result
    
#     # ========== NEW: Company + Years (e.g., "wave city in 2024 and 2025 separately") ==========
#     company_year_pattern = re.search(
#         r'\b(wave city|wmcc|wave estate)\b.*?\b(20\d{2})\s+and\s+(20\d{2})\b.*?\b(separately|separate)\b',
#         query_lower
#     )
    
#     if company_year_pattern:
#         company = company_year_pattern.group(1)
#         y1 = int(company_year_pattern.group(2))
#         y2 = int(company_year_pattern.group(3))
        
#         # Map company to Comp_code
#         comp_code_map = {
#             'wave city': 1000,
#             'wmcc': 1100,
#             'wave estate': 1300
#         }
        
#         comp_code = comp_code_map.get(company)
        
#         if comp_code:
#             result['has_and'] = True
#             result['type'] = 'company_year'
#             result['entities'] = [
#                 {'name': f'{company.title()} FY{y1}', 'year': y1, 'comp_code': comp_code},
#                 {'name': f'{company.title()} FY{y2}', 'year': y2, 'comp_code': comp_code}
#             ]
#             return result
    
#     # ========== ENHANCED: Sales group wise with "and" + "separately" ==========
#     # Pattern: "till september for eden and veridia separately"
#     if re.search(r'\bsales group wise\b|\bfor\s+\w+\s+and\s+\w+\s+separately\b', query_lower):
#         sg_pattern = re.search(
#             r'\bfor\s+([a-z\s]+?)\s+and\s+([a-z\s]+?)\s+separately\b',
#             query_lower
#         )
        
#         if sg_pattern:
#             sg1 = sg_pattern.group(1).strip()
#             sg2 = sg_pattern.group(2).strip()
            
#             # Clean up
#             exclude_words = ['cheque', 'bounce', 'total', 'wave', 'city']
#             sg1_clean = ' '.join([w for w in sg1.split() if w not in exclude_words])
#             sg2_clean = ' '.join([w for w in sg2.split() if w not in exclude_words])
            
#             if sg1_clean and sg2_clean:
#                 result['has_and'] = True
#                 result['type'] = 'sales_group'
#                 result['entities'] = [
#                     {'name': sg1_clean, 'normalized': sg1_clean.replace(' ', '')},
#                     {'name': sg2_clean, 'normalized': sg2_clean.replace(' ', '')}
#                 ]
#                 return result


#     # Check for "month on month" or "mom" - should NOT be treated as "and" query
#     if re.search(r'\b(month on month|mom|month-on-month|monthonmonth)\b', query_lower):
#         return result
    
#     # Check for "quarter on quarter" or "qoq"
#     if re.search(r'\b(quarter on quarter|qoq|quarter-on-quarter|quarteronquarter)\b', query_lower):
#         return result
    
#     # Check for "year on year" or "yoy"
#     if re.search(r'\b(year on year|yoy|year-over-year|yearonyear)\b', query_lower):
#         return result
    
#     # ========== PRIORITY 1: DETECT MONTHS WITH "AND" ==========
#     month_pattern = r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+and\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
#     month_match = re.search(month_pattern, query_lower)
    
#     if month_match:
#         m1 = month_match.group(1)
#         m2 = month_match.group(2)
        
#         year_match = re.search(r'\b(20\d{2})\b', user_query)
#         year = int(year_match.group(1)) if year_match else None
        
#         result['has_and'] = True
#         result['type'] = 'month'
#         result['entities'] = [
#             {'name': m1.capitalize(), 'month': month_map[m1[:3]], 'year': year},
#             {'name': m2.capitalize(), 'month': month_map[m2[:3]], 'year': year}
#         ]
#         return result
    
#     # ========== PRIORITY 2: DETECT QUARTERS WITH "AND" ==========
#     quarter_pattern = r'\bQ([1-4])(?:\s+FY\s+(20\d{2}))?\s+and\s+Q([1-4])(?:\s+FY\s+(20\d{2}))?\b'
#     quarter_match = re.search(quarter_pattern, user_query, re.IGNORECASE)
    
#     if quarter_match:
#         q1 = int(quarter_match.group(1))
#         fy1 = int(quarter_match.group(2)) if quarter_match.group(2) else None
#         q2 = int(quarter_match.group(3))
#         fy2 = int(quarter_match.group(4)) if quarter_match.group(4) else None
        
#         fy_start, _ = get_financial_year_range()
#         default_fy = fy_start.year
        
#         result['has_and'] = True
#         result['type'] = 'quarter'
#         result['entities'] = [
#             {'name': f'Q{q1}', 'quarter': q1, 'fy': fy1 or default_fy},
#             {'name': f'Q{q2}', 'quarter': q2, 'fy': fy2 or default_fy}
#         ]
#         return result
    
#     # ========== PRIORITY 3: DETECT YEARS WITH "AND" ==========
#     year_pattern = r'\b(20\d{2})\s+and\s+(20\d{2})\b'
#     year_match = re.search(year_pattern, user_query)
    
#     if year_match:
#         y1 = int(year_match.group(1))
#         y2 = int(year_match.group(2))
        
#         result['has_and'] = True
#         result['type'] = 'year'
#         result['entities'] = [
#             {'name': str(y1), 'year': y1},
#             {'name': str(y2), 'year': y2}
#         ]
#         return result
    
#     # ========== PRIORITY 4: DETECT BANKS WITH "AND" ==========
#     # Pattern: "XXX bank and YYY bank" or "bank XXX and bank YYY"
#     bank_pattern = r'\b(?:(?:of|for|in)\s+)?([a-z]+(?:\s+[a-z]+)?)\s+bank\s+and\s+([a-z]+(?:\s+[a-z]+)?)\s+bank\b'
#     bank_match = re.search(bank_pattern, query_lower)
    
#     if bank_match:
#         b1 = bank_match.group(1).strip()
#         b2 = bank_match.group(2).strip()
        
#         result['has_and'] = True
#         result['type'] = 'bank'
#         result['entities'] = [
#             {'name': f'{b1} bank', 'search_term': b1.split()[0]},  # Use first word for search
#             {'name': f'{b2} bank', 'search_term': b2.split()[0]}
#         ]
#         return result
    
#     # ========== PRIORITY 5: DETECT SALES GROUPS WITH "AND" ==========
#     # This is the tricky one - we need to detect it intelligently
    
#     # First check if query mentions "sales group" or "sales grp"
#     has_sales_group_keyword = re.search(r'\b(sales\s+group|sales\s+grp)\b', query_lower)
    
#     # Pattern: "XXX and YYY" where XXX and YYY are multi-word phrases
#     # Exclude common words that aren't sales groups
#     exclude_words = ['cheque', 'bounce', 'total', 'gross', 'amount', 'basic', 'company', 'customer', 
#                      'wave', 'city', 'wmcc', 'estate', 'this', 'last', 'year', 'month', 'quarter']
    
#     # Look for pattern: "word1 word2... and word3 word4..."
#     # This captures phrases between trigger words and "and"
#     if has_sales_group_keyword or re.search(r'\bof\s+[a-z\s]+\s+and\s+[a-z\s]+', query_lower):
#         # Extract everything after keywords like "of", "for", "in"
#         context_match = re.search(
#             r'\b(?:of|for|in|wise)\s+((?:[a-z]+(?:\s+[a-z]+)*?))\s+and\s+((?:[a-z]+(?:\s+[a-z]+)*))',
#             query_lower
#         )
        
#         if context_match:
#             sg1 = context_match.group(1).strip()
#             sg2 = context_match.group(2).strip()
            
#             # Filter out exclude words
#             sg1_words = [w for w in sg1.split() if w not in exclude_words]
#             sg2_words = [w for w in sg2.split() if w not in exclude_words]
            
#             if sg1_words and sg2_words:
#                 sg1_clean = ' '.join(sg1_words)
#                 sg2_clean = ' '.join(sg2_words)
                
#                 result['has_and'] = True
#                 result['type'] = 'sales_group'
#                 result['entities'] = [
#                     {'name': sg1_clean, 'normalized': sg1_clean.replace(' ', '')},
#                     {'name': sg2_clean, 'normalized': sg2_clean.replace(' ', '')}
#                 ]
#                 return result
    
#     # ========== PRIORITY 6: DETECT CUSTOMER NAMES WITH "AND" ==========
#     # Pattern: customer-wise queries with "and"
#     if re.search(r'\bcustomer\s*(?:wise|name)\b', query_lower):
#         customer_pattern = r'\b(?:of|for|in)\s+([a-z\s]+?)\s+and\s+([a-z\s]+?)(?:\s+(?:customer|in|for|of|$))'
#         customer_match = re.search(customer_pattern, query_lower)
        
#         if customer_match:
#             c1 = customer_match.group(1).strip()
#             c2 = customer_match.group(2).strip()
            
#             # Clean up
#             c1_words = [w for w in c1.split() if w not in exclude_words]
#             c2_words = [w for w in c2.split() if w not in exclude_words]
            
#             if c1_words and c2_words:
#                 c1_clean = ' '.join(c1_words)
#                 c2_clean = ' '.join(c2_words)
                
#                 result['has_and'] = True
#                 result['type'] = 'customer'
#                 result['entities'] = [
#                     {'name': c1_clean, 'normalized': c1_clean.replace(' ', '')},
#                     {'name': c2_clean, 'normalized': c2_clean.replace(' ', '')}
#                 ]
#                 return result
    
#     return result


def detect_and_queries(user_query: str) -> dict:
    """
    Detects if user is asking for multiple separate periods with 'and'.
    Returns dict with type and entities to query separately.
    """
    query_lower = user_query.lower()
    
    result = {
        'has_and': False,
        'type': None,
        'entities': []
    }
    

    # ========== KNOWN SALES GROUPS (for disambiguation) ==========
    KNOWN_SALES_GROUPS = {
        'new plots', 'old plots', 'amore', 'veridia', 'eden', 'eligo', 
        'wave floors', 'armonia villa', 'dream bazaar', 'dream home',
        'wave floor 85', 'wave floor 89', 'executive floors', 'institutional',
        'verdia 3', 'verdia 4', 'verdia 5', 'verdia 6', 'verdia 7',
        'wave galleria', 'wave garden', 'ews_p2', 'lig_p2', 'prime floors',
        'swamanorath', 'hssc', 'lig_001_(310)', 'ews_001_(410)'
    }


    # Month map
    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
    }
    
    # Skip MoM, QoQ, YoY
    if re.search(r'\b(month on month|mom|quarter on quarter|qoq|year on year|yoy)\b', query_lower):
        return result

    # ========== PRIORITY 0.5: DETECT REMARKS QUERY ==========
    # Pattern: "remarks from X bank and Y bank" or "cheque bounce remarks from..."
    if re.search(r'\bremarks|chequebounce\s+reason?\b', query_lower):
        bank_pattern = r'\b(?:from|of)?\s*([a-z]+)\s+bank\s+and\s+([a-z]+)\s+bank\b'
        bank_match = re.search(bank_pattern, query_lower)
        
        if bank_match:
            b1 = bank_match.group(1).strip()
            b2 = bank_match.group(2).strip()
            
            result['has_and'] = True
            result['type'] = 'bank_remarks'
            result['entities'] = [
                {'name': f'{b1} bank', 'search_term': b1},
                {'name': f'{b2} bank', 'search_term': b2}
            ]
            return result    
    
    # ========== NEW PRIORITY 0: DETECT "YEAR WISE" FOR DATE RANGES ==========
    # Pattern: "from april to september year wise" or "apr to sep year wise"
    if re.search(r'\byear\s*wise\b', query_lower):
        # Check if it's a month range (e.g., "april to september")
        month_range = re.search(
            r'\b(?:from\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
            r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
            r'\s+(?:to|till|until|-)\s+'
            r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
            r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
            query_lower
        )
        
        if month_range:
            m1 = month_range.group(1)
            m2 = month_range.group(2)
            
            result['has_and'] = True
            result['type'] = 'year_wise_month_range'
            result['entities'] = {
                'start_month': month_map[m1[:3]],
                'end_month': month_map[m2[:3]],
                'start_month_name': m1.capitalize(),
                'end_month_name': m2.capitalize()
            }
            return result

    # ========== PRIORITY 1: DETECT QUARTERS WITH "AND" ==========
    # Pattern: "Q1 and Q2" OR "Q1 FY 2024 and Q2 FY 2024" OR "Q1 and Q2 2023"
    # Enhanced to detect year at the end: "q1 and q2 2023"
    quarter_pattern = r'\bq([1-4])(?:\s+fy\s+(20\d{2}))?\s+and\s+q([1-4])(?:\s+fy\s+(20\d{2}))?\b(?:\s+(20\d{2}))?'
    quarter_match = re.search(quarter_pattern, query_lower)
    
    if quarter_match:
        q1 = int(quarter_match.group(1))
        fy1 = quarter_match.group(2)
        q2 = int(quarter_match.group(3))
        fy2 = quarter_match.group(4)
        trailing_year = quarter_match.group(5)  # Year at the end like "q1 and q2 2023"
        
        fy_start, _ = get_financial_year_range()
        default_fy = fy_start.year
        
        # Determine FY for Q1
        if fy1:
            q1_fy = int(fy1)
        elif trailing_year:
            q1_fy = int(trailing_year)
        else:
            q1_fy = default_fy
        
        # Determine FY for Q2
        if fy2:
            q2_fy = int(fy2)
        elif trailing_year:
            q2_fy = int(trailing_year)
        else:
            q2_fy = default_fy
        
        result['has_and'] = True
        result['type'] = 'quarter'
        result['entities'] = [
            {'name': f'Q{q1} FY{q1_fy}', 'quarter': q1, 'fy': q1_fy},
            {'name': f'Q{q2} FY{q2_fy}', 'quarter': q2, 'fy': q2_fy}
        ]
        return result

    # ========== NEW: TWO DATES WITH CUSTOMER QUERY ==========
    # Pattern: "16 and 18 sep by customer" or "5 and 7 october customer wise"
    customer_two_dates = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:and|&|,)\s*(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
        r'.*?\b(by customer|customer wise|customerwise)\b',
        query_lower
    )
    
    if customer_two_dates:
        d1 = int(customer_two_dates.group(1))
        d2 = int(customer_two_dates.group(2))
        month_str = customer_two_dates.group(3).lower()
        
        m = month_map[month_str[:3]]
        
        # Infer year based on FY
        fy_start, fy_end = get_financial_year_range()
        inferred_year = fy_start.year if m >= 4 else fy_end.year
        
        result['has_and'] = True
        result['type'] = 'two_dates_customer'
        result['entities'] = [
            {'day': d1, 'month': m, 'year': inferred_year},
            {'day': d2, 'month': m, 'year': inferred_year}
        ]
        return result


    # ========== NEW PRIORITY 1: COMPANY/PROJECT WISE WITH MULTIPLE YEARS ==========
    # Pattern: "company wise 2024 and 2025" or "project wise in 2024 and 2025"
    if re.search(r'\b(company|project)\s*wise\b', query_lower):
        year_pattern = r'\b(20\d{2})\s+and\s+(20\d{2})\b'
        year_match = re.search(year_pattern, user_query)
        
        if year_match:
            y1 = int(year_match.group(1))
            y2 = int(year_match.group(2))
            
            result['has_and'] = True
            result['type'] = 'company_wise_years'
            result['entities'] = [
                {'name': f'FY {y1}', 'year': y1},
                {'name': f'FY {y2}', 'year': y2}
            ]
            return result
    
    # ========== PRIORITY 2: MONTH WITH DIFFERENT YEARS (e.g., "april 2024 and april 2025") ==========
    # Pattern: "month YYYY and month YYYY"
    month_year_pattern = r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\s+and\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b'
    month_year_match = re.search(month_year_pattern, query_lower)
    
    if month_year_match:
        m1 = month_year_match.group(1)
        y1 = int(month_year_match.group(2))
        m2 = month_year_match.group(3)
        y2 = int(month_year_match.group(4))
        
        result['has_and'] = True
        result['type'] = 'month'
        result['entities'] = [
            {'name': f'{m1.capitalize()} {y1}', 'month': month_map[m1[:3]], 'year': y1},
            {'name': f'{m2.capitalize()} {y2}', 'month': month_map[m2[:3]], 'year': y2}
        ]
        return result
    
    # ========== EXISTING: Company + Years with "separately" ==========
    company_year_pattern = re.search(
        r'\b(wave city|wmcc|wave estate)\b.*?\b(20\d{2})\s+and\s+(20\d{2})\b.*?\b(separately|separate)\b',
        query_lower
    )
    
    if company_year_pattern:
        company = company_year_pattern.group(1)
        y1 = int(company_year_pattern.group(2))
        y2 = int(company_year_pattern.group(3))
        
        # Map company to Comp_code
        comp_code_map = {
            'wave city': 1000,
            'wmcc': 1100,
            'wave estate': 1300
        }
        
        comp_code = comp_code_map.get(company)
        
        if comp_code:
            result['has_and'] = True
            result['type'] = 'company_year'
            result['entities'] = [
                {'name': f'{company.title()} FY{y1}', 'year': y1, 'comp_code': comp_code},
                {'name': f'{company.title()} FY{y2}', 'year': y2, 'comp_code': comp_code}
            ]
            return result
    
    # ========== ENHANCED: Sales group wise with "and" + "separately" ==========
    if re.search(r'\bsales group wise\b|\bfor\s+\w+\s+and\s+\w+\s+separately\b', query_lower):
        sg_pattern = re.search(
            r'\bfor\s+([a-z\s]+?)\s+and\s+([a-z\s]+?)\s+separately\b',
            query_lower
        )
        
        if sg_pattern:
            sg1 = sg_pattern.group(1).strip()
            sg2 = sg_pattern.group(2).strip()
            
            # Clean up
            exclude_words = ['cheque', 'bounce', 'total', 'wave', 'city']
            sg1_clean = ' '.join([w for w in sg1.split() if w not in exclude_words])
            sg2_clean = ' '.join([w for w in sg2.split() if w not in exclude_words])
            
            if sg1_clean and sg2_clean:
                result['has_and'] = True
                result['type'] = 'sales_group'
                result['entities'] = [
                    {'name': sg1_clean, 'normalized': sg1_clean.replace(' ', '')},
                    {'name': sg2_clean, 'normalized': sg2_clean.replace(' ', '')}
                ]
                return result

    

    

    # Check for "month on month" or "mom" - should NOT be treated as "and" query
    if re.search(r'\b(month on month|mom|month-on-month|monthonmonth)\b', query_lower):
        return result
    
    # Check for "quarter on quarter" or "qoq"
    if re.search(r'\b(quarter on quarter|qoq|quarter-on-quarter|quarteronquarter)\b', query_lower):
        return result
    
    # Check for "year on year" or "yoy"
    if re.search(r'\b(year on year|yoy|year-over-year|yearonyear)\b', query_lower):
        return result
    
    # ========== EXISTING PRIORITY: DETECT MONTHS WITH "AND" (SAME YEAR) ==========
    month_pattern = r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+and\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
    month_match = re.search(month_pattern, query_lower)
    
    if month_match:
        m1 = month_match.group(1)
        m2 = month_match.group(2)
        
        year_match = re.search(r'\b(20\d{2})\b', user_query)
        year = int(year_match.group(1)) if year_match else None
        
        result['has_and'] = True
        result['type'] = 'month'
        result['entities'] = [
            {'name': m1.capitalize(), 'month': month_map[m1[:3]], 'year': year},
            {'name': m2.capitalize(), 'month': month_map[m2[:3]], 'year': year}
        ]
        return result
    
    # ========== EXISTING: DETECT QUARTERS WITH "AND" ==========
    quarter_pattern = r'\bQ([1-4])(?:\s+FY\s+(20\d{2}))?\s+and\s+Q([1-4])(?:\s+FY\s+(20\d{2}))?\b'
    quarter_match = re.search(quarter_pattern, user_query, re.IGNORECASE)
    
    if quarter_match:
        q1 = int(quarter_match.group(1))
        fy1 = int(quarter_match.group(2)) if quarter_match.group(2) else None
        q2 = int(quarter_match.group(3))
        fy2 = int(quarter_match.group(4)) if quarter_match.group(4) else None
        
        fy_start, _ = get_financial_year_range()
        default_fy = fy_start.year
        
        result['has_and'] = True
        result['type'] = 'quarter'
        result['entities'] = [
            {'name': f'Q{q1}', 'quarter': q1, 'fy': fy1 or default_fy},
            {'name': f'Q{q2}', 'quarter': q2, 'fy': fy2 or default_fy}
        ]
        return result
    
    # ========== EXISTING: DETECT YEARS WITH "AND" ==========
    year_pattern = r'\b(20\d{2})\s+and\s+(20\d{2})\b'
    year_match = re.search(year_pattern, user_query)
    
    if year_match:
        y1 = int(year_match.group(1))
        y2 = int(year_match.group(2))
        
        result['has_and'] = True
        result['type'] = 'year'
        result['entities'] = [
            {'name': str(y1), 'year': y1},
            {'name': str(y2), 'year': y2}
        ]
        return result
    
    # ========== EXISTING: DETECT BANKS WITH "AND" ==========
    bank_pattern = r'\b(?:(?:of|for|in)\s+)?([a-z]+(?:\s+[a-z]+)?)\s+bank\s+and\s+([a-z]+(?:\s+[a-z]+)?)\s+bank\b'
    bank_match = re.search(bank_pattern, query_lower)
    
    if bank_match:
        b1 = bank_match.group(1).strip()
        b2 = bank_match.group(2).strip()
        
        result['has_and'] = True
        result['type'] = 'bank'
        result['entities'] = [
            {'name': f'{b1} bank', 'search_term': b1.split()[0]},
            {'name': f'{b2} bank', 'search_term': b2.split()[0]}
        ]
        return result
    
    # ========== EXISTING: DETECT SALES GROUPS WITH "AND" ==========
    # has_sales_group_keyword = re.search(r'\b(sales\s+group|sales\s+grp)\b', query_lower)
    
    # exclude_words = ['cheque', 'bounce', 'total', 'gross', 'amount', 'basic', 'company', 'customer', 
    #                  'wave', 'city', 'wmcc', 'estate', 'this', 'last', 'year', 'month', 'quarter']
    
    # if has_sales_group_keyword or re.search(r'\bof\s+[a-z\s]+\s+and\s+[a-z\s]+', query_lower):
    #     context_match = re.search(
    #         r'\b(?:of|for|in|wise)\s+((?:[a-z]+(?:\s+[a-z]+)*?))\s+and\s+((?:[a-z]+(?:\s+[a-z]+)*))',
    #         query_lower
    #     )
        
    #     if context_match:
    #         sg1 = context_match.group(1).strip()
    #         sg2 = context_match.group(2).strip()
            
    #         sg1_words = [w for w in sg1.split() if w not in exclude_words]
    #         sg2_words = [w for w in sg2.split() if w not in exclude_words]
            
    #         if sg1_words and sg2_words:
    #             sg1_clean = ' '.join(sg1_words)
    #             sg2_clean = ' '.join(sg2_words)
                
    #             result['has_and'] = True
    #             result['type'] = 'sales_group'
    #             result['entities'] = [
    #                 {'name': sg1_clean, 'normalized': sg1_clean.replace(' ', '')},
    #                 {'name': sg2_clean, 'normalized': sg2_clean.replace(' ', '')}
    #             ]
    #             return result
    
    # # ========== EXISTING: DETECT CUSTOMER NAMES WITH "AND" ==========
    # if re.search(r'\bcustomer\s*(?:wise|name)\b', query_lower):
    #     customer_pattern = r'\b(?:of|for|in)\s+([a-z\s]+?)\s+and\s+([a-z\s]+?)(?:\s+(?:customer|in|for|of|$))'
    #     customer_match = re.search(customer_pattern, query_lower)
        
    #     if customer_match:
    #         c1 = customer_match.group(1).strip()
    #         c2 = customer_match.group(2).strip()
            
    #         c1_words = [w for w in c1.split() if w not in exclude_words]
    #         c2_words = [w for w in c2.split() if w not in exclude_words]
            
    #         if c1_words and c2_words:
    #             c1_clean = ' '.join(c1_words)
    #             c2_clean = ' '.join(c2_words)
                
    #             result['has_and'] = True
    #             result['type'] = 'customer'
    #             result['entities'] = [
    #                 {'name': c1_clean, 'normalized': c1_clean.replace(' ', '')},
    #                 {'name': c2_clean, 'normalized': c2_clean.replace(' ', '')}
    #             ]
    #             return result
    
    # return result
    # # ========== SMART DETECTION: CUSTOMER vs SALES GROUP WITH "AND" ==========
    # # Check if query has "of X and Y" pattern
    # of_and_pattern = re.search(
    #     r'\bof\s+([a-z\s]+?)\s+and\s+([a-z\s]+?)(?:\s*(?:$|customer|sales|group|in|for))',
    #     query_lower
    # )

    # if of_and_pattern:
    #     entity1 = of_and_pattern.group(1).strip()
    #     entity2 = of_and_pattern.group(2).strip()
        
    #     # Clean up
    #     exclude_words = ['cheque', 'bounce', 'total', 'gross', 'amount', 'basic', 
    #                     'company', 'customer', 'wave', 'city', 'wmcc', 'estate', 
    #                     'this', 'last', 'year', 'month', 'quarter', 'details', 'data']
        
    #     e1_words = [w for w in entity1.split() if w not in exclude_words]
    #     e2_words = [w for w in entity2.split() if w not in exclude_words]
        
    #     if e1_words and e2_words:
    #         e1_clean = ' '.join(e1_words)
    #         e2_clean = ' '.join(e2_words)
            
    #         # ========== DISAMBIGUATION LOGIC ==========
            
    #         # Priority 1: Check for explicit "sales group wise" or "product wise"
    #         if re.search(r'\b(sales\s+group\s*wise|product\s*wise|sales\s*wise)\b', query_lower):
    #             # Definitely sales group
    #             result['has_and'] = True
    #             result['type'] = 'sales_group'
    #             result['entities'] = [
    #                 {'name': e1_clean, 'normalized': e1_clean.replace(' ', '')},
    #                 {'name': e2_clean, 'normalized': e2_clean.replace(' ', '')}
    #             ]
    #             return result
            
    #         # Priority 2: Check for explicit "customer" keyword
    #         if re.search(r'\b(customer\s*wise|customer\s+name|by\s+customer|details)\b', query_lower):
    #             # Definitely customer
    #             result['has_and'] = True
    #             result['type'] = 'customer'
    #             result['entities'] = [
    #                 {'name': e1_clean, 'normalized': e1_clean.replace(' ', '')},
    #                 {'name': e2_clean, 'normalized': e2_clean.replace(' ', '')}
    #             ]
    #             return result
            
    #         # Priority 3: Check if names match KNOWN_SALES_GROUPS
    #         e1_normalized = e1_clean.lower().replace(' ', '')
    #         e2_normalized = e2_clean.lower().replace(' ', '')
            
    #         # Check if both entities are in known sales groups
    #         e1_is_sales_group = any(
    #             sg.replace(' ', '') in e1_normalized or e1_normalized in sg.replace(' ', '')
    #             for sg in KNOWN_SALES_GROUPS
    #         )
    #         e2_is_sales_group = any(
    #             sg.replace(' ', '') in e2_normalized or e2_normalized in sg.replace(' ', '')
    #             for sg in KNOWN_SALES_GROUPS
    #         )
            
    #         if e1_is_sales_group and e2_is_sales_group:
    #             # Both are known sales groups
    #             result['has_and'] = True
    #             result['type'] = 'sales_group'
    #             result['entities'] = [
    #                 {'name': e1_clean, 'normalized': e1_clean.replace(' ', '')},
    #                 {'name': e2_clean, 'normalized': e2_clean.replace(' ', '')}
    #             ]
    #             return result
            
    #         # Priority 4: Default to CUSTOMER if ambiguous
    #         # (Most "of X and Y" queries are for customers)
    #             result['has_and'] = True
    #             result['type'] = 'customer'
    #             result['entities'] = [
    #                 {'name': e1_clean, 'normalized': e1_clean.lower()},  # ⭐ Keep spaces, just lowercase
    #                 {'name': e2_clean, 'normalized': e2_clean.lower()}   # ⭐ Keep spaces, just lowercase
    #             ]
    #         return result

    # ========== SMART DETECTION: CUSTOMER vs SALES GROUP WITH "AND" ==========
    # Check if query has "of X and Y" pattern
    of_and_pattern = re.search(
        r'\bof\s+([a-z\s]+?)\s+and\s+([a-z\s]+?)(?:\s*(?:$|customer|sales|group|in|for))',
        query_lower
    )

    if of_and_pattern:
        entity1 = of_and_pattern.group(1).strip()
        entity2 = of_and_pattern.group(2).strip()
        
        # Clean up
        exclude_words = ['cheque', 'bounce', 'total', 'gross', 'amount', 'basic', 
                        'company', 'customer', 'wave', 'city', 'wmcc', 'estate', 
                        'this', 'last', 'year', 'month', 'quarter', 'details', 'data']
        
        e1_words = [w for w in entity1.split() if w not in exclude_words]
        e2_words = [w for w in entity2.split() if w not in exclude_words]
        
        if e1_words and e2_words:
            e1_clean = ' '.join(e1_words)
            e2_clean = ' '.join(e2_words)
            
            # ========== DISAMBIGUATION LOGIC ==========
            
            # Priority 1: Check for explicit "sales group wise" or "product wise"
            if re.search(r'\b(sales\s+group\s*wise|product\s*wise|sales\s*wise)\b', query_lower):
                # Definitely sales group
                result['has_and'] = True
                result['type'] = 'sales_group'
                result['entities'] = [
                    {'name': e1_clean, 'normalized': e1_clean.replace(' ', '')},
                    {'name': e2_clean, 'normalized': e2_clean.replace(' ', '')}
                ]
                return result
            
            # Priority 2: Check for explicit "customer" keyword
            if re.search(r'\b(customer\s*wise|customer\s+name|by\s+customer|details)\b', query_lower):
                # Definitely customer
                result['has_and'] = True
                result['type'] = 'customer'
                result['entities'] = [
                    {'name': e1_clean, 'normalized': e1_clean.replace(' ', '')},
                    {'name': e2_clean, 'normalized': e2_clean.replace(' ', '')}
                ]
                return result
            
            # Priority 3: Check if names match KNOWN_SALES_GROUPS
            e1_normalized = e1_clean.lower().replace(' ', '')
            e2_normalized = e2_clean.lower().replace(' ', '')
            
            # Check if both entities are in known sales groups
            e1_is_sales_group = any(
                sg.replace(' ', '') in e1_normalized or e1_normalized in sg.replace(' ', '')
                for sg in KNOWN_SALES_GROUPS
            )
            e2_is_sales_group = any(
                sg.replace(' ', '') in e2_normalized or e2_normalized in sg.replace(' ', '')
                for sg in KNOWN_SALES_GROUPS
            )
            
            if e1_is_sales_group and e2_is_sales_group:
                # Both are known sales groups
                result['has_and'] = True
                result['type'] = 'sales_group'
                result['entities'] = [
                    {'name': e1_clean, 'normalized': e1_clean.replace(' ', '')},
                    {'name': e2_clean, 'normalized': e2_clean.replace(' ', '')}
                ]
                return result
            
            # Priority 4: Default to CUSTOMER if ambiguous
            # (Most "of X and Y" queries are for customers)
            # ⭐⭐⭐ FIX: Removed indentation error
            result['has_and'] = True
            result['type'] = 'customer'
            result['entities'] = [
                {'name': e1_clean, 'normalized': e1_clean.lower()},  # ⭐ Keep spaces, just lowercase
                {'name': e2_clean, 'normalized': e2_clean.lower()}   # ⭐ Keep spaces, just lowercase
            ]
            return result
    
    # ⭐⭐⭐ CRITICAL: Always return result at the end of the function
    return result

def detect_query_type(user_query: str) -> dict:
    """
    Detects the type of query and returns appropriate metadata.
    
    Returns:
        dict with keys:
        - query_type: 'quarter_total', 'quarter_mom', 'month_mom', 'year_yoy', etc.
        - quarter: quarter number (1-4) if applicable
        - fy_year: financial year if specified
        - needs_breakdown: True if month-by-month needed
    """
    query_lower = user_query.lower()
    
    # ========== PRIORITY 1: Month-on-Month Detection ==========
    # Must check BEFORE quarter detection to avoid conflicts
    if re.search(r'\b(month on month|mom|month-on-month|monthonmonth)\b', query_lower):
        # Check if it's for a specific quarter
        q_match = re.search(r'\bq([1-4])\b', query_lower, re.IGNORECASE)
        if q_match:
            q_num = int(q_match.group(1))
            
            # ⭐⭐⭐ FIXED: Better year detection for quarters
            fy_year = None
            
            # Check for "last year"
            if re.search(r'\blast year\b', query_lower):
                fy_start, _ = get_financial_year_range()
                fy_year = fy_start.year - 1
            # Check for explicit FY year
            elif re.search(r'\bfy\s*(20\d{2})\b', query_lower, re.IGNORECASE):
                fy_match = re.search(r'\bfy\s*(20\d{2})\b', query_lower, re.IGNORECASE)
                fy_year = int(fy_match.group(1))
            # Check for standalone year (e.g., "Q1 2024")
            elif re.search(r'\bq[1-4]\s+(20\d{2})\b', query_lower, re.IGNORECASE):
                year_match = re.search(r'\bq[1-4]\s+(20\d{2})\b', query_lower, re.IGNORECASE)
                fy_year = int(year_match.group(1))
            # Default to current FY
            else:
                fy_year = get_financial_year_range()[0].year
            
            return {
                'query_type': 'quarter_mom',
                'quarter': q_num,
                'fy_year': fy_year,
                'needs_breakdown': True
            }
        else:
            return {
                'query_type': 'month_mom',
                'needs_breakdown': True
            }
    
    # ========== PRIORITY 2: Quarter Total (Single Result) ==========
    # ⭐⭐⭐ FIXED: Enhanced quarter detection with multiple year patterns
    q_match = re.search(r'\bq([1-4])\b', query_lower, re.IGNORECASE)
    if q_match:
        q_num = int(q_match.group(1))
        fy_year = None
        
        # Priority 1: Check for "last year"
        if re.search(r'\blast year\b', query_lower):
            fy_start, _ = get_financial_year_range()
            fy_year = fy_start.year - 1
        
        # Priority 2: Check for "FY YYYY" format
        elif re.search(r'\bfy\s*(20\d{2})\b', query_lower, re.IGNORECASE):
            fy_match = re.search(r'\bfy\s*(20\d{2})\b', query_lower, re.IGNORECASE)
            fy_year = int(fy_match.group(1))
        
        # Priority 3: Check for "Q1 2024" or "Q1 of 2024" format
        elif re.search(r'\bq[1-4]\s+(?:of\s+)?(20\d{2})\b', query_lower, re.IGNORECASE):
            year_match = re.search(r'\bq[1-4]\s+(?:of\s+)?(20\d{2})\b', query_lower, re.IGNORECASE)
            fy_year = int(year_match.group(1))
        
        # Priority 4: Check for standalone year anywhere in query
        elif re.search(r'\b(20\d{2})\b', user_query):
            year_match = re.search(r'\b(20\d{2})\b', user_query)
            fy_year = int(year_match.group(1))
        
        # Default to current FY
        else:
            fy_year = get_financial_year_range()[0].year
        
        return {
            'query_type': 'quarter_total',
            'quarter': q_num,
            'fy_year': fy_year,
            'needs_breakdown': False
        }
    
    # ========== PRIORITY 3: Quarter-on-Quarter ==========
    if re.search(r'\b(quarter on quarter|qoq|quarter-on-quarter)\b', query_lower):
        return {
            'query_type': 'quarter_qoq',
            'needs_breakdown': False
        }
    
    # ========== PRIORITY 4: Year-on-Year ==========
    if re.search(r'\b(year on year|yoy|year-over-year)\b', query_lower):
        return {
            'query_type': 'year_yoy',
            'needs_breakdown': False
        }
    
    # Default: simple query
    return {
        'query_type': 'simple',
        'needs_breakdown': False
    }


def build_quarter_sql(query_type_info: dict, user_query: str) -> str:
    """
    Builds SQL specifically for quarter queries.
    """
    q_num = query_type_info['quarter']
    fy_year = query_type_info['fy_year']
    
    # ⭐⭐⭐ FIXED: Correct quarter date calculation
    def get_quarter_dates(q, fy):
        """
        Returns start and end dates for Indian FY quarters.
        FY starts April 1 and ends March 31.
        """
        if q == 1:
            # Q1: April-June of FY year
            return date(fy, 4, 1), date(fy, 6, 30)
        elif q == 2:
            # Q2: July-September of FY year
            return date(fy, 7, 1), date(fy, 9, 30)
        elif q == 3:
            # Q3: October-December of FY year
            return date(fy, 10, 1), date(fy, 12, 31)
        else:  # Q4
            # Q4: January-March of NEXT year
            return date(fy + 1, 1, 1), date(fy + 1, 3, 31)
    
    start_date, end_date = get_quarter_dates(q_num, fy_year)
    
    # Amount expression
    amount_expr = """ABS(
        SUM(
            CAST(
                CASE
                    WHEN TRIM("Gross_amount") LIKE '%-' 
                    THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
                    ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
                END AS DOUBLE
            )
        )
    )"""
    
    # ========== CASE 1: Month-on-Month within Quarter ==========
    if query_type_info['query_type'] == 'quarter_mom':
        sql = f"""SELECT 
    EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS year_num,
    EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS month_num,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY 
    EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')),
    EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d'))
ORDER BY year_num, month_num"""
        
        return sql
    
    # ========== CASE 2: Quarter Total (Single Combined Result) ==========
    elif query_type_info['query_type'] == 'quarter_total':
        sql = f"""SELECT 
    'Q{q_num} FY{fy_year}' AS quarter,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
        
        return sql
    
    return None



# def generate_sql_fixed(req: QueryRequest):
#     """
#     Fixed SQL generation with proper quarter detection.
#     """
    
#     # Step 2: Detect query type (existing code)
#     query_info = detect_query_type(req.question)
    
#     # Step 3: Handle quarter queries specially (existing code)
#     if query_info['query_type'] in ['quarter_total', 'quarter_mom']:
#         sql_query = build_quarter_sql(query_info, req.question)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # Step 4: Check for "and" queries (existing code)
#     and_detection = detect_and_queries(req.question)
#     if and_detection['has_and']:
#         sql_query = build_union_query_for_and(req.question, and_detection)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    

#     # ========== NEW: STEP 1 - Check for comparison queries FIRST ==========
#     comparison_info = detect_comparison_query(req.question)
#     if comparison_info['is_comparison']:
#         sql_query = build_comparison_query(req.question, comparison_info, CATALOG, SCHEMA, TABLE_NAME)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}

#     # Step 5: Generate SQL using LLM for other queries (existing code)
#     raw_sql = nl_to_sql(req.question)
#     sql_query = normalize_sql(raw_sql)
    
#     # Step 6: Apply financial year enforcement (existing code)
#     sql_query = enforce_financial_year(sql_query, req.question)
    
#     try:
#         data = run_presto_query(sql_query)
#     except Exception as e:
#         data = [{"error": str(e)}]
    
#     return {"sql": sql_query, "data": data}



# def generate_sql_fixed(req):
#     """
#     ⭐⭐⭐ CRITICAL: Don't call enforce_financial_year() on UNION queries
#     """
#     # Check comparison queries
#     comparison_info = detect_comparison_query(req.question)
#     if comparison_info['is_comparison']:
#         sql_query = build_comparison_query(req.question, comparison_info, CATALOG, SCHEMA, TABLE_NAME)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # Check quarter queries
#     query_info = detect_query_type(req.question)
    
#     if query_info['query_type'] in ['quarter_total', 'quarter_mom']:
#         sql_query = build_quarter_sql(query_info, req.question)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # ⭐⭐⭐ CRITICAL: Check "and" queries with safety check
#     and_detection = detect_and_queries(req.question)
    
#     # ✅ Add None check
#     if and_detection is not None and and_detection.get('has_and', False):
#         sql_query = build_union_query_for_and(req.question, and_detection)
        
#         if sql_query:
#             # ⭐⭐⭐ DO NOT call enforce_financial_year() here!
#             # The queries already have proper date filters
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # LLM fallback
#     raw_sql = nl_to_sql(req.question)
#     sql_query = normalize_sql(raw_sql)
    
#     # ⭐ Only call enforce_financial_year() for non-UNION queries
#     sql_query = enforce_financial_year(sql_query, req.question)
    
#     try:
#         data = run_presto_query(sql_query)
#         data = add_total_row(data)
#     except Exception as e:
#         data = [{"error": str(e)}]
    
#     return {"sql": sql_query, "data": data}

# def build_qoq_comparison_sql(user_query: str, comparison_info: dict) -> str:
#     """
#     Builds QoQ SQL with FY-correct quarters, supporting comparison periods
#     and optional sales group / project filters.
#     """
#     # --- Amount expression ---
#     amount_expr = """ABS(
#         SUM(
#             CAST(
#                 CASE
#                     WHEN TRIM("Gross_amount") LIKE '%-' 
#                     THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
#                     ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
#                 END AS DOUBLE
#             )
#         )
#     )"""

#     # --- FY quarter CASE expressions ---
#     fy_year_expr = (
#         'CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) >= 4 '
#         'THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) '
#         'ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) - 1 END'
#     )
#     quarter_expr = (
#         'CASE '
#         'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN 1 '
#         'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN 2 '
#         'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN 3 '
#         'ELSE 4 END'
#     )

#     # --- Detect optional filters ---
#     query_lower = user_query.lower()

#     # Sales group filter
#     sales_group_filter = ""
#     known_groups = [
#         'eligo', 'eden', 'amore', 'veridia', 'new plots', 'old plots',
#         'wave floors', 'armonia villa', 'dream bazaar', 'wave galleria',
#         'dream home', 'executive floors', 'institutional', 'prime floors'
#     ]
#     for grp in known_groups:
#         if grp in query_lower:
#             normalized = grp.replace(' ', '')
#             sales_group_filter = (
#                 f'AND LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), \'\\\\s+\', \'\')) '
#                 f'LIKE \'%{normalized}%\''
#             )
#             break

#     # Project / company filter
#     project_filter = ""
#     if 'wave city' in query_lower:
#         project_filter = 'AND "Comp_code" = 1000'
#     elif 'wmcc' in query_lower:
#         project_filter = 'AND "Comp_code" = 1100'
#     elif 'wave estate' in query_lower:
#         project_filter = 'AND "Comp_code" = 1300'

#     # Combine extra filters
#     extra_filters = f"{project_filter} {sales_group_filter}".strip()

#     queries = []

#     for period in comparison_info['periods']:
#         period_name = period['name']
#         start_date  = period['start_date']
#         end_date    = period['end_date']

#         query = f"""SELECT
#     '{period_name}' AS comparison_period,
#     {fy_year_expr} AS fy_year,
#     {quarter_expr} AS quarter_num,
#     {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')
#       BETWEEN DATE '{start_date}' AND DATE '{end_date}'
# {extra_filters}
# GROUP BY
#     '{period_name}',
#     {fy_year_expr},
#     {quarter_expr}
# ORDER BY fy_year, quarter_num"""

#         queries.append(query)

#     if queries:
#         return "\nUNION ALL\n".join(queries)
#     return None

def build_qoq_comparison_sql(user_query: str, comparison_info: dict) -> str:
    amount_expr = """ABS(
        SUM(
            CAST(
                CASE
                    WHEN TRIM("Gross_amount") LIKE '%-' 
                    THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
                    ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
                END AS DOUBLE
            )
        )
    )"""

    quarter_label_expr = (
        'CASE '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN \'Q1\' '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN \'Q2\' '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN \'Q3\' '
        'ELSE \'Q4\' END'
    )

    quarter_num_expr = (
        'CASE '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN 1 '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN 2 '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN 3 '
        'ELSE 4 END'
    )

    query_lower = user_query.lower()

    # --- Sales group filter ---
    sales_group_filter = ""
    known_groups = [
        'eligo', 'eden', 'amore', 'veridia', 'new plots', 'old plots', 'comm booth', 'commercial plots','ews_001_(410)','wave galleria','wave garden','fsi',
        'wave floor', 'armonia villa', 'dream bazaar', 'wave galleria', 'wave galleria','ews_p2','executive floors','harmony greens','hssc','institutional','prime floors','lig_001_(310)','lig_p2','mayfair parks',
        'dream homes', 'executive floors', 'institutional', 'prime floors','new plots','old plots','plots-comm','plots-res','plots-res-if','sco','swamanorath','veridia-3','veridia-4','veridia-5','veridia-6','veridia-7','wave city-rental','wave estate, gh2 ph2','wave floor 85','wave floor 99'
    ]
    # Sort longest first so "wave floor 85" matches before "wave floors"
    sorted_groups = sorted(known_groups, key=len, reverse=True)
    for grp in sorted_groups:
        if grp in query_lower:
            like_with_space = grp.lower()
            like_no_space = grp.lower().replace(' ', '')
            sales_group_filter = (
                f'AND ('
                f'LOWER(TRIM("sales_grp_descp")) LIKE \'%{like_with_space}%\' '
                f'OR LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), \'\\\\s+\', \'\')) LIKE \'%{like_no_space}%\''
                f')'
            )
            break

    # --- Project filter ---
    project_filter = ""
    if 'wave city' in query_lower:
        project_filter = 'AND "Comp_code" = 1000'
    elif 'wmcc' in query_lower:
        project_filter = 'AND "Comp_code" = 1100'
    elif 'wave estate' in query_lower:
        project_filter = 'AND "Comp_code" = 1300'

    extra_filters = f"{project_filter} {sales_group_filter}".strip()

    queries = []

    for period in comparison_info['periods']:
        period_name = period['name']
        start_date  = period['start_date']
        end_date    = period['end_date']

        # ⭐ NO ORDER BY here — it goes only at the very end
        query = f"""SELECT
    '{period_name}' AS period,
    {quarter_label_expr} AS quarter,
    {quarter_num_expr} AS quarter_num,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')
      BETWEEN DATE '{start_date}' AND DATE '{end_date}'
      {extra_filters}
GROUP BY
    {quarter_label_expr},
    {quarter_num_expr}"""

        queries.append(query)

    if queries:
        # ⭐ Single ORDER BY at the very end only
        full_query = "\nUNION ALL\n".join(queries)
        full_query += "\nORDER BY period DESC, quarter_num"
        return full_query

    return None


def build_qoq_simple_sql(user_query: str) -> str:
    """
    Builds a simple QoQ SQL (no comparison) with correct FY quarters.
    Both year_num and quarter_num are in SELECT and GROUP BY.
    """
    amount_expr = """ABS(
        SUM(
            CAST(
                CASE
                    WHEN TRIM("Gross_amount") LIKE '%-' 
                    THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
                    ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
                END AS DOUBLE
            )
        )
    )"""

    fy_year_expr = (
        'CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) >= 4 '
        'THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) '
        'ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) - 1 END'
    )

    quarter_label_expr = (
        'CASE '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN \'Q1\' '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN \'Q2\' '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN \'Q3\' '
        'ELSE \'Q4\' END'
    )

    quarter_num_expr = (
        'CASE '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN 1 '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN 2 '
        'WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN 3 '
        'ELSE 4 END'
    )

    query_lower = user_query.lower()

    # --- Sales group filter ---
    sales_group_filter = ""
    known_groups = [
        'eligo', 'eden', 'amore', 'veridia', 'new plots', 'old plots', 'comm booth', 'commercial plots','ews_001_(410)','wave galleria','wave garden','fsi',
        'wave floor', 'armonia villa', 'dream bazaar', 'wave galleria', 'wave galleria','ews_p2','executive floors','harmony greens','hssc','institutional','prime floors','lig_001_(310)','lig_p2','mayfair parks',
        'dream homes', 'executive floors', 'institutional', 'prime floors','new plots','old plots','plots-comm','plots-res','plots-res-if','sco','swamanorath','veridia-3','veridia-4','veridia-5','veridia-6','veridia-7','wave city-rental','wave estate, gh2 ph2','wave floor 85','wave floor 99'
    ]
    # Sort longest first so "wave floor 85" matches before "wave floors"
    sorted_groups = sorted(known_groups, key=len, reverse=True)
    for grp in sorted_groups:
        if grp in query_lower:
            like_with_space = grp.lower()
            like_no_space = grp.lower().replace(' ', '')
            sales_group_filter = (
                f'AND ('
                f'LOWER(TRIM("sales_grp_descp")) LIKE \'%{like_with_space}%\' '
                f'OR LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), \'\\\\s+\', \'\')) LIKE \'%{like_no_space}%\''
                f')'
            )
            break

    # --- Project filter ---
    project_filter = ""
    if 'wave city' in query_lower:
        project_filter = 'AND "Comp_code" = 1000'
    elif 'wmcc' in query_lower:
        project_filter = 'AND "Comp_code" = 1100'
    elif 'wave estate' in query_lower:
        project_filter = 'AND "Comp_code" = 1300'

    extra_filters = f"{project_filter} {sales_group_filter}".strip()

    # --- Date range ---
    # Apply enforce_financial_year to get the correct date range
    temp_sql = f'SELECT 1 FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
    temp_sql = enforce_financial_year(temp_sql, user_query)

    # Extract the date filter from temp_sql
    date_filter = ""
    date_match = re.search(
        r"DATE_PARSE\(TRIM\(CAST\(\"POST_date\" AS VARCHAR\)\),\s*'%Y%m%d'\)\s+"
        r"BETWEEN\s+DATE\s+'[^']+'\s+AND\s+DATE\s+'[^']+'",
        temp_sql
    )
    if date_match:
        date_filter = f"AND {date_match.group(0)}"
    else:
        # Default to current FY
        fy_start, fy_end = get_financial_year_range()
        date_filter = (
            f"AND DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        )

    sql = f"""SELECT
    {fy_year_expr} AS fy_year,
    {quarter_label_expr} AS quarter,
    {quarter_num_expr} AS quarter_num,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1
    {date_filter}
    {extra_filters}
GROUP BY
    {fy_year_expr},
    {quarter_label_expr},
    {quarter_num_expr}
ORDER BY fy_year, quarter_num"""

    return sql    

# def generate_sql_fixed(req):
#     query_lower = req.question.lower()

#     # ========== STEP 1: QoQ + Comparison (e.g. "QoQ of eligo this year vs last year") ==========
#     is_qoq = bool(re.search(r'\b(quarter on quarter|qoq|quarter-on-quarter)\b', query_lower))
#     comparison_info = detect_comparison_query(req.question)

#     if is_qoq and comparison_info['is_comparison']:
#         sql_query = build_qoq_comparison_sql(req.question, comparison_info)
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
#             return {"sql": sql_query, "data": data}

#     # ========== STEP 2: Plain comparison queries (v/s, vs, this year v/s last year) ==========
#     if comparison_info['is_comparison']:
#         sql_query = build_comparison_query(req.question, comparison_info, CATALOG, SCHEMA, TABLE_NAME)
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
#             return {"sql": sql_query, "data": data}

#     # ========== STEP 3: Quarter total / MoM within quarter ==========
#     query_info = detect_query_type(req.question)
#     if query_info['query_type'] in ['quarter_total', 'quarter_mom']:
#         sql_query = build_quarter_sql(query_info, req.question)
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
#             return {"sql": sql_query, "data": data}

#     # ========== STEP 4: "and" queries ==========
#     and_detection = detect_and_queries(req.question)
#     if and_detection is not None and and_detection.get('has_and', False):
#         sql_query = build_union_query_for_and(req.question, and_detection)
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
#             return {"sql": sql_query, "data": data}

#     # ========== STEP 5: LLM fallback ==========
#     raw_sql = nl_to_sql(req.question)
#     sql_query = normalize_sql(raw_sql)
#     sql_query = enforce_financial_year(sql_query, req.question)

#     try:
#         data = run_presto_query(sql_query)
#         data = add_total_row(data)
#     except Exception as e:
#         data = [{"error": str(e)}]

#     return {"sql": sql_query, "data": data}
def detect_bank_name_query(user_query: str) -> dict:
    print(f"DEBUG detect_bank_name_query called with: {user_query}")

    result = {
        'has_bank_filter': False,
        'exact_match': False,
        'bank_filter': ''
    }

    # ========== EXACT MATCH ==========
    exact_pattern = re.search(
        r'\b([A-Za-z]+)\s*BANK\s*[-\s]?\s*([A-Za-z0-9]+(?:[-][A-Za-z0-9]+)*)\b',
        user_query,
        re.IGNORECASE
    )

    print(f"DEBUG exact_pattern: {exact_pattern}")
    if exact_pattern:
        print(f"DEBUG g1='{exact_pattern.group(1)}' g2='{exact_pattern.group(2)}'")

    if exact_pattern:
        word_before = exact_pattern.group(1).strip()
        code_after  = exact_pattern.group(2).strip()

        if re.search(r'[0-9]', code_after):
            full_bank_name = f"{word_before} BANK {code_after}"
            # Strip all non-alphanumeric for flexible matching
            clean_name = re.sub(r'[^a-zA-Z0-9]', '', full_bank_name).lower()
            print(f"DEBUG clean_name: '{clean_name}'")

            result['has_bank_filter'] = True
            result['exact_match'] = True
            # Single clean condition — no nested quotes
            result['bank_filter'] = (
                "LOWER(REGEXP_REPLACE(TRIM(\"Bank_name\"), '[^a-zA-Z0-9]', '')) "
                f"= '{clean_name}'"
            )
            print(f"DEBUG exact bank_filter: {result['bank_filter']}")
            return result

    # ========== PARTIAL MATCH ==========
    partial_pattern = re.search(
        r'\b([A-Za-z]+)\s+bank\b',
        user_query,
        re.IGNORECASE
    )

    print(f"DEBUG partial_pattern: {partial_pattern}")

    if partial_pattern:
        bank_keyword = partial_pattern.group(1).strip().lower()
        print(f"DEBUG bank_keyword: '{bank_keyword}'")

        exclude = {
            'this', 'last', 'the', 'a', 'any', 'all', 'wise', 'by',
            'each', 'every', 'which', 'what', 'total', 'show', 'give',
            'get', 'find', 'fetch', 'display', 'list', 'central', 'state',
            'cheque', 'bounce', 'data', 'of', 'for', 'in', 'from'
        }

        if bank_keyword not in exclude and len(bank_keyword) > 1:
            result['has_bank_filter'] = True
            result['exact_match'] = False
            result['bank_filter'] = (
                "LOWER(REGEXP_REPLACE(TRIM(\"Bank_name\"), '[^a-zA-Z0-9]', '')) "
                f"LIKE '%{bank_keyword}%'"
            )
            print(f"DEBUG partial bank_filter: {result['bank_filter']}")
            return result

    print(f"DEBUG no bank filter detected")
    return result

def build_bank_filter_sql(user_query: str, bank_info: dict) -> str:
    amount_expr = """ABS(
        SUM(
            CAST(
                CASE
                    WHEN TRIM("Gross_amount") LIKE '%-' 
                    THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
                    ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
                END AS DOUBLE
            )
        )
    )"""

    query_lower = user_query.lower()

    # --- Date filter ---
    temp_sql = f'SELECT 1 FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
    temp_sql = enforce_financial_year(temp_sql, user_query)
    date_filter = ""
    date_match = re.search(
        r"DATE_PARSE\(TRIM\(CAST\(\"POST_date\" AS VARCHAR\)\),\s*'%Y%m%d'\)\s+"
        r"BETWEEN\s+DATE\s+'[^']+'\s+AND\s+DATE\s+'[^']+'",
        temp_sql
    )
    if date_match:
        date_filter = f"AND {date_match.group(0)}"
    else:
        fy_start, fy_end = get_financial_year_range()
        date_filter = (
            f"AND DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        )

    # --- Project filter ---
    project_filter = ""
    if 'wave city' in query_lower:
        project_filter = 'AND "Comp_code" = 1000'
    elif 'wmcc' in query_lower:
        project_filter = 'AND "Comp_code" = 1100'
    elif 'wave estate' in query_lower:
        project_filter = 'AND "Comp_code" = 1300'

    # --- Sales group filter ---
    sales_group_filter = ""
    known_groups = [
        'eligo', 'eden', 'amore', 'veridia', 'new plots', 'old plots', 'comm booth', 'commercial plots','ews_001_(410)','wave galleria','wave garden','fsi',
        'wave floor', 'armonia villa', 'dream bazaar', 'wave galleria', 'wave galleria','ews_p2','executive floors','harmony greens','hssc','institutional','prime floors','lig_001_(310)','lig_p2','mayfair parks',
        'dream homes', 'executive floors', 'institutional', 'prime floors','new plots','old plots','plots-comm','plots-res','plots-res-if','sco','swamanorath','veridia-3','veridia-4','veridia-5','veridia-6','veridia-7','wave city-rental','wave estate, gh2 ph2','wave floor 85','wave floor 99'
    ]

    for grp in known_groups:
        if grp in query_lower:
            like_with_space = grp.lower()
            like_no_space = grp.lower().replace(' ', '')
            sales_group_filter = (
                f'AND ('
                f'LOWER(TRIM("sales_grp_descp")) LIKE \'%{like_with_space}%\' '
                f'OR LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), \'\\\\s+\', \'\')) LIKE \'%{like_no_space}%\''
                f')'
            )
            break

    # --- SELECT columns ---
    if re.search(r'\bcustomer\s*wise\b|\bby\s+customer\b', query_lower):
        select_cols = '"Cust_no", "Cust_name", "Bank_name",'
        group_by = 'GROUP BY "Cust_no", "Cust_name", "Bank_name"'
        order_by = 'ORDER BY chequebounce DESC'
    elif re.search(r'\bbank\s*wise\b', query_lower):
        select_cols = '"Bank_name",'
        group_by = 'GROUP BY "Bank_name"'
        order_by = 'ORDER BY chequebounce DESC'
    else:
        select_cols = ''
        group_by = ''
        order_by = ''

    # ========== DETECT "LAST N YEARS" → SEPARATE RESULTS PER YEAR ==========
    last_n_years = re.search(
        r'\b(?:last|past|previous)\s+(\d+|two|three|four|five)\s+years?\b',
        query_lower,
        re.IGNORECASE
    )

    word_to_num = {'two': 2, 'three': 3, 'four': 4, 'five': 5}

    if last_n_years:
        n_str = last_n_years.group(1).lower()
        n = word_to_num.get(n_str, int(n_str) if n_str.isdigit() else 2)

        # Build one query per FY year
        fy_start, _ = get_financial_year_range()
        # Last completed FY end year
        end_fy_year = fy_start.year - 1  # e.g. 2024 means FY 2024-25

        queries = []
        for i in range(n):
            fy_year = end_fy_year - i          # 2024, 2023, 2022 ...
            start_date = date(fy_year, 4, 1)
            end_date   = date(fy_year + 1, 3, 31)
            period_label = f"FY {fy_year}-{str(fy_year + 1)[-2:]}"  # "FY 2024-25"

            query = f"""SELECT
    '{period_label}' AS period,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {bank_info['bank_filter']}
    AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')
        BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {project_filter}
    {sales_group_filter}"""

            queries.append(query)

        if queries:
            full_query = "\nUNION ALL\n".join(queries)
            full_query += "\nORDER BY period DESC"
            return full_query

    # ========== DETECT "2024 AND 2025" → SEPARATE RESULTS PER YEAR ==========
    two_years = re.search(r'\b(20\d{2})\s+and\s+(20\d{2})\b', user_query)
    if two_years:
        y1 = int(two_years.group(1))
        y2 = int(two_years.group(2))

        queries = []
        for yr in [y1, y2]:
            start_date = date(yr, 4, 1)
            end_date   = date(yr + 1, 3, 31)
            period_label = f"FY {yr}-{str(yr + 1)[-2:]}"

            query = f"""SELECT
    '{period_label}' AS period,
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {bank_info['bank_filter']}
    AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')
        BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {project_filter}
    {sales_group_filter}"""

            queries.append(query)

        if queries:
            full_query = "\nUNION ALL\n".join(queries)
            full_query += "\nORDER BY period"
            return full_query

    # ========== DEFAULT: SINGLE RESULT WITH DATE FROM enforce_financial_year ==========
    temp_sql = f'SELECT 1 FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
    temp_sql = enforce_financial_year(temp_sql, user_query)
    date_filter = ""
    date_match = re.search(
        r"DATE_PARSE\(TRIM\(CAST\(\"POST_date\" AS VARCHAR\)\),\s*'%Y%m%d'\)\s+"
        r"BETWEEN\s+DATE\s+'[^']+'\s+AND\s+DATE\s+'[^']+'",
        temp_sql
    )
    if date_match:
        date_filter = f"AND {date_match.group(0)}"
    else:
        fy_start, fy_end = get_financial_year_range()
        date_filter = (
            f"AND DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        )

    sql = f"""SELECT
    {select_cols}
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {bank_info['bank_filter']}
    {date_filter}
    {project_filter}
    {sales_group_filter}
    {group_by}
    {order_by}"""

    sql = re.sub(r'\n\s*\n', '\n', sql).strip()
    return sql








def generate_sql_fixed(req):
    query_lower = req.question.lower()

    is_qoq = bool(re.search(
        r'\b(quarter on quarter|qoq|quarter-on-quarter|quarter\s*wise|quarterwise|quarter\s*by\s*quarter)\b',
        query_lower
    ))
    comparison_info = detect_comparison_query(req.question)

    # ========== STEP 1: QoQ + Comparison ==========
    if is_qoq and comparison_info['is_comparison']:
        sql_query = build_qoq_comparison_sql(req.question, comparison_info)
        if sql_query:
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}

    # ========== STEP 2: Plain QoQ ==========
    if is_qoq:
        sql_query = build_qoq_simple_sql(req.question)
        if sql_query:
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}

    # ========== STEP 3: Plain comparison ==========
    if comparison_info['is_comparison']:
        sql_query = build_comparison_query(req.question, comparison_info, CATALOG, SCHEMA, TABLE_NAME)
        if sql_query:
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}

    # ========== STEP 4: Bank name filter (exact or partial) ==========
    bank_info = detect_bank_name_query(req.question)
    if bank_info['has_bank_filter']:
        sql_query = build_bank_filter_sql(req.question, bank_info)
        if sql_query:
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}

    # ========== STEP 5: Quarter total / MoM ==========
    query_info = detect_query_type(req.question)
    if query_info['query_type'] in ['quarter_total', 'quarter_mom']:
        sql_query = build_quarter_sql(query_info, req.question)
        if sql_query:
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}

    # ========== STEP 6: "and" queries ==========
    and_detection = detect_and_queries(req.question)
    if and_detection is not None and and_detection.get('has_and', False):
        sql_query = build_union_query_for_and(req.question, and_detection)
        if sql_query:
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}

    # ========== STEP 7: LLM fallback ==========
    raw_sql = nl_to_sql(req.question)
    sql_query = normalize_sql(raw_sql)
    sql_query = enforce_financial_year(sql_query, req.question)

    try:
        data = run_presto_query(sql_query)
        data = add_total_row(data)
    except Exception as e:
        data = [{"error": str(e)}]

    return {"sql": sql_query, "data": data}


    # # Step 1: Detect query type
    # query_info = detect_query_type(req.question)
    
    # # Step 2: Handle quarter queries specially
    # if query_info['query_type'] in ['quarter_total', 'quarter_mom']:
    #     sql_query = build_quarter_sql(query_info, req.question)
        
    #     if sql_query:
    #         try:
    #             data = run_presto_query(sql_query)
    #         except Exception as e:
    #             data = [{"error": str(e)}]
            
    #         return {"sql": sql_query, "data": data}
    
    # # Step 3: Check for "and" queries (april and august, etc.)
    # and_detection = detect_and_queries(req.question)
    # if and_detection['has_and']:
    #     sql_query = build_union_query_for_and(req.question, and_detection)
        
    #     if sql_query:
    #         try:
    #             data = run_presto_query(sql_query)
    #         except Exception as e:
    #             data = [{"error": str(e)}]
            
    #         return {"sql": sql_query, "data": data}
    
    # # Step 4: Generate SQL using LLM for other queries
    # raw_sql = nl_to_sql(req.question)
    # sql_query = normalize_sql(raw_sql)
    
    # # Step 5: Apply financial year enforcement
    # sql_query = enforce_financial_year(sql_query, req.question)
    
    # try:
    #     data = run_presto_query(sql_query)
    # except Exception as e:
    #     data = [{"error": str(e)}]
    
    # return {"sql": sql_query, "data": data}


# --------------------
# FastAPI App
# --------------------
app = FastAPI(title="chequebounce NL2SQL + Presto API")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    sql: str
    data: list

# ========== UPDATE YOUR FASTAPI ENDPOINT ==========
@app.post("/generate-sql", response_model=QueryResponse)
def generate_sql(req: QueryRequest):
    return generate_sql_fixed(req)




# def build_union_query_for_and(user_query: str, detection: dict) -> str:
#     """
#     Builds a UNION ALL query for separate period results.
#     """
#     if not detection['has_and']:
#         return None
    
#     # Base amount expression
#     amount_expr = """ABS(
#         SUM(
#             CAST(
#                 CASE
#                     WHEN TRIM("Gross_amount") LIKE '%-' 
#                     THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
#                     ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
#                 END AS DOUBLE
#             )
#         )
#     )"""
    
#     queries = []

#     # ========== NEW: Company-specific year queries ==========
#     if detection['type'] == 'company_year':
#         for entity in detection['entities']:
#             year = entity['year']
#             comp_code = entity['comp_code']
#             period_name = entity['name']
            
#             start_date = date(year, 4, 1)
#             end_date = date(year + 1, 3, 31)
            
#             query = f"""SELECT '{period_name}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE "Comp_code" = {comp_code}
#   AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
#             queries.append(query)
        
#         if queries:
#             return "\nUNION ALL\n".join(queries)
    

#     # Helper function to get date filter
#     def get_date_filter(user_query):
#         fy_start, fy_end = get_financial_year_range()
#         date_filter = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        
#         # Apply custom date logic if specified in query
#         temp_sql = f"SELECT {amount_expr} AS chequebounce FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" WHERE 1=1"
#         temp_sql_with_date = enforce_financial_year(temp_sql, user_query)
        
#         # Extract the date filter
#         date_match = re.search(
#             r"DATE_PARSE\(TRIM\(CAST\(\"POST_date\" AS VARCHAR\)\),\s*'%Y%m%d'\)\s+BETWEEN\s+DATE\s+'[^']+'\s+AND\s+DATE\s+'[^']+'",
#             temp_sql_with_date
#         )
#         if date_match:
#             date_filter = date_match.group(0)
        
#         return date_filter



#     if detection['type'] == 'month':
#         fy_start, fy_end = get_financial_year_range()
        
#         for entity in detection['entities']:
#             month_num = entity['month']
#             month_name = entity['name']
            
#             # Determine year
#             if entity['year']:
#                 year = entity['year']
#             else:
#                 # Use FY logic
#                 year = fy_start.year if month_num >= 4 else fy_end.year
            
#             # Get last day of month
#             last_day = monthrange(year, month_num)[1]
#             start_date = date(year, month_num, 1)
#             end_date = date(year, month_num, last_day)
            
#             query = f"""SELECT '{month_name}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
#             queries.append(query)
    
#     elif detection['type'] == 'quarter':
#         def get_quarter_dates(q, fy_year):
#             if q == 1:
#                 return date(fy_year, 4, 1), date(fy_year, 6, 30)
#             elif q == 2:
#                 return date(fy_year, 7, 1), date(fy_year, 9, 30)
#             elif q == 3:
#                 return date(fy_year, 10, 1), date(fy_year, 12, 31)
#             else:  # Q4
#                 return date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
        
#         for entity in detection['entities']:
#             q_num = entity['quarter']
#             fy_year = entity['fy']
#             q_name = entity['name']
            
#             start_date, end_date = get_quarter_dates(q_num, fy_year)
            
#             query = f"""SELECT '{q_name}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
#             queries.append(query)
    
#     elif detection['type'] == 'year':
#         for entity in detection['entities']:
#             year = entity['year']
#             year_name = entity['name']
            
#             start_date = date(year, 4, 1)
#             end_date = date(year + 1, 3, 31)
            
#             query = f"""SELECT 'FY {year_name}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
#             queries.append(query)
    
#     if queries:
#         # ⭐ REMOVED SEMICOLON - Presto doesn't need it
#         full_query = "\nUNION ALL\n".join(queries)
#         return full_query
    

#     elif detection['type'] == 'sales_group':
#         date_filter = get_date_filter(user_query)
        
#         for entity in detection['entities']:
#             sg_name = entity['name']
#             sg_normalized = entity['normalized']
            
#             query = f"""SELECT '{sg_name.title()}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE {date_filter}
#   AND LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), '\\s+', '')) LIKE '%{sg_normalized}%'"""
            
#             queries.append(query)
    
#     # ========== HANDLE BANKS (DYNAMIC) ==========
#     elif detection['type'] == 'bank':
#         date_filter = get_date_filter(user_query)
        
#         for entity in detection['entities']:
#             bank_name = entity['name']
#             search_term = entity['search_term']
            
#             query = f"""SELECT '{bank_name.title()}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE {date_filter}
#   AND LOWER(REGEXP_REPLACE(TRIM("Bank_name"), '[^a-zA-Z ]', '')) LIKE '%{search_term}%'"""
            
#             queries.append(query)
    
#     # ========== HANDLE CUSTOMERS (DYNAMIC) ==========
#     elif detection['type'] == 'customer':
#         date_filter = get_date_filter(user_query)
        
#         for entity in detection['entities']:
#             cust_name = entity['name']
#             cust_normalized = entity['normalized']
            
#             query = f"""SELECT '{cust_name.title()}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE {date_filter}
#   AND LOWER(REGEXP_REPLACE(TRIM("Cust_name"), '[^a-zA-Z0-9 ]', '')) LIKE '%{cust_normalized}%'"""
            
#             queries.append(query)
    
#     if queries:
#         full_query = "\nUNION ALL\n".join(queries)
#         return full_query
    
    
#     return None

def build_union_query_for_and(user_query: str, detection: dict) -> str:
    """
    Builds a UNION ALL query for separate period results.
    """
    if not detection['has_and']:
        return None
    
    # Base amount expression
    amount_expr = """ABS(
        SUM(
            CAST(
                CASE
                    WHEN TRIM("Gross_amount") LIKE '%-' 
                    THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '')
                    ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '')
                END AS DOUBLE
            )
        )
    )"""
    
    queries = []
    
    # ========== NEW: HANDLE BANK REMARKS QUERIES ==========
    if detection['type'] == 'bank_remarks':
        # Get date filter
        date_filter_result = get_date_filter_for_query(user_query)
        
        for entity in detection['entities']:
            bank_name = entity['name']
            search_term = entity['search_term']
            
            # ⭐⭐⭐ CRITICAL: Return remarks, not aggregated amounts
            query = f"""SELECT 
    '{bank_name.title()}' AS bank_name,
    "Cust_name",
    "remarks"
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {date_filter_result}
  AND LOWER(REGEXP_REPLACE(TRIM("Bank_name"), '[^a-zA-Z ]', '')) LIKE '%{search_term}%'"""
            
            queries.append(query)
        
        if queries:
            return "\nUNION ALL\n".join(queries)

    # ========== NEW: HANDLE YEAR WISE MONTH RANGE ==========
    if detection['type'] == 'year_wise_month_range':
        start_month = detection['entities']['start_month']
        end_month = detection['entities']['end_month']
        
        # Get all available years from data (or use a reasonable range)
        current_year = date.today().year
        start_year = 2020  # Adjust based on your data
        
        for year in range(start_year, current_year + 1):
            # ⭐⭐⭐ CRITICAL FIX: Use Financial Year logic
            # For FY, if start_month >= 4, the FY starts in that year
            # Example: Apr 2024 to Sep 2024 is in FY 2024-25
            
            if start_month >= 4:
                # FY starts in April of this year
                fy_year = year
                start_date = date(fy_year, start_month, 1)
                
                if end_month >= start_month and end_month >= 4:
                    # Same FY year (e.g., Apr to Sep 2024)
                    end_date = date(fy_year, end_month, monthrange(fy_year, end_month)[1])
                elif end_month < 4:
                    # Crosses into next year (e.g., Oct 2024 to Mar 2025)
                    end_date = date(fy_year + 1, end_month, monthrange(fy_year + 1, end_month)[1])
                else:
                    end_date = date(fy_year, end_month, monthrange(fy_year, end_month)[1])
            else:
                # Start month is Jan-Mar, so it belongs to previous FY
                fy_year = year - 1
                start_date = date(year, start_month, 1)
                
                if end_month < 4:
                    # Both in Jan-Mar (same calendar year)
                    end_date = date(year, end_month, monthrange(year, end_month)[1])
                else:
                    # This shouldn't happen with proper FY logic, but handle it
                    end_date = date(year, end_month, monthrange(year, end_month)[1])
            
            # Label with FY year (not calendar year)
            query = f"""SELECT 'FY {fy_year}-{str(fy_year + 1)[-2:]}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
        
        if queries:
            return "\nUNION ALL\n".join(queries)



     # ========== NEW: TWO DATES WITH CUSTOMER BREAKDOWN ==========
    if detection['type'] == 'two_dates_customer':
        date_conditions = []
        
        for entity in detection['entities']:
            day = entity['day']
            month = entity['month']
            year = entity['year']
            target_date = date(year, month, day)
            date_conditions.append(f"DATE '{target_date}'")
        
        # Single query with IN clause, grouped by customer
        query = f"""SELECT 
    "Cust_no",
    "Cust_name",
    {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') IN ({', '.join(date_conditions)})
GROUP BY "Cust_no", "Cust_name"
ORDER BY chequebounce DESC"""
        
        return query

    

    # ========== PRIORITY 1: HANDLE QUARTERS WITH "AND" ==========
    if detection['type'] == 'quarter':
        def get_quarter_dates(q, fy_year):
            """Returns start and end dates for Indian FY quarters."""
            if q == 1:
                return date(fy_year, 4, 1), date(fy_year, 6, 30)
            elif q == 2:
                return date(fy_year, 7, 1), date(fy_year, 9, 30)
            elif q == 3:
                return date(fy_year, 10, 1), date(fy_year, 12, 31)
            else:  # Q4
                return date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
        
        for entity in detection['entities']:
            q_num = entity['quarter']
            fy_year = entity['fy']
            q_name = entity['name']
            
            start_date, end_date = get_quarter_dates(q_num, fy_year)
            
            query = f"""SELECT '{q_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
        
        if queries:
            return "\nUNION ALL\n".join(queries)
    
    # ========== HANDLE MONTHS ==========
    elif detection['type'] == 'month':
        for entity in detection['entities']:
            month_num = entity['month']
            year = entity['year']
            
            if year:
                last_day = monthrange(year, month_num)[1]
                start_date = date(year, month_num, 1)
                end_date = date(year, month_num, last_day)
                period_name = entity['name']
            else:
                fy_start, fy_end = get_financial_year_range()
                year = fy_start.year if month_num >= 4 else fy_end.year
                last_day = monthrange(year, month_num)[1]
                start_date = date(year, month_num, 1)
                end_date = date(year, month_num, last_day)
                period_name = entity['name']
            
            query = f"""SELECT '{period_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
    
    # ========== HANDLE YEARS ==========
    elif detection['type'] == 'year':
        for entity in detection['entities']:
            year = entity['year']
            year_name = entity['name']
            
            start_date = date(year, 4, 1)
            end_date = date(year + 1, 3, 31)
            
            query = f"""SELECT 'FY {year_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)



    # ========== NEW: COMPANY WISE WITH MULTIPLE YEARS ==========
    if detection['type'] == 'company_wise_years':
        for entity in detection['entities']:
            year = entity['year']
            period_name = entity['name']
            
            start_date = date(year, 4, 1)
            end_date = date(year + 1, 3, 31)
            
            # Company-wise means GROUP BY Comp_code
            query = f"""SELECT '{period_name}' AS period, "Comp_code", {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'
GROUP BY "Comp_code\""""
            
            queries.append(query)
        
        if queries:
            return "\nUNION ALL\n".join(queries)

    # ========== EXISTING: Company-specific year queries ==========
    if detection['type'] == 'company_year':
        for entity in detection['entities']:
            year = entity['year']
            comp_code = entity['comp_code']
            period_name = entity['name']
            
            start_date = date(year, 4, 1)
            end_date = date(year + 1, 3, 31)
            
            query = f"""SELECT '{period_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE "Comp_code" = {comp_code}
  AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
        
        if queries:
            return "\nUNION ALL\n".join(queries)
    

    # Helper function to get date filter
    def get_date_filter(user_query):
        fy_start, fy_end = get_financial_year_range()
        date_filter = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        
        # Apply custom date logic if specified in query
        temp_sql = f"SELECT {amount_expr} AS chequebounce FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" WHERE 1=1"
        temp_sql_with_date = enforce_financial_year(temp_sql, user_query)
        
        # Extract the date filter
        date_match = re.search(
            r"DATE_PARSE\(TRIM\(CAST\(\"POST_date\" AS VARCHAR\)\),\s*'%Y%m%d'\)\s+BETWEEN\s+DATE\s+'[^']+'\s+AND\s+DATE\s+'[^']+'",
            temp_sql_with_date
        )
        if date_match:
            date_filter = date_match.group(0)
        
        return date_filter



    if detection['type'] == 'month':
        for entity in detection['entities']:
            month_num = entity['month']
            year = entity['year']
            
            # For month with year, use the provided values
            if year:
                # Use explicit year
                last_day = monthrange(year, month_num)[1]
                start_date = date(year, month_num, 1)
                end_date = date(year, month_num, last_day)
                period_name = entity['name']  # Already has year in it like "April 2024"
            else:
                # Fallback to FY logic (existing behavior)
                fy_start, fy_end = get_financial_year_range()
                year = fy_start.year if month_num >= 4 else fy_end.year
                last_day = monthrange(year, month_num)[1]
                start_date = date(year, month_num, 1)
                end_date = date(year, month_num, last_day)
                period_name = entity['name']
            
            query = f"""SELECT '{period_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
    
    elif detection['type'] == 'quarter':
        def get_quarter_dates(q, fy_year):
            if q == 1:
                return date(fy_year, 4, 1), date(fy_year, 6, 30)
            elif q == 2:
                return date(fy_year, 7, 1), date(fy_year, 9, 30)
            elif q == 3:
                return date(fy_year, 10, 1), date(fy_year, 12, 31)
            else:  # Q4
                return date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
        
        for entity in detection['entities']:
            q_num = entity['quarter']
            fy_year = entity['fy']
            q_name = entity['name']
            
            start_date, end_date = get_quarter_dates(q_num, fy_year)
            
            query = f"""SELECT '{q_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
    
    elif detection['type'] == 'year':
        for entity in detection['entities']:
            year = entity['year']
            year_name = entity['name']
            
            start_date = date(year, 4, 1)
            end_date = date(year + 1, 3, 31)
            
            query = f"""SELECT 'FY {year_name}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"""
            
            queries.append(query)
    
    elif detection['type'] == 'sales_group':
        date_filter = get_date_filter(user_query)
        
        for entity in detection['entities']:
            sg_name = entity['name']
            sg_normalized = entity['normalized']
            
            query = f"""SELECT '{sg_name.title()}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {date_filter}
  AND LOWER(REGEXP_REPLACE(TRIM("sales_grp_descp"), '\\s+', '')) LIKE '%{sg_normalized}%'"""
            
            queries.append(query)
    
    # ========== HANDLE BANKS (DYNAMIC) ==========
    elif detection['type'] == 'bank':
        date_filter = get_date_filter(user_query)
        
        for entity in detection['entities']:
            bank_name = entity['name']
            search_term = entity['search_term']
            
            query = f"""SELECT '{bank_name.title()}' AS period, {amount_expr} AS chequebounce
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {date_filter}
  AND LOWER(REGEXP_REPLACE(TRIM("Bank_name"), '[^a-zA-Z ]', '')) LIKE '%{search_term}%'"""
            
            queries.append(query)
    
    # ========== HANDLE CUSTOMERS (DYNAMIC) ==========
#     elif detection['type'] == 'customer':
#         date_filter = get_date_filter(user_query)
        
#         for entity in detection['entities']:
#             cust_name = entity['name']
#             cust_normalized = entity['normalized']
            
#             query = f"""SELECT '{cust_name.title()}' AS period, {amount_expr} AS chequebounce
# FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
# WHERE {date_filter}
#   AND LOWER(REGEXP_REPLACE(TRIM("Cust_name"), '[^a-zA-Z0-9 ]', '')) LIKE '%{cust_normalized}%'"""
            
#             queries.append(query)
    
#     if queries:
#         full_query = "\nUNION ALL\n".join(queries)
#         return full_query
    
#     return None

    # ========== HANDLE CUSTOMERS (DYNAMIC) - FLEXIBLE MATCHING ==========
    elif detection['type'] == 'customer':
        date_filter = get_date_filter(user_query)
        
        for entity in detection['entities']:
            cust_name = entity['name']
            # Try matching with spaces first, then without spaces
            cust_with_space = cust_name.lower()
            cust_no_space = cust_name.replace(' ', '').lower()
            
            # Create OR condition to match both formats
            query = f"""SELECT '{cust_name.title()}' AS period, {amount_expr} AS chequebounce
    FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
    WHERE {date_filter}
    AND (
        LOWER(REGEXP_REPLACE(TRIM("Cust_name"), '[^a-zA-Z0-9 ]', '')) LIKE '%{cust_with_space}%'
        OR LOWER(REGEXP_REPLACE(TRIM("Cust_name"), '[^a-zA-Z0-9]', '')) LIKE '%{cust_no_space}%'
    )"""
            
            queries.append(query)

# def generate_sql_fixed(req):
#     """
#     ⭐⭐⭐ CRITICAL: Don't call enforce_financial_year() on UNION queries
#     """
#     # Check comparison queries
#     comparison_info = detect_comparison_query(req.question)
#     if comparison_info['is_comparison']:
#         sql_query = build_comparison_query(req.question, comparison_info, CATALOG, SCHEMA, TABLE_NAME)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # Check quarter queries
#     query_info = detect_query_type(req.question)
    
#     if query_info['query_type'] in ['quarter_total', 'quarter_mom']:
#         sql_query = build_quarter_sql(query_info, req.question)
        
#         if sql_query:
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # ⭐⭐⭐ CRITICAL: Check "and" queries
#     and_detection = detect_and_queries(req.question)
#     if and_detection['has_and']:
#         sql_query = build_union_query_for_and(req.question, and_detection)
        
#         if sql_query:
#             # ⭐⭐⭐ DO NOT call enforce_financial_year() here!
#             # The queries already have proper date filters
#             try:
#                 data = run_presto_query(sql_query)
#                 data = add_total_row(data)
#             except Exception as e:
#                 data = [{"error": str(e)}]
            
#             return {"sql": sql_query, "data": data}
    
#     # LLM fallback
#     raw_sql = nl_to_sql(req.question)
#     sql_query = normalize_sql(raw_sql)
    
#     # ⭐ Only call enforce_financial_year() for non-UNION queries
#     sql_query = enforce_financial_year(sql_query, req.question)
    
#     try:
#         data = run_presto_query(sql_query)
#         data = add_total_row(data)
#     except Exception as e:
#         data = [{"error": str(e)}]
    
#     return {"sql": sql_query, "data": data}


def get_date_filter_for_query(user_query: str) -> str:
    """
    Helper to extract date filter from query.
    Handles special cases like "till date", "from X to date", etc.
    """
    query_lower = user_query.lower()
    today = date.today()
    
    # ⭐⭐⭐ CRITICAL: Check for "till date" or "to date" patterns
    if re.search(r'\b(till date|to date|till today|to today)\b', query_lower):
        # Extract start date if present
        # Pattern: "from month year" or "from jan 2023"
        from_date_match = re.search(
            r'\bfrom\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
            r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b',
            query_lower
        )
        
        if from_date_match:
            month_str = from_date_match.group(1)
            year = int(from_date_match.group(2))
            
            month_map = {
                "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
                "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
                "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
                "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
            }
            
            month_num = month_map[month_str[:3]]
            start_date = date(year, month_num, 1)
            
            return f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{today}'"
    
    # Default: Use enforce_financial_year logic
    fy_start, fy_end = get_financial_year_range()
    date_filter = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
    
    amount_expr = """ABS(SUM(CAST(REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') AS DOUBLE)))"""
    
    temp_sql = f"SELECT {amount_expr} AS chequebounce FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" WHERE 1=1"
    temp_sql_with_date = enforce_financial_year(temp_sql, user_query)
    
    date_match = re.search(
        r"DATE_PARSE\(TRIM\(CAST\(\"POST_date\" AS VARCHAR\)\),\s*'%Y%m%d'\)\s+BETWEEN\s+DATE\s+'[^']+'\s+AND\s+DATE\s+'[^']+'",
        temp_sql_with_date
    )
    if date_match:
        date_filter = date_match.group(0)
    
    return date_filter

def normalize_sql(sql: str) -> str:
    sql = sql.replace("```", "").replace("`", '"')
    sql = sql.replace('\\"', '"').replace("\\'", "'")
    sql = re.sub(r"\s+", " ", sql).strip()

    # Extract SELECT ... part if wrapped
    match = re.search(r"(SELECT .*?;)", sql, re.IGNORECASE)
    if match:
        sql = match.group(1)

    # Strip trailing semicolon
    sql = sql.rstrip(";").strip()

    # Fix numbers quoted as strings
    sql = re.sub(r'=\s*\'(\d+)\'', r'= \1', sql)

    # ========== FIX: Move aggregate conditions from WHERE to HAVING ==========
    # Pattern: WHERE ... ABS(SUM(...)) > X or WHERE ... SUM(...) > X
    # These must be in HAVING not WHERE
    def fix_aggregate_in_where(sql):
        # Find WHERE clause
        where_match = re.search(r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)', 
                                 sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return sql
        
        where_body = where_match.group(1).strip()
        
        # Split WHERE conditions by AND
        # Find conditions containing aggregate functions
        agg_pattern = re.compile(
            r'((?:ABS\s*\()?(?:SUM|COUNT|AVG|MIN|MAX)\s*\(.*?\)(?:\s*\))?\s*(?:>|<|>=|<=|=|!=)\s*[\d,.]+)',
            re.IGNORECASE
        )
        
        agg_conditions = []
        normal_conditions = []
        
        # Split on AND but be careful with nested parentheses
        parts = re.split(r'\bAND\b', where_body, flags=re.IGNORECASE)
        
        for part in parts:
            part = part.strip()
            if agg_pattern.search(part):
                agg_conditions.append(part)
            else:
                normal_conditions.append(part)
        
        if not agg_conditions:
            return sql  # Nothing to fix
        
        # Rebuild WHERE with only normal conditions
        if normal_conditions:
            new_where = 'WHERE ' + ' AND '.join(normal_conditions)
        else:
            new_where = ''
        
        # Build HAVING clause
        having_clause = 'HAVING ' + ' AND '.join(agg_conditions)
        
        # Find GROUP BY position to insert HAVING after it
        group_by_match = re.search(r'\bGROUP BY\b.*?(?=\bORDER BY\b|\bLIMIT\b|$)', 
                                    sql, re.IGNORECASE | re.DOTALL)
        
        if group_by_match:
            # Insert HAVING after GROUP BY clause
            group_by_end = group_by_match.end()
            
            # Rebuild SQL
            before_where = sql[:re.search(r'\bWHERE\b', sql, re.IGNORECASE).start()].strip()
            after_group_by = sql[group_by_end:].strip()
            group_by_clause = group_by_match.group(0).strip()
            
            if new_where:
                sql = f"{before_where} {new_where} {group_by_clause} {having_clause} {after_group_by}"
            else:
                sql = f"{before_where} {group_by_clause} {having_clause} {after_group_by}"
        else:
            # No GROUP BY — just replace WHERE condition
            before_where = sql[:re.search(r'\bWHERE\b', sql, re.IGNORECASE).start()].strip()
            after_where = sql[where_match.end():].strip()
            
            if new_where:
                sql = f"{before_where} {new_where} {having_clause} {after_where}"
            else:
                sql = f"{before_where} {having_clause} {after_where}"
        
        return sql.strip()

    sql = fix_aggregate_in_where(sql)

    # Fix trailing ORDER BY with no expression
    if re.search(r'ORDER BY\s*$', sql, re.IGNORECASE):
        if 'year_num' in sql and 'month_num' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num, month_num', sql, flags=re.IGNORECASE)
        elif 'year_num' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num', sql, flags=re.IGNORECASE)
        elif 'chequebounce' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY chequebounce', sql, flags=re.IGNORECASE)
        else:
            sql = re.sub(r'ORDER BY\s*$', '', sql, flags=re.IGNORECASE).strip()

    # Fix YoY CASE GROUPING
    case_match = re.search(
        r'(CASE\s+WHEN\s+EXTRACT\(MONTH FROM DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\)\s*>=\s*4.*?END)\s+AS\s+year_num',
        sql, re.IGNORECASE,
    )
    if case_match:
        case_expr = case_match.group(1)
        if re.search(r'\bGROUP BY\b', sql, re.IGNORECASE):
            sql = re.sub(r'GROUP BY\s+.*', 'GROUP BY ' + case_expr, sql, flags=re.IGNORECASE)

    # Fix Presto alias rules
    year_expr = 'EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
    month_expr = 'EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'

    sql = re.sub(
        r'GROUP BY\s+year_num\s*,\s*month_num(?![^()]*\))',
        f'GROUP BY {year_expr}, {month_expr}',
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'GROUP BY\s+year_num(?![^()]*\))',
        f'GROUP BY {year_expr}',
        sql, flags=re.IGNORECASE,
    )

    if re.search(r"SELECT\s+year_num\s*,\s*month_num\s*,\s*SUM\(chequebounce", sql, re.IGNORECASE):
        if re.search(r"\)\s+AS\s+t\s+ORDER BY", sql, re.IGNORECASE):
            sql = re.sub(
                r"\)\s+AS\s+t\s+ORDER BY",
                r") AS t GROUP BY year_num, month_num ORDER BY",
                sql, flags=re.IGNORECASE
            )

    return enforce_descending_order(sql)