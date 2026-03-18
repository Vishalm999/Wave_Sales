from fastapi import FastAPI
from pydantic import BaseModel
import prestodb
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import Credentials
import re
from datetime import date, timedelta
from calendar import monthrange
import os
from dotenv import load_dotenv

# Load environment variables
# Load environment variables
load_dotenv()

# --------------------
# Helper: Add month name to results containing month_num
# --------------------
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

def add_month_name(data: list) -> list:
    """If data contains month_num column, insert a month_name column right after it."""
    if not data:
        return data
    keys = list(data[0].keys())
    if 'month_num' not in keys:
        return data
    month_idx = keys.index('month_num')
    for row in data:
        val = row.get('month_num')
        if val == '-' or val is None:
            row['month_name'] = 'Total'
        else:
            try:
                row['month_name'] = MONTH_NAMES.get(int(float(str(val))), str(val))
            except (ValueError, TypeError):
                row['month_name'] = str(val)
    # Reorder keys so month_name appears right after month_num
    new_keys = keys[:month_idx + 1] + ['month_name'] + keys[month_idx + 1:]
    return [{k: row.get(k) for k in new_keys} for row in data]

# --------------------
# Config from .env
# --------------------
CATALOG = os.getenv("CATALOG")
SCHEMA = os.getenv("SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME")

username = os.getenv("PRESTO_USERNAME")
password = os.getenv("PRESTO_PASSWORD")
hostname = os.getenv("PRESTO_HOSTNAME")
portnumber = int(os.getenv("PRESTO_PORT"))

# Watsonx credentials
creds = Credentials(
    url=os.getenv("WATSONX_URL"),
    api_key=os.getenv("WATSONX_API_KEY")
)

model = ModelInference(
    model_id=os.getenv("WATSONX_MODEL_ID"),
    credentials=creds,
    project_id=os.getenv("WATSONX_PROJECT_ID"),
    params={
        "temperature": int(os.getenv("MODEL_TEMPERATURE")), 
        "max_new_tokens": int(os.getenv("MODEL_MAX_TOKENS"))
    }
)


# ============================================================
# PATCH 1 of 4 — Add after imports (before get_financial_year_range)
# ============================================================

BANK_CATALOG = {
    "axis": {
        "keywords": ["axis bank", "axis"],
        "condition": 'LOWER("Bank_name") LIKE \'%axis%\'',
    },
    "ccpl": {
        "keywords": ["ccpl mohali", "ccpl"],
        "condition": 'LOWER("Bank_name") LIKE \'%ccpl%\'',
    },
    "indusind": {
        "keywords": ["indusind bank", "indus ind bank", "indusind", "indus ind", "indus-ind"],
        "condition": 'LOWER("Bank_name") LIKE \'%indusind%\'',
    },
    "kotak": {
        "keywords": ["kotak mahindra bank", "kotak bank", "kotak"],
        "condition": 'LOWER("Bank_name") LIKE \'%kotak%\'',
    },
    "yes": {
        "keywords": ["yes bank", "yes bnk", "yesbank"],
        "condition": 'LOWER("Bank_name") LIKE \'%yes%\'',
    },
    "psb": {
        "keywords": [
            "p & s bank", "p&s bank", "p & s",
            "psb bank", "psb",
            "pun & sind bank", "pun & sind", "pun&sind",
            "punjab and sind bank", "punjab & sind bank", "punjab sind",
        ],
        "condition": (
            '(REGEXP_LIKE(LOWER("Bank_name"), \'p\\\\s*&\\\\s*s\') '
            'OR LOWER("Bank_name") LIKE \'%psb%\' '
            'OR REGEXP_LIKE(LOWER("Bank_name"), \'pun\\\\s*&\\\\s*sind\'))'
        ),
    },
}

_BANK_KW_LIST = sorted(
    [(kw, key) for key, v in BANK_CATALOG.items() for kw in v["keywords"]],
    key=lambda x: -len(x[0]),
)


def detect_banks_in_query(query_lower: str) -> list:
    """Return SQL conditions for all banks mentioned in the user query."""
    remaining = query_lower
    found_keys: list = []
    found_conditions: list = []
    for kw, canon_key in _BANK_KW_LIST:
        if kw in remaining and canon_key not in found_keys:
            found_keys.append(canon_key)
            found_conditions.append(BANK_CATALOG[canon_key]["condition"])
            remaining = remaining.replace(kw, " ")
    return found_conditions


def detect_exact_bank_clause(query_lower: str) -> str:
    bank_match = re.search(r'\b(axis|yes|kotak|indusind)\b', query_lower, re.IGNORECASE)
    num_match = re.search(r'\b(\d{3,8})\b', query_lower)
    tail_match = re.search(r'\b(out|in)\b', query_lower, re.IGNORECASE)
    if not (bank_match and num_match and tail_match):
        return ""
    bank = bank_match.group(1).lower()
    num = num_match.group(1)
    tail = tail_match.group(1).lower()
    pattern = f'^{bank}[^0-9]*{num}[^0-9]*{tail}(?:[^a-z0-9]*(?:bank|bnk))?$'
    return f"REGEXP_LIKE(LOWER(\"Bank_name\"), '{pattern}')"


def build_bank_where_clause(conditions: list) -> str:
    """Join multiple bank conditions with OR."""
    if not conditions:
        return ""
    if len(conditions) == 1:
        return conditions[0]
    return "(" + " OR ".join(conditions) + ")"
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
    
    # 🔹 PRESERVE: Handle CURRENT_DATE properly
    clause = clause.replace("DATE 'CURRENT_DATE'", "CURRENT_DATE")
    clause = clause.replace("'CURRENT_DATE'", "CURRENT_DATE")
    
    # 🔹 CRITICAL: Clean up malformed SQL first
    sql = re.sub(r'\s+AND\s+AND\s+', ' AND ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+AND\s+1=1\s+', ' ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'AND\s+CURRENT_DATE\s+AND', ' AND ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'AND\s+1=1\s*AND', ' AND ', sql, flags=re.IGNORECASE)
    sql = re.sub(
        r'\s+WHERE\s+1=1\s+OR\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
        ' WHERE 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\s+AND\s+1=1\s+OR\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
        ' AND 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\s+WHERE\s*\(\s*1=1\s*OR\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?\)\s*(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
        ' WHERE 1=1',
        sql,
        flags=re.IGNORECASE,
    )

    def wrap_where_or(s: str) -> str:
        if "WHERE" not in s.upper() or " OR " not in s.upper():
            return s
        parts = re.split(r"(?i)\bWHERE\b", s, maxsplit=1)
        if len(parts) != 2:
            return s
        head, cond = parts[0], parts[1].strip()
        if cond.startswith("(") and cond.endswith(")"):
            return head + "WHERE " + cond
        return head + "WHERE (" + cond + ")"

    if "GROUP BY" in sql_upper:
        parts = re.split(r"(?i)\bGROUP BY\b", sql, maxsplit=1)
        before, after = parts[0], parts[1]
        if "WHERE" in before.upper():
            before = wrap_where_or(before)
            return before + " AND (" + clause + ") GROUP BY " + after
        else:
            return before + " WHERE " + clause + " GROUP BY " + after

    if "ORDER BY" in sql_upper:
        parts = re.split(r"(?i)\bORDER BY\b", sql, maxsplit=1)
        before, after = parts[0], parts[1]
        if "WHERE" in before.upper():
            before = wrap_where_or(before)
            return before + " AND (" + clause + ") ORDER BY " + after
        else:
            return before + " WHERE " + clause + " ORDER BY " + after

    if "LIMIT" in sql_upper:
        parts = re.split(r"(?i)\bLIMIT\b", sql, maxsplit=1)
        before, after = parts[0], parts[1]
        if "WHERE" in before.upper():
            before = wrap_where_or(before)
            return before + " AND (" + clause + ") LIMIT " + after
        else:
            return before + " WHERE " + clause + " LIMIT " + after

    if "WHERE" in sql_upper:
        sql = wrap_where_or(sql)
        return sql + " AND (" + clause + ")"
    else:
        return sql + " WHERE " + clause



def enforce_financial_year(sql: str, user_query: str) -> str:
    """
    Apply correct financial year, quarter, month, or YoY rules to SQL query.
    CRITICAL: Preserve all existing logic while fixing till date.
    """
    user_query = re.sub(r'\s+', ' ', user_query.replace('\x00', '').replace('\xa0', ' ')).strip()
    multi_quarters = re.findall(r'\bq[1-4]\b', user_query, re.I)
    if (
        len(set(multi_quarters)) >= 2
        and re.search(
            r'BETWEEN\s+DATE\s+\'\d{4}-\d{2}-\d{2}\'\s+AND\s+DATE\s+\'\d{4}-\d{2}-\d{2}\'\s+OR\s+DATE_PARSE',
            sql,
            re.IGNORECASE,
        )
    ):
        return sql
    today = date.today()
    fy_start, fy_end = get_financial_year_range(today)

    # 🆕 "Month A vs Month B" (Handle combined range) - moved to top
    month_names = "january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
    month_vs_match = re.search(
        rf"\b({month_names})\s+vs\s+({month_names})\b", 
        user_query, 
        re.IGNORECASE
    )
    
    if month_vs_match:
        m1_str = month_vs_match.group(1).lower()
        m2_str = month_vs_match.group(2).lower()
        
        # Helper to get month number
        def get_month_num(m_str):
            mapping = {
                'jan':1, 'january':1, 'feb':2, 'february':2, 'mar':3, 'march':3,
                'apr':4, 'april':4, 'may':5, 'jun':6, 'june':6, 'jul':7, 'july':7,
                'aug':8, 'august':8, 'sep':9, 'sept':9, 'september':9, 'oct':10, 'october':10,
                'nov':11, 'november':11, 'dec':12, 'december':12
            }
            return mapping.get(m_str[:3], mapping.get(m_str)) # Handle sept vs sep

        m1_num = get_month_num(m1_str)
        m2_num = get_month_num(m2_str)
        
        if m1_num and m2_num:
            # Resolve years
            # If month > today.month, year = today.year - 1 (Assuming past)
            # If month <= today.month, year = today.year
            
            def get_month_range(m_num, current_date):
                y = current_date.year
                if m_num > current_date.month:
                    y -= 1
                
                # Start: 1st of month
                start = date(y, m_num, 1)
                # End: last day of month
                if m_num == 12:
                    end = date(y, 12, 31)
                else:
                    end = date(y, m_num + 1, 1) - timedelta(days=1)
                return start, end

            s1, e1 = get_month_range(m1_num, today)
            s2, e2 = get_month_range(m2_num, today)
            
            final_start = min(s1, s2)
            final_end = max(e1, e2)
            
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{final_start}' AND DATE '{final_end}'"
             
            # Remove existing conflicting date filters
            sql = re.sub(
                r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
                r'\1 1=1',
                sql,
                flags=re.IGNORECASE,
            )
            
            return insert_where_before_groupby(sql, clause)
    
    # 🔹 STEP 1: Remove quotes around CURRENT_DATE early
    sql = sql.replace("DATE 'CURRENT_DATE'", "CURRENT_DATE")
    sql = sql.replace("'CURRENT_DATE'", "CURRENT_DATE")
    
    # 🔹 HIGHEST PRIORITY: Year-on-Year / YoY queries
    if re.search(r"\b(year on year|yoy|year-over-year)\b", user_query, re.I):
    
        # 🔹 Month name → number map
        month_map = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
        }
    
        # 🔹 Detect month range like "may to sept"
        month_range_match = re.search(
            r'\b(' + '|'.join(month_map.keys()) + r')\s*(to|-|through)\s*(' +
            '|'.join(month_map.keys()) + r')\b',
            user_query,
            re.IGNORECASE
        )
    
        # 🔹 Remove hard BETWEEN date locks
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+BETWEEN\s+DATE\s+\'[0-9\-]+\'\s+AND\s+DATE\s+\'[0-9\-]+\'',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE
        )
    
        # ✅ Month-range YoY → APPLY filter
        if month_range_match:
            start_month = month_map[month_range_match.group(1).lower()]
            end_month = month_map[month_range_match.group(3).lower()]
    
            clause = (
                "EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) "
                f"BETWEEN {start_month} AND {end_month}"
            )
            return insert_where_before_groupby(sql, clause)
    
        # 🔹 Detect other ranges (FY / Q / year)
        if re.search(r'\b(q[1-4]|quarter|fy|financial year|\d{4})\b', user_query, re.I):
            return sql
    
        # 🔹 Pure YoY
        return sql
    



        
    original_sql = sql
    original_has_date_filter = bool(re.search(
        r'DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|>=|<=|>|<|=)',
        sql,
        re.IGNORECASE
    ))
    
    # 🔹 STEP 2: Handle "from X till date" pattern FIRST (before any cleanup)
    till_date_pattern = re.search(
        r'\b(?:from|since)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+'
        r'(\d{4})'
        r'\s+(?:till|until|to)\s+(?:date|now)\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_date_pattern:
        start_month_str = till_date_pattern.group(1).lower()
        start_year = int(till_date_pattern.group(2))
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        start_month = month_map[start_month_str[:3]]
        start_date = date(start_year, start_month, 1)
        
        # 🔹 CRITICAL: Remove ALL existing date filters FIRST before adding new one
        # This prevents malformed SQL like "AND 1=1AND CURRENT_DATE"
        sql = re.sub(
            r'\s+AND\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<)[^)]*(?=\s*(?:AND|GROUP|ORDER|LIMIT|$))',
            ' ',
            sql,
            flags=re.IGNORECASE,
        )
        
        # 🔹 CRITICAL: Also clean up any "1=1" artifacts from previous replacements
        sql = re.sub(r'\s+AND\s+1=1\s+AND\s+', ' AND ', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+AND\s+1=1\s+', ' AND ', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+AND\s+AND\s+', ' AND ', sql, flags=re.IGNORECASE)
        
        # 🔹 Now safely add the new date filter
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND CURRENT_DATE"
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 STEP 3: Clean up WHERE 1=1 clauses
    sql = re.sub(r'WHERE\s+1=1\s+OR\s+', 'WHERE ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'WHERE\s+1=1\s+AND\s+', 'WHERE ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+AND\s+1=1\s+', ' ', sql, flags=re.IGNORECASE)
    if "WHERE 1=1" in sql.upper() and not re.search(r'WHERE 1=1\s+(?:AND|OR)', sql, re.IGNORECASE):
        sql = re.sub(r'WHERE\s+1=1', '', sql, flags=re.IGNORECASE)
    
    # 🔹 STEP 4: Check for quarter range patterns
    quarter_range_year_pattern = re.search(
        r'\b(?:q|Q|quarter)\s*([1-4])\s*(?:and|&|,|to|-|through)\s*(?:q|Q|quarter)\s*([1-4])\s*(?:in\s+)?(\d{4})\b',
        user_query,
        re.IGNORECASE
    )
    
    if quarter_range_year_pattern and original_has_date_filter:
        if re.search(r'BETWEEN DATE\s+\'\d{4}-\d{2}-\d{2}\'\s+AND\s+DATE\s+\'\d{4}-\d{2}-\d{2}\'', sql):
            return sql
    
    # 🔹 STEP 5: Handle "till <month>" with explicit year at end
    till_month_with_trailing_year = re.search(
        r'\b(?:till|until|up to|through)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'.*?\b(20\d{2})\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_month_with_trailing_year:
        end_month_str = till_month_with_trailing_year.group(1).lower()
        explicit_year = till_month_with_trailing_year.group(2)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        end_month = month_map[end_month_str[:3]]
        end_year = int(explicit_year)
        start_year = end_year
        end_day = monthrange(end_year, end_month)[1]
        start_date = date(start_year, 4, 1)
        end_date = date(end_year, end_month, end_day)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 STEP 6: Handle original "till <month>" pattern
    till_month_match = re.search(
        r'\b(?:till|until|up to|through)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_month_match:
        end_month_str = till_month_match.group(1).lower()
        explicit_year = till_month_match.group(2)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        end_month = month_map[end_month_str[:3]]
        
        if explicit_year:
            end_year = int(explicit_year)
            start_year = end_year
        else:
            end_year = fy_end.year if end_month < 4 else fy_start.year
            start_year = fy_start.year
        
        end_day = monthrange(end_year, end_month)[1]
        start_date = date(start_year, 4, 1)
        end_date = date(end_year, end_month, end_day)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
    
    
    # 🔹 PRESERVE LOGIC: Handle "from X till date" / "till now" patterns
    till_date_pattern = re.search(
        r'\b(from|since)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(\d{4})'
        r'\s+(?:till|until|to)\s+(?:date|now)\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_date_pattern:
        start_month_str = till_date_pattern.group(2).lower()
        start_year = int(till_date_pattern.group(3))
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        start_month = month_map[start_month_str[:3]]
        start_date = date(start_year, start_month, 1)
        
        # Use CURRENT_DATE (not quoted) for "till date"
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND CURRENT_DATE"
        
        # Remove any existing conflicting date filters
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|AND|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 PRESERVE LOGIC: Original "till <month>" pattern (without trailing year)
    till_month_match = re.search(
        r'\b(?:till|until|up to|through)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_month_match:
        end_month_str = till_month_match.group(1).lower()
        explicit_year = till_month_match.group(2)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        end_month = month_map[end_month_str[:3]]
        
        if explicit_year:
            end_year = int(explicit_year)
            start_year = end_year
        else:
            end_year = fy_end.year if end_month < 4 else fy_start.year
            start_year = fy_start.year
        
        end_day = monthrange(end_year, end_month)[1]
        start_date = date(start_year, 4, 1)
        end_date = date(end_year, end_month, end_day)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
     
    # Original "till <month>" pattern (without trailing year)
    till_month_match = re.search(
        r'\b(?:till|until|up to|through)\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_month_match:
        end_month_str = till_month_match.group(1).lower()
        explicit_year = till_month_match.group(2)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        end_month = month_map[end_month_str[:3]]
        
        if explicit_year:
            end_year = int(explicit_year)
            start_year = end_year
        else:
            end_year = fy_end.year if end_month < 4 else fy_start.year
            start_year = fy_start.year
        
        end_day = monthrange(end_year, end_month)[1]
        start_date = date(start_year, 4, 1)
        end_date = date(end_year, end_month, end_day)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        
        # Remove any existing conflicting date filters before inserting
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
    
    # 🆕 FIX: Handle "till <day> <month>" pattern (e.g., "till 12 december")
    # Must come BEFORE month-only "till" pattern
    till_day_month_match = re.search(
        r'\b(?:till|until|up to|through)\s+(\d{1,2})(?:st|nd|rd|th)?\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if till_day_month_match:
        day = int(till_day_month_match.group(1))
        month_str = till_day_month_match.group(2).lower()
        explicit_year = till_day_month_match.group(3)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        month = month_map[month_str[:3]]
        
        if explicit_year:
            end_year = int(explicit_year)
            start_year = end_year
        else:
            end_year = fy_end.year if month < 4 else fy_start.year
            start_year = fy_start.year
        
        try:
            start_date = date(start_year, 4, 1)  # Always start from April 1 of FY
            end_date = date(end_year, month, day)
            
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            
            # Remove any existing conflicting date filters
            sql = re.sub(
                r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
                r'\1 1=1',
                sql,
                flags=re.IGNORECASE,
            )
            
            return insert_where_before_groupby(sql, clause)
        except:
            pass    

    full_date_range_match = re.search(
        r'\b(?:from\s+)?(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*(20\d{2})\s*'
        r'(?:to|till|until|through|-)\s*'
        r'(\d{1,2})(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*(20\d{2})\b',
        user_query,
        re.IGNORECASE,
    )

    if full_date_range_match:
        d1 = int(full_date_range_match.group(1))
        m1_str = full_date_range_match.group(2).lower()
        y1 = int(full_date_range_match.group(3))
        d2 = int(full_date_range_match.group(4))
        m2_str = full_date_range_match.group(5).lower()
        y2 = int(full_date_range_match.group(6))

        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }

        try:
            start = date(y1, month_map[m1_str[:3]], d1)
            end = date(y2, month_map[m2_str[:3]], d2)
            if end < start:
                start, end = end, start

            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
            sql = re.sub(
                r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
                r'\1 1=1',
                sql,
                flags=re.IGNORECASE,
            )
            return insert_where_before_groupby(sql, clause)
        except Exception:
            pass
    
    # 🆕 CRITICAL FIX: Handle "april 2024 and april 2025" pattern
    # Check if user is asking for specific months in different years
    month_year_pairs = re.findall(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{4})\b',
        user_query,
        re.IGNORECASE
    )
    
    if len(month_year_pairs) >= 2:
        # User asked for multiple months in different years (e.g., "april 2024 and april 2025")
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        conditions = []
        for month_str, year_str in month_year_pairs:
            month_num = month_map.get(month_str.lower()[:3], 1)
            year_num = int(year_str)
            last_day = monthrange(year_num, month_num)[1]
            start = date(year_num, month_num, 1)
            end = date(year_num, month_num, last_day)
            conditions.append(
                f"(DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start}' AND DATE '{end}')"
            )
        
        if conditions:
            clause = " OR ".join(conditions)
            if len(conditions) > 1:
                clause = f"({clause})"
            
            # Remove any existing conflicting date filters
            sql = re.sub(
                r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
                r'\1 1=1',
                sql,
                flags=re.IGNORECASE,
            )
            
            # Also remove EXTRACT(MONTH/YEAR) filters
            sql = re.sub(
                r'(WHERE|AND)\s+EXTRACT\(MONTH FROM DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
                r'\1 1=1',
                sql,
                flags=re.IGNORECASE,
            )
            
            return insert_where_before_groupby(sql, clause)
    
    # 🔒 ABSOLUTE GUARD: NEVER strip date filters for YoY
    if re.search(r"\b(year on year|yoy|year-over-year|by year|yearly)\b", user_query, re.I):
        return sql


    
    # Strip any existing BETWEEN on POST_date (we'll add our own if needed)
    sql = re.sub(
        r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+BETWEEN\s+DATE\s+\'[0-9\-]+\'\s+AND\s+DATE\s+\'[0-9\-]+\'',
        r'\1 1=1',
        sql,
        flags=re.IGNORECASE,
    )
    
    
    # 🆕 DETECTION PHASE: Analyze the query type
    month_words_full = ['january', 'february', 'march', 'april', 'may', 'june', 
                       'july', 'august', 'september', 'october', 'november', 'december']
    month_words_short = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    all_month_pattern = r'\b(' + '|'.join(month_words_full + month_words_short) + r')\b'
    all_months = re.findall(all_month_pattern, user_query, re.IGNORECASE)
    
    # Determine query characteristics
    is_breakdown_query = (
        re.search(r'\b(breakdown|by month|monthly|each month|per month|month wise|month-wise|mom|month on month)\b', user_query, re.IGNORECASE) or
        re.search(r'\b(customer|cust|customer-wise|customerwise|each customer)\b', user_query, re.IGNORECASE) or
        re.search(r'\b(sales group|sales grp|group-wise|each group)\b', user_query, re.IGNORECASE) or
        re.search(r'\b(quarter.*quarter|qoq|quarter.*comparison)\b', user_query, re.IGNORECASE) or
        len(all_months) >= 2
    )
    
    has_multiple_months = len(all_months) >= 2 and re.search(r'\b(and|,)\b', user_query, re.IGNORECASE)
    needs_month_grouping = has_multiple_months and "GROUP BY" not in sql.upper() and is_breakdown_query
    
    # 🆕 CRITICAL DECISION: Should we preserve AI's date logic?
    # Check for queries where AI likely made correct decisions
    ai_preservation_patterns = [
        # Month ranges without explicit year that AI handled
        r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(?:to|-|through)\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b(?!\s+\d{4})',
        # Specific date formats that AI handles well
        r'\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b',
        r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b',
    ]
    
    should_preserve_ai_logic = False
    for pattern in ai_preservation_patterns:
        if re.search(pattern, user_query, re.IGNORECASE) and original_has_date_filter:
            should_preserve_ai_logic = True
            break
    
    # 🆕 If AI already handled it well, return original SQL
    if should_preserve_ai_logic:
        return original_sql
    
    # ============================================================
    # 🎯 MAIN HANDLERS - Process specific date patterns
    # ============================================================
    
    
    # 🔹 VERY SPECIFIC PATTERNS WITH EXPLICIT DATES
    # 1. "quarter N in YYYY" pattern - MUST come before general quarter patterns
    quarter_in_year = re.search(r'\bquarter\s+([1-4])\s+in\s+(\d{4})\b', user_query, re.IGNORECASE)
    if quarter_in_year:
        q = int(quarter_in_year.group(1))
        fy_year = int(quarter_in_year.group(2))
        
        if q == 1:
            start, end = date(fy_year, 4, 1), date(fy_year, 6, 30)
        elif q == 2:
            start, end = date(fy_year, 7, 1), date(fy_year, 9, 30)
        elif q == 3:
            start, end = date(fy_year, 10, 1), date(fy_year, 12, 31)
        else:  # Q4
            start, end = date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 2. Day-Month range with year (e.g., "16 sep to 30 september 2024")
    day_month_range_with_year = re.search(
        r'\b([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through)\s*'
        r'([0-3]?\d)(?:st|nd|rd|th)?\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))\b',
        user_query,
        re.IGNORECASE
    )
    
    if day_month_range_with_year:
        d1 = int(day_month_range_with_year.group(1))
        m1 = day_month_range_with_year.group(2).lower()
        d2 = int(day_month_range_with_year.group(3))
        m2 = day_month_range_with_year.group(4).lower()
        year = int(day_month_range_with_year.group(5))
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        sm = month_map[m1[:3]]
        em = month_map[m2[:3]]
        
        try:
            start_date = date(year, sm, d1)
            end_date = date(year, em, d2)
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
        except:
            pass
    
    # 3. Two specific dates (e.g., "16th and 18th sep")
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
        inferred_year = fy_start.year if m >= 4 else fy_end.year
        
        date1 = date(inferred_year, m, d1)
        date2 = date(inferred_year, m, d2)
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"IN (DATE '{date1}', DATE '{date2}')"
        )
        return insert_where_before_groupby(sql, clause)
    
    # 4. Single exact date
    date_match = None
    
    # Day-month-year
    date_match = re.search(
        r'\b([0-3]?\d)(?:st|nd|rd|th)?[ \-\/\.]?(?:of\s+)?(?:,?\s*)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[ \-\/\.]?(?:,?\s*)?(20\d{2})\b',
        user_query, re.IGNORECASE
    )
    
    # Month-day-year
    if not date_match:
        date_match = re.search(
            r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[ ,\-\.]+([0-3]?\d)(?:st|nd|rd|th)?(?:,?\s*)?(20\d{2})\b',
            user_query, re.IGNORECASE
        )
    
    # ISO format
    if not date_match:
        date_match = re.search(r'\b(20\d{2})[ \-\/\.]([01]?\d)[ \-\/\.]([0-3]?\d)\b', user_query)
        if date_match:
            iso_year = int(date_match.group(1))
            iso_month = int(date_match.group(2))
            iso_day = int(date_match.group(3))
            try:
                start = end = date(iso_year, iso_month, iso_day)
                clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') = DATE '{start}'"
                return insert_where_before_groupby(sql, clause)
            except:
                date_match = None
    
    if date_match:
        g1 = date_match.group(1)
        g2 = date_match.group(2)
        g3 = date_match.group(3)
        
        if re.match(r'^\d{1,2}$', g1):
            day = int(g1)
            month_str = g2.lower()
            year = int(g3)
        else:
            month_str = g1.lower()
            day = int(g2)
            year = int(g3)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        m = month_map.get(month_str[:3], None)
        if m is None:
            m = month_map.get(month_str, None)
        
        try:
            start = end = date(year, m, day)
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') = DATE '{start}'"
            return insert_where_before_groupby(sql, clause)
        except:
            pass
    
    # 15. Month range (e.g., "april to june", "apr-sep 2025")
    month_range_match = re.search(
        r'\b(?:from\s+)?'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:,|\s|of|in)\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if month_range_match:
        start_month_str = month_range_match.group(1).lower()
        end_month_str = month_range_match.group(2).lower()
        explicit_year = month_range_match.group(3)
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        s_month = month_map[start_month_str[:3]]
        e_month = month_map[end_month_str[:3]]
        
        if explicit_year:
            s_year = int(explicit_year)
            e_year = s_year if e_month >= s_month else s_year + 1
        else:
            s_year = fy_end.year if s_month < 4 else fy_start.year
            e_year = fy_end.year if e_month < 4 else fy_start.year
            if e_year < s_year or (e_month < s_month and e_year == s_year):
                e_year = s_year + 1
        
        s_last_day = monthrange(s_year, s_month)[1]
        e_last_day = monthrange(e_year, e_month)[1]
        
        start = date(s_year, s_month, 1)
        end = date(e_year, e_month, e_last_day)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 18. Two specific months (e.g., "aug and sep")
    two_months_match = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:and|&|,)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    if two_months_match:
        m1 = two_months_match.group(1).lower()
        m2 = two_months_match.group(2).lower()
        explicit_year = two_months_match.group(3)
        
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10,
            "nov": 11, "dec": 12
        }
        
        s_month = month_map[m1[:3]]
        e_month = month_map[m2[:3]]
        
        if explicit_year:
            y = int(explicit_year)
            s_year = y
            e_year = y
        else:
            s_year = fy_end.year if s_month < 4 else fy_start.year
            e_year = fy_end.year if e_month < 4 else fy_start.year
        
        s_start = date(s_year, s_month, 1)
        s_end = date(s_year, s_month, monthrange(s_year, s_month)[1])
        
        e_start = date(e_year, e_month, 1)
        e_end = date(e_year, e_month, monthrange(e_year, e_month)[1])
        
        clause = (
            f"(DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{s_start}' AND DATE '{s_end}' "
            f"OR DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{e_start}' AND DATE '{e_end}')"
        )
        
        # Add grouping if needed
        if needs_month_grouping:
            if not re.search(r'EXTRACT\(YEAR FROM DATE_PARSE', sql, re.IGNORECASE):
                sql = re.sub(
                    r'SELECT\s+ABS\(SUM\(CAST\(',
                    "SELECT EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) AS year_num, EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) AS month_num, ABS(SUM(CAST(",
                    sql,
                    count=1,
                    flags=re.IGNORECASE
                )
        
        sql = insert_where_before_groupby(sql, clause)
        
        if needs_month_grouping and "GROUP BY" not in sql.upper():
            sql = sql.rstrip()
            if sql.endswith(';'):
                sql = sql[:-1]
            sql += " GROUP BY EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')), EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) ORDER BY year_num, month_num"
        return sql
    
    
    # 🆕 FIX: Handle single day only (e.g., "16 september", "16th september")
    # MUST come BEFORE month detection patterns
    single_day_month_match = re.search(
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s+'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'(?:\s*(?:in|of)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    
    # Make sure there's NO "to", "till", "until", "through", "and", "or" after this day-month combo
    if single_day_month_match:
        # Check if this is part of a range (e.g., "16 sep to 30 sep")
        start_pos = single_day_month_match.start()
        end_pos = single_day_month_match.end()
        remaining_text = user_query[end_pos:].strip()
        
        # If remaining text starts with range operators, this is NOT a single day query
        is_range = bool(re.match(r'^(?:to|-|through|till|until|and|,|\s+and|\s+or)', remaining_text, re.IGNORECASE))
        
        if not is_range:
            # This is a single day - process it
            day = int(single_day_month_match.group(1))
            month_str = single_day_month_match.group(2).lower()
            explicit_year = single_day_month_match.group(3)
            
            month_map = {
                "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
                "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
                "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
                "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
            }
            
            month = month_map[month_str[:3]]
            
            if explicit_year:
                year = int(explicit_year)
            else:
                # If month < 4 (Jan-Mar), it's in next calendar year but same FY
                # If month >= 4 (Apr-Dec), it's in current FY start year
                year = fy_start.year if month >= 4 else fy_end.year
            
            try:
                target_date = date(year, month, day)
                clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') = DATE '{target_date}'"
                
                # Remove any existing conflicting date filters
                sql = re.sub(
                    r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
                    r'\1 1=1',
                    sql,
                    flags=re.IGNORECASE,
                )
                
                return insert_where_before_groupby(sql, clause)
            except:
                pass
    
    # 🔹 MONTH + YEAR PATTERNS (MUST come before single year patterns)
    # 5. Month + Year (e.g., "December 2020", "Dec 2020")
    month_year_match = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[\s,]*(\d{4})\b',
        user_query,
        re.IGNORECASE
    )
    if month_year_match:
        month_str = month_year_match.group(1).lower()
        year = int(month_year_match.group(2))
        
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        month_num = month_map.get(month_str[:3], month_map.get(month_str, 1))
        last_day = monthrange(year, month_num)[1]
        start, end = date(year, month_num, 1), date(year, month_num, last_day)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 6. Day-Month range (e.g., "15 sep to 30 sep")
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
        
        if y1:
            y1 = int(y1)
        else:
            y1 = fy_start.year if m1_num >= 4 else fy_end.year
        
        if y2:
            y2 = int(y2)
        else:
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
        except:
            pass
    
    # 🔹 QUARTER + FINANCIAL YEAR PATTERNS
    # 7. Quarter + Financial Year (e.g., "Q1 FY 2024")
    q_fy_pattern = r'\b(?:q|Q)([1-4])(?:\s+(?:FY|fy))?\s+(\d{4})\b'
    q_fy_match = re.search(q_fy_pattern, user_query, re.IGNORECASE)
    if q_fy_match:
        q = int(q_fy_match.group(1))
        fy_year = int(q_fy_match.group(2))
        
        # Map to FY quarter dates
        if q == 1:
            start, end = date(fy_year, 4, 1), date(fy_year, 6, 30)
        elif q == 2:
            start, end = date(fy_year, 7, 1), date(fy_year, 9, 30)
        elif q == 3:
            start, end = date(fy_year, 10, 1), date(fy_year, 12, 31)
        else:  # Q4
            start, end = date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 8. Quarter ranges (e.g., "Q1 to Q3 2024", "from Q2 to Q4")
    quarter_range_match = re.search(
        r'\bQ([1-4])\s*(?:to|-|through)\s*Q([1-4])(?:\s*(?:FY)?\s*(20\d{2}))?\b',
        user_query,
        re.IGNORECASE
    )
    if quarter_range_match:
        q_start = int(quarter_range_match.group(1))
        q_end = int(quarter_range_match.group(2))
        fy_year = int(quarter_range_match.group(3)) if quarter_range_match.group(3) else fy_start.year
        
        def get_quarter_dates(q, fy_yr):
            if q == 1:
                return date(fy_yr, 4, 1), date(fy_yr, 6, 30)
            elif q == 2:
                return date(fy_yr, 7, 1), date(fy_yr, 9, 30)
            elif q == 3:
                return date(fy_yr, 10, 1), date(fy_yr, 12, 31)
            else:
                return date(fy_yr + 1, 1, 1), date(fy_yr + 1, 3, 31)
        
        start, _ = get_quarter_dates(q_start, fy_year)
        _, end = get_quarter_dates(q_end, fy_year)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 9. Multiple quarters (e.g., "Q1 and Q3", "Q2, Q4 2024")
    multi_quarters_list = re.findall(r'\bQ([1-4])\b', user_query, re.IGNORECASE)
    if len(multi_quarters_list) >= 2:
        fy_year = fy_start.year
        
        # Check for explicit year
        explicit_year = re.search(r'\b(20\d{2})\b', user_query)
        if explicit_year:
            fy_year = int(explicit_year.group(1))
        
        def get_quarter_dates(q, fy_yr):
            if q == 1:
                return date(fy_yr, 4, 1), date(fy_yr, 6, 30)
            elif q == 2:
                return date(fy_yr, 7, 1), date(fy_yr, 9, 30)
            elif q == 3:
                return date(fy_yr, 10, 1), date(fy_yr, 12, 31)
            else:
                return date(fy_yr + 1, 1, 1), date(fy_yr + 1, 3, 31)
        
        # Create OR conditions for each quarter
        conditions = []
        quarters_set = sorted(set([int(q) for q in multi_quarters_list]))
        for q in quarters_set:
            start, end = get_quarter_dates(q, fy_year)
            conditions.append(
                f"(DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start}' AND DATE '{end}')"
            )
        
        if conditions:
            clause = " OR ".join(conditions)
            if len(conditions) > 1:
                clause = f"({clause})"
            return insert_where_before_groupby(sql, clause)

        
    # 10. Single quarter (e.g., "Q1 2024", "Q4")
    q_match = re.search(r"\b(?:Q|quarter\s+)([1-4])(?:\s*(?:in|of|for)?\s*(?:FY)?\s*(20\d{2}))?\b", user_query, re.I)
    if q_match:
        q = int(q_match.group(1))
        fy_year = int(q_match.group(2)) if q_match.group(2) else fy_start.year
        if q == 1:
            start, end = date(fy_year, 4, 1), date(fy_year, 6, 30)
        elif q == 2:
            start, end = date(fy_year, 7, 1), date(fy_year, 9, 30)
        elif q == 3:
            start, end = date(fy_year, 10, 1), date(fy_year, 12, 31)
        else:
            start, end = date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 FINANCIAL YEAR PATTERNS
    # 11. Multiple financial years (e.g., "FY 2022, 2023, 2024")
    fy_multiple_match = re.findall(r'\bfy\s*(\d{4})\b', user_query, re.IGNORECASE)
    if fy_multiple_match:
        fy_years = sorted(set([int(year) for year in fy_multiple_match]))
        
        # Also check for standalone years
        all_years = re.findall(r'\b(20\d{2})\b', user_query)
        if all_years:
            all_years_int = sorted(set([int(year) for year in all_years]))
            years = sorted(set(fy_years + all_years_int))
        else:
            years = fy_years
        
        # Check if it's a range (e.g., "FY 2020 to 2024")
        is_fy_range = re.search(r'\bfy\s*(\d{4})\s*(?:to|-|through)\s*fy\s*(\d{4})\b', user_query, re.IGNORECASE)
        if is_fy_range and len(years) == 2:
            start_year = min(years)
            end_year = max(years)
            start_date = date(start_year, 4, 1)
            end_date = date(end_year + 1, 3, 31)
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            return insert_where_before_groupby(sql, clause)
        
        # Multiple individual years - create OR conditions
        if len(years) > 1:
            conditions = []
            for year in years:
                start_date = date(year, 4, 1)
                end_date = date(year + 1, 3, 31)
                conditions.append(
                    f"(DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                    f"BETWEEN DATE '{start_date}' AND DATE '{end_date}')"
                )
            
            if conditions:
                clause = " OR ".join(conditions)
                if len(conditions) > 1:
                    clause = f"({clause})"
                return insert_where_before_groupby(sql, clause)
    
    # 12. Single financial year (e.g., "FY 2020", "financial year 2020")
    fy_explicit_match = re.search(
        r'\b(?:fy|f\.y\.|financial year)\s+(\d{4})\b',
        user_query,
        re.IGNORECASE
    )
    
    if fy_explicit_match:
        fy_year = int(fy_explicit_match.group(1))
        start_date = date(fy_year, 4, 1)
        end_date = date(fy_year + 1, 3, 31)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 YEAR PATTERNS (GENERAL)
     # 13. Multiple years (e.g., "2024 and 2025", "2022, 2023, 2024")
    multiple_years_match = re.findall(r'\b(20\d{2})\b', user_query)
    if len(multiple_years_match) >= 2:
        years = sorted(set([int(year) for year in multiple_years_match]))
        
        # Check if it's a range (e.g., "2020 to 2024")
        is_year_range = re.search(r'\b(?:from\s+)?(20\d{2})\s*(?:to|-|through)\s*(20\d{2})\b', user_query, re.IGNORECASE)
        
        if is_year_range:
            # Year range - single BETWEEN clause
            start_year = min(years)
            end_year = max(years)
            start_date = date(start_year, 4, 1)
            end_date = date(end_year + 1, 3, 31)
            clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            return insert_where_before_groupby(sql, clause)
        else:
            # Multiple individual years - OR conditions
            conditions = []
            for year in years:
                start_date = date(year, 4, 1)
                end_date = date(year + 1, 3, 31)
                conditions.append(
                    f"(DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                    f"BETWEEN DATE '{start_date}' AND DATE '{end_date}')"
                )
            
            if conditions:
                clause = " OR ".join(conditions)
                if len(conditions) > 1:
                    clause = f"({clause})"
                
                # ✅ NEW: Fix calendar year grouping to FY year grouping (ONLY 7 LINES ADDED)
                if re.search(r'EXTRACT\(YEAR FROM DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\)', sql, re.IGNORECASE):
                    fy_year_case = "CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) >= 4 THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) - 1 END"
                    sql = re.sub(r'EXTRACT\(YEAR FROM DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\)\s+AS\s+year_num', f'{fy_year_case} AS year_num', sql, flags=re.IGNORECASE)
                    sql = re.sub(r'GROUP BY\s+EXTRACT\(YEAR FROM DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\)', f'GROUP BY {fy_year_case}', sql, flags=re.IGNORECASE)
                
                return insert_where_before_groupby(sql, clause)
    
    # 14. Single year (e.g., "for 2024", "in 2024", "2024")
    single_year_match = re.search(r'\b(?:for|in|during|of|year)?\s*(20\d{2})\b', user_query, re.IGNORECASE)
    if single_year_match:
        year = int(single_year_match.group(1))
        start_date = date(year, 4, 1)
        end_date = date(year + 1, 3, 31)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 MONTH PATTERNS WITHOUT YEAR
    
    
    # 16. Month range + LAST YEAR (e.g., "apr to sep last year")
    month_range_last_year = re.search(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+last year\b',
        user_query,
        re.IGNORECASE
    )
    
    if month_range_last_year:
        m1 = month_range_last_year.group(1).lower()
        m2 = month_range_last_year.group(2).lower()
        
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
            "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        
        s_month = month_map[m1[:3]]
        e_month = month_map[m2[:3]]
        
        last_fy_start_year = fy_start.year - 1
        
        s_year = last_fy_start_year if s_month >= 4 else last_fy_start_year + 1
        e_year = last_fy_start_year if e_month >= 4 else last_fy_start_year + 1
        
        if (e_year < s_year) or (e_month < s_month and e_year == s_year):
            e_year = s_year + 1
        
        start_date = date(s_year, s_month, 1)
        end_date = date(e_year, e_month, monthrange(e_year, e_month)[1])
        
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    # 17. Multiple months (3+ months)
    multi_month_matches = re.findall(r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b', user_query, re.IGNORECASE)
    
    if len(multi_month_matches) >= 3 and re.search(r'\b(and|,)\b', user_query, re.IGNORECASE):
        month_map = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
            "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        
        month_nums = sorted(set([month_map[m.lower()[:3]] for m in multi_month_matches]))
        
        # Check year
        explicit_year_match = re.search(r'\b(20\d{2})\b', user_query)
        if explicit_year_match:
            y = int(explicit_year_match.group(1))
        else:
            y = fy_start.year if month_nums[0] >= 4 else fy_end.year
        
        # Create OR conditions
        conditions = []
        for m in month_nums:
            last_day = monthrange(y, m)[1]
            start = date(y, m, 1)
            end = date(y, m, last_day)
            conditions.append(
                f"(DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start}' AND DATE '{end}')"
            )
        
        if conditions:
            clause = " OR ".join(conditions)
            if len(conditions) > 1:
                clause = f"({clause})"
            
            # Add grouping if needed
            if needs_month_grouping:
                if not re.search(r'EXTRACT\(YEAR FROM DATE_PARSE', sql, re.IGNORECASE):
                    sql = re.sub(
                        r'SELECT\s+ABS\(SUM\(CAST\(',
                        "SELECT EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) AS year_num, EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) AS month_num, ABS(SUM(CAST(",
                        sql,
                        count=1,
                        flags=re.IGNORECASE
                    )
            
            sql = insert_where_before_groupby(sql, clause)
            
            if needs_month_grouping and "GROUP BY" not in sql.upper():
                sql = sql.rstrip()
                if sql.endswith(';'):
                    sql = sql[:-1]
                sql += " GROUP BY EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')), EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')) ORDER BY year_num, month_num"
            return sql
    
    
    # This should create year-wise grouping without restricting to current year
    month_range_yearwise_pattern = re.search(
        r'\b(from\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(?:year\s+wise|yearly|year on year|yoy|by year)\b',
        user_query,
        re.IGNORECASE
    )
    
    if month_range_yearwise_pattern:
        # This is a year-wise query for a month range - don't apply date filter
        # The AI already generates proper year-wise grouping
        return sql
    
    # Also check the reverse pattern "year wise from april to september"
    yearwise_month_range_pattern = re.search(
        r'\b(?:year\s+wise|yearly|year on year|yoy|by year)\s+(?:from\s+)?'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s*(?:to|-|through)\s*'
        r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|'
        r'sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b',
        user_query,
        re.IGNORECASE
    )
    
    if yearwise_month_range_pattern:
        # This is also a year-wise query for a month range - don't apply date filter
        return sql
    
    # 19. Single month (e.g., "april", "apr", "April 2025")
    single_month_match = re.search(
        r'\b(?:in\s+|for\s+|of\s+|between\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s*(?:,|\s)\s*(20\d{2}))?\b',
        user_query, re.IGNORECASE
    )
    
    if single_month_match:
        month_str = single_month_match.group(1).lower()
        explicit_year = single_month_match.group(2)
        
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        
        m = month_map[month_str[:3]]
        
        if explicit_year:
            y = int(explicit_year)
        else:
            y = fy_end.year if m < 4 else fy_start.year
        
        last_day = monthrange(y, m)[1]
        start = date(y, m, 1)
        end = date(y, m, last_day)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 "LAST/CURRENT" TIME PERIODS
    # 20. "First half" / "Second half" of year
    half_year_match = re.search(
        r'\b(first|second|1st|2nd)\s+half(?:\s+(?:of|for))?\s*(?:FY)?\s*(20\d{2})?\b',
        user_query,
        re.IGNORECASE
    )
    if half_year_match:
        half = half_year_match.group(1).lower()
        fy_year = int(half_year_match.group(2)) if half_year_match.group(2) else fy_start.year
        
        if half in ['first', '1st']:
            start, end = date(fy_year, 4, 1), date(fy_year, 9, 30)
        else:
            start, end = date(fy_year, 10, 1), date(fy_year + 1, 3, 31)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 21. "Current month" specifically
    if re.search(r'\bcurrent\s+month\b', user_query, re.IGNORECASE):
        month_start = date(today.year, today.month, 1)
        month_end = (date(today.year, today.month + 1, 1) - timedelta(days=1)) if today.month != 12 else date(today.year, 12, 31)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{month_start}' AND DATE '{month_end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 🆕 "Last year vs This year" (Handle combined range)
    if (re.search(r"\blast\s+(?:fiscal\s+)?year\b", user_query, re.I) and 
        re.search(r"\b(?:current|this)\s+(?:fiscal\s+)?year\b", user_query, re.I)):
        
        # Current FY
        curr_start = fy_start
        curr_end = fy_end
        
        # Last FY
        last_start = date(fy_start.year - 1, 4, 1)
        last_end = date(fy_start.year, 3, 31)
        
        # Combined range
        final_start = min(curr_start, last_start)
        final_end = max(curr_end, last_end)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{final_start}' AND DATE '{final_end}'"
        
        # Remove existing conflicting date filters
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)

    # 22. "Current year" / "this year"
    if re.search(r'\b(?:current|this)\s+(?:fiscal\s+)?year\b', user_query, re.IGNORECASE):
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        
        # Remove existing conflicting date filters
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
    
    # 23. "Last year"
    if re.search(r"\blast\s+(?:fiscal\s+)?year\b", user_query, re.I):
        last_fy_start = date(fy_start.year - 1, 4, 1)
        last_fy_end = date(fy_start.year, 3, 31)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{last_fy_start}' AND DATE '{last_fy_end}'"
        )
        
        # Remove existing conflicting date filters
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)
    
    # 24. "Yesterday"
    if re.search(r'\byesterday\b', user_query, re.IGNORECASE):
        yesterday = today - timedelta(days=1)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') = DATE '{yesterday}'"
        return insert_where_before_groupby(sql, clause)
    
    # 25. "Today"
    if re.search(r'\btoday\b', user_query, re.IGNORECASE):
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') = DATE '{today}'"
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 "LAST N" TIME PERIODS
    # 26. "Last X years" (e.g., "last 3 years", "past 5 years")
    last_n_years = re.search(r'\b(?:last|past|previous)\s+(\d{1,2})\s+years?\b', user_query, re.IGNORECASE)
    if last_n_years:
        n = int(last_n_years.group(1))
        if n > 0:
            # End at last completed FY
            end_date = date(fy_start.year, 3, 31)
            # Start n years before
            start_date = date(fy_start.year - n, 4, 1)
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
    
    # 27. Last N days
    last_n_days = re.search(r'\b(?:last|past|previous)\s+(\d{1,3})\s+days?\b', user_query, re.IGNORECASE)
    if last_n_days:
        n = int(last_n_days.group(1))
        if n > 0:
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=n-1)
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
    
    # 28. Last N months
    last_n_months = re.search(r'\b(?:last|past|previous)\s+(\d{1,3}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+months?\b', user_query, re.IGNORECASE)
    if last_n_months:
        num_str = last_n_months.group(1).lower()
        
        # Word to number mapping
        word_to_num = {
            'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
            'eleven': 11, 'twelve': 12
        }
        
        # Convert to number
        if num_str.isdigit():
            n = int(num_str)
        else:
            n = word_to_num.get(num_str, 1)  # Default to 1 if word not found
        
        if n > 0:
            end_year = today.year
            end_month = today.month - 1
            if end_month == 0:
                end_month = 12
                end_year -= 1
            end_day = monthrange(end_year, end_month)[1]
            end_date = date(end_year, end_month, end_day)
            
            def subtract_months(d, months):
                y = d.year
                m = d.month - months
                while m <= 0:
                    m += 12
                    y -= 1
                last_day = monthrange(y, m)[1]
                return date(y, m, 1)
            
            # CORRECT: Use n-1 for proper range
            start_date = subtract_months(end_date, n-1)  # n-1 gives correct 2-month range for "last 2 months"
            
            clause = (
                f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
            )
            return insert_where_before_groupby(sql, clause)
    
    # 29. Last N quarters
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
            
            if cm in (1, 2, 3):
                last_q_year = cy - 1
                last_q_num = 3
            elif cm in (4, 5, 6):
                last_q_year = cy - 1
                last_q_num = 4
            elif cm in (7, 8, 9):
                last_q_year = cy
                last_q_num = 1
            else:
                last_q_year = cy
                last_q_num = 2
            
            def quarter_range(q, y):
                if q == 1:  return date(y, 4, 1), date(y, 6, 30)
                if q == 2:  return date(y, 7, 1), date(y, 9, 30)
                if q == 3:  return date(y, 10, 1), date(y, 12, 31)
                if q == 4:  return date(y+1, 1, 1), date(y+1, 3, 31)
            
            end_start, end_date = quarter_range(last_q_num, last_q_year)
            
            # Calculate start quarter
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
    
    # 30. "Last week" / "This week"
    if re.search(r'\blast\s+week\b', user_query, re.IGNORECASE):
        days_since_monday = today.weekday()
        last_monday = today - timedelta(days=days_since_monday + 7)
        last_sunday = last_monday + timedelta(days=6)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{last_monday}' AND DATE '{last_sunday}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    if re.search(r'\bthis\s+week\b', user_query, re.IGNORECASE):
        days_since_monday = today.weekday()
        this_monday = today - timedelta(days=days_since_monday)
        clause = (
            f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
            f"BETWEEN DATE '{this_monday}' AND DATE '{today}'"
        )
        return insert_where_before_groupby(sql, clause)
    
    # 🔹 QUARTER/MONTH HELPERS
    # 31. Quarter helpers (for "this quarter", "last quarter")
    def quarter_start_end(d):
        month = d.month
        year = d.year
        if month in [4, 5, 6]:   # Q1
            return date(year, 4, 1), date(year, 6, 30)
        elif month in [7, 8, 9]: # Q2
            return date(year, 7, 1), date(year, 9, 30)
        elif month in [10, 11, 12]: # Q3
            return date(year, 10, 1), date(year, 12, 31)
        else: # Jan-Mar -> Q4
            # FIX: Q4 should be Jan-Mar of the CURRENT year, not previous year
            return date(year, 1, 1), date(year, 3, 31)
    
    def last_quarter_start_end(d):
        m = (d.month - 1) // 3 * 3 + 1 - 3
        y = d.year
        if m <= 0:
            m += 12
            y -= 1
        return quarter_start_end(date(y, m, 1))
    
    # 🆕 "Last quarter vs This quarter" (Handle combined range)
    if (re.search(r"\blast\s+quarter\b", user_query, re.I) and 
        re.search(r"\b(this|current)\s+quarter\b", user_query, re.I)):
        
        t_start, t_end = quarter_start_end(today)
        l_start, l_end = last_quarter_start_end(today)
        
        # Determine overall range
        final_start = min(t_start, l_start)
        final_end = max(t_end, l_end)
        
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{final_start}' AND DATE '{final_end}'"
        
        # Remove existing conflicting date filters
        sql = re.sub(
            r'(WHERE|AND)\s+DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|IN|=|>=|<=|>|<).*?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            r'\1 1=1',
            sql,
            flags=re.IGNORECASE,
        )
        
        return insert_where_before_groupby(sql, clause)

    # "this quarter" / "last quarter"
    if re.search(r"\bthis quarter\b", user_query, re.I):
        start, end = quarter_start_end(today)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    if re.search(r"\blast quarter\b", user_query, re.I):
        start, end = last_quarter_start_end(today)
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{start}' AND DATE '{end}'"
        return insert_where_before_groupby(sql, clause)
    
    # "this month" / "last month"
    month_start = date(today.year, today.month, 1)
    month_end = (date(today.year, today.month + 1, 1) - timedelta(days=1)) if today.month != 12 else date(today.year, 12, 31)
    
    if re.search(r"\b(this|current)\s+month\b", user_query, re.I):
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{month_start}' AND DATE '{month_end}'"
        return insert_where_before_groupby(sql, clause)
    
    # "last month"
    if today.month == 1:
        last_month_start = date(today.year - 1, 12, 1)
        last_month_end = date(today.year - 1, 12, 31)
    else:
        last_month_start = date(today.year, today.month - 1, 1)
        last_month_end = month_start - timedelta(days=1)
    
    if re.search(r"\blast month\b", user_query, re.I):
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{last_month_start}' AND DATE '{last_month_end}'"
        return insert_where_before_groupby(sql, clause)
    
    # 🆕 "Month A vs Month B" (Handle combined range) removed from here

    # 🔹 32. Format-based patterns (dd/mm/yyyy, mm/dd/yyyy, etc.)
    if re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}", user_query):
        # If AI already handled it, return as-is
        return sql
    
    # ============================================================
    # 🎯 FINAL FALLBACK: Add default financial year filter
    # ============================================================
    
    # Check if SQL already has any date filter
    has_date_filter = re.search(
        r'DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|>=|<=|>|<|=)',
        sql,
        re.IGNORECASE
    )
    
    if not has_date_filter:
        clause = f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        return insert_where_before_groupby(sql, clause)
    
    return sql



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
    """
    Ensures numeric value columns are sorted DESC in ORDER BY.
    Temporal grouping keys (year_num, month_num, quarter_num, period) are left untouched.
    If no ORDER BY exists, detects the main numeric alias from SELECT and adds one.
    """
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
                    new_parts.append(part)               # keep temporal cols as-is
                elif len(tokens) == 1:
                    new_parts.append(part + ' DESC')     # no direction → add DESC
                elif tokens[-1].upper() == 'ASC':
                    new_parts.append(' '.join(tokens[:-1]) + ' DESC')  # ASC → DESC
                else:
                    new_parts.append(part)               # already has DESC or complex expr
            return 'ORDER BY ' + ', '.join(new_parts)

        sql = re.sub(
            r'ORDER BY\s+(.+?)$',
            fix_order_clause,
            sql,
            flags=re.IGNORECASE
        )
    else:
        # No ORDER BY — find the main numeric alias in SELECT and add ORDER BY col DESC
        numeric_alias = re.search(
            r'\bAS\s+(refund|basic_amount|refund_amount|amount|total_amount|'
            r'total|chequebounce|bounce_amount|count|net_amount)\b',
            sql,
            re.IGNORECASE
        )
        if numeric_alias:
            col = numeric_alias.group(1)
            sql += f' ORDER BY {col} DESC'

    return sql


# --------------------
# Post-processing: Append Total row to multi-row results
# --------------------
def add_total_row(data: list) -> list:
    if not data or len(data) <= 1:
        return data

    keys = list(data[0].keys())
    lower_to_key = {k.lower(): k for k in keys}
    refund_metrics = [lower_to_key[m] for m in ('refund', 'refund_amount') if m in lower_to_key]
    net_sums = {m: 0.0 for m in refund_metrics}
    for row in data:
        for m in refund_metrics:
            v = row.get(m)
            try:
                if v is not None:
                    net_sums[m] += float(v)
            except (ValueError, TypeError):
                pass
    for row in data:
        for m in refund_metrics:
            v = row.get(m)
            try:
                if v is not None:
                    row[m] = abs(float(v))
            except (ValueError, TypeError):
                pass

    MEASURE_ALIASES = {
        'refund', 'basic_amount', 'refund_amount', 'amount', 'total_amount',
        'chequebounce', 'bounce_amount', 'count', 'net_amount', 'gross', 'gross_amount'
    }
    measures_present = [lower_to_key[a] for a in MEASURE_ALIASES if a in lower_to_key]

    total_row = {k: '-' for k in keys}

    for m_key in measures_present:
        if m_key in refund_metrics:
            total_row[m_key] = round(abs(net_sums[m_key]), 2)
        else:
            values = [row.get(m_key) for row in data if row.get(m_key) is not None]
            numeric_vals = []
            for v in values:
                if isinstance(v, (int, float)):
                    numeric_vals.append(float(v))
                else:
                    try:
                        numeric_vals.append(float(v))
                    except (ValueError, TypeError):
                        pass
            total_row[m_key] = round(sum(numeric_vals), 2) if numeric_vals else 0.0

    PREFERRED_LABEL_KEYS = ['Cust_name', 'sales_grp_descp', 'Bank_name', 'project_name']
    label_key = next((k for k in PREFERRED_LABEL_KEYS if k in keys), None)
    if label_key:
        total_row[label_key] = 'Total'
    else:
        if 'year_num' in keys:
            total_row['year_num'] = 'Total'
        elif 'month_num' in keys:
            total_row['month_num'] = 'Total'
        else:
            fallback_key = next((k for k in keys if k not in measures_present), keys[0] if keys else None)
            if fallback_key:
                total_row[fallback_key] = 'Total'

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
    elif month in [1,2,3]: # Jan-Mar -> Q4
        return date(year,1,1), date(year,3,31)

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
for the refunds dataset.

REQUIREMENTS:

- Always reference the table as "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}".
- Allowed columns (use exact casing): "Comp_code","fiscal_year","POST_date","Cust_no","Cust_name",
  "sales_doc_no","cust_po_no","sales_grp","sales_grp_descp","mat_descp","Recipt_date",
  "account_doc_no","Doc_hrd_text","cheque_dd","Doc_date_doc","gen_ledger","Bank_name",
  "Basic_amount","Service_tax","Gross_amount","Text","remarks"
  
- Company code / project mapping (treat these as synonyms):
    * "wave city"      ↔ company code 1000
    * "wmcc","wmcc sec 32"  ↔ company code 1100
    * "wave estate"    ↔ company code 1300

  IMPORTANT RULES:
    * NEVER add any filter on "Comp_code" if the user does NOT mention any of these project names.
      - For generic questions like "total refund", "overall refund", etc., DO NOT reference "Comp_code" at all.
    * If the user mentions **wave city**:
        - Add: WHERE "Comp_code" = 1000
    * If the user mentions **wmcc**:
        - Add: WHERE "Comp_code" = 1100
    * If the user mentions **wave estate**:
        - Add: WHERE "Comp_code" = 1300
    * If the user mentions multiple projects (e.g. "wave city and wmcc"):
        - Use: WHERE "Comp_code" IN (1000,1100)  (include all mentioned codes)
    * Never assume a default company. Never write "Comp_code = '1000'" unless the user said "wave city".


- Refund expression (Presto-compatible):
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
    ) AS refund

- Gross amount queries → treat exactly the same as refund.
- Basic_amount queries → use:
    SUM(ABS("Basic_amount")) AS basic_amount

- For customer-wise or bank-wise queries:
    * Always SELECT "Cust_no", "Cust_name"
    * If a specific bank is requested → also SELECT "Bank_name"
    * Always include GROUP BY corresponding columns:
        - customer-wise: GROUP BY "Cust_no", "Cust_name"
        - customer-wise + bank: GROUP BY "Cust_no", "Cust_name", "Bank_name"
    * Always order by the amount descending:
        - refund → ORDER BY refund DESC
        - gross → ORDER BY refund DESC
        - basic_amount → ORDER BY basic_amount DESC

- Filtering by Cust_name:
       WHERE LOWER(REGEXP_REPLACE(TRIM("Cust_name"), '[^a-zA-Z0-9 ]', '')) LIKE '%<customer name>%'
       * This ensures that searching for "akriti" will match "akriti", "aakriti", "Akriti Homes", etc.
       * Always use LIKE '%...%' (NOT = or exact match)
       * Convert user input to lowercase for comparison
       * Remove special characters from customer name for matching
       * Partial match means substring matching works

- **BANK NAME FILTERING — CRITICAL:**

  The "Bank_name" column contains values like:
    "Axis 4910 out", "AXIS 7582 out", "Axis 8647 out", "AXIS BAN - 3697 OUT",
    "AXIS BANK 8856 OUT", "Axis Bank -9643 Out", "Axis out-5482 out",
    "CCPL Mohali-2391 IN", "CCPL Mohali-2391 OUT",
    "Indusind - 2000 Out",
    "Kotak -9933 out",
    "P & S Bank 1342 Out", "PSB Bank - 1377 Out", "Pun & Sind 1099 (O)",
    "Yes 0023 out", "YES 0066 out", "YES BANK 0018 OUT", "YES BANK 0058 OUT",
    "YES BANK 0088 OUT", "YES BANK 0221 OUT", "Yes Bank G2 737 Out",
    "Yes Bnk 2356-10 Out", "Yes out 0075 out", "YES RERA 0121 OUT",
    "YesBank A/c8660 OUT"

  Use EXACTLY these SQL conditions:
    * AXIS:     LOWER("Bank_name") LIKE '%axis%'
    * CCPL:     LOWER("Bank_name") LIKE '%ccpl%'
    * INDUSIND: LOWER("Bank_name") LIKE '%indusind%'
    * KOTAK:    LOWER("Bank_name") LIKE '%kotak%'
    * YES BANK: LOWER("Bank_name") LIKE '%yes%'
      (covers Yes, YES, Yes Bank, Yes Bnk, YesBank, YES RERA, Yes out, etc.)
    * P&S/PSB/PUN&SIND:
      (REGEXP_LIKE(LOWER("Bank_name"), 'p\\s*&\\s*s')
       OR LOWER("Bank_name") LIKE '%psb%'
       OR REGEXP_LIKE(LOWER("Bank_name"), 'pun\\s*&\\s*sind'))

  Rules:
    1. ALWAYS use LOWER("Bank_name") — never exact match.
    2. For "yes bank" / "yes" / "yes bnk" / "yesbank" → LIKE '%yes%'
    3. For multiple banks → use OR:
       WHERE (LOWER("Bank_name") LIKE '%axis%' OR LOWER("Bank_name") LIKE '%yes%')
    4. NEVER strip '&' from the search.
    5. NEVER use = for Bank_name.

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

- **QUARTER QUERY RULES - CRITICAL:**
    * For queries like "show me total refund q1 2024", "total refund q1 2024", "total refund in quarter 1 2024":
        - These ask for a SINGLE TOTAL VALUE
        - DO NOT add GROUP BY month
        - DO NOT add month_num or year_num columns
        - Return: SELECT ABS(SUM(...)) AS refund FROM ... WHERE ... (single row result)
    
    * ONLY add month-by-month breakdown when user explicitly asks:
        - "breakdown by month"
        - "monthly breakdown"
        - "show monthly"
        - "month on month"
        - "each month"
        - Then use: SELECT year_num, month_num, ABS(SUM(...)) AS refund ... GROUP BY year_num, month_num
    
    * Quarter detection patterns (all should work):
        - "q1", "Q1", "q2", "Q2", "q3", "Q3", "q4", "Q4"
        - "quarter 1", "quarter 2", "quarter 3", "quarter 4"
        - "first quarter", "second quarter", "third quarter", "fourth quarter"
        - "in q1", "in quarter 1"
    
    * Quarter date ranges (Indian FY: Apr-Mar):
        - Q1: April 1 to June 30
        - Q2: July 1 to September 30
        - Q3: October 1 to December 31
        - Q4: January 1 to March 31 (next year)

- You are an expert Presto SQL generator for refund data. Follow these rules for Month-on-Month (MoM) queries:

1. Always extract YEAR and MONTH from POST_date as:
   - EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS year_num
   - EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS month_num

2. For TOTAL refund (no customer or sales group filter):
   - SELECT year_num, month_num, SUM(refund) AS refund_amount
   - Wrap as subquery if needed and ORDER BY t.year_num, t.month_num

3. For CUSTOMER-wise queries (all customers):
   - Include "Cust_no" and "Cust_name"
   - GROUP BY Cust_no, Cust_name, year_num, month_num
   - ORDER BY year_num, month_num, refund_amount DESC

- **CRITICAL FIX FOR SALES GROUP QUERIES:**
    * For sales group queries, you MUST SELECT BOTH "sales_grp" AND "sales_grp_descp"
    * You MUST GROUP BY BOTH "sales_grp" AND "sales_grp_descp"
    * Example for "Show me the total refund amount for each sales group for 2021 and 2025":
        SELECT "sales_grp", "sales_grp_descp", 
               ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') END AS DOUBLE))) AS refund
        FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
        WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '2021-04-01' AND DATE '2026-03-31'
        GROUP BY "sales_grp", "sales_grp_descp"
        ORDER BY refund DESC;
    * NEVER forget to include "sales_grp" in GROUP BY when you select it

- Sales Group rules:
    1. Always SELECT "sales_grp", "sales_grp_descp", plus the required amount expression for sales-group queries.
    2. Always GROUP BY "sales_grp", "sales_grp_descp".
    3. Always ORDER BY the amount expression descending.
    4. If the user specifies a sales group by name (e.g. "amore", "veridia", "new plots","SWAMANORATH","swamanorath","EXECUTIVE FLOORS","DREAM HOMES","LIG_P2","OLD PLOTS"), 
       filter ONLY on "sales_grp_descp":
         WHERE LOWER("sales_grp_descp") = '<name in lowercase>'
    5. If the user specifies a sales group by code (e.g. 101), 
       filter ONLY on "sales_grp":
         WHERE "sales_grp" = '101'
    6. Never use "Cust_name" or "Doc_hrd_text" when the question is about a sales group.
    7. If no sales group is specified, return all groups (no WHERE).


- **CRITICAL: Sales Group Name Detection:**
    * Known sales group names (case-insensitive) — these are ALL valid values of "sales_grp_descp":
      "amore", "comm booth", "dream bazaar", "dream homes", "eden", "edenia", "eligo",
      "ews_001_(410)", "ews_p2", "executive floors", "fsi", "harmony greens", "hssc",
      "institutional", "irenia", "lig_001_(310)", "lig_p2", "livork", "mayfair park",
      "metro mart", "new plots", "old plots", "plots-comm", "plots-res", "plots-res-if",
      "prime floors", "sco", "swamanorath", "trucia", "vasilia", "veridia", "veridia-3",
      "veridia-4", "veridia-5", "veridia-6", "veridia-7", "villas", "wave business square",
      "wave city- rental", "wave estate, gh2 ph2", "wave floor", "wave floor 85",
      "wave floor 99", "wave galleria", "wave garden", "wbt 1", "wbt a"
    
    * When user asks for refund for ANY of these names (e.g. "show refund for amore",
      "show refund for fsi", "refund for veridia", "refund for lig_p2"):
        1. Filter on "sales_grp_descp", NOT "Cust_name"
        2. Use: WHERE LOWER("sales_grp_descp") LIKE '%<name>%'
        3. Always SELECT "sales_grp", "sales_grp_descp" 
        4. Always GROUP BY "sales_grp", "sales_grp_descp"
    
    * Example: "total refund for livork"
        SELECT "sales_grp", "sales_grp_descp", 
               ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' 
                   THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') 
                   ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') 
               END AS DOUBLE))) AS refund
        FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
        WHERE LOWER("sales_grp_descp") LIKE '%livork%'
          AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') 
              BETWEEN DATE '2025-04-01' AND DATE '2026-03-31'
        GROUP BY "sales_grp", "sales_grp_descp";
    
    * NEVER use "Cust_name" filter when the query mentions a sales group name
    * NEVER use customer columns when filtering by sales group    

- **CRITICAL: "Separately" & Multiple Sales Groups:**
    * When user asks for multiple sales groups with "separately", "each", "individual", or similar:
        - Example: "Total refund till september for eden and veridia separately"
        - This means: Filter to ONLY those groups, show each one separately
        - DO NOT return all sales groups
        - Create filter using WHERE LOWER("sales_grp_descp") IN ('eden', 'veridia')
    
    * Implementation:
        - Detect keywords: "separately", "each", "individually", "for each", "per group"
        - Combined with multiple sales group names (eden, veridia, amore, etc.)
        - Apply: WHERE LOWER("sales_grp_descp") IN ('<name1>', '<name2>', ...)
        - Always GROUP BY "sales_grp", "sales_grp_descp" to keep them separate in results
    
    * Example: "Total refund till september for eden and veridia separately"
        SELECT "sales_grp", "sales_grp_descp", 
               ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' 
                   THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') 
                   ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') 
               END AS DOUBLE))) AS refund
        FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
        WHERE LOWER("sales_grp_descp") IN ('eden', 'veridia')
          AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '2025-04-01' AND DATE '2025-09-30'
        GROUP BY "sales_grp", "sales_grp_descp"
        ORDER BY refund DESC;
    
    * Pattern matching for multiple groups:
        - "for <group1> and <group2> separately" → IN clause
        - "for <group1>, <group2>, <group3> separately" → IN clause with all groups
        - "each <group>" → single group LIKE filter
        - Never assume user wants ALL groups when specific names are mentioned

5. Always handle refund amounts:
   - If value ends with '-', prepend '-' and remove trailing minus
   - Remove non-numeric characters before casting to DOUBLE
   - Wrap SUM with ABS to get positive totals

6. Use aliases for year_num and month_num in GROUP BY and ORDER BY; do not repeat full EXTRACT expressions

7. Do not leave trailing commas in SELECT, GROUP BY, or ORDER BY

Return only the Presto SQL query, ending with a semicolon. Do not add explanations.

- Rules:
  1. Always start SQL with SELECT and return a **single-line SQL ending with semicolon**. No explanations.
  2. Quarter-on-Quarter (QonQ / "quarter on quarter") MUST use this exact pattern with FINANCIAL YEAR quarters:
     
     SELECT
       EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS year_num,
       CASE 
         WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 4 AND 6 THEN 1
         WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 7 AND 9 THEN 2
         WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 10 AND 12 THEN 3
         ELSE 4
       END AS quarter_num,
       <amount expression> AS <alias>
     FROM ...
     WHERE <date filter if needed>
     GROUP BY
       EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')),
       CASE 
         WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 4 AND 6 THEN 1
         WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 7 AND 9 THEN 2
         WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) BETWEEN 10 AND 12 THEN 3
         ELSE 4
       END
     ORDER BY year_num, quarter_num;

  3. This ensures quarters align with Financial Year (Apr-Jun=Q1, Jul-Sep=Q2, Oct-Dec=Q3, Jan-Mar=Q4)
  4. Do NOT use CEIL() for quarter calculation - it gives calendar quarters, not FY quarters
  5. Do NOT use DATE_ADD in QoQ queries
  6. Do NOT use aliases in GROUP BY (repeat the full CASE expression)
  7. Always order by `year_num, quarter_num`

- If the user specifies a quarter in a given FY (e.g., "Q1 FY 2024"):
    * Always map quarters using Indian FY (Apr–Mar):
        - Q1 FY YYYY → Apr 1 YYYY to Jun 30 YYYY
        - Q2 FY YYYY → Jul 1 YYYY to Sep 30 YYYY
        - Q3 FY YYYY → Oct 1 YYYY to Dec 31 YYYY
        - Q4 FY YYYY → Jan 1 (YYYY+1) to Mar 31 (YYYY+1)
    * Only generate ONE BETWEEN condition for this range.
    * Example: "Q1 FY 2024" → 
        BETWEEN DATE '2024-04-01' AND DATE '2024-06-30'
    
- **YEAR-ON-YEAR (YoY) RULES - CRITICAL FIX:**
    * DETECT keywords: "year on year", "yoy", "year-over-year", "by year", "yearly", "year wise"
    * When detected AND query includes "sales group":
        - DO NOT use CASE expression for year_num
        - Instead use simple: EXTRACT(YEAR FROM DATE_PARSE(...)) AS year_num
        - SELECT: year_num, "sales_grp", "sales_grp_descp", refund
        - GROUP BY: EXTRACT(YEAR FROM DATE_PARSE(...)), "sales_grp", "sales_grp_descp"
        - This ensures EVERY non-aggregated column is in GROUP BY
    * When detected WITHOUT sales group:
        - Use CASE expression for financial year logic
        - SELECT: year_num, refund
        - GROUP BY: CASE WHEN ... END
    * Never add DATE_PARSE BETWEEN clause for pure YoY queries
    * Example for "YoY total refund by sales group":
        SELECT EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) AS year_num, "sales_grp", "sales_grp_descp", ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') END AS DOUBLE))) AS refund FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" GROUP BY EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')), "sales_grp", "sales_grp_descp" ORDER BY year_num, refund DESC;

- For Year-on-Year (YoY) / "year on year" / "year-over-year" / "YoY" / "by year"/"year wise" queries WITHOUT sales group:
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
                <amount expression> AS <alias>
            FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
            GROUP BY 
                CASE 
                    WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) >= 4 
                    THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) 
                    ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) - 1 
                END
            ORDER BY year_num;
        - Never write just "ORDER BY" without specifying "year_num".

- For Year-on-Year (YoY) / "year on year" / "year-over-year" / "YoY" / "by year" queries WITH CUSTOMER:
    * DETECT these keywords: "customer", "customer-wise", "cust", "each customer" PLUS "yoy", "year on year", "year-over-year", "by year", "year wise"
    * When detected:
        - DO NOT add any DATE_PARSE(...) BETWEEN ... clause.
        - DO NOT apply the default financial year filter.
        - CRITICAL: You MUST include "Cust_no" and "Cust_name" in BOTH SELECT AND GROUP BY
        - Instead, aggregate BY FINANCIAL YEAR (April–March) AND customer using EXACTLY this pattern:
            SELECT 
                CASE 
                    WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) >= 4 
                    THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) 
                    ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) - 1 
                END AS year_num,
                "Cust_no",
                "Cust_name",
                ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') END AS DOUBLE))) AS refund
            FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
            GROUP BY 
                CASE 
                    WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) >= 4 
                    THEN EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) 
                    ELSE EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d')) - 1 
                END,
                "Cust_no",
                "Cust_name"
            ORDER BY year_num, refund DESC;
        - **MANDATORY:** Every non-aggregated column in SELECT must appear in GROUP BY
        - **MANDATORY:** "Cust_no", "Cust_name" MUST be in GROUP BY when they are in SELECT
        - **MANDATORY:** If you SELECT "Cust_no", you MUST GROUP BY "Cust_no"
        - **MANDATORY:** If you SELECT "Cust_name", you MUST GROUP BY "Cust_name"
        - Never skip customer columns from GROUP BY - this causes Presto SYNTAX_ERROR


1. Always wrap **customer-wise Month-on-Month (MoM) or Year-on-Year (YoY) queries** in a subquery AS t.
2. Aliases:
   - MoM: year_num, month_num
   - YoY: year_num
   - Amount: refund
3. Use aliases in GROUP BY and ORDER BY; never repeat full EXTRACT or CASE expressions.
4. Handle trailing minus in "Gross_amount":
    CASE WHEN TRIM("Gross_amount") LIKE '%-' THEN '-' || REGEXP_REPLACE(TRIM("Gross_amount"), '-$', '') ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') END
   Wrap SUM(...) with ABS.
5. Customer-wise queries must SELECT "Cust_no", "Cust_name".
6. Always ORDER BY refund DESC for customer-wise.
7. Do NOT add default FY BETWEEN clause for YoY.
8. MoM queries: filter by financial year if specified; otherwise, wrap entire inner query as subquery and aggregate by month.
9. Non-customer-wise queries: aggregate normally by month or year, wrap as subquery AS t if needed.
10. Use DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') for all date operations.
11. Always return a single-line Presto SQL ending with semicolon; no explanations.

- **FIXED SALES GROUP QUERIES:**
    * For "Show me the total refund amount for each sales group", you MUST:
        1. SELECT both "sales_grp" AND "sales_grp_descp"
        2. GROUP BY both "sales_grp" AND "sales_grp_descp"
        3. NEVER select "sales_grp" without including it in GROUP BY
        4. Example correct query:
           SELECT "sales_grp", "sales_grp_descp", ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') END AS DOUBLE))) AS refund
           FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
           WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '2021-04-01' AND DATE '2026-03-31'
           GROUP BY "sales_grp", "sales_grp_descp"
           ORDER BY refund DESC;

- **PROJECT-WISE QUERIES (using Company Code mapping):**
    * Detection patterns: "project wise", "project-wise", "each project", "by project", "project breakdown", "show projects", "project wise refund"
    * CRITICAL: For "project wise" queries, use the company/project mapping (NOT sales_grp_descp):
        - "wave city" → Comp_code = 1000
        - "wmcc" or "wmcc sec 32" → Comp_code = 1100  
        - "wave estate" → Comp_code = 1300
    
    * Implementation for general project-wise queries (no specific project mentioned):
        SELECT CASE 
                 WHEN "Comp_code" = 1000 THEN 'wave city'
                 WHEN "Comp_code" = 1100 THEN 'wmcc'
                 WHEN "Comp_code" = 1300 THEN 'wave estate'
                 ELSE CAST("Comp_code" AS VARCHAR)
               END AS project_name,
               ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' 
                   THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') 
                   ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') 
               END AS DOUBLE))) AS refund
        FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
        WHERE DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start_str}' AND DATE '{fy_end_str}'
        GROUP BY "Comp_code"
        ORDER BY refund DESC;
    
    * If user mentions specific projects (e.g., "project wise for wave city and wmcc"):
        SELECT CASE 
                 WHEN "Comp_code" = 1000 THEN 'wave city'
                 WHEN "Comp_code" = 1100 THEN 'wmcc'
                 WHEN "Comp_code" = 1300 THEN 'wave estate'
               END AS project_name,
               ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' 
                   THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') 
                   ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') 
               END AS DOUBLE))) AS refund
        FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
        WHERE "Comp_code" IN (1000, 1100)
          AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start_str}' AND DATE '{fy_end_str}'
        GROUP BY "Comp_code"
        ORDER BY refund DESC;
    
    * If user asks for "product wise" AND mentions sales group names (amore, veridia, etc.), use sales group rules instead
    * If user asks for "project wise" without specifying sales groups, use company code mapping above
    * DO NOT use "sales_grp_descp" for project-wise queries about wave city, wmcc, wave estate

- **SALES GROUP / PRODUCT QUERIES (for sales group products like amore, veridia):**
    * Detection patterns: "sales group wise", "sales group breakdown", "by sales group", "sales wise", "product wise" [when user also mentions sales group names like amore, veridia, etc.]
    * For these, use sales group rules (sales_grp, sales_grp_descp)
    * Example for "product wise refund for amore and veridia":
        SELECT "sales_grp", "sales_grp_descp", 
               ABS(SUM(CAST(CASE WHEN TRIM("Gross_amount") LIKE '%-' 
                   THEN '-' || REGEXP_REPLACE(REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$', ''), '[^0-9.]', '') 
                   ELSE REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') 
               END AS DOUBLE))) AS refund
        FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
        WHERE LOWER("sales_grp_descp") IN ('amore', 'veridia')
          AND DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_start_str}' AND DATE '{fy_end_str}'
        GROUP BY "sales_grp", "sales_grp_descp"
        ORDER BY refund DESC;

- **KEY DISTINCTION:**
    * "Projects" = wave city, wmcc, wave estate → Use Comp_code mapping
    * "Sales Groups/Products" = amore, veridia, new plots, etc. → Use sales_grp_descp
    * Always check what the user is asking for and use the appropriate logic

- **CRITICAL: CONSISTENCY RULE**
  * For the EXACT same question, always generate the EXACT same SQL structure
  * Never randomly switch between customer-wise and sales-group queries
  * If query mentions sales group names (veridia, amore, etc.), NEVER add customer columns
  * If query mentions "month on month" or "mom", ALWAYS include year_num and month_num in SELECT
  * If query mentions "project wise" without sales group names, use company code mapping
  * If query mentions "product wise" with sales group names, use sales group columns

User question: "{user_query}"
SQL:
"""

    raw = model.generate_text(prompt=prompt)
    return raw

# def normalize_sql(sql: str) -> str:
#     sql = sql.replace("```", "").replace("`", '"')
#     sql = sql.replace('\\"', '"').replace("\\'", "'")
#     sql = re.sub(r"\s+", " ", sql).strip()

#     # DO NOT extract SELECT…; EVER  
#     # (never put that block back here)

#     sql = sql.rstrip(";").strip()

#     # Fix quoted numbers
#     sql = re.sub(r'=\s*\'(\d+)\'', r'= \1', sql)

#     # Fix ORDER BY missing columns
#     if re.search(r'ORDER BY\s*$', sql, re.IGNORECASE):
#         if "quarter_num" in sql:
#             sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num, quarter_num', sql, flags=re.IGNORECASE)
#         elif "month_num" in sql:
#             sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num, month_num', sql, flags=re.IGNORECASE)
#         else:
#             sql = re.sub(r'ORDER BY\s*$', 'ORDER BY refund', sql, flags=re.IGNORECASE)

#     # Fix incorrect alias quarter → quarter_num
#     sql = re.sub(
#         r'ORDER BY\s+year_num\s*,\s*quarter\b',
#         'ORDER BY year_num, quarter_num',
#         sql,
#         flags=re.IGNORECASE
#     )

#     # Expand GROUP BY aliases
#     year_expr = 'EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
#     month_expr = 'EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'

#     sql = re.sub(
#         r'GROUP BY\s+year_num\s*,\s*month_num',
#         f'GROUP BY {year_expr}, {month_expr}',
#         sql,
#         flags=re.IGNORECASE,
#     )
#     sql = re.sub(
#         r'GROUP BY\s+year_num\b',
#         f'GROUP BY {year_expr}',
#         sql,
#         flags=re.IGNORECASE,
#     )

#     return sql.strip()






def normalize_sql(sql: str, user_query: str = "") -> str:
    sql = sql.replace("```", "").replace("`", '"')
    sql = sql.replace('\\"', '"').replace("\\'", "'")
    sql = re.sub(r"\s+", " ", sql).strip()

    if user_query:
        q_lower = user_query.lower()
        exact_clause = detect_exact_bank_clause(q_lower)
        if exact_clause:
            bank_filter_pattern = r'(?:REGEXP_LIKE\s*\(\s*LOWER\s*\(\s*"Bank_name"\s*\)[^)]*\)|LOWER\s*\(\s*"Bank_name"\s*\)\s*LIKE\s*\'[^\']*\')'
            sql = re.sub(
                bank_filter_pattern,
                lambda m: exact_clause,
                sql,
                flags=re.IGNORECASE,
                count=1,
            )
        else:
            bank_conditions = detect_banks_in_query(q_lower)
            if bank_conditions:
                canonical_clause = build_bank_where_clause(bank_conditions)
                bank_filter_pattern = r'(?:REGEXP_LIKE\s*\(\s*LOWER\s*\(\s*"Bank_name"\s*\)[^)]*\)|LOWER\s*\(\s*"Bank_name"\s*\)\s*LIKE\s*\'[^\']*\')'
                sql = re.sub(
                    bank_filter_pattern,
                    lambda m: canonical_clause,
                    sql,
                    flags=re.IGNORECASE,
                    count=1,
                )

    # 🔹 PRESERVE LOGIC: Remove quotes around CURRENT_DATE FIRST
    sql = sql.replace("DATE 'CURRENT_DATE'", "CURRENT_DATE")
    sql = sql.replace("'CURRENT_DATE'", "CURRENT_DATE")

    # Extract SELECT ... part if wrapped
    match = re.search(r"(SELECT .*?;)", sql, re.IGNORECASE)
    if match:
        sql = match.group(1)

    sql = sql.rstrip(";").strip()

    # 🔹 PRESERVE LOGIC: Fix numbers quoted as strings
    sql = re.sub(r'=\s*\'(\d+)\'', r'= \1', sql)

    # 🔹 PRESERVE LOGIC: Remove WHERE 1=1 clauses
    sql = re.sub(r'\s+WHERE\s+1=1\s+AND\s+', ' WHERE ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+WHERE\s+1=1\s+', ' WHERE ', sql, flags=re.IGNORECASE)

    # 🔹 PRESERVE LOGIC: Handle trailing "ORDER BY"
    if re.search(r'ORDER BY\s*$', sql, re.IGNORECASE):
        if 'year_num' in sql and 'month_num' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num, month_num', sql, flags=re.IGNORECASE)
        elif 'year_num' in sql and 'quarter_num' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num, quarter_num', sql, flags=re.IGNORECASE)
        elif 'year_num' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY year_num', sql, flags=re.IGNORECASE)
        elif 'refund' in sql:
            sql = re.sub(r'ORDER BY\s*$', 'ORDER BY refund', sql, flags=re.IGNORECASE)
        else:
            sql = re.sub(r'ORDER BY\s*$', '', sql, flags=re.IGNORECASE).strip()

    # 🔹 PRESERVE LOGIC: Add sales_grp and sales_grp_descp to GROUP BY if missing
    if '"sales_grp"' in sql and '"sales_grp_descp"' in sql:
        groupby_match = re.search(r'GROUP BY\s+(.*?)(?:\s+ORDER BY|\s+LIMIT|$)', sql, re.IGNORECASE)
        
        if groupby_match:
            groupby_clause = groupby_match.group(1).strip()
            has_sales_grp = '"sales_grp"' in groupby_clause
            has_sales_grp_descp = '"sales_grp_descp"' in groupby_clause
            
            if not (has_sales_grp and has_sales_grp_descp):
                groupby_clause = groupby_clause.rstrip().rstrip(',')
                new_groupby = groupby_clause + ', "sales_grp", "sales_grp_descp"'
                
                sql = re.sub(
                    r'GROUP BY\s+(.*?)(?=\s+ORDER BY|\s+LIMIT|$)',
                    f'GROUP BY {new_groupby}',
                    sql,
                    flags=re.IGNORECASE
                )

    # 🔹 PRESERVE LOGIC: YoY + Customer-wise missing GROUP BY columns
    if '"Cust_no"' in sql and '"Cust_name"' in sql:
        groupby_match = re.search(r'GROUP BY\s+(.*?)(?:\s+ORDER BY|\s+LIMIT|$)', sql, re.IGNORECASE)
        
        if groupby_match:
            groupby_clause = groupby_match.group(1).strip()
            has_cust_in_groupby = ('"Cust_no"' in groupby_clause and '"Cust_name"' in groupby_clause)
            
            if not has_cust_in_groupby:
                groupby_clause = groupby_clause.rstrip().rstrip(',')
                new_groupby = groupby_clause + ', "Cust_no", "Cust_name"'
                
                sql = re.sub(
                    r'GROUP BY\s+(.*?)(?=\s+ORDER BY|\s+LIMIT|$)',
                    f'GROUP BY {new_groupby}',
                    sql,
                    flags=re.IGNORECASE
                )

    # 🔹 PRESERVE LOGIC: Expand aliases safely
    year_expr = 'EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
    month_expr = 'EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
    quarter_expr = 'CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN 1 WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN 2 WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN 3 ELSE 4 END'

    if re.search(r'\bGROUP BY\b.*\byear_num\b.*\bquarter_num\b', sql, re.IGNORECASE):
        sql = re.sub(
            r'GROUP BY\s+year_num\s*,\s*quarter_num\b',
            f'GROUP BY {year_expr}, {quarter_expr}',
            sql,
            flags=re.IGNORECASE,
        )
    elif re.search(r'\bGROUP BY\b.*\byear_num\b.*\bmonth_num\b', sql, re.IGNORECASE):
        sql = re.sub(
            r'GROUP BY\s+year_num\s*,\s*month_num\b',
            f'GROUP BY {year_expr}, {month_expr}',
            sql,
            flags=re.IGNORECASE,
        )
    elif re.search(r'\bGROUP BY\s+year_num\b', sql, re.IGNORECASE) and 'month_num' not in sql and 'quarter_num' not in sql:
        sql = re.sub(
            r'GROUP BY\s+year_num\b',
            f'GROUP BY {year_expr}',
            sql,
            flags=re.IGNORECASE,
        )

    return enforce_descending_order(sql.strip())

def detect_funding_type(query_lower: str) -> str:
    has_bank_funded = bool(re.search(r'\bbank\s*[- ]?funded\b', query_lower, re.IGNORECASE))
    has_self_funded = bool(re.search(r'\bself\s*[- ]?funded\b', query_lower, re.IGNORECASE))
    if has_bank_funded and has_self_funded:
        return "(LOWER(\"Text\") LIKE '%bank funded%' OR LOWER(\"Text\") LIKE '%self funded%')"
    elif has_bank_funded:
        return "LOWER(\"Text\") LIKE '%bank funded%'"
    elif has_self_funded:
        return "LOWER(\"Text\") LIKE '%self funded%'"
    return ""


def build_vs_query(user_query: str) -> str | None:
    # Match both "X vs Y" and "X and Y" comparison patterns
    vs_match = re.search(r'^(.+?)\s+(?:vs\.?|and)\s+(.+)$', user_query.strip(), re.IGNORECASE)
    if not vs_match:
        return None

    left_part  = vs_match.group(1).strip()
    right_part = vs_match.group(2).strip()

    comparison_patterns = [
        r'\b(last year|this year|current year)\b',
        r'\b(last month|this month|current month)\b',
        r'\b(last quarter|this quarter|current quarter)\b',
        r'\bq[1-4]\b', r'\bquarter\s*[1-4]\b', r'\bfy\s*\d{4}\b', r'\b\d{4}\b',
        r'\b(bank funded|self funded)\b',
        r'\b(axis|yes bank|yes|kotak|indusind|psb|ccpl)\b',
    ]

    def strip_comparison_entity(text: str) -> str:
        result = text
        for pat in comparison_patterns:
            result = re.sub(pat, '', result, flags=re.IGNORECASE)
        result = re.sub(r'\b(for|in|of|and|the|refund|total|show|me|amount)\b', '', result, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', result).strip()

    shared_subject = strip_comparison_entity(left_part)
    right_subject  = strip_comparison_entity(right_part)

    if not right_subject or len(right_part.split()) <= 4:
        right_query = f"total refund for {shared_subject} {right_part}".strip()
    else:
        right_query = right_part

    def ensure_refund_context(q: str) -> str:
        if not re.search(r'\b(refund|amount|total)\b', q, re.IGNORECASE):
            return f"total refund {q}"
        return q

    left_query  = ensure_refund_context(left_part)
    right_query = ensure_refund_context(re.sub(r'\s+', ' ', right_query))

    def make_label(text: str) -> str:
        label = re.sub(r'\b(total|refund|amount|show|me|for|the)\b', '', text, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', label).strip() or text.strip()

    left_label  = make_label(left_part)
    right_label = make_label(right_part)

    def segment_sql(segment_query: str) -> str | None:
        manual = build_manual_refund_sql(segment_query, _skip_vs=True)
        if manual:
            return manual
        raw        = nl_to_sql(segment_query)
        normalized = normalize_sql(raw, segment_query)
        return enforce_financial_year(normalized, segment_query)

    left_sql  = segment_sql(left_query)
    right_sql = segment_sql(right_query)

    if not left_sql or not right_sql:
        return None

    def safe_scalar(sql: str, label: str) -> str:
        sql = sql.rstrip(';').strip()
        escaped = label.replace("'", "\\'")
        return (
            f"SELECT '{escaped}' AS segment, "
            f"COALESCE(ABS(SUM(t.refund)), 0) AS refund "
            f"FROM ({sql}) t"
        )

    left_scalar  = safe_scalar(left_sql,  left_label)
    right_scalar = safe_scalar(right_sql, right_label)

    # Single-column versions for Total addition (Presto doesn't allow multi-column subqueries in expressions)
    left_scalar_val  = f"SELECT COALESCE(ABS(SUM(t.refund)), 0) FROM ({left_sql.rstrip(';')}) t"
    right_scalar_val = f"SELECT COALESCE(ABS(SUM(t.refund)), 0) FROM ({right_sql.rstrip(';')}) t"

    combined = f"SELECT segment, refund FROM ({left_scalar} UNION ALL {right_scalar}) combined"

    final_sql = (
        f"SELECT segment, refund FROM ({combined}) all_rows "
        f"UNION ALL "
        f"SELECT 'Total' AS segment, "
        f"ABS(({left_scalar_val}) + ({right_scalar_val})) AS refund "
        f"FROM (SELECT 1) dummy"
    )
    return final_sql


def ensure_positive_values(data: list) -> list:
    if not data:
        return data
    MEASURE_ALIASES = {
        'refund', 'basic_amount', 'refund_amount', 'amount', 'total_amount',
        'chequebounce', 'bounce_amount', 'count', 'net_amount', 'gross', 'gross_amount'
    }
    keys = list(data[0].keys())
    lower_to_key = {k.lower(): k for k in keys}
    measure_keys = [lower_to_key[a] for a in MEASURE_ALIASES if a in lower_to_key]
    for row in data:
        for mk in measure_keys:
            v = row.get(mk)
            if v is not None and v != '-':
                try:
                    row[mk] = abs(float(v))
                except (ValueError, TypeError):
                    pass
    return data
def _is_comparison_and_query(user_query: str) -> bool:
    """
    Returns True if the query uses 'and' as a comparison/versus operator
    between two comparable entities (banks, time periods, projects, etc.)
    rather than just listing things.
    
    Examples that should return True:
      "axis and yes bank refund"
      "q1 and q2 refund"
      "last year and this year"
      "wave city and wmcc refund"
    
    Examples that should return False:
      "refund for eden and veridia separately"  ← listing, not comparison
      "total refund"
      "customer wise refund"
    """
    q = user_query.lower()
    
    # Never treat as comparison if "separately" is present — user wants grouped listing
    if re.search(r'\bseparately\b', q):
        return False

    comparison_entity_patterns = [
        # Time period comparisons
        r'\b(last year|this year|current year)\b.*\band\b.*\b(last year|this year|current year)\b',
        r'\b(last month|this month|current month)\b.*\band\b.*\b(last month|this month|current month)\b',
        r'\b(last quarter|this quarter|current quarter)\b.*\band\b.*\b(last quarter|this quarter|current quarter)\b',
        r'\bq[1-4]\b.*\band\b.*\bq[1-4]\b',
        r'\bfy\s*\d{4}\b.*\band\b.*\bfy\s*\d{4}\b',
        # Bank comparisons (two distinct bank names with "and")
        r'\b(axis|yes bank|yes bnk|yesbank|kotak|indusind|indus ind|psb|p\s*&\s*s|ccpl)\b.*\band\b.*\b(axis|yes bank|yes bnk|yesbank|kotak|indusind|indus ind|psb|p\s*&\s*s|ccpl)\b',
        # Project comparisons
        r'\b(wave city|wmcc|wave estate)\b.*\band\b.*\b(wave city|wmcc|wave estate)\b',
        # Funding type comparisons
        r'\b(bank funded|self funded)\b.*\band\b.*\b(bank funded|self funded)\b',
    ]

    for pattern in comparison_entity_patterns:
        if re.search(pattern, q, re.IGNORECASE):
            return True

    return False
def build_separately_query(user_query: str) -> str | None:
    """
    Handles queries with 'separately' keyword.
    Detects what entities the user wants broken down and returns appropriate SQL.
    Works dynamically for projects, banks, sales groups, customers, time periods, etc.
    """
    q = user_query.lower()

    amount_expr_net = '''SUM(
        CAST(
          CASE
            WHEN TRIM("Gross_amount") LIKE '%-' 
                 OR TRIM("Gross_amount") LIKE '-%' 
                 OR REGEXP_LIKE(TRIM("Gross_amount"), '\\(')
              THEN -CAST(
                     REGEXP_REPLACE(
                       REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$|[()]', ''),
                       '[^0-9.]', ''
                     ) AS DOUBLE
                   )
            ELSE CAST(REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') AS DOUBLE)
          END AS DOUBLE
        )
      ) AS refund'''

    def apply_project_filter(sql: str) -> str:
        codes = []
        if "wave city" in q:
            codes.append(1000)
        if "wmcc sec 32" in q or "wmcc" in q:
            codes.append(1100)
        if "wave estate" in q:
            codes.append(1300)
        codes = sorted(set(codes))
        if not codes:
            return sql
        if len(codes) == 1:
            return sql + f' AND "Comp_code" = {codes[0]}'
        codes_str = ",".join(str(c) for c in codes)
        return sql + f' AND "Comp_code" IN ({codes_str})'

    # ── 1. PROJECT entities (wave city, wmcc, wave estate) ──────────────────
    project_keywords = ["wave city", "wmcc sec 32", "wmcc", "wave estate"]
    mentioned_projects = [kw for kw in project_keywords if kw in q]
    # deduplicate: if "wmcc sec 32" matched, don't also count plain "wmcc"
    if "wmcc sec 32" in mentioned_projects and "wmcc" in mentioned_projects:
        mentioned_projects = [p for p in mentioned_projects if p != "wmcc"]

    if mentioned_projects:
        # Build IN clause for only the mentioned projects
        code_map = {"wave city": 1000, "wmcc sec 32": 1100, "wmcc": 1100, "wave estate": 1300}
        codes = sorted(set(code_map[p] for p in mentioned_projects))
        codes_str = ",".join(str(c) for c in codes)

        sql = f'''SELECT
CASE
  WHEN "Comp_code" = 1000 THEN 'wave city'
  WHEN "Comp_code" = 1100 THEN 'wmcc'
  WHEN "Comp_code" = 1300 THEN 'wave estate'
  ELSE CAST("Comp_code" AS VARCHAR)
END AS project_name,
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE "Comp_code" IN ({codes_str})'''
        sql = enforce_financial_year(sql, user_query)
        sql += ' GROUP BY "Comp_code"'
        return enforce_descending_order(sql)

    # ── 2. BANK entities ─────────────────────────────────────────────────────
    bank_conditions = detect_banks_in_query(q)
    if bank_conditions:
        bank_where = build_bank_where_clause(bank_conditions)
        sql = f'''SELECT "Bank_name",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE {bank_where}'''
        sql = enforce_financial_year(sql, user_query)
        sql += ' GROUP BY "Bank_name"'
        return enforce_descending_order(sql)

    # ── 3. SALES GROUP entities ───────────────────────────────────────────────
    sales_grp_descp_list = sorted(set([
        "amore", "comm booth", "dream bazaar", "dream homes", "eden", "edenia",
        "eligo", "executive floors", "fsi", "harmony greens", "hssc",
        "institutional", "irenia", "lig_001_(310)", "lig_p2", "livork",
        "mayfair park", "metro mart", "new plots", "old plots", "plots-comm",
        "plots-res", "plots-res-if", "prime floors", "sco", "swamanorath",
        "trucia", "vasilia", "veridia", "veridia-3", "veridia-4", "veridia-5",
        "veridia-6", "veridia-7", "villas", "wave business square",
        "wave city- rental", "wave estate, gh2 ph2", "wave floor",
        "wave floor 85", "wave floor 99", "wave galleria", "wave garden",
        "wbt 1", "wbt a", "ews_001_(410)", "ews_p2"
    ]))
    matched_groups = [grp for grp in sorted(sales_grp_descp_list, key=len, reverse=True) if grp in q]
    if matched_groups:
        # Remove substrings already covered by longer matches
        filtered_groups = []
        for g in matched_groups:
            if not any(g != mg and g in mg for mg in matched_groups):
                filtered_groups.append(g)
        matched_groups = filtered_groups

        if matched_groups:
            in_clause = ", ".join(f"'{g}'" for g in matched_groups)
            sql = f'''SELECT "sales_grp", "sales_grp_descp",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE LOWER("sales_grp_descp") IN ({in_clause})'''
            sql = enforce_financial_year(sql, user_query)
            sql += ' GROUP BY "sales_grp", "sales_grp_descp"'
            return enforce_descending_order(sql)

    # ── 4. CUSTOMER name mentioned ────────────────────────────────────────────
    cust_match = re.search(
        r'\b(?:customer|cust(?:omer)?)\s+(?:name\s+)?["\']?([a-z0-9 ]+?)["\']?\s+(?:separately|and\b)',
        q
    )
    if cust_match or re.search(r'\b(customer wise|customer-wise|each customer|by customer)\b', q):
        sql = f'''SELECT "Cust_no", "Cust_name",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        sql = enforce_financial_year(sql, user_query)
        sql += ' GROUP BY "Cust_no", "Cust_name"'
        return enforce_descending_order(sql)

    # ── 5. FALLBACK: let the normal pipeline handle it (return None) ──────────
    return None
def detect_and_entities(user_query: str):
    """
    Detects whether an 'and' query is comparing two entities of the same type.
    Returns (entity_type, entities_list) or (None, [])
    
    Handles: projects, banks, quarters, time periods, sales groups
    Does NOT trigger if 'separately' is in query (that goes to build_separately_query)
    """
    q = user_query.lower()

    # "separately" queries are handled by build_separately_query, not here
    # "separately" queries are handled by build_separately_query, not here
    if re.search(r'\bseparately\b', q):
        return None, []

    # Must have "and" to be a comparison query
    if not re.search(r'\band\b', q):
        return None, []

    # Dimension/breakdown queries should NEVER be treated as "X and Y" comparisons.
    # "customer wise refund for axis and yes bank" means show customers, not axis vs yes.
    # "project wise refund wave city and wmcc" means show project rows, not wave city vs wmcc.
    _DIMENSION_PATTERNS = [
        r'\bcustomer[-\s]?wise\b', r'\bby\s+customer\b', r'\beach\s+customer\b',
        r'\bper\s+customer\b', r'\bcustomer\s+breakdown\b',
        r'\bproject[-\s]?wise\b', r'\bby\s+project\b', r'\beach\s+project\b',
        r'\bproduct[-\s]?wise\b',
        r'\bsales[-\s]?group[-\s]?wise\b', r'\bsales[-\s]?wise\b',
        r'\bbank[-\s]?wise\b', r'\bby\s+bank\b',
        r'\bmonth[-\s]?wise\b', r'\bmonthly\b', r'\bmonth\s+on\s+month\b', r'\bmom\b', r'\bby\s+month\b',
        r'\byear[-\s]?wise\b', r'\byear\s+on\s+year\b', r'\byoy\b', r'\byear[-\s]?over[-\s]?year\b',
        r'\bquarter[-\s]?wise\b', r'\bquarter\s+on\s+quarter\b', r'\bqoq\b',
    ]
    for _pat in _DIMENSION_PATTERNS:
        if re.search(_pat, q, re.IGNORECASE):
            return None, []

    # 1. Projects (wave city, wmcc, wave estate) — 2+ distinct projects
    proj_kw = ["wave city", "wmcc sec 32", "wmcc", "wave estate"]
    proj_found = []
    for p in proj_kw:
        if p in q:
            proj_found.append(p)
    # deduplicate: "wmcc sec 32" supersedes plain "wmcc"
    if "wmcc sec 32" in proj_found and "wmcc" in proj_found:
        proj_found = [p for p in proj_found if p != "wmcc"]
    if len(proj_found) >= 2:
        return "projects", proj_found

    # 2. Banks — 2+ distinct banks mentioned
    bank_keys, bank_conds = [], []
    remaining = q
    for kw, canon_key in _BANK_KW_LIST:
        if kw in remaining and canon_key not in bank_keys:
            bank_keys.append(canon_key)
            bank_conds.append(BANK_CATALOG[canon_key]["condition"])
            remaining = remaining.replace(kw, " ")
    if len(bank_keys) >= 2:
        return "banks", list(zip(bank_keys, bank_conds))

    # 3. Quarters — 2+ distinct quarters (q1 and q2, etc.)
    quarters = list(dict.fromkeys(re.findall(r'\bq([1-4])\b', q)))
    if len(quarters) >= 2:
        return "quarters", quarters

    # 4. Time periods — 2+ distinct relative time periods
    time_periods = []
    if re.search(r'\blast\s+(?:fiscal\s+)?year\b', q):    time_periods.append("last_year")
    if re.search(r'\b(?:this|current)\s+(?:fiscal\s+)?year\b', q): time_periods.append("this_year")
    if re.search(r'\blast\s+month\b', q):                  time_periods.append("last_month")
    if re.search(r'\b(?:this|current)\s+month\b', q):      time_periods.append("this_month")
    if re.search(r'\blast\s+quarter\b', q):                time_periods.append("last_quarter")
    if re.search(r'\b(?:this|current)\s+quarter\b', q):    time_periods.append("this_quarter")
    if len(time_periods) >= 2:
        return "time_periods", time_periods

    # 5. Sales groups — 2+ known sales_grp_descp values
    sales_grp_descp_list = sorted(set([
        "amore", "comm booth", "dream bazaar", "dream homes", "eden", "edenia",
        "eligo", "executive floors", "fsi", "harmony greens", "hssc",
        "institutional", "irenia", "lig_001_(310)", "lig_p2", "livork",
        "mayfair park", "metro mart", "new plots", "old plots", "plots-comm",
        "plots-res", "plots-res-if", "prime floors", "sco", "swamanorath",
        "trucia", "vasilia", "veridia", "veridia-3", "veridia-4", "veridia-5",
        "veridia-6", "veridia-7", "villas", "wave business square",
        "wave city- rental", "wave estate, gh2 ph2", "wave floor",
        "wave floor 85", "wave floor 99", "wave galleria", "wave garden",
        "wbt 1", "wbt a", "ews_001_(410)", "ews_p2"
    ]))
    matched_grps = []
    for grp in sorted(sales_grp_descp_list, key=len, reverse=True):
        if grp in q and not any(grp in mg for mg in matched_grps):
            matched_grps.append(grp)
    if len(matched_grps) >= 2:
        return "sales_groups", matched_grps

    return None, []
def build_and_comparison_query(user_query: str) -> str | None:
    """
    Builds SQL for 'X and Y' comparison queries.
    Each entity gets its own row in results using UNION ALL or GROUP BY.
    Works for: projects, banks, quarters, time periods, sales groups.
    """
    q = user_query.lower()
    entity_type, entities = detect_and_entities(user_query)
    if not entity_type:
        return None

    today = date.today()
    fy_s, fy_e = get_financial_year_range(today)

    amount_core = '''SUM(
        CAST(
          CASE
            WHEN TRIM("Gross_amount") LIKE '%%-'
                 OR TRIM("Gross_amount") LIKE '-%%'
                 OR REGEXP_LIKE(TRIM("Gross_amount"), '\\(')
              THEN -CAST(
                     REGEXP_REPLACE(
                       REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$|[()]', ''),
                       '[^0-9.]', ''
                     ) AS DOUBLE
                   )
            ELSE CAST(REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') AS DOUBLE)
          END AS DOUBLE
        )
      )'''
    amount_expr = amount_core + ' AS refund'

    def get_project_codes_filter():
        """Returns AND "Comp_code" IN (...) if projects mentioned, else empty string."""
        codes = []
        if "wave city" in q:     codes.append(1000)
        if "wmcc sec 32" in q or "wmcc" in q: codes.append(1100)
        if "wave estate" in q:   codes.append(1300)
        codes = sorted(set(codes))
        if not codes:
            return ""
        return ' AND "Comp_code" IN (' + ",".join(str(c) for c in codes) + ")"

    # Apply enforce_financial_year to get the correct date clause
    # We pass a dummy SQL with WHERE 1=1 and let enforce_financial_year inject the date
    def get_date_clause_for(query_str: str) -> str:
        """Extract the date WHERE clause by running enforce_financial_year on a dummy SQL."""
        dummy = 'SELECT 1 FROM t WHERE 1=1'
        result = enforce_financial_year(dummy, query_str)
        m = re.search(
            r'DATE_PARSE\(TRIM\(CAST\("POST_date" AS VARCHAR\)\),\s*\'%Y%m%d\'\)\s+(?:BETWEEN|=|>=|<=|>|<|IN).+?(?=(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$))',
            result, re.IGNORECASE
        )
        if m:
            return m.group(0).strip()
        return f"DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{fy_s}' AND DATE '{fy_e}'"

    date_clause = get_date_clause_for(user_query)

    # ── PROJECTS ────────────────────────────────────────────────────────────
    if entity_type == "projects":
        code_map = {"wave city": 1000, "wmcc sec 32": 1100, "wmcc": 1100, "wave estate": 1300}
        codes = sorted(set(code_map[p] for p in entities))
        codes_str = ",".join(str(c) for c in codes)

        sql = (
            f'SELECT CASE '
            f'WHEN "Comp_code" = 1000 THEN \'wave city\' '
            f'WHEN "Comp_code" = 1100 THEN \'wmcc\' '
            f'WHEN "Comp_code" = 1300 THEN \'wave estate\' '
            f'ELSE CAST("Comp_code" AS VARCHAR) END AS project_name, '
            f'{amount_expr} '
            f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" '
            f'WHERE "Comp_code" IN ({codes_str}) AND {date_clause} '
            f'GROUP BY "Comp_code" ORDER BY refund DESC'
        )
        return sql

    # ── BANKS ────────────────────────────────────────────────────────────────
    elif entity_type == "banks":
        proj_filter = get_project_codes_filter()
        selects = []
        for bank_key, bank_cond in entities:
            selects.append(
                f"SELECT '{bank_key}' AS bank_label, {amount_expr} "
                f"FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" "
                f"WHERE {bank_cond} AND {date_clause}{proj_filter}"
            )
        return " UNION ALL ".join(selects) + " ORDER BY refund DESC"

    # ── QUARTERS ─────────────────────────────────────────────────────────────
    elif entity_type == "quarters":
        fy_year_m = re.search(r'\b(20\d{2})\b', q)
        fy_year = int(fy_year_m.group(1)) if fy_year_m else fy_s.year
        proj_filter = get_project_codes_filter()

        def q_dates(n: int):
            n = int(n)
            if n == 1: return date(fy_year, 4, 1),   date(fy_year, 6, 30)
            if n == 2: return date(fy_year, 7, 1),   date(fy_year, 9, 30)
            if n == 3: return date(fy_year, 10, 1),  date(fy_year, 12, 31)
            return date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)

        selects = []
        for qn in entities:
            s, e = q_dates(qn)
            selects.append(
                f"SELECT 'Q{qn}' AS period, {amount_expr} "
                f"FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" "
                f"WHERE DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{s}' AND DATE '{e}'{proj_filter}"
            )
        return " UNION ALL ".join(selects) + " ORDER BY period"

    # ── TIME PERIODS ──────────────────────────────────────────────────────────
    elif entity_type == "time_periods":
        proj_filter = get_project_codes_filter()

        def get_date_range(period: str):
            if period == "this_year":    return fy_s, fy_e
            if period == "last_year":    return date(fy_s.year - 1, 4, 1), date(fy_s.year, 3, 31)
            if period == "this_month":
                ms = date(today.year, today.month, 1)
                me = (date(today.year, today.month + 1, 1) - timedelta(days=1)) if today.month != 12 else date(today.year, 12, 31)
                return ms, me
            if period == "last_month":
                if today.month == 1: return date(today.year - 1, 12, 1), date(today.year - 1, 12, 31)
                return date(today.year, today.month - 1, 1), date(today.year, today.month, 1) - timedelta(days=1)
            if period == "this_quarter":  return quarter_start_end(today)
            if period == "last_quarter":  return last_quarter_start_end(today)

        label_map = {
            "this_year": "this year",     "last_year": "last year",
            "this_month": "this month",   "last_month": "last month",
            "this_quarter": "this quarter", "last_quarter": "last quarter",
        }

        selects = []
        for period in entities:
            s, e = get_date_range(period)
            selects.append(
                f"SELECT '{label_map[period]}' AS period, {amount_expr} "
                f"FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" "
                f"WHERE DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') "
                f"BETWEEN DATE '{s}' AND DATE '{e}'{proj_filter}"
            )
        return " UNION ALL ".join(selects) + " ORDER BY period"

    # ── SALES GROUPS ──────────────────────────────────────────────────────────
    elif entity_type == "sales_groups":
        in_clause = ", ".join(f"'{g}'" for g in entities)
        proj_filter = get_project_codes_filter()
        sql = (
            f'SELECT "sales_grp", "sales_grp_descp", {amount_expr} '
            f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" '
            f'WHERE LOWER("sales_grp_descp") IN ({in_clause}) AND {date_clause}{proj_filter} '
            f'GROUP BY "sales_grp", "sales_grp_descp" ORDER BY refund DESC'
        )
        return sql

    return None

def build_manual_refund_sql(user_query: str, _skip_vs: bool = False) -> str | None:
    user_query = re.sub(r'\s+', ' ', user_query.replace('\xa0', ' ')).strip()
    q = user_query.lower()
    # FIX 3: Handle VS queries
    # Handle "separately" queries — always show each entity independently, never aggregate
    if re.search(r'\bseparately\b', user_query, re.IGNORECASE):
        sep_sql = build_separately_query(user_query)
        if sep_sql:
            return sep_sql

    # FIX 3: Handle VS queries
    # Handle "separately" queries — each entity shown independently, no total
    if re.search(r'\bseparately\b', user_query, re.IGNORECASE):
        sep_sql = build_separately_query(user_query)
        if sep_sql:
            return sep_sql

    # Handle "X and Y" comparison queries
    if not _skip_vs:
        and_sql = build_and_comparison_query(user_query)
        if and_sql:
            return and_sql

    # Handle "X vs Y" comparison queries
    if not _skip_vs and re.search(r'\bvs\.?\b', user_query, re.IGNORECASE):
        vs_sql = build_vs_query(user_query)
        if vs_sql:
            return vs_sql

    # FIX 1: Detect funding type
    funding_condition = detect_funding_type(q)
    amount_expr_abs = '''ABS(
      SUM(
        CAST(
          CASE
            WHEN TRIM("Gross_amount") LIKE '%-' 
                 OR TRIM("Gross_amount") LIKE '-%' 
                 OR REGEXP_LIKE(TRIM("Gross_amount"), '\\(')
              THEN -CAST(
                     REGEXP_REPLACE(
                       REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$|[()]', ''),
                       '[^0-9.]', ''
                     ) AS DOUBLE
                   )
            ELSE CAST(REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') AS DOUBLE)
          END AS DOUBLE
        )
      )
    ) AS refund'''
    amount_expr_net = '''SUM(
        CAST(
          CASE
            WHEN TRIM("Gross_amount") LIKE '%-' 
                 OR TRIM("Gross_amount") LIKE '-%' 
                 OR REGEXP_LIKE(TRIM("Gross_amount"), '\\(')
              THEN -CAST(
                     REGEXP_REPLACE(
                       REGEXP_REPLACE(TRIM("Gross_amount"), '^-|-$|[()]', ''),
                       '[^0-9.]', ''
                     ) AS DOUBLE
                   )
            ELSE CAST(REGEXP_REPLACE(TRIM("Gross_amount"), '[^0-9.]', '') AS DOUBLE)
          END AS DOUBLE
        )
      ) AS refund'''
    amount_core_abs = amount_expr_abs.rsplit(' AS refund', 1)[0]
    amount_core_net = amount_expr_net.rsplit(' AS refund', 1)[0]

    # Full list of known sales_grp_descp values for query detection
    sales_grp_descp_list = sorted(set([
    "amore",
    "comm booth",
    "dream bazaar",
    "dream homes",
    "eden",
    "edenia",
    "eligo",
    "executive floors",
    "fsi",
    "harmony greens",
    "hssc",
    "institutional",
    "irenia",
    "lig_001_(310)",
    "lig_p2",
    "livork",
    "mayfair park",
    "metro mart",
    "new plots",
    "old plots",
    "plots-comm",
    "plots-res",
    "plots-res-if",
    "prime floors",
    "sco",
    "swamanorath",
    "trucia",
    "vasilia",
    "veridia",
    "veridia-3",
    "veridia-4",
    "veridia-5",
    "veridia-6",
    "veridia-7",
    "villas",
    "wave business square",
    "wave city- rental",
    "wave estate, gh2 ph2",
    "wave floor",
    "wave floor 85",
    "wave floor 99",
    "wave galleria",
    "wave garden",
    "wbt 1",
    "wbt a",
    "ews_001_(410)",
    "ews_p2"
]))


    def detect_sales_grp_descp(query_lower: str):
        """Return matched sales_grp_descp value if the query mentions one, else None."""
        for grp in sorted(sales_grp_descp_list, key=len, reverse=True):  # longest match first
            if grp in query_lower:
                return grp
        return None

    def apply_sales_grp_filter(sql: str, grp_name: str) -> str:
        """Append a sales_grp_descp filter to the SQL."""
        return sql + f' AND LOWER("sales_grp_descp") LIKE \'%{grp_name}%\''

    def extract_bank_names(query_lower: str):
        banks = []
        stop = {'out','show','total','refund','from','for','by','and','wise','bank'}
        for m in re.finditer(r'([a-z0-9&/ ]+?)\s+bank', query_lower):
            raw = re.sub(r'\s+', ' ', m.group(1).strip())
            if not raw:
                continue
            if '&' in raw:
                banks.append(raw)
                continue
            tokens = re.findall(r'[a-z]+', raw)
            tokens = [t for t in tokens if t not in stop]
            if tokens:
                banks.append(tokens[-1])
        if not banks:
            m = re.search(r'\bbanks?\s+(.+)', query_lower)
            if m:
                for t in re.split(r'\s*(?:,|and|&|\+)\s*', m.group(1)):
                    t = re.sub(r'\s+', ' ', t.strip())
                    if not t:
                        continue
                    if '&' in t:
                        banks.append(t)
                    else:
                        tokens = [z for z in re.findall(r'[a-z]+', t) if z not in stop]
                        if tokens:
                            banks.append(tokens[-1])
        return sorted(set(banks))

    def bank_condition(b: str) -> str:
        b = re.sub(r'\s+', ' ', b.strip())
        if '&' in b:
            parts = [p.strip() for p in b.split('&') if p.strip()]
            pattern = r'\\s*&\\s*'.join(parts) + r'\\s*bank'
            return f"REGEXP_LIKE(LOWER(\"Bank_name\"), '{pattern}')"
        core = re.sub(r'\s+bank$', '', b)
        return f"LOWER(\"Bank_name\") LIKE '%{core}%'"

    def apply_bank_filter(sql: str, banks) -> str:
        if not banks:
            return sql
        clause = " OR ".join(bank_condition(b) for b in banks)
        return sql + f' AND ({clause})'

    def apply_project_filter(sql: str) -> str:
        codes = []
        if "wave city" in q:
            codes.append(1000)
        if "wmcc sec 32" in q or "wmcc" in q:
            codes.append(1100)
        if "wave estate" in q:
            codes.append(1300)
        codes = sorted(set(codes))
        if not codes:
            return sql
        if len(codes) == 1:
            return sql + f' AND "Comp_code" = {codes[0]}'
        codes_str = ",".join(str(c) for c in codes)
        return sql + f' AND "Comp_code" IN ({codes_str})'

    is_bank_query = (
        "bank wise" in q or "bank-wise" in q or "by bank" in q
        or any(kw in q for key in BANK_CATALOG for kw in BANK_CATALOG[key]["keywords"])
        or re.search(r'\bbank\b', q)
    )

    if is_bank_query:
        exact_clause = detect_exact_bank_clause(q)
        if exact_clause:
            bank_where = exact_clause
        else:
            bank_conditions = detect_banks_in_query(q)
            bank_where = build_bank_where_clause(bank_conditions)

        # Detect explicit multiple quarters like "q1 and q3" with optional year
        mq_list = re.findall(r'\bq([1-4])\b', q, re.IGNORECASE)
        explicit_year_m = re.search(r'\b(20\d{2})\b', q)
        if len(set(mq_list)) >= 2:
            fy_year = int(explicit_year_m.group(1)) if explicit_year_m else get_financial_year_range()[0].year
            def q_dates(n: int):
                n = int(n)
                if n == 1:
                    return date(fy_year, 4, 1), date(fy_year, 6, 30)
                if n == 2:
                    return date(fy_year, 7, 1), date(fy_year, 9, 30)
                if n == 3:
                    return date(fy_year, 10, 1), date(fy_year, 12, 31)
                return date(fy_year + 1, 1, 1), date(fy_year + 1, 3, 31)
            base_sql = f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
            base_sql = apply_project_filter(base_sql)
            selects = []
            for qn in sorted(set(int(x) for x in mq_list)):
                s, e = q_dates(qn)
                inner = f"SELECT {qn} AS quarter_num, COALESCE({amount_core_net}, 0) AS refund {base_sql}"
                if bank_where:
                    inner += f" AND {bank_where}"
                inner += f" AND DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d') BETWEEN DATE '{s}' AND DATE '{e}'"
                selects.append(inner)
            sql = "SELECT quarter_num, refund FROM (" + " UNION ALL ".join(selects) + ") t ORDER BY quarter_num"
            return sql

        sql = f'''SELECT "Bank_name",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        if bank_where:
            sql += f' AND {bank_where}'
        if funding_condition:
            sql += f' AND {funding_condition}'
        sql = enforce_financial_year(apply_project_filter(sql), user_query)
        sql += ' GROUP BY "Bank_name"'
        return enforce_descending_order(sql)

    if "project wise" in q or "project-wise" in q or "projectwise" in q:
        grp = detect_sales_grp_descp(q)
        if grp:
            sql = f'''SELECT
CASE
  WHEN "Comp_code" = 1000 THEN 'wave city'
  WHEN "Comp_code" = 1100 THEN 'wmcc'
  WHEN "Comp_code" = 1300 THEN 'wave estate'
  ELSE CAST("Comp_code" AS VARCHAR)
END AS project_name,
"sales_grp_descp",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
            sql = apply_sales_grp_filter(sql, grp)
            sql = enforce_financial_year(apply_project_filter(sql), user_query)
            sql += ' GROUP BY "Comp_code", "sales_grp_descp"'
            return enforce_descending_order(sql)
        else:
            sql = f'''SELECT
CASE
  WHEN "Comp_code" = 1000 THEN 'wave city'
  WHEN "Comp_code" = 1100 THEN 'wmcc'
  WHEN "Comp_code" = 1300 THEN 'wave estate'
  ELSE CAST("Comp_code" AS VARCHAR)
END AS project_name,
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
            sql = enforce_financial_year(apply_project_filter(sql), user_query)
            sql += ' GROUP BY "Comp_code"'
            return enforce_descending_order(sql)

    if (
        "product wise" in q
        or "product-wise" in q
        or "productwise" in q
        or "sales group wise" in q
        or "sales-group wise" in q
        or "salesgroup wise" in q
    ):
        sql = f'''SELECT "sales_grp", "sales_grp_descp",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        if funding_condition:
            sql += f' AND {funding_condition}'
        sql = enforce_financial_year(apply_project_filter(sql), user_query)
        sql += ' GROUP BY "sales_grp", "sales_grp_descp"'
        return enforce_descending_order(sql)

    matched_grp = detect_sales_grp_descp(q)
    if matched_grp and re.search(r"\b(year on year|yoy|year-over-year|by year|year wise|yearly)\b", q, re.IGNORECASE):
        year_expr = 'EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
        sql = f'''SELECT {year_expr} AS year_num, "sales_grp", "sales_grp_descp",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        sql = apply_sales_grp_filter(sql, matched_grp)
        sql += f' GROUP BY {year_expr}, "sales_grp", "sales_grp_descp" ORDER BY year_num, refund DESC'
        return sql
    # Quarter-wise refund by sales group
    if matched_grp and re.search(r"\b(quarter-wise|quarter wise|by quarter)\b", q, re.IGNORECASE):
        year_expr = 'EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
        quarter_expr = 'CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN 1 WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN 2 WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN 3 ELSE 4 END'
        sql = f'''SELECT {year_expr} AS year_num, {quarter_expr} AS quarter_num, "sales_grp", "sales_grp_descp",
{amount_core_net} AS refund
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        sql = apply_sales_grp_filter(sql, matched_grp)
        if funding_condition:
            sql += f' AND {funding_condition}'
        sql = enforce_financial_year(apply_project_filter(sql), user_query)
        sql += f' GROUP BY {year_expr}, {quarter_expr}, "sales_grp", "sales_grp_descp" ORDER BY year_num, quarter_num, refund DESC'
        return sql
    if matched_grp and not any(w in q for w in [
        'project wise', 'project-wise', 'projectwise',
        'product wise', 'product-wise', 'productwise',
        'sales group wise', 'sales-group wise', 'salesgroup wise',
        'customer', 'cust ', 'bank',
        # time breakdown keywords should not hit the simple sales group branch
        'month on month', 'mom', 'by month', 'monthly', 'each month', 'per month', 'month wise', 'month-wise',
        'quarter on quarter', 'qoq', 'quarter-wise', 'quarter wise', 'by quarter',
        'year on year', 'year-over-year', 'yoy', 'year wise', 'by year', 'yearly'
    ]):
        sql = f'''SELECT "sales_grp", "sales_grp_descp",
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        sql = apply_sales_grp_filter(sql, matched_grp)
        if funding_condition:
            sql += f' AND {funding_condition}'
        sql = enforce_financial_year(apply_project_filter(sql), user_query)
        sql += ' GROUP BY "sales_grp", "sales_grp_descp"'
        return enforce_descending_order(sql)

    if (
        re.search(r"\b(last)\s+(?:fiscal\s+)?year\b", user_query, re.I)
        and re.search(r"\b(current|this)\s+(?:fiscal\s+)?year\b", user_query, re.I)
    ):
        fy_start, fy_end = get_financial_year_range()
        last_fy_start = date(fy_start.year - 1, 4, 1)
        last_fy_end = date(fy_start.year, 3, 31)
        date_expr = "DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')"
        base_sql = f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
        base_sql = apply_project_filter(base_sql)
        this_sql = f"SELECT {fy_start.year} AS year_num, COALESCE({amount_core_net}, 0) AS refund {base_sql} AND {date_expr} BETWEEN DATE '{fy_start}' AND DATE '{fy_end}'"
        last_sql = f"SELECT {last_fy_start.year} AS year_num, COALESCE({amount_core_net}, 0) AS refund {base_sql} AND {date_expr} BETWEEN DATE '{last_fy_start}' AND DATE '{last_fy_end}'"
        sql = f"SELECT year_num, refund FROM ({this_sql} UNION ALL {last_sql}) t ORDER BY year_num DESC"
        return sql
    
    # This quarter vs last quarter — customer-wise (quarter aggregation)
    if (
        re.search(r"\b(current|this)\s+quarter\b", user_query, re.I)
        and re.search(r"\b(last|previous)\s+quarter\b", user_query, re.I)
        and re.search(r'\b(customer|cust|customer-wise|customerwise|each customer|by customer)\b', user_query, re.I)
    ):
        t_start, t_end = quarter_start_end(date.today())
        l_start, l_end = last_quarter_start_end(date.today())
        final_start = min(t_start, l_start)
        final_end = max(t_end, l_end)
        
        date_expr = "DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')"
        base_sql = f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
        base_sql = apply_project_filter(base_sql)
        
        year_expr = 'EXTRACT(YEAR FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\'))'
        quarter_expr = 'CASE WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 4 AND 6 THEN 1 WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 7 AND 9 THEN 2 WHEN EXTRACT(MONTH FROM DATE_PARSE(TRIM(CAST("POST_date" AS VARCHAR)), \'%Y%m%d\')) BETWEEN 10 AND 12 THEN 3 ELSE 4 END'
        
        sql = f'''SELECT {year_expr} AS year_num, {quarter_expr} AS quarter_num, "Cust_no", "Cust_name",
{amount_expr_net}
{base_sql} AND {date_expr} BETWEEN DATE '{final_start}' AND DATE '{final_end}' '''
        sql += f' GROUP BY {year_expr}, {quarter_expr}, "Cust_no", "Cust_name" ORDER BY year_num, quarter_num, refund DESC'
        return sql
    
    # This quarter — customer-wise (single quarter totals per customer)
    if (
        re.search(r"\b(this|current)\s+quarter\b", user_query, re.I)
        and re.search(r'\b(customer|cust|customer-wise|customerwise|each customer|by customer)\b', user_query, re.I)
    ):
        t_start, t_end = quarter_start_end(date.today())
        date_expr = "DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')"
        base_sql = f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
        base_sql = apply_project_filter(base_sql)
        sql = f'''SELECT "Cust_no", "Cust_name",
{amount_expr_net}
{base_sql} AND {date_expr} BETWEEN DATE '{t_start}' AND DATE '{t_end}' '''
        sql += ' GROUP BY "Cust_no", "Cust_name"'
        return enforce_descending_order(sql)

    if (
        re.search(r"\b(last|previous)\s+quarter\b", user_query, re.I)
        and re.search(r'\b(customer|cust|customer-wise|customerwise|each customer|by customer)\b', user_query, re.I)
    ):
        l_start, l_end = last_quarter_start_end(date.today())
        date_expr = "DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')"
        base_sql = f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
        base_sql = apply_project_filter(base_sql)
        sql = f'''SELECT "Cust_no", "Cust_name",
{amount_expr_net}
{base_sql} AND {date_expr} BETWEEN DATE '{l_start}' AND DATE '{l_end}' '''
        sql += ' GROUP BY "Cust_no", "Cust_name"'
        return enforce_descending_order(sql)
    if (
        re.search(r"\b(current|this)\s+month\b", user_query, re.I)
        and re.search(r"\b(last|previous)\s+month\b", user_query, re.I)
    ):
        today = date.today()
        this_start = date(today.year, today.month, 1)
        this_end = (date(today.year, today.month + 1, 1) - timedelta(days=1)) if today.month != 12 else date(today.year, 12, 31)
        if today.month == 1:
            last_start = date(today.year - 1, 12, 1)
            last_end = date(today.year - 1, 12, 31)
        else:
            last_start = date(today.year, today.month - 1, 1)
            last_end = date(today.year, today.month, 1) - timedelta(days=1)

        date_expr = "DATE_PARSE(TRIM(CAST(\"POST_date\" AS VARCHAR)), '%Y%m%d')"
        base_sql = f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE 1=1'
        base_sql = apply_project_filter(base_sql)
        this_sql = f"SELECT {this_start.year} AS year_num, {this_start.month} AS month_num, COALESCE({amount_core_net}, 0) AS refund {base_sql} AND {date_expr} BETWEEN DATE '{this_start}' AND DATE '{this_end}'"
        last_sql = f"SELECT {last_start.year} AS year_num, {last_start.month} AS month_num, COALESCE({amount_core_net}, 0) AS refund {base_sql} AND {date_expr} BETWEEN DATE '{last_start}' AND DATE '{last_end}'"
        sql = f"SELECT year_num, month_num, refund FROM ({this_sql} UNION ALL {last_sql}) t ORDER BY year_num DESC, month_num DESC"
        return sql

    if "total refund" in q or "overall refund" in q:
        multi_quarters = re.findall(r'\bq[1-4]\b', q)
        if len(multi_quarters) >= 2:
            return None

        dimension_words = [
            "project wise",
            "project-wise",
            "projectwise",
            "each project",
            "by project",
            "project breakdown",
            "project wise refund",
            "show projects",
            # Product / sales group
            "product wise",
            "product-wise",
            "productwise",
            "sales group wise",
            "sales-group wise",
            "salesgroup wise",
            "sales group breakdown",
            "by sales group",
            "sales wise",
            # Customer
            "customer",
            "customer-wise",
            "customerwise",
            "each customer",
            "cust ",
            "cust-wise",
            "cust wise",
            "by customer",
            # Bank
            "bank",
            "bank-wise",
            "bank wise",
            "by bank",
            # Time breakdowns
            "month on month",
            "mom",
            "by month",
            "monthly",
            "each month",
            "per month",
            "month wise",
            "month-wise",
            "quarter on quarter",
            "qoq",
            "quarter-wise",
            "quarter wise",
            "by quarter",
            "year on year",
            "year-over-year",
            "year over year",
            "yoy",
            "year wise",
            "by year",
            "yearly",
        ]
        if any(w in q for w in dimension_words):
            return None
        sql = f'''SELECT
{amount_expr_net}
FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"
WHERE 1=1'''
        if funding_condition:
            sql += f' AND {funding_condition}'
        sql = enforce_financial_year(apply_project_filter(sql), user_query)
        return enforce_descending_order(sql)

    return None

app = FastAPI(title="Refund NL2SQL + Presto API")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    sql: str
    data: list




@app.post("/generate-sql", response_model=QueryResponse)
def generate_sql(req: QueryRequest):
    is_vs_query = bool(re.search(r'\bvs\.?\b', req.question, re.IGNORECASE))
    is_separately_query = bool(re.search(r'\bseparately\b', req.question, re.IGNORECASE))
    # "and" comparisons also should not get a Total row appended
    if not is_vs_query:
        _and_type, _and_entities = detect_and_entities(req.question)
        if _and_type is not None:
            is_vs_query = True  # suppress add_total_row for and-comparison results
    # Also treat comparison-and as vs
    if not is_vs_query and _is_comparison_and_query(req.question):
        is_vs_query = True

    manual_sql = build_manual_refund_sql(req.question)
    if manual_sql:
        sql_query = manual_sql
    else:
        raw_sql = nl_to_sql(req.question)
        sql_query = normalize_sql(raw_sql, req.question)
        sql_query = enforce_financial_year(sql_query, req.question)
        _funding_cond = detect_funding_type(req.question.lower())
        if _funding_cond and _funding_cond not in sql_query:
            sql_query = insert_where_before_groupby(sql_query, _funding_cond)

    try:
        data = run_presto_query(sql_query)
        data = ensure_positive_values(data)
        # Skip add_total_row for VS queries (Total already in SQL) and separately queries (user wants individual rows only)
        if not is_vs_query and not is_separately_query:
            data = add_total_row(data)
    except Exception as e:
        data = [{"error": str(e)}]

    return {"sql": sql_query, "data": data}
