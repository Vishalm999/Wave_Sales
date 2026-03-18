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

CATALOG    = os.getenv("PRESTO_CATALOG")
SCHEMA     = os.getenv("PRESTO_SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME")

username   = os.getenv("PRESTO_USERNAME")
password   = os.getenv("PRESTO_PASSWORD")
hostname   = os.getenv("PRESTO_HOST")
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
    params={"temperature": 0, "max_new_tokens": 3000},
)

# ===========================================================================
# COLUMN REFERENCE
# ===========================================================================
# target_description  → 'Old Collection', 'New Collection', 'Booking Units', 'Booking Value'
# target_amount_inr   → target value (VARCHAR with commas)
# achievement         → actual/achieved amount (VARCHAR with commas)
# target_count        → target sales count (VARCHAR)
# start_date          → VARCHAR, DD/MM/YYYY or DD-MM-YYYY or YYYY-MM-DD
# end_date            → VARCHAR
# sales_org_description, sales_organization, sales_group_description, sales_group
# employee_name, personnel_number
# ===========================================================================

TRGT_TEXT_OLD_COLLECTION = "old collection"
TRGT_TEXT_NEW_COLLECTION = "new collection"
TRGT_TEXT_BOOKING_UNITS  = "booking units"
TRGT_TEXT_BOOKING_VALUE  = "booking value"

# ===========================================================================
# DATE PARSE HELPERS (Presto SQL fragments)
# ===========================================================================
_BEGDA_PARSED = (
    "COALESCE("
    "TRY(DATE_PARSE(TRIM(CAST(\"start_date\" AS VARCHAR)), '%d/%m/%Y')), "
    "TRY(DATE_PARSE(TRIM(CAST(\"start_date\" AS VARCHAR)), '%d-%m-%Y')), "
    "TRY(DATE_PARSE(TRIM(CAST(\"start_date\" AS VARCHAR)), '%Y-%m-%d'))"
    ")"
)

_ENDDA_PARSED = (
    "COALESCE("
    "TRY(DATE_PARSE(TRIM(CAST(\"end_date\" AS VARCHAR)), '%d/%m/%Y')), "
    "TRY(DATE_PARSE(TRIM(CAST(\"end_date\" AS VARCHAR)), '%d-%m-%Y')), "
    "TRY(DATE_PARSE(TRIM(CAST(\"end_date\" AS VARCHAR)), '%Y-%m-%d'))"
    ")"
)

# ===========================================================================
# PYTHON DATE / FINANCIAL-YEAR HELPERS
# ===========================================================================

MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6,
    "jul": 7, "july": 7, "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9, "oct": 10, "october": 10,
    "nov": 11, "november": 11, "dec": 12, "december": 12,
}

MONTH_PATTERN = (
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)


def get_financial_year_range(today: date = None):
    """Return (Apr-1, Mar-31) for the FY that contains *today*."""
    if today is None:
        today = date.today()
    fy_start_year = today.year if today.month >= 4 else today.year - 1
    return date(fy_start_year, 4, 1), date(fy_start_year + 1, 3, 31)


def get_fy_for_year_token(y: int):
    """'2024' → FY 2024-25 → Apr 2024 – Mar 2025."""
    return date(y, 4, 1), date(y + 1, 3, 31)


def get_current_month_range() -> tuple:
    today = date.today()
    first = date(today.year, today.month, 1)
    last  = date(today.year, today.month, monthrange(today.year, today.month)[1])
    return first, last


def _month_end(y: int, m: int) -> date:
    return date(y, m, monthrange(y, m)[1])


def _fy_year_for_month(m: int, fy_start_year: int) -> int:
    """Given a bare month number, return the calendar year it falls in within the FY."""
    return fy_start_year if m >= 4 else fy_start_year + 1


def insert_where_before_groupby(sql: str, clause: str) -> str:
    sql_upper = sql.upper()
    for keyword in ("GROUP BY", "ORDER BY", "LIMIT"):
        if keyword in sql_upper:
            parts  = re.split(rf"(?i)\b{keyword}\b", sql, maxsplit=1)
            before = parts[0]
            after  = parts[1]
            joiner = " AND " if "WHERE" in before.upper() else " WHERE "
            return before + joiner + clause + f" {keyword} " + after
    return (sql + " AND " + clause) if "WHERE" in sql_upper else (sql + " WHERE " + clause)


def convert_word_numbers_to_digits(text: str) -> str:
    word_to_num = {
        "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
        "eleven": "11", "twelve": "12", "thirteen": "13", "fourteen": "14",
        "fifteen": "15", "sixteen": "16", "seventeen": "17", "eighteen": "18",
        "nineteen": "19", "twenty": "20",
    }
    pattern = (
        r"\b(last|past|previous)\s+("
        + "|".join(word_to_num.keys())
        + r")\s+(days?|months?|quarters?|years?)\b"
    )
    def _replace(m):
        return f"{m.group(1)} {word_to_num[m.group(2)]} {m.group(3)}"
    return re.sub(pattern, _replace, text, flags=re.IGNORECASE)


# ===========================================================================
# INTENT DETECTION
# ===========================================================================

def is_target_vs_actual_query(query_lower: str) -> bool:
    has_comparison = bool(re.search(r"\b(v\s*/\s*s|vs\.?|versus|compared\s+to)\b", query_lower))
    has_target     = bool(re.search(r"\btarget\b", query_lower))
    # Match 'actual', 'achieved', 'achived', 'acheived' and other common variants/typos
    has_actual     = bool(re.search(
        r"\bactual\b|\bach(?:ie?|ei?)ve[d]?\b|\bachiev(?:ement)?\b", query_lower
    ))
    # If query has v/s + target, treat as target-vs-actual even if "actual/achieved"
    # is omitted (e.g. "show target v/s sales project wise")
    has_implicit_actual = bool(re.search(r"\b(sales|collection|booking)\b", query_lower))
    return has_comparison and has_target and (has_actual or has_implicit_actual)


def detect_metric(query_lower: str) -> dict:
    metrics = {
        "target_collection": False,
        "actual_collection": False,
        "target_sales":      False,
        "actual_sales":      False,
        "collection_types":  [],
        "sales_count":       False,
        "sales_value":       False,
    }

    is_collection = bool(re.search(r"\bcollection\b", query_lower))
    is_sales      = bool(re.search(r"\bsales?\b|\bbooking\b", query_lower))
    is_target     = bool(re.search(r"\btarget\b", query_lower))
    is_actual     = bool(re.search(r"\bactual\b|\bachieve[d]?\b", query_lower))

    if not is_collection and not is_sales:
        is_collection = True
        is_sales      = True
    if not is_target and not is_actual:
        is_target = True
        is_actual = True

    if is_collection:
        if is_target: metrics["target_collection"] = True
        if is_actual: metrics["actual_collection"] = True
    if is_sales:
        if is_target: metrics["target_sales"] = True
        if is_actual: metrics["actual_sales"] = True

    # Explicit count keywords (standalone)
    explicit_count = bool(re.search(r"\bcount\b|\bunits?\b|\bnumber\b|\bhow many\b|\bno\.?\s*of\b", query_lower))
    # Explicit value keywords
    explicit_value = bool(re.search(r"\bvalue\b|\bamount\b|\binr\b|\bworth\b|\brevenue\b", query_lower))
    # "sales and value" / "sales & value" → user wants both count AND value
    sales_and_value = bool(re.search(
        r"\bsales\s*(?:and|&|,)\s*(?:value|amount|inr|worth|revenue)\b"
        r"|\b(?:value|amount|inr|worth|revenue)\s*(?:and|&|,)\s*sales\b"
        r"|\bbooking\s*(?:and|&|,)\s*(?:value|amount)\b"
        r"|\b(?:value|amount)\s*(?:and|&|,)\s*booking\b",
        query_lower
    ))

    # Rules:
    #   "sales and value" / "sales & value"          → both (count + value)
    #   explicit count keyword AND value keyword      → both
    #   "sales value" / "sales amount" / value only  → value only
    #   "sales" / "booking" / count keyword only     → count only
    #   neither                                      → count only (default)
    if sales_and_value or (explicit_count and explicit_value):
        metrics["sales_count"] = True
        metrics["sales_value"] = True
    elif explicit_value:
        metrics["sales_count"] = False
        metrics["sales_value"] = True
    elif explicit_count:
        metrics["sales_count"] = True
        metrics["sales_value"] = False
    else:
        # plain "sales" or "booking" with no qualifier → count only
        metrics["sales_count"] = True
        metrics["sales_value"] = False
        metrics["sales_value"] = False

    want_old = bool(re.search(r"\bold\s+collection\b|\bcollection\s+old\b", query_lower))
    want_new = bool(re.search(r"\bnew\s+collection\b|\bcollection\s+new\b", query_lower))

    if want_old and want_new:
        metrics["collection_types"] = ["old", "new"]
    elif want_old:
        metrics["collection_types"] = ["old"]
    elif want_new:
        metrics["collection_types"] = ["new"]
    else:
        metrics["collection_types"] = ["old", "new"]

    return metrics


# ---------------------------------------------------------------------------
# Temporal grouping helper
# ---------------------------------------------------------------------------

def detect_temporal_grouping(query_lower: str) -> str | None:
    """Returns 'monthly', 'quarterly', 'yearly', or None."""
    if re.search(
        r"\bmonth[\s\-]?wise\b|\bmonth[\s\-]?on[\s\-]?month\b|\bmom\b|\bmonthly\b|\bmonth[\s\-]?over[\s\-]?month\b|\bmonth[\s\-]?by[\s\-]?month\b|\bmonthwise\b",
        query_lower,
    ):
        return "monthly"
    if re.search(
        r"\bquarter[\s\-]?wise\b|\bquarter[\s\-]?on[\s\-]?quarter\b|\bqoq\b|\bquarterly\b|\bquarter[\s\-]?over[\s\-]?quarter\b|\bquarter[\s\-]?by[\s\-]?quarter\b|\bquarterwise\b",
        query_lower,
    ):
        return "quarterly"
    if re.search(
        r"\byear[\s\-]?wise\b|\byear[\s\-]?on[\s\-]?year\b|\byoy\b|\byearly\b|\bannual(?:ly)?\b|\byear[\s\-]?over[\s\-]?year\b|\byear[\s\-]?by[\s\-]?year\b|\byearwise\b",
        query_lower,
    ):
        return "yearly"
    return None


def detect_groupby_dimension(query_lower: str) -> dict:
    """
    Detect GROUP BY dimension.

    Keys: project_wise, sales_grp_wise, employee_wise,
          month_wise, quarter_wise (NEW), year_wise (NEW)
    """
    dim = {
        "project_wise":   False,
        "sales_grp_wise": False,
        "employee_wise":  False,
        "month_wise":     False,
        "quarter_wise":   False,  # NEW
        "year_wise":      False,  # NEW
    }
    if re.search(r"\bproject\s*wise\b|\bsales\s*org\s*wise\b|\bcompany\s*wise\b|\bby\s*project\b|\bby\s*sales\s*org\b", query_lower):
        dim["project_wise"] = True
    if re.search(r"\bsales\s*group\s*wise\b|\bproduct\s*wise\b|\bs_grp\b|\bby\s*sales\s*group\b|\bby\s*product\b", query_lower):
        dim["sales_grp_wise"] = True
    if re.search(r"\bemployee\s*wise\b|\bby\s+employee\b|\bsalesperson\b|\bby\s*employee\b", query_lower):
        dim["employee_wise"] = True

    temporal = detect_temporal_grouping(query_lower)
    if temporal == "monthly":
        dim["month_wise"] = True
    elif temporal == "quarterly":
        dim["quarter_wise"] = True
    elif temporal == "yearly":
        dim["year_wise"] = True

    return dim


# ===========================================================================
# DETECT DATE RANGE  (comprehensive — all 8 scenarios fixed)
# ===========================================================================

def detect_date_range(user_query: str, default_current_month: bool = False) -> tuple:
    """
    Returns (start_date, end_date).

    Priority (first match wins):
     P1.  Month range: "april to june" / "april to june 2024" / "april 2024 to july 2025"
     P2.  last N months           (EXCLUDES current month)
     P3.  last N years            (EXCLUDES current FY)
     P4.  last N quarters
     P5.  this/last quarter       (FY-aware quarters: Q1=Apr-Jun … Q4=Jan-Mar)
     P6.  this/last year          (FY-aware)
     P7.  this/last month
     P8.  monthly / month-on-month grouping → full current FY
     P9.  quarterly grouping      → full current FY
     P10. yearly grouping         → Apr 2000 – end of current FY (show all years)
     P11. month + year  "april 2024" → that calendar month
     P12. bare year "2024"        → FY 2024-25  (Apr 2024 – Mar 2025)
     P13. bare quarter Q1…Q4      → that quarter in current FY
     P14. bare month name         → that month in current FY
     P15. default_current_month   → current calendar month
     P16. fallback                → current FY
    """
    query_lower = user_query.lower()
    query_lower = convert_word_numbers_to_digits(query_lower)  # "two" → "2", "three" → "3" etc.
    today        = date.today()
    fy_start, fy_end = get_financial_year_range(today)
    fy_sy = fy_start.year   # FY start-year integer

    _mp = MONTH_PATTERN

    # ── P0a. Year range: "from 2022 to 2024" / "2022 to 2024" ───────────────
    # Matches two bare years with a range connector — FY interpretation
    yr_rng = re.search(
        r"(?:from\s+)?\b(20\d{2})\b\s*(?:to|through|till|-)\s*\b(20\d{2})\b",
        query_lower,
    )
    if yr_rng:
        y1, y2 = int(yr_rng.group(1)), int(yr_rng.group(2))
        # "from 2022 to 2024" → Apr 2022 to Mar 2025 (FY 2022-23 through FY 2024-25)
        return date(y1, 4, 1), date(y2 + 1, 3, 31)

    # ── P0b. "from MONTH [YEAR]" → that month start to FY end ───────────────
    from_m = re.search(r"\bfrom\s+" + _mp + r"(?:\s+(20\d{2}))?(?!\s*(?:to|through|till|-))", query_lower)
    if from_m:
        m_str = from_m.group(1)
        m = MONTH_MAP[m_str[:3]]
        if from_m.group(2):
            y = int(from_m.group(2))
        else:
            y = _fy_year_for_month(m, fy_sy)
        # Determine FY end that contains/follows this month
        fy_end_of_start = date(y + 1, 3, 31) if m >= 4 else date(y, 3, 31)
        return date(y, m, 1), fy_end_of_start

    # ── P0c. "till/until/upto MONTH [YEAR]" → FY start to that month end ────
    till_m = re.search(r"\b(?:till|until|upto|up\s+to)\s+" + _mp + r"(?:\s+(20\d{2}))?", query_lower)
    if till_m:
        m_str = till_m.group(1)
        m = MONTH_MAP[m_str[:3]]
        if till_m.group(2):
            y = int(till_m.group(2))
        else:
            y = _fy_year_for_month(m, fy_sy)
        # FY start that contains this month
        fy_start_y = y if m >= 4 else y - 1
        return date(fy_start_y, 4, 1), _month_end(y, m)

    # ── P0d. "from MONTH till date/today" → month start to today ────────────
    from_till = re.search(
        r"\bfrom\s+" + _mp + r"(?:\s+(20\d{2}))?\s+(?:till|until|to)\s+(?:date|today)\b",
        query_lower,
    )
    if from_till:
        m_str = from_till.group(1)
        m = MONTH_MAP[m_str[:3]]
        y = int(from_till.group(2)) if from_till.group(2) else _fy_year_for_month(m, fy_sy)
        return date(y, m, 1), today

    # ── P0e. "today" / "for today" ──────────────────────────────────────────
    if re.search(r"\bfor\s+today\b|\btoday(?:'s)?\b", query_lower) and \
       not re.search(r"\btill\s+today\b|\bto\s+today\b|\buntil\s+today\b", query_lower):
        return today, today

    # ── P0f. "yesterday" ────────────────────────────────────────────────────
    if re.search(r"\byesterday\b", query_lower):
        yesterday = today - __import__('datetime').timedelta(days=1)
        return yesterday, yesterday

    # ── P0g. "last N days" ──────────────────────────────────────────────────
    lnd = re.search(r"\blast\s+(\d+)\s+days?\b", query_lower)
    if lnd:
        n = int(lnd.group(1))
        return today - __import__('datetime').timedelta(days=n - 1), today

    # ── P0h. "last week" ────────────────────────────────────────────────────
    if re.search(r"\blast\s+week\b", query_lower):
        # Last Mon–Sun calendar week
        days_since_mon = today.weekday()          # Mon=0 … Sun=6
        last_sun = today - __import__('datetime').timedelta(days=days_since_mon + 1)
        last_mon = last_sun - __import__('datetime').timedelta(days=6)
        return last_mon, last_sun

    # ── P0i. "this week" ────────────────────────────────────────────────────
    if re.search(r"\bthis\s+week\b", query_lower):
        days_since_mon = today.weekday()
        week_start = today - __import__('datetime').timedelta(days=days_since_mon)
        return week_start, today

    # ── P1. Month range ──────────────────────────────────────────────────────

    # "MONTH YEAR to MONTH YEAR"
    rng1 = re.search(
        _mp + r"[\s\-]*(20\d{2})[\s\-]*(?:to|through|till|-)[\s\-]*" + _mp + r"[\s\-]*(20\d{2})",
        query_lower,
    )
    if rng1:
        m1, y1 = MONTH_MAP[rng1.group(1)[:3]], int(rng1.group(2))
        m2, y2 = MONTH_MAP[rng1.group(3)[:3]], int(rng1.group(4))
        return date(y1, m1, 1), _month_end(y2, m2)

    # "MONTH to MONTH YEAR"  (e.g. "april to june 2024")
    rng2 = re.search(
        _mp + r"[\s\-]*(?:to|through|till|-)[\s\-]*" + _mp + r"[\s\-]*(20\d{2})",
        query_lower,
    )
    if rng2:
        m1 = MONTH_MAP[rng2.group(1)[:3]]
        m2 = MONTH_MAP[rng2.group(2)[:3]]
        y2 = int(rng2.group(3))
        y1 = y2 if m1 <= m2 else y2 - 1
        return date(y1, m1, 1), _month_end(y2, m2)

    # "MONTH to MONTH" (within current FY)
    rng3 = re.search(
        _mp + r"[\s\-]*(?:to|through|till|-)[\s\-]*" + _mp,
        query_lower,
    )
    if rng3:
        m1 = MONTH_MAP[rng3.group(1)[:3]]
        m2 = MONTH_MAP[rng3.group(2)[:3]]
        y1 = _fy_year_for_month(m1, fy_sy)
        y2 = _fy_year_for_month(m2, fy_sy)
        # Ensure start <= end; if not, adjust year
        if date(y1, m1, 1) > date(y2, m2, 1):
            y2 = y1  # e.g. nov to feb → both in same FY window
        return date(y1, m1, 1), _month_end(y2, m2)

    # ── P2. Last N months (exclude current month) ────────────────────────────
    lnm = re.search(r"\blast\s+(\d+)\s+months?\b", query_lower)
    if lnm:
        n = int(lnm.group(1))
        if today.month == 1:
            end_y, end_m = today.year - 1, 12
        else:
            end_y, end_m = today.year, today.month - 1
        end_date = _month_end(end_y, end_m)
        start_m, start_y = end_m - n + 1, end_y
        while start_m <= 0:
            start_m += 12
            start_y -= 1
        return date(start_y, start_m, 1), end_date

    # ── P3. Last N years (exclude current FY) ───────────────────────────────
    lny = re.search(r"\blast\s+(\d+)\s+(?:financial\s+)?years?\b", query_lower)
    if lny:
        n = int(lny.group(1))
        return date(fy_sy - n, 4, 1), date(fy_sy, 3, 31)

    # ── P4. Last N quarters ──────────────────────────────────────────────────
    lnq = re.search(r"\blast\s+(\d+)\s+quarters?\b", query_lower)
    if lnq:
        n = int(lnq.group(1))
        all_q = [
            (date(fy_sy - 1, 4, 1),      date(fy_sy - 1, 6, 30)),
            (date(fy_sy - 1, 7, 1),      date(fy_sy - 1, 9, 30)),
            (date(fy_sy - 1, 10, 1),     date(fy_sy - 1, 12, 31)),
            (date(fy_sy, 1, 1),          date(fy_sy, 3, 31)),
            (date(fy_sy, 4, 1),          date(fy_sy, 6, 30)),
            (date(fy_sy, 7, 1),          date(fy_sy, 9, 30)),
            (date(fy_sy, 10, 1),         date(fy_sy, 12, 31)),
            (date(fy_sy + 1, 1, 1),      date(fy_sy + 1, 3, 31)),
        ]
        # Find current quarter index in all_q
        cur_idx = next((i for i, (qs, qe) in enumerate(all_q) if qs <= today <= qe), 7)
        end_idx = cur_idx - 1          # last complete quarter
        start_idx = end_idx - n + 1
        if start_idx < 0:
            start_idx = 0
        return all_q[start_idx][0], all_q[end_idx][1]

    # ── P5. This quarter / last quarter ─────────────────────────────────────
    fy_quarters = [
        (date(fy_sy, 4, 1),     date(fy_sy, 6, 30)),
        (date(fy_sy, 7, 1),     date(fy_sy, 9, 30)),
        (date(fy_sy, 10, 1),    date(fy_sy, 12, 31)),
        (date(fy_sy + 1, 1, 1), date(fy_sy + 1, 3, 31)),
    ]

    if re.search(r"\bthis\s+quarter\b", query_lower):
        for qs, qe in fy_quarters:
            if qs <= today <= qe:
                return qs, qe
        return fy_start, fy_end

    if re.search(r"\blast\s+quarter\b", query_lower):
        for i, (qs, qe) in enumerate(fy_quarters):
            if qs <= today <= qe:
                if i == 0:
                    # Q1 of this FY → last quarter was Q4 of prev FY
                    return date(fy_sy, 1, 1), date(fy_sy, 3, 31)
                return fy_quarters[i - 1]
        return fy_quarters[-1]

    # ── P6. This year / last year ────────────────────────────────────────────
    if re.search(r"\bthis\s+(?:financial\s+)?year\b", query_lower):
        return fy_start, fy_end

    if re.search(r"\blast\s+(?:financial\s+)?year\b", query_lower):
        return date(fy_sy - 1, 4, 1), date(fy_sy, 3, 31)

    # ── P7. This month / last month ──────────────────────────────────────────
    if re.search(r"\bthis\s+month\b", query_lower):
        return date(today.year, today.month, 1), _month_end(today.year, today.month)

    if re.search(r"\blast\s+month\b", query_lower):
        if today.month == 1:
            lm_y, lm_m = today.year - 1, 12
        else:
            lm_y, lm_m = today.year, today.month - 1
        return date(lm_y, lm_m, 1), _month_end(lm_y, lm_m)

    # ── P8-P10. Temporal grouping without explicit date ──────────────────────
    temporal = detect_temporal_grouping(query_lower)
    if temporal in ("monthly", "quarterly"):
        # If a specific year is also mentioned (e.g. "quarter on quarter for 2020"),
        # scope to that FY instead of always defaulting to current FY
        yr_in_q = re.search(r"\b(20\d{2})\b", query_lower)
        if yr_in_q:
            y = int(yr_in_q.group(1))
            return date(y, 4, 1), date(y + 1, 3, 31)
        # If a specific quarter is mentioned (e.g. "month on month for q3"),
        # scope to that quarter's date range
        if temporal == "monthly":
            _q_m = re.search(r"\b(?:q(?:uarter)?\s*([1-4]))\b", query_lower)
            if _q_m:
                _fy_quarters = [
                    (date(fy_sy, 4, 1),     date(fy_sy, 6, 30)),
                    (date(fy_sy, 7, 1),     date(fy_sy, 9, 30)),
                    (date(fy_sy, 10, 1),    date(fy_sy, 12, 31)),
                    (date(fy_sy + 1, 1, 1), date(fy_sy + 1, 3, 31)),
                ]
                return _fy_quarters[int(_q_m.group(1)) - 1]
        return fy_start, fy_end
    if temporal == "yearly":
        return date(2000, 4, 1), fy_end

    # ── P11. Month + explicit year ───────────────────────────────────────────
    myr = re.search(MONTH_PATTERN + r"[\s\-]*(20\d{2})", query_lower)
    if not myr:
        myr = re.search(r"(20\d{2})[\s\-]*" + MONTH_PATTERN, query_lower)
        if myr:
            y, m_str = int(myr.group(1)), myr.group(2)
            m = MONTH_MAP[m_str[:3]]
            return date(y, m, 1), _month_end(y, m)
    if myr:
        m_str, y = myr.group(1), int(myr.group(2))
        m = MONTH_MAP[m_str[:3]]
        return date(y, m, 1), _month_end(y, m)

    # ── P13. Quarter patterns ──────────────────────────────────────────
    # "quarter 1 2023" / "quarter 2" / "q1 2023" / "q2" etc.
    qt_yr = re.search(
        r"\b(?:q(?:uarter)?\s*([1-4]))\s+(20\d{2})\b|\b(?:quarter\s+([1-4]))\s+(20\d{2})\b",
        query_lower,
    )
    if qt_yr:
        qn = int(qt_yr.group(1) or qt_yr.group(3))
        qy = int(qt_yr.group(2) or qt_yr.group(4))
        fy_q_map = {
            1: (date(qy, 4, 1),     date(qy, 6, 30)),
            2: (date(qy, 7, 1),     date(qy, 9, 30)),
            3: (date(qy, 10, 1),    date(qy, 12, 31)),
            4: (date(qy + 1, 1, 1), date(qy + 1, 3, 31)),
        }
        return fy_q_map[qn]

    # bare "quarter 1" / "q1" (no year — current FY)
    q_m = re.search(r"\b(?:q(?:uarter)?\s*([1-4]))\b", query_lower)
    if q_m:
        return fy_quarters[int(q_m.group(1)) - 1]


    # ── P12. Bare year → FY ──────────────────────────────────────────────────
    yr_m = re.search(r"\b(20\d{2})\b", user_query)
    if yr_m:
        return get_fy_for_year_token(int(yr_m.group(1)))

    # ── P14. Bare month name ─────────────────────────────────────────────────
    mo_m = re.search(MONTH_PATTERN, query_lower)
    if mo_m:
        m = MONTH_MAP[mo_m.group(1)[:3]]
        y = _fy_year_for_month(m, fy_sy)
        return date(y, m, 1), _month_end(y, m)

    # ── P15. Default ─────────────────────────────────────────────────────────
    if default_current_month:
        return get_current_month_range()

    # ── P16. Fallback ────────────────────────────────────────────────────────
    return fy_start, fy_end


# ===========================================================================
# PROJECT MAP
# ===========================================================================
PROJECT_MAP = {
    r"\bwave\s+city\b":      "1000",
    r"\bwmcc\s*sec\s*32\b":  "1100",
    r"\bwmcc\b":              "1100",
    r"\bwave\s+estate\b":    "1300",
}


def detect_project_filter(query_lower: str) -> str | None:
    for pattern, code in PROJECT_MAP.items():
        if re.search(pattern, query_lower):
            return code
    return None


# ===========================================================================
# EMPLOYEE FILTER
# ===========================================================================

def detect_employee_filter(user_query: str, query_lower: str) -> str | None:
    STOP_WORDS = {
        "of", "for", "in", "from", "by", "at", "on", "the", "a", "an",
        "and", "or", "vs", "versus",
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
        "january", "february", "march", "april", "june", "july", "august",
        "september", "october", "november", "december",
        "this", "last", "month", "year", "quarter", "today", "week",
        "target", "actual", "collection", "sales", "booking", "show", "me",
        "total", "wise", "give", "what", "is", "are", "get", "wave", "city", "wmcc", "wave", "estate",
        # quarter tokens — never valid as employee names
        "q", "q1", "q2", "q3", "q4",
    }

    # Pre-strip quarter/period tokens from text before name extraction
    # so "for q1 and q2" doesn't get parsed as an employee name
    _PERIOD_PAT = re.compile(
        r"\bq[1-4]\b"                          # q1 q2 q3 q4
        r"|\b(?:fy\s*)?\d{4}(?:[\-/]\d{2,4})?\b"  # 2024, FY2024-25
        r"|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b",
        re.IGNORECASE,
    )

    def extract_name_after(pattern: str, text: str) -> str | None:
        m = re.search(pattern, text)
        if not m:
            return None
        remainder = text[m.end():].strip()
        # Strip period/quarter tokens from the start of remainder
        remainder = _PERIOD_PAT.sub("", remainder).strip()
        words = remainder.split()
        name_words = []
        for word in words:
            clean = re.sub(r"[^a-z]", "", word)
            if clean in STOP_WORDS or not clean:
                break
            # Single-char words that aren't meaningful names → stop
            if len(clean) == 1:
                break
            name_words.append(clean)
            if len(name_words) == 3:
                break
        # Accept 1+ words (single first name like "ajay" is valid)
        if len(name_words) >= 1:
            return " ".join(name_words)
        return None

    code_match = re.search(r"\b(1\d{7,8})\b", query_lower)
    if code_match:
        return code_match.group(1)

    name = extract_name_after(r"\bfor\s+employee\s+", query_lower)
    if name: return name

    name = extract_name_after(r"\bof\s+", query_lower)
    if name: return name

    name = extract_name_after(r"\bfor\s+", query_lower)
    if name: return name

    possessive = re.search(
        r"([a-z]+(?:\s+[a-z]+){0,2})'s\s+(?:target|actual|collection|sales)",
        query_lower,
    )
    if possessive:
        candidate = possessive.group(1).strip()
        if candidate.split()[0] not in STOP_WORDS:
            return candidate

    return None


# ===========================================================================
# PRODUCT MAP
# ===========================================================================
PRODUCT_MAP = {
    "AMORE":                   ["amore"],
    "LIVORK":                  ["livork"],
    "DREAM HOMES":             ["dream homes", "dream home"],
    "DREAM BAZAAR":            ["dream bazaar"],
    "EXECUTIVE FLOORS":        ["executive floors", "executive floor"],
    "WAVE FLOOR":              ["wave floor"],
    "NEW PLOTS":               ["new plots", "new plot"],
    "OLD PLOTS":               ["old plots", "old plot"],
    "VERIDIA":                 ["veridia"],
    "VERIDIA-3":               ["veridia 3", "veridia-3"],
    "VERIDIA-4":               ["veridia 4", "veridia-4"],
    "VERIDIA-5":               ["veridia 5", "veridia-5"],
    "VERIDIA-6":               ["veridia 6", "veridia-6"],
    "VERIDIA-7":               ["veridia 7", "veridia-7"],
    "EDEN":                    ["eden"],
    "EDENIA":                  ["edenia"],
    "ELEGANTIA":               ["elegantia"],
    "ELIGO":                   ["eligo"],
    "EMINENCE":                ["eminence"],
    "IRENIA":                  ["irenia"],
    "TRUCIA":                  ["trucia"],
    "VASILIA":                 ["vasilia"],
    "MAYFAIR PARK":            ["mayfair", "mayfair park"],
    "HARMONY GREENS":          ["harmony greens", "harmony green"],
    "WAVE GALLERIA":           ["galleria", "wave galleria"],
    "WAVE GARDEN":             ["wave garden"],
    "WAVE ESTATE, GH2 PH2":   ["wave estate gh2", "gh2 ph2"],
    "WAVE BUSSINESS SQUARE":  ["business square", "bussiness square"],
    "WBT 1":                   ["wbt 1", "wbt1"],
    "WBT A":                   ["wbt a", "wbta"],
    "PRIME FLOORS":            ["prime floors", "prime floor"],
    "VILLAS":                  ["villas"],
    "ARMONIA VILLA":           ["armonia villa", "armonia"],
    "COMM BOOTH":              ["comm booth", "commercial booth"],
    "COMMERCIAL PLOTS":        ["commercial plots"],
    "PLOTS-COMM":              ["plots comm"],
    "PLOTS-RES":               ["plots res", "residential plots"],
    "PLOTS-RES-IF":            ["plots res if"],
    "SCO":                     ["sco"],
    "METRO MART":              ["metro mart"],
    "SWAMANORATH":             ["swamanorath"],
    "FSI":                     ["fsi project"],
    "INSTITUTIONAL":           ["institutional project"],
    "EWS_001_(410)":           ["ews 410", "ews_001", "ews 001"],
    "EWS_P2":                  ["ews p2"],
    "LIG_001_(310)":           ["lig 310", "lig_001", "lig 001"],
    "LIG_P2":                  ["lig p2"],
    "HSSC":                    ["hssc"],
    "WAVE FLOOR 85":           ["wave floor 85"],
    "WAVE FLOOR 99":           ["wave floor 99"],
}

_PRODUCT_ALIASES = sorted(
    [(alias, sgd) for sgd, aliases in PRODUCT_MAP.items() for alias in aliases],
    key=lambda x: -len(x[0]),
)


def detect_product_filter(query_lower: str, user_query: str) -> tuple | None:
    for alias, sales_group_desc in _PRODUCT_ALIASES:
        if alias in query_lower:
            base = alias.upper()
            has_variants = any(k != base and k.startswith(base) for k in PRODUCT_MAP)
            use_like = has_variants and sales_group_desc.upper() == base
            return sales_group_desc, use_like
    return None


# ===========================================================================
# QUERY BUILDERS
# ===========================================================================

def _date_filter(start: date, end: date) -> str:
    return (
        f"{_BEGDA_PARSED} >= DATE '{start}' "
        f"AND {_ENDDA_PARSED} <= DATE '{end}'"
    )


def _collection_type_filter(collection_types: list) -> str:
    vals = []
    if "old" in collection_types:
        vals.append(f"'{TRGT_TEXT_OLD_COLLECTION}'")
    if "new" in collection_types:
        vals.append(f"'{TRGT_TEXT_NEW_COLLECTION}'")
    if len(vals) == 1:
        return f'LOWER(TRIM("target_description")) = {vals[0]}'
    return f'LOWER(TRIM("target_description")) IN ({", ".join(vals)})'


def build_target_actual_query(user_query: str) -> str:
    """
    Build the Presto SQL query for Target vs Actual Collection & Sales questions.
    Handles all temporal groupings (monthly, quarterly, yearly) and date ranges.
    """
    query_lower = user_query.lower()
    query_lower = convert_word_numbers_to_digits(query_lower)

    metrics = detect_metric(query_lower)
    dim     = detect_groupby_dimension(query_lower)

    start, end = detect_date_range(user_query, default_current_month=False)

    date_clause    = _date_filter(start, end)
    project_filter = detect_project_filter(query_lower)
    product_result = detect_product_filter(query_lower, user_query)
    emp_filter     = None if product_result else detect_employee_filter(user_query, query_lower)

    extra_conditions = []
    if project_filter:
        extra_conditions.append(f'"sales_organization" = {project_filter}')
    if product_result:
        sales_group_desc, use_like = product_result
        if use_like:
            extra_conditions.append(f'LOWER(TRIM("sales_group_description")) LIKE \'%{sales_group_desc.lower()}%\'')
        else:
            extra_conditions.append(f'TRIM("sales_group_description") = \'{sales_group_desc}\'')
    if emp_filter:
        if emp_filter.isdigit():
            extra_conditions.append(f'CAST("personnel_number" AS VARCHAR) = \'{emp_filter}\'')
        elif len(emp_filter.split()) == 1:
            # Single word (e.g. "ajay") → LIKE match to catch all employees with that name
            extra_conditions.append(f'LOWER(TRIM("employee_name")) LIKE \'%{emp_filter.lower()}%\'')
        else:
            # Full name (e.g. "ajay sejwal") → exact full-name LIKE match
            extra_conditions.append(f'LOWER(TRIM("employee_name")) LIKE \'%{emp_filter.lower()}%\'')

    # ── GROUP BY / SELECT dimensions ─────────────────────────────────────────
    group_cols  = []
    select_dims = []

    if dim["project_wise"]:
        select_dims += ['"sales_org_description"']
        group_cols  += ['"sales_org_description"']

    if dim["sales_grp_wise"]:
        select_dims += ['"sales_group_description"']
        group_cols  += ['"sales_group_description"']

    if dim["employee_wise"] or emp_filter:
        if '"employee_name"' not in select_dims:
            select_dims += ['"employee_name"']
            group_cols  += ['"employee_name"']

    if dim["month_wise"]:
        select_dims += [
            f'EXTRACT(YEAR  FROM {_BEGDA_PARSED}) AS year_num',
            f'EXTRACT(MONTH FROM {_BEGDA_PARSED}) AS month_num',
        ]
        group_cols += [
            f'EXTRACT(YEAR  FROM {_BEGDA_PARSED})',
            f'EXTRACT(MONTH FROM {_BEGDA_PARSED})',
        ]

    if dim["quarter_wise"]:
        # Quarter = FY quarter: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
        # Map calendar quarter to FY quarter:
        #   month 4,5,6   → FY Q1
        #   month 7,8,9   → FY Q2
        #   month 10,11,12 → FY Q3
        #   month 1,2,3   → FY Q4
        fy_q_expr = (
            f"CASE "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (4,5,6)   THEN 'Q1' "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (7,8,9)   THEN 'Q2' "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (10,11,12) THEN 'Q3' "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (1,2,3)   THEN 'Q4' "
            f"END"
        )
        fy_yr_expr = (
            f"CASE "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) >= 4 "
            f"THEN CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) AS VARCHAR) || '-' || CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) + 1 AS VARCHAR) "
            f"ELSE CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) - 1 AS VARCHAR) || '-' || CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) AS VARCHAR) "
            f"END"
        )
        select_dims += [
            f'{fy_yr_expr} AS fy_label',
            f'{fy_q_expr} AS fy_quarter',
        ]
        group_cols += [fy_yr_expr, fy_q_expr]

    # ── Single specific quarter (no quarter_wise grouping): add a quarter_label
    # constant so the user sees "Q1" / "Q2" etc. in the result row.
    # e.g. "show me q1 collection", "q3 2022 target sales"
    if not dim["quarter_wise"] and not dim["month_wise"] and not dim["year_wise"]:
        _sq = re.search(
            r"\b(?:q(?:uarter)?\s*([1-4]))\b",
            query_lower,
        )
        if _sq:
            _qlabel = f"Q{_sq.group(1)}"
            select_dims = [f"'{_qlabel}' AS quarter_label"] + select_dims

    if dim["year_wise"]:
        # FY year label: "2024-25"
        fy_label_expr = (
            f"CASE "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) >= 4 "
            f"THEN CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) AS VARCHAR) || '-' || CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) + 1 AS VARCHAR) "
            f"ELSE CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) - 1 AS VARCHAR) || '-' || CAST(EXTRACT(YEAR FROM {_BEGDA_PARSED}) AS VARCHAR) "
            f"END"
        )
        select_dims += [f'{fy_label_expr} AS fy_label']
        group_cols  += [fy_label_expr]

    # ── Metric SELECT expressions ─────────────────────────────────────────────
    select_exprs = []

    if metrics["target_collection"]:
        coll_filter = _collection_type_filter(metrics["collection_types"])
        select_exprs.append((
            "target_collection_value",
            f'SUM(CASE WHEN {coll_filter} THEN CAST(REPLACE("target_amount_inr", \',\', \'\') AS DOUBLE) ELSE 0 END)',
        ))

    if metrics["actual_collection"]:
        coll_filter = _collection_type_filter(metrics["collection_types"])
        select_exprs.append((
            "actual_collection_value",
            f'SUM(CASE WHEN {coll_filter} THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END)',
        ))

    if metrics["target_sales"]:
        if metrics["sales_count"]:
            select_exprs.append((
                "target_sales_count",
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_UNITS}\' THEN CAST("target_count" AS DOUBLE) ELSE 0 END)',
            ))
        if metrics["sales_value"]:
            select_exprs.append((
                "target_sales_value",
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_VALUE}\' THEN CAST(REPLACE("target_amount_inr", \',\', \'\') AS DOUBLE) ELSE 0 END)',
            ))

    if metrics["actual_sales"]:
        if metrics["sales_count"]:
            select_exprs.append((
                "actual_sales_count",
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_UNITS}\' AND CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) <> 0 THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END)',
            ))
        if metrics["sales_value"]:
            select_exprs.append((
                "actual_sales_value",
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_VALUE}\' THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END)',
            ))

    all_select = select_dims + [f"{expr} AS {alias}" for alias, expr in select_exprs]
    select_str = ",\n    ".join(all_select)

    where_parts = [date_clause] + extra_conditions
    where_str   = "\nAND ".join(where_parts)

    group_by_str = ""
    if group_cols:
        group_by_str = "\nGROUP BY " + ", ".join(group_cols)

    order_by_str = ""
    if dim["year_wise"]:
        order_by_str = "\nORDER BY fy_label"
    elif dim["quarter_wise"]:
        order_by_str = "\nORDER BY fy_label, fy_quarter"
    elif dim["month_wise"]:
        order_by_str = "\nORDER BY year_num, month_num"
    elif group_cols:
        order_by_str = f"\nORDER BY {group_cols[0]}"

    # Exclude rows where every metric is 0 (only meaningful when grouping by a dimension)
    having_str = ""
    if group_cols and select_exprs:
        non_zero_checks = " + ".join(f"ABS(COALESCE({expr}, 0))" for _, expr in select_exprs)
        having_str = f"\nHAVING ({non_zero_checks}) > 0"

    return (
        f'SELECT\n    {select_str}\n'
        f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"\n'
        f'WHERE {where_str}'
        f'{group_by_str}'
        f'{having_str}'
        f'{order_by_str}'
    )




# ===========================================================================
# MONTH-RANGE QUERIES  ("from april to sep", "april to june 2023", etc.)
# Expands a month range into one period dict per month → UNION ALL output
# ===========================================================================

def _months_between(start: date, end: date) -> list:
    """Return list of (year, month) tuples from start to end inclusive."""
    result = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


MONTH_NAMES = {
    1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",
    7:"July",8:"August",9:"September",10:"October",11:"November",12:"December",
}


def detect_month_range_periods(user_query: str) -> list | None:
    """
    Detect "MONTH to MONTH" style range queries and expand into per-month periods.

    Patterns handled:
      * "april to sep"            → months in current FY
      * "april to sep 2023"       → months with explicit end-year
      * "from april to sep"       → same as above
      * "april 2024 to june 2025" → cross-year explicit range

    Returns list of {name, start, end} dicts (one per month), or None.
    Does NOT fire for "from april" (no end month) or "till june" (no start month).
    """
    ql = user_query.lower()
    today = date.today()
    fy_start, _ = get_financial_year_range(today)
    fsy = fy_start.year
    _mp = MONTH_PATTERN

    # Pattern A: "MONTH YEAR to MONTH YEAR"  (e.g. "april 2024 to june 2025")
    pa = re.search(
        _mp + r"[\s\-]*(20\d{2})[\s\-]*(?:to|through|till|-)[\s\-]*"
            + _mp + r"[\s\-]*(20\d{2})",
        ql,
    )
    if pa:
        m1, y1 = MONTH_MAP[pa.group(1)[:3]], int(pa.group(2))
        m2, y2 = MONTH_MAP[pa.group(3)[:3]], int(pa.group(4))
        start, end = date(y1, m1, 1), _month_end(y2, m2)
        months = _months_between(start, end)
        if len(months) < 2:
            return None           # single month — let normal path handle it
        return [
            {"name": f"{MONTH_NAMES[m]} {y}",
             "start": date(y, m, 1), "end": _month_end(y, m)}
            for y, m in months
        ]

    # Pattern B: "MONTH to MONTH YEAR"  (e.g. "april to sep 2023")
    pb = re.search(
        _mp + r"[\s\-]*(?:to|through|till|-)[\s\-]*" + _mp + r"[\s\-]*(20\d{2})",
        ql,
    )
    if pb:
        m1 = MONTH_MAP[pb.group(1)[:3]]
        m2 = MONTH_MAP[pb.group(2)[:3]]
        y2 = int(pb.group(3))
        y1 = y2 if m1 <= m2 else y2 - 1
        start, end = date(y1, m1, 1), _month_end(y2, m2)
        months = _months_between(start, end)
        if len(months) < 2:
            return None
        return [
            {"name": f"{MONTH_NAMES[m]} {y}",
             "start": date(y, m, 1), "end": _month_end(y, m)}
            for y, m in months
        ]

    # Pattern C: "MONTH to MONTH" bare (no year)  (e.g. "april to sep", "from april to sep")
    pc = re.search(
        _mp + r"[\s\-]*(?:to|through|till|-)[\s\-]*" + _mp,
        ql,
    )
    if pc:
        m1 = MONTH_MAP[pc.group(1)[:3]]
        m2 = MONTH_MAP[pc.group(2)[:3]]
        y1 = _fy_year_for_month(m1, fsy)
        y2 = _fy_year_for_month(m2, fsy)
        # If calculated start > end, bump end year by 1 (cross-year within FY)
        if date(y1, m1, 1) > date(y2, m2, 1):
            y2 += 1
        start, end = date(y1, m1, 1), _month_end(y2, m2)
        months = _months_between(start, end)
        if len(months) < 2:
            return None
        return [
            {"name": f"{MONTH_NAMES[m]} {y}",
             "start": date(y, m, 1), "end": _month_end(y, m)}
            for y, m in months
        ]

    # Pattern D: comma/space/and-separated list of 3+ months
    # e.g. "for june july aug and sep" → [June, July, August, September]
    found = [(MONTH_MAP[m.group(1)[:3]], int(m.group(2)) if m.group(2) else None)
             for m in re.finditer(_mp + r"(?:\s+(20\d{2}))?", ql)]
    if len(found) >= 3 and len({mn for mn, _ in found}) >= 3:
        result = []
        for mn, yr in found:
            y = yr if yr else _fy_year_for_month(mn, fsy)
            result.append({
                "name": MONTH_NAMES[mn] + (f" {y}" if yr else ""),
                "start": date(y, mn, 1), "end": _month_end(y, mn),
            })
        return result

    return None

def _fy_quarter_range(qn: int, fy_sy: int) -> tuple:
    """Return (start, end) for FY quarter qn (1-4) in the FY starting fy_sy."""
    q_map = {
        1: (date(fy_sy, 4, 1),     date(fy_sy, 6, 30)),
        2: (date(fy_sy, 7, 1),     date(fy_sy, 9, 30)),
        3: (date(fy_sy, 10, 1),    date(fy_sy, 12, 31)),
        4: (date(fy_sy + 1, 1, 1), date(fy_sy + 1, 3, 31)),
    }
    return q_map[qn]


def detect_and_periods(user_query: str) -> list | None:
    """
    Detect "AND"-joined multi-period queries. Returns a list of period dicts
    [{name, start, end}, ...] or None if not applicable.

    Handles:
      * may and june                      -> 2 months (current FY)
      * may 2023 and june 2024            -> 2 months with explicit years
      * q1 and q2                         -> 2 quarters (current FY)
      * q1 2023 and q2 2022               -> explicit-year quarters
      * quarter 1 and quarter 2           -> word-form quarters
      * quarter 1 2023 and quarter 2 2022 -> word-form with years
      * 2022 and 2023                     -> 2 FYs
    """
    ql = user_query.lower()
    today = date.today()
    fy_start, _ = get_financial_year_range(today)
    fsy = fy_start.year
    _mp = MONTH_PATTERN
    AND_SEP = r"\s+and\s+"

    # ── Two months with optional years ───────────────────────────────────────
    m2 = re.search(
        _mp + r"(?:\s+(20\d{2}))?" + AND_SEP + _mp + r"(?:\s+(20\d{2}))?",
        ql,
    )
    if m2:
        m1_str, y1_raw = m2.group(1), m2.group(2)
        m2_str, y2_raw = m2.group(3), m2.group(4)
        mn1 = MONTH_MAP[m1_str[:3]]
        mn2 = MONTH_MAP[m2_str[:3]]
        y1 = int(y1_raw) if y1_raw else _fy_year_for_month(mn1, fsy)
        y2 = int(y2_raw) if y2_raw else _fy_year_for_month(mn2, fsy)
        return [
            {"name": m1_str.capitalize() + (f" {y1}" if y1_raw else ""),
             "start": date(y1, mn1, 1), "end": _month_end(y1, mn1)},
            {"name": m2_str.capitalize() + (f" {y2}" if y2_raw else ""),
             "start": date(y2, mn2, 1), "end": _month_end(y2, mn2)},
        ]

    # ── Two quarters with optional years ─────────────────────────────────────
    q2 = re.search(
        r"\b(?:q(?:uarter)?\s*([1-4]))(?:\s+(?:in\s+)?(20\d{2}))?" + AND_SEP +
        r"(?:q(?:uarter)?\s*([1-4]))(?:\s+(?:in\s+)?(20\d{2}))?\b",
        ql,
    )
    if q2:
        qn1, qy1_raw = int(q2.group(1)), q2.group(2)
        qn2, qy2_raw = int(q2.group(3)), q2.group(4)
        # If only the second quarter has a year (e.g. "q2 and q3 2023"),
        # apply that shared year to both quarters
        if qy2_raw and not qy1_raw:
            qy1_raw = qy2_raw
        # Also check for trailing "in YYYY" (e.g. "q2 and q3 in 2023")
        if not qy1_raw and not qy2_raw:
            in_yr = re.search(r"\bin\s+(20\d{2})\b", ql)
            if in_yr:
                qy1_raw = qy2_raw = in_yr.group(1)
        qy1 = int(qy1_raw) if qy1_raw else fsy
        qy2 = int(qy2_raw) if qy2_raw else fsy
        s1, e1 = _fy_quarter_range(qn1, qy1)
        s2, e2 = _fy_quarter_range(qn2, qy2)
        return [
            {"name": f"Q{qn1}" + (f" FY{qy1}-{qy1+1}" if qy1_raw else ""), "start": s1, "end": e1},
            {"name": f"Q{qn2}" + (f" FY{qy2}-{qy2+1}" if qy2_raw else ""), "start": s2, "end": e2},
        ]

    # ── Two years ─────────────────────────────────────────────────────────────
    yr2 = re.search(r"\b(20\d{2})\b" + AND_SEP + r"\b(20\d{2})\b", ql)
    if yr2:
        y1, y2 = int(yr2.group(1)), int(yr2.group(2))
        return [
            {"name": f"FY {y1}-{y1+1}", "start": date(y1, 4, 1), "end": date(y1 + 1, 3, 31)},
            {"name": f"FY {y2}-{y2+1}", "start": date(y2, 4, 1), "end": date(y2 + 1, 3, 31)},
        ]

    return None

# ===========================================================================
# COMPARISON QUERY  (v/s, vs, versus)
# ===========================================================================

def detect_comparison_query(user_query: str) -> dict:
    query_lower = user_query.lower()
    result = {"is_comparison": False, "periods": [], "comparison_type": None}

    VS_RE = r"\b(v\s*/\s*s|vs\.?|versus|compared\s+to)\b"

    if not re.search(VS_RE, query_lower):
        return result

    result["is_comparison"] = True
    today = date.today()
    fy_start, fy_end = get_financial_year_range(today)
    fy_sy = fy_start.year

    

    # ── This week vs last week ────────────────────────────────────────────────
    if re.search(r"\bthis\s+week\b", query_lower) and re.search(r"\blast\s+week\b", query_lower):
        import datetime as _dt
        days_since_mon  = today.weekday()
        this_week_start = today - _dt.timedelta(days=days_since_mon)
        last_week_end   = this_week_start - _dt.timedelta(days=1)
        last_week_start = last_week_end - _dt.timedelta(days=6)
        result["comparison_type"] = "week"
        result["periods"] = [
            {"name": "This Week", "start": this_week_start, "end": today},
            {"name": "Last Week", "start": last_week_start, "end": last_week_end},
        ]
        return result

    # ── This year vs last year ────────────────────────────────────────────────
    if re.search(r"\bthis\s+year\b", query_lower) and re.search(r"\blast\s+year\b", query_lower):
        result["comparison_type"] = "year"
        result["periods"] = [
            {"name": "This Year", "start": fy_start, "end": fy_end},
            {"name": "Last Year", "start": date(fy_sy - 1, 4, 1), "end": date(fy_sy, 3, 31)},
        ]
        return result

    # ── This month vs last month ──────────────────────────────────────────────
    if re.search(r"\bthis\s+month\b", query_lower) and re.search(r"\blast\s+month\b", query_lower):
        m_start = date(today.year, today.month, 1)
        m_end   = _month_end(today.year, today.month)
        lm_y, lm_m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
        result["comparison_type"] = "month"
        result["periods"] = [
            {"name": "This Month", "start": m_start,             "end": m_end},
            {"name": "Last Month", "start": date(lm_y, lm_m, 1), "end": _month_end(lm_y, lm_m)},
        ]
        return result

    # ── Two quarters e.g. "q1 vs q2" / "q1 2023 vs q2 2024" ─────────────────
    _QP = r"(?:q(?:uarter)?\s*([1-4]))"
    qvq = re.search(
        _QP + r"(?:\s+(20\d{2}))?" + r"\s*" + VS_RE + r"\s*" + _QP + r"(?:\s+(20\d{2}))?",
        query_lower,
    )
    if qvq:
        qn1, qy1_r = int(qvq.group(1)), qvq.group(2)
        qn2, qy2_r = int(qvq.group(4)), qvq.group(5)
        # If only the second quarter has a year (e.g. "q2 vs q3 2023"),
        # apply that shared year to both quarters
        if qy2_r and not qy1_r:
            qy1_r = qy2_r
        qy1 = int(qy1_r) if qy1_r else fy_sy
        qy2 = int(qy2_r) if qy2_r else fy_sy
        s1, e1 = _fy_quarter_range(qn1, qy1)
        s2, e2 = _fy_quarter_range(qn2, qy2)
        result["comparison_type"] = "quarter"
        result["periods"] = [
            {"name": f"Q{qn1}" + (f" FY{qy1}-{qy1+1}" if qy1_r else ""), "start": s1, "end": e1},
            {"name": f"Q{qn2}" + (f" FY{qy2}-{qy2+1}" if qy2_r else ""), "start": s2, "end": e2},
        ]
        return result

    # ── Two months with optional years e.g. "may vs june" / "may 2023 vs june 2024" ──
    _MP = MONTH_PATTERN
    mvs = re.search(
        _MP + r"(?:\s+(20\d{2}))?" + r"\s*" + VS_RE + r"\s*" + _MP + r"(?:\s+(20\d{2}))?",
        query_lower,
    )
    if mvs:
        m1_str, y1_r = mvs.group(1), mvs.group(2)
        m2_str, y2_r = mvs.group(4), mvs.group(5)
        mn1, mn2 = MONTH_MAP[m1_str[:3]], MONTH_MAP[m2_str[:3]]
        y1 = int(y1_r) if y1_r else _fy_year_for_month(mn1, fy_sy)
        y2 = int(y2_r) if y2_r else _fy_year_for_month(mn2, fy_sy)
        result["comparison_type"] = "month"
        result["periods"] = [
            {"name": m1_str.capitalize() + (f" {y1}" if y1_r else ""),
             "start": date(y1, mn1, 1), "end": _month_end(y1, mn1)},
            {"name": m2_str.capitalize() + (f" {y2}" if y2_r else ""),
             "start": date(y2, mn2, 1), "end": _month_end(y2, mn2)},
        ]
        return result

    # ── Two specific years e.g. "2024 vs 2025" ────────────────────────────────
    year_vs = re.search(r"\b(20\d{2})\b.*" + VS_RE + r".*\b(20\d{2})\b", query_lower)
    if year_vs:
        y1, y2 = int(year_vs.group(1)), int(year_vs.group(3))
        result["comparison_type"] = "year"
        result["periods"] = [
            {"name": f"FY {y1}-{y1+1}", "start": date(y1, 4, 1), "end": date(y1 + 1, 3, 31)},
            {"name": f"FY {y2}-{y2+1}", "start": date(y2, 4, 1), "end": date(y2 + 1, 3, 31)},
        ]
        return result

    # ── Target v/s actual (same period, two metric columns) ───────────────────
    if is_target_vs_actual_query(query_lower):
        start, end = detect_date_range(user_query, default_current_month=False)
        result["comparison_type"] = "target_vs_actual"
        result["periods"] = [
            {"name": "Target", "start": start, "end": end},
            {"name": "Actual", "start": start, "end": end},
        ]
        return result

    return result


def build_comparison_query(user_query: str, comparison_info: dict) -> str:
    query_lower = user_query.lower()
    metrics     = detect_metric(query_lower)
    dim         = detect_groupby_dimension(query_lower)
    emp_filter  = detect_employee_filter(user_query, query_lower)

    group_cols  = []
    select_dims = []

    if dim["project_wise"]:
        select_dims += ['"sales_org_description"']
        group_cols  += ['"sales_org_description"']
    if dim["sales_grp_wise"]:
        select_dims += ['"sales_group_description"']
        group_cols  += ['"sales_group_description"']
    if dim["employee_wise"] or emp_filter:
        if '"employee_name"' not in select_dims:
            select_dims += ['"employee_name"']
            group_cols  += ['"employee_name"']

    # ── Target v/s actual (single period, two metric sets) ──────────────────
    if comparison_info.get("comparison_type") == "target_vs_actual":
        start = comparison_info["periods"][0]["start"]
        end   = comparison_info["periods"][0]["end"]
        date_clause = _date_filter(start, end)
        col_exprs = []

        if metrics["target_collection"] or metrics["actual_collection"]:
            coll_filter = _collection_type_filter(metrics["collection_types"])
            if metrics["target_collection"]:
                col_exprs.append(
                    f'SUM(CASE WHEN {coll_filter} THEN CAST(REPLACE("target_amount_inr", \',\', \'\') AS DOUBLE) ELSE 0 END) AS target_collection_value'
                )
            if metrics["actual_collection"]:
                col_exprs.append(
                    f'SUM(CASE WHEN {coll_filter} THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END) AS actual_collection_value'
                )

        if metrics["target_sales"]:
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_UNITS}\' THEN CAST("target_count" AS DOUBLE) ELSE 0 END) AS target_sales_count'
            )
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_VALUE}\' THEN CAST(REPLACE("target_amount_inr", \',\', \'\') AS DOUBLE) ELSE 0 END) AS target_sales_value'
            )

        if metrics["actual_sales"]:
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_UNITS}\' AND CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) <> 0 THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END) AS actual_sales_count'
            )
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_VALUE}\' THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END) AS actual_sales_value'
            )

        all_select = select_dims + col_exprs
        select_str = ",\n    ".join(all_select)
        group_by   = ("\nGROUP BY " + ", ".join(group_cols)) if group_cols else ""

        # Exclude all-zero rows when grouping by a dimension
        having_zero = ""
        if group_cols and col_exprs:
            # Extract just the SUM expressions (before AS alias)
            sum_exprs = [e.rsplit(" AS ", 1)[0] for e in col_exprs]
            non_zero  = " + ".join(f"ABS(COALESCE({e}, 0))" for e in sum_exprs)
            having_zero = f"\nHAVING ({non_zero}) > 0"

        return (
            f'SELECT\n    {select_str}\n'
            f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"\n'
            f'WHERE {date_clause}'
            f'{group_by}'
            f'{having_zero}'
        )

    # ── Standard period comparison (UNION ALL) ───────────────────────────────
    # When quarter_wise is requested (e.g. "quarter on quarter 2022 vs 2023"),
    # build FY-quarter expressions to GROUP BY inside each UNION ALL leg.
    _qoq_group_cols  = list(group_cols)   # per-leg group cols (may add fy_quarter)
    _qoq_select_dims = list(select_dims)  # per-leg select dims (may add fy_quarter / month)
    if dim["quarter_wise"]:
        _fy_q_expr = (
            f"CASE "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (4,5,6)    THEN 'Q1' "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (7,8,9)    THEN 'Q2' "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (10,11,12) THEN 'Q3' "
            f"WHEN EXTRACT(MONTH FROM {_BEGDA_PARSED}) IN (1,2,3)    THEN 'Q4' "
            f"END"
        )
        _qoq_select_dims = _qoq_select_dims + [f"{_fy_q_expr} AS fy_quarter"]
        _qoq_group_cols  = _qoq_group_cols  + [_fy_q_expr]
    elif dim["month_wise"]:
        # month on month 2021 and 2022 → each leg groups by year_num, month_num
        _qoq_select_dims = _qoq_select_dims + [
            f"EXTRACT(YEAR  FROM {_BEGDA_PARSED}) AS year_num",
            f"EXTRACT(MONTH FROM {_BEGDA_PARSED}) AS month_num",
        ]
        _qoq_group_cols = _qoq_group_cols + [
            f"EXTRACT(YEAR  FROM {_BEGDA_PARSED})",
            f"EXTRACT(MONTH FROM {_BEGDA_PARSED})",
        ]

    def period_query(period: dict) -> str:
        start = period["start"]
        end   = period["end"]
        name  = period["name"]
        date_clause = _date_filter(start, end)

        select_exprs_parts = [f"'{name}' AS period"] + _qoq_select_dims
        col_exprs = []

        if metrics["target_collection"]:
            coll_filter = _collection_type_filter(metrics["collection_types"])
            col_exprs.append(
                f'SUM(CASE WHEN {coll_filter} THEN CAST(REPLACE("target_amount_inr", \',\', \'\') AS DOUBLE) ELSE 0 END) AS target_collection_value'
            )
        if metrics["actual_collection"]:
            coll_filter = _collection_type_filter(metrics["collection_types"])
            col_exprs.append(
                f'SUM(CASE WHEN {coll_filter} THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END) AS actual_collection_value'
            )
        if metrics["target_sales"]:
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_UNITS}\' THEN CAST("target_count" AS DOUBLE) ELSE 0 END) AS target_sales_count'
            )
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_VALUE}\' THEN CAST(REPLACE("target_amount_inr", \',\', \'\') AS DOUBLE) ELSE 0 END) AS target_sales_value'
            )
        if metrics["actual_sales"]:
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_UNITS}\' AND CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) <> 0 THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END) AS actual_sales_count'
            )
            col_exprs.append(
                f'SUM(CASE WHEN LOWER(TRIM("target_description")) = \'{TRGT_TEXT_BOOKING_VALUE}\' THEN CAST(REPLACE("achievement", \',\', \'\') AS DOUBLE) ELSE 0 END) AS actual_sales_value'
            )

        all_select = select_exprs_parts + col_exprs
        select_str = ",\n    ".join(all_select)
        # Presto cannot reference a SELECT alias in GROUP BY.
        # 'period' is a literal constant per UNION leg — no need to group by it.
        # Use _qoq_group_cols which may include fy_quarter when quarter_wise=True.
        group_by   = ("\nGROUP BY " + ", ".join(_qoq_group_cols)) if _qoq_group_cols else ""

        # Exclude all-zero rows when grouping by a dimension
        having_zero = ""
        if _qoq_group_cols and col_exprs:
            sum_exprs = [e.rsplit(" AS ", 1)[0] for e in col_exprs]
            non_zero  = " + ".join(f"ABS(COALESCE({e}, 0))" for e in sum_exprs)
            having_zero = f"\nHAVING ({non_zero}) > 0"

        return (
            f'SELECT\n    {select_str}\n'
            f'FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"\n'
            f'WHERE {date_clause}'
            f'{group_by}'
            f'{having_zero}'
        )

    legs = [period_query(p) for p in comparison_info["periods"]]
    # Order by period (year label) then temporal sub-dimension
    if dim["quarter_wise"]:
        order_by = "\nORDER BY period, fy_quarter"
    elif dim["month_wise"]:
        order_by = "\nORDER BY period, year_num, month_num"
    else:
        order_by = "\nORDER BY period"
    return "\nUNION ALL\n".join(legs) + order_by


# ===========================================================================
# LLM FALLBACK
# ===========================================================================

def _is_filter_aggregate_query(query_lower: str) -> bool:
    """
    Returns True if the query requires HAVING / threshold / MAX / MIN / TOP / RANK
    that cannot be expressed by build_target_actual_query (which only does plain GROUP BY).
    Examples: 'greater than 50 lakh', 'maximum sales', 'highest collection employee',
              'minimum booking value', 'top 5 employees'.
    """
    # Strip all temporal "X over X" / "X on X" phrases so "over"/"on" in them
    # don't falsely match the threshold keywords (over, above, below, under).
    cleaned = re.sub(
        r"\b(?:year|month|quarter)[\s\-]?over[\s\-]?(?:year|month|quarter)\b"
        r"|\b(?:year|month|quarter)[\s\-]?on[\s\-]?(?:year|month|quarter)\b"
        r"|\byoy\b|\bmom\b|\bqoq\b",
        "", query_lower
    )
    return bool(re.search(
        r"\b(greater\s+than|less(?:er)?\s+than|more\s+than|above|below|over|under)\b"
        r"|\b(max(?:imum)?|min(?:imum)?|highest|lowest|top\s+\d|bottom\s+\d|rank)\b",
        cleaned,
    ))

def nl_to_sql(user_query: str) -> str:
    fy_start, fy_end = get_financial_year_range()

    prompt = f"""
You are an expert Presto SQL generator for Watsonx.data.

Convert the user's natural language question into a valid Presto SQL query
for the Target vs Actual Collection & Sales dataset.

REQUIREMENTS:
- Always reference the table as "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}".
- Allowed columns (use exact casing):
    "target_description"  -> filter category values: 'Old Collection', 'New Collection',
                      'Booking Units', 'Booking Values'  (always use LOWER(TRIM(...)) for comparison)
    "target_amount_inr"   -> target_amount_inr (use for target collection value and
                      target sales value when target_description = 'booking value')
    "achievement"    -> actual/achieved amount (exclude achievement  = 0 for counts)
    "target_count"     -> target sales unit count base (use when target_description = 'booking units')
    "start_date"        -> beginning date (VARCHAR, may be stored as DD-MM-YYYY or YYYY-MM-DD)
    "end_date"        -> end_date (VARCHAR)
    "sales_org_description"      -> project name / sales_org_description
    "sales_organization"        -> project / sales org / company code
    "sales_group_description"      -> product / sales_group_description
    "sales_group"        -> product / sales_group code
    "employee_name"        -> employee name. Filter with LOWER(TRIM("employee_name")) LIKE '%name%'. Single word (e.g. "ajay") → LIKE '%ajay%' to match all employees with that word in their name. Full name (e.g. "ajay sejwal") → LIKE '%ajay sejwal%' for exact full-name match.
    "personnel_number"       -> employee code
    

METRIC LOGIC (collection COUNT columns are NOT used — only VALUE columns):
1. TARGET COLLECTION VALUE:
   Filter LOWER(TRIM("target_description")) IN ('old collection','new collection'),
   SUM(CAST(REPLACE("target_amount_inr", ',', '') AS DOUBLE))

2. ACTUAL COLLECTION VALUE:
   Filter LOWER(TRIM("target_description")) IN ('old collection','new collection'),
   SUM(CAST(REPLACE("achievement", ',', '') AS DOUBLE))

3. TARGET SALES COUNT/BOOKING UNITS:
   Use when user says: "sales", "sales count", "booking", "booking units", "number of sales", "units"
   Filter LOWER(TRIM("target_description")) = 'booking units',
   SUM(CAST("target_count" AS DOUBLE))

4. TARGET SALES VALUE/BOOKING VALUE:
   Use ONLY when user explicitly says: "sales value", "sales amount", "booking value", "sales INR", "revenue"
   Filter LOWER(TRIM("target_description")) = 'booking value',
   SUM(CAST(REPLACE("target_amount_inr", ',', '') AS DOUBLE))

5. ACTUAL SALES COUNT/BOOKING UNITS:
   Use when user says: "sales", "sales count", "booking", "booking units", "number of sales", "units"
   Filter LOWER(TRIM("target_description")) = 'booking units' AND CAST(REPLACE("achievement", ',', '') AS DOUBLE) <> 0,
   SUM(CAST(REPLACE("achievement", ',', '') AS DOUBLE))

6. ACTUAL SALES VALUE/BOOKING VALUE:
   Use ONLY when user explicitly says: "sales value", "sales amount", "booking value", "sales INR", "revenue"
   Filter LOWER(TRIM("target_description")) = 'booking value',
   SUM(CAST(REPLACE("achievement", ',', '') AS DOUBLE))

SALES METRIC SELECTION RULE (CRITICAL):
- User says "sales" or "booking" (no qualifier)      → SELECT count only  (metrics 3 and/or 5)
- User says "sales value" / "sales amount" / "revenue" → SELECT value only  (metrics 4 and/or 6)
- User says "sales count" AND "sales value" explicitly → SELECT both count and value
- NEVER return both count and value unless the user explicitly asked for both.

IMPORTANT: "target_amount_inr" and "achievement" are stored as VARCHAR with comma-formatted
numbers (e.g. '43,218,255.00'). ALWAYS use CAST(REPLACE(col, ',', '') AS DOUBLE) for these.
"target_count" is stored as a plain numeric VARCHAR (no commas). Use CAST("target_count" AS DOUBLE) directly.
NEVER use REPLACE on "target_count" — it will cause a Presto SYNTAX_ERROR.

DATE RULES:
- "start_date" and "end_date" are VARCHAR stored as DD/MM/YYYY (e.g. '01/02/2026').
  They may also appear as DD-MM-YYYY or YYYY-MM-DD in older rows.
- ALWAYS parse using COALESCE + TRY, trying DD/MM/YYYY first:
    COALESCE(
        TRY(DATE_PARSE(TRIM(CAST("start_date" AS VARCHAR)), '%d/%m/%Y')),
        TRY(DATE_PARSE(TRIM(CAST("start_date" AS VARCHAR)), '%d-%m-%Y')),
        TRY(DATE_PARSE(TRIM(CAST("start_date" AS VARCHAR)), '%Y-%m-%d'))
    )
- Use the same COALESCE pattern for "end_date".
- DEFAULT date range when no period is specified: {fy_start} to {fy_end} (current financial year)
- If a specific month is mentioned, use that month's full date range.
- Default financial year (for FY queries): {fy_start} to {fy_end}
- FY starts April 1 and ends March 31
- A bare year like "2024" means FY 2024-25 (Apr 2024 – Mar 2025)

FILTER / AGGREGATE RULES:
- "greater than X" / "more than X" / "above X"  → use HAVING SUM(...) > X
- "less than X" / "lesser than X" / "below X"   → use HAVING SUM(...) < X
- "maximum" / "highest"   → use ORDER BY metric DESC LIMIT 1  (or MAX() with subquery)
- "minimum" / "lowest"    → use ORDER BY metric ASC  LIMIT 1  (or MIN() with subquery)
- "top N"                 → use ORDER BY metric DESC LIMIT N
- "highest sales count"   → ORDER BY actual_sales_count DESC LIMIT 1 (group by employee/project/product as needed)
- Threshold values may be in lakhs (1 lakh = 100000) or crores (1 crore = 10000000); convert accordingly.
  e.g. "greater than 50 lakh" → HAVING SUM(...) > 5000000

ZERO-ROW FILTER RULE:
- When using GROUP BY, always add a HAVING clause to exclude rows where ALL metric columns are 0.
- Example: HAVING (ABS(COALESCE(SUM(...metric1...), 0)) + ABS(COALESCE(SUM(...metric2...), 0))) > 0
- This ensures employees/projects/products with no activity are not shown.
- If there is already a threshold HAVING (e.g. HAVING SUM(...) > 50000000), merge the zero-filter into it:
  HAVING SUM(...) > 50000000  (the threshold itself already excludes zeros)

CRITICAL HAVING RULE:
- HAVING must contain ONLY the aggregate threshold, e.g.: HAVING SUM(...) > 50000000
- NEVER put date filters (start_date, end_date) or any non-aggregate column in HAVING.
- Date filters ALWAYS go in WHERE. The structure must be:
    WHERE "target_description" IN (...)
      AND <start_date COALESCE parse> >= DATE 'YYYY-MM-DD'
      AND <end_date COALESCE parse>   <= DATE 'YYYY-MM-DD'
    HAVING SUM(...) > X
- WRONG (causes Presto SYNTAX_ERROR):
    HAVING SUM(...) > X AND start_date >= ... AND end_date <= ...
- Do NOT wrap HAVING conditions in extra parentheses.

THRESHOLD QUERY RULES ("greater than X", "more than X", "above X", "less than X"):
- If no dimension is mentioned (no "employee wise", "project wise", "product wise", etc.):
  → Default: GROUP BY "employee_name", "sales_org_description", "sales_group_description"
  → SELECT "employee_name", "sales_org_description", "sales_group_description", SUM(...) AS total_target_collection
  → HAVING SUM(...) > X
  → ORDER BY total_target_collection DESC
- If "employee wise" or "employee" is mentioned:
  → GROUP BY "employee_name"
  → SELECT "employee_name", SUM(...) AS total_target_collection
  → HAVING SUM(...) > X
  → ORDER BY total_target_collection DESC
- If "project wise" or "company wise" is mentioned:
  → GROUP BY "sales_org_description"
  → SELECT "sales_org_description", SUM(...) AS total_target_collection
  → HAVING SUM(...) > X
  → ORDER BY total_target_collection DESC
- If "product wise" or "sales group wise" or "by product" is mentioned:
  → GROUP BY "sales_group_description"
  → SELECT "sales_group_description", SUM(...) AS total_target_collection
  → HAVING SUM(...) > X
  → ORDER BY total_target_collection DESC
- NEVER produce a plain SUM with no GROUP BY for threshold queries — that returns
  the grand total (ignoring the threshold) instead of the per-entity breakdown.

GROUPING / DIMENSION RULES:
- project wise / company wise  -> GROUP BY "sales_organization", "sales_org_description"
- sales_group wise / product wise / by product -> GROUP BY "sales_group", "sales_group_description"
- employee wise -> GROUP BY "personnel_number", "employee_name"
- month wise / monthly / month on month -> GROUP BY year_num, month_num (EXTRACT from start_date)
- quarterly / quarter wise -> GROUP BY FY quarter label (Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar)
- yearly / year wise / year on year -> GROUP BY FY label (e.g. '2024-25')

OUTPUT:
Return ONLY a single SQL query with absolutely no explanation, no preamble, no examples,
no "USER QUESTION:" lines, and no additional text. Do not repeat the question.
Output must start with SELECT and contain only one SQL statement.

USER QUESTION: {user_query}
"""
    response = model.generate_text(prompt=prompt)
    return response.strip()


def _is_aggregate(expr: str) -> bool:
    return bool(re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", expr, re.IGNORECASE))


def _fix_groupby(sql: str) -> str:
    """
    Fix two common LLM GROUP BY mistakes in Presto SQL:

    1. GROUP BY <ordinal> where ordinal refers to an aggregate column
       (e.g. 'GROUP BY 1' when col 1 is SUM(...)) → remove that item.
       If all items are invalid ordinals, remove GROUP BY entirely.

    2. GROUP BY <expr> that duplicates a HAVING aggregate with no real dimension
       → keep only non-aggregate GROUP BY columns; if none remain, drop GROUP BY.
    """
    upper = sql.upper()
    if "GROUP BY" not in upper:
        return sql

    # Split into parts: before GROUP BY, the GROUP BY list, and optional HAVING/ORDER/LIMIT
    gb_match = re.search(
        r"\bGROUP\s+BY\s+(.*?)(?=\s*(?:HAVING|ORDER\s+BY|LIMIT|$))",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not gb_match:
        return sql

    gb_expr = gb_match.group(1).strip()
    before_gb = sql[:gb_match.start()].rstrip()
    after_gb  = sql[gb_match.end():].strip()

    # Parse SELECT columns to map ordinal → expression
    sel_match = re.search(r"\bSELECT\b(.*?)\bFROM\b", sql, re.IGNORECASE | re.DOTALL)
    select_cols = []
    if sel_match:
        # Split by comma but respect nested parens
        raw = sel_match.group(1)
        depth, cur = 0, []
        for ch in raw:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                select_cols.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
        if cur:
            select_cols.append("".join(cur).strip())

    # Split GROUP BY items (respect nested parens)
    depth, cur, items = 0, [], []
    for ch in gb_expr:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        items.append("".join(cur).strip())

    valid_items = []
    for item in items:
        item_stripped = item.strip()
        # Check if it's a bare ordinal (e.g. "1", "2")
        if re.fullmatch(r"\d+", item_stripped):
            idx = int(item_stripped) - 1
            if 0 <= idx < len(select_cols) and _is_aggregate(select_cols[idx]):
                continue  # skip — ordinal points to an aggregate
            # if ordinal is out of range, also skip to be safe
            if idx < 0 or idx >= len(select_cols):
                continue
        # Check if the item itself is an aggregate expression
        if _is_aggregate(item_stripped):
            continue  # skip — grouping by aggregate is illegal
        valid_items.append(item_stripped)

    if not valid_items:
        # No valid GROUP BY columns → drop GROUP BY entirely, keep HAVING intact
        return before_gb + (" " + after_gb if after_gb else "")

    new_gb = "GROUP BY " + ", ".join(valid_items)
    return before_gb + " " + new_gb + (" " + after_gb if after_gb else "")


def _fix_having(sql: str) -> str:
    """
    Robustly move any non-aggregate conditions out of HAVING into WHERE.

    Strategy: rebuild the SQL from scratch by:
      1. Extracting everything before HAVING, the HAVING body, and anything after.
      2. Tokenising the HAVING body by walking char-by-char and tracking paren depth,
         splitting on top-level AND regardless of outer wrapping parens.
      3. Any condition that contains start_date, end_date, or is not an aggregate
         expression goes to WHERE; pure aggregates stay in HAVING.
    """
    having_m = re.search(r'\bHAVING\b', sql, re.IGNORECASE)
    if not having_m:
        return sql

    h_start = having_m.start()
    before   = sql[:h_start].rstrip()

    rest = sql[h_start + len("HAVING"):].lstrip()

    # Split off anything after the HAVING clause (ORDER BY / LIMIT)
    tail_m = re.search(r'\b(ORDER\s+BY|LIMIT)\b', rest, re.IGNORECASE)
    if tail_m:
        body       = rest[:tail_m.start()].strip()
        after_body = " " + rest[tail_m.start():]
    else:
        body       = rest.strip()
        after_body = ""

    # ------------------------------------------------------------------ #
    # Tokenise: walk char by char, split on AND at depth == 0.           #
    # This handles both bare conditions and ones wrapped in parens.      #
    # ------------------------------------------------------------------ #
    def tokenise(expr):
        """Return list of top-level AND-separated conditions, parens stripped."""
        # First, peel any single outer paren that wraps the whole expression
        e = expr.strip()
        while e.startswith("(") and e.endswith(")"):
            depth = 0
            for idx, ch in enumerate(e):
                if ch == "(": depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        if idx == len(e) - 1:
                            e = e[1:-1].strip()
                        break
            else:
                break
            # re-check after stripping
            if not (e.startswith("(") and e.endswith(")")):
                break

        parts, cur, depth = [], [], 0
        i = 0
        while i < len(e):
            ch = e[i]
            if   ch == "(": depth += 1; cur.append(ch)
            elif ch == ")": depth -= 1; cur.append(ch)
            elif depth == 0 and e[i:i+3].upper() == "AND":
                pre_ok  = (i == 0 or not e[i-1].isalnum() and e[i-1] != "_")
                post_ok = (i+3 >= len(e) or not e[i+3].isalnum() and e[i+3] != "_")
                if pre_ok and post_ok:
                    parts.append("".join(cur).strip())
                    cur = []; i += 3; continue
            else:
                cur.append(ch)
            i += 1
        if cur:
            parts.append("".join(cur).strip())
        return [p for p in parts if p]

    conditions = tokenise(body)

    # If we only got 1 condition and it looks mixed, try splitting differently
    # (last resort: split on the known date-column patterns)
    if len(conditions) == 1 and re.search(r'start_date|end_date', conditions[0], re.IGNORECASE):
        # manually split on ") AND " or " AND COALESCE"
        raw = conditions[0]
        parts = re.split(r'(?<=\))\s+AND\s+(?=COALESCE|start_date|end_date)', raw, flags=re.IGNORECASE)
        if len(parts) > 1:
            conditions = [p.strip() for p in parts if p.strip()]

    agg_conds   = []
    where_conds = []
    for cond in conditions:
        bare = cond.strip().lstrip("(")
        if _is_aggregate(bare) and not re.search(r'start_date|end_date', cond, re.IGNORECASE):
            agg_conds.append(cond)
        else:
            where_conds.append(cond)

    new_having = (" HAVING " + " AND ".join(agg_conds)) if agg_conds else ""

    if where_conds:
        extra = " AND ".join(where_conds)
        if re.search(r'\bWHERE\b', before, re.IGNORECASE):
            before = before + " AND " + extra
        else:
            before = before + " WHERE " + extra

    return before + new_having + after_body


def normalize_sql(sql: str) -> str:
    sql = sql.replace("```sql", "").replace("```", "").replace("`", '"')
    sql = sql.replace('\\"', '"').replace("\\'", "'")
    sql = re.sub(r"\s+", " ", sql).strip()

    # Cut off anything after "USER QUESTION:" — the LLM sometimes echoes the prompt
    cut = re.search(r"\bUSER\s+QUESTION\s*:", sql, re.IGNORECASE)
    if cut:
        sql = sql[:cut.start()].strip()

    # If there are multiple SELECT statements (no semicolons between them),
    # keep only the first one by stopping before the second top-level SELECT
    # (a top-level SELECT is one NOT preceded by (, WITH, UNION, INTERSECT, EXCEPT)
    top_selects = [m.start() for m in re.finditer(r"(?<![(\s])(?:^|\s)SELECT\b", sql, re.IGNORECASE)]
    if len(top_selects) > 1:
        sql = sql[:top_selects[1]].strip()

    # Fallback: try to grab SELECT ... ; if semicolon present
    match = re.search(r"(SELECT\b.*?;)", sql, re.IGNORECASE | re.DOTALL)
    if match:
        sql = match.group(1)

    sql = sql.rstrip(";").strip()

    # Fix illegal GROUP BY (ordinals pointing to aggregates, or aggregate expressions)
    sql = _fix_groupby(sql)

    # Fix illegal HAVING (move non-aggregate conditions back to WHERE)
    sql = _fix_having(sql)

    # Fix LLM mistake: CAST(REPLACE("target_count", ...) AS DOUBLE)
    # target_count has no commas — REPLACE causes a Presto SYNTAX_ERROR
    sql = re.sub(
        r'CAST\s*\(\s*REPLACE\s*\(\s*"target_count"\s*,\s*\'[^\']*\'\s*,\s*\'[^\']*\'\s*\)\s*AS\s+DOUBLE\s*\)',
        'CAST("target_count" AS DOUBLE)',
        sql, flags=re.IGNORECASE
    )

    # Fix LLM mistake: GROUP BY year_num, month_num (aliases not allowed in Presto GROUP BY)
    # Replace with the full EXTRACT expressions from the SELECT clause
    _BEGDA = (
        "COALESCE(TRY(DATE_PARSE(TRIM(CAST(\"start_date\" AS VARCHAR)), '%d/%m/%Y')),"
        "TRY(DATE_PARSE(TRIM(CAST(\"start_date\" AS VARCHAR)), '%d-%m-%Y')),"
        "TRY(DATE_PARSE(TRIM(CAST(\"start_date\" AS VARCHAR)), '%Y-%m-%d')))"
    )
    sql = re.sub(
        r'\bGROUP\s+BY\s+year_num\s*,\s*month_num\b',
        f'GROUP BY EXTRACT(YEAR FROM {_BEGDA}), EXTRACT(MONTH FROM {_BEGDA})',
        sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r'\bGROUP\s+BY\s+month_num\s*,\s*year_num\b',
        f'GROUP BY EXTRACT(YEAR FROM {_BEGDA}), EXTRACT(MONTH FROM {_BEGDA})',
        sql, flags=re.IGNORECASE
    )

    return sql


# ===========================================================================
# PRESTO EXECUTION
# ===========================================================================

def run_presto_query(sql: str) -> list:
    conn = prestodb.dbapi.connect(
        host=hostname,
        port=portnumber,
        user=username,
        catalog=CATALOG,
        schema=SCHEMA,
        http_scheme="https",
        auth=prestodb.auth.BasicAuthentication(username, password),
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


_DIMENSION_COLUMNS = {
    "employee_name", "sales_org_description", "sales_group_description",
}


def add_total_row(data: list) -> list:
    if not data:
        return data
    first_row = data[0]
    metric_keys = [
        k for k, v in first_row.items()
        if isinstance(v, float)
        or (isinstance(v, int) and k not in _DIMENSION_COLUMNS
            and k.lower() not in {kk.lower() for kk in _DIMENSION_COLUMNS})
    ]
    if not metric_keys:
        return data
    sums     = {k: sum(row.get(k, 0) or 0 for row in data) for k in metric_keys}
    dim_keys = [k for k in first_row if k not in metric_keys]
    if len(metric_keys) == 1:
        total_row = {"TOTAL": sums[metric_keys[0]]}
    else:
        total_row = {"TOTAL": sums}
    data.append(total_row)
    return data


# ===========================================================================
# MAIN ORCHESTRATOR
# ===========================================================================

def generate_sql_fixed(req) -> dict:
    user_query = req.question

    # ── Month-range queries: "april to sep", "april 2024 to june 2025" ────────
    month_range_periods = detect_month_range_periods(user_query)
    if month_range_periods:
        mr_info = {"is_comparison": True, "periods": month_range_periods, "comparison_type": "month_range"}
        sql_query = build_comparison_query(user_query, mr_info)
        try:
            data = run_presto_query(sql_query)
            data = add_total_row(data)
        except Exception as e:
            data = [{"error": str(e)}]
        return {"sql": sql_query, "data": data}

    # ── "AND" multi-period queries (before v/s check) ────────────────────────
    and_periods = detect_and_periods(user_query)
    if and_periods:
        and_info = {"is_comparison": True, "periods": and_periods, "comparison_type": "and_periods"}
        sql_query = build_comparison_query(user_query, and_info)
        try:
            data = run_presto_query(sql_query)
            data = add_total_row(data)
        except Exception as e:
            data = [{"error": str(e)}]
        return {"sql": sql_query, "data": data}

    comparison_info = detect_comparison_query(user_query)
    if comparison_info["is_comparison"] and comparison_info["periods"]:
        sql_query = build_comparison_query(user_query, comparison_info)
        try:
            data = run_presto_query(sql_query)
            data = add_total_row(data)
        except Exception as e:
            data = [{"error": str(e)}]
        return {"sql": sql_query, "data": data}

    try:
        if not _is_filter_aggregate_query(user_query.lower()):
            sql_query = build_target_actual_query(user_query)
            try:
                data = run_presto_query(sql_query)
                data = add_total_row(data)
            except Exception as e:
                data = [{"error": str(e)}]
            return {"sql": sql_query, "data": data}
    except Exception:
        pass

    raw_sql   = nl_to_sql(user_query)
    sql_query = normalize_sql(raw_sql)

    # ── Guard: ensure current FY date filter is always present ──────────────
    # The LLM sometimes omits the date filter for threshold/aggregate queries
    # (e.g. "greater than 50000000"), causing it to aggregate across ALL years.
    # If the generated SQL has no reference to start_date / end_date at all,
    # inject the current financial-year window so results are scoped correctly.
    if "start_date" not in sql_query and "end_date" not in sql_query:
        fy_start, fy_end = get_financial_year_range()
        fy_date_clause = _date_filter(fy_start, fy_end)
        sql_query = insert_where_before_groupby(sql_query, fy_date_clause)

    try:
        data = run_presto_query(sql_query)
        data = add_total_row(data)
    except Exception as e:
        data = [{"error": str(e)}]
    return {"sql": sql_query, "data": data}


# ===========================================================================
# FASTAPI APP
# ===========================================================================

app = FastAPI(title="Target vs Actual Collection & Sales NL2SQL API")


class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    sql: str
    data: list


@app.post("/generate-sql", response_model=QueryResponse)
def generate_sql(req: QueryRequest):
    return generate_sql_fixed(req)