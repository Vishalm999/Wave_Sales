from fastapi import FastAPI
from pydantic import BaseModel
import prestodb
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import Credentials
import re
import datetime
import calendar
import pytz
from dotenv import load_dotenv
import os

# --------------------
# Configuration
# --------------------
load_dotenv()
CATALOG    = os.getenv("PRESTO_CATALOG")
SCHEMA     = os.getenv("PRESTO_SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME")
username   = os.getenv("PRESTO_USERNAME")
password   = os.getenv("PRESTO_PASSWORD")
hostname   = os.getenv("PRESTO_HOST")
portnumber = int(os.getenv("PRESTO_PORT", "30984"))

creds = Credentials(
    url=os.getenv("WATSONX_URL", "https://us-south.ml.cloud.ibm.com"),
    api_key=os.getenv("WATSONX_API_KEY")
)
model = ModelInference(
    model_id=os.getenv("WATSONX_MODEL_ID", "meta-llama/llama-3-3-70b-instruct"),
    credentials=creds,
    project_id=os.getenv("WATSONX_PROJECT_ID"),
    params={"temperature": 0, "max_new_tokens": 300}
)

# =============================================================
# SALES GROUP MAPPING  (product name -> numeric code)
# Used for WHERE filtering AND CASE display expression
# =============================================================
SALES_GROUP_MAP = {
    'old plots': 1,         'new plots': 2,          'prime floors': 3,
    'wave floor': 4,        'dream homes': 5,         'executive floors': 6,
    'armonia villa': 7,     'fsi': 8,                 'institutional': 9,
    'villas': 10,           'healthcare plot': 11,    'sco': 12,
    'aranyam valley': 13,   'wave galleria': 14,      'ews_001_(410)': 15,
    'lig_001_(310)': 16,    'dream bazaar': 17,       'swamanorath': 18,
    'ews_p2': 19,           'lig_p2': 20,             'mayfair park': 21,
    'commercial plots': 22, 'veridia': 23,            'veridia-3': 24,
    'eligo': 25,            'veridia-4': 26,          'veridia-5': 27,
    'veridia-6': 28,        'veridia-7': 29,          'eden': 30,
    'amore': 101,           'eminence': 102,          'irenia': 103,
    'trucia': 104,          'vasilia': 105,           'edenia': 106,
    'elegantia': 107,       'hssc': 108,              'metro mart': 109,
    'wave business square': 110, 'wave boulevard': 111, 'villa': 112,
    'livork': 113,          'wbt a': 114,             'wbt 1': 115,
    'plots-res': 301,       'plots-comm': 302,        'comm booth': 303,
    'wave garden': 304,     'wave floor 85': 305,     'wave floor 99': 308,
    'group housing 1': 309, 'wave residency': 310,    'plots-res-if': 311,
    'harmony greens': 313,  'wave estate, gh2 ph2': 314,
}
# Static display labels (code -> label) for CASE expression
SALES_GROUP_DISPLAY = {
    1:'Old Plots', 2:'New Plots', 3:'Prime Floors', 4:'Wave Floor', 5:'Dream Homes',
    6:'Executive Floors', 7:'Armonia Villa', 8:'FSI', 9:'Institutional', 10:'Villas',
    11:'Healthcare Plot', 12:'SCO', 13:'Aranyam Valley', 14:'Wave Galleria',
    15:'EWS_001_(410)', 16:'LIG_001_(310)', 17:'Dream Bazaar', 18:'Swamanorath',
    19:'EWS_P2', 20:'LIG_P2', 21:'Mayfair Park', 22:'Commercial Plots', 23:'Veridia',
    24:'Veridia-3', 25:'Eligo', 26:'Veridia-4', 27:'Veridia-5', 28:'Veridia-6',
    29:'Veridia-7', 30:'Eden', 101:'Amore', 102:'Eminence', 103:'Irenia', 104:'Trucia',
    105:'Vasilia', 106:'Edenia', 107:'Elegantia', 108:'HSSC', 109:'Metro Mart',
    110:'Wave Business Square', 111:'Wave Boulevard', 112:'Villa', 113:'Livork',
    114:'WBT A', 115:'WBT 1', 301:'Plots-Res', 302:'Plots-Comm', 303:'Comm Booth',
    304:'Wave Garden', 305:'Wave Floor 85', 306:'Villas', 307:'SCO', 308:'Wave Floor 99',
    309:'Group Housing 1', 310:'Wave Residency', 311:'Plots-Res-IF', 312:'Dream Homes',
    313:'Harmony Greens', 314:'Wave Estate, GH2 PH2', 315:'Institutional',
}


def get_sales_group_case_expr() -> str:
    lines = [f"WHEN \"sales_group\" = {code} THEN '{label}'" for code, label in sorted(SALES_GROUP_DISPLAY.items())]
    return "CASE\n" + "\n".join(lines) + "\nELSE CAST(\"sales_group\" AS VARCHAR)\nEND"


# =============================================================
# SHARED CONSTANTS
# =============================================================
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}
MONTH_ABBR = {
    # Token-based month abbreviations (regex-free).
    'jan': 'january', 'feb': 'february', 'mar': 'march',
    'apr': 'april',   'jun': 'june',     'jul': 'july',
    'aug': 'august',  'sep': 'september','sept': 'september',
    'oct': 'october', 'nov': 'november', 'dec': 'december'
}
WORD_TO_NUM = {
    'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6,
    'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10, 'eleven': 11, 'twelve': 12,
    'fifteen': 15, 'twenty': 20, 'fifty': 50, 'hundred': 100
}

# ALL phrasings for each quarter
QUARTER_PATTERNS = [
    (r'\bq\.?1\b',             1), (r'\bquarter\s*[-\s]?1\b', 1),
    (r'\bfirst\s+quarter\b',   1), (r'\b1st\s+quarter\b',     1),
    (r'\bq\.?2\b',             2), (r'\bquarter\s*[-\s]?2\b', 2),
    (r'\bsecond\s+quarter\b',  2), (r'\b2nd\s+quarter\b',     2),
    (r'\bq\.?3\b',             3), (r'\bquarter\s*[-\s]?3\b', 3),
    (r'\bthird\s+quarter\b',   3), (r'\b3rd\s+quarter\b',     3),
    (r'\bq\.?4\b',             4), (r'\bquarter\s*[-\s]?4\b', 4),
    (r'\bfourth\s+quarter\b',  4), (r'\b4th\s+quarter\b',     4),
]


# =============================================================
# DATE UTILITIES
# =============================================================
def expand_abbreviations(q: str) -> str:
    # Regex-free month abbreviation expansion (token/word-boundary based).
    tokens = []
    for raw in str(q).split():
        t = raw.strip().strip('.,;:!?()[]{}"\'').lower()
        if t in MONTH_ABBR:
            tokens.append(MONTH_ABBR[t])
        else:
            tokens.append(raw)
    return " ".join(tokens)

def _collapse_spaces(s: str) -> str:
    return " ".join(str(s).split())

def _normalize_separators(q: str) -> str:
    # Regex-free: split common inline separators into spaces so token/phrase matching works.
    s = str(q)
    for ch in ("/", "\\", "|"):
        s = s.replace(ch, " ")
    return _collapse_spaces(s)

def _normalize_vs_and(q: str) -> str:
    """
    Regex-free normalization: converts vs/versus/& into 'and' and collapses spaces.
    Keeps the output lowercase because it is used for intent matching.
    """
    out = []
    for raw in str(q).lower().split():
        t = raw.strip().strip('.,;:!?()[]{}"\'')
        if t in {"vs", "versus", "&"}:
            out.append("and")
        else:
            out.append(t)
    return _collapse_spaces(" ".join(out))

def _has_separately(q: str) -> bool:
    # Regex-free detection (avoid brittle patterns).
    tokens = [_strip_token_edges(t).lower() for t in str(q).split()]
    separately_words = {
        "separately", "seprately", "seperately",
        "separate", "individually", "respectively",
    }
    return any(t in separately_words for t in tokens if t)

def _has_explicit_year_token(q: str) -> bool:
    # Regex-free: detect explicit year tokens like 2022/2023.
    for raw in str(q).split():
        t = _strip_token_edges(raw)
        if len(t) == 4 and t.isdigit() and t.startswith("20"):
            return True
    return False

def _has_quarter_token(q: str) -> bool:
    # Regex-free: detect q1/q2/q3/q4 and "quarter".
    for raw in str(q).split():
        t = _strip_token_edges(raw).lower().replace(".", "")
        if t in {"quarter", "q1", "q2", "q3", "q4"}:
            return True
    return False

def _has_month_token(q: str) -> bool:
    # Regex-free: detect month names.
    for raw in str(q).split():
        t = _strip_token_edges(raw).lower()
        if t in MONTH_MAP:
            return True
    return False

def _norm_key(s: str) -> str:
    # Regex-free string normalizer for loose matching (letters+digits only).
    return "".join(ch.lower() for ch in str(s) if ch.isalnum())

def _safe_alias(s: str) -> str:
    # Produce a safe SQL identifier (unquoted) for AS <alias>.
    out = []
    for ch in str(s).lower():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    alias = "".join(out).strip("_")
    if not alias:
        alias = "group_key"
    if alias[0].isdigit():
        alias = "col_" + alias
    return alias

def _has_customer_keyword(q: str) -> bool:
    # Regex-free: only treat names/codes as customer when user explicitly says "customer(s)".
    tokens = [_strip_token_edges(t).lower() for t in str(q).split()]
    return any(t in {"customer", "customers", "cust"} for t in tokens if t)

def _strip_token_edges(tok: str) -> str:
    return str(tok).strip().strip('.,;:!?()[]{}"\'/\\|')

def _parse_number_token(tok: str):
    t = _strip_token_edges(tok).replace(",", "")
    if not t:
        return None
    # Allow simple decimals (no exponent).
    dot_count = t.count(".")
    if dot_count > 1:
        return None
    if dot_count == 1:
        left, right = t.split(".", 1)
        if (left.isdigit() or left == "") and right.isdigit():
            return float(t) if left != "" else float("0" + t)
        return None
    if t.isdigit():
        return int(t)
    # Word numbers (small set).
    if t.lower() in WORD_TO_NUM:
        return WORD_TO_NUM[t.lower()]
    return None

def _amount_multiplier(unit: str) -> int:
    u = _strip_token_edges(unit).lower()
    if u in {"lakh", "lakhs", "lac", "lacs"}:
        return 100_000
    if u in {"crore", "crores"}:
        return 10_000_000
    if u in {"thousand", "k"}:
        return 1_000
    if u in {"million"}:
        return 1_000_000
    if u in {"billion"}:
        return 1_000_000_000
    return 1

def _parse_amount_from_tokens(tokens: list, start_idx: int):
    """
    Parse amounts like:
    - 1000000
    - 10,00,000
    - 10 lakh / 10 lakhs / 10 lacs
    - 1.5 crore

    Returns (value_as_int_or_float, consumed_tokens_count) or (None, 0).
    """
    if start_idx < 0 or start_idx >= len(tokens):
        return None, 0
    n = _parse_number_token(tokens[start_idx])
    if n is None:
        return None, 0
    consumed = 1
    mult = 1
    if start_idx + 1 < len(tokens):
        u = _strip_token_edges(tokens[start_idx + 1]).lower()
        if u and u not in {"rs", "inr", "rupee", "rupees"}:
            mult = _amount_multiplier(u)
            if mult != 1:
                consumed += 1
    val = float(n) * float(mult)
    # Prefer int when it's effectively whole.
    if abs(val - int(val)) < 1e-9:
        return int(val), consumed
    return val, consumed

def parse_num_word(s) -> int:
    if s is None: return 1
    s = str(s).strip().lower()
    return int(s) if s.isdigit() else WORD_TO_NUM.get(s, 1)

def fy_of_date(d: datetime.date) -> int:
    return d.year if d.month >= 4 else d.year - 1

def fy_dates(fy: int):
    return f"{fy}0401", f"{fy + 1}0331"

def month_dates(yr: int, mn: int):
    last = calendar.monthrange(yr, mn)[1]
    return f"{yr}{mn:02d}01", f"{yr}{mn:02d}{last:02d}"

def quarter_dates(fy: int, q: int):
    """Indian FY quarters: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar"""
    if q == 1: return f"{fy}0401",     f"{fy}0630"
    if q == 2: return f"{fy}0701",     f"{fy}0930"
    if q == 3: return f"{fy}1001",     f"{fy}1231"
    if q == 4: return f"{fy + 1}0101", f"{fy + 1}0331"

def current_quarter_info(d: datetime.date):
    m = d.month; fy = fy_of_date(d)
    if   4 <= m <= 6:   return 1, fy
    elif 7 <= m <= 9:   return 2, fy
    elif 10 <= m <= 12: return 3, fy
    else:               return 4, fy

def prev_quarter(q, fy):
    return (4, fy - 1) if q == 1 else (q - 1, fy)

def infer_year_for_month(mn: int, fy_start: int) -> int:
    return fy_start if mn >= 4 else fy_start + 1


# =============================================================
# PRESTO HELPERS
# =============================================================
def run_presto_query(sql: str):
    try:
        with prestodb.dbapi.connect(
            host=hostname, port=portnumber, user=username,
            catalog=CATALOG, schema=SCHEMA, http_scheme="https",
            auth=prestodb.auth.BasicAuthentication(username, password),
        ) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return [{"error": f"Presto execution failed: {str(e)}"}]

def get_columns_from_presto():
    try:
        res = run_presto_query(f'SHOW COLUMNS FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"')
        return [r['Column'] for r in res] if res and not any('error' in r for r in res) else []
    except Exception:
        return []


# =============================================================
# POST-PROCESSING
# =============================================================
def _detect_numeric_alias(sql: str):
    m = re.search(r'(?:SUM|COUNT|ABS|AVG)\s*\([^)]*(?:\([^)]*\)[^)]*)*\)\s+AS\s+(\w+)', sql, re.IGNORECASE)
    if m: return m.group(1)
    sm = re.search(r'SELECT\s+(.+?)\s+FROM\b', sql, re.IGNORECASE | re.DOTALL)
    if sm:
        aliases = re.findall(r'\)\s+AS\s+(\w+)', sm.group(1), re.IGNORECASE)
        if aliases: return aliases[-1]
    return None

def enforce_descending_order(sql: str) -> str:
    TEMPORAL = {'year_num','month_num','quarter_num','year','month','quarter',
                'period','fiscal_year','fiscal_quarter','financial_year','week_label'}
    sql = sql.rstrip(';').strip()

    # Keep LIMIT at the end. `nl_to_sql()` already appends LIMIT, and this function may
    # add/modify ORDER BY. If ORDER BY ends up after LIMIT, Presto will error.
    limit_clause = ""
    lm = re.search(r'\s+LIMIT\s+\d+\s*$', sql, re.IGNORECASE)
    if lm:
        limit_clause = sql[lm.start():].strip()
        sql = sql[:lm.start()].rstrip()

    num_col = _detect_numeric_alias(sql)
    if re.search(r'\bORDER BY\b', sql, re.IGNORECASE):
        def fix(m):
            parts = [p.strip() for p in m.group(1).strip().split(',')]
            out = []
            for part in parts:
                toks = part.split()
                col  = toks[0].lower().strip('"\'')
                if col in TEMPORAL:
                    out.append(part)
                elif num_col and col == num_col.lower():
                    if len(toks) == 1: out.append(part + ' DESC')
                    elif toks[-1].upper() == 'ASC': out.append(' '.join(toks[:-1]) + ' DESC')
                    else: out.append(part)
                else:
                    if num_col: out.append(f'{num_col} DESC')
                    elif len(toks) == 1: out.append(part + ' DESC')
                    elif toks[-1].upper() == 'ASC': out.append(' '.join(toks[:-1]) + ' DESC')
                    else: out.append(part)
            seen = []; [seen.append(p) for p in out if p not in seen]
            return 'ORDER BY ' + ', '.join(seen)
        sql = re.sub(r'ORDER BY\s+(.+?)$', fix, sql, flags=re.IGNORECASE)
    elif num_col:
        sql += f' ORDER BY {num_col} DESC'
    if limit_clause:
        sql = f"{sql} {limit_clause}"
    return sql

def add_total_row(data: list) -> list:
    if not data or len(data) <= 1: return data
    OC = {'demand_total', 'total_collection', 'credit_note_total', 'others', 'refund_amount'}
    total_row = {}; first_str_done = False
    for key in data[0].keys():
        if key == 'outstanding': total_row[key] = None; continue
        vals = [r.get(key) for r in data if r.get(key) is not None]
        try:    total_row[key] = round(sum(float(v) for v in vals), 2)
        except: 
            if not first_str_done: total_row[key] = 'Total'; first_str_done = True
            else: total_row[key] = '-'
    if 'outstanding' in total_row:
        total_row['outstanding'] = round(sum(
            float(total_row[c]) for c in OC if total_row.get(c) not in (None, '')), 2)
    data.append(total_row)
    return data


# =============================================================
# SQL EXPRESSION BUILDERS
# =============================================================
def numeric_sum_expr(col: str) -> str:
    return (f'SUM(CASE WHEN substr(TRIM("{col}"),-1)=\'-\' THEN -1*TRY_CAST(REGEXP_REPLACE(TRIM("{col}"),'
            f'\'[^0-9.]\',\'\') AS DOUBLE) WHEN substr(TRIM("{col}"),1,1)=\'-\' THEN '
            f'-1*TRY_CAST(REGEXP_REPLACE(TRIM("{col}"),\'[^0-9.]\',\'\') AS DOUBLE) '
            f'ELSE TRY_CAST(REGEXP_REPLACE(TRIM("{col}"),\'[^0-9.]\',\'\') AS DOUBLE) END)')

def outstanding_expr() -> str:
    fields = ['demand_total', 'collection', 'credit_note_total', 'others', 'refund_amount']
    return 'ABS(' + '+'.join(numeric_sum_expr(f) for f in fields) + ')'

def get_company_name_expr() -> str:
    return ("CASE WHEN \"company_code\"=1000 THEN 'Wave City' "
            "WHEN \"company_code\"=1300 THEN 'Wave Estate' "
            "WHEN \"company_code\"=1100 THEN 'WMCC Sec 32' "
            "ELSE CAST(\"company_code\" AS VARCHAR) END")

def get_doc_type_expr() -> str:
    return "COALESCE(CAST(\"Doc_Type_FI\" AS VARCHAR),'Unknown')"

def get_bank_name_expr() -> str:
    return "COALESCE(CAST(\"Customer_Bank_and_Mode\" AS VARCHAR),'Unknown')"

# Shared time group expressions
MONTH_GRP   = "date_format(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')),'%M %Y')"
QUARTER_GRP = ("CASE WHEN MONTH(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))>=4 "
               "THEN CONCAT('FY',CAST(YEAR(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d'))) AS VARCHAR),"
               "'-Q',CAST(FLOOR((MONTH(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))-4)/3)+1 AS VARCHAR)) "
               "ELSE CONCAT('FY',CAST(YEAR(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))-1 AS VARCHAR),"
               "'-Q',CAST(FLOOR((MONTH(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))+8)/3)+1 AS VARCHAR)) END")
YEAR_GRP    = ("CASE WHEN MONTH(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))>=4 "
               "THEN CAST(YEAR(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d'))) AS VARCHAR)||'-'||"
               "CAST(YEAR(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))+1 AS VARCHAR) "
               "ELSE CAST(YEAR(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')))-1 AS VARCHAR)||'-'||"
               "CAST(YEAR(TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d'))) AS VARCHAR) END")


# =============================================================
# METRIC EXTRACTION
# =============================================================
def extract_metrics(q: str) -> list:
    ql = q.lower()
    outstanding_kw = ["outstanding", "outstandings", "pending", "due", "balance",
                      "arrear", "receivable", "dues", "pending collection"]
    if any(k in ql for k in outstanding_kw):
        return [("demand_total","demand_total"),("collection","total_collection"),
                ("credit_note_total","credit_note_total"),("others","others"),
                ("refund_amount","refund_amount"),("outstanding","outstanding")]
    metrics = []
    if "demand tax"    in ql: metrics.append(("demand_tax","demand_tax"))
    elif "demand value" in ql: metrics.append(("demand_value","demand_value"))
    elif any(k in ql for k in ["demand total","total demand","demand"]):
        metrics.append(("demand_total","total_demand"))
    # Regex-free "collection" detection with a lightweight token check.
    toks = [_strip_token_edges(t).lower() for t in ql.split()]
    if "collection" in toks or ("total" in toks and "collection" in toks):
        metrics.append(("collection", "total_collection"))
    if "refund" in ql: metrics.append(("refund_amount","refund_amount"))
    if   "credit note total" in ql or "credit_note_total" in ql: metrics.append(("credit_note_total","credit_note_total"))
    elif "credit note tax"   in ql or "credit_note_tax"   in ql: metrics.append(("credit_note_tax","credit_note_tax"))
    elif "credit note"       in ql or "credit_note"       in ql: metrics.append(("credit_note","credit_note"))
    if not metrics: metrics.append(("demand_total","total_demand"))
    return metrics


# =============================================================
# QUARTER EXTRACTION  (FIX #1, #4)
# Returns list of (q_num, fy_year) — one entry per quarter mention
# Handles: "q2 in 2022 and q2 in 2023", "q1 and q3", "second quarter 2024"
# =============================================================
def extract_quarters_with_years(ql: str, default_fy: int) -> list:
    year_positions = [(m.start(), int(m.group(1))) for m in re.finditer(r'\b(20\d{2})\b', ql)]
    quarter_hits = []
    for pat, qnum in QUARTER_PATTERNS:
        for m in re.finditer(pat, ql):
            quarter_hits.append((m.start(), m.end(), qnum))
    quarter_hits.sort(key=lambda x: x[0])

    results = []
    for (qs, qe, qnum) in quarter_hits:
        # Find nearest year within 50 chars
        best_yr, best_dist = None, 9999
        for (ypos, yr) in year_positions:
            dist = min(abs(ypos - qs), abs(ypos - qe))
            if dist < best_dist and dist <= 50:
                best_dist = dist; best_yr = yr
        results.append((qnum, best_yr if best_yr else default_fy))

    # Deduplicate
    seen = set(); out = []
    for item in results:
        if item not in seen: seen.add(item); out.append(item)
    return out


# =============================================================
# WHERE CONDITIONS  (FIX #6: product name mapped to sales_group code)
#                  (FIX #7: customer name filter improved)
# =============================================================
def build_where_conditions(user_query: str, columns: list, ql_norm: str = None):
    conditions = []
    # Use pre-normalized ql (VS->AND etc.) if provided, else use raw lowercase
    ql = ql_norm if ql_norm is not None else user_query.lower()
    comparison_dim = None  # set when AND/VS used between same-type entities
    has_and = ' and ' in (ql_norm or user_query.lower())  # True after VS/& normalization
    mentions_customer = _has_customer_keyword(ql)
    # Treat "separately" like an AND/VS comparison trigger for grouping (without changing filtering logic).
    if not has_and and _has_separately(ql):
        has_and = True

    # ── Company ──────────────────────────────────────────────────────────────
    company_map = {'wave city': 1000, 'wmcc sec 32': 1100, 'wave estate': 1300}
    matched_cos = []
    for name, code in company_map.items():
        if name in ql: matched_cos.append(str(code))
    for code in re.findall(r'\b(1000|1100|1300)\b', ql): matched_cos.append(code)
    ecm = re.search(r'\bcompany\s+code\s+["\']?(\d+)["\']?\b', ql)
    if ecm: matched_cos.append(ecm.group(1))
    matched_cos = list(set(matched_cos))
    if   len(matched_cos) == 1: conditions.append(f'"company_code" = {matched_cos[0]}')
    elif len(matched_cos) > 1:
        conditions.append(f'"company_code" IN ({",".join(matched_cos)})')
        if has_and: comparison_dim = 'company'  # AND/VS between companies → show each separately

    # ── Sales group / product name  (FIX #6) ─────────────────────────────────
    # Sort by name length descending so "armonia villa" beats "villa"
    matched_sg = []
    for name, code in sorted(SALES_GROUP_MAP.items(), key=lambda x: -len(x[0])):
        if re.search(rf'\b{re.escape(name)}\b', ql):
            matched_sg.append(code)
    # Also raw numeric codes
    for raw in re.findall(r'\b(0\d{2}|[1-3]\d{2})\b', user_query):
        matched_sg.append(int(raw))
    matched_sg = list(set(matched_sg))
    if   len(matched_sg) == 1: conditions.append(f'"sales_group" = {matched_sg[0]}')
    elif len(matched_sg) > 1:
        conditions.append(f'"sales_group" IN ({",".join(str(c) for c in matched_sg)})')
        if has_and and not comparison_dim: comparison_dim = 'sales_group'

    # ── Doc type ──────────────────────────────────────────────────────────────
    doc_types = re.findall(r'\b(RV|DZ|DA|DB|DR|Z8|Z9|SK|DG|DC|SA|Z7|Z5|R1|RG)\b', user_query.upper())
    if doc_types:
        fmt = ','.join(f"UPPER('{d}')" for d in set(doc_types))
        conditions.append(f'UPPER("Doc_Type_FI") IN ({fmt})')

    # ── Bank name ─────────────────────────────────────────────────────────────
    bank_kw = {
        'hdfc': 'HDFC', 'icici': 'ICICI', 'axis bank': 'AXIS', 'axis': 'AXIS',
        'kotak': 'KOTAK', 'indusind': 'INDUSIND', 'yes bank': 'YES BANK',
        'bank of baroda': 'BANK OF BARODA', 'pnb': 'PNB', 'idfc': 'IDFC',
        'federal bank': 'FEDERAL', 'rbl': 'RBL', 'bandhan': 'BANDHAN',
        'city union': 'CITY UNION', 'tamilnad': 'TAMILNAD', 'au small': 'AU SMALL',
        'au bank': 'AU SMALL', 'dcb': 'DCB', 'jana': 'JANA', 'uco': 'UCO',
        'canara': 'CANARA', 'bank of india': 'BANK OF INDIA', 'central bank': 'CENTRAL BANK',
        'indian bank': 'INDIAN BANK', 'idbi': 'IDBI', 'state bank': 'STATE BANK',
        'sbi upi': 'SBI UPI', 'sbi dd': 'SBI DD', 'sbi visa': 'SBI VISA',
        'upi': 'UPI', 'cheque': 'CHEQUE', 'demand draft': 'SBI DD', 'union bank': 'UNION BANK',
    }
    explicit_bank = re.search(
        r'(?:bank\s+(?:name\s+)?(?:is|:)\s*|via\s+|paid\s+(?:via|through)\s+|mode\s+(?:is|:)\s*)'
        r'([a-z][a-z\s]{1,40}?)(?:\s+bank\b|\s+(?:and|where|for|with|in|from|having)\b|$)', ql)
    matched_banks = []
    if explicit_bank:
        matched_banks.append(explicit_bank.group(1).strip().upper())
    else:
        sbi_sub = re.findall(r'\b(sbi-\d{4,6})\b', ql)
        matched_banks.extend(c.upper() for c in sbi_sub)
        for kw, val in sorted(bank_kw.items(), key=lambda x: -len(x[0])):
            if kw in ql: matched_banks.append(val)
    matched_banks = list(set(matched_banks))
    if   len(matched_banks) == 1:
        conditions.append(f'UPPER("Customer_Bank_and_Mode") LIKE UPPER(\'%{matched_banks[0]}%\')')
    elif len(matched_banks) > 1:
        bc = ' OR '.join(f'UPPER("Customer_Bank_and_Mode") LIKE UPPER(\'%{b}%\')' for b in matched_banks)
        conditions.append(f'({bc})')

    # ── Customer name  (FIX #7) ───────────────────────────────────────────────
    FORBIDDEN_NAME_WORDS = {
        'total','demand','collection','refund','credit','note','tax','value','amount','net',
        'wave','city','estate','january','february','march','april','may','june','july',
        'august','september','october','november','december','fy','financial','year',
        'quarter','month','last','this','current','today','yesterday','week','all','each',
        'every','wise','sales','group','company','show','provide','give','display','get',
        'fetch','find','and','or','for','from','in','of','the','a','an','is','are','was',
        'separately','seprately','seperately','separate','individually','respectively',
    }
    # Add all product names to forbidden (so "eden" doesn't get treated as customer)
    FORBIDDEN_NAME_WORDS.update(SALES_GROUP_MAP.keys())
    FORBIDDEN_NAME_WORDS.update(company_map.keys())

    customer_name_added = False

    # Pattern 1: explicit "customer name is X" or "customer name: X"
    explicit_cust = re.search(
        r'\bcustomer\s+(?:name\s+)?(?:is\s*[:=]?|[:=])\s*["\']?([a-zA-Z][a-zA-Z\s]{1,60})["\']?'
        r'(?=\s*(?:\band\b|\bwhere\b|\bfor\b|\bwith\b|\bin\b|\bfrom\b|\bof\b|\bhaving\b|\bcompany\b'
        r'|\bsales\b|\bdoc\b|\bbank\b|\btax\b|$))',
        ql
    )
    if explicit_cust:
        name = explicit_cust.group(1).strip()
        # Stop at first forbidden word so query keywords don't bleed in
        name_words = name.split()
        clean_words = []
        for w in name_words:
            if w.lower() in FORBIDDEN_NAME_WORDS:
                break
            clean_words.append(w)
        name = ' '.join(clean_words).strip()
        name_parts = name.split()
        if len(name) >= 3 and not set(w.lower() for w in name_parts).issubset(FORBIDDEN_NAME_WORDS):
            if len(name_parts) >= 2:
                like_clauses = ' AND '.join(
                    f'UPPER("Customer_Name") LIKE UPPER(\'%{w}%\')' for w in name_parts
                )
                conditions.append(f'({like_clauses})')
            else:
                conditions.append(f'UPPER("Customer_Name") LIKE UPPER(\'%{name}%\')')
            customer_name_added = True
            print(f"DEBUG: customer name (explicit): '{name}'")

    if not customer_name_added and mentions_customer:
        # Pattern 2: quoted string anywhere
        quoted = re.search(r'["\']([A-Za-z][A-Za-z\s]{2,50})["\']', user_query)
        if quoted:
            nm = quoted.group(1).strip()
            nm_parts = nm.split()
            if not set(nm.lower().split()).issubset(FORBIDDEN_NAME_WORDS):
                if len(nm_parts) >= 2:
                    like_clauses = ' AND '.join(
                        f'UPPER("Customer_Name") LIKE UPPER(\'%{w}%\')' for w in nm_parts
                    )
                    conditions.append(f'({like_clauses})')
                else:
                    conditions.append(f'UPPER("Customer_Name") LIKE UPPER(\'%{nm}%\')')
                customer_name_added = True

    if not customer_name_added and mentions_customer:
        # Pattern 3: "for customer A and B" / "of customers A, B"
        cust_with_names = re.search(
            r'(?:for|of)\s+customers?\s+(.+?)'
            r'(?=\s*(?:\bwhere\b|\bhaving\b|\bgroup\b|\border\b|\blimit\b|\btop\b|\bby\b|\bcompany\b'
            r'|\bsales\b|\bdoc\b|\bbank\b|\btax\b|\bfy\b|\bfrom\b|\bin\b|\btill\b|\buntil\b|\bup\s*to\b|\bseparately\b|\bseprately\b|\bseperately\b|\bindividually\b|\brespectively\b|$))',
            user_query,
            re.IGNORECASE
        )
        if cust_with_names:
            seg = cust_with_names.group(1).strip().strip('.,;:')
            candidates = [c.strip() for c in re.split(r'\s*(?:,| and | & )\s*', seg, flags=re.IGNORECASE) if c.strip()]
            like_parts = []
            for cand in candidates:
                nm_raw = cand.strip().strip('\'"')
                name_words = []
                for w in nm_raw.split():
                    wl = _strip_token_edges(w).lower()
                    if wl in FORBIDDEN_NAME_WORDS:
                        break
                    if re.fullmatch(r'20\d{2}', wl):
                        break
                    name_words.append(_strip_token_edges(w))
                nm = ' '.join(name_words).strip()
                if len(nm) < 3:
                    continue
                nm_parts = nm.split()
                if set(w.lower() for w in nm_parts).issubset(FORBIDDEN_NAME_WORDS):
                    continue
                if len(nm_parts) >= 2:
                    like_clause = ' AND '.join(
                        f'UPPER("Customer_Name") LIKE UPPER(\'%{w}%\')' for w in nm_parts
                    )
                    like_parts.append(f'({like_clause})')
                else:
                    like_parts.append(f'UPPER("Customer_Name") LIKE UPPER(\'%{nm}%\')')
            if like_parts:
                conditions.append('(' + ' OR '.join(like_parts) + ')')
                customer_name_added = True
                if has_and and len(like_parts) > 1 and not comparison_dim:
                    comparison_dim = 'customer'
                print(f"DEBUG: customer name list ('for/of customer ...'): {candidates}")

    # NOTE: We intentionally do not infer a customer name unless the user explicitly uses
    # the "customer" keyword. This prevents entities like banks (e.g. HDFC) from being
    # misinterpreted as customer names when the query is actually about bank/company/etc.

    # NOTE: We intentionally do NOT add customer name filter just because
    # "customer" keyword is present in the query — that causes grouping/display
    # queries like "show by customer" to add a spurious WHERE filter.
    # Customer name is only filtered when an actual name is provided.

    # ── Customer code (7-10 digit numbers) ────────────────────────────────────
    cust_codes = re.findall(r'\b(\d{7,10})\b', user_query) if mentions_customer else []
    if cust_codes:
        if   len(cust_codes) == 1:
            conditions.append(f'"Customer_Code" = CAST({cust_codes[0]} AS BIGINT)')
        else:
            conditions.append(f'"Customer_Code" IN ({",".join(cust_codes)})')
            if has_and and not comparison_dim:
                comparison_dim = 'customer'

    # ── Tax code ──────────────────────────────────────────────────────────────
    known_tax = ['G3','GC','ZO','Z0','S1','S2','S3','G1','G2','A1','A2',
                 'B1','B2','C1','C2','T1','T2','IG','SG','CG','I1','I2','I3','I4','I5']
    has_tax = any(k in ql for k in ['tax code','tax','gst','tax type'])
    matched_tax = set()
    if has_tax:
        near = re.findall(r'tax\s+(?:code\s+)?([A-Z]{1,2}\d{0,1})\b', user_query.upper())
        matched_tax.update(near)
        for tc in known_tax:
            if re.search(rf'\b{re.escape(tc)}\b', user_query.upper()): matched_tax.add(tc)
    else:
        for tc in known_tax:
            if re.search(rf'\b{re.escape(tc)}\b', user_query.upper()): matched_tax.add(tc)
    if matched_tax:
        conditions.append(f'CAST("Tax_Code" AS VARCHAR) IN ({",".join(repr(t) for t in matched_tax)})')

    return conditions, comparison_dim


# =============================================================
# TIME GROUPING KEYWORD DETECTION
# Returns (grouping_type, group_expr, group_alias, force_current_fy)
# force_current_fy=True means: if no explicit date found, restrict to current FY
# =============================================================
def determine_time_grouping(ql: str):
    if any(k in ql for k in ["year on year","yoy","yearly","year wise","by year","year-wise","annually"]):
        return ('year', YEAR_GRP, 'fiscal_year', False)
    if any(k in ql for k in ["quarter on quarter","qoq","quarterly","quarter wise","by quarter","quarter-wise"]):
        return ('quarter', QUARTER_GRP, 'fiscal_quarter', True)
    if any(k in ql for k in ["month on month","mom","monthly","month wise","by month","month-wise"]):
        return ('month', MONTH_GRP, 'month', True)
    return (None, None, None, False)

def extract_limit(q: str):
    tokens = [_strip_token_edges(t).lower() for t in str(q).split()]
    for i, tok in enumerate(tokens[:-1]):
        if tok in {"top", "first", "best"}:
            n = _parse_number_token(tokens[i + 1])
            if isinstance(n, (int, float)):
                try:
                    return int(n)
                except Exception:
                    return None
    return None

def extract_having(q: str):
    ql = _normalize_vs_and(q)
    tokens = [_strip_token_edges(t).lower() for t in ql.split() if _strip_token_edges(t)]

    def _metric_expr(metric_alias: str) -> str | None:
        # Use full expressions (not SELECT aliases) so Presto can resolve them in HAVING.
        if metric_alias == "total_demand":
            return numeric_sum_expr("demand_total")
        if metric_alias == "total_collection":
            return numeric_sum_expr("collection")
        if metric_alias == "refund_amount":
            return numeric_sum_expr("refund_amount")
        if metric_alias == "credit_note_total":
            return numeric_sum_expr("credit_note_total")
        if metric_alias == "outstanding":
            return outstanding_expr()
        return None

    # Metric phrase -> SQL alias used in SELECT/GROUP/HAVING.
    col_map = {
        ("demand",): "total_demand",
        ("collection",): "total_collection",
        ("refund",): "refund_amount",
        ("outstanding",): "outstanding",
        ("credit", "note"): "credit_note_total",
    }

    # Comparator phrases (token tuples) -> SQL operator.
    op_map = {
        ("greater", "than"): ">",
        ("more", "than"): ">",
        ("higher", "than"): ">",
        ("above",): ">",
        ("exceeds",): ">",
        ("less", "than"): "<",
        ("lower", "than"): "<",
        ("below",): "<",
        ("under",): "<",
        ("equal", "to"): "=",
        ("equals",): "=",
        (">=",): ">=",
        ("<=",): "<=",
        (">",): ">",
        ("<",): "<",
        ("=",): "=",
    }

    fillers = {"is", "are", "was", "were", "value", "amount", ":"}

    def _match_phrase_at(idx: int, phrase: tuple) -> bool:
        if idx < 0 or idx + len(phrase) > len(tokens):
            return False
        return tuple(tokens[idx:idx + len(phrase)]) == phrase

    def _find_metric_at(idx: int):
        for phrase, alias in sorted(col_map.items(), key=lambda kv: -len(kv[0])):
            if _match_phrase_at(idx, phrase):
                return phrase, alias
        return None, None

    # Pattern 1: "<metric> [is] <op> <metric>"
    for i in range(len(tokens)):
        m_phrase, m_alias = _find_metric_at(i)
        if not m_alias:
            continue
        j = i + len(m_phrase)
        while j < len(tokens) and tokens[j] in fillers:
            j += 1
        for op_phrase, op_sym in op_map.items():
            if _match_phrase_at(j, op_phrase):
                k = j + len(op_phrase)
                while k < len(tokens) and tokens[k] in fillers:
                    k += 1
                r_phrase, r_alias = _find_metric_at(k)
                if r_alias and r_alias != m_alias:
                    l_expr = _metric_expr(m_alias)
                    r_expr = _metric_expr(r_alias)
                    if l_expr and r_expr:
                        return f"({l_expr}) {op_sym} ({r_expr})"

    # Pattern 2: "<metric> [is] <op> <amount>" (supports lakh/crore)
    for i in range(len(tokens)):
        m_phrase, m_alias = _find_metric_at(i)
        if not m_alias:
            continue
        j = i + len(m_phrase)
        while j < len(tokens) and tokens[j] in fillers:
            j += 1
        for op_phrase, op_sym in op_map.items():
            if _match_phrase_at(j, op_phrase):
                k = j + len(op_phrase)
                while k < len(tokens) and tokens[k] in fillers:
                    k += 1
                amt, consumed = _parse_amount_from_tokens(tokens, k)
                if amt is not None and consumed > 0:
                    l_expr = _metric_expr(m_alias)
                    if l_expr:
                        return f"({l_expr}) {op_sym} {amt}"
    return None


# =============================================================
# MAIN: NL → SQL
# =============================================================
def nl_to_sql(user_query: str) -> str:
    user_query = expand_abbreviations(user_query)
    user_query = _normalize_separators(user_query)

    # Normalize VS / VERSUS / & -> AND so every downstream "and" check also handles VS
    # ql is used for all pattern matching; user_query preserved for name extraction
    ql = _normalize_vs_and(user_query)

    columns  = get_columns_from_presto()
    now      = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
    today    = now.date()
    fy_start = fy_of_date(today)

    # Pass normalized ql to metric extraction too
    metrics = extract_metrics(ql)
    grouping_type, group_expr, group_alias, force_current_fy = determine_time_grouping(ql)

    print(f"\nDEBUG query='{user_query}'  ql='{ql[:80]}'  today={today}  fy_start={fy_start}")

    date_condition = None   # SQL date filter string

    def _min_posting_date_expr() -> str:
        # Use table minimum posting_date (as YYYYMMDD string) as a dynamic "from the beginning" anchor.
        return (f"(SELECT MIN(CAST(\"posting_date\" AS VARCHAR)) "
                f"FROM \"{CATALOG}\".\"{SCHEMA}\".\"{TABLE_NAME}\" "
                f"WHERE TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')) IS NOT NULL)")

    def _infer_year_for_day_month(mn: int, day: int) -> int:
        # Choose the most recent occurrence of (day, month) that is not in the future vs `today`.
        # Example: on 2026-03-16, "12 dec" -> 2025-12-12, "12 jan" -> 2026-01-12.
        yr = today.year
        try:
            dt = datetime.date(yr, mn, day)
        except ValueError:
            return infer_year_for_month(mn, fy_start)
        return yr - 1 if dt > today else yr

    # ── TODAY ─────────────────────────────────────────────────────────────────
    if re.search(r'\btoday\b', ql):
        d = today.strftime('%Y%m%d')
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) = '{d}'"

    # ── YESTERDAY ────────────────────────────────────────────────────────────
    elif re.search(r'\byesterday\b', ql):
        d = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) = '{d}'"

    # ── THIS WEEK + LAST WEEK comparison  (FIX #3) ───────────────────────────
    elif 'last week' in ql and 'this week' in ql:
        monday_this = today - datetime.timedelta(days=today.weekday())
        monday_last = monday_this - datetime.timedelta(days=7)
        sunday_last = monday_last + datetime.timedelta(days=6)
        ts = monday_this.strftime('%Y%m%d'); te = today.strftime('%Y%m%d')
        ls = monday_last.strftime('%Y%m%d'); le = sunday_last.strftime('%Y%m%d')
        date_condition = (f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ts}' AND '{te}' "
                          f"OR CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ls}' AND '{le}')")
        if not grouping_type:
            grouping_type = 'custom_period'
            group_expr    = (f"CASE WHEN CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ts}' AND '{te}' "
                             f"THEN 'This Week' ELSE 'Last Week' END")
            group_alias   = 'week_label'

    # ── THIS WEEK ────────────────────────────────────────────────────────────
    elif re.search(r'\bthis\s+week\b', ql):
        monday = today - datetime.timedelta(days=today.weekday())
        date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                          f"'{monday.strftime('%Y%m%d')}' AND '{today.strftime('%Y%m%d')}'")

    # ── LAST WEEK ────────────────────────────────────────────────────────────
    elif re.search(r'\blast\s+week\b', ql):
        mt = today - datetime.timedelta(days=today.weekday())
        ml = mt - datetime.timedelta(days=7)
        sl = ml + datetime.timedelta(days=6)
        date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                          f"'{ml.strftime('%Y%m%d')}' AND '{sl.strftime('%Y%m%d')}'")

    # ── LAST N DAYS ───────────────────────────────────────────────────────────
    if not date_condition:
        m = re.search(r'last\s+(\d+|' + '|'.join(WORD_TO_NUM.keys()) + r')\s+days?', ql)
        if m:
            n = parse_num_word(m.group(1))
            s = (today - datetime.timedelta(days=n)).strftime('%Y%m%d')
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"

    # ── THIS MONTH + LAST MONTH comparison ───────────────────────────────────
    if not date_condition and 'this month' in ql and 'last month' in ql:
        ts = today.replace(day=1).strftime('%Y%m%d'); te = today.strftime('%Y%m%d')
        le = today.replace(day=1) - datetime.timedelta(days=1)
        ls = le.replace(day=1).strftime('%Y%m%d'); le_s = le.strftime('%Y%m%d')
        date_condition = (f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ts}' AND '{te}' "
                          f"OR CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ls}' AND '{le_s}')")
        if not grouping_type: grouping_type='month'; group_expr=MONTH_GRP; group_alias='month'

    # ── THIS MONTH ────────────────────────────────────────────────────────────
    if not date_condition and re.search(r'\bthis\s+month\b', ql):
        s = today.replace(day=1).strftime('%Y%m%d')
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"

    # ── LAST N MONTHS ─────────────────────────────────────────────────────────
    if not date_condition:
        m = re.search(r'last\s+(\d+|' + '|'.join(WORD_TO_NUM.keys()) + r')\s+months?', ql)
        if m:
            n = parse_num_word(m.group(1))
            le = today.replace(day=1) - datetime.timedelta(days=1)
            start = le
            for _ in range(n - 1): start = start.replace(day=1) - datetime.timedelta(days=1)
            date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                              f"'{start.replace(day=1).strftime('%Y%m%d')}' AND '{le.strftime('%Y%m%d')}'")

    # ── LAST MONTH ────────────────────────────────────────────────────────────
    if not date_condition and re.search(r'\blast\s+month\b', ql):
        le = today.replace(day=1) - datetime.timedelta(days=1)
        date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                          f"'{le.replace(day=1).strftime('%Y%m%d')}' AND '{le.strftime('%Y%m%d')}'")

    # ── THIS YEAR + LAST YEAR comparison ──────────────────────────────────────
    if not date_condition and 'this year' in ql and 'last year' in ql:
        s1,e1 = fy_dates(fy_start); s2,e2 = fy_dates(fy_start-1)
        date_condition = (f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s1}' AND '{e1}' "
                          f"OR CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s2}' AND '{e2}')")
        if not grouping_type: grouping_type='year'; group_expr=YEAR_GRP; group_alias='fiscal_year'

    # ── THIS YEAR / CURRENT YEAR ──────────────────────────────────────────────
    if not date_condition and re.search(r'\b(this|current)\s+year\b', ql):
        s, _ = fy_dates(fy_start)
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"

    # ── LAST YEAR / PREVIOUS YEAR ─────────────────────────────────────────────
    if not date_condition and re.search(r'\b(last|previous)\s+year\b', ql):
        s, e = fy_dates(fy_start - 1)
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"

    # ── LAST N YEARS ──────────────────────────────────────────────────────────
    if not date_condition:
        m = re.search(r'last\s+(\d+|' + '|'.join(WORD_TO_NUM.keys()) + r')\s+years?', ql)
        if m:
            n = parse_num_word(m.group(1))
            date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                              f"'{fy_start - n}0401' AND '{fy_start}0331'")
            # If the user asks "last N years separately", default to year-wise output.
            if _has_separately(ql) and not grouping_type:
                grouping_type = 'year'
                group_expr = YEAR_GRP
                group_alias = 'fiscal_year'

    # ── THIS QUARTER + LAST QUARTER comparison ────────────────────────────────
    if not date_condition and 'this quarter' in ql and 'last quarter' in ql:
        cq,cfy = current_quarter_info(today); pq,pfy = prev_quarter(cq,cfy)
        cs,ce_r = quarter_dates(cfy,cq); ps,pe = quarter_dates(pfy,pq)
        ce = min(datetime.date(int(ce_r[:4]),int(ce_r[4:6]),int(ce_r[6:])), today).strftime('%Y%m%d')
        date_condition = (f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{cs}' AND '{ce}' "
                          f"OR CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ps}' AND '{pe}')")
        if not grouping_type: grouping_type='quarter'; group_expr=QUARTER_GRP; group_alias='fiscal_quarter'

    # ── THIS QUARTER ──────────────────────────────────────────────────────────
    if not date_condition and re.search(r'\b(this|current)\s+quarter\b', ql):
        cq,cfy = current_quarter_info(today)
        cs,ce_r = quarter_dates(cfy,cq)
        ce = min(datetime.date(int(ce_r[:4]),int(ce_r[4:6]),int(ce_r[6:])), today).strftime('%Y%m%d')
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{cs}' AND '{ce}'"

    # ── LAST N QUARTERS ───────────────────────────────────────────────────────
    if not date_condition:
        m = re.search(r'last\s+(\d+|' + '|'.join(WORD_TO_NUM.keys()) + r')\s+quarters?', ql)
        if m:
            n = parse_num_word(m.group(1))
            qi,fyi = current_quarter_info(today)
            all_s,all_e = [],[]
            for _ in range(n):
                qi,fyi = prev_quarter(qi,fyi)
                qs,qe  = quarter_dates(fyi,qi)
                all_s.append(qs); all_e.append(qe)
            date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                              f"'{min(all_s)}' AND '{max(all_e)}'")

    # ── LAST QUARTER (single) ─────────────────────────────────────────────────
    if not date_condition and re.search(r'\blast\s+quarter\b', ql):
        cq,cfy = current_quarter_info(today); pq,pfy = prev_quarter(cq,cfy)
        ps,pe  = quarter_dates(pfy,pq)
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ps}' AND '{pe}'"

    # ── SPECIFIC QUARTER(S) with optional years  (FIX #1, #4) ────────────────
    if not date_condition:
        q_hits = extract_quarters_with_years(ql, fy_start)
        if q_hits:
            conds = []; labels = []
            for (qn, qfy) in q_hits:
                qs, qe = quarter_dates(qfy, qn)
                conds.append(f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{qs}' AND '{qe}')")
                labels.append((qs, qe, f"Q{qn} FY{qfy}-{str(qfy+1)[-2:]}"))
            date_condition = " OR ".join(conds)
            # Multiple distinct periods → add grouping so results are separate (FIX #1, #4)
            if len(q_hits) > 1 and not grouping_type:
                case_lines = "\n".join(
                    f"WHEN CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}' THEN '{lbl}'"
                    for s,e,lbl in labels
                )
                grouping_type = 'custom_period'
                group_expr    = f"CASE\n{case_lines}\nEND"
                group_alias   = 'period'
            print(f"DEBUG: quarters extracted={q_hits}")

    # ── FY explicit ───────────────────────────────────────────────────────────
    if not date_condition:
        fym = re.search(r'\bfy\s*(20\d{2})\b', ql)
        if fym:
            yr = int(fym.group(1)); s,e = fy_dates(yr)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"

    # ── FIX #2: ALL "FROM ..." OPEN-ENDED PATTERNS (no TO/TILL after them) ────
    # These must fire before range patterns so "from August" isn't swallowed
    # by the single-month detector and "from 2022" isn't ignored.
    if not date_condition:
        # FROM DD Month YYYY  (open-ended → to today)
        from_dd = re.search(
            r'\bfrom\s+(\d{1,2})(?:st|nd|rd|th)?\s+'
            r'(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'\s+(\d{4})'
            r'(?!\s*(?:to\b|till\b|until\b|up\s*to\b|-|–|—))',
            ql
        )
        if from_dd:
            dy = int(from_dd.group(1)); mn = MONTH_MAP[from_dd.group(2)]; yr = int(from_dd.group(3))
            try:
                datetime.date(yr, mn, dy)
                s = f"{yr}{mn:02d}{dy:02d}"
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"
                print(f"DEBUG: 'from DD Month YYYY' → {s} to today")
            except ValueError:
                pass

    if not date_condition:
        # FROM DD Month (no year, open-ended → to today)
        # Examples: "from 12 dec", "from 12 december"
        # NOTE: "from DD Month YYYY" (with year) is already handled by from_dd above.
        # Exclude cases where a year follows the month (the lookahead (?!\s+\d{4})) so that
        # "from 16 september 2022 to 11 december 2022" is NOT stolen here and falls through
        # to the proper range pattern below.
        from_dd_mon = re.search(
            r'\bfrom\s+(\d{1,2})(?:st|nd|rd|th)?\s+'
            r'(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'(?!\s+\d{4})'
            r'(?!\s*(?:to\b|till\b|until\b|up\s*to\b|-|–|—))',
            ql
        )
        if from_dd_mon:
            dy = int(from_dd_mon.group(1))
            mn = MONTH_MAP[from_dd_mon.group(2)]
            yr = _infer_year_for_day_month(mn, dy)
            try:
                datetime.date(yr, mn, dy)
                s = f"{yr}{mn:02d}{dy:02d}"
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"
                print(f"DEBUG: 'from DD Month' → {s} to today")
            except ValueError:
                pass

    if not date_condition:
        # FROM Month YYYY  (open-ended → to today)
        from_mon_yr = re.search(
            r'\bfrom\s+(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'\s+(\d{4})'
            r'(?!\s*(?:to\b|till\b|until\b|up\s*to\b|-|–|—))',
            ql
        )
        if from_mon_yr:
            mn = MONTH_MAP[from_mon_yr.group(1)]; yr = int(from_mon_yr.group(2))
            s, _ = month_dates(yr, mn)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"
            print(f"DEBUG: 'from Month YYYY' → {s} to today")

    if not date_condition:
        # FROM Month (no year, open-ended → to today)
        from_mon_only = re.search(
            r'\bfrom\s+(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'(?!\s*\d{4})'
            r'(?!\s*(?:to\b|till\b|until\b|up\s*to\b|-|–|—))',
            ql
        )
        if from_mon_only:
            mn  = MONTH_MAP[from_mon_only.group(1)]
            yr  = infer_year_for_month(mn, fy_start)
            s, _ = month_dates(yr, mn)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"
            print(f"DEBUG: 'from Month' (no year) → {s} to today")

    if not date_condition:
        # FROM YYYY  (open-ended → to today, treat YYYY as FY start)
        from_yr_only = re.search(
            r'\bfrom\s+(20\d{2})\b'
            r'(?!\s*(?:to\b|till\b|until\b|up\s*to\b|-|–|—))',
            ql
        )
        if from_yr_only:
            yr = int(from_yr_only.group(1))
            s, _ = fy_dates(yr)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"
            print(f"DEBUG: 'from YYYY' → {s} to today")

    # ── ISO date ──────────────────────────────────────────────────────────────
    if not date_condition:
        iso = re.search(r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b', ql)
        if iso:
            yr,mn,dy = int(iso.group(1)),int(iso.group(2)),int(iso.group(3))
            try:
                datetime.date(yr,mn,dy)
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) = '{yr}{mn:02d}{dy:02d}'"
            except ValueError: pass

    # ── DD/MM/YYYY ────────────────────────────────────────────────────────────
    if not date_condition:
        dmy = re.search(r'\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b', ql)
        if dmy:
            dy,mn,yr = int(dmy.group(1)),int(dmy.group(2)),int(dmy.group(3))
            try:
                datetime.date(yr,mn,dy)
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) = '{yr}{mn:02d}{dy:02d}'"
            except ValueError: pass

    # ── FROM DD Month YYYY TO DD Month YYYY ──────────────────────────────────
    if not date_condition:
        fp = (r'(?:from\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+'
              r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})'
              r'\s*(?:to|till|until|up\s*to|-+|–|—)\s*'
              r'(\d{1,2})(?:st|nd|rd|th)?\s+'
              r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})')
        fm = re.search(fp, ql)
        if fm:
            sd,sm,sy = int(fm.group(1)),MONTH_MAP[fm.group(2)],int(fm.group(3))
            ed,em,ey = int(fm.group(4)),MONTH_MAP[fm.group(5)],int(fm.group(6))
            date_condition = (f"CAST(\"posting_date\" AS VARCHAR) BETWEEN "
                              f"'{sy}{sm:02d}{sd:02d}' AND '{ey}{em:02d}{ed:02d}'")

    # ── Month YYYY TO Month YYYY ──────────────────────────────────────────────
    if not date_condition:
        myp = (r'(?:from\s+)?(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})'
               r'\s*(?:to|till|until|up\s*to|-+|–|—)\s*'
               r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})')
        mym = re.search(myp, ql)
        if mym:
            sm,sy = MONTH_MAP[mym.group(1)],int(mym.group(2))
            em,ey = MONTH_MAP[mym.group(3)],int(mym.group(4))
            s,_ = month_dates(sy,sm); _,e = month_dates(ey,em)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"

    # ── FROM Month TO DD Month [YYYY]  (e.g. "from july to 11 feb 2024") ────────
    if not date_condition:
        mddp = (r'(?:from\s+)?(january|february|march|april|may|june|july|august|september|october|november|december)'
                r'\s*(?:to|till|until|up\s*to|-+|–|—)\s*'
                r'(\d{1,2})(?:st|nd|rd|th)?\s+'
                r'(january|february|march|april|may|june|july|august|september|october|november|december)'
                r'(?:\s+(\d{4}))?')
        mddm = re.search(mddp, ql)
        if mddm:
            sm_n = MONTH_MAP[mddm.group(1)]
            ed_n = int(mddm.group(2))
            em_n = MONTH_MAP[mddm.group(3)]
            ey_n = int(mddm.group(4)) if mddm.group(4) else infer_year_for_month(em_n, fy_start)
            sy_n = ey_n if em_n >= sm_n else ey_n - 1
            s, _ = month_dates(sy_n, sm_n)
            try:
                datetime.date(ey_n, em_n, ed_n)
                ed_str = f"{ey_n}{em_n:02d}{ed_n:02d}"
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{ed_str}'"
                print(f"DEBUG: 'Month TO DD Month' → {s} to {ed_str}")
            except ValueError:
                pass

    # ── Month TO Month (no explicit year) ────────────────────────────────────
    if not date_condition:
        mrp = (r'(?:from\s+)?(january|february|march|april|may|june|july|august|september|october|november|december)'
               r'\s*(?:to|till|until|up\s*to|-+|–|—)\s*'
               r'(january|february|march|april|may|june|july|august|september|october|november|december)'
               r'(?:\s+(\d{4}))?')
        mrm = re.search(mrp, ql)
        if mrm:
            sm_n,em_n = MONTH_MAP[mrm.group(1)],MONTH_MAP[mrm.group(2)]
            yh = int(mrm.group(3)) if mrm.group(3) else None
            if yh:
                sy_n = ey_n = yh
                if em_n < sm_n: ey_n += 1
            else:
                sy_n = infer_year_for_month(sm_n, fy_start)
                ey_n = infer_year_for_month(em_n, fy_start)
                if em_n < sm_n: ey_n += 1
            s,_ = month_dates(sy_n,sm_n); _,e = month_dates(ey_n,em_n)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"

    # ── TILL DATE / UP TO TODAY ───────────────────────────────────────────────
    # Keep "till date" as current FY by default, but do not override more-specific "from ... till date" queries.
    if not date_condition and not re.search(r'\bfrom\b', ql) and re.search(r'\btill\s+(date|today|now)\b|\bup\s*to\s+(today|now|date)\b', ql):
        s,_ = fy_dates(fy_start)
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"

    # ── TILL Month / UP TO Month ──────────────────────────────────────────────
    if not date_condition:
        tm = re.search(
            r'(?:till|up\s*to|until)\s+(?:(\d{1,2})(?:st|nd|rd|th)?\s+)?'
            r'(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'(?:\s+(\d{4}))?', ql)
        if tm:
            mn  = MONTH_MAP[tm.group(2)]
            yr  = int(tm.group(3)) if tm.group(3) else infer_year_for_month(mn, fy_start)
            if tm.group(1): ed = f"{yr}{mn:02d}{int(tm.group(1)):02d}"
            else:           _, ed = month_dates(yr, mn)
            start_expr = f"'{fy_dates(fy_start)[0]}'"
            # If the question explicitly pins an end month+year ("till Sep 2022"), interpret it as
            # "from the beginning of available data until that month", unless an explicit FROM is present.
            if tm.group(3) and not re.search(r'\bfrom\b', ql):
                start_expr = _min_posting_date_expr()
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN {start_expr} AND '{ed}'"

    # ── FROM Month TILL TODAY ─────────────────────────────────────────────────
    if not date_condition:
        fmt2 = re.search(
            r'(?:from\s+)?(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'(?:\s+(\d{4}))?\s+till\s+(date|today|now)', ql)
        if fmt2:
            mn = MONTH_MAP[fmt2.group(1)]
            yr = int(fmt2.group(2)) if fmt2.group(2) else infer_year_for_month(mn, fy_start)
            s,_ = month_dates(yr, mn)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"

    # ── Specific date DD MonthName ────────────────────────────────────────────
    if not date_condition:
        p1 = r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(january|february|march|april|may|june|july|august|september|october|november|december)(?:\s+(?:of\s+)?(\d{4}))?\b'
        p2 = r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s+(?:of\s+)?(\d{4}))?\b'
        dm = re.search(p1,ql) or re.search(p2,ql)
        if dm:
            g = dm.groups()
            if re.search(p1,ql): day,mon,yr_s = int(g[0]),g[1],g[2]
            else:                 mon,day,yr_s = g[0],int(g[1]),g[2]
            mn = MONTH_MAP[mon]; yr = int(yr_s) if yr_s else infer_year_for_month(mn,fy_start)
            try:
                datetime.date(yr,mn,day)
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) = '{yr}{mn:02d}{day:02d}'"
            except ValueError: pass

    # ── TWO MONTHS with AND/VS ────────────────────────────────────────────────
    if not date_condition:
        mon_hits = re.findall(
            r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s*(\d{4})?\b', ql)
        if len(mon_hits) >= 2 and (any(w in ql for w in ['and','vs','versus','compared']) or _has_separately(ql)):
            conds2 = []; labels2 = []; seen_m = set()
            for mname, yr_s in mon_hits:
                mn = MONTH_MAP[mname]; yr = int(yr_s) if yr_s else infer_year_for_month(mn, fy_start)
                key = (mn, yr)
                if key in seen_m: continue
                seen_m.add(key)
                ms,me = month_dates(yr,mn)
                conds2.append(f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{ms}' AND '{me}')")
                labels2.append((ms,me,f"{mname.title()} {yr}"))
            if conds2:
                date_condition = " OR ".join(conds2)
                if len(labels2) > 1 and not grouping_type:
                    cl = "\n".join(f"WHEN CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}' THEN '{lb}'" for s,e,lb in labels2)
                    grouping_type='custom_period'; group_expr=f"CASE\n{cl}\nEND"; group_alias='period'

    # ── SINGLE MONTH ──────────────────────────────────────────────────────────
    if not date_condition:
        no_range = not re.search(
            r'(january|february|march|april|may|june|july|august|september|october|november|december)'
            r'\s*(?:to|till|until|up\s*to|-|–|—)', ql)
        if no_range:
            sm2 = re.search(
                r'\b(?:for\s+|in\s+)?(january|february|march|april|may|june|july|august|september|october|november|december)'
                r'(?:\s+(?:of\s+)?(\d{4}))?\b', ql)
            if sm2:
                mn = MONTH_MAP[sm2.group(1)]; yr = int(sm2.group(2)) if sm2.group(2) else infer_year_for_month(mn, fy_start)
                s,e = month_dates(yr,mn)
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"

    # ── TWO YEARS FY comparison ───────────────────────────────────────────────
    if not date_condition:
        two_yrs = list(dict.fromkeys(re.findall(r'\b(20\d{2})\b', ql)))
        if len(two_yrs) == 2 and (any(w in ql for w in ['and','vs','versus','compared']) or _has_separately(ql)):
            y1,y2 = int(two_yrs[0]),int(two_yrs[1])
            s1,e1 = fy_dates(y1); s2,e2 = fy_dates(y2)
            date_condition = (f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s1}' AND '{e1}' "
                              f"OR CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s2}' AND '{e2}')")
            if not grouping_type:
                grouping_type = 'custom_period'
                group_expr    = (f"CASE WHEN CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s1}' AND '{e1}' "
                                 f"THEN 'FY{y1}-{str(y1+1)[-2:]}' "
                                 f"ELSE 'FY{y2}-{str(y2+1)[-2:]}' END")
                group_alias   = 'fiscal_year'

    # ── YEAR RANGE 2022 to 2024 ───────────────────────────────────────────────
    if not date_condition:
        # MULTI-YEAR LIST (e.g. "2022, 2023 and 2024"):
        # Include all explicitly mentioned years. Skip contiguous ranges ("YYYY to YYYY") because the
        # dedicated range logic should capture those spans.
        if not re.search(r'\b20\d{2}\s*(?:to|-+)\s*20\d{2}\b', ql):
            yrs = list(dict.fromkeys(re.findall(r'\b(20\d{2})\b', ql)))
            if len(yrs) >= 2:
                year_conds = []
                labels = []
                for y_s in yrs:
                    y = int(y_s)
                    s, e = fy_dates(y)
                    year_conds.append(f"(CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}')")
                    labels.append((s, e, f"FY{y}-{str(y+1)[-2:]}"))
                date_condition = " OR ".join(year_conds)
                if not grouping_type:
                    case_lines = "\n".join(
                        f"WHEN CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}' THEN '{lbl}'"
                        for s, e, lbl in labels
                    )
                    grouping_type = 'custom_period'
                    group_expr = f"CASE\n{case_lines}\nEND"
                    group_alias = 'fiscal_year'

        # YEAR RANGE 2022 to 2024
        if not date_condition:
            yr_r = re.search(r'\b(20\d{2})\s*(?:to|-+)\s*(20\d{2})\b', ql)
            if yr_r:
                y1,y2 = int(yr_r.group(1)),int(yr_r.group(2))
                date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{y1}0401' AND '{y2+1}0331'"
                # If user asked for a multi-year range "separately", default to year-wise output.
                if _has_separately(ql) and not grouping_type:
                    grouping_type = 'year'
                    group_expr = YEAR_GRP
                    group_alias = 'fiscal_year'

    # ── SINGLE YEAR ───────────────────────────────────────────────────────────
    if not date_condition:
        yrm = re.search(r'\b(20\d{2})\b', ql)
        if yrm:
            yr = int(yrm.group(1)); s,e = fy_dates(yr)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"

    # ── DEFAULT: current FY to today ─────────────────────────────────────────
    if not date_condition:
        date_kws = ["year","fy","financial","quarter","month","date","yoy","qoq","last","current",
                    "this","week","today","yesterday","till","until","upto","from","january",
                    "february","march","april","may","june","july","august","september","october",
                    "november","december"] + [f"20{i:02d}" for i in range(20,31)]
        if not any(w in ql for w in date_kws):
            s,_ = fy_dates(fy_start)
            date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{today.strftime('%Y%m%d')}'"
            print("DEBUG: default current FY applied")

    # If "separately" is present, default to a split view (time-wise) when a time range is implied
    # and the user didn't explicitly request a grouping.
    if _has_separately(ql) and not grouping_type:
        if _has_explicit_year_token(ql) or ' year' in f" {ql} " or ' years' in f" {ql} " or ' fy' in f" {ql} ":
            grouping_type = 'year'
            group_expr = YEAR_GRP
            group_alias = 'fiscal_year'
            force_current_fy = False
        elif _has_quarter_token(ql) or ' quarter' in f" {ql} " or ' quarters' in f" {ql} ":
            grouping_type = 'quarter'
            group_expr = QUARTER_GRP
            group_alias = 'fiscal_quarter'
            force_current_fy = True
        elif _has_month_token(ql) or ' month' in f" {ql} " or ' months' in f" {ql} ":
            grouping_type = 'month'
            group_expr = MONTH_GRP
            group_alias = 'month'
            force_current_fy = True

    print(f"DEBUG: date_condition={date_condition}")
    print(f"DEBUG: grouping={grouping_type}/{group_alias}")

    # ── "by X" keyword overrides ─────────────────────────────────────────────
    if re.search(r'\bby\s+month\b',   ql) and not grouping_type:
        grouping_type='month';   group_expr=MONTH_GRP;   group_alias='month';   force_current_fy=True
    if re.search(r'\bby\s+quarter\b', ql) and not grouping_type:
        grouping_type='quarter'; group_expr=QUARTER_GRP; group_alias='fiscal_quarter'; force_current_fy=True
    if re.search(r'\bby\s+year\b',    ql) and not grouping_type:
        grouping_type='year';    group_expr=YEAR_GRP;    group_alias='fiscal_year';    force_current_fy=False

    # FIX #1: monthly/quarterly grouping with no explicit date → restrict to current FY only
    if force_current_fy and not date_condition:
        s, e = fy_dates(fy_start)
        date_condition = f"CAST(\"posting_date\" AS VARCHAR) BETWEEN '{s}' AND '{e}'"
        print(f"DEBUG: force_current_fy applied → {s} to {e}")

    # ── Dimension grouping flags ──────────────────────────────────────────────
    group_by_customer    = bool(re.search(r'\b(customer\s*(?:name)?\s*wise|by\s+customer(?:\s+name)?|customer\s+wise)\b', ql))
    group_by_company     = bool(re.search(r'\b(company\s*(?:code)?\s*wise|by\s+(?:company|project)|project\s+wise)\b', ql))
    group_by_sales_group = bool(re.search(r'\b(sales\s*group\s*wise|by\s+(?:sales\s*group|product)|product\s+wise|sale\s+group\s+wise)\b', ql))
    group_by_doc_type    = bool(re.search(r'\b(doc(?:ument)?\s*type\s*wise|by\s+doc(?:\s*type)?)\b', ql))
    group_by_bank        = bool(re.search(r'\b(bank\s*(?:name)?\s*wise|by\s+bank(?:\s*name)?)\b', ql))
    # FIX #3: tax code wise grouping — "tax code wise", "by tax code", "tax code wise"
    group_by_tax_code    = bool(re.search(r'\b(tax\s*code\s*wise|by\s+tax\s*code|tax\s*code\s+wise)\b', ql))

    # Generic "<xyz> wise" grouping (safe fallback):
    # If the user asks "<something> wise" and it doesn't match the known groupers above,
    # try to map <xyz> to a real column name and group by it.
    extra_group_col = None
    extra_group_alias = None
    if (' wise' in f" {ql} ") and not any([group_by_customer, group_by_company, group_by_sales_group, group_by_doc_type, group_by_bank, group_by_tax_code]):
        toks = [_strip_token_edges(t).lower() for t in ql.split() if _strip_token_edges(t)]
        col_key_map = {_norm_key(c): c for c in (columns or [])}
        matched = False
        for i, tok in enumerate(toks):
            if tok != 'wise' or i == 0:
                continue
            for n in (3, 2, 1):
                if i - n < 0:
                    continue
                phrase = " ".join(toks[i - n:i])
                key = _norm_key(phrase)
                # Prefer known semantic groupers.
                if key in {'customer', 'customers', 'customername', 'customercode'}:
                    group_by_customer = True; matched = True; break
                if key in {'company', 'companies', 'project', 'projects', 'companycode'}:
                    group_by_company = True; matched = True; break
                if key in {'product', 'products', 'salesgroup', 'salegroup'}:
                    group_by_sales_group = True; matched = True; break
                if key in {'doctype', 'documenttype'}:
                    group_by_doc_type = True; matched = True; break
                if key in {'bank', 'bankname'}:
                    group_by_bank = True; matched = True; break
                if key in {'taxcode', 'gst'}:
                    group_by_tax_code = True; matched = True; break
                # Otherwise, attempt direct column match.
                if key in col_key_map:
                    extra_group_col = col_key_map[key]
                    extra_group_alias = _safe_alias(extra_group_col)
                    matched = True
                    break
            if matched:
                break



    entity_conditions, comparison_dim = build_where_conditions(user_query, columns, ql)
    query_limit       = extract_limit(ql)
    having_condition  = extract_having(ql)

    # Customer listing heuristics ("show/top customers ..." / "customer whose ..."):
    # Enable customer-level grouping when the user is asking for a set of customers,
    # not when a specific customer filter is already present.
    has_specific_customer_filter = any('"Customer_Code"' in c or '"Customer_Name"' in c for c in entity_conditions)
    wants_customer_list = bool(re.search(
        r'(^\s*(?:show|list|give|provide|display|get|find|fetch)\s+customers?\b)|'
        r'(\bcustomers?\s+(?:with|whose)\b)|'
        r'(\b(top|best|first)\s+\d+\s+customers?\b)',
        ql
    ))
    if wants_customer_list and not group_by_customer and not has_specific_customer_filter:
        group_by_customer = True
    if having_condition and re.search(r'\bcustomers?\b', ql) and not group_by_customer and not has_specific_customer_filter:
        group_by_customer = True

    # ── Auto-enable grouping when AND/VS used between same-type entities ─────
    # e.g. "wave city vs wmcc sec 32" → comparison_dim='company' → auto GROUP BY company
    if comparison_dim == 'company'    and not group_by_company:     group_by_company     = True
    if comparison_dim == 'sales_group' and not group_by_sales_group: group_by_sales_group = True
    if comparison_dim == 'customer'   and not group_by_customer:    group_by_customer    = True


    print(f"DEBUG: entity_conditions={entity_conditions}")
    print(f"DEBUG: group_by_customer={group_by_customer}, group_by_company={group_by_company}, group_by_sg={group_by_sales_group}, group_by_tax={group_by_tax_code}")

    # ── Build SQL ─────────────────────────────────────────────────────────────
    def sel_parts():
        p = []
        if group_by_customer:    p.append('"Customer_Name"')
        if group_by_company:     p.append(f'{get_company_name_expr()} AS company_name')
        if group_by_sales_group: p.append(f'{get_sales_group_case_expr()} AS sales_group')
        if group_by_doc_type:    p.append(f'{get_doc_type_expr()} AS doc_type_fi')
        if group_by_bank:        p.append(f'{get_bank_name_expr()} AS customer_bank_and_mode')
        if group_by_tax_code:    p.append('CAST("Tax_Code" AS VARCHAR) AS tax_code')
        if extra_group_col:      p.append(f'CAST("{extra_group_col}" AS VARCHAR) AS {extra_group_alias}')
        if grouping_type:        p.append(f'{group_expr} AS {group_alias}')
        for field, alias in metrics:
            if field == "outstanding": p.append(f"{outstanding_expr()} AS outstanding")
            else:                      p.append(f'{numeric_sum_expr(field)} AS {alias}')
        return p

    def grp_parts():
        p = []
        if group_by_customer:    p.append('"Customer_Name"')
        if group_by_company:     p.append('"company_code"')
        if group_by_sales_group: p.append('"sales_group"')
        if group_by_doc_type:    p.append('"Doc_Type_FI"')
        if group_by_bank:        p.append('"Customer_Bank_and_Mode"')
        if group_by_tax_code:    p.append('"Tax_Code"')
        if extra_group_col:      p.append(f'"{extra_group_col}"')
        if grouping_type:        p.append(group_expr)
        return p

    def ord_parts():
        p = []
        if group_by_customer:    p.append('"Customer_Name"')
        if group_by_company:     p.append('"company_code"')
        if group_by_sales_group: p.append('"sales_group"')
        if group_by_doc_type:    p.append('"Doc_Type_FI"')
        if group_by_bank:        p.append('"Customer_Bank_and_Mode"')
        if group_by_tax_code:    p.append('"Tax_Code"')
        if extra_group_col:      p.append(f'"{extra_group_col}"')
        if grouping_type:        p.append(group_alias)
        return p

    sp = sel_parts(); gp = grp_parts(); op = ord_parts()

    sql = f'SELECT {", ".join(sp)} FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"'

    # WHERE
    where = ["TRY(date_parse(CAST(\"posting_date\" AS VARCHAR),'%Y%m%d')) IS NOT NULL"]
    if date_condition:
        where.append(f"({date_condition})")
    where.extend(entity_conditions)
    null_chk = [f'"{f}" IS NOT NULL' for f,_ in metrics if f not in ("outstanding","credit_note_total","others","refund_amount")]
    if len(null_chk) > 1:   where.append('(' + ' OR '.join(null_chk) + ')')
    elif null_chk:           where.extend(null_chk)
    sql += ' WHERE ' + ' AND '.join(where)

    if gp:
        sql += ' GROUP BY ' + ', '.join(gp)
        if having_condition: sql += f' HAVING {having_condition}'
    if op:
        sql += ' ORDER BY ' + ', '.join(op)
    if query_limit:
        sql += f' LIMIT {query_limit}'

    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("Generated SQL is invalid")

    print(f"DEBUG SQL (first 500): {sql[:500]}")
    return sql.strip()


# =============================================================
# FastAPI
# =============================================================
app = FastAPI(title="Watsonx NL2SQL + Presto API")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    sql: str
    data: list

@app.post("/customer-generate-sql", response_model=QueryResponse)
def generate_sql(req: QueryRequest):
    try:
        raw_sql   = nl_to_sql(req.question)
        clean_sql = enforce_descending_order(raw_sql)
        data      = run_presto_query(clean_sql)
        data      = add_total_row(data)
        print("SQL:", clean_sql[:400])
    except Exception as e:
        import traceback; traceback.print_exc()
        clean_sql = ""; data = [{"error": str(e)}]
    return {"sql": clean_sql, "data": data}
