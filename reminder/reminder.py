# ============================================================
# VERSION: rem4 - FIXED (group_by_* flags use _cmp_col)
# To confirm this version is running, check your server logs
# for: DEBUG: _cmp_col=
# ============================================================
from fastapi import FastAPI
from pydantic import BaseModel
import prestodb
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import Credentials
import calendar
import datetime
import pytz

# --------------------
# Configuration
# --------------------
CATALOG = "zsdva05"
SCHEMA = "reminder_report"
TABLE_NAME = "reminder_sf_report"
username = "ibmlhapikey_utkarshj@gadieltechnologies.com"
password = "kEYC-iaRZRuEb0AIck5x1iCDB32Zdb8MkC_3j6AzpIz3"
hostname = "892a9f01-c70b-4277-99b9-1dfcb04290c6.cvbhm81d0dmnvl5rjek0.lakehouse.appdomain.cloud"
portnumber = 30984
# Watsonx config — model is lazy-initialized on first use, NOT at startup
WATSONX_API_KEY = "kEYC-iaRZRuEb0AIck5x1iCDB32Zdb8MkC_3j6AzpIz3"
WATSONX_URL = "https://us-south.ml.cloud.ibm.com"
WATSONX_PROJECT_ID = "4152f31e-6a49-40aa-9b62-0ecf629aae42"

_model = None

def get_model():
    global _model
    if _model is None:
        creds = Credentials(url=WATSONX_URL, api_key=WATSONX_API_KEY)
        _model = ModelInference(
            model_id="meta-llama/llama-3-3-70b-instruct",
            credentials=creds,
            project_id=WATSONX_PROJECT_ID,
            params={"temperature": 0, "max_new_tokens": 300}
        )
    return _model

# --------------------
# Helper: Run SQL on Presto
# --------------------
def run_presto_query(sql: str):
    try:
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
    except Exception as e:
        return [{"error": f"Presto execution failed: {str(e)}"}]

# --------------------
# Helper: Get columns
# --------------------
def get_columns_from_presto():
    try:
        sql = f'SHOW COLUMNS FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"'
        result = run_presto_query(sql)
        return [row['Column'] for row in result] if result and not any('error' in row for row in result) else []
    except Exception:
        return []

# --------------------
# Helper: Normalize SQL
# --------------------

# Cache for live-fetched mappings so DB is only queried once per process lifetime
_sg_company_cache: dict = {}

def get_sg_and_company_mappings() -> tuple:
    """
    Fetch sales_group → code and company_name → code mappings live from the DB.
    Results are cached in-process so the DB is only hit once.
    Returns (sales_group_mapping, company_code_mapping) as lowercase-key dicts.
    Falls back to hardcoded defaults if the DB query fails.
    """
    global _sg_company_cache
    if _sg_company_cache:
        return _sg_company_cache["sg"], _sg_company_cache["co"]

    sg_map: dict = {}
    co_map: dict = {}

    try:
        # Fetch distinct SG_Description values
        sg_sql = f'SELECT DISTINCT "SG_Description", "sales_group" FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE "SG_Description" IS NOT NULL AND "sales_group" IS NOT NULL'
        sg_rows = run_presto_query(sg_sql)
        if sg_rows and not any('error' in r for r in sg_rows):
            for row in sg_rows:
                name = str(row.get('SG_Description', '') or '').strip().lower()
                code = str(row.get('sales_group', '') or '').strip()
                if name and code:
                    sg_map[name] = code
    except Exception as e:
        print(f"DEBUG: SG mapping fetch failed: {e}")

    try:
        # Fetch distinct company_name / company_code values
        co_sql = f'SELECT DISTINCT "company_name", "company_code" FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}" WHERE "company_name" IS NOT NULL AND "company_code" IS NOT NULL'
        co_rows = run_presto_query(co_sql)
        if co_rows and not any('error' in r for r in co_rows):
            for row in co_rows:
                name = str(row.get('company_name', '') or '').strip().lower()
                code = str(row.get('company_code', '') or '').strip()
                if name and code:
                    co_map[name] = code
    except Exception as e:
        print(f"DEBUG: Company mapping fetch failed: {e}")

    # Fallback hardcoded values if DB fetch returned nothing
    if not sg_map:
        sg_map = {
            'old plots': '001', 'new plots': '002', 'prime floors': '003',
            'wave floor': '004', 'dream homes': '005', 'executive floors': '006',
            'armonia villa': '007', 'fsi': '008', 'institutional': '009',
            'villas': '010', 'healthcare plot': '011', 'sco': '012',
            'aranyam valley': '013', 'wave galleria': '014', 'ews_001_(410)': '015',
            'lig_001_(310)': '016', 'dream bazaar': '017', 'swamanorath': '018',
            'ews_p2': '019', 'lig_p2': '020', 'mayfair park': '021',
            'commercial plots': '022', 'veridia': '023', 'veridia-3': '024',
            'eligo': '025', 'veridia-4': '026', 'veridia-5': '027',
            'veridia-6': '028', 'veridia-7': '029', 'eden': '030',
            'amore': '101', 'eminence': '102', 'irenia': '103',
            'trucia': '104', 'vasilia': '105', 'edenia': '106',
            'elegantia': '107', 'hssc': '108', 'metro mart': '109',
            'wave business square': '110', 'wave boulevard': '111', 'villa': '112',
            'livork': '113', 'wbt a': '114', 'wbt 1': '115',
            'plots-res': '301', 'plots-comm': '302', 'comm booth': '303',
            'wave garden': '304', 'wave floor 85': '305', 'villas': '306',
            'sco': '307', 'wave floor 99': '308', 'group housing 1': '309',
            'wave residency': '310', 'plots-res-if': '311', 'dream homes': '312',
            'harmony greens': '313', 'wave estate, gh2 ph2': '314', 'institutional': '315',
        }
    if not co_map:
        co_map = {
            'wave city': '1000', 'wmcc sec 32': '1100', 'wave estate': '1300',
        }

    # Ensure common canonical aliases always exist for matching, even if the live
    # SG_Description labels differ (prevents "new plots vs old plots" from
    # accidentally falling through to description-based comparison/grouping).
    _canonical_sg_aliases = {
        'old plots': '001',
        'new plots': '002',
    }
    for _k, _v in _canonical_sg_aliases.items():
        sg_map.setdefault(_k, _v)

    _sg_company_cache["sg"] = sg_map
    _sg_company_cache["co"] = co_map
    print(f"DEBUG: Loaded {len(sg_map)} SG mappings, {len(co_map)} company mappings")
    return sg_map, co_map


def normalize_sql(sql: str) -> str:
    sql = sql.replace("```", "").replace("`", '"')
    sql = sql.replace('\\"', '"').replace("\\'", "'")
    # Replace multiple whitespace with single space
    sql = " ".join(sql.split()).strip()
    # Remove inline comments (# to end of line)
    lines = sql.split('\n')
    lines = [line.split('#')[0] for line in lines]
    sql = ' '.join(lines).strip()
    sql = " ".join(sql.split()).strip()
    # Extract only the first SELECT statement
    sql_upper = sql.upper()
    first_select = sql_upper.find('SELECT')
    if first_select != -1:
        rest = sql[first_select:]
        rest_upper = rest.upper()
        second_select = rest_upper.find('SELECT', 1)
        if second_select != -1:
            sql = rest[:second_select].strip()
        else:
            sql = rest.strip()
    # Remove semicolons and standalone 'sql' keyword
    sql = sql.replace(';', '')
    # Remove standalone word 'sql' (case-insensitive)
    parts = sql.split()
    parts = [p for p in parts if p.lower() != 'sql']
    sql = ' '.join(parts).strip()
    # Split on noise phrases and keep only first part
    noise_phrases = ['User question:', 'Please provide', 'However', 'Note', 'Tip']
    for phrase in noise_phrases:
        idx = sql.lower().find(phrase.lower())
        if idx != -1:
            sql = sql[:idx].strip()
            break
    return sql

# --------------------
# Helper: Numeric SUM Expression
# --------------------
def numeric_sum_expr(column_name: str, alias: str = None) -> str:
    """
    Returns SUM(...) for numeric fields, but COUNT(...) if column is 'Description' (for reminders)
    """
    if column_name == "Description":
        return f'COUNT("{column_name}")'  # Count non-null Description rows
    else:
        return f"""SUM(
            TRY_CAST("{column_name}" AS DOUBLE)
        )"""


def minus_aware_sum_expr(field):
    return f"""SUM(
        TRY_CAST("{field}" AS DOUBLE)
    )"""



def get_company_name_expr():
    """
    Returns a CASE expression that maps company_code to company name
    """
    return """CASE 
                WHEN "company_code" = 1000 THEN 'Wave City'
                WHEN "company_code" = 1300 THEN 'Wave Estate'
                WHEN "company_code" = 1100 THEN 'WMCC Sec 32'
                ELSE CAST("company_code" AS VARCHAR)
            END"""
            
def get_description_expr():
    return """CASE
                WHEN UPPER("description") IN (
                    'PRE CANCELLATION NOTICE',
                    'PRE-CANCELLATION NOTICE',
                    'PRECANCELLATION NOTICE'
                )
                THEN 'PRE-CANCELLATION NOTICE'

                WHEN UPPER("description") IN (
                    'REMINDER 1',
                    'REMINDER ONE',
                    'REMINDER1'
                )
                THEN 'REMINDER 1'

                WHEN UPPER("description") IN (
                    'REMINDER2',
                    'REMINDER TWO',
                    'REMINDER 2'
                )
                THEN 'REMINDER2'

                WHEN UPPER("description") = 'OUTSTANDING LETTER'
                THEN 'OUTSTANDING LETTER'

                ELSE "description"
            END"""


def get_sales_group_expr():
    """
    Returns a CASE expression that maps sales_group code to sales group name
    """
    return """CASE 
                WHEN "sales_group" = 001 THEN 'old plots'
                WHEN "sales_group" = 002 THEN 'new plots'
                WHEN "sales_group" = 003 THEN 'prime floors'
                WHEN "sales_group" = 004 THEN 'wave floor'
                WHEN "sales_group" = 005 THEN 'dream homes'
                WHEN "sales_group" = 006 THEN 'executive floors'
                WHEN "sales_group" = 007 THEN 'armonia villa'
                WHEN "sales_group" = 008 THEN 'fsi'
                WHEN "sales_group" = 009 THEN 'institutional'
                WHEN "sales_group" = 010 THEN 'villas'
                WHEN "sales_group" = 011 THEN 'healthcare plot'
                WHEN "sales_group" = 012 THEN 'sco'
                WHEN "sales_group" = 013 THEN 'aranyam valley'
                WHEN "sales_group" = 014 THEN 'wave galleria'
                WHEN "sales_group" = 015 THEN 'ews_001_(410)'
                WHEN "sales_group" = 016 THEN 'lig_001_(310)'
                WHEN "sales_group" = 017 THEN 'dream bazaar'
                WHEN "sales_group" = 018 THEN 'swamanorath'
                WHEN "sales_group" = 019 THEN 'ews_p2'
                WHEN "sales_group" = 020 THEN 'lig_p2'
                WHEN "sales_group" = 021 THEN 'mayfair park'
                WHEN "sales_group" = 022 THEN 'commercial plots'
                WHEN "sales_group" = 023 THEN 'veridia'
                WHEN "sales_group" = 024 THEN 'veridia-3'
                WHEN "sales_group" = 025 THEN 'eligo'
                WHEN "sales_group" = 026 THEN 'veridia-4'
                WHEN "sales_group" = 027 THEN 'veridia-5'
                WHEN "sales_group" = 028 THEN 'veridia-6'
                WHEN "sales_group" = 029 THEN 'veridia-7'
                WHEN "sales_group" = 030 THEN 'eden'
                WHEN "sales_group" = 101 THEN 'amore'
                WHEN "sales_group" = 102 THEN 'eminence'
                WHEN "sales_group" = 103 THEN 'irenia'
                WHEN "sales_group" = 104 THEN 'trucia'
                WHEN "sales_group" = 105 THEN 'vasilia'
                WHEN "sales_group" = 106 THEN 'edenia'
                WHEN "sales_group" = 107 THEN 'elegantia'
                WHEN "sales_group" = 108 THEN 'hssc'
                WHEN "sales_group" = 109 THEN 'metro mart'
                WHEN "sales_group" = 110 THEN 'wave business square'
                WHEN "sales_group" = 111 THEN 'wave boulevard'
                WHEN "sales_group" = 112 THEN 'villa'
                WHEN "sales_group" = 113 THEN 'livork'
                WHEN "sales_group" = 114 THEN 'wbt a'
                WHEN "sales_group" = 115 THEN 'wbt 1'
                WHEN "sales_group" = 301 THEN 'plots-res'
                WHEN "sales_group" = 302 THEN 'plots-comm'
                WHEN "sales_group" = 303 THEN 'comm booth'
                WHEN "sales_group" = 304 THEN 'wave garden'
                WHEN "sales_group" = 305 THEN 'wave floor 85'
                WHEN "sales_group" = 306 THEN 'villas'
                WHEN "sales_group" = 307 THEN 'sco'
                WHEN "sales_group" = 308 THEN 'wave floor 99'
                WHEN "sales_group" = 309 THEN 'group housing 1'
                WHEN "sales_group" = 310 THEN 'wave residency'
                WHEN "sales_group" = 311 THEN 'plots-res-if'
                WHEN "sales_group" = 312 THEN 'dream homes'
                WHEN "sales_group" = 313 THEN 'harmony greens'
                WHEN "sales_group" = 314 THEN 'wave estate, gh2 ph2'
                WHEN "sales_group" = 315 THEN 'institutional'
                ELSE CAST("sales_group" AS VARCHAR)
            END"""




# --------------------
# Helper: Extract Metrics from Query (UPDATED)
# --------------------
def extract_metrics_from_query(user_query: str) -> list:
    """
    Extract all metrics mentioned in the query.
    Returns a list of tuples: [(field_name, alias), ...]

    RULE:
    - 'reminder'/'reminders'/'reminder count' etc. WITHOUT 'description'/'desc' keyword
      → COUNT("Description") as reminder_count  (counts all reminder rows)
    - 'description'/'desc' keyword used to GROUP or FILTER (e.g. 'description wise',
      'description is reminder 1') → still COUNT("Description") as reminder_count,
      because we're counting rows per description type
    - 'outstanding'/'due'/'balance'/'pending' → SUM("Outstanding")
    - If none match → default to reminder_count (this is a reminder report)
    """
    metrics = []
    query_lower = user_query.lower()

    # 1. Reminder-related keywords → COUNT(Description)
    #    NOTE: 'description wise' / 'by description' alone do NOT force reminder_count
    #    when the user clearly wants outstanding (e.g. "outstanding description wise").
    #    They only add reminder_count when NO outstanding keyword is present.
    reminder_keywords = [
        'reminder', 'reminders', 'total reminder', 'reminder count',
        'how many reminder', 'reminder wise', 'reminder report',
        'count of reminder', 'number of reminder',
    ]
    # These description-grouping words add reminder_count only as a fallback
    desc_grouping_keywords = [
        'description wise', 'desc wise', 'by description', 'per description',
        'each description', 'description type', 'desc type',
    ]
    # Strip out any description-filter value from the query before checking metrics,
    # so that "desc: outstanding letter" does not trigger the Outstanding metric.
    # We remove the portion after the description trigger phrase.
    _desc_trigger_prefixes = [
        'description is ', 'description= ', 'description : ', 'description: ',
        'desc is ', 'desc= ', 'desc : ', 'desc: ',
        'reminder type is ', 'reminder type: ',
        'letter type is ', 'letter type: ',
    ]
    query_lower_for_metrics = query_lower
    for _prefix in _desc_trigger_prefixes:
        _idx = query_lower_for_metrics.find(_prefix)
        if _idx != -1:
            # Remove from the trigger phrase to end-of-value (up to next noise boundary)
            _after_trigger = query_lower_for_metrics[_idx + len(_prefix):]
            _noise_check = {'for', 'of', 'in', 'by', 'from', 'count', 'total', 'sum', 'report',
                            'monthly', 'quarterly', 'yearly', 'wise', 'separately'}
            _val_words = []
            for _w in _after_trigger.split():
                if _w in _noise_check: break
                _val_words.append(_w)
            _val_len = len(' '.join(_val_words))
            # Blank out the trigger + value
            query_lower_for_metrics = (
                query_lower_for_metrics[:_idx] +
                ' ' * (len(_prefix) + _val_len) +
                query_lower_for_metrics[_idx + len(_prefix) + _val_len:]
            )
            break

    has_reminder = any(keyword in query_lower for keyword in reminder_keywords)
    has_outstanding = any(keyword in query_lower_for_metrics for keyword in [
        'outstanding', 'total outstanding', 'outstanding amount',
        'outstanding wise', 'due', 'balance', 'pending'
    ])
    has_desc_grouping = any(keyword in query_lower for keyword in desc_grouping_keywords)

    if has_reminder or (has_desc_grouping and not has_outstanding):
        metrics.append(("Description", "reminder_count"))

    # 2. Outstanding-related keywords → SUM(Outstanding)
    if has_outstanding:
        metrics.append(("Outstanding", "outstanding"))

    # 3. If nothing matched → safe default is reminder count (this is a reminder report)
    if not metrics:
        if any(word in query_lower for word in ['total', 'sum', 'amount', 'wise', 'report']):
            metrics.append(("Outstanding", "outstanding"))
        else:
            metrics.append(("Description", "reminder_count"))

    return metrics


# --------------------
# Helper: Build WHERE conditions
# -------------------- NEW HELPER: Mail Sent Filtering --------------------
def build_mail_sent_condition(user_query: str) -> str | None:
    """
    Returns SQL condition for Mail_Sent column or None if no email intent detected.
    """
    q = user_query.lower()
    
    # Keywords that mean "sent"
    sent_keywords = [
        'mail sent', 'email sent', 'sent mail', 'sent email',
        'mails sent', 'emails sent', 'reminder sent', 'reminders sent',
        'mail is sent', 'email is sent', 'delivered mail', 'delivered email', 'mail delivered', 'email was delivered'
    ]
    
    # Keywords that mean "not sent"
    not_sent_keywords = [
        'not sent', 'mail not sent', 'email not sent', 'not mail sent',
        'unsent', 'pending mail', 'pending email', 'mail pending',
        'email pending', 'reminder not sent', 'reminders not sent',
        'yet to send', 'to be sent', 'mail not send', 'mail not delivered', 'email not delivered', 'email was not delivered'
    ]
    
    has_sent = any(kw in q for kw in sent_keywords)
    has_not_sent = any(kw in q for kw in not_sent_keywords)
    
    if has_sent and not has_not_sent:
        return '"Mail_Sent" = \'X\''
    
    if has_not_sent and not has_sent:
        # Most common pattern — adjust if your "not sent" value is NULL or 'N'
        return '("Mail_Sent" IS NULL OR TRIM("Mail_Sent") = \'\')'
        # return '"Mail_Sent" != \'X\''
        # Alternative (if unsent = NULL): return '"Mail_Sent" IS NULL OR "Mail_Sent" != \'X\''
    
    # No clear email intent → no filter
    return None
# --------------------

# ─────────────────────────────────────────────────────────────────────────────
# Universal vs / and / separately comparison detector
# Returns a dict:
#   {
#     "active": bool,          # True when comparison mode should be used
#     "column": str | None,    # SQL column name to GROUP BY
#     "group_expr": str | None,# CASE expr for display (None → use column directly)
#     "group_alias": str,      # alias for SELECT
#     "values": list[str],     # SQL filter values (codes / names)
#     "in_clause": str | None, # ready-made "col IN (...)" or LIKE clause
#   }
# ─────────────────────────────────────────────────────────────────────────────
import re as _re

def _match_sg_whole_word(query_lower: str, sg_map: dict) -> list:
    """
    Match sales-group names against the query using whole-word matching so that
    'villa' does NOT fire inside 'armonia villa' or 'villas', and 'veridia' does
    NOT fire inside 'veridia-3'.

    Returns list of (name, code) in order of match, longest names first (so
    'wave floor 85' is consumed before 'wave floor').
    """
    matched = []
    seen_codes = set()
    seen_names = set()
    remaining = query_lower
    for name in sorted(sg_map.keys(), key=len, reverse=True):
        code = sg_map[name]
        # Word-boundary pattern: not preceded or followed by alphanumeric chars
        pattern = r'(?<![a-z0-9\-_])' + _re.escape(name) + r'(?![a-z0-9\-_])'
        if _re.search(pattern, remaining):
            if name not in seen_names:
                matched.append((name, code))
                seen_codes.add(code)
                seen_names.add(name)
                # Blank out the matched name so shorter sub-names don't fire inside it
                remaining = _re.sub(pattern, ' ', remaining)
    return matched



def _match_co_whole_word(query_lower: str, co_map: dict) -> list:
    """Same whole-word matching for company names."""
    matched = []
    seen_codes = set()
    seen_names = set()
    remaining = query_lower
    for name in sorted(co_map.keys(), key=len, reverse=True):
        code = co_map[name]
        pattern = r'(?<![a-z0-9\-_])' + _re.escape(name) + r'(?![a-z0-9\-_])'
        if _re.search(pattern, remaining):
            if name not in seen_names:
                matched.append((name, code))
                seen_codes.add(code)
                seen_names.add(name)
                remaining = _re.sub(pattern, ' ', remaining)
    return matched


def detect_comparison_intent(user_query: str, sales_group_mapping: dict, company_code_mapping: dict) -> dict:
    """
    Detects whether the user wants to compare two or more values of the SAME column
    using 'vs', 'versus', 'and', or 'separately'.
    Works for: sales_group, company, description/reminder-type, mail_sent,
               dunning_level, booking_no, user_id, letter_no, customer_name.
    """
    q = user_query.lower()
    # Include ' and ', 'compare', 'comparison', 'between' as comparison signals.
    # 'and' alone is very broad, so it is only meaningful when 2+ matching entities
    # are found for the same column — the per-section checks below enforce that.
    HAS_COMPARISON = any(marker in q for marker in [
        'vs.', 'vs ', ' vs', 'versus', 'separately',
        ' and ', 'compare', 'comparison', 'between',
    ])

    # ── 1. Sales group ────────────────────────────────────────────────────────
    # Use whole-word matching to prevent 'villa' firing inside 'armonia villa',
    # 'veridia' firing inside 'veridia-3', etc.
    unique_sg = _match_sg_whole_word(q, sales_group_mapping)

    if HAS_COMPARISON and len(unique_sg) >= 2:
        codes = [c for _, c in unique_sg]
        return {
            "active": True,
            "column": '"sales_group"',
            "group_expr": get_sales_group_expr(),
            "group_alias": "sales_group",
            "in_clause": f'"sales_group" IN ({", ".join(codes)})',
        }

    # ── 2. Company ────────────────────────────────────────────────────────────
    unique_co = _match_co_whole_word(q, company_code_mapping)

    if HAS_COMPARISON and len(unique_co) >= 2:
        codes = [c for _, c in unique_co]
        return {
            "active": True,
            "column": '"company_code"',
            "group_expr": get_company_name_expr(),
            "group_alias": "company_name",
            "in_clause": f'"company_code" IN ({", ".join(codes)})',
        }

    # ── 3. Description / reminder type ───────────────────────────────────────
    # Match known keywords AND dynamically extract values after "description is X and Y"
    desc_keywords = [
        'pre-cancellation notice', 'pre cancellation notice', 'precancellation notice',
        'pre-cancellation', 'pre cancellation', 'precancellation',
        'cancellation letter', 'cancellation notice',
        'reminder 1', 'reminder1', 'reminder one',
        'reminder 2', 'reminder2', 'reminder two',
        'reminder 3', 'reminder3', 'reminder three',
        'reminder 4', 'reminder4', 'reminder four',
        'reminder 5', 'reminder5', 'reminder five',
        'outstanding letter', 'outstanding notice',
        'first reminder', 'second reminder', 'third reminder',
        '1st reminder', '2nd reminder', '3rd reminder',
    ]
    found_descs = [d for d in desc_keywords if d in q]

    # Dynamic extraction: find "description is/: X and/vs Y" pattern and pull all values
    # This handles any description value, not just hardcoded ones
    _desc_trigger_phrases = [
        'description is ', 'description: ', 'description= ',
        'desc is ', 'desc: ', 'desc= ',
        'reminder type is ', 'reminder type: ',
        'letter type is ', 'letter type: ',
    ]
    _desc_noise = {
        'for', 'of', 'in', 'with', 'by', 'from', 'last', 'this', 'next',
        'monthly', 'quarterly', 'yearly', 'separately', 'wise',
        'project', 'company', 'customer', 'wave', 'city', 'estate',
        'till', 'until', 'upto', 'up', 'before', 'after', 'on', 'to',
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december',
        'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'sept', 'oct', 'nov', 'dec',
        'show', 'give', 'display', 'fetch', 'get', 'find', 'total', 'count', 'sum',
        'whose', 'which', 'where', 'that', 'the', 'a', 'an',
    }
    _split_connectors = [' and ', ' vs ', ' or ', ' versus ', ',']
    _dynamic_descs = []
    for _phrase in _desc_trigger_phrases:
        _idx = q.find(_phrase)
        if _idx != -1:
            _after = q[_idx + len(_phrase):].strip()
            # Split on connectors to get individual values
            _parts = [_after]
            for _conn in _split_connectors:
                _new_parts = []
                for _p in _parts:
                    _new_parts.extend(_p.split(_conn))
                _parts = _new_parts
            for _part in _parts:
                _part = _part.strip().strip('"\',.')
                # Collect words until a noise/stop word
                _words = _part.split()
                _clean = []
                for _w in _words:
                    if _w.lower() in _desc_noise:
                        break
                    _clean.append(_w)
                _val = ' '.join(_clean).strip()
                if _val and len(_val) > 2:
                    _dynamic_descs.append(_val)
            break

    # Merge: use dynamic values if they give more coverage
    # If dynamic extraction found 2+ values, use those (they're the explicit user intent)
    if len(_dynamic_descs) >= 2:
        found_descs = _dynamic_descs
    elif _dynamic_descs and not found_descs:
        found_descs = _dynamic_descs

    _explicit_split = any(m in q for m in ['separately', 'compare', 'comparison', 'vs', 'versus', 'between'])
    _reminder_group_signal = (
        ('reminder' in q or 'description' in q or 'letter type' in q or 'reminder type' in q)
        and _explicit_split
    )
    # Also trigger when query says "description is X and Y" (and implies multiple values)
    _desc_multi_and = (
        len(_dynamic_descs) >= 2
        and any(f' and ' in q[q.find(_ph):q.find(_ph)+80] for _ph in _desc_trigger_phrases if _ph in q)
    )
    if HAS_COMPARISON and (len(found_descs) >= 2 or (len(found_descs) >= 1 and _explicit_split) or _reminder_group_signal or _desc_multi_and):
        if found_descs:
            like_clauses = ' OR '.join(
                [f'UPPER("Description") LIKE UPPER(\'%{d.upper()}%\')' for d in found_descs]
            )
            in_clause = f'({like_clauses})'
        else:
            in_clause = None
        return {
            "active": True,
            "column": '"Description"',
            "group_expr": get_description_expr(),
            "group_alias": "description",
            "in_clause": in_clause,
        }

    # ── 4. Mail sent vs not sent ──────────────────────────────────────────────
    mail_sent_words = ['mail sent', 'email sent', 'sent mail', 'sent email', 'mails sent', 'emails sent']
    mail_not_sent_words = ['not sent', 'mail not sent', 'email not sent', 'unsent', 'pending mail']
    has_sent = any(w in q for w in mail_sent_words)
    has_not_sent = any(w in q for w in mail_not_sent_words)
    if HAS_COMPARISON and has_sent and has_not_sent:
        return {
            "active": True,
            "column": '"Mail_Sent"',
            "group_expr": None,
            "group_alias": "mail_sent",
            "in_clause": None,  # no filter — show all
        }

    # ── 5. Dunning level ──────────────────────────────────────────────────────
    # Find all dunning level numbers (e.g. "dunning level 1", "dunning level 2")
    dl_matches = []
    words = q.split()
    for i, word in enumerate(words):
        if word == 'dunning' and i + 1 < len(words) and words[i+1] == 'level':
            # Next token may be the number, or it could be "dunning level separately"
            if i + 2 < len(words) and words[i+2].isdigit():
                dl_matches.append(words[i+2])
    # Also handle "dunning level separately" / "dunning level wise" — show all levels grouped
    _dunning_group_signal = 'dunning level' in q and _explicit_split
    if HAS_COMPARISON and (len(dl_matches) >= 2 or _dunning_group_signal):
        if dl_matches:
            codes = ", ".join(dl_matches)
            in_clause = f'"Dunning_Level" IN ({codes})'
        else:
            in_clause = None  # No filter — group all dunning levels
        return {
            "active": True,
            "column": '"Dunning_Level"',
            "group_expr": None,
            "group_alias": "dunning_level",
            "in_clause": in_clause,
        }

    return {"active": False}

def build_where_conditions(user_query: str, columns: list, skip_columns: set = None, known_sg_names: list = None) -> tuple:
    """
    Build WHERE conditions based on filters in the query (excluding date conditions).
    Returns a list of condition strings.

    skip_columns: set of column aliases ('description', 'company_name', 'sales_group',
                  'dunning_level') that comparison intent will own — skip generating
                  a filter for those here to avoid AND-conflict with the comparison's
                  own in_clause.
    known_sg_names: list of sales-group name strings already matched from the query.
                    Stripped from the query before customer-name extraction so product
                    names like 'new plots' are never mistaken for customer names.
    """
    if skip_columns is None:
        skip_columns = set()
    if known_sg_names is None:
        known_sg_names = []
    conditions = []
    query_lower = user_query.lower()

    # Load live mappings (cached after first call)
    sales_group_mapping, company_code_mapping = get_sg_and_company_mappings()

    # Check for company name patterns and map to company_code
    matched_companies = [code for _, code in _match_co_whole_word(query_lower, company_code_mapping)]
    matched_companies = list(set(matched_companies))
    
    # Build company condition with OR logic if multiple companies
    # (compare_companies flag set later — company WHERE added conditionally in sales group block)
    if 'company_name' not in skip_columns:
        if len(matched_companies) == 1:
            conditions.append(f'"company_code" = {matched_companies[0]}')
        elif len(matched_companies) > 1:
            # Don't add WHERE yet — will be decided after vs/and detection below
            print(f"DEBUG: Multiple companies detected: {matched_companies}")
        
    #======================== start for sales group logic ========================
        # ----------------------------------------------------
    # sales_group_mapping already loaded via get_sg_and_company_mappings() above

    # 1️⃣ Detect by sales group name (whole-word, longest first)
    matched_sales_groups = [code for _, code in _match_sg_whole_word(query_lower, sales_group_mapping)]

    # 2️⃣ Detect direct code mention (001, 102, 305, etc.)
    # Detect direct 3-digit sales group code mentions (001-315)
    direct_code_match = []
    for token in query_lower.split():
        # Strip punctuation
        clean = token.strip('.,;:!?()"\'')
        if len(clean) == 3 and clean.isdigit():
            n = int(clean)
            if 0 <= n <= 399:
                direct_code_match.append(clean)
    for code in direct_code_match:
        matched_sales_groups.append(code)

    # 3️⃣ Detect "vs" / "and" comparison intent between sales groups
    has_vs_or_and = any(marker in query_lower for marker in [
        'vs.', 'vs ', ' vs', 'versus', ' and ', 'separately', 'compare', 'comparison', 'between'
    ])
    compare_sales_groups = len(matched_sales_groups) > 1 and has_vs_or_and
    compare_companies    = len(matched_companies)    > 1 and has_vs_or_and

    # If the user explicitly mentioned multiple sales groups with vs/and,
    # keep the result restricted to only those values even when not grouping.
    # (When universal comparison detection is active, it owns this column and
    # will add its own in_clause; skip_columns prevents double-filtering.)
    if compare_sales_groups and 'sales_group' not in skip_columns:
        sg_conditions = ', '.join(sorted(set(matched_sales_groups)))
        conditions.append(f'"sales_group" IN ({sg_conditions})')

    # Apply OR / single filter (only as WHERE when NOT in compare mode and not owned by comparison)
    if not compare_sales_groups and 'sales_group' not in skip_columns:
        if len(matched_sales_groups) == 1:
            conditions.append(f'"sales_group" = {matched_sales_groups[0]}')
        elif len(matched_sales_groups) > 1:
            sg_conditions = ', '.join(matched_sales_groups)
            conditions.append(f'"sales_group" IN ({sg_conditions})')

    if not compare_companies and len(matched_companies) > 1 and 'company_name' not in skip_columns:
        # Multiple companies, no vs/and — add as OR filter
        company_or_conditions = ' OR '.join([f'"company_code" = {code}' for code in matched_companies])
        conditions.append(f'({company_or_conditions})')

    #========================= end for sales group logic =========================

   #============================== DYNAMIC customer name detection ============================
    extracted_customer_name = None
    partial_match_used = False
    query_lower_cust = user_query.lower()

    # ── Strip matched SG names + time keywords from query before customer extraction ──
    # Prevents "new plots yoy" or "prime floors" from being captured as a customer name.
    query_for_cust_extract = query_lower_cust
    for sg_name in known_sg_names:
        query_for_cust_extract = query_for_cust_extract.replace(sg_name.lower(), ' ')
    _time_strip = [
        'yoy', 'qoq', 'mom', 'wow', 'yearly', 'monthly', 'quarterly', 'weekly',
        'year wise', 'month wise', 'quarter wise', 'week wise',
        'year on year', 'year over year', 'month on month', 'quarter on quarter',
        'by year', 'by month', 'by quarter', 'by week',
        'annually', 'annual', 'each year', 'per year', 'yearwise',
    ]
    for _ts in _time_strip:
        query_for_cust_extract = query_for_cust_extract.replace(_ts, ' ')
    query_for_cust_extract = ' '.join(query_for_cust_extract.split())

    # Skip customer extraction if query is purely about a sales group/product
    personal_name_signals = [
        'customer', 'client', 'buyer', 'owner', 'person', 'name',
        'mr.', 'mrs.', 'ms.', 'dr.', 'sh.', 'shri'
    ]
    is_pure_sales_group_query = (
        any(sg in query_lower_cust for sg in sales_group_mapping.keys())
        and not any(signal in query_lower_cust for signal in personal_name_signals)
    )
    print(f"DEBUG: is_pure_sales_group_query = {is_pure_sales_group_query}")

    if not is_pure_sales_group_query:
        # --- Step 1: Quoted name (highest priority) ---
        quoted_match = None
        for quote_char in ['"', "'"]:
            start = user_query.find(quote_char)
            if start != -1:
                end = user_query.find(quote_char, start + 1)
                if end != -1 and end - start - 1 >= 3:
                    quoted_match = user_query[start+1:end]
                    break
        if quoted_match:
            extracted_customer_name = quoted_match.strip()
            print(f"DEBUG: Customer from quotes: '{extracted_customer_name}'")

        # --- Step 2: Trigger-phrase extraction (on sanitised query) ---
        if not extracted_customer_name:
            stopwords = {
                'all', 'the', 'a', 'an', 'is', 'are', 'was', 'total', 'outstanding',
                'reminder', 'report', 'monthly', 'quarterly', 'yearly', 'weekly',
                'by', 'of', 'for', 'from', 'in', 'with', 'and', 'or', 'show',
                'wise', 'company', 'sales', 'group', 'description', 'wave', 'city',
                'estate', 'data', 'details', 'info', 'information', 'mail', 'sent',
                'booking', 'dunning', 'letter', 'user', 'month', 'year', 'quarter', 'week',
                'project', 'product', 'date', 'daily', 'top', 'first', 'last', 'previous',
                'current', 'this', 'next', 'give', 'display', 'fetch', 'find', 'get',
                'whose', 'which', 'where', 'that', 'has', 'have', 'having',
                'plots', 'floors', 'villas', 'villa', 'homes', 'garden', 'valley',
                'galleria', 'bazaar', 'park', 'residency', 'greens', 'housing',
                'booth', 'institutional', 'commercial', 'healthcare', 'executive',
                'armonia', 'veridia', 'eligo', 'eden', 'amore', 'eminence', 'irenia',
                'trucia', 'vasilia', 'edenia', 'elegantia', 'hssc', 'livork', 'metro',
                'mart', 'boulevard', 'square', 'new', 'old', 'prime', 'dream', 'wave',
                # time keywords must never start a customer name
                'yoy', 'qoq', 'mom', 'wow', 'annual', 'annually', 'count', 'sum',
            }
            break_words = {
                'by', 'of', 'for', 'from', 'in', 'with', 'or', 'vs',
                'total', 'outstanding', 'reminder', 'report', 'wise',
                'monthly', 'quarterly', 'yearly', 'weekly', 'month', 'year', 'quarter', 'week',
                'company', 'sales', 'group', 'description', 'mail', 'sent',
                'booking', 'dunning', 'letter', 'user', 'show', 'give', 'display', 'fetch', 'find',
                'whose', 'which', 'where', 'that', 'has', 'have', 'having',
                'yoy', 'qoq', 'mom', 'wow', 'annual', 'annually', 'count', 'sum',
            }
            # Trigger phrases that precede a customer name.
            # NOTE: broad triggers 'of ' and 'for ' are intentionally REMOVED —
            # they caused product names and time keywords to be captured as customer names.
            trigger_phrases = [
                'customer name is ',
                'customer name ',
                'for customer ',
                'of customer ',
                'customer ',
                'details of ',
                'details for ',
                'data of ',
                'data for ',
                'report of ',
                'report for ',
                'info of ',
                'info for ',
                'information of ',
                'information for ',
            ]
            # Use sanitised query (SG names & time keywords already stripped)
            query_for_trigger = query_for_cust_extract
            for trigger in trigger_phrases:
                idx = query_for_trigger.lower().find(trigger)
                if idx != -1:
                    after = query_for_trigger[idx + len(trigger):].strip()
                    # Collect words until a break word or end
                    words_after = after.split()
                    clean_words = []
                    for w in words_after:
                        bare = w.strip('.,;:!?()"\'').lower()
                        if bare in break_words:
                            break
                        if w[0:1].isupper() or len(clean_words) > 0:
                            clean_words.append(w.strip('.,;:!?()"\''))
                        else:
                            break
                    candidate = ' '.join(clean_words).strip()
                    if len(candidate) > 2 and not all(w.lower() in stopwords for w in candidate.split()):
                        extracted_customer_name = candidate
                        print(f"DEBUG: Customer from trigger pattern: '{extracted_customer_name}'")
                        break

        # --- Step 3: LLM fallback ---
        if not extracted_customer_name:
            customer_signal_words = [
                'customer', 'client', 'buyer', 'owner', 'person', 'name',
                'mr.', 'mrs.', 'ms.', 'dr.', 'sh.', 'shri'
            ]
            has_customer_signal = any(word in query_lower_cust for word in customer_signal_words)
            if has_customer_signal:
                try:
                    llm_prompt = f"""Extract ONLY the customer/person name from this query.
    Return ONLY the name, nothing else. If no specific customer name is mentioned, return "NONE".

    Query: "{user_query}"

    Rules:
    - Return only the proper name (e.g. "Anita Agarwal", "Ravi Garg")
    - Do NOT return keywords like "customer", "total", "outstanding", "reminder"
    - Do NOT return company names like "Wave City", "Wave Estate"
    - If the query is about all customers or no specific customer, return "NONE"

    Customer name:"""
                    llm_response = get_model().generate_text(prompt=llm_prompt)
                    llm_name = llm_response.strip().strip('"\'').strip()
                    # Only take the first line — LLM sometimes returns multi-line code
                    llm_name = llm_name.split('\n')[0].strip().strip('"\'').strip()
                    stopwords_check = {'none', 'all', 'total', 'outstanding', 'reminder', 'customer', 'report'}
                    
                    # Strict validation: reject if it looks like code or garbage
                    invalid_chars = {'(', ')', '[', ']', '{', '}', '=', '#', '`', '%', ':', '\n'}
                    name_chars_valid = (
                        llm_name
                        and all(c.isalpha() or c in ' .-' for c in llm_name)
                        and not any(c in llm_name for c in invalid_chars)
                    )
                    is_valid_name = (
                        llm_name
                        and llm_name.upper() != "NONE"
                        and len(llm_name) > 2
                        and len(llm_name) <= 60
                        and llm_name.lower() not in stopwords_check
                        and name_chars_valid
                        and llm_name[0:1].isalpha()
                    )
                    
                    if is_valid_name:
                        extracted_customer_name = llm_name
                        print(f"DEBUG: Customer from LLM: '{extracted_customer_name}'")
                    else:
                        print(f"DEBUG: LLM returned invalid/code response, ignoring: '{llm_name[:80]}'")
                except Exception as e:
                    print(f"DEBUG: LLM customer extraction failed: {e}")

    # --- Apply SQL condition --- (always runs, outside the if block)
    if extracted_customer_name:
        name_words = extracted_customer_name.strip().split()
        if len(name_words) == 1:
            conditions.append(
                f'LOWER("customer_name") LIKE LOWER(\'%{name_words[0]}%\')'
            )
            partial_match_used = True
        elif len(name_words) >= 2:
            word_conditions = ' AND '.join(
                [f'LOWER("customer_name") LIKE LOWER(\'%{w}%\')' for w in name_words]
            )
            conditions.append(f'({word_conditions})')
            partial_match_used = False
        print(f"DEBUG: Final customer SQL condition for: '{extracted_customer_name}'")

    #=================================== end of dynamic customer name ==================

    #=================================== end of dynamic customer name ==================
    #============================= for code description column============================
    # ====== DYNAMIC description filter ======
    # RULE: Only add a WHERE filter on "Description" column when the user explicitly
    # specifies a description VALUE (e.g. "description is reminder 1", "desc: outstanding letter").
    # Plain grouping words like "description wise", "by description", or bare "reminder"
    # do NOT add a filter — they just affect GROUP BY (handled by group_by_description_type).
    extracted_description = None

    # Only EXPLICIT value-assignment triggers fire a WHERE Description LIKE filter.
    # Broad triggers like 'description ' and 'desc ' are intentionally REMOVED here
    # to prevent grouping words from accidentally triggering a filter.
    desc_trigger_phrases = [
        'description is ', 'description= ', 'description : ', 'description: ',
        'desc is ', 'desc= ', 'desc : ', 'desc: ',
        'reminder type is ', 'reminder type: ',
        'letter type is ', 'letter type: ',
    ]
    noise = {
        'for', 'of', 'in', 'with', 'and', 'by', 'from', 'last', 'this',
        'next', 'monthly', 'quarterly', 'yearly', 'separately', 'wise',
        'project', 'company', 'customer', 'wave', 'city', 'estate',
        'till', 'until', 'upto', 'up', 'before', 'after', 'on', 'to',
        'count', 'total', 'sum', 'report', 'data', 'show', 'get', 'find',
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december',
        'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug',
        'sep', 'sept', 'oct', 'nov', 'dec',
    }

    raw_desc = None
    for phrase in desc_trigger_phrases:
        idx = user_query.lower().find(phrase)
        if idx != -1:
            after = user_query[idx + len(phrase):].strip().strip('"\'')
            if after and after[0:1].replace('-','').replace('_','').isalnum():
                raw_desc = after
                break

    if not raw_desc:
        # Support "desc <value>" / "description <value>" only when it clearly looks like a filter.
        # Example: "show total reminder of desc reminder1"
        ql = user_query.lower()
        reminder_count_context = (
            ('total reminder' in ql)
            or ('reminder count' in ql)
            or ('count of reminder' in ql)
            or ('number of reminder' in ql)
            or bool(_re.search(r'\b(of|for)\s+(desc|description)\b', ql))
        )
        if reminder_count_context:
            m = _re.search(r'\b(?:desc|description)\b\s+(?P<rest>.+)$', user_query, flags=_re.IGNORECASE)
            if m:
                after = (m.group('rest') or '').strip().strip('"\'')
                if after:
                    # Do not treat grouping hints as filter values (e.g. "description wise").
                    first = after.split()[0].lower()
                    if first not in {'wise', 'type', 'by', 'per', 'each', 'group', 'column', 'field'}:
                        raw_desc = after

    if raw_desc:
        desc_words = raw_desc.split()
        clean_desc = []
        for i, w in enumerate(desc_words):
            # Stop at noise words
            if w.lower() in noise:
                break
            # Allow a digit/alphanumeric token (e.g. "1", "2a") if it immediately
            # follows a non-digit word — this keeps "Reminder 1", "Reminder 2" intact.
            # Only stop on a standalone digit if there is NO preceding word collected.
            if w[0:1].isdigit():
                if clean_desc:
                    # Include this numeric part (e.g. the "1" in "Reminder 1")
                    clean_desc.append(w)
                # Either way, stop after consuming the numeric token so we don't
                # accidentally absorb unrelated trailing words.
                break
            clean_desc.append(w)
        extracted_description = ' '.join(clean_desc).strip()
        print(f"DEBUG: Description extracted: '{extracted_description}'")

    if extracted_description and 'description' not in skip_columns:
        # Strip trailing punctuation (dots, commas, etc.)
        extracted_description = ''.join(c for c in extracted_description if c.isalnum() or c in ' -_').strip()
        # Match both literal text and "compact" (space-less) text so "reminder1" matches "REMINDER 1".
        desc_like = f'LOWER("Description") LIKE LOWER(\'%{extracted_description}%\')'
        compact = _re.sub(r'\s+', '', extracted_description.lower())
        compact = ''.join(c for c in compact if c.isalnum() or c in '-_').strip()
        if compact:
            pattern = _re.escape(compact)
            # Avoid "reminder1" matching "reminder10".
            if compact[-1].isdigit():
                pattern += r'(?!\d)'
            desc_like = f'({desc_like} OR regexp_like(replace(lower("Description"), \' \', \'\'), \'{pattern}\'))'
        conditions.append(desc_like)
        print(f"DEBUG: Description SQL condition added → '{extracted_description}'")
    # ====== end description filter ======


    #=================================== end of code======================================
    # Check for customer names (capitalized words, but not month names)
    
    # Check for explicit company code mention (e.g., "company code 1000")
    company_code_val = None
    cc_phrase = 'company code'
    cc_idx = query_lower.find(cc_phrase)
    if cc_idx != -1:
        after_cc = query_lower[cc_idx + len(cc_phrase):].strip().strip('"\'')
        tokens = after_cc.split()
        if tokens and tokens[0].isdigit():
            company_code_val = tokens[0]
    if company_code_val and not any('company_code' in c for c in conditions):
        conditions.append(f'"company_code" = {company_code_val}')
    
    # Return conditions AND compare flags for vs/and queries
    # compare_sales_groups=True means: multiple sales groups with vs/and → force GROUP BY sales_group
    # compare_companies=True    means: multiple companies with vs/and    → force GROUP BY company
    return conditions, {
        "compare_sales_groups": compare_sales_groups,
        "compare_companies": compare_companies,
        "matched_sales_groups": matched_sales_groups if compare_sales_groups else [],
        "matched_companies": matched_companies if compare_companies else [],
    }


# --------------------
# Helper: Determine time grouping
# --------------------
def determine_time_grouping(user_query: str):
    query_lower = user_query.lower()

    if any(kw in query_lower for kw in [
        "year on year", "yoy", "yearly", "year wise", "year by year", "year over year", "annually", "annual",
        "by year", "each year", "per year", "yearwise"
    ]):
        group_expr = """CASE 
                            WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) >= 4 
                                THEN CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR) 
                                    || '-' || CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) + 1 AS VARCHAR)
                            ELSE CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1 AS VARCHAR) 
                                || '-' || CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR)
                        END"""
        return ('year', group_expr, 'fiscal_year')

    elif any(kw in query_lower for kw in [
        "quarter on quarter", "qoq", "quarterly", "quarter wise", "quarter by quarter",
        "quarter over quarter", "by quarter", "each quarter", "per quarter"
    ]):
        group_expr = """CASE 
                            WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) >= 4 
                                THEN CONCAT(
                                    'FY', 
                                    CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR),
                                    '-Q',
                                    CAST(FLOOR((MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 4) / 3) + 1 AS VARCHAR)
                                )
                            ELSE CONCAT(
                                'FY', 
                                CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1 AS VARCHAR),
                                '-Q',
                                CAST(FLOOR((MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) + 8) / 3) + 1 AS VARCHAR)
                            )
                        END"""
        return ('quarter', group_expr, 'fiscal_quarter')

    elif any(kw in query_lower for kw in [
        "month on month", "mom", "monthly", "month wise", "month by month",
        "month over month", "by month", "each month", "per month", "monthwise"
    ]):
        group_expr = "date_format(TRY(date_parse(CAST(\"DATE\" AS VARCHAR), '%Y%m%d')), '%M %Y')"
        return ('month', group_expr, 'month')

    elif any(kw in query_lower for kw in [
        "week on week", "wow", "weekly", "week wise", "week by week",
        "week over week", "by week", "each week", "per week", "weekwise"
    ]):
        # ISO week: 'YYYY-Www' e.g. '2025-W14'
        group_expr = """CONCAT(
                            CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR),
                            '-W',
                            LPAD(CAST(WEEK(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR), 2, '0')
                        )"""
        return ('week', group_expr, 'week')

    return (None, None, None)


# --------------------
# Helper: Extract quarters from query
# --------------------
def extract_quarters_from_query(user_query: str) -> list:
    query_lower = user_query.lower()
    quarters = []
    # Find all 'qN' mentions (q1, q2, q3, q4)
    words = query_lower.split()
    for word in words:
        clean = word.strip('.,;:!?()"\'')
        if len(clean) == 2 and clean[0] == 'q' and clean[1] in '1234':
            quarters.append(clean[1])
    # Also check for spelled out quarters
    quarter_words = {
        'first quarter': 1, 'second quarter': 2,
        'third quarter': 3, 'fourth quarter': 4,
        'quarter 1': 1, 'quarter 2': 2, 'quarter 3': 3, 'quarter 4': 4
    }
    for word, num in quarter_words.items():
        if word in query_lower:
            quarters.append(str(num))
    unique_quarters = list(set([int(q) for q in quarters]))
    return sorted(unique_quarters)


# --------------------
# Helper: Extract year from query
# --------------------
def extract_year_from_query(user_query: str):
    """Extract year from the query if specified. Returns year as int or None."""
    # Check for 'for/in/of YYYY' pattern first
    for prefix in ['for ', 'in ', 'of ']:
        idx = user_query.lower().find(prefix)
        while idx != -1:
            after = user_query[idx + len(prefix):].strip()
            tokens = after.split()
            if tokens:
                candidate = tokens[0].strip('.,;:!?()"\'')
                if len(candidate) == 4 and candidate.isdigit() and candidate.startswith('20'):
                    return int(candidate)
            idx = user_query.lower().find(prefix, idx + 1)
    # Fallback: find any 4-digit year starting with 20
    tokens = user_query.split()
    for token in tokens:
        candidate = token.strip('.,;:!?()"\'')
        if len(candidate) == 4 and candidate.isdigit() and candidate.startswith('20'):
            return int(candidate)
    return None


# --------------------
# Helper: Pure Python Date Parsing Utilities (no regex)
# --------------------
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
}
MONTH_NAMES = list(MONTH_MAP.keys())
ORDINAL_SUFFIXES = ['st', 'nd', 'rd', 'th']
RANGE_SEPARATORS = [' to ', ' - ', ' – ', ' — ', '-', '–', '—']

def _strip_ordinal(token: str) -> str:
    """Remove ordinal suffix from day tokens like '5th', '3rd' → '5', '3'."""
    for suf in ORDINAL_SUFFIXES:
        if token.endswith(suf) and token[:-len(suf)].isdigit():
            return token[:-len(suf)]
    return token

def _tokenize_query(text: str) -> list:
    """Split text into lowercase tokens, preserving punctuation-stripped words."""
    tokens = []
    for tok in text.lower().replace('–', ' ').replace('—', ' ').split():
        tokens.append(tok.strip('.,;:!?()"\''))
    return tokens

def _find_year_in_token(token: str):
    """Return int year if token is a 4-digit year starting with 20, else None."""
    c = token.strip('.,;:!?()"\'')
    if len(c) == 4 and c.isdigit() and c.startswith('20'):
        return int(c)
    return None

def _parse_day_month_range(query_lower: str, current_fy_start: int):
    """
    Parse patterns: [day] month [year] to [day] month [year]
    Returns (start_day, start_month_name, start_year_str, end_day, end_month_name, end_year_str) or None
    """
    tokens = _tokenize_query(query_lower)
    # Find range separator positions
    # We join tokens to find ' to ' or '-' between date parts
    text = ' ' + ' '.join(tokens) + ' '
    
    for sep in [' to ', ' - ']:
        idx = text.find(sep)
        if idx == -1:
            continue
        left = text[:idx].strip().split()
        right = text[idx + len(sep):].strip().split()
        
        # Try to parse left side: [day] month [year]
        def parse_side(parts):
            day_str = None
            month_name = None
            year_str = None
            for p in parts:
                clean = _strip_ordinal(p)
                if clean.isdigit() and 1 <= int(clean) <= 31 and day_str is None and month_name is None:
                    day_str = clean
                elif clean in MONTH_MAP:
                    month_name = clean
                elif _find_year_in_token(clean):
                    year_str = clean
            return day_str, month_name, year_str

        # Take last few tokens of left (at most 4)
        start_day, start_month_name, start_year_str = parse_side(left[-4:])
        end_day, end_month_name, end_year_str = parse_side(right[:4])
        
        if start_day and start_month_name and end_day and end_month_name:
            return start_day, start_month_name, start_year_str, end_day, end_month_name, end_year_str
    return None

def _parse_month_range(query_lower: str, current_fy_start: int):
    """
    Parse patterns: month [year] to month [year]
    Returns (start_month_name, start_year_str, end_month_name, end_year_str) or None
    """
    text = ' ' + query_lower + ' '
    for sep in [' to ', ' - ']:
        idx = text.find(sep)
        if idx == -1:
            continue
        left = text[:idx].strip().split()
        right = text[idx + len(sep):].strip().split()
        
        def find_month_year(parts):
            month_name = None
            year_str = None
            for p in parts:
                clean = p.strip('.,;:!?()"\'')
                if clean in MONTH_MAP:
                    month_name = clean
                elif _find_year_in_token(clean):
                    year_str = clean
            return month_name, year_str

        start_month_name, start_year_str = find_month_year(left[-3:])
        end_month_name, end_year_str = find_month_year(right[:3])
        
        if start_month_name and end_month_name:
            return start_month_name, start_year_str, end_month_name, end_year_str
    return None

def _parse_year_range(query_lower: str):
    """
    Parse patterns: YYYY to YYYY
    Returns (start_fy, end_fy) as ints or None
    """
    text = ' ' + query_lower + ' '
    for sep in [' to ', ' - ']:
        idx = text.find(sep)
        if idx == -1:
            continue
        left_tokens = text[:idx].strip().split()
        right_tokens = text[idx + len(sep):].strip().split()
        
        left_year = None
        right_year = None
        for t in reversed(left_tokens[-2:]):
            y = _find_year_in_token(t)
            if y:
                left_year = y
                break
        for t in right_tokens[:2]:
            y = _find_year_in_token(t)
            if y:
                right_year = y
                break
        if left_year and right_year:
            return left_year, right_year
    return None

def _parse_iso_date(query_lower: str):
    """Parse YYYY-MM-DD patterns. Returns (year, month, day) or None."""
    tokens = query_lower.split()
    for tok in tokens:
        clean = tok.strip('.,;:!?()"\'')
        parts = clean.split('-')
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            if 2000 <= y <= 2099 and 1 <= m <= 12 and 1 <= d <= 31:
                try:
                    datetime.date(y, m, d)
                    return y, m, d
                except ValueError:
                    pass
    return None

def _parse_dmy_date(query_lower: str):
    """Parse DD/MM/YYYY or DD-MM-YYYY patterns. Returns (year, month, day) or None."""
    tokens = query_lower.split()
    for tok in tokens:
        clean = tok.strip('.,;:!?()"\'')
        for sep in ['/', '-']:
            if sep in clean:
                parts = clean.split(sep)
                if len(parts) == 3 and all(p.isdigit() for p in parts):
                    d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
                    if 2000 <= y <= 2099 and 1 <= m <= 12 and 1 <= d <= 31:
                        try:
                            datetime.date(y, m, d)
                            return y, m, d
                        except ValueError:
                            pass
    return None

def _parse_specific_date(query_lower: str, current_fy_start: int):
    """
    Parse patterns: 5 april [year], april 5 [year], 5th april [year]
    Returns (month_num, day_num, year) or None
    """
    tokens = _tokenize_query(query_lower)
    
    for i, tok in enumerate(tokens):
        day_clean = _strip_ordinal(tok)
        if day_clean.isdigit() and 1 <= int(day_clean) <= 31:
            # Check next token for month
            if i + 1 < len(tokens) and tokens[i+1] in MONTH_MAP:
                day_num = int(day_clean)
                month_name = tokens[i+1]
                month_num = MONTH_MAP[month_name]
                # Check for year
                year_str = None
                if i + 2 < len(tokens):
                    y = _find_year_in_token(tokens[i+2])
                    if y:
                        year_str = str(y)
                    elif tokens[i+2] == 'of' and i + 3 < len(tokens):
                        y = _find_year_in_token(tokens[i+3])
                        if y:
                            year_str = str(y)
                year = int(year_str) if year_str else (current_fy_start if month_num >= 4 else current_fy_start + 1)
                try:
                    datetime.date(year, month_num, day_num)
                    return month_num, day_num, year
                except ValueError:
                    pass
        
        if tok in MONTH_MAP:
            # Check next token for day
            if i + 1 < len(tokens):
                day_clean2 = _strip_ordinal(tokens[i+1])
                if day_clean2.isdigit() and 1 <= int(day_clean2) <= 31:
                    month_num = MONTH_MAP[tok]
                    day_num = int(day_clean2)
                    year_str = None
                    if i + 2 < len(tokens):
                        y = _find_year_in_token(tokens[i+2])
                        if y:
                            year_str = str(y)
                        elif tokens[i+2] == 'of' and i + 3 < len(tokens):
                            y = _find_year_in_token(tokens[i+3])
                            if y:
                                year_str = str(y)
                    year = int(year_str) if year_str else (current_fy_start if month_num >= 4 else current_fy_start + 1)
                    try:
                        datetime.date(year, month_num, day_num)
                        return month_num, day_num, year
                    except ValueError:
                        pass
    return None

def _parse_date_range_from_to(query_lower: str, current_fy_start: int):
    """
    Parse 'from DAY MONTH to DAY MONTH [YEAR]' and 'from MONTH DAY to MONTH DAY' patterns.
    Returns (start_month_num, start_day, start_year, end_month_num, end_day, end_year) or None
    """
    text = query_lower
    from_idx = text.find('from ')
    if from_idx == -1:
        return None
    after_from = text[from_idx + 5:]
    to_idx = after_from.find(' to ')
    if to_idx == -1:
        return None
    left_str = after_from[:to_idx].strip()
    right_str = after_from[to_idx + 4:].strip()

    def parse_day_month(s):
        tokens = _tokenize_query(s)
        day = None
        month_name = None
        year_str = None
        for tok in tokens:
            clean = _strip_ordinal(tok)
            if clean.isdigit() and 1 <= int(clean) <= 31 and day is None and month_name is None:
                day = int(clean)
            elif tok in MONTH_MAP and month_name is None:
                month_name = tok
            elif clean.isdigit() and 1 <= int(clean) <= 31 and month_name is not None and day is None:
                day = int(clean)
            elif _find_year_in_token(tok):
                year_str = tok
        return day, month_name, year_str

    sd, smn, sy = parse_day_month(left_str)
    ed, emn, ey = parse_day_month(right_str)
    if sd and smn and ed and emn:
        sm = MONTH_MAP[smn]
        em = MONTH_MAP[emn]
        start_year = int(sy) if sy else (current_fy_start if sm >= 4 else current_fy_start + 1)
        end_year = int(ey) if ey else (current_fy_start if em >= 4 else current_fy_start + 1)
        if em < sm:
            end_year = start_year + 1
        try:
            datetime.date(start_year, sm, sd)
            datetime.date(end_year, em, ed)
            return sm, sd, start_year, em, ed, end_year
        except ValueError:
            pass
    return None

def _find_years_with_connector(query_lower: str, connectors=(' and ', ' vs ')):
    """Find multiple 4-digit years connected by 'and' or 'vs'. Returns list of year strings."""
    years = []
    for conn in connectors:
        text = query_lower
        while conn in text:
            idx = text.find(conn)
            left_tokens = text[:idx].strip().split()
            right_after = text[idx + len(conn):].strip()
            right_tokens = right_after.split()
            # Check left end for a year
            for t in reversed(left_tokens[-2:]):
                y = _find_year_in_token(t)
                if y and str(y) not in years:
                    years.append(str(y))
                    break
            # Check right start for a year
            if right_tokens:
                y = _find_year_in_token(right_tokens[0])
                if y and str(y) not in years:
                    years.append(str(y))
            # Advance past this connector
            text = text[idx + len(conn):]
    return years

def _find_months_with_and(query_lower: str, current_fy_start: int):
    """
    Find multiple month (with optional year) connected by 'and'.
    Returns list of (month_name, year_str_or_None).
    """
    month_year_list = []
    text = query_lower
    while ' and ' in text:
        idx = text.find(' and ')
        left_tokens = text[:idx].strip().split()
        right_after = text[idx + 5:].strip()
        right_tokens = right_after.split()
        
        # Check end of left for month [year]
        month_left = None
        year_left = None
        for t in reversed(left_tokens[-3:]):
            if t in MONTH_MAP and not month_left:
                month_left = t
            elif _find_year_in_token(t) and not year_left:
                year_left = t
        if month_left:
            entry = (month_left, year_left)
            if entry not in month_year_list:
                month_year_list.append(entry)
        
        # Check start of right for month [year]
        month_right = None
        year_right = None
        for t in right_tokens[:3]:
            if t in MONTH_MAP and not month_right:
                month_right = t
            elif _find_year_in_token(t) and not year_right:
                year_right = t
        if month_right:
            entry = (month_right, year_right)
            if entry not in month_year_list:
                month_year_list.append(entry)
        
        text = right_after
    return month_year_list

def _check_is_month_range(query_lower: str) -> bool:
    """Check if query contains a pattern like 'month to month' or 'month - month'."""
    tokens = _tokenize_query(query_lower)
    for i, tok in enumerate(tokens):
        if tok in MONTH_MAP:
            # Check if next non-empty token is 'to' or '-' and token after is also a month
            for j in range(i+1, min(i+4, len(tokens))):
                if tokens[j] in ('to', '-'):
                    for k in range(j+1, min(j+4, len(tokens))):
                        if tokens[k] in MONTH_MAP:
                            return True
    return False

def _find_single_month(query_lower: str):
    """
    Find a single month (with optional year) in patterns like 'for april', 'in april', 'april YYYY'.
    Returns (month_name, year_str_or_None) or None.
    Does not trigger if there's a range.
    """
    tokens = _tokenize_query(query_lower)
    # Look for 'for/in MONTH [YEAR]' pattern
    for i, tok in enumerate(tokens):
        if tok in ('for', 'in') and i + 1 < len(tokens):
            if tokens[i+1] in MONTH_MAP:
                month_name = tokens[i+1]
                year_str = None
                if i + 2 < len(tokens):
                    y = _find_year_in_token(tokens[i+2])
                    if y:
                        year_str = str(y)
                    elif tokens[i+2] == 'of' and i + 3 < len(tokens):
                        y = _find_year_in_token(tokens[i+3])
                        if y:
                            year_str = str(y)
                return month_name, year_str
    # Bare month at end
    for i in range(len(tokens) - 1, -1, -1):
        tok = tokens[i]
        if tok in MONTH_MAP:
            year_str = None
            if i + 1 < len(tokens):
                y = _find_year_in_token(tokens[i+1])
                if y:
                    year_str = str(y)
            return tok, year_str
    return None

def _find_quarter_tokens(query_lower: str):
    """Find all 'qN' tokens and return list of quarter numbers."""
    tokens = _tokenize_query(query_lower)
    quarters = []
    for tok in tokens:
        if len(tok) == 2 and tok[0] == 'q' and tok[1] in '1234':
            quarters.append(int(tok[1]))
    return list(set(quarters))

def _find_last_n_pattern(query_lower: str, unit: str, words_map: dict):
    """
    Parse 'last N unit(s)' or 'last unit' patterns. Returns N as int or None.
    unit is like 'month', 'quarter', 'year', 'day'.
    words_map maps word-strings to numbers, e.g. {'one': 1, 'two': 2, ...}
    """
    tokens = _tokenize_query(query_lower)
    unit_variants = [unit, unit + 's']
    for i, tok in enumerate(tokens):
        if tok in ('last', 'previous'):
            if i + 1 < len(tokens):
                next_tok = tokens[i+1]
                # 'last N units'
                if next_tok.isdigit():
                    if i + 2 < len(tokens) and tokens[i+2] in unit_variants:
                        return int(next_tok)
                elif next_tok in words_map:
                    if i + 2 < len(tokens) and tokens[i+2] in unit_variants:
                        return words_map[next_tok]
                # 'last unit' (no number)
                elif next_tok in unit_variants:
                    return None  # means 1 (singular last)
    return False  # not found at all

def _parse_till_date(query_lower: str, current_fy_start: int, current_date):
    """
    Parse 'till date', '[month] till date', 'till [month]' patterns.
    Returns (start_date_str, end_date_str) or None.
    """
    month_map = MONTH_MAP
    
    # Standalone 'till date' with no month
    if 'till date' in query_lower or 'till today' in query_lower or 'till current date' in query_lower:
        tokens = _tokenize_query(query_lower)
        # Find month before 'till'
        till_idx = None
        for i, tok in enumerate(tokens):
            if tok == 'till':
                till_idx = i
                break
        if till_idx is not None:
            # Look for month in tokens before 'till'
            month_name = None
            year_str = None
            day_str = None
            for j in range(till_idx - 1, max(till_idx - 5, -1), -1):
                t = tokens[j]
                if t in month_map and not month_name:
                    month_name = t
                elif _find_year_in_token(t) and not year_str:
                    year_str = t
                elif _strip_ordinal(t).isdigit() and not day_str:
                    day_str = _strip_ordinal(t)
            if month_name:
                month_num = month_map[month_name]
                year = int(year_str) if year_str else (current_fy_start if month_num >= 4 else current_fy_start + 1)
                day = int(day_str) if day_str else 1
                start_date_str = f'{year}{month_num:02d}{day:02d}'
                end_date_str = current_date.strftime('%Y%m%d')
                return start_date_str, end_date_str
        # Pure standalone 'till date'
        month_num = current_date.month
        base_year = current_date.year
        start_date_str = f'{base_year}{month_num:02d}01'
        end_date_str = current_date.strftime('%Y%m%d')
        return start_date_str, end_date_str
    
    # 'till [month]'
    if 'till ' in query_lower:
        idx = query_lower.find('till ')
        after = query_lower[idx + 5:].strip()
        tokens_after = _tokenize_query(after)
        day_str = None
        month_name = None
        year_str = None
        for tok in tokens_after[:4]:
            clean = _strip_ordinal(tok)
            if clean.isdigit() and 1 <= int(clean) <= 31 and not day_str and not month_name:
                day_str = clean
            elif tok in month_map and not month_name:
                month_name = tok
            elif _find_year_in_token(tok) and not year_str:
                year_str = tok
        if month_name:
            month_num = month_map[month_name]
            base_year = int(year_str) if year_str else (current_fy_start if month_num >= 4 else current_fy_start + 1)
            fy_year = base_year if month_num >= 4 else base_year - 1
            start_date_str = f'{fy_year}0401'
            day = int(day_str) if day_str else (
                31 if month_num == 12 else
                calendar.monthrange(base_year, month_num)[1] if month_num == 2 else
                (datetime.date(base_year, month_num + 1, 1) - datetime.timedelta(days=1)).day
            )
            end_year = base_year if month_num >= 4 else base_year + 1
            end_date_str = f'{end_year}{month_num:02d}{day:02d}'
            return start_date_str, end_date_str
    return None

# --------------------
# Natural Language to SQL Conversion (FIXED VERSION)
# --------------------
def nl_to_sql(user_query: str) -> str:
    try:
        columns = get_columns_from_presto()
        
        # Determine the financial year based on the current date
        current_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).date()
        current_year = current_date.year
        if current_date.month < 4:
            current_fy_start = current_year - 1
        else:
            current_fy_start = current_year

        # Define query_lower early so all blocks below can use it
        # Define query_lower early so all blocks below can use it
        query_lower = user_query.lower()

        # Normalize short month names to full names
        short_month_map = {
            'jan': 'january', 'feb': 'february', 'mar': 'march',
            'apr': 'april', 'jun': 'june', 'jul': 'july',
            'aug': 'august', 'sep': 'september', 'sept': 'september',
            'oct': 'october', 'nov': 'november', 'dec': 'december'
        }
        # Replace whole-word short month names
        def replace_short_months(text: str) -> str:
            words = text.split()
            result = []
            for w in words:
                clean = w.strip('.,;:!?()"\'')
                if clean.lower() in short_month_map:
                    # Preserve surrounding punctuation
                    prefix = w[:len(w) - len(w.lstrip('.,;:!?()"\''))]
                    suffix = w[len(w.rstrip('.,;:!?()"\'')):]
                    result.append(prefix + short_month_map[clean.lower()] + suffix)
                else:
                    result.append(w)
            return ' '.join(result)
        query_lower = replace_short_months(query_lower)
        user_query = replace_short_months(user_query)

        # Extract metrics from the query
        metrics = extract_metrics_from_query(user_query)
        
        # If no metrics found, default to Outstanding
        if not metrics:
            metrics = [("Outstanding", "Outstanding")]
        
        # Determine time-based grouping
        # Determine time-based grouping
        grouping_type, group_expr, group_alias = determine_time_grouping(user_query)

        # ── "separately" keyword → auto-detect best grouping dimension ──────
        # Uses whole-word SG/company matching so named products set the dimension
        # even when keywords like "sales group" or "product" are absent.
        if 'separately' in query_lower and not grouping_type:
            _sg_map_sep, _co_map_sep = get_sg_and_company_mappings()
            _sg_sep = _match_sg_whole_word(query_lower, _sg_map_sep)
            _co_sep = _match_co_whole_word(query_lower, _co_map_sep)

            if _sg_sep:
                # Named product(s) → group by sales_group
                grouping_type = 'sales_group'
                group_expr    = get_sales_group_expr()
                group_alias   = 'sales_group'
            elif _co_sep:
                grouping_type = 'company'
                group_expr    = get_company_name_expr()
                group_alias   = 'company_name'
            elif any(w in query_lower for w in ['year', 'yearly', 'fy', 'annual', 'last year', 'this year', 'years']):
                group_expr = """CASE 
                    WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) >= 4 
                        THEN CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR) 
                            || '-' || CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) + 1 AS VARCHAR)
                    ELSE CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1 AS VARCHAR) 
                        || '-' || CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR)
                END"""
                grouping_type = 'year'
                group_alias   = 'fiscal_year'
            elif any(w in query_lower for w in ['quarter', 'quarterly', 'q1', 'q2', 'q3', 'q4']):
                group_expr = """CASE 
                    WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) >= 4 
                        THEN CONCAT('FY', CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR),
                            '-Q', CAST(FLOOR((MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 4) / 3) + 1 AS VARCHAR))
                    ELSE CONCAT('FY', CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1 AS VARCHAR),
                            '-Q', CAST(FLOOR((MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) + 8) / 3) + 1 AS VARCHAR))
                END"""
                grouping_type = 'quarter'
                group_alias   = 'fiscal_quarter'
            elif any(w in query_lower for w in ['month', 'monthly', 'last month', 'this month']):
                group_expr    = "date_format(TRY(date_parse(CAST(\"DATE\" AS VARCHAR), '%Y%m%d')), '%M %Y')"
                grouping_type = 'month'
                group_alias   = 'month'
            elif any(w in query_lower for w in ['week', 'weekly', 'last week', 'this week']):
                group_expr = """CONCAT(
                    CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR),
                    '-W',
                    LPAD(CAST(WEEK(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR), 2, '0')
                )"""
                grouping_type = 'week'
                group_alias   = 'week'
            elif any(w in query_lower for w in ['sales group', 'product']):
                grouping_type = 'sales_group'
                group_expr    = get_sales_group_expr()
                group_alias   = 'sales_group'
            elif any(w in query_lower for w in ['company', 'project']):
                grouping_type = 'company'
                group_expr    = get_company_name_expr()
                group_alias   = 'company_name'
            elif any(w in query_lower for w in ['customer', 'customer name']):
                grouping_type = 'customer'
                group_expr    = '"customer_name"'
                group_alias   = 'customer_name'
            elif any(w in query_lower for w in ['description', 'reminder type']):
                grouping_type = 'description'
                group_expr    = get_description_expr()
                group_alias   = 'description'
            elif any(w in query_lower for w in ['mail sent', 'email sent']):
                grouping_type = 'mail_sent'
                group_expr    = '"Mail_Sent"'
                group_alias   = 'mail_sent'
            elif any(w in query_lower for w in ['dunning level']):
                grouping_type = 'dunning_level'
                group_expr    = '"Dunning_Level"'
                group_alias   = 'dunning_level'
            elif any(w in query_lower for w in ['booking no', 'booking number']):
                grouping_type = 'booking_no'
                group_expr    = '"Booking_No"'
                group_alias   = 'booking_no'
            elif any(w in query_lower for w in ['user id']):
                grouping_type = 'user_id'
                group_expr    = '"User_Id"'
                group_alias   = 'user_id'

            print(f"DEBUG: 'separately' keyword → grouping forced to: {grouping_type}/{group_alias}")
        
        # Extract year from query if specified
        specified_year = extract_year_from_query(user_query)
        
        # Build date condition
        date_condition = None
        
        print(f"\nDEBUG: Processing query: '{user_query}'")
        print(f"DEBUG: Query lowercase: '{query_lower}'")
        print(f"DEBUG: Specified year from query: {specified_year}")
        print(f"DEBUG: Grouping type: {grouping_type}")
        
        # This must come BEFORE flexible range
        # ================================================================
        if not date_condition:
            year_context = None
            if "last year" in query_lower:
                year_context = current_fy_start - 1
                print(f"DEBUG: 'last year' detected → using FY {year_context}-{year_context+1}")
            else:
                # Look for 'fy YYYY' or 'fy2024' patterns
                year_context = None
                fy_idx = query_lower.find('fy')
                if fy_idx != -1:
                    after_fy = query_lower[fy_idx + 2:].strip()
                    tokens = after_fy.split()
                    if tokens:
                        candidate = tokens[0].strip('.,;:!?()"\'')
                        if len(candidate) == 4 and candidate.isdigit() and candidate.startswith('20'):
                            year_context = int(candidate)
                            print(f"DEBUG: 'fy {year_context}' detected → using FY {year_context}-{year_context+1}")
            
            if year_context is not None:
                # Look for month range in the query
                month_names_list = ['january', 'february', 'march', 'april', 'may', 'june',
                                    'july', 'august', 'september', 'october', 'november', 'december']
                mr_start = None
                mr_end = None
                range_seps = [' to ', ' - ', ' – ', ' — ']
                for sep in range_seps:
                    idx = query_lower.find(sep)
                    if idx != -1:
                        # Find month before separator
                        before = query_lower[:idx]
                        after = query_lower[idx + len(sep):]
                        for mn in month_names_list:
                            if before.endswith(mn) or (' ' + mn) in before:
                                mr_start = mn
                            if after.startswith(mn) or after.split()[0:1] == [mn] if after.split() else False:
                                mr_end = mn
                        if mr_start and mr_end:
                            break
                # Simpler approach: scan for two month names with a separator between them
                if not (mr_start and mr_end):
                    found_months = []
                    tokens = query_lower.replace('-', ' ').replace('–', ' ').replace('—', ' ').split()
                    for tok in tokens:
                        clean = tok.strip('.,;:!?()"\'')
                        if clean in month_names_list:
                            found_months.append(clean)
                    if len(found_months) >= 2:
                        mr_start = found_months[0]
                        mr_end = found_months[1]

                if mr_start and mr_end:
                    start_month_name = mr_start.lower()
                    end_month_name = mr_end.lower()
                    
                    month_map = {
                        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
                    }
                    
                    start_month = month_map[start_month_name]
                    end_month = month_map[end_month_name]
                    
                    fy_start_year = year_context
                    start_date = f'{fy_start_year}0401'
                    
                    end_year = fy_start_year if end_month >= 4 else fy_start_year + 1
                    
                    if end_month == 12:
                        last_day = 31
                    elif end_month == 2:
                        last_day = calendar.monthrange(end_year, end_month)[1]
                    else:
                        last_day = (datetime.date(end_year, end_month + 1, 1) - datetime.timedelta(days=1)).day
                    
                    end_date = f'{end_year}{end_month:02d}{last_day:02d}'
                    
                    date_condition = f"CAST(\"DATE\" AS VARCHAR) >= '{start_date}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date}'"
                    print(f"DEBUG: FY/LAST YEAR + MONTH RANGE SUCCESS: {start_date} to {end_date}")
        # 1. DATE RANGE DETECTION FIRST (Highest Priority)
        #========================== new code for the date month range=================
        if not date_condition:
            # Pattern: [day] month [year] to [day] month [year]
            dmr = _parse_day_month_range(query_lower, current_fy_start)
            if dmr:
                start_day_s, start_month_name, start_year_str, end_day_s, end_month_name, end_year_str = dmr
                start_month = MONTH_MAP[start_month_name]
                end_month = MONTH_MAP[end_month_name]
                start_day = int(start_day_s)
                end_day = int(end_day_s)
                base_year = current_fy_start
                start_year = int(start_year_str) if start_year_str else (base_year if start_month >= 4 else base_year + 1)
                end_year = int(end_year_str) if end_year_str else (start_year + 1 if end_month < start_month else start_year)
                start_date_str = f'{start_year}{start_month:02d}{start_day:02d}'
                end_date_str = f'{end_year}{end_month:02d}{end_day:02d}'
                date_condition = f"CAST(\"DATE\" AS VARCHAR) >= '{start_date_str}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date_str}'"
                print(f"DEBUG: Day-month range applied: {start_date_str} to {end_date_str}")
        #============================ end of the date month range==========================
        # ===================================================================
        if not date_condition:
            # Pattern: month [year] to month [year]
            mr = _parse_month_range(query_lower, current_fy_start)
            if mr:
                start_month_name, start_year_str, end_month_name, end_year_str = mr
                start_month = MONTH_MAP[start_month_name]
                end_month = MONTH_MAP[end_month_name]
                if start_year_str and end_year_str:
                    start_year = int(start_year_str)
                    end_year = int(end_year_str)
                elif start_year_str:
                    start_year = int(start_year_str)
                    end_year = start_year + 1 if end_month < start_month else start_year
                elif end_year_str:
                    end_year = int(end_year_str)
                    start_year = end_year - 1 if end_month < start_month else end_year
                else:
                    base_year = current_fy_start
                    start_year = base_year if start_month >= 4 else base_year + 1
                    end_year = base_year if end_month >= 4 else base_year + 1
                    if end_month < start_month:
                        end_year += 1
                start_date_str = f'{start_year}{start_month:02d}01'
                if end_month == 12:
                    last_day = 31
                elif end_month == 2:
                    last_day = calendar.monthrange(end_year, end_month)[1]
                else:
                    last_day = (datetime.date(end_year, end_month + 1, 1) - datetime.timedelta(days=1)).day
                end_date_str = f'{end_year}{end_month:02d}{last_day:02d}'
                date_condition = f"CAST(\"DATE\" AS VARCHAR) >= '{start_date_str}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date_str}'"
                print(f"DEBUG: Multi-year month range applied: {start_date_str} to {end_date_str}")
        
        # ===================================================================
        # YEAR TO YEAR RANGE (e.g., "2020 to 2021" or "from 2020 to 2022")
        # ===================================================================
        if not date_condition:
            yr = _parse_year_range(query_lower)
            if yr:
                start_fy, end_fy = yr
                start_date_str = f'{start_fy}0401'
                end_date_str = f'{end_fy + 1}0331'
                date_condition = f"CAST(\"DATE\" AS VARCHAR) >= '{start_date_str}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date_str}'"
                print(f"DEBUG: Year range applied: FY {start_fy}-{start_fy+1} to FY {end_fy}-{end_fy+1}")

        if not date_condition:
            # ----------------------------------------
            # Pattern: optional day + month + optional year (multiple entries)
            # ----------------------------------------
            entries = []
            tokens = _tokenize_query(query_lower)
            i = 0
            while i < len(tokens):
                tok = tokens[i]
                day_str = None
                month_name = None
                year_str = None
                # Check for [day] month [year]
                day_candidate = _strip_ordinal(tok)
                if day_candidate.isdigit() and 1 <= int(day_candidate) <= 31:
                    if i + 1 < len(tokens) and tokens[i+1] in MONTH_MAP:
                        day_str = day_candidate
                        month_name = tokens[i+1]
                        if i + 2 < len(tokens):
                            y = _find_year_in_token(tokens[i+2])
                            if y:
                                year_str = str(y)
                        i += (3 if year_str else 2)
                    else:
                        i += 1
                    if month_name:
                        entries.append((day_str, month_name, year_str))
                elif tok in MONTH_MAP:
                    month_name = tok
                    day_str = None
                    year_str = None
                    if i + 1 < len(tokens):
                        y = _find_year_in_token(tokens[i+1])
                        if y:
                            year_str = str(y)
                    i += (2 if year_str else 1)
                    entries.append((day_str, month_name, year_str))
                else:
                    i += 1
            # Remove duplicates
            seen = set()
            unique_entries = []
            for day_str, month_name, year_str in entries:
                key = (day_str or "full", month_name, year_str or "current")
                if key not in seen:
                    seen.add(key)
                    unique_entries.append((day_str, month_name, year_str))

            # ----------------------------------------
            # MULTIPLE MONTH / DAY DETECTED
            # ----------------------------------------
            if len(unique_entries) >= 2:
                print(f"DEBUG: Multiple month/day entries detected → {unique_entries}")

                date_conditions = []
                case_blocks = []

                for day_str, month_name, year_str in unique_entries:
                    month_num = MONTH_MAP[month_name]

                    # Determine year (FY logic)
                    if year_str:
                        year = int(year_str)
                    else:
                        year = current_fy_start if month_num >= 4 else current_fy_start + 1

                    if day_str:
                        day = int(day_str)
                        date_str = f"{year}{month_num:02d}{day:02d}"
                        date_conditions.append(f"CAST(\"DATE\" AS VARCHAR) = '{date_str}'")
                        label = f"{day} {month_name.capitalize()} {year}"
                        case_blocks.append(f"""
                            WHEN CAST("DATE" AS VARCHAR) = '{date_str}'
                            THEN '{label}'
                            """)
                    else:
                        start_date = f"{year}{month_num:02d}01"
                        last_day = calendar.monthrange(year, month_num)[1]
                        end_date = f"{year}{month_num:02d}{last_day:02d}"
                        date_conditions.append(f"(CAST(\"DATE\" AS VARCHAR) BETWEEN '{start_date}' AND '{end_date}')")
                        label = f"{month_name.capitalize()} {year}"
                        case_blocks.append(f"""
                            WHEN CAST("DATE" AS VARCHAR)
                            BETWEEN '{start_date}' AND '{end_date}'
                            THEN '{label}'
                            """)

                date_condition = "(" + " OR ".join(date_conditions) + ")"
                grouping_type = "custom_period"
                group_expr = "CASE\n" + "\n".join(case_blocks) + "\nEND"
                group_alias = "period"
                print(f"DEBUG: Date condition → {date_condition}")
                print(f"DEBUG: Grouping → {group_alias}")
        
        
        #================================= end of multi month logic ==============================
        #5.========================= till date================================
        if not date_condition:
            till_result = _parse_till_date(query_lower, current_fy_start, current_date)
            if till_result:
                start_date_str, end_date_str = till_result
                date_condition = f"CAST(\"DATE\" AS VARCHAR) >= '{start_date_str}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date_str}'"
                print(f"DEBUG: Till block SUCCESS: {start_date_str} to {end_date_str}")

        # ===================================================================
        # 2. Specific date / date range (only if no range found)
        # ===================================================================
        specific_date_match = None
        date_range_match = None

        if not date_condition:
            sd = _parse_specific_date(query_lower, current_fy_start)
            if sd:
                month_num, day_num, year = sd
                specific_date = f'{year}{month_num:02d}{day_num:02d}'
                date_condition = f'CAST("DATE" AS VARCHAR) = \'{specific_date}\''
                specific_date_match = True
                print(f"DEBUG: Specific date detected: month={month_num}, day={day_num}, year={year}")

        if not specific_date_match:
            # 'from DAY MONTH to DAY MONTH' range
            dfr = _parse_date_range_from_to(query_lower, current_fy_start)
            if dfr:
                sm, sd_day, sy, em, ed_day, ey = dfr
                start_date = f'{sy}{sm:02d}{sd_day:02d}'
                end_date = f'{ey}{em:02d}{ed_day:02d}'
                date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
                date_range_match = True
                print(f"DEBUG: Date range (from...to) detected: {start_date} to {end_date}")

        # Check for standard date formats: "2024-04-05", "05/04/2024", "05-04-2024"
        if not specific_date_match and not date_range_match:
            iso = _parse_iso_date(query_lower)
            if iso:
                y, m, d = iso
                specific_date = f'{y}{m:02d}{d:02d}'
                date_condition = f'CAST("DATE" AS VARCHAR) = \'{specific_date}\''
                print(f"DEBUG: ISO date detected: {y}-{m:02d}-{d:02d}")
            if not date_condition:
                dmy = _parse_dmy_date(query_lower)
                if dmy:
                    d, m, y = dmy
                    specific_date = f'{y}{m:02d}{d:02d}'
                    date_condition = f'CAST("DATE" AS VARCHAR) = \'{specific_date}\''
                    print(f"DEBUG: DD/MM/YYYY date detected: {d}/{m}/{y}")

        # Check for month range (only if no range found yet)
        if not specific_date_match and not date_range_match and not date_condition:
            if _check_is_month_range(query_lower):
                mr2 = _parse_month_range(query_lower, current_fy_start)
                if mr2:
                    smn, syr, emn, eyr = mr2
                    sm2 = MONTH_MAP[smn]
                    em2 = MONTH_MAP[emn]
                    if syr:
                        year2 = int(syr)
                        if sm2 <= em2:
                            start_date2 = f'{year2}{sm2:02d}01'
                            last_day = (datetime.date(year2, em2 + 1, 1) - datetime.timedelta(days=1)).day if em2 < 12 else 31
                            end_date2 = f'{year2}{em2:02d}{last_day:02d}'
                        else:
                            next_year = year2 + 1
                            start_date2 = f'{year2}{sm2:02d}01'
                            last_day = (datetime.date(next_year, em2 + 1, 1) - datetime.timedelta(days=1)).day if em2 < 12 else 31
                            end_date2 = f'{next_year}{em2:02d}{last_day:02d}'
                    else:
                        start_y = current_fy_start if sm2 >= 4 else current_fy_start + 1
                        end_y = current_fy_start if em2 >= 4 else current_fy_start + 1
                        start_date2 = f'{start_y}{sm2:02d}01'
                        if em2 == 12:
                            end_date2 = f'{end_y}1231'
                        elif em2 == 2:
                            last_day = calendar.monthrange(end_y, 2)[1]
                            end_date2 = f'{end_y}{em2:02d}{last_day:02d}'
                        else:
                            last_day = (datetime.date(end_y, em2 + 1, 1) - datetime.timedelta(days=1)).day
                            end_date2 = f'{end_y}{em2:02d}{last_day:02d}'
                    date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date2}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date2}\''
                    print(f"DEBUG: Month range - Final dates: {start_date2} to {end_date2}")
        
        # Check for single month: "april", "for april", "in april" with optional year
        # NEW: Multiple specific years with "and" (e.g., "2024 and 2022", "in 2023 and 2025")
        # ================================================================
        if not date_condition:
            years = _find_years_with_connector(query_lower)
            years = list(dict.fromkeys(years))  # preserve order, dedup
            if len(years) >= 2:
                print(f"DEBUG: Multiple specific years with 'and' detected: {years}")
                year_conditions = []
                for yr in years:
                    year_int = int(yr)
                    start_date = f'{year_int}0401'
                    end_date = f'{year_int + 1}0331'
                    year_conditions.append(
                        f"(CAST(\"DATE\" AS VARCHAR) >= '{start_date}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date}')"
                    )
                date_condition = " OR ".join(year_conditions)
                print(f"DEBUG: Multiple years applied: {date_condition}")
                grouping_type = "financial_year"
                group_expr = "CASE\n"
                for yr in years:
                    yr_int = int(yr)
                    start_date = f"{yr_int}0401"
                    end_date = f"{yr_int + 1}0331"
                    fy_label = f"FY {yr_int}-{str(yr_int+1)[-2:]}"
                    group_expr += f"""
                        WHEN CAST("DATE" AS VARCHAR)
                        BETWEEN '{start_date}' AND '{end_date}'
                        THEN '{fy_label}'
                    """
                group_expr += "\nEND"
                group_alias = "financial_year"
                print(f"DEBUG: Date condition → {date_condition}")
                print(f"DEBUG: Grouping forced → {group_alias}")
        
        
        if not date_condition:
            # Pattern: months connected by "and" (e.g. "april and june", "april 2024 and june")
            month_year_list = _find_months_with_and(query_lower, current_fy_start)
            if len(month_year_list) >= 2:
                print(f"DEBUG: Multiple months with 'and' (with year support) detected: {month_year_list}")
                month_conditions = []
                for month_name, year_str in month_year_list:
                    month_num = MONTH_MAP[month_name]
                    year = int(year_str) if year_str else (current_fy_start if month_num >= 4 else current_fy_start + 1)
                    start_date = f'{year}{month_num:02d}01'
                    if month_num == 12:
                        last_day = 31
                    elif month_num == 2:
                        last_day = calendar.monthrange(year, 2)[1]
                    else:
                        last_day = (datetime.date(year, month_num + 1, 1) - datetime.timedelta(days=1)).day
                    end_date = f'{year}{month_num:02d}{last_day:02d}'
                    month_conditions.append(
                        f"(CAST(\"DATE\" AS VARCHAR) >= '{start_date}' AND CAST(\"DATE\" AS VARCHAR) <= '{end_date}')"
                    )
                date_condition = " OR ".join(month_conditions)
                print(f"DEBUG: Multiple months (with year) applied: {date_condition}")
        
        # Check this AFTER month range so "april to july" doesn't match as single month
        print(f"DEBUG: Checking single month... date_condition={date_condition}")
        if not date_condition:
            is_range = _check_is_month_range(query_lower)
            print(f"DEBUG: Is range check: {bool(is_range)}")
            if not is_range:
                sm_result = _find_single_month(query_lower)
                if sm_result:
                    month_name, local_year_str = sm_result
                    print(f"DEBUG: ✅ SINGLE MONTH MATCHED!")
                    print(f"DEBUG: Single month matched! Month: {month_name}, Year: {local_year_str}")
                    month_num = MONTH_MAP[month_name]
                    year = int(local_year_str) if local_year_str else (current_fy_start if month_num >= 4 else current_fy_start + 1)
                    start_date = f'{year}{month_num:02d}01'
                    if month_num == 12:
                        last_day = 31
                    elif month_num == 2:
                        last_day = calendar.monthrange(year, 2)[1]
                    else:
                        last_day = (datetime.date(year, month_num + 1, 1) - datetime.timedelta(days=1)).day
                    end_date = f'{year}{month_num:02d}{last_day:02d}'
                    date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
                    print(f"DEBUG: Single month detected: {month_name} {year} -> {start_date} to {end_date}")
                else:
                    print(f"DEBUG: ❌ No single month match found")

        # # Check for "this month" or "current month"
        #========================== quarter logic code starts here ==========================
        # ---------------------------------------------------------
        # MULTI-QUARTER SUPPORT (Q1, Q2, Q3, Q4 / quarter 1 and 2)
        # ---------------------------------------------------------
        quarter_matches = _find_quarter_tokens(query_lower)

        if quarter_matches:
            # Detect year (if provided)
            base_year_val = None
            for tok in _tokenize_query(query_lower):
                y = _find_year_in_token(tok)
                if y:
                    base_year_val = y
                    break
            if base_year_val:
                base_year = base_year_val
            else:
                base_year = current_date.year if current_date.month >= 4 else current_date.year - 1

            quarter_conditions = []

            # Loop for all quarters user mentioned
            for q in quarter_matches:
                q = int(q)

                if q == 1:
                    start = f"{base_year}0401"
                    end   = f"{base_year}0630"
                elif q == 2:
                    start = f"{base_year}0701"
                    end   = f"{base_year}0930"
                elif q == 3:
                    start = f"{base_year}1001"
                    end   = f"{base_year}1231"
                elif q == 4:
                    start = f"{base_year + 1}0101"
                    end   = f"{base_year + 1}0331"

                quarter_conditions.append(
                    f"(CAST(\"DATE\" AS VARCHAR) >= '{start}' AND CAST(\"DATE\" AS VARCHAR) <= '{end}')"
                )

            # Combine multiple quarters using OR
            if not date_condition:
                date_condition = " OR ".join(quarter_conditions)
            else:
                date_condition = " OR ".join(quarter_conditions)


        #============================== compare quarters logic ==============================
        # if (
        #     "this quarter" in query_lower
        #     and "last quarter" in query_lower
        #     and any(word in query_lower for word in ["and", "vs", "versus", "compared", "comparison"])
        # ) and date_condition is None:

        #     now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
        #     current_date = now.date()
        #     current_month = now.month
        #     current_year = now.year

        #     # Fiscal quarter detection
        #     if 4 <= current_month <= 6:
        #         q_num = 1
        #         fy_start_year = current_year
        #     elif 7 <= current_month <= 9:
        #         q_num = 2
        #         fy_start_year = current_year
        #     elif 10 <= current_month <= 12:
        #         q_num = 3
        #         fy_start_year = current_year
        #     else:
        #         q_num = 4
        #         fy_start_year = current_year - 1

        #     q_ranges = {
        #         1: (4, 1, 6, 30),
        #         2: (7, 1, 9, 30),
        #         3: (10, 1, 12, 31),
        #         4: (1, 1, 3, 31),
        #     }

        #     # This quarter
        #     sm, sd, em, ed = q_ranges[q_num]
        #     this_year = fy_start_year if q_num != 4 else fy_start_year + 1
        #     this_start_date = datetime.date(this_year, sm, sd)
        #     this_end_temp = datetime.date(this_year, em, ed)
        #     this_end_date = min(this_end_temp, current_date)

        #     # Previous quarter
        #     prev_q = q_num - 1
        #     prev_fy = fy_start_year
        #     if prev_q < 1:
        #         prev_q = 4
        #         prev_fy -= 1
        #     psm, psd, pem, ped = q_ranges[prev_q]
        #     prev_year = prev_fy if prev_q != 4 else prev_fy + 1
        #     prev_start = datetime.date(prev_year, psm, psd)
        #     prev_end = datetime.date(prev_year, pem, ped)

        #     # Format strings
        #     ts = this_start_date.strftime('%Y%m%d')
        #     te = this_end_date.strftime('%Y%m%d')
        #     ps = prev_start.strftime('%Y%m%d')
        #     pe = prev_end.strftime('%Y%m%d')

        #     date_condition = f"""
        #     (
        #         CAST("DATE" AS VARCHAR) >= '{ts}' AND CAST("DATE" AS VARCHAR) <= '{te}'
        #         OR
        #         CAST("DATE" AS VARCHAR) >= '{ps}' AND CAST("DATE" AS VARCHAR) <= '{pe}'
        #     )
        #     """

        #     # Force grouping by quarter
        #     if not grouping_type:
        #         grouping_type = 'quarter'
        #         group_expr = """
        #         CONCAT(
        #             'Q',
        #             CASE
        #                 WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) IN (4,5,6) THEN '1'
        #                 WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) IN (7,8,9) THEN '2'
        #                 WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) IN (10,11,12) THEN '3'
        #                 ELSE '4'
        #             END,
        #             ' ',
        #             CASE
        #                 WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) <= 3
        #                 THEN CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1 AS VARCHAR)
        #                 ELSE CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR)
        #             END
        #         )
        #         """
        #         group_alias = 'quarter'
        #     print("ppppppppppppppppppppppppp", date_condition)
        #     print(f"DEBUG: Quarter comparison → Q{q_num} {fy_start_year} and previous")
        #     print(f"DEBUG: date_condition = {date_condition}")
        #=============================== end of quarter comparison ===============================
        if (
            not date_condition
            and "this year" in query_lower
            and "last year" in query_lower
            and ("and" in query_lower or "vs" in query_lower or "versus" in query_lower)
        ):

            # Current FY (this year)
            this_fy_start = current_fy_start
            this_fy_end   = current_fy_start + 1

            start_date_this = f"{this_fy_start}0401"
            end_date_this   = f"{this_fy_end}0331"

            # Previous FY (last year)
            last_fy_start = current_fy_start - 1
            last_fy_end   = current_fy_start

            start_date_last = f"{last_fy_start}0401"
            end_date_last   = f"{last_fy_end}0331"

            date_condition = f"""
            (
                (CAST("DATE" AS VARCHAR) >= '{start_date_this}' 
                AND CAST("DATE" AS VARCHAR) <= '{end_date_this}')
                OR
                (CAST("DATE" AS VARCHAR) >= '{start_date_last}' 
                AND CAST("DATE" AS VARCHAR) <= '{end_date_last}')
            )
            """
            
            print(f"DEBUG: This year ({start_date_this}–{end_date_this}) + Last year ({start_date_last}–{end_date_last})")

            # ────────────────────────────────────────────────
            # Force YEAR grouping — always override so rows are separated by year
            # ────────────────────────────────────────────────
            grouping_type = 'year'
            group_expr = """
                CAST(
                    CASE
                        WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) BETWEEN 1 AND 3
                        THEN YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1
                        ELSE YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d')))
                    END AS VARCHAR
                )
            """
            group_alias = 'financial_year'
            print("DEBUG: Forcing YEAR grouping → separate rows for this year and last year")
        #============================== end of compare logic code========================================
        if (
            not date_condition
            and "this quarter" in query_lower
            and "last quarter" in query_lower
            and any(word in query_lower for word in ["and", "vs", "versus", "compared", "comparison"])
        ):

            now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
            current_date = now.date()
            current_month = now.month
            current_year = now.year

            # Fiscal quarter detection
            if 4 <= current_month <= 6:
                q_num = 1
                fy_start_year = current_year
            elif 7 <= current_month <= 9:
                q_num = 2
                fy_start_year = current_year
            elif 10 <= current_month <= 12:
                q_num = 3
                fy_start_year = current_year
            else:
                q_num = 4
                fy_start_year = current_year - 1

            q_ranges = {
                1: (4, 1, 6, 30),
                2: (7, 1, 9, 30),
                3: (10, 1, 12, 31),
                4: (1, 1, 3, 31),
            }

            # This quarter
            sm, sd, em, ed = q_ranges[q_num]
            this_year = fy_start_year if q_num != 4 else fy_start_year + 1
            this_start_date = datetime.date(this_year, sm, sd)
            this_end_temp = datetime.date(this_year, em, ed)
            this_end_date = min(this_end_temp, current_date)

            # Previous quarter
            prev_q = q_num - 1
            prev_fy = fy_start_year
            if prev_q < 1:
                prev_q = 4
                prev_fy -= 1
            psm, psd, pem, ped = q_ranges[prev_q]
            prev_year = prev_fy if prev_q != 4 else prev_fy + 1
            prev_start = datetime.date(prev_year, psm, psd)
            prev_end = datetime.date(prev_year, pem, ped)

            # Format strings
            ts = this_start_date.strftime('%Y%m%d')
            te = this_end_date.strftime('%Y%m%d')
            ps = prev_start.strftime('%Y%m%d')
            pe = prev_end.strftime('%Y%m%d')

            date_condition = f"""
            (
                CAST("DATE" AS VARCHAR) >= '{ts}' AND CAST("DATE" AS VARCHAR) <= '{te}'
                OR
                CAST("DATE" AS VARCHAR) >= '{ps}' AND CAST("DATE" AS VARCHAR) <= '{pe}'
            )
            """

            # Force grouping by quarter — always override for comparison
            grouping_type = 'quarter'
            group_expr = """
            CONCAT(
                'Q',
                CASE
                    WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) IN (4,5,6) THEN '1'
                    WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) IN (7,8,9) THEN '2'
                    WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) IN (10,11,12) THEN '3'
                    ELSE '4'
                END,
                ' ',
                CASE
                    WHEN MONTH(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) <= 3
                    THEN CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) - 1 AS VARCHAR)
                    ELSE CAST(YEAR(TRY(date_parse(CAST("DATE" AS VARCHAR), '%Y%m%d'))) AS VARCHAR)
                END
            )
            """
            group_alias = 'quarter'

            print(f"DEBUG: Quarter comparison → Q{q_num} {fy_start_year} and previous")
            print(f"DEBUG: date_condition = {date_condition}")
        
        # Check for "this quarter" or "current quarter"
        elif not date_condition and ('this quarter' in query_lower or 'current quarter' in query_lower):
            # Determine which quarter we're in based on current month
            current_month = current_date.month
            if current_month >= 4 and current_month <= 6:
                # Q1 (Apr-Jun)
                quarter_start = datetime.date(current_date.year, 4, 1)
                quarter_end = datetime.date(current_date.year, 6, 30)
            elif current_month >= 7 and current_month <= 9:
                # Q2 (Jul-Sep)
                quarter_start = datetime.date(current_date.year, 7, 1)
                quarter_end = datetime.date(current_date.year, 9, 30)
            elif current_month >= 10 and current_month <= 12:
                # Q3 (Oct-Dec)
                quarter_start = datetime.date(current_date.year, 10, 1)
                quarter_end = datetime.date(current_date.year, 12, 31)
            else:
                # Q4 (Jan-Mar)
                quarter_start = datetime.date(current_date.year, 1, 1)
                quarter_end = datetime.date(current_date.year, 3, 31)
            
            # Use today if we're still in the quarter
            actual_end = min(quarter_end, current_date)
            start_date = quarter_start.strftime('%Y%m%d')
            end_date = actual_end.strftime('%Y%m%d')
            print("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
       
        # Check for "this year" or "current year"
        elif not date_condition and ('this year' in query_lower or 'current year' in query_lower):
            start_date = f'{current_fy_start}0401'
            # Use today if we're still in current FY, otherwise end of FY
            if current_date.month >= 4:
                end_date = current_date.strftime('%Y%m%d')
            else:
                end_date = current_date.strftime('%Y%m%d')
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
        
        # Check for "last quarter" or "last N quarters"
        elif not date_condition and ('last' in query_lower or 'previous' in query_lower) and 'quarter' in query_lower:
            q_words_map = {'one': 1, 'two': 2, 'three': 3, 'four': 4}
            num_str_q = _find_last_n_pattern(query_lower, 'quarter', q_words_map)
            # num_str_q: int if found with number, None if 'last quarter', False if not found
            if num_str_q is not False:
                # Determine current fiscal quarter
                current_month = current_date.month
                
                # Define fiscal quarters: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
                if current_month >= 4 and current_month <= 6:
                    current_q = 1
                    current_q_year = current_date.year
                elif current_month >= 7 and current_month <= 9:
                    current_q = 2
                    current_q_year = current_date.year
                elif current_month >= 10 and current_month <= 12:
                    current_q = 3
                    current_q_year = current_date.year
                else:  # Jan-Mar
                    current_q = 4
                    current_q_year = current_date.year - 1  # Q4 belongs to previous FY
                
                if num_str_q is not None:
                    # "last N quarters" - return N complete fiscal quarters
                    num_quarters = num_str_q
                    
                    # Calculate which quarters to include
                    # Go back N quarters from the last complete quarter
                    quarters_to_fetch = []
                    
                    # Start from previous quarter
                    q = current_q - 1
                    q_year = current_q_year
                    
                    for i in range(num_quarters):
                        if q < 1:
                            q = 4
                            q_year -= 1
                        quarters_to_fetch.append((q, q_year))
                        q -= 1
                    
                    # Reverse to get chronological order
                    quarters_to_fetch.reverse()
                    
                    # Get start of first quarter and end of last quarter
                    first_q, first_q_year = quarters_to_fetch[0]
                    last_q, last_q_year = quarters_to_fetch[-1]
                    
                    # Quarter start and end dates
                    quarter_dates = {
                        1: ('04', '01', '06', '30'),  # Q1: Apr 1 - Jun 30
                        2: ('07', '01', '09', '30'),  # Q2: Jul 1 - Sep 30
                        3: ('10', '01', '12', '31'),  # Q3: Oct 1 - Dec 31
                        4: ('01', '01', '03', '31')   # Q4: Jan 1 - Mar 31
                    }
                    
                    first_q_start_m, first_q_start_d, _, _ = quarter_dates[first_q]
                    _, _, last_q_end_m, last_q_end_d = quarter_dates[last_q]
                    
                    # Determine actual calendar years
                    if first_q == 4:  # Q4 is in next calendar year
                        start_year = first_q_year + 1
                    else:
                        start_year = first_q_year
                    
                    if last_q == 4:  # Q4 is in next calendar year
                        end_year = last_q_year + 1
                    else:
                        end_year = last_q_year
                    
                    start_date = f'{start_year}{first_q_start_m}{first_q_start_d}'
                    end_date = f'{end_year}{last_q_end_m}{last_q_end_d}'
                    
                    print(f"DEBUG: Last {num_quarters} quarters: Q{first_q} FY{first_q_year} to Q{last_q} FY{last_q_year}")
                else:
                    # "last quarter" (singular, no number) - return complete previous fiscal quarter
                    # Determine previous quarter
                    prev_q = current_q - 1
                    prev_q_year = current_q_year
                    if prev_q < 1:
                        prev_q = 4
                        prev_q_year -= 1
                    
                    # Quarter dates
                    quarter_dates = {
                        1: (4, 1, 6, 30),   # Q1: Apr 1 - Jun 30
                        2: (7, 1, 9, 30),   # Q2: Jul 1 - Sep 30
                        3: (10, 1, 12, 31), # Q3: Oct 1 - Dec 31
                        4: (1, 1, 3, 31)    # Q4: Jan 1 - Mar 31
                    }
                    
                    start_m, start_d, end_m, end_d = quarter_dates[prev_q]
                    
                    # Q4 is in next calendar year
                    if prev_q == 4:
                        start_year = end_year = prev_q_year + 1
                    else:
                        start_year = end_year = prev_q_year
                    
                    start_date = f'{start_year}{start_m:02d}{start_d:02d}'
                    end_date = f'{end_year}{end_m:02d}{end_d:02d}'
                    
                    print(f"DEBUG: Last quarter: Q{prev_q} FY{prev_q_year}")
                
                date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
                print(f"DEBUG: Last quarter(s) date_condition: {start_date} to {end_date}")

        # =========================== quarter logic code ends here ==========================
        # #============================== new code compare two quarter ==========================
        # # ========== this quarter + last quarter (and / vs) ==========
        # if (
        #     "this quarter" in query_lower
        #     and "last quarter" in query_lower
        #     and ("and" in query_lower or "vs" in query_lower)
        # ):
        #     current_month = current_date.month

        #     # Determine current fiscal quarter
        #     if 4 <= current_month <= 6:
        #         current_q = 1
        #         current_q_year = current_date.year
        #     elif 7 <= current_month <= 9:
        #         current_q = 2
        #         current_q_year = current_date.year
        #     elif 10 <= current_month <= 12:
        #         current_q = 3
        #         current_q_year = current_date.year
        #     else:
        #         current_q = 4
        #         current_q_year = current_date.year - 1  # Q4 belongs to previous FY

        #     # Quarter date map
        #     quarter_dates = {
        #         1: (4, 1, 6, 30),    # Q1 Apr-Jun
        #         2: (7, 1, 9, 30),    # Q2 Jul-Sep
        #         3: (10, 1, 12, 31),  # Q3 Oct-Dec
        #         4: (1, 1, 3, 31)     # Q4 Jan-Mar
        #     }

        #     # ---------- THIS QUARTER ----------
        #     tq_start_m, tq_start_d, tq_end_m, tq_end_d = quarter_dates[current_q]
        #     if current_q == 4:
        #         this_q_year = current_q_year + 1
        #     else:
        #         this_q_year = current_q_year

        #     this_q_start = datetime.date(this_q_year, tq_start_m, tq_start_d)
        #     this_q_end = min(datetime.date(this_q_year, tq_end_m, tq_end_d), current_date)

        #     # ---------- LAST QUARTER ----------
        #     prev_q = current_q - 1
        #     prev_q_year = current_q_year
        #     if prev_q < 1:
        #         prev_q = 4
        #         prev_q_year -= 1

        #     lq_start_m, lq_start_d, lq_end_m, lq_end_d = quarter_dates[prev_q]
        #     if prev_q == 4:
        #         last_q_year = prev_q_year + 1
        #     else:
        #         last_q_year = prev_q_year

        #     last_q_start = datetime.date(last_q_year, lq_start_m, lq_start_d)
        #     last_q_end = datetime.date(last_q_year, lq_end_m, lq_end_d)

        #     start_date_last = last_q_start.strftime('%Y%m%d')
        #     end_date_last = last_q_end.strftime('%Y%m%d')
        #     start_date_this = this_q_start.strftime('%Y%m%d')
        #     end_date_this = this_q_end.strftime('%Y%m%d')

        #     date_condition = (
        #         f"(CAST(\"DATE\" AS VARCHAR) BETWEEN '{start_date_last}' AND '{end_date_last}' "
        #         f"OR CAST(\"DATE\" AS VARCHAR) BETWEEN '{start_date_this}' AND '{end_date_this}')"
        #     )

        #     print("DEBUG: This quarter + Last quarter combined")
        
        
        #=======================================================================
        
        if not date_condition and "this month" in query_lower and "last month" in query_lower and ("and" in query_lower or "vs" in query_lower):

            now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
            
            # This month
            this_start = now.replace(day=1).strftime("%Y%m%d")
            this_end   = now.strftime("%Y%m%d")

            # Last month (full previous month)
            last_month_end   = now.replace(day=1) - datetime.timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            
            last_start = last_month_start.strftime("%Y%m%d")
            last_end   = last_month_end.strftime("%Y%m%d")

            date_condition = f"""
            (
                (CAST("DATE" AS VARCHAR) >= '{this_start}' AND CAST("DATE" AS VARCHAR) <= '{this_end}')
                OR
                (CAST("DATE" AS VARCHAR) >= '{last_start}' AND CAST("DATE" AS VARCHAR) <= '{last_end}')
            )
            """

            # Force grouping by month — always override for comparison
            grouping_type = 'month'
            group_expr = "date_format(TRY(date_parse(CAST(\"DATE\" AS VARCHAR), '%Y%m%d')), '%M %Y')"
            group_alias = 'month'
        #=================================== end of code compare two month======================
        # ----------------------------------------------------------
        # # FIX: "this month" → filter current financial year + current month
        if not date_condition and "this month" in query_lower:
            now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
            
            # First day of current month
            start_date = now.replace(day=1).strftime("%Y%m%d")
            
            # Today
            end_date = now.strftime("%Y%m%d")

            date_condition = (
                f"CAST(\"DATE\" AS VARCHAR) BETWEEN '{start_date}' AND '{end_date}'"
            )


        
        # Check for "last N months" or "last month" or "previous month"
        if not date_condition and (("last" in query_lower or "previous" in query_lower) and "month" in query_lower):
            month_words_map = {
                'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6,
                'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10, 'eleven': 11, 'twelve': 12
            }
            num_str_m = _find_last_n_pattern(query_lower, 'month', month_words_map)
            if num_str_m is not False:
                if num_str_m is not None:
                    # "last N months" - return N complete calendar months
                    num_months = num_str_m
                    
                    # Calculate start and end dates
                    # Start: First day of the month N months ago (from previous month)
                    # End: Last day of previous month
                    
                    # Get first day of current month
                    first_day_current_month = current_date.replace(day=1)
                    # Last day of previous month
                    last_day_prev_month = first_day_current_month - datetime.timedelta(days=1)
                    
                    # Go back N-1 more months from the previous month
                    start_month = last_day_prev_month
                    for i in range(num_months - 1):
                        # Go to first day of this month
                        start_month = start_month.replace(day=1)
                        # Go back one day to get to previous month
                        start_month = start_month - datetime.timedelta(days=1)
                    
                    # First day of start month
                    first_day_start_month = start_month.replace(day=1)
                    
                    start_date = first_day_start_month.strftime('%Y%m%d')
                    end_date = last_day_prev_month.strftime('%Y%m%d')
                    
                    print(f"DEBUG: Last {num_months} months: {first_day_start_month.strftime('%B %Y')} to {last_day_prev_month.strftime('%B %Y')}")
                else:
                    # "last month" (singular, no number) - use complete previous calendar month
                    # Get first day of current month
                    first_day_current_month = current_date.replace(day=1)
                    # Last day of previous month
                    last_day_prev_month = first_day_current_month - datetime.timedelta(days=1)
                    # First day of previous month
                    first_day_prev_month = last_day_prev_month.replace(day=1)
                    
                    start_date = first_day_prev_month.strftime('%Y%m%d')
                    end_date = last_day_prev_month.strftime('%Y%m%d')
                
                date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
        
       
        
        # Check for "last N years"
        year_words_map = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5}
        num_str_y = _find_last_n_pattern(query_lower, 'year', year_words_map)
        if not date_condition and num_str_y is not None and num_str_y is not False:
            num_years = num_str_y
            
            # "last N years" means N complete PAST fiscal years, not including current FY
            # Example: Today is Dec 2025 (FY 2025-2026)
            # "last 2 years" = FY 2023-2024 and FY 2024-2025 (NOT current FY 2025-2026)
            
            # Start from the first of the N previous years
            start_fy = current_fy_start - num_years
            # End at the end of the most recent complete year (previous FY)
            end_fy = current_fy_start - 1
            
            start_date = f'{start_fy}0401'
            end_date = f'{end_fy + 1}0331'
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
            print(f"DEBUG: Last {num_years} years: FY {start_fy}-{start_fy+1} to FY {end_fy}-{end_fy+1}")
        
        if not date_condition and ("last year" in query_lower or "previous year" in query_lower) and "this year" not in query_lower:
            last_fy_start = current_fy_start - 1
            start_date = f'{last_fy_start}0401'
            end_date = f'{last_fy_start + 1}0331'
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
        

        # DEFAULT DATE FILTER (ONLY WHEN USER DID NOT MENTION ANY DATE)
        # ----------------------------------------------------------
        # DEFAULT DATE FILTER (ONLY WHEN USER DID NOT MENTION ANY DATE)
        # ----------------------------------------------------------
        date_keywords = [
            "year", "fy", "financial", "quarter", "q1", "q2", "q3", "q4",
            "month", "monthly", "date",
            "week", "weekly", "week wise", "weekwise", "wow",
            "yoy", "qoq",
            "last", "current", "this",
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
            "2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027", "2028", "2029"
        ]

        # Pure token-based matching — no regex.
        # For ambiguous short month names ('may', 'march', 'june', 'july') that can appear
        # inside product/place names (e.g. 'may' inside 'mayfair'), we split the query into
        # tokens and check for an exact token match.
        # For 4-digit year strings we also check token-by-token.
        # All other keywords use normal substring matching so compound words like
        # 'yearly', 'yearwise', 'yoy', 'quarterly', 'monthly' still trigger correctly.
        _strict_token_match = {'may', 'march', 'june', 'july'}
        def _has_date_word(text, keywords):
            tokens = set(text.replace(',', ' ').replace('.', ' ').split())
            for kw in keywords:
                if kw in _strict_token_match:
                    # Exact token match only — prevents 'may' firing inside 'mayfair'
                    if kw in tokens:
                        return True
                elif kw.isdigit():
                    # 4-digit year — exact token match to avoid partial number matches
                    if kw in tokens:
                        return True
                else:
                    # Substring match for compound-friendly keywords (yearly, yoy, quarterly…)
                    if kw in text:
                        return True
            return False

        has_any_date_word = _has_date_word(query_lower, date_keywords)
        
     
        if not date_condition and not has_any_date_word:
            start_date = f'{current_fy_start}0401'
            end_date = current_date.strftime('%Y%m%d')

            date_condition = (
                f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' '
                f'AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
            )

            print(
                f"DEBUG: No date keyword found → Applying DEFAULT CURRENT FY: "
                f"{start_date} to {end_date}"
            )
        
        # Check for "last N days"
        num_days_val = None
        if not date_condition and 'last' in query_lower and 'day' in query_lower:
            tokens_d = _tokenize_query(query_lower)
            for i, tok in enumerate(tokens_d):
                if tok == 'last' and i + 2 < len(tokens_d):
                    if tokens_d[i+1].isdigit() and tokens_d[i+2].startswith('day'):
                        num_days_val = int(tokens_d[i+1])
                        break
        if not date_condition and num_days_val:
            num_days = num_days_val
            
            end_date_obj = current_date - datetime.timedelta(days=1)
            start_date_obj = end_date_obj - datetime.timedelta(days=num_days - 1)
            
            start_date = start_date_obj.strftime('%Y%m%d')
            end_date = end_date_obj.strftime('%Y%m%d')
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
            print(f"DEBUG: Last {num_days} days: {start_date} to {end_date}")
        
        if not date_condition and ("current year" in query_lower or "this year" in query_lower) and "last year" not in query_lower:
            start_date = f'{current_fy_start}0401'
            end_date = f'{current_fy_start + 1}0331'
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
        
        # CRITICAL FIX: Handle specific year for grouped queries
        # If we have a specific year AND a grouping type, set date condition for that fiscal year
        if specified_year and grouping_type and not date_condition:
            # For "quarter on quarter 2023" or "month on month 2023" or "year on year 2023"
            # Use the full fiscal year: April 2023 to March 2024
            start_date = f'{specified_year}0401'
            end_date = f'{specified_year + 1}0331'
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
            print(f"DEBUG: Grouped query with specific year {specified_year} -> FY {specified_year}-{specified_year+1}: {start_date} to {end_date}")
        
        # Check for specific year mentioned (e.g., "2022", "2023")
        # But DON'T overwrite if we already have a date condition from above
        if not date_condition and specified_year:
            # Only set year-based date if no date condition exists yet
            start_date = f'{specified_year}0401'
            end_date = f'{specified_year + 1}0331'
            date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
            print(f"DEBUG: Year-based date set: {start_date} to {end_date}")
        
        # DEFAULT: If no date condition specified, determine based on grouping type
        # CRITICAL FIX: Only set default FY for GROUPED queries, not simple queries
        print(f"DEBUG: Before default check - date_condition={date_condition}, grouping_type={grouping_type}, specified_year={specified_year}")
        if not date_condition:
            if grouping_type == 'year':
                # Year-on-year: Show ALL years (no date filter)
                pass  # date_condition remains None
                print("DEBUG: Year-on-year grouping - no date filter")
            elif grouping_type in ['quarter', 'month', 'week']:
                # Quarter/Month grouping: Use current FY only if grouping
                start_date = f'{current_fy_start}0401'
                end_date = f'{current_fy_start + 1}0331'
                date_condition = f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
                print(f"DEBUG: Grouping with no specific date - using current FY: {start_date} to {end_date}")
            else:
                # Entity-based grouping (sales_group, company, description, etc.)
                # Apply default current FY date filter so comparisons don't pull all-time data
                start_date = f'{current_fy_start}0401'
                end_date = current_date.strftime('%Y%m%d')
                date_condition = (
                    f'CAST("DATE" AS VARCHAR) >= \'{start_date}\' '
                    f'AND CAST("DATE" AS VARCHAR) <= \'{end_date}\''
                )
                print(f"DEBUG: Entity grouping with no specific date → Applying DEFAULT CURRENT FY: {start_date} to {end_date}")
        
        print(f"DEBUG: Final date_condition: {date_condition}\n")

        # ── Universal vs / and / separately comparison detection ─────────────
        # Load live mappings (cached — DB queried only once per process)
        sales_group_mapping_local, company_code_mapping_local = get_sg_and_company_mappings()

        # Detect comparison intent FIRST so we know which column it owns
        cmp = detect_comparison_intent(user_query, sales_group_mapping_local, company_code_mapping_local)
        print(f"DEBUG: comparison intent = {cmp}")

        # Build skip_columns: whichever column comparison owns, don't let
        # build_where_conditions also generate a filter for it — that causes
        # conflicting AND conditions that wipe out all but one value.
        skip_cols: set = set()
        if cmp["active"]:
            skip_cols.add(cmp["group_alias"])  # e.g. 'description', 'company_name', 'sales_group', 'dunning_level'

        # Collect all matched SG names so build_where_conditions can strip them
        # before customer-name extraction — prevents "new plots yoy" → customer name.
        _matched_sg_names = [name for name, _ in _match_sg_whole_word(query_lower, sales_group_mapping_local)]

        # Build entity WHERE conditions (project names, customer names, etc.)
        entity_conditions, _ = build_where_conditions(user_query, columns, skip_columns=skip_cols, known_sg_names=_matched_sg_names)

        # SAFETY NET: strip any single-column filters for the column comparison owns.
        # This is a second layer of protection — if skip_columns somehow didn't prevent
        # a filter from being added, we remove it here so comparison's in_clause is
        # the ONLY filter for that column. Without this, both conditions end up ANDed
        # together which eliminates all rows except those matching the single filter.
        if cmp["active"]:
            _col_strip_keywords = {
                "description":   ["description"],
                "company_name":  ["company_code"],
                "sales_group":   ["sales_group"],
                "dunning_level": ["dunning_level", "dunning"],
                "mail_sent":     ["mail_sent"],
            }.get(cmp["group_alias"], [])
            if _col_strip_keywords:
                before = list(entity_conditions)
                entity_conditions = [
                    c for c in entity_conditions
                    if not any(kw in c.lower() for kw in _col_strip_keywords)
                ]
                stripped = [c for c in before if c not in entity_conditions]
                if stripped:
                    print(f"DEBUG: Safety net stripped conflicting conditions: {stripped}")

        # Apply comparison grouping
        if cmp["active"]:
            col_alias = cmp["group_alias"]

            # Add the comparison's own IN/LIKE filter (authoritative for that column)
            if cmp.get("in_clause"):
                entity_conditions.append(cmp["in_clause"])
            # Force the right GROUP BY column (overrides separately / keyword detection)
            if not grouping_type:
                alias = cmp["group_alias"]
                expr  = cmp.get("group_expr") or cmp["column"]
                grouping_type = alias
                group_expr    = expr
                group_alias   = alias
            print(f"DEBUG: comparison active → grouping by {col_alias}, final entity_conditions={entity_conditions}")
        # ─────────────────────────────────────────────────────────────────────

        # NEW: Mail sent / not sent filtering
        mail_condition = build_mail_sent_condition(user_query)
        print(f"DEBUG: Mail condition → {mail_condition}")

        # ── group_by_* flags ──────────────────────────────────────────────────
        # THE FIX: cmp["group_alias"] is the AUTHORITATIVE source when comparison
        # is active. We capture it ONCE here, then every flag checks it first.
        # This means "wave galleria and prime floors" correctly sets
        # group_by_sales_group=True even though neither word is "sales group".
        _cmp_col = cmp["group_alias"] if cmp.get("active") else ""

        group_by_customer = (
            _cmp_col == "customer_name"
            or ('customer' in query_lower and ('wise' in query_lower or 'by customer' in query_lower or 'each customer' in query_lower or 'per customer' in query_lower or 'customer name' in query_lower))
            or 'customer name wise' in query_lower or 'customer wise' in query_lower or 'customer' in query_lower
        )
        group_by_mail_sent = (
            _cmp_col == "mail_sent"
            or ('mail sent' in query_lower and ('wise' in query_lower or 'by mail sent' in query_lower or 'each mail sent' in query_lower or 'per mail sent' in query_lower or 'mail sent wise' in query_lower))
            or 'mail sent wise' in query_lower or 'mail sent' in query_lower or 'email sent' in query_lower
        )
        group_by_mail_id = (
            _cmp_col == "mail_id"
            or ('mail id' in query_lower and ('wise' in query_lower or 'by mail id' in query_lower or 'each mail id' in query_lower or 'per mail id' in query_lower or 'mail id wise' in query_lower))
            or 'mail id wise' in query_lower or 'mail id' in query_lower or 'email id' in query_lower
        )
        group_by_booking_no = (
            _cmp_col == "booking_no"
            or ('booking no' in query_lower and ('wise' in query_lower or 'by booking no' in query_lower or 'each booking no' in query_lower or 'per booking no' in query_lower or 'booking number wise' in query_lower))
            or 'booking number wise' in query_lower or 'booking no' in query_lower or 'booking number' in query_lower
        )
        group_by_dunning_level = (
            _cmp_col == "dunning_level"
            or ('dunning level' in query_lower and ('wise' in query_lower or 'by dunning level' in query_lower or 'each dunning level' in query_lower or 'per dunning level' in query_lower or 'dunning level wise' in query_lower))
            or 'dunning level wise' in query_lower or 'dunning level' in query_lower
        )
        group_by_user_id = (
            _cmp_col == "user_id"
            or ('user id' in query_lower and ('wise' in query_lower or 'by user id' in query_lower or 'each user id' in query_lower or 'per user id' in query_lower or 'user id wise' in query_lower))
            or 'user id wise' in query_lower or 'user id' in query_lower
        )
        group_by_company = (
            _cmp_col == "company_name"
            or ('company' in query_lower and ('wise' in query_lower or 'by company' in query_lower or 'each company' in query_lower or 'per company' in query_lower))
            or 'company code wise' in query_lower or 'project wise' in query_lower
            or 'by project' in query_lower or 'project' in query_lower
        )
        group_by_sales_group = (
            _cmp_col == "sales_group"
            or ('sales group' in query_lower and ('wise' in query_lower or 'by sale group' in query_lower or 'each sales group' in query_lower or 'per sales group' in query_lower))
            or 'sales group wise' in query_lower or 'product wise' in query_lower or 'sale group' in query_lower
            or 'by product' in query_lower or 'by sales group' in query_lower
        )
        group_by_description_type = (
            _cmp_col == "description"
            or ('description' in query_lower and ('wise' in query_lower or 'by description' in query_lower or 'each description' in query_lower or 'per description' in query_lower))
            or 'description wise' in query_lower or 'desc wise' in query_lower
        )
        group_by_Letter_No = (
            _cmp_col == "letter_no"
            or ('letter no' in query_lower and ('wise' in query_lower or 'by letter no' in query_lower or 'each letter no' in query_lower or 'per letter no' in query_lower or 'letter number wise' in query_lower))
            or 'letter number wise' in query_lower or 'letter no' in query_lower or 'letter number' in query_lower
        )

        print(f"DEBUG: _cmp_col={_cmp_col!r} | group_by_sales_group={group_by_sales_group}, group_by_company={group_by_company}, group_by_description_type={group_by_description_type}")
        # ── Determine if grouping_type is a time-based or entity-based group ────
        TIME_GROUP_TYPES = {'year', 'quarter', 'month', 'week',
                             'fiscal_year', 'fiscal_quarter', 'financial_year'}
        # For non-time grouping types (sales_group, company, description, mail_sent,
        # dunning_level, etc.) the group_by_* flags already handle SELECT/GROUP BY.
        # We only add the group_expr column for time-based grouping.
        is_time_grouping = grouping_type in TIME_GROUP_TYPES

        # Build the SQL query
        if grouping_type:
            # Aggregation query (time-based OR entity-based)
            select_parts = []
            
            # Add customer name if needed
            if group_by_customer:
                select_parts.append('"customer_name"')
            if group_by_mail_sent:
                select_parts.append('"Mail_Sent"')
            if group_by_mail_id:
                select_parts.append('"Mail_Id"')
            if group_by_booking_no:
                select_parts.append('"Booking_No"')
            if group_by_dunning_level:
                select_parts.append('"Dunning_Level"')
            if group_by_Letter_No:
                select_parts.append('"Letter_No"')
            if group_by_user_id:
                select_parts.append('"User_Id"')
            
            # Add company name if needed
            if group_by_company:
                select_parts.append(f'{get_company_name_expr()} AS company_name')
            if group_by_sales_group:
                select_parts.append(f'{get_sales_group_expr()} AS sales_group')
            if group_by_description_type:
                select_parts.append(f'{get_description_expr()} AS description')
            
            # Add time grouping column ONLY for time-based groupings
            if is_time_grouping:
                select_parts.append(f'{group_expr} AS {group_alias}')
            
            # Add all metrics
            for field, alias in metrics:
                select_parts.append(f'{numeric_sum_expr(field)} AS {alias}')
            
            # Build the query
            sql = f'SELECT {", ".join(select_parts)} FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"'
            
            # Add WHERE conditions
            where_parts = []
            
            # Add date validity check
            where_parts.append('TRY(date_parse(CAST("DATE" AS VARCHAR), \'%Y%m%d\')) IS NOT NULL')
            
            # SAFETY NET: If date_condition is still None and grouping is not year-on-year,
            # apply the default current FY date filter to prevent fetching all-time data.
            _non_date_groupings = {'sales_group', 'company_name', 'description', 'mail_sent',
                                   'dunning_level', 'booking_no', 'user_id', 'letter_no',
                                   'customer_name', 'custom_period'}
            if not date_condition and grouping_type in _non_date_groupings:
                _safe_start = f'{current_fy_start}0401'
                _safe_end = current_date.strftime('%Y%m%d')
                date_condition = (
                    f'CAST("DATE" AS VARCHAR) >= \'{_safe_start}\' '
                    f'AND CAST("DATE" AS VARCHAR) <= \'{_safe_end}\''
                )
                print(f"DEBUG: SAFETY NET date filter applied → {_safe_start} to {_safe_end}")

            # Add date range condition if specified (may be None for year-on-year to show all years)
            if date_condition:
                where_parts.append(date_condition)
            
            # Add entity conditions
            if entity_conditions:
                where_parts.extend(entity_conditions)
            # Add mail sent condition
            if mail_condition:
                where_parts.append(mail_condition)
            
            # Add NOT NULL checks for metrics (use OR for flexibility)
            null_checks = [f'"{field}" IS NOT NULL' for field, _ in metrics]
            if len(null_checks) > 1:
                where_parts.append('(' + ' OR '.join(null_checks) + ')')
            else:
                where_parts.extend(null_checks)
            
            sql += ' WHERE ' + ' AND '.join(where_parts)
            
            # Add GROUP BY - MUST repeat the CASE expression, not use alias
            group_by_parts = []
            if group_by_customer:
                group_by_parts.append('"customer_name"')
            if group_by_mail_sent:
                group_by_parts.append('"Mail_Sent"')
            if group_by_mail_id:
                group_by_parts.append('"Mail_Id"')
            if group_by_booking_no: 
                group_by_parts.append('"Booking_No"')
            if group_by_dunning_level:
                group_by_parts.append('"Dunning_Level"')
            if group_by_Letter_No:
                group_by_parts.append('"Letter_No"')
            if group_by_user_id:
                group_by_parts.append('"User_Id"')
            #================ new added line ==================
            if group_by_company:
                group_by_parts.append('"company_code"') 
            if group_by_sales_group:
                group_by_parts.append('"sales_group"') 
            if group_by_description_type:
                group_by_parts.append('"description"') 
            
            if is_time_grouping:
                group_by_parts.append(group_expr)  # Use the full expression, not the alias
            sql += ' GROUP BY ' + ', '.join(group_by_parts) if group_by_parts else ''
            
            # Add ORDER BY - can use alias here
            order_by_parts = []
            if group_by_customer:
                order_by_parts.append('"customer_name"')
            if group_by_mail_sent:
                order_by_parts.append('"Mail_Sent"')
            if group_by_mail_id:
                order_by_parts.append('"Mail_Id"')
            if group_by_booking_no: 
                order_by_parts.append('"Booking_No"')
            if group_by_dunning_level:
                order_by_parts.append('"Dunning_Level"')
            if group_by_Letter_No:
                order_by_parts.append('"Letter_No"')
            if group_by_user_id:
                order_by_parts.append('"User_Id"')
            #================ new added line ==================
            if group_by_company:
                order_by_parts.append('"company_code"')
            if group_by_sales_group:
                order_by_parts.append('"sales_group"')
            if group_by_description_type:
                order_by_parts.append('"description"')
            
            if is_time_grouping:
                order_by_parts.append(group_alias)
            sql += ' ORDER BY ' + ', '.join(order_by_parts)
        
        else:
            # Simple aggregation query (no time grouping)
            select_parts = []
            
            if group_by_customer:
                select_parts.append('"customer_name"')
            if group_by_mail_sent:
                select_parts.append('"Mail_Sent"')
            if group_by_mail_id:
                select_parts.append('"Mail_Id"')
            if group_by_booking_no: 
                select_parts.append('"Booking_No"')
            if group_by_dunning_level:
                select_parts.append('"Dunning_Level"')
            if group_by_Letter_No:
                select_parts.append('"Letter_No"')
            if group_by_user_id:
                select_parts.append('"User_Id"')
            # Add company name if needed
            if group_by_company:
                select_parts.append(f'{get_company_name_expr()} AS company_name')
            if group_by_sales_group:
                select_parts.append(f'{get_sales_group_expr()} AS sales_group')
            if group_by_description_type:
                select_parts.append(f'{get_description_expr()} AS description')
           
            # Add all metrics
            for field, alias in metrics:
                select_parts.append(f'{numeric_sum_expr(field)} AS {alias}')
            
            # Build the query
            sql = f'SELECT {", ".join(select_parts)} FROM "{CATALOG}"."{SCHEMA}"."{TABLE_NAME}"'
            
            # Add WHERE conditions
            where_parts = []
            
            # Add date validity check
            where_parts.append('TRY(date_parse(CAST("DATE" AS VARCHAR), \'%Y%m%d\')) IS NOT NULL')
            
            # SAFETY NET: If date_condition is still None for a non-year query,
            # apply the default current FY date filter.
            if not date_condition and grouping_type != 'year':
                _safe_start = f'{current_fy_start}0401'
                _safe_end = current_date.strftime('%Y%m%d')
                date_condition = (
                    f'CAST("DATE" AS VARCHAR) >= \'{_safe_start}\' '
                    f'AND CAST("DATE" AS VARCHAR) <= \'{_safe_end}\''
                )
                print(f"DEBUG: SAFETY NET (simple path) date filter applied → {_safe_start} to {_safe_end}")

            # Add date range condition if specified
            if date_condition:
                where_parts.append(date_condition)
            
            # Add entity conditions
            if entity_conditions:
                where_parts.extend(entity_conditions)
            # Add mail sent condition
            if mail_condition:
                where_parts.append(mail_condition)
            
            # Add NOT NULL checks for metrics (use OR for flexibility)
            null_checks = [f'"{field}" IS NOT NULL' for field, _ in metrics]
            if len(null_checks) > 1:
                where_parts.append('(' + ' OR '.join(null_checks) + ')')
            else:
                where_parts.extend(null_checks)
            
            sql += ' WHERE ' + ' AND '.join(where_parts)
            
            # Build GROUP BY dynamically based on all grouping flags
            group_by_parts = []
            if group_by_customer:
                group_by_parts.append('"customer_name"')
            if group_by_mail_sent:
                group_by_parts.append('"Mail_Sent"')
            if group_by_mail_id:
                group_by_parts.append('"Mail_Id"')
            if group_by_booking_no:
                group_by_parts.append('"Booking_No"')
            if group_by_dunning_level:
                group_by_parts.append('"Dunning_Level"')
            if group_by_Letter_No:
                group_by_parts.append('"Letter_No"')
            if group_by_user_id:
                group_by_parts.append('"User_Id"')
            if group_by_company:
                group_by_parts.append('"company_code"')
            if group_by_sales_group:
                group_by_parts.append('"sales_group"')
            if group_by_description_type:
                group_by_parts.append('"description"')
            
            if group_by_parts:
                sql += ' GROUP BY ' + ', '.join(group_by_parts)
            
            # Add HAVING clause for amount thresholds
            having_parts = []
            
            # Check for amount threshold patterns using pure string parsing
            def _parse_amount_threshold(q_lower: str):
                """
                Detect patterns like 'greater than 5 lakh', 'less than 2 crore'.
                Returns (amount_float, unit_str, comparison_str) or None.
                """
                greater_triggers = ['greater than', 'more than', 'above', 'exceeding']
                less_triggers = ['less than', 'below', 'under']
                currency_prefixes = ['₹', 'rs.', 'rs', 'rupees', 'rupee']
                units = ['lakh', 'lac', 'cr', 'crore']
                
                for trigger in greater_triggers + less_triggers:
                    idx = q_lower.find(trigger)
                    if idx == -1:
                        continue
                    comparison = 'greater' if trigger in greater_triggers else 'less'
                    after = q_lower[idx + len(trigger):].strip()
                    # Strip currency prefix
                    for cp in currency_prefixes:
                        if after.startswith(cp):
                            after = after[len(cp):].strip()
                            break
                    # Parse number and unit
                    tokens = after.split()
                    if not tokens:
                        continue
                    num_str = tokens[0].strip('.,')
                    try:
                        amount_value = float(num_str)
                    except ValueError:
                        continue
                    unit = tokens[1].strip('.,').lower() if len(tokens) > 1 else ''
                    if unit in units:
                        return amount_value, unit, comparison
                return None
            
            threshold_result = _parse_amount_threshold(user_query.lower())
            if threshold_result:
                amount_value, unit, comparison = threshold_result
                if unit in ['lakh', 'lac']:
                    threshold = amount_value * 100000
                elif unit in ['cr', 'crore']:
                    threshold = amount_value * 10000000
                else:
                    threshold = amount_value
                if metrics:
                    field, alias = metrics[0]
                    aggregate_expr = numeric_sum_expr(field)
                    if comparison == 'greater':
                        having_parts.append(f'{aggregate_expr} > {threshold}')
                    else:
                        having_parts.append(f'{aggregate_expr} < {threshold}')
            
            if having_parts:
                sql += ' HAVING ' + ' AND '.join(having_parts)
           
            
            
        
        # Validate SQL
        if not sql.strip().startswith("SELECT"):
            raise ValueError("Generated SQL is invalid")

        # ── Always sort by numeric metric(s) DESC (highest → lowest) ──────────
        # Strip any ORDER BY that was built above (text-based), then re-add
        # a clean one that orders by every aggregated metric descending.
        sql_upper = sql.upper()
        ob_idx = sql_upper.rfind(' ORDER BY ')
        if ob_idx != -1:
            sql = sql[:ob_idx].strip()
        if metrics:
            metric_order = ', '.join(f'{alias} DESC' for _, alias in metrics)
            sql += f' ORDER BY {metric_order}'
        # ──────────────────────────────────────────────────────────────────────
        
        # ── Add LIMIT clause for "top N" queries ──────────────────────────────
        limit_value = None
        tokens_limit = _tokenize_query(user_query.lower())
        for i, tok in enumerate(tokens_limit):
            if tok == 'top' and i + 1 < len(tokens_limit):
                if tokens_limit[i+1].isdigit():
                    limit_value = int(tokens_limit[i+1])
                    break
        if limit_value:
            sql += f' LIMIT {limit_value}'
            print(f"DEBUG: Added LIMIT {limit_value} for 'top {limit_value}' query")
        # ──────────────────────────────────────────────────────────────────────

        return sql.strip()
    
    except Exception as e:
        print(f"Error generating SQL: {e}")
        import traceback
        traceback.print_exc()
        raise


# --------------------
# Helper: Append a TOTAL row to multi-row numeric results
# --------------------
def add_total_row(data: list) -> list:
    """
    Appends a TOTAL row to multi-row results.
    - Grouping/period columns (year, quarter, month, etc.) → always treated as labels, never summed.
    - Columns where every value is numeric → summed.
    - First non-numeric / grouping column  → labelled "TOTAL".
    - All other non-summable columns       → filled with "-".
    """
    if not data or len(data) <= 1:
        return data

    # Column names that are grouping/period labels and must never be summed
    LABEL_COLUMNS = {
        'financial_year', 'fiscal_year', 'fiscal_quarter', 'quarter',
        'month', 'week', 'year', 'date',
        'company_name', 'company_code', 'sales_group', 'description',
        'customer_name', 'mail_sent', 'dunning_level', 'booking_no',
        'user_id', 'letter_no', 'mail_id',
    }

    total_row: dict = {}
    label_placed = False

    for key in data[0].keys():
        # Force label for known grouping columns
        if key.lower() in LABEL_COLUMNS:
            if not label_placed:
                total_row[key] = "TOTAL"
                label_placed = True
            else:
                total_row[key] = "-"
            continue

        numeric_values = []
        all_numeric = True

        for row in data:
            val = row.get(key)
            if val is None:
                all_numeric = False
                break
            try:
                numeric_values.append(float(val))
            except (ValueError, TypeError):
                all_numeric = False
                break

        if all_numeric and numeric_values:
            total = sum(numeric_values)
            total_row[key] = int(total) if total == int(total) else round(total, 2)
        else:
            if not label_placed:
                total_row[key] = "TOTAL"
                label_placed = True
            else:
                total_row[key] = "-"

    return data + [total_row]


# --------------------
# FastAPI Application
# --------------------
app = FastAPI(title="Watsonx NL2SQL + Presto API")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    sql: str
    data: list

@app.post("/generate-sql", response_model=QueryResponse)
def generate_sql(req: QueryRequest):
    try:
        raw_sql = nl_to_sql(req.question)
        clean_sql = raw_sql
        data = run_presto_query(clean_sql)

        print("Raw SQL executed:", clean_sql)
        print("Raw data from Presto (before formatting):", data)

        # Append a TOTAL row when the result has multiple rows and no errors
        if data and len(data) > 1 and not any('error' in row for row in data):
            data = add_total_row(data)

    except Exception as e:
        clean_sql = ""
        data = [{"error": str(e)}]
        print(f"API Error: {e}")

    return {"sql": clean_sql, "data": data}

@app.get("/debug-desc-regex")
def debug_desc_regex():
    test_query = "Total outstanding of wave city whose description is PRE-CANCELLATION NOTICE"
    
    # Pure string matching for description trigger
    desc_trigger_phrases = [
        'description is ', 'desc is ', 'reminder type is ', 'letter type is ',
        'description ', 'desc ',
    ]
    matched = False
    group1 = None
    for phrase in desc_trigger_phrases:
        idx = test_query.lower().find(phrase)
        if idx != -1:
            after = test_query[idx + len(phrase):].strip().strip('"\'')
            if after:
                words = after.split()
                noise = {'for', 'of', 'in', 'with', 'and', 'by', 'from', 'last', 'this', 'next'}
                clean = []
                for w in words:
                    if w.lower() in noise or w[0:1].isdigit():
                        break
                    clean.append(w)
                group1 = ' '.join(clean).strip()
                matched = bool(group1)
            break
    
    return {
        "matched": matched,
        "group1": group1,
        "query": test_query
    }
