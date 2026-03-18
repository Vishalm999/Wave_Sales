# # import json
# # import re
# # import datetime
# # from typing import Any, Dict, List, Optional, Tuple
# # # from app.semantic.intent import SemanticIntent
# # from semantic.intent import SemanticIntent


# # class WatsonxSemanticAdapter:
# #     """
# #     Adapter responsible ONLY for:
# #     Natural Language -> SemanticIntent
# #     Uses Watsonx ModelInference.generate_text()
    
# #     IMPROVEMENTS:
# #     1. Comprehensive dimension-to-keyword mapping
# #     2. Filter value extraction (e.g., "tower 7", "amore", "16th floor")
# #     3. Better metric selection logic
# #     4. Enhanced natural language understanding
# #     5. Custom date parsing
# #     """
    

# #     # =====================================================
# #     # MONTH NAME MAPPINGS FOR DATE PARSING
# #     # =====================================================
# #     MONTH_NAMES = {
# #         "january": 1, "jan": 1,
# #         "february": 2, "feb": 2,
# #         "march": 3, "mar": 3,
# #         "april": 4, "apr": 4,
# #         "may": 5,
# #         "june": 6, "jun": 6,
# #         "july": 7, "jul": 7,
# #         "august": 8, "aug": 8,
# #         "september": 9, "sep": 9, "sept": 9,
# #         "october": 10, "oct": 10,
# #         "november": 11, "nov": 11,
# #         "december": 12, "dec": 12,
# #     }


# #     # =====================================================
# #     # SEMANTIC MAPPINGS - FIXED ALL NAMING
# #     # =====================================================
    
# #     DIMENSION_KEYWORDS = {
# #         # Property
# #         "sales_group_desc": ["product", "eden", "amore", "livork"],
# #         "tower": ["tower", "block"],
# #         "floor_desc": ["floor", "level"],
# #         "inventory_code": ["inventory code", "inventory"],
# #         "type_desc": ["unit type", "shop", "office", "bhk"],
# #         "sector": ["sector"],
        
# #         # Customer & Sales
# #         "sold_to_name": ["customer", "sold to", "buyer"],
# #         "payer_name": ["payer","paying customer","payment by"],
# #         "customer_type": ["booking status", "customer type", "booked"],  # Column name
# #         "sales_executive_name": ["sales executive", "salesman"],
# #         "back_office_executive_name": ["back office"],
        
# #         # Channel
# #         "broker_name": ["broker name", "agent"],
# #         "sub_broker_name": ["sub broker"],
# #         "dist_channel_desc": ["channel", "distribution"],
# #         "refferal": ["referral", "ref"],
# #         "consortium_name": ["consortium"],
        
# #         # Transaction
# #         "booking_type": ["booking type", "fresh", "relocation"],
# #         "division_desc": ["division"],
# #         "sales_group_desc": ["sales group", "group"],
# #         "sales_office_desc": ["sales office", "office"],
# #         "sales_org_desc": ["sales organization", "sales org", "organization", "org"],
# #         "billing_plan": ["billing plan", "payment plan"],
# #         "billing_block_description": ["billing block", "block reason"],
        
# #         # Financial
# #         "loan_bank": ["bank", "loan bank", "lender"],
# #         "material_pricing_group_desc": ["pricing group", "material group","apartment"],
# #         "scheme_code": ["scheme"],
# #         "reason_for_rejection": ["rejection reason", "reason for rejection"],

# #         # FIX #3: Status breakdown dimensions
# #         "possession_status": ["possession status", "possession stage", "possession breakdown", "possession wise"],
# #         "agreement_status": ["agreement status", "agreement stage", "agreement breakdown", "agreement wise"],

# #         "cancellation_reason": ["cancellation reason","reason", "reason for cancellation", "cancel reason", "why cancelled", "cancellation", "reason cancelled"],
# #     }

# #     # METRIC_KEYWORDS = {
# #     #     "total_sales": ["total sales", "sales count", "number of sales", "sales orders"],
# #     #     "sales_value": ["sales value", "total revenue", "total amount", "gross sales", "net value", "net amount"],
# #     #     "net_value": ["net value", "net amount"],
# #     #     "amount_received": ["amount received", "collection", "received", "payment received"],
# #     #     "amount_demanded": ["amount demanded", "billed", "bill amount", "invoice"],
# #     #     "collection_percentage": ["collection %", "collection percentage", "collection efficiency"],
# #     #     "area_sold": ["area sold", "total area", "carpet area"],
# #     #     "basic_selling_price": ["basic price", "base price"],
# #     #     "discount": ["discount", "total discount"],
# #     #     "loan_sanctioned": ["loan sanctioned", "loan amount"],
# #     # }
    

# #     METRIC_KEYWORDS = {
# #         "total_sales": ["total sales", "sales count", "number of sales", "sales orders"],
# #         "sales_value": ["sales value", "total revenue", "total amount", "gross sales", "net value", "net amount"],
# #         "net_value": ["net value", "net amount"],
# #         "amount_received": ["amount received", "collection", "received", "payment received"],
# #         "amount_demanded": ["amount demanded", "billed", "bill amount", "invoice"],
# #         "collection_percentage": ["collection %", "collection percentage", "collection efficiency"],
# #         "area_sold": ["area sold", "total area", "carpet area"],
# #         "basic_selling_price": ["basic price", "base price"],
# #         "discount": ["discount", "total discount"],
# #         "loan_sanctioned": ["loan sanctioned", "loan amount"],
# #         # CORRECTED: Transfer metrics (counts unique customers - Sold_To_Name)
# #        # ENHANCED TRANSFER METRICS
# #                 "transferred_sales": [
# #                     "transferred sales", "transfer count", "transferred customers", 
# #                     "customers who transferred", "how many transferred",
# #                     "count of transfers", "transfers customer"
# #                 ],
# #                 "transfer_product_wise": [
# #                     "product wise transfer", "transfer product wise", 
# #                     "product transfer", "transfers by product", "product wise transfers",
# #                     "product-wise transfer", "productwise transfer"
# #                 ],
# #                 "transferred_sales_count": [
# #                     "transfer orders", "transferred units count", "transfer sales orders",
# #                     "number of transfer orders", "count of transfer orders"
# #                 ],
# #                 "transferred_sales_value": [
# #                     "transfer value", "transfer revenue", "value of transfers", 
# #                     "transferred sales value", "transfer amount", "revenue from transfers"
# #                 ],
# #                 "transfer_recipients": [
# #                     "customers who received", "transfer recipients", "final payers", 
# #                     "transferred to", "recipients of transfer", "who received transfer"
# #                 ],
# #                 "non_transferred_sales": [
# #                     "non transferred sales", "normal sales", 
# #                     "sales without transfer", "customers without transfer", "regular sales"
# #                 ],
# #                 "transfer_rate": [
# #                     "transfer rate", "transfer percentage", "transfer %", 
# #                     "percentage of transfers", "what percent transferred"
# #                 ],

# #                 # ═══════════════════════════════════════════════════════════════
# #                     # POSSESSION JOURNEY METRICS
# #                     # ═══════════════════════════════════════════════════════════════
# #                     "possession_pending_count": [
# #                         "possession pending", "pending possession", "possession not given",
# #                         "possession yet to be given", "possession not handed over",
# #                         "how many possessions pending", "pending possessions"
# #                     ],
# #                     "possession_given_count": [
# #                         "possession given", "possession handed over", "possession completed",
# #                         "possessions done", "how many possessions given"
# #                     ],
# #                     "agreement_pending_count": [
# #                         "agreement pending", "pending agreement", "agreement not created",
# #                         "agreement not prepared", "agreements pending"
# #                     ],
# #                     "agreement_given_count": [
# #                         "agreement given", "agreement created", "agreement prepared",
# #                         "agreement signed", "agreements done"
# #                     ],
# #                     "possession_completion_rate": [
# #                         "possession completion rate", "possession completion %",
# #                         "percentage of possessions given", "possession completion percentage"
# #                     ],
# #                     "possession_status_breakdown": [
# #                         "possession status", "possession journey", "possession wise breakdown",
# #                         "status of possession", "possession stages"
# #                     ],
# #                     "average_days_to_possession": [
# #                         "average days to possession", "possession TAT", "time to possession",
# #                         "how long for possession", "possession turnaround time"
# #                     ],
# #                     "average_days_to_agreement": [
# #                         "average days to agreement", "agreement TAT", "time to agreement",
# #                         "how long for agreement"
# #                     ],
# #                     "possession_pending_value": [
# #                         "value of pending possessions", "pending possession value",
# #                         "revenue in pending possessions"
# #                     ],

# #                     "cancelled_sales": [
# #                         "cancelled sales", "total cancelled", "cancellations", "cancelled bookings",
# #                         "cancelled orders", "how many cancelled", "number of cancellations",
# #                         "cancelled count", "cancellation reason", "show cancelled", "cancel", "cancelled reason", "reason"
# # ],
# #                     "cancelled_sales_value": [
# #                         "cancelled sales value", "cancellation value", "value of cancellations",
# #                         "cancelled revenue", "value of cancelled bookings"
# #               ],
# #     }
    

    

# #     COMPARISON_KEYWORDS = {
# #         "mom": ["month on month", "month-on-month", "monthly", "m-o-m", "vs last month"],
# #         "wow": ["week on week", "week-on-week", "weekly", "w-o-w"],
# #         "qoq": ["quarter on quarter", "quarter-on-quarter", "quarterly", "q-o-q"],
# #         "yoy": ["year on year", "year-on-year", "yearly", "y-o-y", "vs last year"],
# #     }

# #     TIME_GRAIN_KEYWORDS = {
# #         "day": ["daily", "day", "by day"],
# #         "month": ["monthly", "month", "by month", "month wise"],
# #         "quarter": ["quarterly", "quarter", "by quarter"],
# #         "year": ["yearly", "year", "by year"],
# #     }

# #     DIMENSION_ALIASES = {
# #         "sales_channel": "dist_channel_desc",
# #         "channel": "dist_channel_desc",
# #         "distribution_channel": "dist_channel_desc",
# #         "product": "sales_group_desc",
# #         "booking_status": "customer_type",
# #         "status": "customer_type",
# #         "customer": "sold_to_name",
# #         "buyer": "sold_to_name",
# #         "broker name": "broker_name",
# #         "agent": "broker_name",
# #         "salesman": "sales_executive_name",
# #         "sales_executive": "sales_executive_name",
# #         "executive": "sales_executive_name",
# #         "inventory code": "inventory_code",
# #         "inventory": "inventory_code",
# #         "unit_type": "type_desc",
# #         "flat_type": "type_desc",
# #         "billing_block": "billing_block_description",
# #         "block_reason": "billing_block_description",
# #         "sales_org": "sales_org_desc",
# #         "organization": "sales_org_desc",
# #         "wave city": "sales_org_desc",
# #         "bank": "loan_bank",
# #         "lender": "loan_bank",
# #         "referral": "refferal",
# #         "source": "refferal",
# #         "scheme": "scheme_code",
# #         "pricing_group": "material_pricing_group_desc",
# #         "material_group": "material_pricing_group_desc",
# #         "sales_office": "sales_office_desc",
# #         "office": "sales_office_desc",
# #         "sales_group": "sales_group_desc",
# #         "group": "sales_group_desc",
# #         "consortium": "consortium_name",
# #         "payment_plan": "billing_plan",
# #     }

# #     def __init__(self, model):
# #         """
# #         model: ibm_watsonx_ai.foundation_models.ModelInference
# #         """
# #         self.model = model



# #      # =====================================================
# #     # MULTI-QUERY DETECTION (NEW!)
# #     # =====================================================
    
# #     # def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
# #     #     """
# #     #     Detect if query contains multiple separate requests using "and"
        
# #     #     Examples:
# #     #     - "sales for wave city and wave estate" → 2 queries
# #     #     - "direct and broker sales" → 2 queries
# #     #     - "april and june sales" → 2 queries
        
# #     #     Returns list of intents (1 if single query, 2+ if multiple)
# #     #     """
# #     #     query_lower = user_query.lower()
        
# #     #     # Pattern 1: Product names with "and"
# #     #     product_match = re.search(r'(eden|amore|livork)\s+and\s+(veridia|amore|livork)', query_lower)
# #     #     if product_match:
# #     #         base_query = re.sub(r'(eden|amore|livork)\s+and\s+(veridia|amore|livork)', '', user_query, flags=re.IGNORECASE).strip()
# #     #         product1 = product_match.group(1)
# #     #         product2 = product_match.group(2)
            
# #     #         intent1 = self.extract_intent(f"{base_query} for {product1}")
# #     #         intent2 = self.extract_intent(f"{base_query} for {product2}")
# #     #         return [intent1, intent2]
        
# #     #     # Pattern 2: Channel with "and" (direct and broker)
# #     #     channel_match = re.search(r'(direct|broker|indirect)\s+and\s+(direct|broker|indirect)', query_lower)
# #     #     if channel_match:
# #     #         base_query = re.sub(r'(direct|broker|indirect)\s+and\s+(direct|broker|indirect)', '', user_query, flags=re.IGNORECASE).strip()
# #     #         channel1 = channel_match.group(1)
# #     #         channel2 = channel_match.group(2)
            
# #     #         intent1 = self.extract_intent(f"{base_query} {channel1}")
# #     #         intent2 = self.extract_intent(f"{base_query} {channel2}")
# #     #         return [intent1, intent2]
        
# #     #     # Pattern 3: Months with "and" (april and june)
# #     #     month_and_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\s+and\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)'
# #     #     month_match = re.search(month_and_pattern, query_lower)
        
# #     #     if month_match and ' to ' not in query_lower and ' till ' not in query_lower:
# #     #         base_query = re.sub(month_and_pattern, '', user_query, flags=re.IGNORECASE).strip()
# #     #         month1 = month_match.group(1)
# #     #         month2 = month_match.group(2)
            
# #     #         intent1 = self.extract_intent(f"{base_query} {month1}")
# #     #         intent2 = self.extract_intent(f"{base_query} {month2}")
# #     #         return [intent1, intent2]
        
# #     #     # Single query
# #     #     return [self.extract_intent(user_query)]

# #     #     # Pattern 4: year with "and" (2024 and 2025)

# #     #             # Pattern 4: year with "and" (2024 and 2025)
# #     #     year_and_pattern = r'\b(20\d{2})\s+and\s+(20\d{2})\b'
# #     #     year_match = re.search(year_and_pattern, query_lower)

# #     #     if year_match:
# #     #         base_query = re.sub(year_and_pattern, '', user_query, flags=re.IGNORECASE).strip()

# #     #         year1 = int(year_match.group(1))
# #     #         year2 = int(year_match.group(2))

# #     #         # Financial year format: April YYYY to March YYYY+1
# #     #         fy1 = f"april {year1} to march {year1 + 1}"
# #     #         fy2 = f"april {year2} to march {year2 + 1}"

# #     #         intent1 = self.extract_intent(f"{base_query} {fy1}")
# #     #         intent2 = self.extract_intent(f"{base_query} {fy2}")

# #     #         return [intent1, intent2]

# #     #     # Single query
# #     #     return [self.extract_intent(user_query)]
    
# #     # running
# #     # def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
# #     #     """
# #     #     Detect if query contains multiple separate requests.
        
# #     #     Handles TWO types of multi-queries:
# #     #     1. Dimension-based: "wave city and wave estate" → split by products
# #     #     2. Date-based: "Q1 and Q2" or "2024 and 2025" → split by dates
        
# #     #     Returns:
# #     #         List of SemanticIntent objects (one per query)
# #     #     """
# #     #     import re
        
# #     #     query_lower = user_query.lower()
        
# #     #     # ========================================
# #     #     # PRIORITY 1: Check for multi-DATE queries FIRST
# #     #     # ========================================
        
# #     #     # Pattern 1: Multiple years (e.g., "2024 and 2025")
# #     #     year_pattern = r'\b(20\d{2})\b'
# #     #     years = re.findall(year_pattern, query_lower)
# #     #     if len(years) > 1:
# #     #         print(f"[MULTI-QUERY] Detected {len(years)} years: {years}")
# #     #         intents = []
# #     #         for year in years:
# #     #             # Create a modified query for each year
# #     #             modified_query = re.sub(r'\b20\d{2}(?:\s+and\s+20\d{2})+\b', year, user_query, flags=re.IGNORECASE)
# #     #             intent = self.extract_intent(modified_query)
# #     #             intents.append(intent)
# #     #         return intents
        
# #     #     # Pattern 2: Multiple quarters (e.g., "Q1 and Q2")
# #     #     quarter_pattern = r'\bq([1-4])\b'
# #     #     quarters = re.findall(quarter_pattern, query_lower)
# #     #     if len(quarters) > 1:
# #     #         print(f"[MULTI-QUERY] Detected {len(quarters)} quarters: Q{quarters}")
# #     #         intents = []
# #     #         for q in quarters:
# #     #             # Create a modified query for each quarter
# #     #             modified_query = re.sub(r'\bq[1-4](?:\s+and\s+q[1-4])+\b | \bvs\b|\bv/s\b', f'q{q}', user_query, flags=re.IGNORECASE)
# #     #             intent = self.extract_intent(modified_query)
# #     #             intents.append(intent)
# #     #         return intents
        
# #     #     # Pattern 3: Multiple months (e.g., "april and may")
# #     #     month_names = ['january', 'february', 'march', 'april', 'may', 'june', 
# #     #                   'july', 'august', 'september', 'october', 'november', 'december',
# #     #                   'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        
# #     #     found_months = []
# #     #     for month in month_names:
# #     #         if month in query_lower:
# #     #             found_months.append(month)
        
# #     #     if len(found_months) > 1:
# #     #         print(f"[MULTI-QUERY] Detected {len(found_months)} months: {found_months}")
# #     #         intents = []
# #     #         for month in found_months:
# #     #             # Create a modified query for each month
# #     #             month_pattern = '|'.join(found_months)
# #     #             modified_query = re.sub(f'\\b({month_pattern})(?:\\s+and\\s+({month_pattern}))+\\b', 
# #     #                                    month, user_query, flags=re.IGNORECASE)
# #     #             intent = self.extract_intent(modified_query)
# #     #             intents.append(intent)
# #     #         return intents
        
# #     #     # ========================================
# #     #     # PRIORITY 2: Check for multi-DIMENSION queries
# #     #     # ========================================
        
# #     #     # Original dimension-based split logic
# #     #     # Keywords that indicate dimension-based multi-query
# #     #     dimension_keywords = {
# #     #         'product': ['wave city', 'wave estate', 'amore', 'livork'],
# #     #         'tower': ['tower', 'block'],
# #     #         'floor': ['floor'],
# #     #         'type': ['apartment', 'shop', 'plot', 'office'],
# #     #     }
        
# #     #     # Check if query contains "and" with dimension values
# #     #     if ' and ' in query_lower:
# #     #         for dim_type, values in dimension_keywords.items():
# #     #             found_values = [v for v in values if v in query_lower]
# #     #             if len(found_values) >= 2:
# #     #                 print(f"[MULTI-QUERY] Detected {len(found_values)} {dim_type} values: {found_values}")
# #     #                 intents = []
# #     #                 for value in found_values:
# #     #                     # Create a query for each dimension value
# #     #                     modified_query = user_query.replace(' and ', ' ').lower()
# #     #                     # Keep only the current value
# #     #                     for other_value in found_values:
# #     #                         if other_value != value:
# #     #                             modified_query = modified_query.replace(other_value, '')
# #     #                     modified_query = modified_query.replace('  ', ' ').strip()
                        
# #     #                     intent = self.extract_intent(modified_query)
# #     #                     # Add filter for the specific value
# #     #                     if not intent.filters:
# #     #                         intent.filters = {}
# #     #                     intent.filters[dim_type] = value
# #     #                     intents.append(intent)
# #     #                 return intents
        
# #     #     # ========================================
# #     #     # DEFAULT: Single query
# #     #     # ========================================
# #     #     return [self.extract_intent(user_query)]
    


# #     def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
# #         """
# #         Detect if a query contains multiple separate requests and split them.

# #         Decision tree (in order):
# #         1. vs / versus / v/s  → comparison (handled by _detect_comparison_query)
# #         2. Range query (from...to, from...till, between...and) → single query always
# #         3. Explicit AND/comma list of same-type items (years, quarters, months, projects) → multi
# #         4. Everything else → single query

# #         Key rule: only split when `and` explicitly joins items of the SAME type.
# #         e.g. "2024 and 2025", "Q1 and Q2", "april and may", "eden and amore"
# #         NOT: "from 2023 to 2024", "from june to oct", "between Q1 and Q2"
# #         """
# #         import re
# #         import datetime as _dt

# #         query_lower = user_query.lower().strip()

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 0: Comparison queries (vs / versus / v/s)
# #         # ──────────────────────────────────────────────────────────────
# #         comparison = self._detect_comparison_query(user_query)
# #         if comparison:
# #             print(f"[MULTI-QUERY] Comparison: {comparison['type']}")
# #             intents = []

# #             if comparison['type'] in ['date_year', 'date_quarter']:
# #                 for i, date_range in enumerate(comparison['date_ranges']):
# #                     clean = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', user_query, flags=re.IGNORECASE)
# #                     for item in comparison['items']:
# #                         clean = re.sub(r'\b' + re.escape(item) + r'\b', '', clean, flags=re.IGNORECASE)
# #                     clean = re.sub(r'\s+', ' ', clean).strip()
# #                     intent = self.extract_intent(clean)
# #                     intent.date_range = date_range
# #                     intent.original_query = f"{clean} {comparison['items'][i]}"
# #                     intents.append(intent)
# #                 return intents

# #             elif comparison['type'] == 'date_month':
# #                 current_year = _dt.datetime.now().year
# #                 year_match = re.search(r'\b(20\d{2})\b', query_lower)
# #                 if year_match:
# #                     current_year = int(year_match.group(1))
# #                 for i, month_num in enumerate(comparison['month_nums']):
# #                     clean = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', user_query, flags=re.IGNORECASE)
# #                     for mn in comparison['items']:
# #                         clean = re.sub(r'\b' + re.escape(mn) + r'\b', '', clean, flags=re.IGNORECASE)
# #                     if year_match:
# #                         clean = re.sub(r'\b' + str(current_year) + r'\b', '', clean)
# #                     clean = re.sub(r'\s+', ' ', clean).strip()
# #                     intent = self.extract_intent(clean)
# #                     intent.date_range = "custom_range"
# #                     intent.custom_dates = [{"month_num": month_num, "year": current_year}]
# #                     intent.original_query = f"{clean} {comparison['items'][i]} {current_year}"
# #                     intents.append(intent)
# #                 return intents

# #             elif comparison['type'] == 'dimension':
# #                 base = user_query.lower()
# #                 for item in comparison['items']:
# #                     base = base.replace(item.lower(), '')
# #                 base = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', base)
# #                 base = re.sub(r'\s+', ' ', base).strip()
# #                 for i, value in enumerate(comparison['items']):
# #                     intent = self.extract_intent(base)
# #                     if comparison['dimension'] in intent.dimensions:
# #                         intent.dimensions.remove(comparison['dimension'])
# #                     if not intent.filters:
# #                         intent.filters = {}
# #                     intent.filters[comparison['dimension']] = value
# #                     label = comparison.get('labels', comparison['items'])[i]
# #                     intent.original_query = f"{base} - {label}"
# #                     intents.append(intent)
# #                 return intents

# #             elif comparison['type'] == 'generic':
# #                 for item in comparison['items']:
# #                     intents.append(self.extract_intent(item))
# #                 return intents

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 1: Range queries are ALWAYS single queries
# #         # Patterns: "from X to Y", "from X till Y", "between X and Y"
# #         # ──────────────────────────────────────────────────────────────
# #         range_patterns = [
# #             r'\bfrom\b.{1,60}\bto\b',
# #             r'\bfrom\b.{1,60}\btill\b',
# #             r'\bfrom\b.{1,60}\buntil\b',
# #             r'\bbetween\b.{1,60}\band\b',
# #             r'\bthrough\b',
# #         ]
# #         if any(re.search(p, query_lower) for p in range_patterns):
# #             print("[MULTI-QUERY] Range query — single intent")
# #             return [self.extract_intent(user_query)]

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 2: AND/comma list of YEARS  e.g. "2023 and 2024"
# #         # ──────────────────────────────────────────────────────────────
# #         if re.search(r'\b20\d{2}\b(?:\s*,\s*|\s+and\s+)\b20\d{2}\b', query_lower):
# #             years = re.findall(r'\b(20\d{2})\b', query_lower)
# #             print(f"[MULTI-QUERY] Year list: {years}")
# #             intents = []
# #             for year in years:
# #                 clean = re.sub(r'\b20\d{2}\b(?:\s*,\s*|\s+and\s+)\b20\d{2}\b', year, user_query, flags=re.IGNORECASE)
# #                 intents.append(self.extract_intent(clean))
# #             return intents

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 3: AND/comma list of QUARTERS  e.g. "Q1 and Q2"
# #         # ──────────────────────────────────────────────────────────────
# #         if re.search(r'\bq([1-4])\b(?:\s*,\s*|\s+and\s+)\bq([1-4])\b', query_lower):
# #             quarters = re.findall(r'\bq([1-4])\b', query_lower)
# #             print(f"[MULTI-QUERY] Quarter list: Q{quarters}")
# #             intents = []
# #             for q in quarters:
# #                 clean = re.sub(r'\bq[1-4]\b(?:\s*,\s*|\s+and\s+)\bq[1-4]\b', f'q{q}', user_query, flags=re.IGNORECASE)
# #                 intents.append(self.extract_intent(clean))
# #             return intents

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 4: AND/comma list of MONTHS  e.g. "april and may"
# #         # Only when months are DIRECTLY joined by "and" or ","
# #         # ──────────────────────────────────────────────────────────────
# #         _months = ('january|february|march|april|may|june|july|august|september'
# #                    '|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec')
# #         if re.search(rf'\b({_months})\b(?:\s*,\s*|\s+and\s+)\b({_months})\b', query_lower):
# #             found = [m for m in _months.split('|') if re.search(r'\b' + m + r'\b', query_lower)]
# #             # deduplicate preserving order
# #             seen, unique = set(), []
# #             for m in found:
# #                 if m not in seen:
# #                     seen.add(m); unique.append(m)
# #             if len(unique) >= 2:
# #                 print(f"[MULTI-QUERY] Month list: {unique}")
# #                 intents = []
# #                 all_re = '|'.join(re.escape(m) for m in unique)
# #                 for month in unique:
# #                     clean = re.sub(
# #                         rf'\b({all_re})\b(?:\s*,\s*|\s+and\s+)\b({all_re})\b',
# #                         month, user_query, flags=re.IGNORECASE
# #                     )
# #                     intents.append(self.extract_intent(clean))
# #                 return intents

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 5: AND list of named DIMENSION VALUES
# #         # e.g. "wave city and wave estate", "broker and direct", "eden and amore"
# #         # Each entry: dimension_key -> list of (trigger_phrase, filter_value) tuples
# #         # filter_value is what gets passed to _build_single_value_filter (LIKE match)
# #         #
# #         # IMPORTANT: ALL matched trigger phrases are stripped from the base query
# #         # before passing to extract_intent. This prevents the LLM from misreading
# #         # project/product names as dimension keywords (e.g. "city" → dimension).
# #         # The filter is applied explicitly after intent extraction.
# #         # ──────────────────────────────────────────────────────────────
# #         if ' and ' in query_lower or ',' in query_lower:
# #             dim_values = {
# #                 # Project / Sales Org  e.g. "wave city and wave estate"
# #                 'sales_org_desc': [
# #                     ('wave city', 'Wave City'),
# #                     ('wave estate', 'Wave Estate'),
# #                     ('wmcc', 'WMCC'),
# #                 ],
# #                 # Product / Sales Group  e.g. "eden and amore", "fsi and hssc", "veridia and eligo"
# #                 'sales_group_desc': [
# #                     ('eligo', 'ELIGO'),
# #                     ('eden', 'EDEN'),
# #                     ('amore', 'AMORE'),
# #                     ('livork', 'LIVORK'),
# #                     ('veridia 7', 'VERIDIA-7'),
# #                     ('veridia 6', 'VERIDIA-6'),
# #                     ('veridia 5', 'VERIDIA-5'),
# #                     ('veridia 4', 'VERIDIA-4'),
# #                     ('veridia 3', 'VERIDIA-3'),
# #                     ('veridia', 'VERIDIA'),
# #                     ('edenia', 'EDENIA'),
# #                     ('elegantia', 'ELEGANTIA'),
# #                     ('eminence', 'EMINENCE'),
# #                     ('irenia', 'IRENIA'),
# #                     ('trucia', 'TRUCIA'),
# #                     ('vasilia', 'VASILIA'),
# #                     ('mayfair park', 'MAYFAIR PARK'),
# #                     ('mayfair', 'MAYFAIR PARK'),
# #                     ('harmony greens', 'HARMONY GREENS'),
# #                     ('wave galleria', 'WAVE GALLERIA'),
# #                     ('galleria', 'WAVE GALLERIA'),
# #                     ('wave garden', 'WAVE GARDEN'),
# #                     ('wave floor 99', 'WAVE FLOOR 99'),
# #                     ('wave floor 85', 'WAVE FLOOR 85'),
# #                     ('wave floor', 'WAVE FLOOR'),
# #                     ('dream homes', 'DREAM HOMES'),
# #                     ('dream bazaar', 'DREAM BAZAAR'),
# #                     ('villas', 'VILLAS'),
# #                     ('armonia villa', 'ARMONIA VILLA'),
# #                     ('armonia', 'ARMONIA VILLA'),
# #                     ('business square', 'WAVE BUSSINESS SQUARE'),
# #                     ('wave estate gh2', 'WAVE ESTATE, GH2 PH2'),
# #                     ('executive floors', 'EXECUTIVE FLOORS'),
# #                     ('prime floors', 'PRIME FLOORS'),
# #                     ('new plots', 'NEW PLOTS'),
# #                     ('old plots', 'OLD PLOTS'),
# #                     ('commercial plots', 'COMMERCIAL PLOTS'),
# #                     ('residential plots', 'PLOTS-RES'),
# #                     ('fsi', 'FSI'),
# #                     ('hssc', 'HSSC'),
# #                     ('institutional', 'INSTITUTIONAL'),
# #                     ('metro mart', 'METRO MART'),
# #                     ('swamanorath', 'SWAMANORATH'),
# #                     ('wave bussiness square', 'WAVE BUSSINESS SQUARE'),
# #                     ('wbt 1', 'WBT 1'),
# #                     ('wbt a', 'WBT A'),
# #                     ('sco', 'SCO'),
# #                     ('comm booth', 'COMM BOOTH'),
# #                     ('ews p2', 'EWS_P2'),
# #                     ('lig p2', 'LIG_P2'),
# #                 ],
# #                 # Distribution Channel  e.g. "broker and direct"
# #                 'dist_channel_desc': [
# #                     ('broker', 'Broker'),
# #                     ('direct', 'Direct'),
# #                     ('walk-in', 'Direct'),
# #                     ('walkin', 'Direct'),
# #                     ('channel partner', 'Broker'),
# #                     ('referral', 'Referral'),
# #                 ],
# #                 # Booking Type  e.g. "fresh and transfer"
# #                 'booking_type': [
# #                     ('fresh', 'Fresh'),
# #                     ('transfer', 'Transfer'),
# #                     ('resale', 'Resale'),
# #                 ],
# #             }
# #             for dim, value_tuples in dim_values.items():
# #                 # Find which trigger phrases appear in the query
# #                 matched = []
# #                 seen_filter_vals = set()
# #                 for trigger, filter_val in value_tuples:
# #                     if re.search(r'\b' + re.escape(trigger) + r'\b', query_lower):
# #                         if filter_val not in seen_filter_vals:
# #                             matched.append((trigger, filter_val))
# #                             seen_filter_vals.add(filter_val)

# #                 if len(matched) >= 2:
# #                     print(f"[MULTI-QUERY] Dimension AND-list ({dim}): {[m[1] for m in matched]}")
# #                     intents = []

# #                     # Build a base query with ALL matched trigger phrases removed.
# #                     # This prevents the LLM from misinterpreting product/project names
# #                     # as dimension names (e.g. "city" in "wave city" → dimension error).
# #                     base_clean = query_lower
# #                     for trigger, _ in matched:
# #                         base_clean = re.sub(r'\b' + re.escape(trigger) + r'\b', '', base_clean)
# #                     base_clean = re.sub(r'\band\b', '', base_clean)
# #                     base_clean = re.sub(r'\bof\b\s*$|\bof\b\s+(?=and|,|$)', '', base_clean)
# #                     base_clean = re.sub(r'\s+', ' ', base_clean).strip()

# #                     # Extract intent once from the clean base (no product names in it)
# #                     base_intent = self.extract_intent(base_clean)

# #                     for trigger, filter_val in matched:
# #                         # Copy base intent and apply this specific filter
# #                         import copy
# #                         intent = copy.deepcopy(base_intent)
# #                         if not intent.filters:
# #                             intent.filters = {}
# #                         intent.filters[dim] = filter_val
# #                         intent.original_query = f"{base_clean} - {filter_val}"
# #                         intents.append(intent)
# #                     return intents

# #         # ──────────────────────────────────────────────────────────────
# #         # STEP 6: AND list of FLOORS  e.g. "10th floor and 8th floor"
# #         # Patterns: "Nth floor and Mth floor", "floor N and floor M"
# #         # ──────────────────────────────────────────────────────────────
# #         if 'floor' in query_lower and ' and ' in query_lower:
# #             floor_and_patterns = [
# #                 r'(\d+)(?:st|nd|rd|th)?\s+floor\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor',  # "10th floor and 8th floor"
# #                 r'floor\s+(\d+)\s+and\s+floor\s+(\d+)',                                   # "floor 10 and floor 8"
# #                 r'(\d+)(?:st|nd|rd|th)?\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor',           # "10th and 8th floor"
# #             ]
# #             for fpat in floor_and_patterns:
# #                 fm = re.search(fpat, query_lower)
# #                 if fm:
# #                     floor1, floor2 = fm.group(1), fm.group(2)
# #                     print(f"[MULTI-QUERY] Floor list: [{floor1}, {floor2}]")
# #                     intents = []
# #                     # Build a base query with the floor list removed
# #                     base_clean = re.sub(fpat, '', query_lower, flags=re.IGNORECASE)
# #                     base_clean = re.sub(r'\s+', ' ', base_clean).strip()
# #                     import copy
# #                     base_intent = self.extract_intent(base_clean)
# #                     for floor_num in [floor1, floor2]:
# #                         intent = copy.deepcopy(base_intent)
# #                         if not intent.filters:
# #                             intent.filters = {}
# #                         intent.filters['floor'] = floor_num
# #                         intent.original_query = f"{base_clean} - floor {floor_num}"
# #                         intents.append(intent)
# #                     return intents

# #         # ──────────────────────────────────────────────────────────────
# #         # DEFAULT: single query
# #         # ──────────────────────────────────────────────────────────────
# #         return [self.extract_intent(user_query)]


# #     def _detect_multi_date_query(self, query: str) -> List[Dict]:
# #         """
# #         Detect if query requests multiple separate time periods.
        
# #         Examples:
# #             "total sales in 2024 and 2025" → [{"year": 2024}, {"year": 2025}]
# #             "sales in Q1 and Q2" → [{"quarter": 1}, {"quarter": 2}]
# #             "sales in april and may" → [{"month": 4}, {"month": 5}]
            
        
# #         Returns:
# #             List of date dictionaries, or empty list if single period
# #         """
# #         query_lower = query.lower()
        
# #         # Pattern 1: Multiple years (e.g., "2024 and 2025", "in 2024, 2025")
# #         year_pattern = r'\b(20\d{2})\b'
# #         years = re.findall(year_pattern, query_lower)
# #         if len(years) > 1:
# #             return [{"year": int(y)} for y in years]
        
# #         # Pattern 2: Multiple quarters (e.g., "Q1 and Q2", "Q1, Q2, Q3")
# #         quarter_pattern = r'\bq([1-4])\b'
# #         quarters = re.findall(quarter_pattern, query_lower)
# #         if len(quarters) > 1:
# #             return [{"quarter": int(q)} for q in quarters]
        
# #         # Pattern 3: Multiple months (e.g., "april and may", "jan, feb, mar")
# #         month_names = {
# #             'january': 1, 'jan': 1,
# #             'february': 2, 'feb': 2,
# #             'march': 3, 'mar': 3,
# #             'april': 4, 'apr': 4,
# #             'may': 5,
# #             'june': 6, 'jun': 6,
# #             'july': 7, 'jul': 7,
# #             'august': 8, 'aug': 8,
# #             'september': 9, 'sep': 9, 'sept': 9,
# #             'october': 10, 'oct': 10,
# #             'november': 11, 'nov': 11,
# #             'december': 12, 'dec': 12
# #         }
        
# #         found_months = []
# #         for month_name, month_num in month_names.items():
# #             if month_name in query_lower:
# #                 found_months.append(month_num)
        
# #         # Remove duplicates while preserving order
# #         found_months = list(dict.fromkeys(found_months))
        
# #         if len(found_months) > 1:
# #             return [{"month_num": m} for m in found_months]
        
# #         return []
    

# #     def _detect_comparison_query(self, user_query: str) -> Optional[Dict]:
# #         """
# #         Detect if query is a comparison using "vs", "v/s", or "versus".
        
# #         Examples:
# #             "2024 vs 2025" → {"type": "date", "items": ["2024", "2025"]}
# #             "broker vs direct" → {"type": "dimension", "items": ["broker", "direct"]}
# #             "Q1 vs Q2" → {"type": "date", "items": ["Q1", "Q2"]}
        
# #         Returns:
# #             Dictionary with comparison type and items, or None if not a comparison
# #         """
# #         import re
        
# #         query_lower = user_query.lower()
        
# #         # Check for comparison keywords
# #         comparison_patterns = [
# #             r'\bv/s\b',
# #             r'\bvs\b',
# #             r'\bversus\b',
# #             r'\bv\.s\b',
# #             r'\bcompare\b'
# #         ]
        
# #         has_comparison = False
# #         for pattern in comparison_patterns:
# #             if re.search(pattern, query_lower):
# #                 has_comparison = True
# #                 break
        
# #         if not has_comparison:
# #             return None
        
# #         print(f"[COMPARISON] Detected comparison query: {user_query}")
        
# #         # ========================================
# #         # Type 1: DATE COMPARISONS
# #         # ========================================
        
# #         # Years (2024 vs 2025)
# #         year_pattern = r'\b(20\d{2})\b'
# #         years = re.findall(year_pattern, query_lower)
# #         if len(years) == 2:
# #             print(f"[COMPARISON] Year comparison: {years}")
# #             return {
# #                 "type": "date_year",
# #                 "items": years,
# #                 "date_ranges": [f"fy_{y}" for y in years]
# #             }
        
# #         # Quarters (Q1 vs Q2)
# #         quarter_pattern = r'\bq([1-4])\b'
# #         quarters = re.findall(quarter_pattern, query_lower)
# #         if len(quarters) == 2:
# #             print(f"[COMPARISON] Quarter comparison: Q{quarters}")
# #             return {
# #                 "type": "date_quarter",
# #                 "items": [f"Q{q}" for q in quarters],
# #                 "date_ranges": [f"q{q}" for q in quarters]
# #             }
        
# #         # Months (april vs may)
# #         month_map = {
# #             'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
# #             'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
# #             'may': 5, 'june': 6, 'jun': 6,
# #             'july': 7, 'jul': 7, 'august': 8, 'aug': 8,
# #             'september': 9, 'sep': 9, 'sept': 9,
# #             'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
# #             'december': 12, 'dec': 12
# #         }
        
# #         found_months = []
# #         for month_name, month_num in month_map.items():
# #             if month_name in query_lower:
# #                 found_months.append((month_name, month_num))
        
# #         if len(found_months) == 2:
# #             print(f"[COMPARISON] Month comparison: {[m[0] for m in found_months]}")
# #             return {
# #                 "type": "date_month",
# #                 "items": [m[0] for m in found_months],
# #                 "month_nums": [m[1] for m in found_months]
# #             }
        
# #         # ========================================
# #         # Type 2: DIMENSION VALUE COMPARISONS
# #         # ========================================
        
# #         # Split query by comparison keyword
# #         # split_pattern = r'\s+(?:v/s|vs|versus|v\.s)\s+'
# #         # parts = re.split(split_pattern, query_lower, flags=re.IGNORECASE)
        
# #         # if len(parts) == 2:
# #         #     left = parts[0].strip()
# #         #     right = parts[1].strip()
            
# #         #     # Common dimension value patterns
# #         #     dimension_patterns = {
# #         #         # Channel
# #         #         'dist_channel_desc': {
# #         #             'broker': ['broker', 'channel partner', 'agent'],
# #         #             'direct': ['direct', 'walk-in', 'walkin', 'self']
# #         #         },
# #         #         # Division
# #         #         'division_desc': {
# #         #             'residential': ['residential', 'housing', 'apartment'],
# #         #             'commercial': ['commercial', 'office', 'shop', 'retail']
# #         #         },
# #         #         # Product
# #         #         'sales_group_desc': {
# #         #             'wave city': ['wave city', 'wavecity'],
# #         #             'wave estate': ['wave estate', 'waveestate'],
# #         #             'amore': ['amore'],
# #         #             'livork': ['livork']
# #         #         },
# #         #         # Booking Type
# #         #         'booking_type': {
# #         #             'fresh': ['fresh', 'new booking'],
# #         #             'transfer': ['transfer', 'transferred']
# #         #         }
# #         #     }
            
# #         #     # Check which dimension this comparison belongs to
# #         #     for dim_name, value_patterns in dimension_patterns.items():
# #         #         left_match = None
# #         #         right_match = None
                
# #         #         for value_key, patterns in value_patterns.items():
# #         #             for pattern in patterns:
# #         #                 if pattern in left:
# #         #                     left_match = value_key
# #         #                 if pattern in right:
# #         #                     right_match = value_key
                
# #         #         if left_match and right_match:
# #         #             print(f"[COMPARISON] Dimension comparison: {dim_name} - {left_match} vs {right_match}")
# #         #             return {
# #         #                 "type": "dimension",
# #         #                 "dimension": dim_name,
# #         #                 "items": [left_match, right_match]
# #         #             }


# #         # ========================================
# #         # Type 2: DIMENSION VALUE COMPARISONS
# #         # ========================================
        
# #         # Split query by comparison keyword
# #         split_pattern = r'\s+(?:v/s|vs|versus|v\.s)\s+'
# #         parts = re.split(split_pattern, query_lower, flags=re.IGNORECASE)
        
# #         if len(parts) == 2:
# #             left = parts[0].strip()
# #             right = parts[1].strip()
            
# #             # Common dimension value patterns
# #             dimension_patterns = {
# #                 # Channel - MOST IMPORTANT
# #                 'dist_channel_desc': {
# #                     'patterns': {
# #                         'broker': ['broker', 'channel partner', 'agent', 'brokers'],
# #                         'direct': ['direct', 'walk-in', 'walkin', 'self', 'walk in']
# #                     },
# #                     'priority': 1  # Check this first
# #                 },
# #                 # Division
# #                 'division_desc': {
# #                     'patterns': {
# #                         'residential': ['residential', 'housing', 'apartment', 'resi'],
# #                         'commercial': ['commercial', 'office', 'shop', 'retail', 'comm']
# #                     },
# #                     'priority': 2
# #                 },
# #                 # Product
# #                 'sales_group_desc': {
# #                     'patterns': {
# #                         'Wave City': ['wave city', 'wavecity'],
# #                         'Wave Estate': ['wave estate', 'waveestate'],
# #                         'Amore': ['amore'],
# #                         'Livork': ['livork']
# #                     },
# #                     'priority': 3
# #                 },
# #                 # Booking Type
# #                 'booking_type': {
# #                     'patterns': {
# #                         'Fresh': ['fresh', 'new booking', 'new'],
# #                         'Transfer': ['transfer', 'transferred']
# #                     },
# #                     'priority': 4
# #                 }
# #             }
            
# #             # Sort by priority
# #             sorted_patterns = sorted(
# #                 dimension_patterns.items(), 
# #                 key=lambda x: x[1].get('priority', 999)
# #             )
            
# #             # Check which dimension this comparison belongs to
# #             for dim_name, dim_config in sorted_patterns:
# #                 left_match = None
# #                 right_match = None
                
# #                 value_patterns = dim_config['patterns']
                
# #                 for value_key, patterns in value_patterns.items():
# #                     for pattern in patterns:
# #                         if pattern in left:
# #                             left_match = value_key
# #                         if pattern in right:
# #                             right_match = value_key
                
# #                 if left_match and right_match:
# #                     print(f"[COMPARISON] Dimension comparison: {dim_name} - '{left_match}' vs '{right_match}'")
# #                     return {
# #                         "type": "dimension",
# #                         "dimension": dim_name,
# #                         "items": [left_match, right_match],
# #                         "labels": [left_match, right_match]
# #                     }
        
# #         # ========================================
# #         # Type 3: GENERIC COMPARISON (fallback)
# #         # ========================================
        
# #         # If we detected comparison keywords but couldn't classify the type,
# #         # treat it as a generic multi-query comparison
# #         if len(parts) == 2:
# #             print(f"[COMPARISON] Generic comparison: {parts}")
# #             return {
# #                 "type": "generic",
# #                 "items": parts
# #             }
        
# #         return None
    


# #     def _detect_cancellation_query(self, user_query: str) -> Optional[SemanticIntent]:
# #         """
# #         Detect cancellation-related queries and build the correct intent.

# #         Always applies:
# #         - metric: cancelled_sales
# #         - mandatory filter: Customer_Type = 'cancelled'  (from metric definition)
# #         - dimension: cancellation_reason (Description column) by default

# #         Also supports filters by:
# #         - payer_name, sold_to_name, sales_order, sales_group_desc, project, product
# #         """
# #         query_lower = user_query.lower()

# #         cancellation_triggers = [
# #             "cancellation reason", "reason for cancellation", "cancel reason",
# #             "why cancelled", "why was cancelled", "cancelled reason",
# #             "cancellation reason wise", "reason of cancellation",
# #         ]

# #         if not any(trigger in query_lower for trigger in cancellation_triggers):
# #             return None

# #         print(f"[CANCELLATION] Detected cancellation reason query")

# #         # Always group by cancellation reason (Description column)
# #         dimensions = ["cancellation_reason"]

# #         # Check for additional dimension filters
# #         filters = {}

# #         # By payer
# #         if any(kw in query_lower for kw in ["payer", "paying customer"]):
# #             dimensions.append("payer_name")

# #         # By sold to / customer name
# #         if any(kw in query_lower for kw in ["sold to", "customer", "buyer"]):
# #             dimensions.append("sold_to_name")

# #         # By sales order number — extract it
# #         order_match = re.search(r'\b(\d{7,12})\b', query_lower)
# #         if order_match:
# #             filters["sales_order"] = order_match.group(1)

# #         # By project
# #         if any(kw in query_lower for kw in ["project", "wave city", "wave estate", "wmcc"]):
# #             dimensions.append("sales_org_desc")

# #         # By product / sales group (eden, amore, livork)
# #         product_map = {"eden": "Eden", "amore": "Amore", "livork": "Livork"}
# #         for kw, val in product_map.items():
# #             if kw in query_lower:
# #                 filters["sales_group_desc"] = val

# #         # By tower
# #         if "tower" in query_lower:
# #             dimensions.append("tower")

# #         # By broker
# #         if "broker" in query_lower:
# #             dimensions.append("broker_name")

# #         # Extract date
# #         custom_dates, date_range = self._extract_custom_dates_enhanced(user_query)

# #         intent = SemanticIntent(
# #             metric="cancelled_sales",
# #             dimensions=dimensions,
# #             date_range=date_range or "current_financial_year",
# #             custom_dates=custom_dates or [],
# #             filters=filters if filters else None,
# #             original_query=user_query,
# #         )

# #         return intent



# #     # =====================================================
# #     # CUSTOM DATE EXTRACTION (NEW!)
# #     # =====================================================
    
# #     def _extract_custom_dates_enhanced(self, query: str) -> Tuple[List[Dict], str]:
# #         """
# #         Extract custom dates from natural language query with proper Financial Year (FY) handling.
# #         FY runs from April to March (Apr 2025 - Mar 2026 is current)
        
# #         Handles:
# #         1. Date ranges: "from 23 jan to 5 feb", "from 20 jan 2022 to 2 feb 2023"
# #         2. Specific dates: "20 jan", "23 march 2025"
# #         3. From patterns: "from may" (may to end of current FY = March)
# #         4. From-till patterns: "from may till 15 jan", "from sep till 15 feb"
# #         5. From-to patterns: "from sep to feb", "from april to sep 2024"
# #         6. Till patterns: "till 13 dec" (april to 13 dec)
# #         7. Month ranges: "april to june"
# #         8. Quarters: "q1", "q2", "q3", "q4", "q1 2024", "q4 2023"
        
# #         Returns: (custom_dates_list, date_range_type)
# #         """
# #         query_lower = query.lower().strip()
# #         today = datetime.datetime.now()
# #         current_year = today.year
# #         current_month = today.month
        
# #         print(f"[EXTRACT_ENHANCED] Input query: '{query}' (lowercased: '{query_lower}')")
        
# #         # *** CRITICAL: Determine correct Financial Year (Apr-Mar) ***
# #         # Current date: Feb 9, 2026 → FY is April 2025 - March 2026
# #         # Logic: If month is Apr-Dec (>=4), FY starts in current year
# #         #        If month is Jan-Mar (<4), FY started in previous year
# #         if current_month >= 4:
# #             fy_start_year = current_year
# #             fy_end_year = current_year + 1
# #         else:
# #             # We're in Jan-Mar, so FY started last year
# #             fy_start_year = current_year - 1
# #             fy_end_year = current_year
        
# #         print(f"[FY_CALC] Today: {today.date()}, Month: {current_month}, FY: {fy_start_year}-{fy_end_year}")
        
# #         def get_fy_year_for_month(month_num: int, explicit_year: int = None) -> int:
# #             """
# #             Get the correct financial year for a given month.
# #             If explicit_year provided, use it. Otherwise, infer from FY context.
            
# #             Examples (assuming current FY is 2025-2026, i.e., Apr 2025 - Mar 2026):
# #             - April-Dec: return 2025 (fy_start_year)
# #             - Jan-Mar: return 2026 (fy_end_year)
# #             """
# #             if explicit_year:
# #                 return explicit_year
            
# #             # Months Apr(4) through Dec(12) belong to fy_start_year
# #             # Months Jan(1) through Mar(3) belong to fy_end_year
# #             if month_num >= 4:
# #                 return fy_start_year
# #             else:
# #                 return fy_end_year
        
# #         # ========================================
# #         # PATTERN 0 (PRIORITY): Financial Quarter (e.g., "q1", "q2", "q3", "q4", "q4 2024", "q1_2025")
# #         # Check FIRST to avoid conflicts with month patterns
# #         # FY Quarters: Q1=Apr-Jun(4-6), Q2=Jul-Sep(7-9), Q3=Oct-Dec(10-12), Q4=Jan-Mar(1-3)
# #         # ========================================
# #         # PRE-PATTERN 0: "quarter N YYYY" / "quarter N" (written-out quarter)
# #         # Examples: "quarter 3 2023" → Q3 FY2023, "3rd quarter 2022" → Q3 FY2022
# #         # Covers: "quarter 1", "quarter 2", "1st quarter", "2nd quarter" etc.
# #         written_quarter_pattern = r'(?:(?:1st|2nd|3rd|4th|first|second|third|fourth)\s+quarter|quarter\s+(?:1|2|3|4|one|two|three|four))(?:\s+(\d{4}))?'
# #         written_q_match = re.search(written_quarter_pattern, query_lower)
# #         if written_q_match:
# #             # Map words to quarter numbers
# #             q_word_map = {
# #                 "1st": 1, "first": 1, "one": 1, "1": 1,
# #                 "2nd": 2, "second": 2, "two": 2, "2": 2,
# #                 "3rd": 3, "third": 3, "three": 3, "3": 3,
# #                 "4th": 4, "fourth": 4, "four": 4, "4": 4,
# #             }
# #             raw = written_q_match.group()
# #             # Extract the number word
# #             q_num = None
# #             for token in re.split(r'\s+', raw):
# #                 if token in q_word_map:
# #                     q_num = q_word_map[token]
# #                     break
# #             year_explicit = int(written_q_match.group(1)) if written_q_match.group(1) else None

# #             if q_num:
# #                 quarter_months = {1: (4, 6), 2: (7, 9), 3: (10, 12), 4: (1, 3)}
# #                 start_month, end_month = quarter_months[q_num]
# #                 if year_explicit:
# #                     if q_num in (1, 2, 3):
# #                         start_year = end_year = year_explicit
# #                     else:
# #                         start_year = end_year = year_explicit + 1
# #                 else:
# #                     if q_num in (1, 2, 3):
# #                         start_year = end_year = fy_start_year
# #                     else:
# #                         start_year = end_year = fy_end_year

# #                 print(f"[PRE-PATTERN 0 - WRITTEN QUARTER] Q{q_num} year={year_explicit or 'current FY'}: {start_month}/{start_year} to {end_month}/{end_year}")
# #                 return [
# #                     {"month_num": start_month, "year": start_year},
# #                     {"month_num": end_month, "year": end_year}
# #                 ], "custom_range"

# #         # ========================================
# #         # Try pattern with optional space or underscore + year
# #         quarter_pattern = r'\bq([1-4])(?:[\s_]+(\d{4}))?\b'
# #         print(f"[PATTERN 0] Searching with regex: {quarter_pattern}")
# #         quarter_match = re.search(quarter_pattern, query_lower)
        
# #         if quarter_match:
# #             q_num = int(quarter_match.group(1))  # 1, 2, 3, or 4
# #             year_explicit = int(quarter_match.group(2)) if quarter_match.group(2) else None
            
# #             print(f"[PATTERN 0 - MATCH] YES! Matched: q{q_num}, year_explicit: {year_explicit}, full match: '{quarter_match.group()}'")
            
# #             # Map quarter to months (based on Financial Year Apr-Mar)
# #             quarter_months = {
# #                 1: (4, 5, 6),    # Q1: Apr, May, Jun
# #                 2: (7, 8, 9),    # Q2: Jul, Aug, Sep
# #                 3: (10, 11, 12), # Q3: Oct, Nov, Dec
# #                 4: (1, 2, 3),    # Q4: Jan, Feb, Mar
# #             }
            
# #             start_month, mid_month, end_month = quarter_months[q_num]
            
# #             # Determine year(s) for the quarter
# #             if year_explicit:
# #                 # User specified explicit year (e.g., "q1 2024", "q4 2023")
# #                 # The year refers to the Financial Year (FY)
# #                 # Q1-Q3 of FY 2024 = Apr-Dec 2024
# #                 # Q4 of FY 2024 = Jan-Mar 2025
# #                 if q_num in (1, 2, 3):
# #                     # Q1-Q3: use exact year (Apr-Dec of that year)
# #                     start_year = year_explicit
# #                     end_year = year_explicit
# #                 else:
# #                     # Q4: Jan-Mar of the NEXT year (Q4 of FY 2024 = Jan-Mar 2025)
# #                     start_year = year_explicit + 1
# #                     end_year = year_explicit + 1
# #             else:
# #                 # No explicit year - use Financial Year context
# #                 # Current FY: Apr fy_start_year - Mar fy_end_year
# #                 if q_num in (1, 2, 3):
# #                     # Q1-Q3 (Apr-Dec): use FY start year
# #                     start_year = fy_start_year
# #                     end_year = fy_start_year
# #                 else:
# #                     # Q4 (Jan-Mar): use FY end year
# #                     start_year = fy_end_year
# #                     end_year = fy_end_year
            
# #             print(f"[PATTERN 0 - QUARTER] q{q_num}: months {start_month}-{end_month}, year {start_year}-{end_year}")
# #             print(f"[PATTERN 0 - DATES] Start: month={start_month}/year={start_year}, End: month={end_month}/year={end_year}")
# #             return [
# #                 {"month_num": start_month, "year": start_year},
# #                 {"month_num": end_month, "year": end_year}
# #             ], "custom_range"
# #         else:
# #             print(f"[PATTERN 0 - NO MATCH] Quarter pattern did not match")
        
# #         # PATTERN 1: "from DATE to DATE" (e.g., "from 23 jan to 5 feb", "from 20 jan 2022 to 2 feb 2023")
# #         from_date_to_date = r'from\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+to\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         from_date_to_date_match = re.search(from_date_to_date, query_lower)
        
# #         if from_date_to_date_match:
# #             day1 = int(from_date_to_date_match.group(1))
# #             month1_str = from_date_to_date_match.group(2)
# #             month1_num = self.MONTH_NAMES.get(month1_str, 1)
# #             year1_explicit = int(from_date_to_date_match.group(3)) if from_date_to_date_match.group(3) else None
            
# #             day2 = int(from_date_to_date_match.group(4))
# #             month2_str = from_date_to_date_match.group(5)
# #             month2_num = self.MONTH_NAMES.get(month2_str, 12)
# #             year2_explicit = int(from_date_to_date_match.group(6)) if from_date_to_date_match.group(6) else None
            
# #             # Intelligent year assignment
# #             if year1_explicit and year2_explicit:
# #                 year1 = year1_explicit
# #                 year2 = year2_explicit
# #             elif year1_explicit and not year2_explicit:
# #                 year1 = year1_explicit
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
# #             elif year2_explicit and not year1_explicit:
# #                 year2 = year2_explicit
# #                 if month1_num <= month2_num:
# #                     year1 = year2
# #                 else:
# #                     year1 = year2 - 1
# #             else:
# #                 year1 = get_fy_year_for_month(month1_num)
# #                 year2 = get_fy_year_for_month(month2_num)
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
            
# #             print(f"[PATTERN 1] from date to date: {day1}/{month1_num}/{year1} to {day2}/{month2_num}/{year2}")
# #             return [
# #                 {"day": day1, "month_num": month1_num, "year": year1},
# #                 {"day": day2, "month_num": month2_num, "year": year2}
# #             ], "custom_range"
        
# #         # PATTERN 2: "from MONTH till DATE" (e.g., "from may till 15 jan", "from sep till 15 feb")
# #         from_month_till_date = r'from\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+till\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         from_month_till_date_match = re.search(from_month_till_date, query_lower)
        
# #         if from_month_till_date_match:
# #             month1_str = from_month_till_date_match.group(1)
# #             month1_num = self.MONTH_NAMES.get(month1_str, 1)
# #             year1_explicit = int(from_month_till_date_match.group(2)) if from_month_till_date_match.group(2) else None
            
# #             day2 = int(from_month_till_date_match.group(3))
# #             month2_str = from_month_till_date_match.group(4)
# #             month2_num = self.MONTH_NAMES.get(month2_str, 12)
# #             year2_explicit = int(from_month_till_date_match.group(5)) if from_month_till_date_match.group(5) else None
            
# #             # Intelligent year assignment
# #             if year1_explicit and year2_explicit:
# #                 year1 = year1_explicit
# #                 year2 = year2_explicit
# #             elif year1_explicit and not year2_explicit:
# #                 year1 = year1_explicit
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
# #             elif year2_explicit and not year1_explicit:
# #                 year2 = year2_explicit
# #                 if month1_num <= month2_num:
# #                     year1 = year2
# #                 else:
# #                     year1 = year2 - 1
# #             else:
# #                 year1 = get_fy_year_for_month(month1_num)
# #                 year2 = get_fy_year_for_month(month2_num)
# #                 if not year2_explicit and month2_num < month1_num:
# #                     year2 = year1 + 1
            
# #             print(f"[PATTERN 2] from month till date: month {month1_num}/{year1} till {day2}/{month2_num}/{year2}")
# #             return [
# #                 {"month_num": month1_num, "year": year1},
# #                 {"day": day2, "month_num": month2_num, "year": year2}
# #             ], "custom_range"
        
# #         # PATTERN 3: "from MONTH to MONTH" (e.g., "from sep to feb", "from april to sep 2024")
# #         from_month_to_month = r'from\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+to\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         from_month_to_month_match = re.search(from_month_to_month, query_lower)
        
# #         if from_month_to_month_match:
# #             month1_str = from_month_to_month_match.group(1)
# #             month1_num = self.MONTH_NAMES.get(month1_str, 1)
# #             year1_explicit = int(from_month_to_month_match.group(2)) if from_month_to_month_match.group(2) else None
            
# #             month2_str = from_month_to_month_match.group(3)
# #             month2_num = self.MONTH_NAMES.get(month2_str, 12)
# #             year2_explicit = int(from_month_to_month_match.group(4)) if from_month_to_month_match.group(4) else None
            
# #             # Intelligent year assignment logic:
# #             # Priority: explicit years > inferred from one year > FY context
# #             if year1_explicit and year2_explicit:
# #                 # Both years explicit
# #                 year1 = year1_explicit
# #                 year2 = year2_explicit
# #             elif year1_explicit and not year2_explicit:
# #                 # Only year1 explicit - infer year2 from month order
# #                 year1 = year1_explicit
# #                 if month2_num < month1_num:
# #                     # Crosses year boundary (e.g., "from sep 2024 to feb" -> 2024 to 2025)
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
# #             elif year2_explicit and not year1_explicit:
# #                 # Only year2 explicit - infer year1 from month order
# #                 year2 = year2_explicit
# #                 if month1_num <= month2_num:
# #                     # Same calendar order (e.g., "from april to sep 2024" -> both 2024)
# #                     year1 = year2
# #                 else:
# #                     # Crosses year boundary (e.g., "from sep to april 2024" -> sep 2023 to apr 2024)
# #                     year1 = year2 - 1
# #             else:
# #                 # No explicit years - use FY context
# #                 year1 = get_fy_year_for_month(month1_num)
# #                 if month2_num < month1_num:
# #                     # Crosses FY boundary (e.g., "from oct to feb" = Oct 2025 to Feb 2026)
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
            
# #             print(f"[PATTERN 3] from month to month: {month1_num}/{year1} to {month2_num}/{year2}")
# #             return [
# #                 {"month_num": month1_num, "year": year1},
# #                 {"month_num": month2_num, "year": year2}
# #             ], "custom_range"
        
# #         # PATTERN 5: "till DATE" or "until DATE" pattern (from start of FY to specified date)
# #         # MUST come before Pattern 4 (single date) to avoid "13 oct" being grabbed as a lone date
# #         till_date_pattern = r'(?:till|until)\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         till_date_match = re.search(till_date_pattern, query_lower)
        
# #         if till_date_match:
# #             day = int(till_date_match.group(1))
# #             month_str = till_date_match.group(2)
# #             month_num = self.MONTH_NAMES.get(month_str, 12)
# #             year_explicit = int(till_date_match.group(3)) if till_date_match.group(3) else None
# #             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
# #             # Start year: if ending month is Jan-Mar (2026), start is Apr 2025
# #             start_year = year if month_num >= 4 else year - 1
            
# #             print(f"[PATTERN 5] till date: Apr/{start_year} till {day}/{month_num}/{year}")
# #             return [
# #                 {"month_num": 4, "year": start_year},
# #                 {"day": day, "month_num": month_num, "year": year}
# #             ], "custom_range"
        
# #         # PATTERN 6A: "from DAY MONTH" pattern (from specific date to end of current FY)
# #         # Examples: "from 15 sep" → Sep 15 to Mar 31 (end of current FY)
# #         #           "from 15 sep 2022" → Sep 15 2022 to Mar 31 2023
# #         # MUST come before Pattern 4 (single date) to avoid "15 sep" being grabbed as a lone date
# #         from_day_month_pattern = r'\bfrom\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
# #         from_day_match = re.search(from_day_month_pattern, query_lower)

# #         if from_day_match:
# #             day = int(from_day_match.group(1))
# #             month_str = from_day_match.group(2)
# #             month_num = self.MONTH_NAMES.get(month_str, 4)
# #             year_explicit = int(from_day_match.group(3)) if from_day_match.group(3) else None
# #             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)

# #             # End: March 31 of the FY that contains the start month
# #             end_year = year + 1 if month_num >= 4 else year

# #             print(f"[PATTERN 6A - FROM DAY MONTH] {day}/{month_num}/{year} to 31/3/{end_year}")
# #             return [
# #                 {"day": day, "month_num": month_num, "year": year},
# #                 {"day": 31, "month_num": 3, "year": end_year}
# #             ], "custom_range"

# #         # PATTERN 4: Single specific date (e.g., "20 jan", "5 february 2025", "20 jan 2024", "on 15 sep")
# #         # Placed AFTER till/from open-range patterns so those take priority.
# #         day_month_pattern = r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         day_month_match = re.search(day_month_pattern, query_lower)
        
# #         if day_month_match:
# #             day = int(day_month_match.group(1))
# #             month_str = day_month_match.group(2)
# #             month_num = self.MONTH_NAMES.get(month_str, 1)
# #             year = int(day_month_match.group(3)) if day_month_match.group(3) else get_fy_year_for_month(month_num)
            
# #             print(f"[PATTERN 4] single date: {day}/{month_num}/{year}")
# #             return [{"day": day, "month_num": month_num, "year": year}], "custom_date"

# #         # PATTERN 6: "from MONTH" pattern (from month to end of current FY)
# #         # Examples: "from may" (May 2025 to Mar 2026), "from june 2024" (Jun 2024 to Mar 2025)
# #         # This pattern explicitly looks for the word "from" followed by a month
# #         from_month_pattern = r'\bfrom\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
# #         from_match = re.search(from_month_pattern, query_lower)
        
# #         if from_match:
# #             month_str = from_match.group(1)
# #             month_num = self.MONTH_NAMES.get(month_str, 4)
# #             year_explicit = int(from_match.group(2)) if from_match.group(2) else None
# #             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
# #             # End year: if starting month is Apr-Dec (2025), end is Mar 2026  
# #             end_year = year + 1 if month_num >= 4 else year
            
# #             print(f"[PATTERN 6 - FROM MONTH] Matched: '{month_str}' (month={month_num}), Start year: {year}, End year: {end_year}")
            
# #             return [
# #                 {"month_num": month_num, "year": year},
# #                 {"month_num": 3, "year": end_year}
# #             ], "custom_range"
        
# #         # PATTERN 7: "till MONTH" or "until MONTH" pattern (from start of FY to end of that month)
# #         till_month_pattern = r'(?:till|until)\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         till_month_match = re.search(till_month_pattern, query_lower)
        
# #         if till_month_match:
# #             month_str = till_month_match.group(1)
# #             month_num = self.MONTH_NAMES.get(month_str, 12)
# #             year_explicit = int(till_month_match.group(2)) if till_month_match.group(2) else None
# #             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
# #             # Start year: if ending month is Jan-Mar, start is Apr of previous year
# #             start_year = year if month_num >= 4 else year - 1
            
# #             end_date = {"month_num": month_num, "year": year}
            
# #             return [
# #                 {"month_num": 4, "year": start_year},
# #                 end_date
# #             ], "custom_range"
        
# #         # PATTERN 8: Month-to-month range without "to" (e.g., "april - june", "sep - november")
# #         month_dash_month = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s*-\s*(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         month_dash_match = re.search(month_dash_month, query_lower)
        
# #         if month_dash_match:
# #             month1_str = month_dash_match.group(1)
# #             month1_num = self.MONTH_NAMES.get(month1_str, 1)
# #             year1_explicit = int(month_dash_match.group(2)) if month_dash_match.group(2) else None
            
# #             month2_str = month_dash_match.group(3)
# #             month2_num = self.MONTH_NAMES.get(month2_str, 12)
# #             year2_explicit = int(month_dash_match.group(4)) if month_dash_match.group(4) else None
            
# #             # Intelligent year assignment logic
# #             if year1_explicit and year2_explicit:
# #                 year1 = year1_explicit
# #                 year2 = year2_explicit
# #             elif year1_explicit and not year2_explicit:
# #                 year1 = year1_explicit
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
# #             elif year2_explicit and not year1_explicit:
# #                 year2 = year2_explicit
# #                 if month1_num <= month2_num:
# #                     year1 = year2
# #                 else:
# #                     year1 = year2 - 1
# #             else:
# #                 year1 = get_fy_year_for_month(month1_num)
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
            
# #             return [
# #                 {"month_num": month1_num, "year": year1},
# #                 {"month_num": month2_num, "year": year2}
# #             ], "custom_range"
        
# #         # PATTERN 9: Month-to-month with "to" (e.g., "april to june", "april to sep 2024")
# #         month_to_month = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+(?:to)\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
# #         month_to_match = re.search(month_to_month, query_lower)
        
# #         if month_to_match:
# #             month1_str = month_to_match.group(1)
# #             month1_num = self.MONTH_NAMES.get(month1_str, 1)
# #             year1_explicit = int(month_to_match.group(2)) if month_to_match.group(2) else None
            
# #             month2_str = month_to_match.group(3)
# #             month2_num = self.MONTH_NAMES.get(month2_str, 12)
# #             year2_explicit = int(month_to_match.group(4)) if month_to_match.group(4) else None
            
# #             # Intelligent year assignment logic
# #             if year1_explicit and year2_explicit:
# #                 year1 = year1_explicit
# #                 year2 = year2_explicit
# #             elif year1_explicit and not year2_explicit:
# #                 year1 = year1_explicit
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
# #             elif year2_explicit and not year1_explicit:
# #                 year2 = year2_explicit
# #                 if month1_num <= month2_num:
# #                     year1 = year2
# #                 else:
# #                     year1 = year2 - 1
# #             else:
# #                 year1 = get_fy_year_for_month(month1_num)
# #                 if month2_num < month1_num:
# #                     year2 = year1 + 1
# #                 else:
# #                     year2 = year1
            
# #             return [
# #                 {"month_num": month1_num, "year": year1},
# #                 {"month_num": month2_num, "year": year2}
# #             ], "custom_range"
        
# #         # PATTERN 10: Single month (e.g., "april 2024", "june")
# #         single_month_pattern = r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
# #         single_month_match = re.search(single_month_pattern, query_lower)
        
# #         if single_month_match:
# #             month_str = single_month_match.group(1)
# #             month_num = self.MONTH_NAMES.get(month_str, 1)
# #             year = int(single_month_match.group(2)) if single_month_match.group(2) else get_fy_year_for_month(month_num)
            
# #             return [{"month_num": month_num, "year": year}], "custom_range"
        
# #         return [], "current_financial_year"



# #     # =====================================================
# #     # PUBLIC ENTRY POINT
# #     # =====================================================
    
           
# #     def _detect_possession_metric(self, user_query: str) -> Optional[str]:
# #         """
# #         Detect if query is about possession status/journey.
    
# #     Possession journey stages:
# #     1. Sale Done (Document_Date exists)
# #     2. Agreement Given (Agreement_Date exists)
# #     3. Possession Given (Possession_Given_On exists)
    
# #     Args:
# #         user_query: User's natural language query
    
# #     Returns:
# #         Possession metric name or None
    
# #     Examples:
# #         "how many possessions are pending" → "possession_pending_count"
# #         "possession given last month" → "possession_given_count"
# #         "tower wise possession pending" → "possession_pending_count"
# #         "possession status breakdown" → "possession_status_breakdown"
# #         "average days to possession" → "average_days_to_possession"
# #     """
# #         query_lower = user_query.lower()
        
# #         # ────────────────────────────────────────────────────────
# #         # Check if this is a possession-related query
# #         # ────────────────────────────────────────────────────────
# #         possession_keywords = ["possession", "possessions", "handover", "handed over"]
# #         agreement_keywords = ["agreement", "agreements"]
        
# #         has_possession = any(kw in query_lower for kw in possession_keywords)
# #         has_agreement = any(kw in query_lower for kw in agreement_keywords)
        
# #         if not has_possession and not has_agreement:
# #             return None
        
# #         # ────────────────────────────────────────────────────────
# #         # Possession-related metrics
# #         # ────────────────────────────────────────────────────────
# #         if has_possession:
# #             # Priority 1: Status breakdown
# #             if any(kw in query_lower for kw in ["status", "journey", "breakdown", "stages", "wise breakdown"]):
# #                 return "possession_status_breakdown"
            
# #             # Priority 2: Completion rate
# #             if any(kw in query_lower for kw in ["rate", "%", "percentage", "completion rate"]):
# #                 return "possession_completion_rate"
            
# #             # Priority 3: TAT/Average days
# #             if any(kw in query_lower for kw in ["average", "avg", "days", "time", "tat", "turnaround", "how long"]):
# #                 if "agreement" in query_lower:
# #                     return "average_days_to_agreement"
# #                 elif "total" in query_lower or "cycle" in query_lower:
# #                     return "average_total_cycle_time"
# #                 else:
# #                     return "average_days_to_possession"
            
# #             # Priority 4: Value metrics
# #             if any(kw in query_lower for kw in ["value", "revenue", "amount", "worth"]):
# #                 if any(kw in query_lower for kw in ["pending", "not given", "yet to"]):
# #                     return "possession_pending_value"
# #                 else:
# #                     return "possession_given_value"
            
# #             # Priority 5: Pending possession
# #             if any(kw in query_lower for kw in ["pending", "not given", "not handed", "yet to", "awaiting"]):
# #                 return "possession_pending_count"
            
# #             # Priority 6: Given possession
# #             if any(kw in query_lower for kw in ["given", "handed over", "completed", "done", "delivered"]):
# #                 return "possession_given_count"
            
# #             # Default possession metric
# #             return "possession_pending_count"
    
# #         # ────────────────────────────────────────────────────────
# #         # Agreement-related metrics
# #         # ────────────────────────────────────────────────────────
# #         if has_agreement:
# #             # Completion rate
# #             if any(kw in query_lower for kw in ["rate", "%", "percentage", "completion"]):
# #                 return "agreement_completion_rate"
            
# #             # TAT
# #             if any(kw in query_lower for kw in ["average", "avg", "days", "time", "tat", "turnaround"]):
# #                 return "average_days_to_agreement"
            
# #             # Value
# #             if any(kw in query_lower for kw in ["value", "revenue", "amount"]):
# #                 if "pending" in query_lower:
# #                     return "agreement_pending_value"
# #                 else:
# #                     return "agreement_given_count"
            
# #             # Pending agreement
# #             if any(kw in query_lower for kw in ["pending", "not created", "not prepared", "yet to"]):
# #                 return "agreement_pending_count"
            
# #             # Given agreement
# #             if any(kw in query_lower for kw in ["given", "created", "prepared", "signed", "done"]):
# #                 return "agreement_given_count"
            
# #             # Default
# #             return "agreement_pending_count"
        
# #         return None


# #     def _detect_transfer_query(self, user_query: str) -> Optional[str]:
# #         """
# #         Intelligently detect if query is about transfers.
        
# #         Returns appropriate transfer metric or None.
# #         """
# #         query_lower = user_query.lower()
        
# #         # Check if transfer keyword is present
# #         transfer_indicators = ["transfer", "transferred", "transferring", "transfers"]
# #         has_transfer = any(indicator in query_lower for indicator in transfer_indicators)
        
# #         if not has_transfer:
# #             return None
        
# #         # Exclude non-metric transfer queries
# #         exclusions = [
# #             "transfer policy", "transfer process", "transfer procedure",
# #             "how to transfer", "transfer ownership", "transfer documentation"
# #         ]
# #         if any(excl in query_lower for excl in exclusions):
# #             return None
        
# #         # Determine specific transfer metric type
        
# #         # Priority 1: Transfer rate
# #         if any(kw in query_lower for kw in ["rate", "percentage", "%", "percent"]):
# #             return "transfer_rate"
        
# #         # Priority 2: Transfer value
# #         if any(kw in query_lower for kw in ["value", "revenue", "amount", "worth"]):
# #             return "transferred_sales_value"
        
# #         # Priority 3: Transfer recipients
# #         if any(kw in query_lower for kw in ["received", "recipients", "final payer", "transferred to"]):
# #             return "transfer_recipients"
        
# #         # Priority 4: Non-transferred
# #         if any(kw in query_lower for kw in ["non transferred", "without transfer", "normal", "regular"]):
# #             return "non_transferred_sales"
        
# #         # Priority 5: Transfer orders count
# #         if any(kw in query_lower for kw in ["orders", "units count", "sales orders"]):
# #             return "transferred_sales_count"
        
# #         # Priority 6: Product-wise transfer
# #         if any(kw in query_lower for kw in ["product", "eden", "sales group", "amore", "livork"]):
# #             return "transfer_product_wise"
        
# #         # Default: Customer count
# #         return "transferred_sales"

# #     def extract_intent(self, user_query: str) -> SemanticIntent:
# #         """
# #         Extract semantic intent from user query.
        
# #         WORKFLOW:
# #         1. Call WatsonX LLM for metric, dimensions, date_range
# #         2. Use keyword fallbacks for dimension inference
# #         3. Use _extract_filter_values() for precise filter extraction (NOT LLM)
# #         """
# #         # 1. Build prompt
# #         prompt = self._build_prompt(user_query)
        
# #         print(f"[DEBUG] User Query: {user_query}")
# #         print(f"[DEBUG] User Query: {user_query}")
# #         raw_text = self.model.generate_text(prompt)
# #         print(f"[DEBUG] Watsonx Response: {raw_text}")
        
# #         # 3. Parse JSON
# #         intent_dict = self._parse_json(raw_text)
        
# #         # 4. Normalize keys (daterange -> date_range, etc.)
# #         intent_dict = self._normalize_intent_keys(intent_dict)
# #         print(f"[DEBUG] Parsed Intent (from LLM): {intent_dict}")
        


# #         # query_lower = user_query.lower()
# #         # status_breakdown = self._detect_status_breakdown_query(query_lower)
# #         # if status_breakdown:
# #         #     dimension, metric = status_breakdown
# #         #     print(f"[STATUS BREAKDOWN] Detected: {dimension}")
# #         #     return SemanticIntent(
# #         #         metric=metric,
# #         #         dimensions=[dimension],
# #         #         date_range="current_financial_year",
# #         #         filters={},
# #         #         original_query=user_query
# #         #     )
# #         # FIX #3: Check for status breakdown queries FIRST
# #         query_lower = user_query.lower()
# #         status_dim = self._detect_status_breakdown_query(query_lower)
# #         if status_dim:
# #             print(f"[STATUS BREAKDOWN] Using dimension: {status_dim}")
# #             return SemanticIntent(
# #                 metric="total_sales",
# #                 dimensions=[status_dim],
# #                 date_range="current_financial_year",
# #                 filters={},
# #                 original_query=user_query
# #             )
        


# #         cancellation_intent = self._detect_cancellation_query(user_query)
# #         if cancellation_intent:
# #             return cancellation_intent


# #         # ============================================================
# #         # KEYWORD FALLBACKS - Step 1: Date Range
# #         # ============================================================
# #         if not intent_dict.get("date_range") or intent_dict["date_range"] == "current_financial_year":
# #             detected_date = self._detect_date_range_keywords(user_query)
# #             if detected_date:
# #                 intent_dict["date_range"] = detected_date
# #                 print(f"[FALLBACK] Fixed date_range: {detected_date}")

# #         # ============================================================
# #         # TREND QUERY OVERRIDES (MoM, QoQ, YoY)
# #         # ============================================================
# #         query_lower = user_query.lower()
# #         current_dr = intent_dict.get("date_range", "")
        
# #         # 1. Year on Year (YoY) or Year Wise — always use last_3_financial_years
# #         #    Force override regardless of what the LLM or keyword detector returned,
# #         #    because these are always multi-year trend queries.
# #         yoy_triggers = ["year on year", "yoy", "year-on-year", "y-o-y",
# #                         "year wise", "yearwise", "year-wise", "yearly", "all years",
# #                         "year by year", "each year", "per year", "annual trend"]
# #         if any(t in query_lower for t in yoy_triggers):
# #             intent_dict["date_range"] = "last_3_financial_years"
# #             if not intent_dict.get("time_grain"):
# #                 intent_dict["time_grain"] = "year"
# #             print(f"[OVERRIDE] YoY/year-wise detected -> last_3_financial_years")

# #         # 2. Quarter on Quarter (QoQ) -> Defaults to current financial year
# #         elif "quarter on quarter" in query_lower or "qoq" in query_lower:
# #             # Override if the date range is too narrow (single quarter/month)
# #             if current_dr in ["this_quarter", "quarter", "this_month", "this_week", "today", "current_financial_year"]:
# #                 intent_dict["date_range"] = "current_financial_year"
# #                 print(f"[OVERRIDE] QoQ detected -> defaulting to current_financial_year")

# #         # 3. Month on Month (MoM) -> Defaults to current financial year
# #         elif "month on month" in query_lower or "mom" in query_lower:
# #              # Override if the date range is too narrow (single month)
# #             if current_dr in ["this_month", "month", "this_week", "today", "current_financial_year"]:
# #                 intent_dict["date_range"] = "current_financial_year"
# #                 print(f"[OVERRIDE] MoM detected -> defaulting to current_financial_year")
        
# #         # ============================================================
# #         # KEYWORD FALLBACKS - Step 2: Dimension Inference
# #         # ============================================================
# #         if not intent_dict.get("dimensions"):
# #             inferred_dims = self._infer_dimensions(user_query)
# #             if inferred_dims:
# #                 intent_dict["dimensions"] = inferred_dims
# #                 print(f"[FALLBACK] Inferred dimensions: {inferred_dims}")
        
# #         # Check for bifurcation queries
# #         if not intent_dict.get("dimensions"):
# #             bifurcation_dim = self._detect_bifurcation_query(user_query)
# #             if bifurcation_dim:
# #                 intent_dict["dimensions"] = [bifurcation_dim]
# #                 print(f"[FALLBACK] Bifurcation dimension: {bifurcation_dim}")
        
# #         # ============================================================
# #         # KEYWORD FALLBACKS - Step 3: Transfer Detection
# #         # ============================================================
# #         if "transfer" in user_query.lower():
# #             detected_transfer = self._detect_transfer_query(user_query)
# #             if detected_transfer:
# #                 intent_dict["metric"] = detected_transfer
# #                 print(f"[TRANSFER FALLBACK] Detected transfer metric: {detected_transfer}")
        

# #         # ============================================================
# #         # POSSESSION JOURNEY DETECTION
# #         # ============================================================
# #         if "possession" in user_query.lower() or "agreement" in user_query.lower():
# #             detected_possession = self._detect_possession_metric(user_query)
# #             if detected_possession:
# #                 intent_dict["metric"] = detected_possession
# #                 print(f"[POSSESSION] Detected metric: {detected_possession}")
# #         # ============================================================
# #         # CRITICAL FIX: OVERRIDE LLM FILTERS WITH KEYWORD EXTRACTION
# #         # ============================================================
# #         # ALWAYS use keyword-based filter extraction (ignore LLM filters)
# #         dimensions = intent_dict.get("dimensions", [])
# #         keyword_filters = self._extract_filter_values(user_query, dimensions)
        
# #         if keyword_filters:
# #             # Replace LLM filters with keyword-extracted filters
# #             intent_dict["filters"] = keyword_filters
# #             print(f"[OVERRIDE] Keyword-extracted filters: {keyword_filters}")
# #         else:
# #             # If keyword extraction fails, clear invalid LLM filters
# #             if "filters" in intent_dict:
# #                 print(f"[WARNING] Clearing invalid LLM filters: {intent_dict['filters']}")
# #                 intent_dict["filters"] = None
        
# #         # ============================================================
# #         # METRIC NORMALIZATION
# #         # ============================================================
# #         intent_dict["metric"] = self._normalize_metric(
# #             intent_dict.get("metric", "total_sales"), 
# #             user_query
# #         )
        
# #         # ============================================================
# #         # TIME GRAIN DETECTION
# #         # ============================================================
# #         if not intent_dict.get("time_grain"):
# #             detected_grain = self._detect_time_grain(user_query)
# #             if detected_grain:
# #                 intent_dict["time_grain"] = detected_grain
# #                 print(f"[FALLBACK] Time grain: {detected_grain}")
        
# #         # ============================================================
# #         # COMPARISON TYPE DETECTION (mom, qoq, yoy, wow)
# #         # ============================================================
# #         if not intent_dict.get("compare_to"):
# #             detected_compare = self._detect_comparison_type(user_query)
# #             if detected_compare:
# #                 intent_dict["compare_to"] = detected_compare
# #                 intent_dict["is_trend"] = True
# #                 print(f"[FALLBACK] Comparison: {detected_compare}")
                
# #                 # Auto-detect time_grain if not already set
# #                 if not intent_dict.get("time_grain"):
# #                     intent_dict["time_grain"] = self._suggest_time_grain_for_comparison(detected_compare)
        
# #         # ============================================================
# #         # CUSTOM DATE EXTRACTION
# #         # ============================================================
# #         # Try enhanced date extraction first
# #         custom_dates_enhanced, date_range_type = self._extract_custom_dates_enhanced(user_query)
# #         if custom_dates_enhanced:
# #             custom_dates = custom_dates_enhanced
# #             intent_dict["date_range"] = date_range_type
# #             intent_dict["custom_dates"] = custom_dates
# #             print(f"[DATE EXTRACTION] Enhanced dates found: {custom_dates}, type: {date_range_type}")
# #         else:
# #             # Fallback to original method
# #             custom_dates = self._extract_custom_dates(user_query)
# #             if custom_dates:
# #                 intent_dict["custom_dates"] = custom_dates
# #                 print(f"[DATE EXTRACTION] Fallback dates found: {custom_dates}")
        
# #         # ============================================================
# #         # CLEANUP: REMOVE TIME GRAINS FROM DIMENSIONS
# #         # ============================================================
# #         if intent_dict.get("dimensions"):
# #             dims = intent_dict["dimensions"]
# #             time_grains = set(self.TIME_GRAIN_KEYWORDS.keys())
            
# #             valid_dims = []
# #             for dim in dims:
# #                 if dim in time_grains:
# #                     print(f"[CORRECTION] Removed '{dim}' from dimensions (it is a time grain)")
# #                     # Ensure time_grain is set if not already
# #                     if not intent_dict.get("time_grain"):
# #                         intent_dict["time_grain"] = dim
# #                         print(f"[CORRECTION] Set time_grain to '{dim}'")
# #                 else:
# #                     valid_dims.append(dim)
            
# #             intent_dict["dimensions"] = valid_dims

# #         # ============================================================
# #         # RETURN CANONICAL INTENT
# #         # ============================================================
# #         print(f"[FINAL] Intent: {intent_dict}")
# #         return SemanticIntent(
# #             **intent_dict,
# #             original_query=user_query
# #         )



# #     # =====================================================
# #     # DIMENSION INFERENCE
# #     # =====================================================
    
# #     def _infer_dimensions(self, user_query: str) -> List[str]:
# #         """
# #         Infer dimensions from keywords in user query.
# #         Uses pattern matching with the DIMENSION_KEYWORDS map.
# #         """
# #         query_lower = user_query.lower()
# #         detected = set()
        
# #         for dim, keywords in self.DIMENSION_KEYWORDS.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     detected.add(dim)
# #                     break  # Found this dimension, move to next
        
# #         return list(detected)

# #     def _detect_bifurcation_query(self, user_query: str) -> Optional[str]:
# #         """
# #         Detect queries that ask for a breakdown/split WITHOUT mentioning dimensions.
        
# #         Examples:
# #         - "Booking type wise sales" -> booking_type
# #         - "Sales by broker" -> broker
# #         - "Channel split" -> channel
# #         - "Tower wise sales" -> tower
# #         """
# #         query_lower = user_query.lower()
        
# #         bifurcation_patterns = {
# #             "booking_type": ["booking type wise", "booking type bifurcation", "booking split"],
# #             "broker_name": ["broker wise", "broker split", "sales by broker"],
# #             "cancellation_reason": ["cancellation reason wise", "reason wise cancellation", "cancellation reason breakdown","reason"],
# #             "dist_channel_desc": ["channel wise", "channel split", "sales by channel"],
# #             "sales_group_desc": ["product wise", "product split", "sales by product"],
# #             "Sales_Org_Desc": ["project wise", "project split", "sales by project"],
# #             "tower": ["tower wise", "tower split", "sales by tower"],
# #             "division_desc": ["division wise", "division split"],
# #             "customer_type": ["booking status wise", "customer type wise", "status wise", "booked vs cancelled"],
# #             "sales_executive_name": ["executive wise", "salesman wise"],
# #             "floor_desc": ["floor wise", "floor split"],
# #             "sector": ["sector wise", "sector split"],
# #             "type_desc": ["unit type wise", "type wise"],
# #             "sales_group_desc": ["sales group wise", "sales group split", "group wise"],
# #             "sales_office_desc": ["sales office wise", "office wise", "sales office split"],
# #             "billing_plan": ["billing plan wise", "payment plan wise", "plan wise"],
# #             "sales_org_desc": ["organization wise", "org wise", "sales org wise"],
# #             "loan_bank": ["bank wise", "bank split", "lender wise"],
# #             "billing_block_description": ["billing block wise", "block reason wise"],
# #             "material_pricing_group_desc": ["pricing group wise", "material group wise"],
# #             "scheme_code": ["scheme wise", "scheme split"],
# #             "refferal": ["referral wise", "referral split"],
# #             "reason_for_rejection": ["rejection reason wise"],
# #         }
        
# #         for dim, patterns in bifurcation_patterns.items():
# #             for pattern in patterns:
# #                 if pattern in query_lower:
# #                     return dim
        
# #         return None

# #     # =====================================================
# #     # FILTER VALUE EXTRACTION
# #     # =====================================================
    
# #     def _extract_filter_values(self, user_query: str, dimensions: List[str]) -> Optional[Dict[str, Any]]:
# #         """
# #         INTELLIGENT FILTER EXTRACTOR - AUTO-DETECTS FILTERS FROM ALL 121 COLUMNS
        
# #         Extracts specific filter values from natural language queries for:
# #         - Projects (50 types)
# #         - Channels (Broker/Direct/Referral)
# #         - Divisions (Residential/Commercial/FSI/Institutional)
# #         - Sales Org (Wave City/Wave Estate/WMCC)
# #         - Towers (111 types)
# #         - Floors (51 types)
# #         - Unit Types (61 types)
# #         - Loan Banks (55 banks)
# #         - Sales Executives (154 people)
# #         - Brokers (774 brokers)
# #         - Material Pricing Groups (9 categories)
# #         - Booking Types (6 types)
# #         - Customer Types (Booked/Cancelled)
# #         - And 100+ other dimensions
# #         """
# #         filters = {}
# #         query_lower = user_query.lower()
        
# #         # FIX #2: Try person name extraction FIRST
# #         for dim_name in ["payer_name", "sold_to_name", "sales_executive_name", "broker_name"]:
# #             person_name = self._extract_person_name_filter(query_lower, dim_name)
# #             if person_name:
# #                 filters[dim_name] = person_name
# #                 print(f"[FILTER] Extracted {dim_name}: {person_name}")
        
# #         # FIX #4 & #5: Try multi-value extraction
# #         for dim_name in ["floor_desc", "tower", "sales_group_desc"]:
# #             multi_values = self._extract_multi_values_with_and(query_lower, dim_name)
# #             if multi_values:
# #                 filters[dim_name] = multi_values
# #                 print(f"[FILTER] Extracted {dim_name}: {multi_values}")


# #         def extract_multi(patterns: Dict[str, List[str]], dim_key: str):
# #             found_values = set()
# #             for value, keywords in patterns.items():
# #                 for keyword in keywords:
# #                     if keyword in query_lower:
# #                         # Special check for "sub" to avoid false positives (e.g. "broker" in "sub broker")
# #                         if dim_key == "dist_channel_desc" and "sub" in query_lower and "sub" not in keyword:
# #                             continue
                            
# #                         # IGNORE "WISE" suffix (e.g. "tower wise" should not match "wise")
# #                         if keyword == "wise":
# #                             continue
                            
# #                         found_values.add(value)
# #                         break
            
# #             if found_values:
# #                 # If multiple values found, return list. If single, return string.
# #                 # BUT for "broker and direct", we want list.
# #                 # The downstream SQL builder handles lists.
# #                 final_val = list(found_values)
# #                 if len(final_val) == 1:
# #                     filters[dim_key] = final_val[0]
# #                 else:
# #                     filters[dim_key] = final_val

# #         # ==========================================
# #         # 1. DISTRIBUTION CHANNEL (Dist_Channel_Desc)
# #         # ==========================================
# #         channel_patterns = {
# #             "Broker": ["broker", "agent", "channel broker", "brokerage"],
# #             "Direct": ["direct", "direct sales", "walk-in", "walk in"],
# #             "Referral": ["referral", "referred", "reference"]
# #         }
# #         # Skip channel filter when the query is asking for broker-name-wise breakdown
# #         # e.g. "broker name wise total sales" — "broker" here is a dimension, not a channel filter
# #         if not any(kw in query_lower for kw in ["broker name", "broker wise", "broker name wise"]):
# #             extract_multi(channel_patterns, "dist_channel_desc")
        
# #         # ==========================================
# #         # 2. PROJECT (Project_Desc) - 50 Projects
# #         # ==========================================
# #         project_patterns = {
# #             "AMORE": ["amore"],
# #             "LIVORK": ["livork"],
# #             "DREAM HOMES": ["dream homes", "dream home"],
# #             "DREAM BAZAAR": ["dream bazaar"],
# #             "EXECUTIVE FLOORS": ["executive floors", "executive floor"],
# #             "WAVE FLOOR": ["wave floor"],
# #             "NEW PLOTS": ["new plots", "new plot"],
# #             "OLD PLOTS": ["old plots", "old plot"],
# #             "VERIDIA": ["veridia"],
# #             "VERIDIA-3": ["veridia 3", "veridia-3"],
# #             "VERIDIA-4": ["veridia 4", "veridia-4"],
# #             "VERIDIA-5": ["veridia 5", "veridia-5"],
# #             "VERIDIA-6": ["veridia 6", "veridia-6"],
# #             "VERIDIA-7": ["veridia 7", "veridia-7"],
# #             "EDEN": ["eden"],
# #             "EDENIA": ["edenia"],
# #             "ELEGANTIA": ["elegantia"],
# #             "ELIGO": ["eligo"],
# #             "EMINENCE": ["eminence"],
# #             "IRENIA": ["irenia"],
# #             "TRUCIA": ["trucia"],
# #             "VASILIA": ["vasilia"],
# #             "MAYFAIR PARK": ["mayfair", "mayfair park"],
# #             "HARMONY GREENS": ["harmony greens", "harmony green"],
# #             "WAVE GALLERIA": ["galleria", "wave galleria"],
# #             "WAVE GARDEN": ["wave garden"],
# #             "WAVE ESTATE, GH2 PH2": ["wave estate gh2", "gh2 ph2"],
# #             "WAVE BUSSINESS SQUARE": ["business square", "bussiness square"],
# #             "WBT 1": ["wbt 1", "wbt1"],
# #             "WBT A": ["wbt a", "wbta"],
# #             "PRIME FLOORS": ["prime floors", "prime floor"],
# #             "VILLAS": ["villas"],
# #             "ARMONIA VILLA": ["armonia villa", "armonia"],
# #             "COMM BOOTH": ["comm booth", "commercial booth"],
# #             "COMMERCIAL PLOTS": ["commercial plots"],
# #             "PLOTS-COMM": ["plots comm"],
# #             "PLOTS-RES": ["plots res", "residential plots"],
# #             "PLOTS-RES-IF": ["plots res if"],
# #             "SCO": ["sco"],
# #             "METRO MART": ["metro mart"],
# #             "SWAMANORATH": ["swamanorath"],
# #             "FSI": ["fsi project", "fsi"],
# #             "INSTITUTIONAL": ["institutional project"],
# #             "EWS_001_(410)": ["ews 410", "ews_001"],
# #             "EWS_P2": ["ews p2"],
# #             "LIG_001_(310)": ["lig 310", "lig_001"],
# #             "LIG_P2": ["lig p2"],
# #             "HSSC": ["hssc"],
# #             "WAVE FLOOR 85": ["wave floor 85"],
# #             "WAVE FLOOR 99": ["wave floor 99"]
# #         }
# #         extract_multi(project_patterns, "sales_group_desc")
        
# #         # ==========================================
# #         # 3. DIVISION (Division_Desc)
# #         # ==========================================
# #         # division_patterns = {
# #         #     "Residential": ["residential"],
# #         #     "Commercial": ["commercial"],
# #         #     "Institutional": ["institutional"],
# #         #     "FSI": ["fsi division"]
# #         # }
# #         # extract_multi(division_patterns, "division_desc")

# #         # ==========================================
# #         # 3. DIVISION (Division_Desc)
# #         # ==========================================
# #         # NOTE: Only match if "division" keyword is present
# #         # Otherwise, "residential/commercial" will match both division AND sector
# #         # if "division" in query_lower:
# #         if any(keyword in query_lower for keyword in ["division", "residential", "commercial", "institutional"]):
# #             division_patterns = {
# #                 "Residential": ["residential"],
# #                 "Commercial": ["commercial"],
# #                 "Institutional": ["institutional"],
# #                 "FSI": ["fsi division"]
# #             }
# #             extract_multi(division_patterns, "division_desc")
        
# #         # ==========================================
# #         # 4. SALES ORGANIZATION (Sales_Org_Desc)
# #         # ==========================================
# #         if any(keyword in query_lower for keyword in ["project", "wave city", "wave estate", "wmcc"]):
# #             sales_org_patterns = {
# #                 "Wave City": ["wave city"],
# #                 "Wave Estate": ["wave estate"],
# #                 "WMCC Sec 32": ["wmcc", "sector 32 org", "sec 32 sales"]
# #             }
# #             extract_multi(sales_org_patterns, "sales_org_desc")
        
# #         # ==========================================
# #         # 5. SALES OFFICE (Sales_Office_Desc)
# #         # ==========================================
# #         sales_office_patterns = {
# #             "Wave City": ["wave city office"],
# #             "Wave Estate": ["wave estate office"],
# #             "WMCC Sec 32": ["wmcc office", "sector 32 office"]
# #         }
# #         extract_multi(sales_office_patterns, "sales_office_desc")
        
# #         # ==========================================
# #         # 6. TOWER DETECTION (Tower / Tower_Desc)
# #         # ==========================================
# #         # if "tower" in query_lower or "block" in query_lower:
# #         #     # Pattern: "tower 7", "tower A", "block 032", "tower 3JA"
# #         #     # BUT exclude "tower wise", "tower split" (grouping keywords)
# #         #     match = re.search(r'(?:tower|block)\s+([a-zA-Z0-9]+)', query_lower)
# #         #     if match:
# #         #         tower_val = match.group(1).upper()
# #         #         # Ignore if it's a grouping keyword
# #         #         if tower_val.lower() not in ['wise', 'split', 'breakdown', 'group']:
# #         #             # If numeric, pad with zeros (e.g., "7" -> "007")
# #         #             if tower_val.isdigit():
# #         #                 filters["tower"] = tower_val.zfill(3)
# #         #             else:
# #         #                 filters["tower_desc"] = tower_val

# #         # ==========================================
# #         # 6. TOWER DETECTION (Tower / Tower_Desc) - MULTI-VALUE SUPPORT
# #         # ==========================================
# #         if "tower" in query_lower or "block" in query_lower:
# #             tower_values = []
            
# #             # Pattern 1: "tower X and tower Y" (explicit tower for each)
# #             pattern1 = r'(?:tower|block)\s+([a-zA-Z0-9]+)(?:\s+and\s+(?:tower|block)\s+([a-zA-Z0-9]+))?'
# #             matches = re.findall(pattern1, query_lower)
            
# #             for match in matches:
# #                 for val in match:
# #                     if val and val.lower() not in ['wise', 'split', 'breakdown', 'group', 'and']:
# #                         tower_values.append(val.upper())
            
# #             # Pattern 2: "tower X and Y" (without repeating "tower")
# #             pattern2 = r'(?:tower|block)\s+([a-zA-Z0-9]+)\s+and\s+([a-zA-Z0-9]+)(?:\s+(?:tower|block))?'
# #             matches2 = re.findall(pattern2, query_lower)
            
# #             for match in matches2:
# #                 for val in match:
# #                     if val and val.lower() not in ['wise', 'split', 'breakdown', 'group', 'and', 'tower', 'block']:
# #                         tower_values.append(val.upper())
            
# #             # Remove duplicates
# #             tower_values = list(set(tower_values))
            
# #             if tower_values:
# #                 # If multiple towers, store as list
# #                 if len(tower_values) > 1:
# #                     filters["tower"] = tower_values
# #                 else:
# #                     # Single tower - pad if numeric
# #                     if tower_values[0].isdigit():
# #                         filters["tower"] = tower_values[0].zfill(3)
# #                     else:
# #                         filters["tower"] = tower_values[0]
        
# #         # ==========================================
# #         # 7. FLOOR DETECTION (Floor_Desc)
# #         # ==========================================
# #         # if "floor" in query_lower:
# #         #     floor_patterns = [
# #         #         (r'ground\s+floor', "Ground Floor"),
# #         #         (r'lower\s+ground\s+floor', "Lower Ground Floor"),
# #         #         (r'upper\s+ground\s+floor', "Upper Ground Floor"),
# #         #         (r'basement\s+(\d+)', "Basement {0}"),
# #         #         (r'podium\s+(\d+)', "Podium {0}"),
# #         #         (r'(\d+)(?:st|nd|rd|th)\s+floor', "{0}th Floor"),
# #         #         (r'floor\s+(\d+)', "{0}th Floor"),
# #         #         (r'g\+(\d+)', "G+{0}")
# #         #     ]
            
# #         #     for pattern, floor_format in floor_patterns:
# #         #         match = re.search(pattern, query_lower)
# #         #         if match:
# #         #             if "{0}" in floor_format:
# #         #                 floor_num = match.group(1)
# #         #                 if floor_num == "1":
# #         #                     floor_format = floor_format.replace("{0}th", "1st")
# #         #                 elif floor_num == "2":
# #         #                     floor_format = floor_format.replace("{0}th", "2nd")
# #         #                 elif floor_num == "3":
# #         #                     floor_format = floor_format.replace("{0}th", "3rd")
# #         #                 filters["floor_desc"] = floor_format.format(floor_num)
# #         #             else:
# #         #                 filters["floor_desc"] = floor_format
# #         #             break


# #         # ==========================================
# #         # 7. FLOOR DETECTION (Floor_Desc) - MULTI-VALUE SUPPORT
# #         # ==========================================
# #         if "floor" in query_lower:
# #             floor_values = []
            
# #             # Pattern for multiple floors: "15th and 16th floor", "15 and 16 floor"
# #             multi_floor_pattern = r'(\d+)(?:st|nd|rd|th)?\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor'
# #             multi_match = re.search(multi_floor_pattern, query_lower)
            
# #             if multi_match:
# #                 # Extract both floor numbers
# #                 floor1 = multi_match.group(1)
# #                 floor2 = multi_match.group(2)
# #                 floor_values.extend([floor1, floor2])
# #             else:
# #                 # Single floor patterns
# #                 floor_patterns = [
# #                     (r'ground\s+floor', "Ground Floor"),
# #                     (r'lower\s+ground\s+floor', "Lower Ground Floor"),
# #                     (r'upper\s+ground\s+floor', "Upper Ground Floor"),
# #                     (r'basement\s+(\d+)', "Basement {0}"),
# #                     (r'podium\s+(\d+)', "Podium {0}"),
# #                     (r'(\d+)(?:st|nd|rd|th)\s+floor', "{0}"),
# #                     (r'floor\s+(\d+)', "{0}"),
# #                     (r'g\+(\d+)', "G+{0}")
# #                 ]
                
# #                 for pattern, floor_format in floor_patterns:
# #                     match = re.search(pattern, query_lower)
# #                     if match:
# #                         if "{0}" in floor_format:
# #                             floor_num = match.group(1)
# #                             floor_values.append(floor_num)
# #                         else:
# #                             # Special floors like "Ground Floor"
# #                             filters["floor"] = floor_format
# #                             floor_values = []  # Clear list since we set filters directly
# #                         break
            
# #             # If we have numeric floor values, store them
# #             if floor_values:
# #                 # Remove duplicates
# #                 floor_values = list(set(floor_values))
                
# #                 if len(floor_values) > 1:
# #                     # Multiple floors - store as list
# #                     filters["floor"] = floor_values
# #                 else:
# #                     # Single floor
# #                     filters["floor"] = floor_values[0]
        
# #         # ==========================================
# #         # 8. UNIT TYPE (Type_Desc) - 61 Types
# #         # ==========================================
# #         unit_type_patterns = {
# #             "1 BHK": ["1 bhk", "1bhk", "one bhk"],
# #             "2 BHK": ["2 bhk", "2bhk", "two bhk"],
# #             "3 BHK": ["3 bhk", "3bhk", "three bhk"],
# #             "4 BHK": ["4 bhk", "4bhk", "four bhk"],
# #             "2 BHK with Study": ["2 bhk with study", "2 bhk study"],
# #             "3 BHK with Study": ["3 bhk with study", "3 bhk study"],
# #             "4 BHK with Study": ["4 bhk with study", "4 bhk study"],
# #             "3 BHK with Servent Qtr": ["3 bhk with servant", "3 bhk servant"],
# #             "4 BHK with Servent Qtr.": ["4 bhk with servant", "4 bhk servant"],
# #             "Shop": ["shop", "shops"],
# #             "Office": ["office", "offices"],
# #             "Villa": ["villa", "villas"],
# #             "Duplex": ["duplex"],
# #             "Penthouse": ["penthouse"],
# #             "Sky Villa": ["sky villa"],
# #             # "Plot": ["plot", "plots"],
# #             "Commercial": ["commercial unit"],
# #             "Food Court": ["food court"],
# #             "Commercial Booth": ["commercial booth"]
# #         }
# #         extract_multi(unit_type_patterns, "type_desc")
        
# #         # ==========================================
# #         # 9. MATERIAL PRICING GROUP (Material_Pricing_Group_Desc)
# #         # ==========================================
# #         pricing_group_patterns = {
# #             "Apartment": ["apartment pricing", "apartment group", "apartment"],
# #             "Independent Floor": ["independent floor", "independent floors"],
# #             "Office": ["office pricing"],
# #             "Shops": ["shops pricing"],
# #             "Plots": ["plots pricing","plots","plot"],
# #             "Villas": ["villas pricing"],
# #             "SCO": ["sco pricing"],
# #             "FSI": ["fsi pricing"],
# #             "Institutional Site": ["institutional site"]
# #         }
# #         extract_multi(pricing_group_patterns, "material_pricing_group_desc")
        
# #         # ==========================================
# #         # 10. SECTOR DETECTION (Sector - numeric)
# #         # ==========================================
# #         if "sector" in query_lower:
# #             match = re.search(r'sector\s+(\d+)', query_lower)
# #             if match:
# #                 filters["sector"] = match.group(1)
# #         # ==========================================
# #         # 10B. SECTOR CATEGORY (Residential/Commercial) - NEW
# #         # ==========================================
# #         # NOTE: This detects sector TYPE (residential/commercial)
# #         # which is different from numeric sector (e.g., "sector 32")
# #         # sector_category_patterns = {
# #         #     "Residential": ["residential", "resi"],
# #         #     "Commercial": ["commercial", "comm"]
# #         # }
        
# #         # Check if query contains residential/commercial keywords
# #         # found_sectors = []
# #         # for sector_val, keywords in sector_category_patterns.items():
# #         #     for keyword in keywords:
# #         #         if keyword in query_lower:
# #         #             found_sectors.append(sector_val)
# #         #             break
        
# #         # if found_sectors:
# #         #     # If multiple sectors found (e.g., "residential and commercial")
# #         #     if len(found_sectors) > 1:
# #         #         # Only override if we haven't already set a numeric sector
# #         #         if "sector" not in filters:
# #         #             filters["sector"] = found_sectors
# #         #     else:
# #         #         # Single sector - only set if numeric sector wasn't found
# #         #         if "sector" not in filters:
# #         #             filters["sector"] = found_sectors[0]
# #         # ==========================================
# #         # 11. BOOKING TYPE (Booking_Type)
# #         # ==========================================
# #         booking_type_patterns = {
# #             "Fresh": ["fresh booking", "fresh", "new booking"],
# #             "Fresh (Indirect)": ["fresh indirect", "indirect fresh"],
# #             "Relocation": ["relocation", "relocated"],
# #             "Relocation (Indirect)": ["relocation indirect", "indirect relocation"],
# #             "Unit Transfer": ["unit transfer booking"],
# #             "Farmer (Indirect)": ["farmer indirect", "indirect farmer"]
# #         }
# #         extract_multi(booking_type_patterns, "booking_type")
        
# #         # ==========================================
# #         # 12. CUSTOMER TYPE (Customer_Type) - Booking Status
# #         # ==========================================
# #         if "cancelled" in query_lower and "not" not in query_lower:
# #             filters["customer_type"] = "Cancelled"
# #         # elif "transferred" in query_lower:
# #         #     filters["customer_type"] = "Transferred"
# #         elif "booked" in query_lower and "pre" not in query_lower:
# #             filters["customer_type"] = "Booked"
        
# #         # ==========================================
# #         # 13. LOAN BANK DETECTION (55 Banks)
# #         # ==========================================
# #         bank_patterns = {
# #             "HDF": ["hdfc", "hdfc bank", "housing development"],
# #             "ICI": ["icici", "icici bank"],
# #             "SBI": ["sbi", "state bank of india", "state bank"],
# #             "AXB": ["axis", "axis bank"],
# #             "PNB": ["pnb", "punjab national", "punjab national bank"],
# #             "KMB": ["kotak", "kotak mahindra", "kotak bank"],
# #             "BOI": ["boi", "bank of india"],
# #             "BOB": ["bob", "bank of baroda", "baroda bank"],
# #             "CNB": ["canara", "canara bank"],
# #             "UBI": ["ubi", "union bank", "union bank of india"],
# #             "YES": ["yes bank", "yes"],
# #             "IDB": ["idbi", "idbi bank"],
# #             "PGB": ["punjab gramin bank"],
# #             "RBL": ["rbl", "rbl bank"],
# #             "DCH": ["dewan housing", "dhfl", "dch"],
# #             "L&T": ["l&t", "l&t finance", "larsen toubro"],
# #             "IDF": ["indiabulls", "indiabulls housing"],
# #             "LIC": ["lic", "lic housing", "lic housing finance"],
# #             "J&K": ["j&k", "jammu kashmir bank"],
# #             "OBC": ["obc", "oriental bank"],
# #             "IOB": ["iob", "indian overseas bank"],
# #             "CBI": ["cbi", "central bank of india"],
# #             "SBH": ["sbh", "state bank hyderabad"],
# #             "SBP": ["sbp", "state bank patiala"]
# #         }
# #         extract_multi(bank_patterns, "loan_bank")
        
# #         # 14. SALES EXECUTIVE (Sales_Executive_Name)
# #         # ==========================================
# #         if "sales executive" in query_lower or "salesman" in query_lower or "executive" in query_lower:
# #             # Extract name after "sales executive" or "salesman"
# #             match = re.search(r'(?:sales executive|salesman|executive)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
# #             if match:
# #                 filters["sales_executive_name"] = match.group(1)
        
# #         # ==========================================
# #         # 15. BACK OFFICE EXECUTIVE (Back_Office_Executive_Name)
# #         # ==========================================
# #         if "back office" in query_lower:
# #             match = re.search(r'back office\s+(?:executive\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
# #             if match:
# #                 filters["back_office_executive_name"] = match.group(1)
        
# #         # ==========================================
# #         # 16. BROKER NAME (Broker_Name)
# #         # ==========================================
# #         if "broker" in query_lower and "sales" not in query_lower and "channel" not in query_lower:
# #             # Try to extract broker name
# #             match = re.search(r'broker\s+([A-Z][A-Z\s&.()]+)', user_query)
# #             if match:
# #                 filters["broker_name"] = match.group(1).strip()
        
# #         # ==========================================
# #         # 17. SUB BROKER NAME (Sub_Broker_Name)
# #         # ==========================================
# #         if "sub broker" in query_lower or "sub-broker" in query_lower:
# #             match = re.search(r'sub[\s-]?broker\s+([A-Z][A-Z\s&.()]+)', user_query)
# #             if match:
# #                 filters["sub_broker_name"] = match.group(1).strip()
        
# #         # ==========================================
# #         # 18. CUSTOMER NAME (Sold_To_Name)
# #         # ==========================================
# #         if "customer" in query_lower or "buyer" in query_lower:
# #             match = re.search(r'(?:customer|buyer)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
# #             if match:
# #                 filters["sold_to_name"] = match.group(1)
        
# #         # ==========================================
# #         # 19. PAYER NAME (Payer_Name)
# #         # ==========================================
# #         if "payer" in query_lower:
# #             match = re.search(r'payer\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
# #             if match:
# #                 filters["payer_name"] = match.group(1)
        
# #         # ==========================================
# #         # 20. BILLING BLOCK (Billing_Block_Description)
# #         # ==========================================
# #         billing_block_patterns = {
# #             "Booking Cancel": ["booking cancel", "cancelled booking"],
# #             "Cancel - In Process": ["cancel in process", "cancellation in process"],
# #             "Cancelled/Others": ["cancelled others"],
# #             "Dormant Sale": ["dormant", "dormant sale"],
# #             "Hold-Legal Case": ["legal case", "hold legal"],
# #             "Merger": ["merger"],
# #             "Relocation": ["relocation block"],
# #             "Billed in Legacy ERP": ["legacy erp", "billed in legacy"]
# #         }
        
# #         for block, keywords in billing_block_patterns.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     filters["billing_block_description"] = block
# #                     break
        
# #         # Check for "not blocked" or "no billing block"
# #         if any(phrase in query_lower for phrase in ["not blocked", "no billing block", "unblocked"]):
# #             filters["billing_block"] = "NULL"
# #         elif "blocked" in query_lower or "billing block" in query_lower:
# #             if "billing_block_description" not in filters:
# #                 filters["billing_block"] = "NOT NULL"
        
# #         # ==========================================
# #         # 21. REASON FOR REJECTION (Reason_for_Rejection)
# #         # ==========================================
# #         if "rejected" in query_lower or "rejection" in query_lower:
# #             # Check if specific rejection code is mentioned (Z1-ZB)
# #             match = re.search(r'\b(Z[0-9AB]|00)\b', user_query, re.IGNORECASE)
# #             if match:
# #                 filters["reason_for_rejection"] = match.group(1).upper()
# #             else:
# #                 filters["reason_for_rejection"] = "NOT NULL"
        
# #         # ==========================================
# #         # 22. SALES ORDER NUMBER (Sales_Order)
# #         # ==========================================
# #         if "sales order" in query_lower or re.search(r'\bSO[\s:-]?\d+', user_query, re.IGNORECASE):
# #             match = re.search(r'(?:SO|sales order)[\s:-]?(\d+)', user_query, re.IGNORECASE)
# #             if match:
# #                 filters["sales_order"] = match.group(1)
        
# #         # ==========================================
# #         # 23. DOCUMENT TYPE (Document_Type)
# #         # ==========================================
# #         if "document type" in query_lower:
# #             match = re.search(r'document type\s+([A-Z0-9]+)', user_query)
# #             if match:
# #                 filters["document_type"] = match.group(1)
        
# #         # ==========================================
# #         # 24. INVENTORY CODE (Inventory_Code)
# #         # ==========================================
# #         if "inventory" in query_lower:
# #             # Match inventory codes like "WC-1234", "INV-001"
# #             match = re.search(r'(?:unit|inventory)[\s:-]?([A-Z0-9-]+)', user_query, re.IGNORECASE)
# #             if match:
# #                 filters["inventory_code"] = match.group(1).upper()
        
# #         # ==========================================
# #         # 25. UOM (Unit of Measurement)
# #         # ==========================================
# #         uom_patterns = {
# #             "FT2": ["sq ft", "square feet", "sqft", "ft2"],
# #             "M2": ["sq m", "square meter", "sqm", "m2"],
# #             "YD2": ["sq yd", "square yard", "sqyd", "yd2"],
# #             "ACR": ["acre", "acres"]
# #         }
        
# #         for uom, keywords in uom_patterns.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     filters["uom"] = uom
# #                     break
        
# #         # ==========================================
# #         # 26. MATERIAL GROUP (Material_Group)
# #         # ==========================================
# #         material_group_patterns = {
# #             "ZSDBSP": ["zsdbsp"],
# #             "ZSDBSP1": ["zsdbsp1"],
# #             "ZSDBSP4": ["zsdbsp4"],
# #             "ZSDBSP5": ["zsdbsp5"]
# #         }
        
# #         for group, keywords in material_group_patterns.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     filters["material_group"] = group
# #                     break
        
# #         # ==========================================
# #         # 27. REFERRAL (Refferal)
# #         # ==========================================
# #         if "referral by" in query_lower or "referred by" in query_lower:
# #             match = re.search(r'(?:referral|referred)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
# #             if match:
# #                 filters["refferal"] = match.group(1)
        
# #         # ==========================================
# #         # 28. CO-APPLICANT NAMES
# #         # ==========================================
# #         if "co-applicant" in query_lower or "co applicant" in query_lower:
# #             match = re.search(r'co[\s-]?applicant\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
# #             if match:
# #                 filters["co_applicant1_name"] = match.group(1)
        
# #         # ==========================================
# #         # 29. PLC MATERIAL (PLC_Material_Desc)
# #         # ==========================================
# #         if "plc" in query_lower:
# #             match = re.search(r'plc\s+([A-Z0-9/]+)', user_query, re.IGNORECASE)
# #             if match:
# #                 filters["plc_material_desc"] = match.group(1).upper()
        
# #         # ==========================================
# #         # 30. SCHEME CODE (Scheme_Code)
# #         # ==========================================
# #         if "scheme" in query_lower:
# #             match = re.search(r'scheme\s+([A-Z0-9]+)', user_query, re.IGNORECASE)
# #             if match:
# #                 filters["scheme_code"] = match.group(1).upper()
        
# #         # ==========================================
# #         # 31. CONSORTIUM (Consortium_Name)
# #         # ==========================================
# #         if "consortium" in query_lower:
# #             match = re.search(r'consortium\s+([A-Z][A-Z\s&.()]+)', user_query)
# #             if match:
# #                 filters["consortium_name"] = match.group(1).strip()
        
# #         # ==========================================
# #         # 32. CHARGE TYPE (Charge_Type)
# #         # ==========================================
# #         if "charge type" in query_lower:
# #             match = re.search(r'charge type\s+([A-Z][a-z]+)', user_query)
# #             if match:
# #                 filters["charge_type"] = match.group(1)
        
# #         # ==========================================
# #         # 33. TAX CLASSES (Tax_Class_1 to Tax_Class_7)
# #         # ==========================================
# #         if "tax class" in query_lower:
# #             match = re.search(r'tax class\s+(\d+)', query_lower)
# #             if match:
# #                 tax_num = match.group(1)
# #                 # Extract tax value
# #                 tax_match = re.search(r'tax class\s+\d+\s+([A-Z0-9]+)', user_query, re.IGNORECASE)
# #                 if tax_match:
# #                     filters[f"tax_class_{tax_num}"] = tax_match.group(1).upper()

# #         # ==========================================
# #         # 33. SALES GROUP (Sales_Group / Sales_Group_Desc)
# #         # ==========================================
# #         if "sales group" in query_lower or "group" in query_lower:
# #             # Pattern: "Sales Group 01", "Group 01", "Sales Group 10"
# #             match = re.search(r'(?:sales )?group\s+(\d+)', query_lower)
# #             if match:
# #                 group_val = match.group(1)
# #                 # Ensure 2-digit padding if needed (e.g. "1" -> "01")
# #                 filters["sales_group"] = group_val.zfill(2)

# #         # ==========================================
# #         # 34. SALES OFFICE (Sales_Office / Sales_Office_Desc)
# #         # ==========================================
# #         if "sales office" in query_lower:
# #             match = re.search(r'sales office\s+([A-Z0-9\s]+)', user_query, re.IGNORECASE)
# #             if match:
# #                 # Capture the office name (e.g. "Sales Office Noida")
# #                 filters["sales_office_desc"] = match.group(1).strip()

# #         # ==========================================
# #         # 35. PAYMENT PLAN / BILLING PLAN (Billing_Plan)
# #         # ==========================================
# #         if "plan" in query_lower:
# #             # Pattern: "CLP Plan", "Down Payment Plan"
# #             if "clp" in query_lower:
# #                 filters["billing_plan"] = "CLP"
# #             elif "down payment" in query_lower:
# #                 filters["billing_plan"] = "Down Payment"
# #             elif "flexi" in query_lower:
# #                 filters["billing_plan"] = "Flexi"

# #         # ============================================================
# #         # IMPLICIT GROUPING LOGIC (Moved to end)
# #         # ============================================================
# #         # If the user specifies multiple values for a filter (e.g. "Broker and Direct"),
# #         # AND they ask for "group by" or "wise", we should add that dimension to the grouping.
        
# #         has_grouping_intent = any(k in query_lower for k in ["group", "wise", "breakdown", "split", "vs"])
        
# #         if has_grouping_intent:
# #             for dim, val in filters.items():
# #                 if isinstance(val, list) and len(val) > 1:
# #                     if dim not in dimensions:
# #                         dimensions.append(dim)
# #                         print(f"[INFERENCE] Added {dim} to dimensions due to multi-value filter + grouping intent")

# #         return filters if filters else None

# #     # =====================================================
# #     # METRIC NORMALIZATION
# #     # =====================================================
    
# #     def _normalize_metric(self, metric: str, user_query: str) -> str:
# #         """
# #         Map natural language to proper metric name.
# #         Handles aliases and context.
# #         """
# #         query_lower = user_query.lower()

# #         # If a specialist detector (_detect_transfer_query, _detect_possession_metric)
# #         # already resolved the metric to a known specific metric, trust it and return
# #         # immediately — skip keyword scanning to avoid false substring matches.
# #         # e.g. "transferred sales value" contains "sales value" which would otherwise
# #         # match the sales_value keyword and overwrite the correct transferred_sales_value.
# #         specialist_metrics = {
# #             "transferred_sales", "transfer_product_wise", "transferred_sales_count",
# #             "transferred_sales_value", "transfer_recipients", "non_transferred_sales",
# #             "transfer_rate",
# #             "possession_pending_count", "possession_given_count",
# #             "agreement_pending_count", "agreement_given_count",
# #             "possession_completion_rate", "possession_status_breakdown",
# #             "average_days_to_possession", "average_days_to_agreement",
# #             "average_total_cycle_time", "possession_pending_value",
# #             "possession_given_value", "agreement_pending_value",
# #             "tower_wise_possession_pending", "tower_wise_possession_given",
# #             "floor_wise_possession_pending", "product_wise_possession_pending",
# #             "cancelled_sales", "cancelled_units", "cancelled_sales_value",
# #         }
# #         if metric in specialist_metrics:
# #             return metric

# #         # Check keyword matches
# #         for metric_name, keywords in self.METRIC_KEYWORDS.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     # Special: "net value" could mean sales_value
# #                     if metric_name == "net_value":
# #                         return "sales_value"
# #                     return metric_name
        
# #         # Fallback: if metric is already valid, use it
# #         valid_metrics = [
# #             "total_sales", "sales_value", "net_value",
# #             "amount_received", "amount_demanded",
# #             "collection_percentage", "area_sold",
# #             # Transfer metrics
# #             "transferred_sales", "transfer_product_wise", "transferred_sales_count",
# #             "transferred_sales_value", "transfer_recipients",
# #             "transfer_rate",
# #             # Possession metrics
# #             "possession_pending_count", "possession_given_count",
# #             "agreement_pending_count", "agreement_given_count",
# #             "possession_completion_rate", "possession_status_breakdown",
# #             "average_days_to_possession", "average_days_to_agreement",
# #             "average_total_cycle_time",
# #             "possession_pending_value", "possession_given_value", "agreement_pending_value",
# #             # Dimensional possession metrics
# #             "tower_wise_possession_pending", "tower_wise_possession_given",
# #             "floor_wise_possession_pending", "product_wise_possession_pending"
# #         ]
        
# #         if metric in valid_metrics:
# #             return metric
        
# #         # Default
# #         return "total_sales"

# #     # =====================================================
# #     # TIME GRAIN DETECTION
# #     # =====================================================
    
# #     def _detect_time_grain(self, user_query: str) -> Optional[str]:
# #         """
# #         Detect time grain from keywords.
        
# #         Examples:
# #         - "monthly sales" -> "month"
# #         - "quarterly breakdown" -> "quarter"
# #         - "daily trend" -> "day"
# #         """
# #         query_lower = user_query.lower()
        
# #         for grain, keywords in self.TIME_GRAIN_KEYWORDS.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     return grain
        
# #         return None
    


# #     def _extract_person_name_filter(self, query_lower: str, dim_name: str) -> Optional[str]:
# #         """
# #         FIX #2: Extract person names from queries.
        
# #         Examples:
# #             "total sales of payer sunil" -> "sunil"
# #             "sales sold to amit hora" -> "amit hora"  
# #             "sales by sales executive john smith" -> "john smith"
# #         """
# #         # Keywords that indicate a person name follows
# #         person_indicators = {
# #             "payer_name": ["payer"],
# #             "sold_to_name": ["sold to", "customer name"],
# #             "sales_executive_name": ["sales executive", "salesman"],
# #             "broker_name": ["broker name", "agent"],
# #         }
        
# #         if dim_name not in person_indicators:
# #             return None
        
# #         indicators = person_indicators[dim_name]
        
# #         # Words that indicate a grouping/dimension request, NOT a person name
# #         name_stopwords = {
# #             'wise', 'split', 'breakdown', 'total', 'sales', 'count', 'number',
# #             'of', 'by', 'in', 'for', 'and', 'the', 'with', 'report', 'data',
# #             'summary', 'analysis', 'trend', 'all', 'each', 'per', 'show', 'me'
# #         }

# #         for indicator in indicators:
# #             # Look for "payer X", "sold to X", etc.
# #             # Match 1-3 words after the indicator
# #             pattern = rf'{indicator}\s+([a-z]+(?:\s+[a-z]+){{0,2}})'
# #             match = re.search(pattern, query_lower)
# #             if match:
# #                 name = match.group(1).strip()
# #                 first_word = name.split()[0].lower()
# #                 # Reject if it starts with a grouping/reporting word — not a person name
# #                 if first_word in name_stopwords:
# #                     continue
# #                 # Capitalize properly
# #                 return ' '.join(word.capitalize() for word in name.split())
        
# #         return None


    

# #     def _extract_multi_values_with_and(self, query_lower: str, dim_name: str) -> Optional[List[str]]:
# #         """
# #         FIX #4 & #5: Extract multiple values connected by 'and'.
        
# #         Examples:
# #             "15th floor and 16th floor" -> ["15th", "16th"]
# #             "tower a and tower 7" -> ["a", "7"]
# #             "eden and amore" -> ["eden", "amore"]
# #         """
# #         # Dimension-specific extraction patterns
# #         patterns = {
# #             "floor_desc": [
# #                 # Match: "15th floor and 16th floor"
# #                 r'(\d+(?:st|nd|rd|th)?)\s+floor\s+and\s+(\d+(?:st|nd|rd|th)?)\s+floor',
# #                 # Match: "floor 15 and floor 16"
# #                 r'floor\s+(\d+)\s+and\s+floor\s+(\d+)',
# #                 # Match: "15th and 16th floor"
# #                 r'(\d+(?:st|nd|rd|th)?)\s+and\s+(\d+(?:st|nd|rd|th)?)\s+floor',
# #             ],
# #             "tower": [
# #                 # Match: "tower a and tower 7"
# #                 r'tower\s+([a-z0-9]+)\s+and\s+tower\s+([a-z0-9]+)',
# #                 # Match: "tower a and 7"
# #                 r'tower\s+([a-z0-9]+)\s+and\s+([a-z0-9]+)',
# #             ],
# #             "sales_group_desc": [
# #                 # Match: "eden and amore"
# #                 r'(eden|amore|livork)\s+and\s+(eden|amore|livork)',
# #             ]
# #         }
        
# #         if dim_name not in patterns:
# #             return None
        
# #         for pattern in patterns[dim_name]:
# #             match = re.search(pattern, query_lower)
# #             if match:
# #                 values = list(match.groups())
# #                 # Clean and deduplicate
# #                 values = [v.strip() for v in values if v]
# #                 return list(dict.fromkeys(values))  # Remove duplicates, preserve order
        
# #         return None



# #     # =====================================================
# #     # COMPARISON TYPE DETECTION
# #     # =====================================================
    
# #     def _detect_comparison_type(self, user_query: str) -> Optional[str]:
# #         """
# #         Detect time-based comparisons.
        
# #         Examples:
# #         - "month on month sales" -> "mom"
# #         - "year on year growth" -> "yoy"
# #         """
# #         query_lower = user_query.lower()
        
# #         for compare_type, keywords in self.COMPARISON_KEYWORDS.items():
# #             for keyword in keywords:
# #                 if keyword in query_lower:
# #                     return compare_type
        
# #         return None

# #     def _suggest_time_grain_for_comparison(self, compare_to: str) -> str:
# #         """
# #         Suggest appropriate time grain for a comparison type.
# #         """
# #         grain_map = {
# #             "mom": "month",
# #             "wow": "day",  # week is not in Presto, so day for daily data
# #             "qoq": "quarter",
# #             "yoy": "year",
# #         }
        
# #         return grain_map.get(compare_to, "month")
    

# #     # def _detect_status_breakdown_query(self, query_lower: str) -> Optional[Tuple[str, str]]:
# #     #     """
# #     #     FIX #3: Detect if query is asking for status breakdown.
# #     #     Returns tuple of (dimension_name, metric_name) or None.
# #     #     """
# #     #     # Possession status queries
# #     #     if any(phrase in query_lower for phrase in ["possession status", "possession breakdown", "possession wise"]):
# #     #         return ("possession_status", "total_sales")
        
# #     #     # Agreement status queries  
# #     #     if any(phrase in query_lower for phrase in ["agreement status", "agreement breakdown", "agreement wise"]):
# #     #         return ("agreement_status", "total_sales")
        
# #     #     return None
# #     def _detect_status_breakdown_query(self, query_lower: str) -> Optional[str]:
# #         """
# #         Detect if query is asking for status breakdown.
# #         Returns the dimension name to use.
# #         """
# #         # Possession status queries
# #         if any(phrase in query_lower for phrase in [
# #             "possession status", 
# #             "possession breakdown", 
# #             "possession wise",
# #             "possession stage"
# #         ]):
# #             return "possession_status"
        
# #         # Agreement status queries
# #         if any(phrase in query_lower for phrase in [
# #             "agreement status", 
# #             "agreement breakdown", 
# #             "agreement wise",
# #             "agreement stage"
# #         ]):
# #             return "agreement_status"
        
# #         return None

# #     # =====================================================
# #     # CUSTOM DATE EXTRACTION
# #     # =====================================================
    
# #     def _extract_custom_dates(self, user_query: str) -> Optional[List[Dict]]:
# #         """
# #         Extract specific dates/months from user query.
        
# #         Examples:
# #         - "april 2024" -> [{"month_num": 4, "year": 2024}]
# #         - "from april to september 2024" -> [{"month_num": 4, "year": 2024}, {"month_num": 9, "year": 2024}]
# #         - "in may" -> [{"month_num": 5, "year": <inferred_fy_year>}]
# #         """
# #         query_lower = user_query.lower()
# #         dates = []
        
# #         month_map = {
# #             'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
# #             'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
# #             'aug': 8, 'august': 8, 'sep': 9, 'september': 9, 'oct': 10, 'october': 10,
# #             'nov': 11, 'november': 11, 'dec': 12, 'december': 12
# #         }

# #         months_regex = r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'

# #         # Helper: Infer year for a month in Current Financial Year
# #         def infer_fy_year(m_num):
# #             today = datetime.date.today()
# #             # *** CRITICAL: Correct FY Logic ***
# #             # If today is Jan-Mar (month < 4), FY started in PREVIOUS year
# #             # If today is Apr-Dec (month >= 4), FY started in CURRENT year
# #             if today.month < 4:
# #                 # We're in Jan-Mar, so FY started last year
# #                 fy_start_year = today.year - 1
# #             else:
# #                 # We're in Apr-Dec, so FY started this year
# #                 fy_start_year = today.year
                
# #             # Now determine which year the target month belongs to:
# #             # Months Apr(4)-Dec(12) belong to fy_start_year
# #             # Months Jan(1)-Mar(3) belong to fy_start_year + 1
# #             if m_num >= 4:
# #                 return fy_start_year
# #             else:
# #                 return fy_start_year + 1

# #         # Pattern 0: Range "from Month to Month Year" (e.g. "from april to september 2024")
# #         range_pattern = rf'from\s+{months_regex}\s+to\s+{months_regex}\s+(\d{{4}})'
# #         range_matches = re.findall(range_pattern, query_lower)
        
# #         if range_matches:
# #             for m1, m2, y_str in range_matches:
# #                 y = int(y_str)
# #                 dates.append({"month_num": month_map.get(m1), "year": y})
# #                 dates.append({"month_num": month_map.get(m2), "year": y})
# #             return dates

# #         # Pattern 1: Month Year (e.g., "april 2024")
# #         month_year_pattern = rf'{months_regex}\s+(\d{{4}})'
# #         matches = re.findall(month_year_pattern, query_lower)
        
# #         if matches:
# #             for month_str, year_str in matches:
# #                 month_num = month_map.get(month_str, 1)
# #                 year_num = int(year_str)
# #                 dates.append({
# #                     "month_num": month_num,
# #                     "year": year_num
# #                 })
# #             return dates
        
# #         # Pattern 2: Full dates (e.g., "2024-04-15", "15/04/2024")
# #         date_pattern = r'(\d{4})-(\d{2})-(\d{2})|(\d{2})/(\d{2})/(\d{4})'
# #         date_matches = re.findall(date_pattern, query_lower)
        
# #         if date_matches:
# #             for match in date_matches:
# #                 if match[0]:  # YYYY-MM-DD
# #                     dates.append({"year": int(match[0]), "month_num": int(match[1]), "day": int(match[2])})
# #                 elif match[3]:  # DD/MM/YYYY
# #                     dates.append({"day": int(match[3]), "month_num": int(match[4]), "year": int(match[5])})
# #             return dates

# #         # Pattern 3: Month Only (Infer Year)
# #         # Matches standalone months like "sales in May", "August sales"
# #         # Only if NO other date patterns matched
# #         standalone_matches = re.findall(rf'\b{months_regex}\b', query_lower)
# #         if standalone_matches:
# #             unique_months = set(standalone_matches)
# #             for m_str in unique_months:
# #                 m_num = month_map.get(m_str)
# #                 if m_num:
# #                     y_inferred = infer_fy_year(m_num)
# #                     dates.append({"month_num": m_num, "year": y_inferred})
# #             return dates
        
# #         return None
    
    
# #     def _normalize_intent_keys(self, data: dict) -> dict:
# #         """
# #         Normalize LLM response keys to match SemanticIntent fields.
# #         Handles: daterange→date_range, customdates→custom_dates, etc.
# #         """
# #         key_map = {
# #             "daterange": "date_range",
# #             "customdates": "custom_dates",
# #             "timegrain": "time_grain",
# #             "istrend": "is_trend",
# #             "compareto": "compare_to",
# #             "orderby": "order_by",
# #             "orderdirection": "order_direction",
# #         }
        
# #         for src, dst in key_map.items():
# #             if src in data and dst not in data:
# #                 data[dst] = data[src]
        
# #         # Normalize date_range variants
# #         if "date_range" in data and data["date_range"]:
# #             dr = str(data["date_range"]).strip().lower().replace(" ", "_")
# #             alias_map = {
# #                 "thisweek": "this_week",
# #                 "lastweek": "last_week",
# #                 "thismonth": "this_month",
# #                 "lastmonth": "last_month",
# #                 "thisquarter": "this_quarter",
# #                 "lastquarter": "last_quarter",
# #                 "thisyear": "this_year",
# #                 "lastyear": "last_year",
# #                 "currentfinancialyear": "current_financial_year",
# #                 "lastfinancialyear": "last_financial_year",
# #                 "rolling7days": "rolling_7_days",
# #                 "rolling30days": "rolling_30_days",
# #                 "rolling90days": "rolling_90_days",
# #             }
# #             data["date_range"] = alias_map.get(dr, dr)
            
# #         # =========================================================
# #         # DIMENSION NORMALIZATION (Fix LLM Hallucinations)
# #         # =========================================================
# #         if "dimensions" in data and isinstance(data["dimensions"], list):
# #             normalized_dims = []
# #             for dim in data["dimensions"]:
# #                 # Lowercase and strip
# #                 d = str(dim).strip().lower().replace(" ", "_")
                
# #                 # Check aliases
# #                 if d in self.DIMENSION_ALIASES:
# #                     d = self.DIMENSION_ALIASES[d]
                
# #                 normalized_dims.append(d)
            
# #             data["dimensions"] = normalized_dims
        
# #         return data


# #     # def _detect_date_range_keywords(self, user_query: str) -> Optional[str]:
# #     #     """
# #     #     Keyword-based date detection (fallback when LLM fails).
# #     #     Returns canonical snake_case format.
# #     #     NOTE: q1-q4 are handled by _extract_custom_dates_enhanced(), not here.
# #     #     """
# #     #     query_lower = user_query.lower()
        
# #     #     # Order matters - check specific phrases first!
# #     #     date_patterns = [
# #     #         # Financial year
# #     #         (["current financial year", "current fy", "this fy"], "current_financial_year"),
# #     #         (["last financial year", "previous fy", "last fy"], "last_financial_year"),
# #     #         (["fytd", "financial year to date"], "fytd"),
            
# #     #         # Year on Year (defaults to last 3 FYs)
# #     #         (["year on year", "yoy", "year-on-year", "last 3 years", "last 3 year"], "last_3_financial_years"),
# #     #         (["quarter on quarter", "qoq", "quarter-on-quarter"], "current_financial_year"),
            
# #     #         # Specific periods (with spaces)
# #     #         (["last month"], "last_month"),
# #     #         (["this month"], "this_month"),
# #     #         (["last quarter"], "last_quarter"),
# #     #         (["this quarter"], "this_quarter"),
# #     #         (["last week"], "last_week"),
# #     #         (["this week"], "this_week"),
# #     #         (["last year"], "last_financial_year"),
# #     #         (["this year"], "current_financial_year"),
# #     #         (["yesterday"], "yesterday"),
# #     #         (["today"], "today"),
            
# #     #         # To-date
# #     #         (["mtd", "month to date"], "mtd"),
# #     #         (["qtd", "quarter to date"], "qtd"),
# #     #         (["ytd", "year to date"], "ytd"),
            
# #     #         # Rolling
# #     #         (["last 7 days", "past 7 days"], "rolling_7_days"),
# #     #         (["last 30 days", "past 30 days"], "rolling_30_days"),
# #     #         (["last 90 days", "past 90 days"], "rolling_90_days"),
# #     #     ]
        
# #     #     for patterns, date_range in date_patterns:
# #     #         for pattern in patterns:
# #     #             if pattern in query_lower:
# #     #                 return date_range
        
# #     #     return None
# #     def _detect_date_range_keywords(self, user_query: str) -> Optional[str]:
# #         """
# #         ENHANCED keyword-based date detection with support for:
# #         1. Specific years (2024, 2025, etc.)
# #         2. Specific quarters (Q1, Q2, etc.)
# #         3. Last N periods (last 3 years, last 2 quarters, etc.)
# #         4. All existing relative date patterns
        
# #         Returns canonical snake_case format.
# #         """
# #         query_lower = user_query.lower().strip()
        
# #         # ========================================
# #         # 0. YoY / YEAR-WISE (MUST come before year regex to avoid fy_YYYY mismatch)
# #         # ========================================
# #         yoy_triggers = ["year on year", "yoy", "year-on-year", "y-o-y",
# #                         "year wise", "yearwise", "year-wise", "yearly",
# #                         "all years", "year by year", "each year", "per year", "annual trend"]
# #         if any(t in query_lower for t in yoy_triggers):
# #             return "last_3_financial_years"
        
# #         # ========================================
# #         # 1. SPECIFIC YEAR PATTERN (Highest Priority)
# #         # ========================================
# #         # Patterns: "in 2024", "for 2024", "during 2024", "2024"
# #         year_patterns = [
# #             r'\bin\s+(20\d{2})\b',
# #             r'\bfor\s+(20\d{2})\b', 
# #             r'\bduring\s+(20\d{2})\b',
# #             r'\byear\s+(20\d{2})\b',
# #             r'\b(20\d{2})\b'  # Standalone year
# #         ]
        
# #         for pattern in year_patterns:
# #             match = re.search(pattern, query_lower)
# #             if match:
# #                 year = int(match.group(1))
# #                 # Return financial year identifier
# #                 return f"fy_{year}"
        
# #         # ========================================
# #         # 2. SPECIFIC QUARTER PATTERN  
# #         # ========================================
# #         # Patterns: "Q1", "Q2", "first quarter", "second quarter"
# #         quarter_map = {
# #             r'\bq1\b': 'q1',
# #             r'\bq2\b': 'q2', 
# #             r'\bq3\b': 'q3',
# #             r'\bq4\b': 'q4',
# #             r'\bfirst quarter\b': 'q1',
# #             r'\bsecond quarter\b': 'q2',
# #             r'\bthird quarter\b': 'q3',
# #             r'\bfourth quarter\b': 'q4'
# #         }
        
# #         for pattern, quarter_id in quarter_map.items():
# #             if re.search(pattern, query_lower):
# #                 return quarter_id
        
# #         # ========================================
# #         # 3. LAST N PERIODS (Financial Year Aware)
# #         # ========================================
        
# #         # Last N financial years
# #         last_fy_match = re.search(r'\blast\s+(\d+)\s+(?:financial\s+)?years?\b', query_lower)
# #         if last_fy_match:
# #             n = int(last_fy_match.group(1))
# #             return f"last_{n}_financial_years"
        
# #         # Last N quarters (financial quarters)
# #         last_q_match = re.search(r'\blast\s+(\d+)\s+quarters?\b', query_lower)
# #         if last_q_match:
# #             n = int(last_q_match.group(1))
# #             return f"last_{n}_quarters"
        
# #         # Last N months
# #         last_m_match = re.search(r'\blast\s+(\d+)\s+months?\b', query_lower)
# #         if last_m_match:
# #             n = int(last_m_match.group(1))
# #             return f"last_{n}_months"
        
# #         # ========================================
# #         # 4. EXISTING RELATIVE TIME PATTERNS
# #         # ========================================
        
# #         # Order matters - check specific phrases first!
# #         date_patterns = [
# #             # Financial year
# #             (["current financial year", "current fy", "this fy"], "current_financial_year"),
# #             (["last financial year", "previous fy", "last fy"], "last_financial_year"),
# #             (["fytd", "financial year to date"], "fytd"),
            
# #             # Year on Year (defaults to last 3 FYs)
# #             (["year on year", "yoy", "year-on-year"], "last_3_financial_years"),
# #             (["quarter on quarter", "qoq", "quarter-on-quarter"], "current_financial_year"),
            
# #             # Specific periods (with spaces)
# #             (["last month"], "last_month"),
# #             (["this month"], "this_month"),
# #             (["last quarter"], "last_quarter"),
# #             (["this quarter"], "this_quarter"),
# #             (["last week"], "last_week"),
# #             (["this week"], "this_week"),
# #             (["last year"], "last_financial_year"),
# #             (["this year"], "current_financial_year"),
# #             (["yesterday"], "yesterday"),
# #             (["today"], "today"),
            
# #             # To-date
# #             (["mtd", "month to date"], "mtd"),
# #             (["qtd", "quarter to date"], "qtd"),
# #             (["ytd", "year to date"], "ytd"),
            
# #             # Rolling
# #             (["last 7 days", "past 7 days"], "rolling_7_days"),
# #             (["last 30 days", "past 30 days"], "rolling_30_days"),
# #             (["last 90 days", "past 90 days"], "rolling_90_days"),
# #         ]
        
# #         for patterns, date_range in date_patterns:
# #             for pattern in patterns:
# #                 if pattern in query_lower:
# #                     return date_range
        
# #         return None


# #     # =====================================================
# #     # PROMPT BUILDER
# #     # =====================================================
    
# #     def _build_prompt(self, user_query: str) -> str:
# #         return f"""
# #     You are a semantic intent extractor for an enterprise sales analytics system.
# #     You MUST return a SINGLE valid JSON object.

# #     --------------------------- JSON SCHEMA (STRICT) ---------------------------
# #     {{
# #     "metric": "string",
# #     "dimensions": ["string"],
# #     "date_range": "string",
# #     "custom_dates": [{{"month_num": 4, "year": 2024}}] or null,
# #     "filters": object or null,
# #     "time_grain": "string or null",
# #     "is_trend": boolean,
# #     "compare_to": "string or null",
# #     "order_by": "string or null",
# #     "order_direction": "asc|desc|null",
# #     "limit": number or null
# #     }}

# #     --------------------------- DATE HANDLING (ENHANCED) ---------------------------

# #     **1. SPECIFIC YEARS:**
# #     - "total sales in 2024" → date_range: "fy_2024"
# #     - "sales in 2025" → date_range: "fy_2025"  
# #     - "revenue for 2023" → date_range: "fy_2023"

# #     **2. SPECIFIC QUARTERS (Financial Year):**
# #     - "sales in Q1" → date_range: "q1"
# #     - "Q2 sales" → date_range: "q2"
# #     - "first quarter" → date_range: "q1"

# #     **3. LAST N PERIODS (Financial Year Based):**
# #     - "last 3 years" → date_range: "last_3_financial_years"
# #     - "last 5 years" → date_range: "last_5_financial_years"
# #     - "last 2 quarters" → date_range: "last_2_quarters"
# #     - "last 4 quarters" → date_range: "last_4_quarters"
# #     - "last 6 months" → date_range: "last_6_months"

# #     **4. RELATIVE TIME PERIODS:**
# #     - "today" → "today"
# #     - "yesterday" → "yesterday"  
# #     - "this week" → "this_week"
# #     - "last week" → "last_week"
# #     - "this month" → "this_month"
# #     - "last month" → "last_month"
# #     - "this quarter" → "this_quarter"
# #     - "last quarter" → "last_quarter"
# #     - "this year" OR "current FY" → "current_financial_year"
# #     - "last year" OR "last FY" → "last_financial_year"

# #     **5. TO-DATE RANGES:**
# #     - "MTD" or "month to date" → "mtd"
# #     - "QTD" or "quarter to date" → "qtd"
# #     - "YTD" or "year to date" → "ytd"
# #     - "FYTD" or "financial year to date" → "fytd"

# #     **6. ROLLING WINDOWS:**
# #     - "last 7 days" → "rolling_7_days"
# #     - "last 30 days" → "rolling_30_days"
# #     - "last 90 days" → "rolling_90_days"

# #     **7. CUSTOM DATE (use custom_dates field):**
# #     - "April 2024" → date_range: "custom_range", custom_dates: [{{"month_num": 4, "year": 2024}}]

# #     **DEFAULT:** If NO date mentioned → "current_financial_year"

# #     **IMPORTANT:** Financial year runs April-March. FY 2024 = Apr 2024 to Mar 2025.

# #     --------------------------- EXAMPLES ---------------------------

# #     Query: "total sales in 2024"
# #     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "fy_2024", "time_grain": null}}

# #     Query: "sales in Q1"  
# #     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "q1", "time_grain": null}}

# #     Query: "last 3 years sales"
# #     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_3_financial_years", "time_grain": "year"}}

# #     Query: "tower wise sales last 2 quarters"
# #     Output: {{"metric": "total_sales", "dimensions": ["tower"], "date_range": "last_2_quarters", "time_grain": "quarter"}}

# #     Query: "total sales last month"
# #     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_month", "time_grain": null}}

# #     Query: "project wise sales by channel last month"
# #     Output: {{"metric": "total_sales", "dimensions": ["sales_org_desc", "dist_channel_desc"], "date_range": "last_month", "time_grain": null}}

# #     Query: "sales value this quarter"
# #     Output: {{"metric": "sales_value", "dimensions": [], "date_range": "this_quarter", "time_grain": null}}

# #     ---------------------------USER QUESTION ---------------------------
# #     {user_query}

# #     Return JSON ONLY. No explanation.
# #     """

# #     # =====================================================
# #     # SAFE JSON PARSER (NEVER FAILS)
# #     # =====================================================
    
# #     def _parse_json(self, raw_text: str) -> dict:
# #         """
# #         Parses LLM output into a safe intent dictionary.
# #         Falls back gracefully if output is invalid.
# #         """
# #         fallback = {
# #             "metric": "total_sales",
# #             "dimensions": [],
# #             "date_range": "current_financial_year",
# #             "is_trend": False,
# #             "time_grain": None,
# #             "compare_to": None,
# #             "order_by": None,
# #             "order_direction": None,
# #             "limit": None,
# #         }

# #         if not raw_text or not raw_text.strip():
# #             return fallback

# #         text = raw_text.strip()

# #         # Try normal parse
# #         try:
# #             data = json.loads(text)
# #         except Exception:
# #             # Try extracting JSON from surrounding text
# #             json_match = re.search(r'\{.*\}', text, re.DOTALL)
# #             if json_match:
# #                 try:
# #                     data = json.loads(json_match.group())
# #                 except Exception:
# #                     return fallback
# #             else:
# #                 # Try repairing truncated JSON
# #                 repaired = text
# #                 if repaired.startswith("{"):
# #                     if repaired.count('"') % 2 != 0:
# #                         repaired += '"'
# #                     if not repaired.endswith("}"):
# #                         repaired += "}"
# #                     try:
# #                         data = json.loads(repaired)
# #                     except Exception:
# #                         return fallback
# #                 else:
# #                     return fallback

# #         # Merge with defaults
# #         for key, value in fallback.items():
# #             if key not in data or data[key] in ("", None):
# #                 data[key] = value

# #         # Fix partial date ranges
# #         if "date_range" in data and data["date_range"]:
# #             date_range = data["date_range"]
            
# #             # Handle truncated responses
# #             if date_range == "current":
# #                 data["date_range"] = "current_financial_year"
# #             elif date_range == "last":
# #                 data["date_range"] = "last_financial_year"
# #             elif date_range == "this":
# #                 data["date_range"] = "this_month"  # Best guess

# #         return data


















# import json
# import re
# import datetime
# from typing import Any, Dict, List, Optional, Tuple
# # from app.semantic.intent import SemanticIntent
# from semantic.intent import SemanticIntent


# class WatsonxSemanticAdapter:
#     """
#     Adapter responsible ONLY for:
#     Natural Language -> SemanticIntent
#     Uses Watsonx ModelInference.generate_text()
    
#     IMPROVEMENTS:
#     1. Comprehensive dimension-to-keyword mapping
#     2. Filter value extraction (e.g., "tower 7", "amore", "16th floor")
#     3. Better metric selection logic
#     4. Enhanced natural language understanding
#     5. Custom date parsing
#     """
    

#     # =====================================================
#     # MONTH NAME MAPPINGS FOR DATE PARSING
#     # =====================================================
#     MONTH_NAMES = {
#         "january": 1, "jan": 1,
#         "february": 2, "feb": 2,
#         "march": 3, "mar": 3,
#         "april": 4, "apr": 4,
#         "may": 5,
#         "june": 6, "jun": 6,
#         "july": 7, "jul": 7,
#         "august": 8, "aug": 8,
#         "september": 9, "sep": 9, "sept": 9,
#         "october": 10, "oct": 10,
#         "november": 11, "nov": 11,
#         "december": 12, "dec": 12,
#     }


#     # =====================================================
#     # SEMANTIC MAPPINGS - FIXED ALL NAMING
#     # =====================================================
    
#     DIMENSION_KEYWORDS = {
#         # Property
#         # NOTE: specific product/project names (eden, amore, wave city etc.) are intentionally
#         # NOT listed here — they are filter values, not dimension keywords.
#         # Only generic grouping words trigger a dimension.
#         "sales_group_desc": ["product wise", "product split", "product breakdown"],
#         "tower": ["tower", "block"],
#         "floor": ["floor wise", "floor split", "floor breakdown", "floor level"],
#         "inventory_code": ["inventory code", "inventory"],
#         "type_desc": ["unit type", "shop", "office", "bhk"],
#         "sector": ["sector"],
        
#         # Customer & Sales
#         "sold_to_name": ["customer", "sold to", "buyer"],
#         "payer_name": ["payer","paying customer","payment by"],
#         "customer_type": ["booking status", "customer type", "booked"],  # Column name
#         "sales_executive_name": ["sales executive", "salesman"],
#         "back_office_executive_name": ["back office"],
        
#         # Channel
#         "broker_name": ["broker name", "agent"],
#         "sub_broker_name": ["sub broker"],
#         "dist_channel_desc": ["channel", "distribution"],
#         "refferal": ["referral", "ref"],
#         "consortium_name": ["consortium"],
        
#         # Transaction
#         "booking_type": ["booking type", "fresh", "relocation"],
#         "division_desc": ["division"],
#         "sales_group_desc": ["sales group", "group"],
#         "sales_office_desc": ["sales office", "office"],
#         "sales_org_desc": ["sales organization", "sales org", "organization", "org"],
#         "billing_plan": ["billing plan", "payment plan"],
#         "billing_block_description": ["billing block", "block reason"],
        
#         # Financial
#         "loan_bank": ["bank", "loan bank", "lender"],
#         "material_pricing_group_desc": ["pricing group", "material group","apartment"],
#         "scheme_code": ["scheme"],
#         "reason_for_rejection": ["rejection reason", "reason for rejection"],

#         # FIX #3: Status breakdown dimensions
#         "possession_status": ["possession status", "possession stage", "possession breakdown", "possession wise"],
#         "agreement_status": ["agreement status", "agreement stage", "agreement breakdown", "agreement wise"],

#         "cancellation_reason": ["cancellation reason","reason", "reason for cancellation", "cancel reason", "why cancelled", "cancellation", "reason cancelled"],
#     }

#     # METRIC_KEYWORDS = {
#     #     "total_sales": ["total sales", "sales count", "number of sales", "sales orders"],
#     #     "sales_value": ["sales value", "sales amount", "total revenue", "total amount", "gross sales", "net value", "net amount", "booking amount", "booking value"],
#     #     "net_value": ["net value", "net amount"],
#     #     "amount_received": ["amount received", "collection", "received", "payment received"],
#     #     "amount_demanded": ["amount demanded", "billed", "bill amount", "invoice"],
#     #     "collection_percentage": ["collection %", "collection percentage", "collection efficiency"],
#     #     "area_sold": ["area sold", "total area", "carpet area"],
#     #     "basic_selling_price": ["basic price", "base price"],
#     #     "discount": ["discount", "total discount"],
#     #     "loan_sanctioned": ["loan sanctioned", "loan amount"],
#     # }
    

#     METRIC_KEYWORDS = {
#         # sales_value MUST come before total_sales so "total sales amount" matches sales_value
#         # before "total sales" keyword matches total_sales
#         "sales_value": ["total sales amount", "sales value", "sales amount", "total revenue",
#                         "total amount", "gross sales", "net value", "net amount",
#                         "booking amount", "booking value"],
#         "total_sales": ["total sales", "sales count", "number of sales", "sales orders"],
#         "net_value": ["net value", "net amount"],
#         "amount_received": ["amount received", "collection", "received", "payment received"],
#         "amount_demanded": ["amount demanded", "billed", "bill amount", "invoice"],
#         "collection_percentage": ["collection %", "collection percentage", "collection efficiency"],
#         "area_sold": ["area sold", "total area", "carpet area"],
#         "basic_selling_price": ["basic price", "base price"],
#         "discount": ["discount", "total discount"],
#         "loan_sanctioned": ["loan sanctioned", "loan amount"],
#         # CORRECTED: Transfer metrics (counts unique customers - Sold_To_Name)
#        # ENHANCED TRANSFER METRICS
#                 "transferred_sales": [
#                     "transferred sales", "transfer count", "transferred customers", 
#                     "customers who transferred", "how many transferred",
#                     "count of transfers", "transfers customer"
#                 ],
#                 "transfer_product_wise": [
#                     "product wise transfer", "transfer product wise", 
#                     "product transfer", "transfers by product", "product wise transfers",
#                     "product-wise transfer", "productwise transfer"
#                 ],
#                 "transferred_sales_count": [
#                     "transfer orders", "transferred units count", "transfer sales orders",
#                     "number of transfer orders", "count of transfer orders"
#                 ],
#                 "transferred_sales_value": [
#                     "transfer value", "transfer revenue", "value of transfers", 
#                     "transferred sales value", "transfer amount", "revenue from transfers"
#                 ],
#                 "transfer_recipients": [
#                     "customers who received", "transfer recipients", "final payers", 
#                     "transferred to", "recipients of transfer", "who received transfer"
#                 ],
#                 "non_transferred_sales": [
#                     "non transferred sales", "normal sales", 
#                     "sales without transfer", "customers without transfer", "regular sales"
#                 ],
#                 "transfer_rate": [
#                     "transfer rate", "transfer percentage", "transfer %", 
#                     "percentage of transfers", "what percent transferred"
#                 ],

#                 # ═══════════════════════════════════════════════════════════════
#                     # POSSESSION JOURNEY METRICS
#                     # ═══════════════════════════════════════════════════════════════
#                     "possession_pending_count": [
#                         "possession pending", "pending possession", "possession not given",
#                         "possession yet to be given", "possession not handed over",
#                         "how many possessions pending", "pending possessions"
#                     ],
#                     "possession_given_count": [
#                         "possession given", "possession handed over", "possession completed",
#                         "possessions done", "how many possessions given"
#                     ],
#                     "agreement_pending_count": [
#                         "agreement pending", "pending agreement", "agreement not created",
#                         "agreement not prepared", "agreements pending"
#                     ],
#                     "agreement_given_count": [
#                         "agreement given", "agreement created", "agreement prepared",
#                         "agreement signed", "agreements done"
#                     ],
#                     "possession_completion_rate": [
#                         "possession completion rate", "possession completion %",
#                         "percentage of possessions given", "possession completion percentage"
#                     ],
#                     "possession_status_breakdown": [
#                         "possession status", "possession journey", "possession wise breakdown",
#                         "status of possession", "possession stages"
#                     ],
#                     "average_days_to_possession": [
#                         "average days to possession", "possession TAT", "time to possession",
#                         "how long for possession", "possession turnaround time"
#                     ],
#                     "average_days_to_agreement": [
#                         "average days to agreement", "agreement TAT", "time to agreement",
#                         "how long for agreement"
#                     ],
#                     "possession_pending_value": [
#                         "value of pending possessions", "pending possession value",
#                         "revenue in pending possessions"
#                     ],

#                     "cancelled_sales": [
#                         "cancelled sales", "total cancelled", "cancellations", "cancelled bookings",
#                         "cancelled orders", "how many cancelled", "number of cancellations",
#                         "cancelled count", "cancellation reason", "show cancelled", "cancel", "cancelled reason", "reason"
# ],
#                     "cancelled_sales_value": [
#                         "cancelled sales value", "cancellation value", "value of cancellations",
#                         "cancelled revenue", "value of cancelled bookings"
#               ],
#     }
    

    

#     COMPARISON_KEYWORDS = {
#         "mom": ["month on month", "month-on-month", "monthly", "m-o-m", "vs last month"],
#         "wow": ["week on week", "week-on-week", "weekly", "w-o-w"],
#         "qoq": ["quarter on quarter", "quarter-on-quarter", "quarterly", "q-o-q"],
#         "yoy": ["year on year", "year-on-year", "yearly", "y-o-y", "vs last year"],
#     }

#     TIME_GRAIN_KEYWORDS = {
#         "day": ["daily", "day", "by day"],
#         "month": ["monthly", "month", "by month", "month wise"],
#         "quarter": ["quarterly", "quarter", "by quarter"],
#         "year": ["yearly", "year", "by year"],
#     }

#     DIMENSION_ALIASES = {
#         "sales_channel": "dist_channel_desc",
#         "channel": "dist_channel_desc",
#         "distribution_channel": "dist_channel_desc",
#         "product": "sales_group_desc",
#         "booking_status": "customer_type",
#         "status": "customer_type",
#         "customer": "sold_to_name",
#         "buyer": "sold_to_name",
#         "broker name": "broker_name",
#         "agent": "broker_name",
#         "salesman": "sales_executive_name",
#         "sales_executive": "sales_executive_name",
#         "executive": "sales_executive_name",
#         "inventory code": "inventory_code",
#         "inventory": "inventory_code",
#         "unit_type": "type_desc",
#         "flat_type": "type_desc",
#         "billing_block": "billing_block_description",
#         "block_reason": "billing_block_description",
#         "sales_org": "sales_org_desc",
#         "organization": "sales_org_desc",
#         "wave city": "sales_org_desc",
#         "bank": "loan_bank",
#         "lender": "loan_bank",
#         "referral": "refferal",
#         "source": "refferal",
#         "scheme": "scheme_code",
#         "pricing_group": "material_pricing_group_desc",
#         "material_group": "material_pricing_group_desc",
#         "sales_office": "sales_office_desc",
#         "office": "sales_office_desc",
#         "sales_group": "sales_group_desc",
#         "group": "sales_group_desc",
#         "consortium": "consortium_name",
#         "payment_plan": "billing_plan",
#     }

#     def __init__(self, model):
#         """
#         model: ibm_watsonx_ai.foundation_models.ModelInference
#         """
#         self.model = model



#      # =====================================================
#     # MULTI-QUERY DETECTION (NEW!)
#     # =====================================================
    
#     # def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
#     #     """
#     #     Detect if query contains multiple separate requests using "and"
        
#     #     Examples:
#     #     - "sales for wave city and wave estate" → 2 queries
#     #     - "direct and broker sales" → 2 queries
#     #     - "april and june sales" → 2 queries
        
#     #     Returns list of intents (1 if single query, 2+ if multiple)
#     #     """
#     #     query_lower = user_query.lower()
        
#     #     # Pattern 1: Product names with "and"
#     #     product_match = re.search(r'(eden|amore|livork)\s+and\s+(veridia|amore|livork)', query_lower)
#     #     if product_match:
#     #         base_query = re.sub(r'(eden|amore|livork)\s+and\s+(veridia|amore|livork)', '', user_query, flags=re.IGNORECASE).strip()
#     #         product1 = product_match.group(1)
#     #         product2 = product_match.group(2)
            
#     #         intent1 = self.extract_intent(f"{base_query} for {product1}")
#     #         intent2 = self.extract_intent(f"{base_query} for {product2}")
#     #         return [intent1, intent2]
        
#     #     # Pattern 2: Channel with "and" (direct and broker)
#     #     channel_match = re.search(r'(direct|broker|indirect)\s+and\s+(direct|broker|indirect)', query_lower)
#     #     if channel_match:
#     #         base_query = re.sub(r'(direct|broker|indirect)\s+and\s+(direct|broker|indirect)', '', user_query, flags=re.IGNORECASE).strip()
#     #         channel1 = channel_match.group(1)
#     #         channel2 = channel_match.group(2)
            
#     #         intent1 = self.extract_intent(f"{base_query} {channel1}")
#     #         intent2 = self.extract_intent(f"{base_query} {channel2}")
#     #         return [intent1, intent2]
        
#     #     # Pattern 3: Months with "and" (april and june)
#     #     month_and_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\s+and\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)'
#     #     month_match = re.search(month_and_pattern, query_lower)
        
#     #     if month_match and ' to ' not in query_lower and ' till ' not in query_lower:
#     #         base_query = re.sub(month_and_pattern, '', user_query, flags=re.IGNORECASE).strip()
#     #         month1 = month_match.group(1)
#     #         month2 = month_match.group(2)
            
#     #         intent1 = self.extract_intent(f"{base_query} {month1}")
#     #         intent2 = self.extract_intent(f"{base_query} {month2}")
#     #         return [intent1, intent2]
        
#     #     # Single query
#     #     return [self.extract_intent(user_query)]

#     #     # Pattern 4: year with "and" (2024 and 2025)

#     #             # Pattern 4: year with "and" (2024 and 2025)
#     #     year_and_pattern = r'\b(20\d{2})\s+and\s+(20\d{2})\b'
#     #     year_match = re.search(year_and_pattern, query_lower)

#     #     if year_match:
#     #         base_query = re.sub(year_and_pattern, '', user_query, flags=re.IGNORECASE).strip()

#     #         year1 = int(year_match.group(1))
#     #         year2 = int(year_match.group(2))

#     #         # Financial year format: April YYYY to March YYYY+1
#     #         fy1 = f"april {year1} to march {year1 + 1}"
#     #         fy2 = f"april {year2} to march {year2 + 1}"

#     #         intent1 = self.extract_intent(f"{base_query} {fy1}")
#     #         intent2 = self.extract_intent(f"{base_query} {fy2}")

#     #         return [intent1, intent2]

#     #     # Single query
#     #     return [self.extract_intent(user_query)]
    
#     # running
#     # def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
#     #     """
#     #     Detect if query contains multiple separate requests.
        
#     #     Handles TWO types of multi-queries:
#     #     1. Dimension-based: "wave city and wave estate" → split by products
#     #     2. Date-based: "Q1 and Q2" or "2024 and 2025" → split by dates
        
#     #     Returns:
#     #         List of SemanticIntent objects (one per query)
#     #     """
#     #     import re
        
#     #     query_lower = user_query.lower()
        
#     #     # ========================================
#     #     # PRIORITY 1: Check for multi-DATE queries FIRST
#     #     # ========================================
        
#     #     # Pattern 1: Multiple years (e.g., "2024 and 2025")
#     #     year_pattern = r'\b(20\d{2})\b'
#     #     years = re.findall(year_pattern, query_lower)
#     #     if len(years) > 1:
#     #         print(f"[MULTI-QUERY] Detected {len(years)} years: {years}")
#     #         intents = []
#     #         for year in years:
#     #             # Create a modified query for each year
#     #             modified_query = re.sub(r'\b20\d{2}(?:\s+and\s+20\d{2})+\b', year, user_query, flags=re.IGNORECASE)
#     #             intent = self.extract_intent(modified_query)
#     #             intents.append(intent)
#     #         return intents
        
#     #     # Pattern 2: Multiple quarters (e.g., "Q1 and Q2")
#     #     quarter_pattern = r'\bq([1-4])\b'
#     #     quarters = re.findall(quarter_pattern, query_lower)
#     #     if len(quarters) > 1:
#     #         print(f"[MULTI-QUERY] Detected {len(quarters)} quarters: Q{quarters}")
#     #         intents = []
#     #         for q in quarters:
#     #             # Create a modified query for each quarter
#     #             modified_query = re.sub(r'\bq[1-4](?:\s+and\s+q[1-4])+\b | \bvs\b|\bv/s\b', f'q{q}', user_query, flags=re.IGNORECASE)
#     #             intent = self.extract_intent(modified_query)
#     #             intents.append(intent)
#     #         return intents
        
#     #     # Pattern 3: Multiple months (e.g., "april and may")
#     #     month_names = ['january', 'february', 'march', 'april', 'may', 'june', 
#     #                   'july', 'august', 'september', 'october', 'november', 'december',
#     #                   'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        
#     #     found_months = []
#     #     for month in month_names:
#     #         if month in query_lower:
#     #             found_months.append(month)
        
#     #     if len(found_months) > 1:
#     #         print(f"[MULTI-QUERY] Detected {len(found_months)} months: {found_months}")
#     #         intents = []
#     #         for month in found_months:
#     #             # Create a modified query for each month
#     #             month_pattern = '|'.join(found_months)
#     #             modified_query = re.sub(f'\\b({month_pattern})(?:\\s+and\\s+({month_pattern}))+\\b', 
#     #                                    month, user_query, flags=re.IGNORECASE)
#     #             intent = self.extract_intent(modified_query)
#     #             intents.append(intent)
#     #         return intents
        
#     #     # ========================================
#     #     # PRIORITY 2: Check for multi-DIMENSION queries
#     #     # ========================================
        
#     #     # Original dimension-based split logic
#     #     # Keywords that indicate dimension-based multi-query
#     #     dimension_keywords = {
#     #         'product': ['wave city', 'wave estate', 'amore', 'livork'],
#     #         'tower': ['tower', 'block'],
#     #         'floor': ['floor'],
#     #         'type': ['apartment', 'shop', 'plot', 'office'],
#     #     }
        
#     #     # Check if query contains "and" with dimension values
#     #     if ' and ' in query_lower:
#     #         for dim_type, values in dimension_keywords.items():
#     #             found_values = [v for v in values if v in query_lower]
#     #             if len(found_values) >= 2:
#     #                 print(f"[MULTI-QUERY] Detected {len(found_values)} {dim_type} values: {found_values}")
#     #                 intents = []
#     #                 for value in found_values:
#     #                     # Create a query for each dimension value
#     #                     modified_query = user_query.replace(' and ', ' ').lower()
#     #                     # Keep only the current value
#     #                     for other_value in found_values:
#     #                         if other_value != value:
#     #                             modified_query = modified_query.replace(other_value, '')
#     #                     modified_query = modified_query.replace('  ', ' ').strip()
                        
#     #                     intent = self.extract_intent(modified_query)
#     #                     # Add filter for the specific value
#     #                     if not intent.filters:
#     #                         intent.filters = {}
#     #                     intent.filters[dim_type] = value
#     #                     intents.append(intent)
#     #                 return intents
        
#     #     # ========================================
#     #     # DEFAULT: Single query
#     #     # ========================================
#     #     return [self.extract_intent(user_query)]
    


#     def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
#         """
#         Detect if a query contains multiple separate requests and split them.

#         Decision tree (in order):
#         1. vs / versus / v/s  → comparison (handled by _detect_comparison_query)
#         2. Range query (from...to, from...till, between...and) → single query always
#         3. Explicit AND/comma list of same-type items (years, quarters, months, projects) → multi
#         4. Everything else → single query

#         Key rule: only split when `and` explicitly joins items of the SAME type.
#         e.g. "2024 and 2025", "Q1 and Q2", "april and may", "eden and amore"
#         NOT: "from 2023 to 2024", "from june to oct", "between Q1 and Q2"
#         """
#         import re
#         import datetime as _dt

#         query_lower = user_query.lower().strip()

#         # ──────────────────────────────────────────────────────────────
#         # STEP 0: Comparison queries (vs / versus / v/s)
#         # ──────────────────────────────────────────────────────────────
#         comparison = self._detect_comparison_query(user_query)
#         if comparison:
#             print(f"[MULTI-QUERY] Comparison: {comparison['type']}")
#             intents = []

#             if comparison['type'] in ['date_year', 'date_quarter']:
#                 for i, date_range in enumerate(comparison['date_ranges']):
#                     clean = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', user_query, flags=re.IGNORECASE)
#                     for item in comparison['items']:
#                         clean = re.sub(r'\b' + re.escape(item) + r'\b', '', clean, flags=re.IGNORECASE)
#                     clean = re.sub(r'\s+', ' ', clean).strip()
#                     intent = self.extract_intent(clean)
#                     intent.date_range = date_range
#                     intent.original_query = f"{clean} {comparison['items'][i]}"
#                     intents.append(intent)
#                 return intents

#             elif comparison['type'] == 'date_month':
#                 current_year = _dt.datetime.now().year
#                 year_match = re.search(r'\b(20\d{2})\b', query_lower)
#                 if year_match:
#                     current_year = int(year_match.group(1))
#                 for i, month_num in enumerate(comparison['month_nums']):
#                     clean = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', user_query, flags=re.IGNORECASE)
#                     for mn in comparison['items']:
#                         clean = re.sub(r'\b' + re.escape(mn) + r'\b', '', clean, flags=re.IGNORECASE)
#                     if year_match:
#                         clean = re.sub(r'\b' + str(current_year) + r'\b', '', clean)
#                     clean = re.sub(r'\s+', ' ', clean).strip()
#                     intent = self.extract_intent(clean)
#                     intent.date_range = "custom_range"
#                     intent.custom_dates = [{"month_num": month_num, "year": current_year}]
#                     intent.original_query = f"{clean} {comparison['items'][i]} {current_year}"
#                     intents.append(intent)
#                 return intents

#             elif comparison['type'] == 'dimension':
#                 base = user_query.lower()
#                 for item in comparison['items']:
#                     base = base.replace(item.lower(), '')
#                 base = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', base)
#                 base = re.sub(r'\s+', ' ', base).strip()
#                 for i, value in enumerate(comparison['items']):
#                     intent = self.extract_intent(base)
#                     if comparison['dimension'] in intent.dimensions:
#                         intent.dimensions.remove(comparison['dimension'])
#                     if not intent.filters:
#                         intent.filters = {}
#                     intent.filters[comparison['dimension']] = value
#                     label = comparison.get('labels', comparison['items'])[i]
#                     intent.original_query = f"{base} - {label}"
#                     intents.append(intent)
#                 return intents

#             elif comparison['type'] == 'generic':
#                 for item in comparison['items']:
#                     intents.append(self.extract_intent(item))
#                 return intents

#         # ──────────────────────────────────────────────────────────────
#         # STEP 1: Range queries are ALWAYS single queries
#         # Patterns: "from X to Y", "from X till Y", "between X and Y"
#         # ──────────────────────────────────────────────────────────────
#         range_patterns = [
#             r'\bfrom\b.{1,60}\bto\b',
#             r'\bfrom\b.{1,60}\btill\b',
#             r'\bfrom\b.{1,60}\buntil\b',
#             r'\bbetween\b.{1,60}\band\b',
#             r'\bthrough\b',
#         ]
#         if any(re.search(p, query_lower) for p in range_patterns):
#             print("[MULTI-QUERY] Range query — single intent")
#             return [self.extract_intent(user_query)]

#         # ──────────────────────────────────────────────────────────────
#         # STEP 2: AND/comma list of YEARS  e.g. "2023 and 2024"
#         # ──────────────────────────────────────────────────────────────
#         if re.search(r'\b20\d{2}\b(?:\s*,\s*|\s+and\s+)\b20\d{2}\b', query_lower):
#             years = re.findall(r'\b(20\d{2})\b', query_lower)
#             print(f"[MULTI-QUERY] Year list: {years}")
#             intents = []
#             for year in years:
#                 clean = re.sub(r'\b20\d{2}\b(?:\s*,\s*|\s+and\s+)\b20\d{2}\b', year, user_query, flags=re.IGNORECASE)
#                 intents.append(self.extract_intent(clean))
#             return intents

#         # ──────────────────────────────────────────────────────────────
#         # STEP 3: AND/comma list of QUARTERS  e.g. "Q1 and Q2"
#         # ──────────────────────────────────────────────────────────────
#         if re.search(r'\bq([1-4])\b(?:\s*,\s*|\s+and\s+)\bq([1-4])\b', query_lower):
#             quarters = re.findall(r'\bq([1-4])\b', query_lower)
#             print(f"[MULTI-QUERY] Quarter list: Q{quarters}")
#             intents = []
#             for q in quarters:
#                 clean = re.sub(r'\bq[1-4]\b(?:\s*,\s*|\s+and\s+)\bq[1-4]\b', f'q{q}', user_query, flags=re.IGNORECASE)
#                 intents.append(self.extract_intent(clean))
#             return intents

#         # ──────────────────────────────────────────────────────────────
#         # STEP 4: AND/comma list of MONTHS  e.g. "april and may"
#         # Only when months are DIRECTLY joined by "and" or ","
#         # ──────────────────────────────────────────────────────────────
#         _months = ('january|february|march|april|may|june|july|august|september'
#                    '|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec')
#         if re.search(rf'\b({_months})\b(?:\s*,\s*|\s+and\s+)\b({_months})\b', query_lower):
#             found = [m for m in _months.split('|') if re.search(r'\b' + m + r'\b', query_lower)]
#             # deduplicate preserving order
#             seen, unique = set(), []
#             for m in found:
#                 if m not in seen:
#                     seen.add(m); unique.append(m)
#             if len(unique) >= 2:
#                 print(f"[MULTI-QUERY] Month list: {unique}")
#                 intents = []
#                 all_re = '|'.join(re.escape(m) for m in unique)
#                 for month in unique:
#                     clean = re.sub(
#                         rf'\b({all_re})\b(?:\s*,\s*|\s+and\s+)\b({all_re})\b',
#                         month, user_query, flags=re.IGNORECASE
#                     )
#                     intents.append(self.extract_intent(clean))
#                 return intents

#         # ──────────────────────────────────────────────────────────────
#         # STEP 5: AND list of named DIMENSION VALUES
#         # e.g. "wave city and wave estate", "broker and direct", "eden and amore"
#         # Each entry: dimension_key -> list of (trigger_phrase, filter_value) tuples
#         # filter_value is what gets passed to _build_single_value_filter (LIKE match)
#         #
#         # IMPORTANT: ALL matched trigger phrases are stripped from the base query
#         # before passing to extract_intent. This prevents the LLM from misreading
#         # project/product names as dimension keywords (e.g. "city" → dimension).
#         # The filter is applied explicitly after intent extraction.
#         # ──────────────────────────────────────────────────────────────
#         if ' and ' in query_lower or ',' in query_lower:
#             dim_values = {
#                 # Project / Sales Org  e.g. "wave city and wave estate"
#                 'sales_org_desc': [
#                     ('wave city', 'Wave City'),
#                     ('wave estate', 'Wave Estate'),
#                     ('wmcc', 'WMCC'),
#                 ],
#                 # Product / Sales Group  e.g. "eden and amore", "fsi and hssc", "veridia and eligo"
#                 'sales_group_desc': [
#                     ('eligo', 'ELIGO'),
#                     ('eden', 'EDEN'),
#                     ('amore', 'AMORE'),
#                     ('livork', 'LIVORK'),
#                     ('veridia 7', 'VERIDIA-7'),
#                     ('veridia 6', 'VERIDIA-6'),
#                     ('veridia 5', 'VERIDIA-5'),
#                     ('veridia 4', 'VERIDIA-4'),
#                     ('veridia 3', 'VERIDIA-3'),
#                     ('veridia', 'VERIDIA'),
#                     ('edenia', 'EDENIA'),
#                     ('elegantia', 'ELEGANTIA'),
#                     ('eminence', 'EMINENCE'),
#                     ('irenia', 'IRENIA'),
#                     ('trucia', 'TRUCIA'),
#                     ('vasilia', 'VASILIA'),
#                     ('mayfair park', 'MAYFAIR PARK'),
#                     ('mayfair', 'MAYFAIR PARK'),
#                     ('harmony greens', 'HARMONY GREENS'),
#                     ('wave galleria', 'WAVE GALLERIA'),
#                     ('galleria', 'WAVE GALLERIA'),
#                     ('wave garden', 'WAVE GARDEN'),
#                     ('wave floor 99', 'WAVE FLOOR 99'),
#                     ('wave floor 85', 'WAVE FLOOR 85'),
#                     ('wave floor', 'WAVE FLOOR'),
#                     ('dream homes', 'DREAM HOMES'),
#                     ('dream bazaar', 'DREAM BAZAAR'),
#                     ('villas', 'VILLAS'),
#                     ('armonia villa', 'ARMONIA VILLA'),
#                     ('armonia', 'ARMONIA VILLA'),
#                     ('business square', 'WAVE BUSSINESS SQUARE'),
#                     ('wave estate gh2', 'WAVE ESTATE, GH2 PH2'),
#                     ('executive floors', 'EXECUTIVE FLOORS'),
#                     ('prime floors', 'PRIME FLOORS'),
#                     ('new plots', 'NEW PLOTS'),
#                     ('old plots', 'OLD PLOTS'),
#                     ('commercial plots', 'COMMERCIAL PLOTS'),
#                     ('residential plots', 'PLOTS-RES'),
#                     ('fsi', 'FSI'),
#                     ('hssc', 'HSSC'),
#                     ('institutional', 'INSTITUTIONAL'),
#                     ('metro mart', 'METRO MART'),
#                     ('swamanorath', 'SWAMANORATH'),
#                     ('wave bussiness square', 'WAVE BUSSINESS SQUARE'),
#                     ('wbt 1', 'WBT 1'),
#                     ('wbt a', 'WBT A'),
#                     ('sco', 'SCO'),
#                     ('comm booth', 'COMM BOOTH'),
#                     ('ews p2', 'EWS_P2'),
#                     ('lig p2', 'LIG_P2'),
#                 ],
#                 # Distribution Channel  e.g. "broker and direct"
#                 'dist_channel_desc': [
#                     ('broker', 'Broker'),
#                     ('direct', 'Direct'),
#                     ('walk-in', 'Direct'),
#                     ('walkin', 'Direct'),
#                     ('channel partner', 'Broker'),
#                     ('referral', 'Referral'),
#                 ],
#                 # Booking Type  e.g. "fresh and transfer"
#                 'booking_type': [
#                     ('fresh', 'Fresh'),
#                     ('transfer', 'Transfer'),
#                     ('resale', 'Resale'),
#                 ],
#             }
#             for dim, value_tuples in dim_values.items():
#                 # Find which trigger phrases appear in the query
#                 matched = []
#                 seen_filter_vals = set()
#                 for trigger, filter_val in value_tuples:
#                     if re.search(r'\b' + re.escape(trigger) + r'\b', query_lower):
#                         if filter_val not in seen_filter_vals:
#                             matched.append((trigger, filter_val))
#                             seen_filter_vals.add(filter_val)

#                 if len(matched) >= 2:
#                     print(f"[MULTI-QUERY] Dimension AND-list ({dim}): {[m[1] for m in matched]}")
#                     intents = []

#                     # Build a base query with ALL matched trigger phrases removed.
#                     # This prevents the LLM from misinterpreting product/project names
#                     # as dimension names (e.g. "city" in "wave city" → dimension error).
#                     base_clean = query_lower
#                     for trigger, _ in matched:
#                         base_clean = re.sub(r'\b' + re.escape(trigger) + r'\b', '', base_clean)
#                     base_clean = re.sub(r'\band\b', '', base_clean)
#                     base_clean = re.sub(r'\bof\b\s*$|\bof\b\s+(?=and|,|$)', '', base_clean)
#                     base_clean = re.sub(r'\s+', ' ', base_clean).strip()

#                     # Extract intent once from the clean base (no product names in it)
#                     base_intent = self.extract_intent(base_clean)

#                     for trigger, filter_val in matched:
#                         # Copy base intent and apply this specific filter
#                         import copy
#                         intent = copy.deepcopy(base_intent)
#                         if not intent.filters:
#                             intent.filters = {}
#                         intent.filters[dim] = filter_val
#                         intent.original_query = f"{base_clean} - {filter_val}"
#                         intents.append(intent)
#                     return intents

#         # ──────────────────────────────────────────────────────────────
#         # STEP 6: AND list of FLOORS  e.g. "10th floor and 8th floor"
#         # Patterns: "Nth floor and Mth floor", "floor N and floor M"
#         # ──────────────────────────────────────────────────────────────
#         if 'floor' in query_lower and ' and ' in query_lower:
#             floor_and_patterns = [
#                 r'(\d+)(?:st|nd|rd|th)?\s+floor\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor',  # "10th floor and 8th floor"
#                 r'floor\s+(\d+)\s+and\s+floor\s+(\d+)',                                   # "floor 10 and floor 8"
#                 r'(\d+)(?:st|nd|rd|th)?\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor',           # "10th and 8th floor"
#             ]
#             for fpat in floor_and_patterns:
#                 fm = re.search(fpat, query_lower)
#                 if fm:
#                     floor1, floor2 = fm.group(1), fm.group(2)
#                     print(f"[MULTI-QUERY] Floor list: [{floor1}, {floor2}]")
#                     intents = []
#                     # Build a base query with the floor list removed
#                     base_clean = re.sub(fpat, '', query_lower, flags=re.IGNORECASE)
#                     base_clean = re.sub(r'\s+', ' ', base_clean).strip()
#                     import copy
#                     base_intent = self.extract_intent(base_clean)
#                     for floor_num in [floor1, floor2]:
#                         intent = copy.deepcopy(base_intent)
#                         if not intent.filters:
#                             intent.filters = {}
#                         intent.filters['floor'] = floor_num
#                         intent.original_query = f"{base_clean} - floor {floor_num}"
#                         intents.append(intent)
#                     return intents

#         # ──────────────────────────────────────────────────────────────
#         # DEFAULT: single query
#         # ──────────────────────────────────────────────────────────────
#         return [self.extract_intent(user_query)]


#     def _detect_multi_date_query(self, query: str) -> List[Dict]:
#         """
#         Detect if query requests multiple separate time periods.
        
#         Examples:
#             "total sales in 2024 and 2025" → [{"year": 2024}, {"year": 2025}]
#             "sales in Q1 and Q2" → [{"quarter": 1}, {"quarter": 2}]
#             "sales in april and may" → [{"month": 4}, {"month": 5}]
            
        
#         Returns:
#             List of date dictionaries, or empty list if single period
#         """
#         query_lower = query.lower()
        
#         # Pattern 1: Multiple years (e.g., "2024 and 2025", "in 2024, 2025")
#         year_pattern = r'\b(20\d{2})\b'
#         years = re.findall(year_pattern, query_lower)
#         if len(years) > 1:
#             return [{"year": int(y)} for y in years]
        
#         # Pattern 2: Multiple quarters (e.g., "Q1 and Q2", "Q1, Q2, Q3")
#         quarter_pattern = r'\bq([1-4])\b'
#         quarters = re.findall(quarter_pattern, query_lower)
#         if len(quarters) > 1:
#             return [{"quarter": int(q)} for q in quarters]
        
#         # Pattern 3: Multiple months (e.g., "april and may", "jan, feb, mar")
#         month_names = {
#             'january': 1, 'jan': 1,
#             'february': 2, 'feb': 2,
#             'march': 3, 'mar': 3,
#             'april': 4, 'apr': 4,
#             'may': 5,
#             'june': 6, 'jun': 6,
#             'july': 7, 'jul': 7,
#             'august': 8, 'aug': 8,
#             'september': 9, 'sep': 9, 'sept': 9,
#             'october': 10, 'oct': 10,
#             'november': 11, 'nov': 11,
#             'december': 12, 'dec': 12
#         }
        
#         found_months = []
#         for month_name, month_num in month_names.items():
#             if month_name in query_lower:
#                 found_months.append(month_num)
        
#         # Remove duplicates while preserving order
#         found_months = list(dict.fromkeys(found_months))
        
#         if len(found_months) > 1:
#             return [{"month_num": m} for m in found_months]
        
#         return []
    

#     def _detect_comparison_query(self, user_query: str) -> Optional[Dict]:
#         """
#         Detect if query is a comparison using "vs", "v/s", or "versus".
        
#         Examples:
#             "2024 vs 2025" → {"type": "date", "items": ["2024", "2025"]}
#             "broker vs direct" → {"type": "dimension", "items": ["broker", "direct"]}
#             "Q1 vs Q2" → {"type": "date", "items": ["Q1", "Q2"]}
        
#         Returns:
#             Dictionary with comparison type and items, or None if not a comparison
#         """
#         import re
        
#         query_lower = user_query.lower()
        
#         # Check for comparison keywords
#         comparison_patterns = [
#             r'\bv/s\b',
#             r'\bvs\b',
#             r'\bversus\b',
#             r'\bv\.s\b',
#             r'\bcompare\b'
#         ]
        
#         has_comparison = False
#         for pattern in comparison_patterns:
#             if re.search(pattern, query_lower):
#                 has_comparison = True
#                 break
        
#         if not has_comparison:
#             return None
        
#         print(f"[COMPARISON] Detected comparison query: {user_query}")
        
#         # ========================================
#         # Type 1: DATE COMPARISONS
#         # ========================================
        
#         # Years (2024 vs 2025)
#         year_pattern = r'\b(20\d{2})\b'
#         years = re.findall(year_pattern, query_lower)
#         if len(years) == 2:
#             print(f"[COMPARISON] Year comparison: {years}")
#             return {
#                 "type": "date_year",
#                 "items": years,
#                 "date_ranges": [f"fy_{y}" for y in years]
#             }
        
#         # Quarters (Q1 vs Q2)
#         quarter_pattern = r'\bq([1-4])\b'
#         quarters = re.findall(quarter_pattern, query_lower)
#         if len(quarters) == 2:
#             print(f"[COMPARISON] Quarter comparison: Q{quarters}")
#             return {
#                 "type": "date_quarter",
#                 "items": [f"Q{q}" for q in quarters],
#                 "date_ranges": [f"q{q}" for q in quarters]
#             }
        
#         # Months (april vs may)
#         month_map = {
#             'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
#             'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
#             'may': 5, 'june': 6, 'jun': 6,
#             'july': 7, 'jul': 7, 'august': 8, 'aug': 8,
#             'september': 9, 'sep': 9, 'sept': 9,
#             'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
#             'december': 12, 'dec': 12
#         }
        
#         found_months = []
#         for month_name, month_num in month_map.items():
#             if month_name in query_lower:
#                 found_months.append((month_name, month_num))
        
#         if len(found_months) == 2:
#             print(f"[COMPARISON] Month comparison: {[m[0] for m in found_months]}")
#             return {
#                 "type": "date_month",
#                 "items": [m[0] for m in found_months],
#                 "month_nums": [m[1] for m in found_months]
#             }
        
#         # ========================================
#         # Type 2: DIMENSION VALUE COMPARISONS
#         # ========================================
        
#         # Split query by comparison keyword
#         # split_pattern = r'\s+(?:v/s|vs|versus|v\.s)\s+'
#         # parts = re.split(split_pattern, query_lower, flags=re.IGNORECASE)
        
#         # if len(parts) == 2:
#         #     left = parts[0].strip()
#         #     right = parts[1].strip()
            
#         #     # Common dimension value patterns
#         #     dimension_patterns = {
#         #         # Channel
#         #         'dist_channel_desc': {
#         #             'broker': ['broker', 'channel partner', 'agent'],
#         #             'direct': ['direct', 'walk-in', 'walkin', 'self']
#         #         },
#         #         # Division
#         #         'division_desc': {
#         #             'residential': ['residential', 'housing', 'apartment'],
#         #             'commercial': ['commercial', 'office', 'shop', 'retail']
#         #         },
#         #         # Product
#         #         'sales_group_desc': {
#         #             'wave city': ['wave city', 'wavecity'],
#         #             'wave estate': ['wave estate', 'waveestate'],
#         #             'amore': ['amore'],
#         #             'livork': ['livork']
#         #         },
#         #         # Booking Type
#         #         'booking_type': {
#         #             'fresh': ['fresh', 'new booking'],
#         #             'transfer': ['transfer', 'transferred']
#         #         }
#         #     }
            
#         #     # Check which dimension this comparison belongs to
#         #     for dim_name, value_patterns in dimension_patterns.items():
#         #         left_match = None
#         #         right_match = None
                
#         #         for value_key, patterns in value_patterns.items():
#         #             for pattern in patterns:
#         #                 if pattern in left:
#         #                     left_match = value_key
#         #                 if pattern in right:
#         #                     right_match = value_key
                
#         #         if left_match and right_match:
#         #             print(f"[COMPARISON] Dimension comparison: {dim_name} - {left_match} vs {right_match}")
#         #             return {
#         #                 "type": "dimension",
#         #                 "dimension": dim_name,
#         #                 "items": [left_match, right_match]
#         #             }


#         # ========================================
#         # Type 2: DIMENSION VALUE COMPARISONS
#         # ========================================
        
#         # Split query by comparison keyword
#         split_pattern = r'\s+(?:v/s|vs|versus|v\.s)\s+'
#         parts = re.split(split_pattern, query_lower, flags=re.IGNORECASE)
        
#         if len(parts) == 2:
#             left = parts[0].strip()
#             right = parts[1].strip()
            
#             # Common dimension value patterns
#             dimension_patterns = {
#                 # Channel - MOST IMPORTANT
#                 'dist_channel_desc': {
#                     'patterns': {
#                         'broker': ['broker', 'channel partner', 'agent', 'brokers'],
#                         'direct': ['direct', 'walk-in', 'walkin', 'self', 'walk in']
#                     },
#                     'priority': 1  # Check this first
#                 },
#                 # Division
#                 'division_desc': {
#                     'patterns': {
#                         'residential': ['residential', 'housing', 'apartment', 'resi'],
#                         'commercial': ['commercial', 'office', 'shop', 'retail', 'comm']
#                     },
#                     'priority': 2
#                 },
#                 # Product
#                 'sales_group_desc': {
#                     'patterns': {
#                         'Wave City': ['wave city', 'wavecity'],
#                         'Wave Estate': ['wave estate', 'waveestate'],
#                         'Amore': ['amore'],
#                         'Livork': ['livork']
#                     },
#                     'priority': 3
#                 },
#                 # Booking Type
#                 'booking_type': {
#                     'patterns': {
#                         'Fresh': ['fresh', 'new booking', 'new'],
#                         'Transfer': ['transfer', 'transferred']
#                     },
#                     'priority': 4
#                 }
#             }
            
#             # Sort by priority
#             sorted_patterns = sorted(
#                 dimension_patterns.items(), 
#                 key=lambda x: x[1].get('priority', 999)
#             )
            
#             # Check which dimension this comparison belongs to
#             for dim_name, dim_config in sorted_patterns:
#                 left_match = None
#                 right_match = None
                
#                 value_patterns = dim_config['patterns']
                
#                 for value_key, patterns in value_patterns.items():
#                     for pattern in patterns:
#                         if pattern in left:
#                             left_match = value_key
#                         if pattern in right:
#                             right_match = value_key
                
#                 if left_match and right_match:
#                     print(f"[COMPARISON] Dimension comparison: {dim_name} - '{left_match}' vs '{right_match}'")
#                     return {
#                         "type": "dimension",
#                         "dimension": dim_name,
#                         "items": [left_match, right_match],
#                         "labels": [left_match, right_match]
#                     }
        
#         # ========================================
#         # Type 3: GENERIC COMPARISON (fallback)
#         # ========================================
        
#         # If we detected comparison keywords but couldn't classify the type,
#         # treat it as a generic multi-query comparison
#         if len(parts) == 2:
#             print(f"[COMPARISON] Generic comparison: {parts}")
#             return {
#                 "type": "generic",
#                 "items": parts
#             }
        
#         return None
    


#     def _detect_cancellation_query(self, user_query: str) -> Optional[SemanticIntent]:
#         """
#         Detect cancellation-related queries and build the correct intent.

#         Always applies:
#         - metric: cancelled_sales
#         - mandatory filter: Customer_Type = 'cancelled'  (from metric definition)
#         - dimension: cancellation_reason (Description column) by default

#         Also supports filters by:
#         - payer_name, sold_to_name, sales_order, sales_group_desc, project, product
#         """
#         query_lower = user_query.lower()

#         cancellation_triggers = [
#             "cancellation reason", "reason for cancellation", "cancel reason",
#             "why cancelled", "why was cancelled", "cancelled reason",
#             "cancellation reason wise", "reason of cancellation",
#         ]

#         if not any(trigger in query_lower for trigger in cancellation_triggers):
#             return None

#         print(f"[CANCELLATION] Detected cancellation reason query")

#         # Always group by cancellation reason (Description column)
#         dimensions = ["cancellation_reason"]

#         # Check for additional dimension filters
#         filters = {}

#         # By payer
#         if any(kw in query_lower for kw in ["payer", "paying customer"]):
#             dimensions.append("payer_name")

#         # By sold to / customer name
#         if any(kw in query_lower for kw in ["sold to", "customer", "buyer"]):
#             dimensions.append("sold_to_name")

#         # By sales order number — extract it
#         order_match = re.search(r'\b(\d{7,12})\b', query_lower)
#         if order_match:
#             filters["sales_order"] = order_match.group(1)

#         # By project
#         if any(kw in query_lower for kw in ["project", "wave city", "wave estate", "wmcc"]):
#             dimensions.append("sales_org_desc")

#         # By product / sales group (eden, amore, livork)
#         product_map = {"eden": "Eden", "amore": "Amore", "livork": "Livork"}
#         for kw, val in product_map.items():
#             if kw in query_lower:
#                 filters["sales_group_desc"] = val

#         # By tower
#         if "tower" in query_lower:
#             dimensions.append("tower")

#         # By broker
#         if "broker" in query_lower:
#             dimensions.append("broker_name")

#         # Extract date
#         custom_dates, date_range = self._extract_custom_dates_enhanced(user_query)

#         intent = SemanticIntent(
#             metric="cancelled_sales",
#             dimensions=dimensions,
#             date_range=date_range or "current_financial_year",
#             custom_dates=custom_dates or [],
#             filters=filters if filters else None,
#             original_query=user_query,
#         )

#         return intent



#     # =====================================================
#     # CUSTOM DATE EXTRACTION (NEW!)
#     # =====================================================
    
#     def _extract_custom_dates_enhanced(self, query: str) -> Tuple[List[Dict], str]:
#         """
#         Extract custom dates from natural language query with proper Financial Year (FY) handling.
#         FY runs from April to March (Apr 2025 - Mar 2026 is current)
        
#         Handles:
#         1. Date ranges: "from 23 jan to 5 feb", "from 20 jan 2022 to 2 feb 2023"
#         2. Specific dates: "20 jan", "23 march 2025"
#         3. From patterns: "from may" (may to end of current FY = March)
#         4. From-till patterns: "from may till 15 jan", "from sep till 15 feb"
#         5. From-to patterns: "from sep to feb", "from april to sep 2024"
#         6. Till patterns: "till 13 dec" (april to 13 dec)
#         7. Month ranges: "april to june"
#         8. Quarters: "q1", "q2", "q3", "q4", "q1 2024", "q4 2023"
        
#         Returns: (custom_dates_list, date_range_type)
#         """
#         query_lower = query.lower().strip()
#         today = datetime.datetime.now()
#         current_year = today.year
#         current_month = today.month
        
#         print(f"[EXTRACT_ENHANCED] Input query: '{query}' (lowercased: '{query_lower}')")
        
#         # *** CRITICAL: Determine correct Financial Year (Apr-Mar) ***
#         # Current date: Feb 9, 2026 → FY is April 2025 - March 2026
#         # Logic: If month is Apr-Dec (>=4), FY starts in current year
#         #        If month is Jan-Mar (<4), FY started in previous year
#         if current_month >= 4:
#             fy_start_year = current_year
#             fy_end_year = current_year + 1
#         else:
#             # We're in Jan-Mar, so FY started last year
#             fy_start_year = current_year - 1
#             fy_end_year = current_year
        
#         print(f"[FY_CALC] Today: {today.date()}, Month: {current_month}, FY: {fy_start_year}-{fy_end_year}")
        
#         def get_fy_year_for_month(month_num: int, explicit_year: int = None) -> int:
#             """
#             Get the correct financial year for a given month.
#             If explicit_year provided, use it. Otherwise, infer from FY context.
            
#             Examples (assuming current FY is 2025-2026, i.e., Apr 2025 - Mar 2026):
#             - April-Dec: return 2025 (fy_start_year)
#             - Jan-Mar: return 2026 (fy_end_year)
#             """
#             if explicit_year:
#                 return explicit_year
            
#             # Months Apr(4) through Dec(12) belong to fy_start_year
#             # Months Jan(1) through Mar(3) belong to fy_end_year
#             if month_num >= 4:
#                 return fy_start_year
#             else:
#                 return fy_end_year
        
#         # ========================================
#         # PATTERN 0 (PRIORITY): Financial Quarter (e.g., "q1", "q2", "q3", "q4", "q4 2024", "q1_2025")
#         # Check FIRST to avoid conflicts with month patterns
#         # FY Quarters: Q1=Apr-Jun(4-6), Q2=Jul-Sep(7-9), Q3=Oct-Dec(10-12), Q4=Jan-Mar(1-3)
#         # ========================================
#         # PRE-PATTERN 0: "quarter N YYYY" / "quarter N" (written-out quarter)
#         # Examples: "quarter 3 2023" → Q3 FY2023, "3rd quarter 2022" → Q3 FY2022
#         # Covers: "quarter 1", "quarter 2", "1st quarter", "2nd quarter" etc.
#         written_quarter_pattern = r'(?:(?:1st|2nd|3rd|4th|first|second|third|fourth)\s+quarter|quarter\s+(?:1|2|3|4|one|two|three|four))(?:\s+(\d{4}))?'
#         written_q_match = re.search(written_quarter_pattern, query_lower)
#         if written_q_match:
#             # Map words to quarter numbers
#             q_word_map = {
#                 "1st": 1, "first": 1, "one": 1, "1": 1,
#                 "2nd": 2, "second": 2, "two": 2, "2": 2,
#                 "3rd": 3, "third": 3, "three": 3, "3": 3,
#                 "4th": 4, "fourth": 4, "four": 4, "4": 4,
#             }
#             raw = written_q_match.group()
#             # Extract the number word
#             q_num = None
#             for token in re.split(r'\s+', raw):
#                 if token in q_word_map:
#                     q_num = q_word_map[token]
#                     break
#             year_explicit = int(written_q_match.group(1)) if written_q_match.group(1) else None

#             if q_num:
#                 quarter_months = {1: (4, 6), 2: (7, 9), 3: (10, 12), 4: (1, 3)}
#                 start_month, end_month = quarter_months[q_num]
#                 if year_explicit:
#                     if q_num in (1, 2, 3):
#                         start_year = end_year = year_explicit
#                     else:
#                         start_year = end_year = year_explicit + 1
#                 else:
#                     if q_num in (1, 2, 3):
#                         start_year = end_year = fy_start_year
#                     else:
#                         start_year = end_year = fy_end_year

#                 print(f"[PRE-PATTERN 0 - WRITTEN QUARTER] Q{q_num} year={year_explicit or 'current FY'}: {start_month}/{start_year} to {end_month}/{end_year}")
#                 return [
#                     {"month_num": start_month, "year": start_year},
#                     {"month_num": end_month, "year": end_year}
#                 ], "custom_range"

#         # ========================================
#         # Try pattern with optional space or underscore + year
#         quarter_pattern = r'\bq([1-4])(?:[\s_]+(\d{4}))?\b'
#         print(f"[PATTERN 0] Searching with regex: {quarter_pattern}")
#         quarter_match = re.search(quarter_pattern, query_lower)
        
#         if quarter_match:
#             q_num = int(quarter_match.group(1))  # 1, 2, 3, or 4
#             year_explicit = int(quarter_match.group(2)) if quarter_match.group(2) else None
            
#             print(f"[PATTERN 0 - MATCH] YES! Matched: q{q_num}, year_explicit: {year_explicit}, full match: '{quarter_match.group()}'")
            
#             # Map quarter to months (based on Financial Year Apr-Mar)
#             quarter_months = {
#                 1: (4, 5, 6),    # Q1: Apr, May, Jun
#                 2: (7, 8, 9),    # Q2: Jul, Aug, Sep
#                 3: (10, 11, 12), # Q3: Oct, Nov, Dec
#                 4: (1, 2, 3),    # Q4: Jan, Feb, Mar
#             }
            
#             start_month, mid_month, end_month = quarter_months[q_num]
            
#             # Determine year(s) for the quarter
#             if year_explicit:
#                 # User specified explicit year (e.g., "q1 2024", "q4 2023")
#                 # The year refers to the Financial Year (FY)
#                 # Q1-Q3 of FY 2024 = Apr-Dec 2024
#                 # Q4 of FY 2024 = Jan-Mar 2025
#                 if q_num in (1, 2, 3):
#                     # Q1-Q3: use exact year (Apr-Dec of that year)
#                     start_year = year_explicit
#                     end_year = year_explicit
#                 else:
#                     # Q4: Jan-Mar of the NEXT year (Q4 of FY 2024 = Jan-Mar 2025)
#                     start_year = year_explicit + 1
#                     end_year = year_explicit + 1
#             else:
#                 # No explicit year - use Financial Year context
#                 # Current FY: Apr fy_start_year - Mar fy_end_year
#                 if q_num in (1, 2, 3):
#                     # Q1-Q3 (Apr-Dec): use FY start year
#                     start_year = fy_start_year
#                     end_year = fy_start_year
#                 else:
#                     # Q4 (Jan-Mar): use FY end year
#                     start_year = fy_end_year
#                     end_year = fy_end_year
            
#             print(f"[PATTERN 0 - QUARTER] q{q_num}: months {start_month}-{end_month}, year {start_year}-{end_year}")
#             print(f"[PATTERN 0 - DATES] Start: month={start_month}/year={start_year}, End: month={end_month}/year={end_year}")
#             return [
#                 {"month_num": start_month, "year": start_year},
#                 {"month_num": end_month, "year": end_year}
#             ], "custom_range"
#         else:
#             print(f"[PATTERN 0 - NO MATCH] Quarter pattern did not match")
        
#         # PATTERN 1: "from DATE to DATE" (e.g., "from 23 jan to 5 feb", "from 20 jan 2022 to 2 feb 2023")
#         from_date_to_date = r'from\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+to\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         from_date_to_date_match = re.search(from_date_to_date, query_lower)
        
#         if from_date_to_date_match:
#             day1 = int(from_date_to_date_match.group(1))
#             month1_str = from_date_to_date_match.group(2)
#             month1_num = self.MONTH_NAMES.get(month1_str, 1)
#             year1_explicit = int(from_date_to_date_match.group(3)) if from_date_to_date_match.group(3) else None
            
#             day2 = int(from_date_to_date_match.group(4))
#             month2_str = from_date_to_date_match.group(5)
#             month2_num = self.MONTH_NAMES.get(month2_str, 12)
#             year2_explicit = int(from_date_to_date_match.group(6)) if from_date_to_date_match.group(6) else None
            
#             # Intelligent year assignment
#             if year1_explicit and year2_explicit:
#                 year1 = year1_explicit
#                 year2 = year2_explicit
#             elif year1_explicit and not year2_explicit:
#                 year1 = year1_explicit
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
#             elif year2_explicit and not year1_explicit:
#                 year2 = year2_explicit
#                 if month1_num <= month2_num:
#                     year1 = year2
#                 else:
#                     year1 = year2 - 1
#             else:
#                 year1 = get_fy_year_for_month(month1_num)
#                 year2 = get_fy_year_for_month(month2_num)
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
            
#             print(f"[PATTERN 1] from date to date: {day1}/{month1_num}/{year1} to {day2}/{month2_num}/{year2}")
#             return [
#                 {"day": day1, "month_num": month1_num, "year": year1},
#                 {"day": day2, "month_num": month2_num, "year": year2}
#             ], "custom_range"
        
#         # PATTERN 2: "from MONTH till DATE" (e.g., "from may till 15 jan", "from sep till 15 feb")
#         from_month_till_date = r'from\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+till\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         from_month_till_date_match = re.search(from_month_till_date, query_lower)
        
#         if from_month_till_date_match:
#             month1_str = from_month_till_date_match.group(1)
#             month1_num = self.MONTH_NAMES.get(month1_str, 1)
#             year1_explicit = int(from_month_till_date_match.group(2)) if from_month_till_date_match.group(2) else None
            
#             day2 = int(from_month_till_date_match.group(3))
#             month2_str = from_month_till_date_match.group(4)
#             month2_num = self.MONTH_NAMES.get(month2_str, 12)
#             year2_explicit = int(from_month_till_date_match.group(5)) if from_month_till_date_match.group(5) else None
            
#             # Intelligent year assignment
#             if year1_explicit and year2_explicit:
#                 year1 = year1_explicit
#                 year2 = year2_explicit
#             elif year1_explicit and not year2_explicit:
#                 year1 = year1_explicit
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
#             elif year2_explicit and not year1_explicit:
#                 year2 = year2_explicit
#                 if month1_num <= month2_num:
#                     year1 = year2
#                 else:
#                     year1 = year2 - 1
#             else:
#                 year1 = get_fy_year_for_month(month1_num)
#                 year2 = get_fy_year_for_month(month2_num)
#                 if not year2_explicit and month2_num < month1_num:
#                     year2 = year1 + 1
            
#             print(f"[PATTERN 2] from month till date: month {month1_num}/{year1} till {day2}/{month2_num}/{year2}")
#             return [
#                 {"month_num": month1_num, "year": year1},
#                 {"day": day2, "month_num": month2_num, "year": year2}
#             ], "custom_range"
        
#         # PATTERN 3: "from MONTH to MONTH" (e.g., "from sep to feb", "from april to sep 2024")
#         from_month_to_month = r'from\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+to\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         from_month_to_month_match = re.search(from_month_to_month, query_lower)
        
#         if from_month_to_month_match:
#             month1_str = from_month_to_month_match.group(1)
#             month1_num = self.MONTH_NAMES.get(month1_str, 1)
#             year1_explicit = int(from_month_to_month_match.group(2)) if from_month_to_month_match.group(2) else None
            
#             month2_str = from_month_to_month_match.group(3)
#             month2_num = self.MONTH_NAMES.get(month2_str, 12)
#             year2_explicit = int(from_month_to_month_match.group(4)) if from_month_to_month_match.group(4) else None
            
#             # Intelligent year assignment logic:
#             # Priority: explicit years > inferred from one year > FY context
#             if year1_explicit and year2_explicit:
#                 # Both years explicit
#                 year1 = year1_explicit
#                 year2 = year2_explicit
#             elif year1_explicit and not year2_explicit:
#                 # Only year1 explicit - infer year2 from month order
#                 year1 = year1_explicit
#                 if month2_num < month1_num:
#                     # Crosses year boundary (e.g., "from sep 2024 to feb" -> 2024 to 2025)
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
#             elif year2_explicit and not year1_explicit:
#                 # Only year2 explicit - infer year1 from month order
#                 year2 = year2_explicit
#                 if month1_num <= month2_num:
#                     # Same calendar order (e.g., "from april to sep 2024" -> both 2024)
#                     year1 = year2
#                 else:
#                     # Crosses year boundary (e.g., "from sep to april 2024" -> sep 2023 to apr 2024)
#                     year1 = year2 - 1
#             else:
#                 # No explicit years - use FY context
#                 year1 = get_fy_year_for_month(month1_num)
#                 if month2_num < month1_num:
#                     # Crosses FY boundary (e.g., "from oct to feb" = Oct 2025 to Feb 2026)
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
            
#             print(f"[PATTERN 3] from month to month: {month1_num}/{year1} to {month2_num}/{year2}")
#             return [
#                 {"month_num": month1_num, "year": year1},
#                 {"month_num": month2_num, "year": year2}
#             ], "custom_range"
        
#         # PATTERN 5: "till DATE" or "until DATE" pattern (from start of FY to specified date)
#         # MUST come before Pattern 4 (single date) to avoid "13 oct" being grabbed as a lone date
#         till_date_pattern = r'(?:till|until)\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         till_date_match = re.search(till_date_pattern, query_lower)
        
#         if till_date_match:
#             day = int(till_date_match.group(1))
#             month_str = till_date_match.group(2)
#             month_num = self.MONTH_NAMES.get(month_str, 12)
#             year_explicit = int(till_date_match.group(3)) if till_date_match.group(3) else None
#             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
#             # Start year: if ending month is Jan-Mar (2026), start is Apr 2025
#             start_year = year if month_num >= 4 else year - 1
            
#             print(f"[PATTERN 5] till date: Apr/{start_year} till {day}/{month_num}/{year}")
#             return [
#                 {"month_num": 4, "year": start_year},
#                 {"day": day, "month_num": month_num, "year": year}
#             ], "custom_range"
        
#         # PATTERN 6A: "from DAY MONTH" pattern (from specific date to end of current FY)
#         # Examples: "from 15 sep" → Sep 15 to Mar 31 (end of current FY)
#         #           "from 15 sep 2022" → Sep 15 2022 to Mar 31 2023
#         # MUST come before Pattern 4 (single date) to avoid "15 sep" being grabbed as a lone date
#         from_day_month_pattern = r'\bfrom\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
#         from_day_match = re.search(from_day_month_pattern, query_lower)

#         if from_day_match:
#             day = int(from_day_match.group(1))
#             month_str = from_day_match.group(2)
#             month_num = self.MONTH_NAMES.get(month_str, 4)
#             year_explicit = int(from_day_match.group(3)) if from_day_match.group(3) else None
#             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)

#             # End: March 31 of the FY that contains the start month
#             end_year = year + 1 if month_num >= 4 else year

#             print(f"[PATTERN 6A - FROM DAY MONTH] {day}/{month_num}/{year} to 31/3/{end_year}")
#             return [
#                 {"day": day, "month_num": month_num, "year": year},
#                 {"day": 31, "month_num": 3, "year": end_year}
#             ], "custom_range"

#         # PATTERN 4: Single specific date (e.g., "20 jan", "5 february 2025", "20 jan 2024", "on 15 sep")
#         # Placed AFTER till/from open-range patterns so those take priority.
#         day_month_pattern = r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         day_month_match = re.search(day_month_pattern, query_lower)
        
#         if day_month_match:
#             day = int(day_month_match.group(1))
#             month_str = day_month_match.group(2)
#             month_num = self.MONTH_NAMES.get(month_str, 1)
#             year = int(day_month_match.group(3)) if day_month_match.group(3) else get_fy_year_for_month(month_num)
            
#             print(f"[PATTERN 4] single date: {day}/{month_num}/{year}")
#             return [{"day": day, "month_num": month_num, "year": year}], "custom_date"

#         # PATTERN 6: "from MONTH" pattern (from month to end of current FY)
#         # Examples: "from may" (May 2025 to Mar 2026), "from june 2024" (Jun 2024 to Mar 2025)
#         # This pattern explicitly looks for the word "from" followed by a month
#         from_month_pattern = r'\bfrom\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
#         from_match = re.search(from_month_pattern, query_lower)
        
#         if from_match:
#             month_str = from_match.group(1)
#             month_num = self.MONTH_NAMES.get(month_str, 4)
#             year_explicit = int(from_match.group(2)) if from_match.group(2) else None
#             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
#             # End year: if starting month is Apr-Dec (2025), end is Mar 2026  
#             end_year = year + 1 if month_num >= 4 else year
            
#             print(f"[PATTERN 6 - FROM MONTH] Matched: '{month_str}' (month={month_num}), Start year: {year}, End year: {end_year}")
            
#             return [
#                 {"month_num": month_num, "year": year},
#                 {"month_num": 3, "year": end_year}
#             ], "custom_range"
        
#         # PATTERN 7: "till MONTH" or "until MONTH" pattern (from start of FY to end of that month)
#         till_month_pattern = r'(?:till|until)\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         till_month_match = re.search(till_month_pattern, query_lower)
        
#         if till_month_match:
#             month_str = till_month_match.group(1)
#             month_num = self.MONTH_NAMES.get(month_str, 12)
#             year_explicit = int(till_month_match.group(2)) if till_month_match.group(2) else None
#             year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
#             # Start year: if ending month is Jan-Mar, start is Apr of previous year
#             start_year = year if month_num >= 4 else year - 1
            
#             end_date = {"month_num": month_num, "year": year}
            
#             return [
#                 {"month_num": 4, "year": start_year},
#                 end_date
#             ], "custom_range"
        
#         # PATTERN 8: Month-to-month range without "to" (e.g., "april - june", "sep - november")
#         month_dash_month = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s*-\s*(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         month_dash_match = re.search(month_dash_month, query_lower)
        
#         if month_dash_match:
#             month1_str = month_dash_match.group(1)
#             month1_num = self.MONTH_NAMES.get(month1_str, 1)
#             year1_explicit = int(month_dash_match.group(2)) if month_dash_match.group(2) else None
            
#             month2_str = month_dash_match.group(3)
#             month2_num = self.MONTH_NAMES.get(month2_str, 12)
#             year2_explicit = int(month_dash_match.group(4)) if month_dash_match.group(4) else None
            
#             # Intelligent year assignment logic
#             if year1_explicit and year2_explicit:
#                 year1 = year1_explicit
#                 year2 = year2_explicit
#             elif year1_explicit and not year2_explicit:
#                 year1 = year1_explicit
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
#             elif year2_explicit and not year1_explicit:
#                 year2 = year2_explicit
#                 if month1_num <= month2_num:
#                     year1 = year2
#                 else:
#                     year1 = year2 - 1
#             else:
#                 year1 = get_fy_year_for_month(month1_num)
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
            
#             return [
#                 {"month_num": month1_num, "year": year1},
#                 {"month_num": month2_num, "year": year2}
#             ], "custom_range"
        
#         # PATTERN 9: Month-to-month with "to" (e.g., "april to june", "april to sep 2024")
#         month_to_month = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+(?:to)\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
#         month_to_match = re.search(month_to_month, query_lower)
        
#         if month_to_match:
#             month1_str = month_to_match.group(1)
#             month1_num = self.MONTH_NAMES.get(month1_str, 1)
#             year1_explicit = int(month_to_match.group(2)) if month_to_match.group(2) else None
            
#             month2_str = month_to_match.group(3)
#             month2_num = self.MONTH_NAMES.get(month2_str, 12)
#             year2_explicit = int(month_to_match.group(4)) if month_to_match.group(4) else None
            
#             # Intelligent year assignment logic
#             if year1_explicit and year2_explicit:
#                 year1 = year1_explicit
#                 year2 = year2_explicit
#             elif year1_explicit and not year2_explicit:
#                 year1 = year1_explicit
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
#             elif year2_explicit and not year1_explicit:
#                 year2 = year2_explicit
#                 if month1_num <= month2_num:
#                     year1 = year2
#                 else:
#                     year1 = year2 - 1
#             else:
#                 year1 = get_fy_year_for_month(month1_num)
#                 if month2_num < month1_num:
#                     year2 = year1 + 1
#                 else:
#                     year2 = year1
            
#             return [
#                 {"month_num": month1_num, "year": year1},
#                 {"month_num": month2_num, "year": year2}
#             ], "custom_range"
        
#         # PATTERN 10: Single month (e.g., "april 2024", "june")
#         single_month_pattern = r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
#         single_month_match = re.search(single_month_pattern, query_lower)
        
#         if single_month_match:
#             month_str = single_month_match.group(1)
#             month_num = self.MONTH_NAMES.get(month_str, 1)
#             year = int(single_month_match.group(2)) if single_month_match.group(2) else get_fy_year_for_month(month_num)
            
#             return [{"month_num": month_num, "year": year}], "custom_range"
        
#         return [], "current_financial_year"



#     # =====================================================
#     # PUBLIC ENTRY POINT
#     # =====================================================
    
           
#     def _detect_possession_metric(self, user_query: str) -> Optional[str]:
#         """
#         Detect if query is about possession status/journey.
    
#     Possession journey stages:
#     1. Sale Done (Document_Date exists)
#     2. Agreement Given (Agreement_Date exists)
#     3. Possession Given (Possession_Given_On exists)
    
#     Args:
#         user_query: User's natural language query
    
#     Returns:
#         Possession metric name or None
    
#     Examples:
#         "how many possessions are pending" → "possession_pending_count"
#         "possession given last month" → "possession_given_count"
#         "tower wise possession pending" → "possession_pending_count"
#         "possession status breakdown" → "possession_status_breakdown"
#         "average days to possession" → "average_days_to_possession"
#     """
#         query_lower = user_query.lower()
        
#         # ────────────────────────────────────────────────────────
#         # Check if this is a possession-related query
#         # ────────────────────────────────────────────────────────
#         possession_keywords = ["possession", "possessions", "handover", "handed over"]
#         agreement_keywords = ["agreement", "agreements"]
        
#         has_possession = any(kw in query_lower for kw in possession_keywords)
#         has_agreement = any(kw in query_lower for kw in agreement_keywords)
        
#         if not has_possession and not has_agreement:
#             return None
        
#         # ────────────────────────────────────────────────────────
#         # Possession-related metrics
#         # ────────────────────────────────────────────────────────
#         if has_possession:
#             # Priority 1: Status breakdown
#             if any(kw in query_lower for kw in ["status", "journey", "breakdown", "stages", "wise breakdown"]):
#                 return "possession_status_breakdown"
            
#             # Priority 2: Completion rate
#             if any(kw in query_lower for kw in ["rate", "%", "percentage", "completion rate"]):
#                 return "possession_completion_rate"
            
#             # Priority 3: TAT/Average days
#             if any(kw in query_lower for kw in ["average", "avg", "days", "time", "tat", "turnaround", "how long"]):
#                 if "agreement" in query_lower:
#                     return "average_days_to_agreement"
#                 elif "total" in query_lower or "cycle" in query_lower:
#                     return "average_total_cycle_time"
#                 else:
#                     return "average_days_to_possession"
            
#             # Priority 4: Value metrics
#             if any(kw in query_lower for kw in ["value", "revenue", "amount", "worth"]):
#                 if any(kw in query_lower for kw in ["pending", "not given", "yet to"]):
#                     return "possession_pending_value"
#                 else:
#                     return "possession_given_value"
            
#             # Priority 5: Pending possession
#             if any(kw in query_lower for kw in ["pending", "not given", "not handed", "yet to", "awaiting"]):
#                 return "possession_pending_count"
            
#             # Priority 6: Given possession
#             if any(kw in query_lower for kw in ["given", "handed over", "completed", "done", "delivered"]):
#                 return "possession_given_count"
            
#             # Default possession metric
#             return "possession_pending_count"
    
#         # ────────────────────────────────────────────────────────
#         # Agreement-related metrics
#         # ────────────────────────────────────────────────────────
#         if has_agreement:
#             # Completion rate
#             if any(kw in query_lower for kw in ["rate", "%", "percentage", "completion"]):
#                 return "agreement_completion_rate"
            
#             # TAT
#             if any(kw in query_lower for kw in ["average", "avg", "days", "time", "tat", "turnaround"]):
#                 return "average_days_to_agreement"
            
#             # Value
#             if any(kw in query_lower for kw in ["value", "revenue", "amount"]):
#                 if "pending" in query_lower:
#                     return "agreement_pending_value"
#                 else:
#                     return "agreement_given_count"
            
#             # Pending agreement
#             if any(kw in query_lower for kw in ["pending", "not created", "not prepared", "yet to"]):
#                 return "agreement_pending_count"
            
#             # Given agreement
#             if any(kw in query_lower for kw in ["given", "created", "prepared", "signed", "done"]):
#                 return "agreement_given_count"
            
#             # Default
#             return "agreement_pending_count"
        
#         return None


#     def _detect_transfer_query(self, user_query: str) -> Optional[str]:
#         """
#         Intelligently detect if query is about transfers.
        
#         Returns appropriate transfer metric or None.
#         """
#         query_lower = user_query.lower()
        
#         # Check if transfer keyword is present
#         transfer_indicators = ["transfer", "transferred", "transferring", "transfers"]
#         has_transfer = any(indicator in query_lower for indicator in transfer_indicators)
        
#         if not has_transfer:
#             return None
        
#         # Exclude non-metric transfer queries
#         exclusions = [
#             "transfer policy", "transfer process", "transfer procedure",
#             "how to transfer", "transfer ownership", "transfer documentation"
#         ]
#         if any(excl in query_lower for excl in exclusions):
#             return None
        
#         # Determine specific transfer metric type
        
#         # Priority 1: Transfer rate
#         if any(kw in query_lower for kw in ["rate", "percentage", "%", "percent"]):
#             return "transfer_rate"
        
#         # Priority 2: Transfer value
#         if any(kw in query_lower for kw in ["value", "revenue", "amount", "worth"]):
#             return "transferred_sales_value"
        
#         # Priority 3: Transfer recipients
#         if any(kw in query_lower for kw in ["received", "recipients", "final payer", "transferred to"]):
#             return "transfer_recipients"
        
#         # Priority 4: Non-transferred
#         if any(kw in query_lower for kw in ["non transferred", "without transfer", "normal", "regular"]):
#             return "non_transferred_sales"
        
#         # Priority 5: Transfer orders count
#         if any(kw in query_lower for kw in ["orders", "units count", "sales orders"]):
#             return "transferred_sales_count"
        
#         # Priority 6: Product-wise transfer
#         if any(kw in query_lower for kw in ["product", "eden", "sales group", "amore", "livork"]):
#             return "transfer_product_wise"
        
#         # Default: Customer count
#         return "transferred_sales"

#     def extract_intent(self, user_query: str) -> SemanticIntent:
#         """
#         Extract semantic intent from user query.
        
#         WORKFLOW:
#         1. Call WatsonX LLM for metric, dimensions, date_range
#         2. Use keyword fallbacks for dimension inference
#         3. Use _extract_filter_values() for precise filter extraction (NOT LLM)
#         """
#         # 1. Build prompt
#         prompt = self._build_prompt(user_query)
        
#         print(f"[DEBUG] User Query: {user_query}")
#         print(f"[DEBUG] User Query: {user_query}")
#         raw_text = self.model.generate_text(prompt)
#         print(f"[DEBUG] Watsonx Response: {raw_text}")
        
#         # 3. Parse JSON
#         intent_dict = self._parse_json(raw_text)
        
#         # 4. Normalize keys (daterange -> date_range, etc.)
#         intent_dict = self._normalize_intent_keys(intent_dict)
#         print(f"[DEBUG] Parsed Intent (from LLM): {intent_dict}")
        


#         # query_lower = user_query.lower()
#         # status_breakdown = self._detect_status_breakdown_query(query_lower)
#         # if status_breakdown:
#         #     dimension, metric = status_breakdown
#         #     print(f"[STATUS BREAKDOWN] Detected: {dimension}")
#         #     return SemanticIntent(
#         #         metric=metric,
#         #         dimensions=[dimension],
#         #         date_range="current_financial_year",
#         #         filters={},
#         #         original_query=user_query
#         #     )
#         # FIX #3: Check for status breakdown queries FIRST
#         query_lower = user_query.lower()
#         status_dim = self._detect_status_breakdown_query(query_lower)
#         if status_dim:
#             print(f"[STATUS BREAKDOWN] Using dimension: {status_dim}")
#             return SemanticIntent(
#                 metric="total_sales",
#                 dimensions=[status_dim],
#                 date_range="current_financial_year",
#                 filters={},
#                 original_query=user_query
#             )
        


#         cancellation_intent = self._detect_cancellation_query(user_query)
#         if cancellation_intent:
#             return cancellation_intent


#         # ============================================================
#         # KEYWORD FALLBACKS - Step 1: Date Range
#         # ============================================================
#         if not intent_dict.get("date_range") or intent_dict["date_range"] == "current_financial_year":
#             detected_date = self._detect_date_range_keywords(user_query)
#             if detected_date:
#                 intent_dict["date_range"] = detected_date
#                 print(f"[FALLBACK] Fixed date_range: {detected_date}")

#         # ============================================================
#         # TREND QUERY OVERRIDES (MoM, QoQ, YoY)
#         # ============================================================
#         query_lower = user_query.lower()
#         current_dr = intent_dict.get("date_range", "")
        
#         # 1. Year on Year (YoY) or Year Wise — always use last_3_financial_years
#         #    Force override regardless of what the LLM or keyword detector returned,
#         #    because these are always multi-year trend queries.
#         yoy_triggers = ["year on year", "yoy", "year-on-year", "y-o-y",
#                         "year wise", "yearwise", "year-wise", "yearly", "all years",
#                         "year by year", "each year", "per year", "annual trend"]
#         if any(t in query_lower for t in yoy_triggers):
#             intent_dict["date_range"] = "last_3_financial_years"
#             if not intent_dict.get("time_grain"):
#                 intent_dict["time_grain"] = "year"
#             print(f"[OVERRIDE] YoY/year-wise detected -> last_3_financial_years")

#         # 2. Quarter on Quarter (QoQ) -> Always use current FY with quarter grain
#         elif "quarter on quarter" in query_lower or "qoq" in query_lower or "quarter-on-quarter" in query_lower:
#             intent_dict["date_range"] = "current_financial_year"
#             if not intent_dict.get("time_grain"):
#                 intent_dict["time_grain"] = "quarter"
#             print(f"[OVERRIDE] QoQ detected -> current_financial_year + time_grain=quarter")

#         # 3. Month on Month (MoM) -> Always use current FY with month grain
#         elif "month on month" in query_lower or "mom" in query_lower or "month-on-month" in query_lower:
#             intent_dict["date_range"] = "current_financial_year"
#             if not intent_dict.get("time_grain"):
#                 intent_dict["time_grain"] = "month"
#             print(f"[OVERRIDE] MoM detected -> current_financial_year + time_grain=month")
        
#         # ============================================================
#         # KEYWORD FALLBACKS - Step 2: Dimension Inference
#         # ============================================================
#         if not intent_dict.get("dimensions"):
#             inferred_dims = self._infer_dimensions(user_query)
#             if inferred_dims:
#                 intent_dict["dimensions"] = inferred_dims
#                 print(f"[FALLBACK] Inferred dimensions: {inferred_dims}")
        
#         # Check for bifurcation queries
#         if not intent_dict.get("dimensions"):
#             bifurcation_dim = self._detect_bifurcation_query(user_query)
#             if bifurcation_dim:
#                 intent_dict["dimensions"] = [bifurcation_dim]
#                 print(f"[FALLBACK] Bifurcation dimension: {bifurcation_dim}")
        
#         # ============================================================
#         # KEYWORD FALLBACKS - Step 3: Transfer Detection
#         # ============================================================
#         if "transfer" in user_query.lower():
#             detected_transfer = self._detect_transfer_query(user_query)
#             if detected_transfer:
#                 intent_dict["metric"] = detected_transfer
#                 print(f"[TRANSFER FALLBACK] Detected transfer metric: {detected_transfer}")
        

#         # ============================================================
#         # MULTI-METRIC DETECTION: "count and value", "sales count and amount", etc.
#         # When user asks for both count and value, set intent.metrics list so
#         # sql_builder emits both aggregate columns in a single query.
#         # ============================================================
#         multi_metrics = self._detect_multi_metric(user_query)
#         if multi_metrics:
#             intent_dict["metrics"] = multi_metrics
#             # Primary metric drives filter resolution; set to first in list
#             intent_dict["metric"] = multi_metrics[0]
#             print(f"[MULTI-METRIC] Detected: {multi_metrics}")

#         # ============================================================
#         # HAVING FILTER DETECTION (greater than / less than on metric value)
#         # e.g. "sales amount greater than 50000", "value more than 1 lakh"
#         # ============================================================
#         having_filter = self._detect_having_filter(user_query, intent_dict.get("metric", "total_sales"))
#         if having_filter:
#             intent_dict["having_filter"] = having_filter
#             print(f"[HAVING] Detected having filter: {having_filter}")

#         # ============================================================
#         # POSSESSION JOURNEY DETECTION
#         # ============================================================
#         if "possession" in user_query.lower() or "agreement" in user_query.lower():
#             detected_possession = self._detect_possession_metric(user_query)
#             if detected_possession:
#                 intent_dict["metric"] = detected_possession
#                 print(f"[POSSESSION] Detected metric: {detected_possession}")
#         # ============================================================
#         # CRITICAL FIX: OVERRIDE LLM FILTERS WITH KEYWORD EXTRACTION
#         # ============================================================
#         # ALWAYS use keyword-based filter extraction (ignore LLM filters)
#         dimensions = intent_dict.get("dimensions", [])
#         keyword_filters = self._extract_filter_values(user_query, dimensions)
        
#         if keyword_filters:
#             # Replace LLM filters with keyword-extracted filters
#             intent_dict["filters"] = keyword_filters
#             print(f"[OVERRIDE] Keyword-extracted filters: {keyword_filters}")
#         else:
#             # If keyword extraction fails, clear invalid LLM filters
#             if "filters" in intent_dict:
#                 print(f"[WARNING] Clearing invalid LLM filters: {intent_dict['filters']}")
#                 intent_dict["filters"] = None
        
#         # ============================================================
#         # METRIC NORMALIZATION
#         # ============================================================
#         intent_dict["metric"] = self._normalize_metric(
#             intent_dict.get("metric", "total_sales"), 
#             user_query
#         )
        
#         # ============================================================
#         # TIME GRAIN DETECTION
#         # ============================================================
#         if not intent_dict.get("time_grain"):
#             detected_grain = self._detect_time_grain(user_query)
#             if detected_grain:
#                 intent_dict["time_grain"] = detected_grain
#                 print(f"[FALLBACK] Time grain: {detected_grain}")
        
#         # ============================================================
#         # COMPARISON TYPE DETECTION (mom, qoq, yoy, wow)
#         # ============================================================
#         if not intent_dict.get("compare_to"):
#             detected_compare = self._detect_comparison_type(user_query)
#             if detected_compare:
#                 intent_dict["compare_to"] = detected_compare
#                 intent_dict["is_trend"] = True
#                 print(f"[FALLBACK] Comparison: {detected_compare}")
                
#                 # Auto-detect time_grain if not already set
#                 if not intent_dict.get("time_grain"):
#                     intent_dict["time_grain"] = self._suggest_time_grain_for_comparison(detected_compare)
        
#         # ============================================================
#         # CUSTOM DATE EXTRACTION
#         # ============================================================
#         # Try enhanced date extraction first
#         custom_dates_enhanced, date_range_type = self._extract_custom_dates_enhanced(user_query)
#         if custom_dates_enhanced:
#             custom_dates = custom_dates_enhanced
#             intent_dict["date_range"] = date_range_type
#             intent_dict["custom_dates"] = custom_dates
#             print(f"[DATE EXTRACTION] Enhanced dates found: {custom_dates}, type: {date_range_type}")
#         else:
#             # Fallback to original method
#             custom_dates = self._extract_custom_dates(user_query)
#             if custom_dates:
#                 intent_dict["custom_dates"] = custom_dates
#                 print(f"[DATE EXTRACTION] Fallback dates found: {custom_dates}")
        
#         # ============================================================
#         # CLEANUP: STRIP INVALID DIMENSIONS (time grains + unknown LLM hallucinations)
#         # ============================================================
#         # Full set of valid dimension keys from dimensions.yaml registry
#         REGISTRY_DIMENSIONS = {
#             'agreement_given_to_customer', 'agreement_prepared_on', 'agreement_status',
#             'allotment_letter_date', 'back_office_executive', 'back_office_executive_name',
#             'bank_branch', 'bank_branch1', 'billing_block', 'billing_block_changed_on',
#             'billing_block_description', 'billing_plan', 'booking_created_by',
#             'booking_type', 'broker', 'broker_name', 'cancellation_reason',
#             'charge_type', 'co_applicant1', 'co_applicant1_name', 'co_applicant2',
#             'co_applicant2_name', 'co_applicant3', 'co_applicant3_name', 'co_applicant4',
#             'co_applicant4_name', 'co_applicant5', 'co_applicant5_name',
#             'completion_date', 'consortium', 'consortium_name', 'customer_type',
#             'description', 'disbursed_date', 'dist_channel_desc', 'distribution_channel',
#             'division', 'division_desc', 'document_handed_over_to_cust_or_bank',
#             'document_type', 'floor', 'given_to_accounts', 'given_to_dept_head',
#             'given_to_dept_manager', 'given_to_finance_director', 'handed_over_to_customer',
#             'inventory_code', 'inventory_text', 'loan_bank', 'material', 'material_group',
#             'material_pricing_group', 'material_pricing_group_desc', 'noc_date',
#             'old_billing_block', 'old_billing_block_description', 'old_booking_no',
#             'payer', 'payer_name', 'plc_material', 'plc_material_desc', 'po_number',
#             'possession_given_on', 'possession_status', 'product', 'product_desc',
#             'project', 'reason', 'reason_for_rejection', 'refferal', 'registry_date',
#             'rejection_date', 'sale_organization', 'sales_executive', 'sales_executive_name',
#             'sales_group', 'sales_group_desc', 'sales_office', 'sales_office_desc',
#             'sales_order', 'sales_org', 'sales_org_desc', 'sanction_letter_provided_by_bank_on',
#             'scheme_code', 'sector', 'signed_agreement_received_on', 'signed_by_accounts',
#             'signed_by_dept_head', 'signed_by_dept_manager', 'sold_to', 'sold_to_name',
#             'sub_broker', 'sub_broker_name', 'tax_class_1', 'tax_class_2', 'tax_class_3',
#             'tax_class_4', 'tax_class_5', 'tax_class_6', 'tax_class_7', 'total_transfer',
#             'tower', 'tower_desc', 'tpt_and_ptm_given_for_sign', 'tpt_ptm_sign_date',
#             'tpt_received_date', 'type', 'type_desc', 'uom',
#         }

#         if intent_dict.get("dimensions"):
#             dims = intent_dict["dimensions"]
#             time_grains = set(self.TIME_GRAIN_KEYWORDS.keys())

#             valid_dims = []
#             for dim in dims:
#                 if dim in time_grains:
#                     print(f"[CORRECTION] Removed '{dim}' from dimensions (time grain)")
#                     if not intent_dict.get("time_grain"):
#                         intent_dict["time_grain"] = dim
#                 elif dim not in REGISTRY_DIMENSIONS:
#                     print(f"[CORRECTION] Removed invalid dimension '{dim}' (not in registry)")
#                 else:
#                     valid_dims.append(dim)

#             intent_dict["dimensions"] = valid_dims

#         # ============================================================
#         # RETURN CANONICAL INTENT
#         # ============================================================
#         print(f"[FINAL] Intent: {intent_dict}")
#         return SemanticIntent(
#             **intent_dict,
#             original_query=user_query
#         )



#     # =====================================================
#     # MULTI-METRIC DETECTION
#     # =====================================================

#     def _detect_multi_metric(self, user_query: str) -> Optional[List[str]]:
#         """
#         Detect queries asking for both count and value in one response.

#         Examples:
#             "total sales count and value of eden"        -> ["total_sales", "sales_value"]
#             "sales count and amount of tower 7"          -> ["total_sales", "sales_value"]
#             "show count and value of wave city"          -> ["total_sales", "sales_value"]
#             "count and sales value of 1bhk"              -> ["total_sales", "sales_value"]
#             "total sales and amount of broker"           -> ["total_sales", "sales_value"]
#         """
#         q = user_query.lower()

#         count_kws  = ["sales count", "count of sales", "number of sales", "total sales count",
#                       "bookings count", "total bookings", "count and value", "count and amount",
#                       "count and sales"]
#         value_kws  = ["sales value", "sales amount", "amount", "value", "revenue"]

#         has_count = any(kw in q for kw in count_kws)
#         has_value = any(kw in q for kw in value_kws)

#         # Explicit "count and value / count and amount" pattern — clearest signal
#         explicit_patterns = [
#             "count and value", "count and amount", "count and sales value",
#             "count and sales amount", "sales count and value", "sales count and amount",
#             "total count and value", "total count and amount", "sales (count and value)",
#             "number and value", "number and amount", "total sales amount and count", "sales value and count"
#         ]
#         if any(p in q for p in explicit_patterns):
#             return ["total_sales", "sales_value"]

#         # "total sales count and value of X" / "sales and amount" combos
#         if has_count and has_value:
#             return ["total_sales", "sales_value"]

#         return None

#     # =====================================================
#     # HAVING FILTER DETECTION
#     # =====================================================

#     def _detect_having_filter(self, user_query: str, metric: str) -> Optional[str]:
#         """
#         Detect aggregate filter conditions from natural language.

#         Examples:
#             "sales amount greater than 50000"   -> "SUM(\"net_value\") > 50000"
#             "value more than 1 lakh"            -> "SUM(\"net_value\") > 100000"
#             "sales less than 10000"             -> "SUM(\"net_value\") < 10000"
#             "count greater than 5"              -> "COUNT(DISTINCT Sales_Order) > 5"
#             "amount at least 50000"             -> "SUM(\"net_value\") >= 50000"
#         """
#         query_lower = user_query.lower()

#         # Metric expression to use in HAVING
#         value_metrics = {"sales_value", "transferred_sales_value", "possession_pending_value",
#                          "possession_given_value", "agreement_pending_value", "net_value",
#                          "amount_received", "amount_demanded"}
#         count_metrics = {"total_sales", "transferred_sales", "transferred_sales_count",
#                          "possession_pending_count", "possession_given_count",
#                          "cancelled_sales", "cancelled_units"}

#         if metric in value_metrics:
#             agg_expr = 'SUM("net_value")'
#         elif metric in count_metrics:
#             agg_expr = "COUNT(DISTINCT Sales_Order)"
#         else:
#             agg_expr = 'SUM("net_value")'  # default to value

#         # Operator patterns
#         gt_keywords = ["greater than", "more than", "above", "over", "exceeds", "atleast", "at least", "minimum"]
#         lt_keywords = ["less than", "below", "under", "maximum", "at most", "atmost"]
#         gte_keywords = ["greater than or equal", "at least", "atleast", "minimum", ">="]
#         lte_keywords = ["less than or equal", "at most", "atmost", "maximum", "<="]

#         # Parse numeric value — support lakhs and crores
#         def parse_amount(text):
#             # Match: "50000", "50,000", "50 lakh", "1.5 crore", "1 lakh"
#             lakh_match = re.search(r'(\d+(?:\.\d+)?)\s*lakh', text)
#             crore_match = re.search(r'(\d+(?:\.\d+)?)\s*crore', text)
#             num_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)', text)
#             if crore_match:
#                 return int(float(crore_match.group(1)) * 10_000_000)
#             if lakh_match:
#                 return int(float(lakh_match.group(1)) * 100_000)
#             if num_match:
#                 return int(num_match.group(1).replace(",", ""))
#             return None

#         for kw in gte_keywords:
#             if kw in query_lower:
#                 val = parse_amount(query_lower.split(kw)[-1])
#                 if val is not None:
#                     return f"{agg_expr} >= {val}"

#         for kw in lte_keywords:
#             if kw in query_lower:
#                 val = parse_amount(query_lower.split(kw)[-1])
#                 if val is not None:
#                     return f"{agg_expr} <= {val}"

#         for kw in gt_keywords:
#             if kw in query_lower:
#                 val = parse_amount(query_lower.split(kw)[-1])
#                 if val is not None:
#                     return f"{agg_expr} > {val}"

#         for kw in lt_keywords:
#             if kw in query_lower:
#                 val = parse_amount(query_lower.split(kw)[-1])
#                 if val is not None:
#                     return f"{agg_expr} < {val}"

#         return None

#     # =====================================================
#     # DIMENSION INFERENCE
#     # =====================================================
    
#     def _infer_dimensions(self, user_query: str) -> List[str]:
#         """
#         Infer dimensions from keywords in user query.
#         Uses pattern matching with the DIMENSION_KEYWORDS map.
#         """
#         query_lower = user_query.lower()
#         detected = set()
        
#         for dim, keywords in self.DIMENSION_KEYWORDS.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     detected.add(dim)
#                     break  # Found this dimension, move to next
        
#         return list(detected)

#     def _detect_bifurcation_query(self, user_query: str) -> Optional[str]:
#         """
#         Detect queries that ask for a breakdown/split WITHOUT mentioning dimensions.
        
#         Examples:
#         - "Booking type wise sales" -> booking_type
#         - "Sales by broker" -> broker
#         - "Channel split" -> channel
#         - "Tower wise sales" -> tower
#         """
#         query_lower = user_query.lower()
        
#         bifurcation_patterns = {
#             "booking_type": ["booking type wise", "booking type bifurcation", "booking split"],
#             "broker_name": ["broker wise", "broker split", "sales by broker"],
#             "cancellation_reason": ["cancellation reason wise", "reason wise cancellation", "cancellation reason breakdown","reason"],
#             "dist_channel_desc": ["channel wise", "channel split", "sales by channel"],
#             "sales_group_desc": ["product wise", "product split", "sales by product"],
#             "Sales_Org_Desc": ["project wise", "project split", "sales by project"],
#             "tower": ["tower wise", "tower split", "sales by tower"],
#             "division_desc": ["division wise", "division split"],
#             "customer_type": ["booking status wise", "customer type wise", "status wise", "booked vs cancelled"],
#             "sales_executive_name": ["executive wise", "salesman wise"],
#             "floor_desc": ["floor wise", "floor split"],
#             "sector": ["sector wise", "sector split"],
#             "type_desc": ["unit type wise", "type wise", "product wise"],
#             "sales_group_desc": ["sales group wise", "sales group split", "group wise"],
#             "sales_office_desc": ["sales office wise", "office wise", "sales office split"],
#             "billing_plan": ["billing plan wise", "payment plan wise", "plan wise"],
#             "sales_org_desc": ["organization wise", "org wise", "sales org wise"],
#             "loan_bank": ["bank wise", "bank split", "lender wise"],
#             "billing_block_description": ["billing block wise", "block reason wise"],
#             "material_pricing_group_desc": ["pricing group wise", "material group wise"],
#             "scheme_code": ["scheme wise", "scheme split"],
#             "refferal": ["referral wise", "referral split"],
#             "reason_for_rejection": ["rejection reason wise"],
#         }
        
#         for dim, patterns in bifurcation_patterns.items():
#             for pattern in patterns:
#                 if pattern in query_lower:
#                     return dim
        
#         return None

#     # =====================================================
#     # FILTER VALUE EXTRACTION
#     # =====================================================
    
#     def _extract_filter_values(self, user_query: str, dimensions: List[str]) -> Optional[Dict[str, Any]]:
#         """
#         INTELLIGENT FILTER EXTRACTOR - AUTO-DETECTS FILTERS FROM ALL 121 COLUMNS
        
#         Extracts specific filter values from natural language queries for:
#         - Projects (50 types)
#         - Channels (Broker/Direct/Referral)
#         - Divisions (Residential/Commercial/FSI/Institutional)
#         - Sales Org (Wave City/Wave Estate/WMCC)
#         - Towers (111 types)
#         - Floors (51 types)
#         - Unit Types (61 types)
#         - Loan Banks (55 banks)
#         - Sales Executives (154 people)
#         - Brokers (774 brokers)
#         - Material Pricing Groups (9 categories)
#         - Booking Types (6 types)
#         - Customer Types (Booked/Cancelled)
#         - And 100+ other dimensions
#         """
#         filters = {}
#         query_lower = user_query.lower()
        
#         # FIX #2: Try person name extraction FIRST
#         for dim_name in ["payer_name", "sold_to_name", "sales_executive_name", "broker_name"]:
#             person_name = self._extract_person_name_filter(query_lower, dim_name)
#             if person_name:
#                 filters[dim_name] = person_name
#                 print(f"[FILTER] Extracted {dim_name}: {person_name}")
        
#         # FIX #4 & #5: Try multi-value extraction
#         for dim_name in ["floor_desc", "tower", "sales_group_desc"]:
#             multi_values = self._extract_multi_values_with_and(query_lower, dim_name)
#             if multi_values:
#                 filters[dim_name] = multi_values
#                 print(f"[FILTER] Extracted {dim_name}: {multi_values}")


#         def extract_multi(patterns: Dict[str, List[str]], dim_key: str):
#             found_values = set()
#             for value, keywords in patterns.items():
#                 for keyword in keywords:
#                     if keyword in query_lower:
#                         # Special check for "sub" to avoid false positives (e.g. "broker" in "sub broker")
#                         if dim_key == "dist_channel_desc" and "sub" in query_lower and "sub" not in keyword:
#                             continue
                            
#                         # IGNORE "WISE" suffix (e.g. "tower wise" should not match "wise")
#                         if keyword == "wise":
#                             continue
                            
#                         found_values.add(value)
#                         break
            
#             if found_values:
#                 # If multiple values found, return list. If single, return string.
#                 # BUT for "broker and direct", we want list.
#                 # The downstream SQL builder handles lists.
#                 final_val = list(found_values)
#                 if len(final_val) == 1:
#                     filters[dim_key] = final_val[0]
#                 else:
#                     filters[dim_key] = final_val

#         # ==========================================
#         # 1. DISTRIBUTION CHANNEL (Dist_Channel_Desc)
#         # ==========================================
#         channel_patterns = {
#             "Broker": ["broker", "agent", "channel broker", "brokerage"],
#             "Direct": ["direct", "direct sales", "walk-in", "walk in"],
#             "Referral": ["referral", "referred", "reference"]
#         }
#         # Skip channel filter when the query is asking for broker-name-wise breakdown
#         # e.g. "broker name wise total sales" — "broker" here is a dimension, not a channel filter
#         if not any(kw in query_lower for kw in ["broker name", "broker wise", "broker name wise"]):
#             extract_multi(channel_patterns, "dist_channel_desc")
        
#         # ==========================================
#         # 2. PROJECT (Project_Desc) - 50 Projects
#         # ==========================================
#         project_patterns = {
#             "AMORE": ["amore"],
#             "LIVORK": ["livork"],
#             "DREAM HOMES": ["dream homes", "dream home"],
#             "DREAM BAZAAR": ["dream bazaar"],
#             "EXECUTIVE FLOORS": ["executive floors", "executive floor"],
#             "WAVE FLOOR": ["wave floor"],
#             "NEW PLOTS": ["new plots", "new plot"],
#             "OLD PLOTS": ["old plots", "old plot"],
#             "VERIDIA": ["veridia"],
#             "VERIDIA-3": ["veridia 3", "veridia-3"],
#             "VERIDIA-4": ["veridia 4", "veridia-4"],
#             "VERIDIA-5": ["veridia 5", "veridia-5"],
#             "VERIDIA-6": ["veridia 6", "veridia-6"],
#             "VERIDIA-7": ["veridia 7", "veridia-7"],
#             "EDEN": ["eden"],
#             "EDENIA": ["edenia"],
#             "ELEGANTIA": ["elegantia"],
#             "ELIGO": ["eligo"],
#             "EMINENCE": ["eminence"],
#             "IRENIA": ["irenia"],
#             "TRUCIA": ["trucia"],
#             "VASILIA": ["vasilia"],
#             "MAYFAIR PARK": ["mayfair", "mayfair park"],
#             "HARMONY GREENS": ["harmony greens", "harmony green"],
#             "WAVE GALLERIA": ["galleria", "wave galleria"],
#             "WAVE GARDEN": ["wave garden"],
#             "WAVE ESTATE, GH2 PH2": ["wave estate gh2", "gh2 ph2"],
#             "WAVE BUSSINESS SQUARE": ["business square", "bussiness square"],
#             "WBT 1": ["wbt 1", "wbt1"],
#             "WBT A": ["wbt a", "wbta"],
#             "PRIME FLOORS": ["prime floors", "prime floor"],
#             "VILLAS": ["villas"],
#             "ARMONIA VILLA": ["armonia villa", "armonia"],
#             "COMM BOOTH": ["comm booth", "commercial booth"],
#             "COMMERCIAL PLOTS": ["commercial plots"],
#             "PLOTS-COMM": ["plots comm"],
#             "PLOTS-RES": ["plots res", "residential plots"],
#             "PLOTS-RES-IF": ["plots res if"],
#             "SCO": ["sco"],
#             "METRO MART": ["metro mart"],
#             "SWAMANORATH": ["swamanorath"],
#             "FSI": ["fsi project", "fsi"],
#             "INSTITUTIONAL": ["institutional project"],
#             "EWS_001_(410)": ["ews 410", "ews_001"],
#             "EWS_P2": ["ews p2"],
#             "LIG_001_(310)": ["lig 310", "lig_001"],
#             "LIG_P2": ["lig p2"],
#             "HSSC": ["hssc"],
#             "WAVE FLOOR 85": ["wave floor 85"],
#             "WAVE FLOOR 99": ["wave floor 99"]
#         }
#         extract_multi(project_patterns, "sales_group_desc")
        
#         # ==========================================
#         # 3. DIVISION (Division_Desc)
#         # ==========================================
#         # division_patterns = {
#         #     "Residential": ["residential"],
#         #     "Commercial": ["commercial"],
#         #     "Institutional": ["institutional"],
#         #     "FSI": ["fsi division"]
#         # }
#         # extract_multi(division_patterns, "division_desc")

#         # ==========================================
#         # 3. DIVISION (Division_Desc)
#         # ==========================================
#         # NOTE: Only match if "division" keyword is present
#         # Otherwise, "residential/commercial" will match both division AND sector
#         # if "division" in query_lower:
#         if any(keyword in query_lower for keyword in ["division", "residential", "commercial", "institutional"]):
#             division_patterns = {
#                 "Residential": ["residential"],
#                 "Commercial": ["commercial"],
#                 "Institutional": ["institutional"],
#                 "FSI": ["fsi division"]
#             }
#             extract_multi(division_patterns, "division_desc")
        
#         # ==========================================
#         # 4. SALES ORGANIZATION (Sales_Org_Desc)
#         # ==========================================
#         if any(keyword in query_lower for keyword in ["project", "wave city", "wave estate", "wmcc"]):
#             sales_org_patterns = {
#                 "Wave City": ["wave city"],
#                 "Wave Estate": ["wave estate"],
#                 "WMCC Sec 32": ["wmcc", "sector 32 org", "sec 32 sales"]
#             }
#             extract_multi(sales_org_patterns, "sales_org_desc")
        
#         # ==========================================
#         # 5. SALES OFFICE (Sales_Office_Desc)
#         # ==========================================
#         sales_office_patterns = {
#             "Wave City": ["wave city office"],
#             "Wave Estate": ["wave estate office"],
#             "WMCC Sec 32": ["wmcc office", "sector 32 office"]
#         }
#         extract_multi(sales_office_patterns, "sales_office_desc")
        
#         # ==========================================
#         # 6. TOWER DETECTION (Tower / Tower_Desc)
#         # ==========================================
#         # if "tower" in query_lower or "block" in query_lower:
#         #     # Pattern: "tower 7", "tower A", "block 032", "tower 3JA"
#         #     # BUT exclude "tower wise", "tower split" (grouping keywords)
#         #     match = re.search(r'(?:tower|block)\s+([a-zA-Z0-9]+)', query_lower)
#         #     if match:
#         #         tower_val = match.group(1).upper()
#         #         # Ignore if it's a grouping keyword
#         #         if tower_val.lower() not in ['wise', 'split', 'breakdown', 'group']:
#         #             # If numeric, pad with zeros (e.g., "7" -> "007")
#         #             if tower_val.isdigit():
#         #                 filters["tower"] = tower_val.zfill(3)
#         #             else:
#         #                 filters["tower_desc"] = tower_val

#         # ==========================================
#         # 6. TOWER DETECTION (Tower / Tower_Desc) - MULTI-VALUE SUPPORT
#         # ==========================================
#         if "tower" in query_lower or "block" in query_lower:
#             tower_values = []
            
#             # Pattern 1: "tower X and tower Y" (explicit tower for each)
#             pattern1 = r'(?:tower|block)\s+([a-zA-Z0-9]+)(?:\s+and\s+(?:tower|block)\s+([a-zA-Z0-9]+))?'
#             matches = re.findall(pattern1, query_lower)
            
#             for match in matches:
#                 for val in match:
#                     if val and val.lower() not in ['wise', 'split', 'breakdown', 'group', 'and']:
#                         tower_values.append(val.upper())
            
#             # Pattern 2: "tower X and Y" (without repeating "tower")
#             pattern2 = r'(?:tower|block)\s+([a-zA-Z0-9]+)\s+and\s+([a-zA-Z0-9]+)(?:\s+(?:tower|block))?'
#             matches2 = re.findall(pattern2, query_lower)
            
#             for match in matches2:
#                 for val in match:
#                     if val and val.lower() not in ['wise', 'split', 'breakdown', 'group', 'and', 'tower', 'block']:
#                         tower_values.append(val.upper())
            
#             # Remove duplicates
#             tower_values = list(set(tower_values))
            
#             if tower_values:
#                 # If multiple towers, store as list
#                 if len(tower_values) > 1:
#                     filters["tower"] = tower_values
#                 else:
#                     # Single tower - pad if numeric
#                     if tower_values[0].isdigit():
#                         filters["tower"] = tower_values[0].zfill(3)
#                     else:
#                         filters["tower"] = tower_values[0]
        
#         # ==========================================
#         # 7. FLOOR DETECTION (Floor_Desc)
#         # ==========================================
#         # if "floor" in query_lower:
#         #     floor_patterns = [
#         #         (r'ground\s+floor', "Ground Floor"),
#         #         (r'lower\s+ground\s+floor', "Lower Ground Floor"),
#         #         (r'upper\s+ground\s+floor', "Upper Ground Floor"),
#         #         (r'basement\s+(\d+)', "Basement {0}"),
#         #         (r'podium\s+(\d+)', "Podium {0}"),
#         #         (r'(\d+)(?:st|nd|rd|th)\s+floor', "{0}th Floor"),
#         #         (r'floor\s+(\d+)', "{0}th Floor"),
#         #         (r'g\+(\d+)', "G+{0}")
#         #     ]
            
#         #     for pattern, floor_format in floor_patterns:
#         #         match = re.search(pattern, query_lower)
#         #         if match:
#         #             if "{0}" in floor_format:
#         #                 floor_num = match.group(1)
#         #                 if floor_num == "1":
#         #                     floor_format = floor_format.replace("{0}th", "1st")
#         #                 elif floor_num == "2":
#         #                     floor_format = floor_format.replace("{0}th", "2nd")
#         #                 elif floor_num == "3":
#         #                     floor_format = floor_format.replace("{0}th", "3rd")
#         #                 filters["floor_desc"] = floor_format.format(floor_num)
#         #             else:
#         #                 filters["floor_desc"] = floor_format
#         #             break


#         # ==========================================
#         # 7. FLOOR DETECTION (Floor_Desc) - MULTI-VALUE SUPPORT
#         # ==========================================
#         if "floor" in query_lower:
#             floor_values = []
            
#             # Pattern for multiple floors: "15th and 16th floor", "15 and 16 floor"
#             multi_floor_pattern = r'(\d+)(?:st|nd|rd|th)?\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor'
#             multi_match = re.search(multi_floor_pattern, query_lower)
            
#             if multi_match:
#                 # Extract both floor numbers
#                 floor1 = multi_match.group(1)
#                 floor2 = multi_match.group(2)
#                 floor_values.extend([floor1, floor2])
#             else:
#                 # Single floor patterns
#                 floor_patterns = [
#                     (r'ground\s+floor', "Ground Floor"),
#                     (r'lower\s+ground\s+floor', "Lower Ground Floor"),
#                     (r'upper\s+ground\s+floor', "Upper Ground Floor"),
#                     (r'basement\s+(\d+)', "Basement {0}"),
#                     (r'podium\s+(\d+)', "Podium {0}"),
#                     (r'(\d+)(?:st|nd|rd|th)\s+floor', "{0}"),
#                     (r'floor\s+(\d+)', "{0}"),
#                     (r'g\+(\d+)', "G+{0}")
#                 ]
                
#                 for pattern, floor_format in floor_patterns:
#                     match = re.search(pattern, query_lower)
#                     if match:
#                         if "{0}" in floor_format:
#                             floor_num = match.group(1)
#                             floor_values.append(floor_num)
#                         else:
#                             # Special floors like "Ground Floor"
#                             filters["floor"] = floor_format
#                             floor_values = []  # Clear list since we set filters directly
#                         break
            
#             # If we have numeric floor values, store them
#             if floor_values:
#                 # Remove duplicates
#                 floor_values = list(set(floor_values))
                
#                 if len(floor_values) > 1:
#                     # Multiple floors - store as list
#                     filters["floor"] = floor_values
#                 else:
#                     # Single floor
#                     filters["floor"] = floor_values[0]
        
#         # ==========================================
#         # 8. UNIT TYPE (Type_Desc) - 61 Types
#         # ==========================================
#         unit_type_patterns = {
#             "1 BHK": ["1 bhk", "1bhk", "one bhk"],
#             "2 BHK": ["2 bhk", "2bhk", "two bhk"],
#             "3 BHK": ["3 bhk", "3bhk", "three bhk"],
#             "4 BHK": ["4 bhk", "4bhk", "four bhk"],
#             "2 BHK with Study": ["2 bhk with study", "2 bhk study"],
#             "3 BHK with Study": ["3 bhk with study", "3 bhk study"],
#             "4 BHK with Study": ["4 bhk with study", "4 bhk study"],
#             "3 BHK with Servent Qtr": ["3 bhk with servant", "3 bhk servant"],
#             "4 BHK with Servent Qtr.": ["4 bhk with servant", "4 bhk servant"],
#             "Shop": ["shop", "shops"],
#             "Office": ["office", "offices"],
#             "Villa": ["villa", "villas"],
#             "Duplex": ["duplex"],
#             "Penthouse": ["penthouse"],
#             "Sky Villa": ["sky villa"],
#             # "Plot": ["plot", "plots"],
#             "Commercial": ["commercial unit"],
#             "Food Court": ["food court"],
#             "Commercial Booth": ["commercial booth"]
#         }
#         extract_multi(unit_type_patterns, "type_desc")
        
#         # ==========================================
#         # 9. MATERIAL PRICING GROUP (Material_Pricing_Group_Desc)
#         # ==========================================
#         pricing_group_patterns = {
#             "Apartment": ["apartment pricing", "apartment group", "apartment"],
#             "Independent Floor": ["independent floor", "independent floors"],
#             "Office": ["office pricing"],
#             "Shops": ["shops pricing"],
#             "Plots": ["plots pricing","plots","plot"],
#             "Villas": ["villas pricing"],
#             "SCO": ["sco pricing"],
#             "FSI": ["fsi pricing"],
#             "Institutional Site": ["institutional site"]
#         }
#         extract_multi(pricing_group_patterns, "material_pricing_group_desc")
        
#         # ==========================================
#         # 10. SECTOR DETECTION (Sector - numeric)
#         # ==========================================
#         if "sector" in query_lower:
#             match = re.search(r'sector\s+(\d+)', query_lower)
#             if match:
#                 filters["sector"] = match.group(1)
#         # ==========================================
#         # 10B. SECTOR CATEGORY (Residential/Commercial) - NEW
#         # ==========================================
#         # NOTE: This detects sector TYPE (residential/commercial)
#         # which is different from numeric sector (e.g., "sector 32")
#         # sector_category_patterns = {
#         #     "Residential": ["residential", "resi"],
#         #     "Commercial": ["commercial", "comm"]
#         # }
        
#         # Check if query contains residential/commercial keywords
#         # found_sectors = []
#         # for sector_val, keywords in sector_category_patterns.items():
#         #     for keyword in keywords:
#         #         if keyword in query_lower:
#         #             found_sectors.append(sector_val)
#         #             break
        
#         # if found_sectors:
#         #     # If multiple sectors found (e.g., "residential and commercial")
#         #     if len(found_sectors) > 1:
#         #         # Only override if we haven't already set a numeric sector
#         #         if "sector" not in filters:
#         #             filters["sector"] = found_sectors
#         #     else:
#         #         # Single sector - only set if numeric sector wasn't found
#         #         if "sector" not in filters:
#         #             filters["sector"] = found_sectors[0]
#         # ==========================================
#         # 11. BOOKING TYPE (Booking_Type)
#         # ==========================================
#         booking_type_patterns = {
#             "Fresh": ["fresh booking", "fresh", "new booking"],
#             "Fresh (Indirect)": ["fresh indirect", "indirect fresh"],
#             "Relocation": ["relocation", "relocated"],
#             "Relocation (Indirect)": ["relocation indirect", "indirect relocation"],
#             "Unit Transfer": ["unit transfer booking"],
#             "Farmer (Indirect)": ["farmer indirect", "indirect farmer"]
#         }
#         extract_multi(booking_type_patterns, "booking_type")
        
#         # ==========================================
#         # 12. CUSTOMER TYPE (Customer_Type) - Booking Status
#         # ==========================================
#         if "cancelled" in query_lower and "not" not in query_lower:
#             filters["customer_type"] = "Cancelled"
#         # elif "transferred" in query_lower:
#         #     filters["customer_type"] = "Transferred"
#         elif "booked" in query_lower and "pre" not in query_lower:
#             filters["customer_type"] = "Booked"
        
#         # ==========================================
#         # 13. LOAN BANK DETECTION (55 Banks)
#         # ==========================================
#         bank_patterns = {
#             "HDF": ["hdfc", "hdfc bank", "housing development"],
#             "ICI": ["icici", "icici bank"],
#             "SBI": ["sbi", "state bank of india", "state bank"],
#             "AXB": ["axis", "axis bank"],
#             "PNB": ["pnb", "punjab national", "punjab national bank"],
#             "KMB": ["kotak", "kotak mahindra", "kotak bank"],
#             "BOI": ["boi", "bank of india"],
#             "BOB": ["bob", "bank of baroda", "baroda bank"],
#             "CNB": ["canara", "canara bank"],
#             "UBI": ["ubi", "union bank", "union bank of india"],
#             "YES": ["yes bank", "yes"],
#             "IDB": ["idbi", "idbi bank"],
#             "PGB": ["punjab gramin bank"],
#             "RBL": ["rbl", "rbl bank"],
#             "DCH": ["dewan housing", "dhfl", "dch"],
#             "L&T": ["l&t", "l&t finance", "larsen toubro"],
#             "IDF": ["indiabulls", "indiabulls housing"],
#             "LIC": ["lic", "lic housing", "lic housing finance"],
#             "J&K": ["j&k", "jammu kashmir bank"],
#             "OBC": ["obc", "oriental bank"],
#             "IOB": ["iob", "indian overseas bank"],
#             "CBI": ["cbi", "central bank of india"],
#             "SBH": ["sbh", "state bank hyderabad"],
#             "SBP": ["sbp", "state bank patiala"]
#         }
#         extract_multi(bank_patterns, "loan_bank")
        
#         # 14. SALES EXECUTIVE (Sales_Executive_Name)
#         # ==========================================
#         if "sales executive" in query_lower or "salesman" in query_lower or "executive" in query_lower:
#             # Extract name after "sales executive" or "salesman"
#             match = re.search(r'(?:sales executive|salesman|executive)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
#             if match:
#                 filters["sales_executive_name"] = match.group(1)
        
#         # ==========================================
#         # 15. BACK OFFICE EXECUTIVE (Back_Office_Executive_Name)
#         # ==========================================
#         if "back office" in query_lower:
#             match = re.search(r'back office\s+(?:executive\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
#             if match:
#                 filters["back_office_executive_name"] = match.group(1)
        
#         # ==========================================
#         # 16. BROKER NAME (Broker_Name)
#         # ==========================================
#         if "broker" in query_lower and "sales" not in query_lower and "channel" not in query_lower:
#             # Try to extract broker name
#             match = re.search(r'broker\s+([A-Z][A-Z\s&.()]+)', user_query)
#             if match:
#                 filters["broker_name"] = match.group(1).strip()
        
#         # ==========================================
#         # 17. SUB BROKER NAME (Sub_Broker_Name)
#         # ==========================================
#         if "sub broker" in query_lower or "sub-broker" in query_lower:
#             match = re.search(r'sub[\s-]?broker\s+([A-Z][A-Z\s&.()]+)', user_query)
#             if match:
#                 filters["sub_broker_name"] = match.group(1).strip()
        
#         # ==========================================
#         # 18. CUSTOMER NAME (Sold_To_Name)
#         # ==========================================
#         if "customer" in query_lower or "buyer" in query_lower:
#             match = re.search(r'(?:customer|buyer)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
#             if match:
#                 filters["sold_to_name"] = match.group(1)
        
#         # ==========================================
#         # 19. PAYER NAME (Payer_Name)
#         # ==========================================
#         if "payer" in query_lower:
#             match = re.search(r'payer\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
#             if match:
#                 filters["payer_name"] = match.group(1)
        
#         # ==========================================
#         # 20. BILLING BLOCK (Billing_Block_Description)
#         # ==========================================
#         billing_block_patterns = {
#             "Booking Cancel": ["booking cancel", "cancelled booking"],
#             "Cancel - In Process": ["cancel in process", "cancellation in process"],
#             "Cancelled/Others": ["cancelled others"],
#             "Dormant Sale": ["dormant", "dormant sale"],
#             "Hold-Legal Case": ["legal case", "hold legal"],
#             "Merger": ["merger"],
#             "Relocation": ["relocation block"],
#             "Billed in Legacy ERP": ["legacy erp", "billed in legacy"]
#         }
        
#         for block, keywords in billing_block_patterns.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     filters["billing_block_description"] = block
#                     break
        
#         # Check for "not blocked" or "no billing block"
#         if any(phrase in query_lower for phrase in ["not blocked", "no billing block", "unblocked"]):
#             filters["billing_block"] = "NULL"
#         elif "blocked" in query_lower or "billing block" in query_lower:
#             if "billing_block_description" not in filters:
#                 filters["billing_block"] = "NOT NULL"
        
#         # ==========================================
#         # 21. REASON FOR REJECTION (Reason_for_Rejection)
#         # ==========================================
#         if "rejected" in query_lower or "rejection" in query_lower:
#             # Check if specific rejection code is mentioned (Z1-ZB)
#             match = re.search(r'\b(Z[0-9AB]|00)\b', user_query, re.IGNORECASE)
#             if match:
#                 filters["reason_for_rejection"] = match.group(1).upper()
#             else:
#                 filters["reason_for_rejection"] = "NOT NULL"
        
#         # ==========================================
#         # 22. SALES ORDER NUMBER (Sales_Order)
#         # ==========================================
#         if "sales order" in query_lower or re.search(r'\bSO[\s:-]?\d+', user_query, re.IGNORECASE):
#             match = re.search(r'(?:SO|sales order)[\s:-]?(\d+)', user_query, re.IGNORECASE)
#             if match:
#                 filters["sales_order"] = match.group(1)
        
#         # ==========================================
#         # 23. DOCUMENT TYPE (Document_Type)
#         # ==========================================
#         if "document type" in query_lower:
#             match = re.search(r'document type\s+([A-Z0-9]+)', user_query)
#             if match:
#                 filters["document_type"] = match.group(1)
        
#         # ==========================================
#         # 24. INVENTORY CODE (Inventory_Code)
#         # ==========================================
#         if "unit" in query_lower or "inventory" in query_lower:
#             # Match inventory codes like "WC-1234", "INV-001"
#             match = re.search(r'(?:unit|inventory)[\s:-]?([A-Z0-9-]+)', user_query, re.IGNORECASE)
#             if match:
#                 filters["inventory_code"] = match.group(1).upper()
        
#         # ==========================================
#         # 25. UOM (Unit of Measurement)
#         # ==========================================
#         uom_patterns = {
#             "FT2": ["sq ft", "square feet", "sqft", "ft2"],
#             "M2": ["sq m", "square meter", "sqm", "m2"],
#             "YD2": ["sq yd", "square yard", "sqyd", "yd2"],
#             "ACR": ["acre", "acres"]
#         }
        
#         for uom, keywords in uom_patterns.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     filters["uom"] = uom
#                     break
        
#         # ==========================================
#         # 26. MATERIAL GROUP (Material_Group)
#         # ==========================================
#         material_group_patterns = {
#             "ZSDBSP": ["zsdbsp"],
#             "ZSDBSP1": ["zsdbsp1"],
#             "ZSDBSP4": ["zsdbsp4"],
#             "ZSDBSP5": ["zsdbsp5"]
#         }
        
#         for group, keywords in material_group_patterns.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     filters["material_group"] = group
#                     break
        
#         # ==========================================
#         # 27. REFERRAL (Refferal)
#         # ==========================================
#         if "referral by" in query_lower or "referred by" in query_lower:
#             match = re.search(r'(?:referral|referred)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
#             if match:
#                 filters["refferal"] = match.group(1)
        
#         # ==========================================
#         # 28. CO-APPLICANT NAMES
#         # ==========================================
#         if "co-applicant" in query_lower or "co applicant" in query_lower:
#             match = re.search(r'co[\s-]?applicant\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
#             if match:
#                 filters["co_applicant1_name"] = match.group(1)
        
#         # ==========================================
#         # 29. PLC MATERIAL (PLC_Material_Desc)
#         # ==========================================
#         if "plc" in query_lower:
#             match = re.search(r'plc\s+([A-Z0-9/]+)', user_query, re.IGNORECASE)
#             if match:
#                 filters["plc_material_desc"] = match.group(1).upper()
        
#         # ==========================================
#         # 30. SCHEME CODE (Scheme_Code)
#         # ==========================================
#         if "scheme" in query_lower:
#             match = re.search(r'scheme\s+([A-Z0-9]+)', user_query, re.IGNORECASE)
#             if match:
#                 filters["scheme_code"] = match.group(1).upper()
        
#         # ==========================================
#         # 31. CONSORTIUM (Consortium_Name)
#         # ==========================================
#         if "consortium" in query_lower:
#             match = re.search(r'consortium\s+([A-Z][A-Z\s&.()]+)', user_query)
#             if match:
#                 filters["consortium_name"] = match.group(1).strip()
        
#         # ==========================================
#         # 32. CHARGE TYPE (Charge_Type)
#         # ==========================================
#         if "charge type" in query_lower:
#             match = re.search(r'charge type\s+([A-Z][a-z]+)', user_query)
#             if match:
#                 filters["charge_type"] = match.group(1)
        
#         # ==========================================
#         # 33. TAX CLASSES (Tax_Class_1 to Tax_Class_7)
#         # ==========================================
#         if "tax class" in query_lower:
#             match = re.search(r'tax class\s+(\d+)', query_lower)
#             if match:
#                 tax_num = match.group(1)
#                 # Extract tax value
#                 tax_match = re.search(r'tax class\s+\d+\s+([A-Z0-9]+)', user_query, re.IGNORECASE)
#                 if tax_match:
#                     filters[f"tax_class_{tax_num}"] = tax_match.group(1).upper()

#         # ==========================================
#         # 33. SALES GROUP (Sales_Group / Sales_Group_Desc)
#         # ==========================================
#         if "sales group" in query_lower or "group" in query_lower:
#             # Pattern: "Sales Group 01", "Group 01", "Sales Group 10"
#             match = re.search(r'(?:sales )?group\s+(\d+)', query_lower)
#             if match:
#                 group_val = match.group(1)
#                 # Ensure 2-digit padding if needed (e.g. "1" -> "01")
#                 filters["sales_group"] = group_val.zfill(2)

#         # ==========================================
#         # 34. SALES OFFICE (Sales_Office / Sales_Office_Desc)
#         # ==========================================
#         if "sales office" in query_lower:
#             match = re.search(r'sales office\s+([A-Z0-9\s]+)', user_query, re.IGNORECASE)
#             if match:
#                 # Capture the office name (e.g. "Sales Office Noida")
#                 filters["sales_office_desc"] = match.group(1).strip()

#         # ==========================================
#         # 35. PAYMENT PLAN / BILLING PLAN (Billing_Plan)
#         # ==========================================
#         if "plan" in query_lower:
#             # Pattern: "CLP Plan", "Down Payment Plan"
#             if "clp" in query_lower:
#                 filters["billing_plan"] = "CLP"
#             elif "down payment" in query_lower:
#                 filters["billing_plan"] = "Down Payment"
#             elif "flexi" in query_lower:
#                 filters["billing_plan"] = "Flexi"

#         # ============================================================
#         # IMPLICIT GROUPING LOGIC (Moved to end)
#         # ============================================================
#         # If the user specifies multiple values for a filter (e.g. "Broker and Direct"),
#         # AND they ask for "group by" or "wise", we should add that dimension to the grouping.
        
#         has_grouping_intent = any(k in query_lower for k in ["group", "wise", "breakdown", "split", "vs"])
        
#         if has_grouping_intent:
#             for dim, val in filters.items():
#                 if isinstance(val, list) and len(val) > 1:
#                     if dim not in dimensions:
#                         dimensions.append(dim)
#                         print(f"[INFERENCE] Added {dim} to dimensions due to multi-value filter + grouping intent")

#         return filters if filters else None

#     # =====================================================
#     # METRIC NORMALIZATION
#     # =====================================================
    
#     def _normalize_metric(self, metric: str, user_query: str) -> str:
#         """
#         Map natural language to proper metric name.
#         Handles aliases and context.
#         """
#         query_lower = user_query.lower()

#         # If a specialist detector (_detect_transfer_query, _detect_possession_metric)
#         # already resolved the metric to a known specific metric, trust it and return
#         # immediately — skip keyword scanning to avoid false substring matches.
#         # e.g. "transferred sales value" contains "sales value" which would otherwise
#         # match the sales_value keyword and overwrite the correct transferred_sales_value.
#         specialist_metrics = {
#             "transferred_sales", "transfer_product_wise", "transferred_sales_count",
#             "transferred_sales_value", "transfer_recipients", "non_transferred_sales",
#             "transfer_rate",
#             "possession_pending_count", "possession_given_count",
#             "agreement_pending_count", "agreement_given_count",
#             "possession_completion_rate", "possession_status_breakdown",
#             "average_days_to_possession", "average_days_to_agreement",
#             "average_total_cycle_time", "possession_pending_value",
#             "possession_given_value", "agreement_pending_value",
#             "tower_wise_possession_pending", "tower_wise_possession_given",
#             "floor_wise_possession_pending", "product_wise_possession_pending",
#             "cancelled_sales", "cancelled_units", "cancelled_sales_value",
#         }
#         if metric in specialist_metrics:
#             return metric

#         # Check keyword matches
#         for metric_name, keywords in self.METRIC_KEYWORDS.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     # Special: "net value" could mean sales_value
#                     if metric_name == "net_value":
#                         return "sales_value"
#                     return metric_name
        
#         # Fallback: if metric is already valid, use it
#         valid_metrics = [
#             "total_sales", "sales_value", "net_value",
#             "amount_received", "amount_demanded",
#             "collection_percentage", "area_sold",
#             # Transfer metrics
#             "transferred_sales", "transfer_product_wise", "transferred_sales_count",
#             "transferred_sales_value", "transfer_recipients",
#             "transfer_rate",
#             # Possession metrics
#             "possession_pending_count", "possession_given_count",
#             "agreement_pending_count", "agreement_given_count",
#             "possession_completion_rate", "possession_status_breakdown",
#             "average_days_to_possession", "average_days_to_agreement",
#             "average_total_cycle_time",
#             "possession_pending_value", "possession_given_value", "agreement_pending_value",
#             # Dimensional possession metrics
#             "tower_wise_possession_pending", "tower_wise_possession_given",
#             "floor_wise_possession_pending", "product_wise_possession_pending"
#         ]
        
#         if metric in valid_metrics:
#             return metric
        
#         # Default
#         return "total_sales"

#     # =====================================================
#     # TIME GRAIN DETECTION
#     # =====================================================
    
#     def _detect_time_grain(self, user_query: str) -> Optional[str]:
#         """
#         Detect time grain from keywords.
        
#         Examples:
#         - "monthly sales" -> "month"
#         - "quarterly breakdown" -> "quarter"
#         - "daily trend" -> "day"
#         """
#         query_lower = user_query.lower()
        
#         for grain, keywords in self.TIME_GRAIN_KEYWORDS.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     return grain
        
#         return None
    


#     def _extract_person_name_filter(self, query_lower: str, dim_name: str) -> Optional[str]:
#         """
#         FIX #2: Extract person names from queries.
        
#         Examples:
#             "total sales of payer sunil" -> "sunil"
#             "sales sold to amit hora" -> "amit hora"  
#             "sales by sales executive john smith" -> "john smith"
#         """
#         # Keywords that indicate a person name follows
#         person_indicators = {
#             "payer_name": ["payer"],
#             "sold_to_name": ["sold to", "customer name"],
#             "sales_executive_name": ["sales executive", "salesman"],
#             "broker_name": ["broker name", "agent"],
#         }
        
#         if dim_name not in person_indicators:
#             return None
        
#         indicators = person_indicators[dim_name]
        
#         # Words that indicate a grouping/dimension request, NOT a person name
#         name_stopwords = {
#             'wise', 'split', 'breakdown', 'total', 'sales', 'count', 'number',
#             'of', 'by', 'in', 'for', 'and', 'the', 'with', 'report', 'data',
#             'summary', 'analysis', 'trend', 'all', 'each', 'per', 'show', 'me'
#         }

#         for indicator in indicators:
#             # Look for "payer X", "sold to X", etc.
#             # Match 1-3 words after the indicator
#             pattern = rf'{indicator}\s+([a-z]+(?:\s+[a-z]+){{0,2}})'
#             match = re.search(pattern, query_lower)
#             if match:
#                 name = match.group(1).strip()
#                 first_word = name.split()[0].lower()
#                 # Reject if it starts with a grouping/reporting word — not a person name
#                 if first_word in name_stopwords:
#                     continue
#                 # Capitalize properly
#                 return ' '.join(word.capitalize() for word in name.split())
        
#         return None


    

#     def _extract_multi_values_with_and(self, query_lower: str, dim_name: str) -> Optional[List[str]]:
#         """
#         FIX #4 & #5: Extract multiple values connected by 'and'.
        
#         Examples:
#             "15th floor and 16th floor" -> ["15th", "16th"]
#             "tower a and tower 7" -> ["a", "7"]
#             "eden and amore" -> ["eden", "amore"]
#         """
#         # Dimension-specific extraction patterns
#         patterns = {
#             "floor_desc": [
#                 # Match: "15th floor and 16th floor"
#                 r'(\d+(?:st|nd|rd|th)?)\s+floor\s+and\s+(\d+(?:st|nd|rd|th)?)\s+floor',
#                 # Match: "floor 15 and floor 16"
#                 r'floor\s+(\d+)\s+and\s+floor\s+(\d+)',
#                 # Match: "15th and 16th floor"
#                 r'(\d+(?:st|nd|rd|th)?)\s+and\s+(\d+(?:st|nd|rd|th)?)\s+floor',
#             ],
#             "tower": [
#                 # Match: "tower a and tower 7"
#                 r'tower\s+([a-z0-9]+)\s+and\s+tower\s+([a-z0-9]+)',
#                 # Match: "tower a and 7"
#                 r'tower\s+([a-z0-9]+)\s+and\s+([a-z0-9]+)',
#             ],
#             "sales_group_desc": [
#                 # Match: "eden and amore"
#                 r'(eden|amore|livork)\s+and\s+(eden|amore|livork)',
#             ]
#         }
        
#         if dim_name not in patterns:
#             return None
        
#         for pattern in patterns[dim_name]:
#             match = re.search(pattern, query_lower)
#             if match:
#                 values = list(match.groups())
#                 # Clean and deduplicate
#                 values = [v.strip() for v in values if v]
#                 return list(dict.fromkeys(values))  # Remove duplicates, preserve order
        
#         return None



#     # =====================================================
#     # COMPARISON TYPE DETECTION
#     # =====================================================
    
#     def _detect_comparison_type(self, user_query: str) -> Optional[str]:
#         """
#         Detect time-based comparisons.
        
#         Examples:
#         - "month on month sales" -> "mom"
#         - "year on year growth" -> "yoy"
#         """
#         query_lower = user_query.lower()
        
#         for compare_type, keywords in self.COMPARISON_KEYWORDS.items():
#             for keyword in keywords:
#                 if keyword in query_lower:
#                     return compare_type
        
#         return None

#     def _suggest_time_grain_for_comparison(self, compare_to: str) -> str:
#         """
#         Suggest appropriate time grain for a comparison type.
#         """
#         grain_map = {
#             "mom": "month",
#             "wow": "day",  # week is not in Presto, so day for daily data
#             "qoq": "quarter",
#             "yoy": "year",
#         }
        
#         return grain_map.get(compare_to, "month")
    

#     # def _detect_status_breakdown_query(self, query_lower: str) -> Optional[Tuple[str, str]]:
#     #     """
#     #     FIX #3: Detect if query is asking for status breakdown.
#     #     Returns tuple of (dimension_name, metric_name) or None.
#     #     """
#     #     # Possession status queries
#     #     if any(phrase in query_lower for phrase in ["possession status", "possession breakdown", "possession wise"]):
#     #         return ("possession_status", "total_sales")
        
#     #     # Agreement status queries  
#     #     if any(phrase in query_lower for phrase in ["agreement status", "agreement breakdown", "agreement wise"]):
#     #         return ("agreement_status", "total_sales")
        
#     #     return None
#     def _detect_status_breakdown_query(self, query_lower: str) -> Optional[str]:
#         """
#         Detect if query is asking for status breakdown.
#         Returns the dimension name to use.
#         """
#         # Possession status queries
#         if any(phrase in query_lower for phrase in [
#             "possession status", 
#             "possession breakdown", 
#             "possession wise",
#             "possession stage"
#         ]):
#             return "possession_status"
        
#         # Agreement status queries
#         if any(phrase in query_lower for phrase in [
#             "agreement status", 
#             "agreement breakdown", 
#             "agreement wise",
#             "agreement stage"
#         ]):
#             return "agreement_status"
        
#         return None

#     # =====================================================
#     # CUSTOM DATE EXTRACTION
#     # =====================================================
    
#     def _extract_custom_dates(self, user_query: str) -> Optional[List[Dict]]:
#         """
#         Extract specific dates/months from user query.
        
#         Examples:
#         - "april 2024" -> [{"month_num": 4, "year": 2024}]
#         - "from april to september 2024" -> [{"month_num": 4, "year": 2024}, {"month_num": 9, "year": 2024}]
#         - "in may" -> [{"month_num": 5, "year": <inferred_fy_year>}]
#         """
#         query_lower = user_query.lower()
#         dates = []
        
#         month_map = {
#             'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
#             'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
#             'aug': 8, 'august': 8, 'sep': 9, 'september': 9, 'oct': 10, 'october': 10,
#             'nov': 11, 'november': 11, 'dec': 12, 'december': 12
#         }

#         months_regex = r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'

#         # Helper: Infer year for a month in Current Financial Year
#         def infer_fy_year(m_num):
#             today = datetime.date.today()
#             # *** CRITICAL: Correct FY Logic ***
#             # If today is Jan-Mar (month < 4), FY started in PREVIOUS year
#             # If today is Apr-Dec (month >= 4), FY started in CURRENT year
#             if today.month < 4:
#                 # We're in Jan-Mar, so FY started last year
#                 fy_start_year = today.year - 1
#             else:
#                 # We're in Apr-Dec, so FY started this year
#                 fy_start_year = today.year
                
#             # Now determine which year the target month belongs to:
#             # Months Apr(4)-Dec(12) belong to fy_start_year
#             # Months Jan(1)-Mar(3) belong to fy_start_year + 1
#             if m_num >= 4:
#                 return fy_start_year
#             else:
#                 return fy_start_year + 1

#         # Pattern 0: Range "from Month to Month Year" (e.g. "from april to september 2024")
#         range_pattern = rf'from\s+{months_regex}\s+to\s+{months_regex}\s+(\d{{4}})'
#         range_matches = re.findall(range_pattern, query_lower)
        
#         if range_matches:
#             for m1, m2, y_str in range_matches:
#                 y = int(y_str)
#                 dates.append({"month_num": month_map.get(m1), "year": y})
#                 dates.append({"month_num": month_map.get(m2), "year": y})
#             return dates

#         # Pattern 1: Month Year (e.g., "april 2024")
#         month_year_pattern = rf'{months_regex}\s+(\d{{4}})'
#         matches = re.findall(month_year_pattern, query_lower)
        
#         if matches:
#             for month_str, year_str in matches:
#                 month_num = month_map.get(month_str, 1)
#                 year_num = int(year_str)
#                 dates.append({
#                     "month_num": month_num,
#                     "year": year_num
#                 })
#             return dates
        
#         # Pattern 2: Full dates (e.g., "2024-04-15", "15/04/2024")
#         date_pattern = r'(\d{4})-(\d{2})-(\d{2})|(\d{2})/(\d{2})/(\d{4})'
#         date_matches = re.findall(date_pattern, query_lower)
        
#         if date_matches:
#             for match in date_matches:
#                 if match[0]:  # YYYY-MM-DD
#                     dates.append({"year": int(match[0]), "month_num": int(match[1]), "day": int(match[2])})
#                 elif match[3]:  # DD/MM/YYYY
#                     dates.append({"day": int(match[3]), "month_num": int(match[4]), "year": int(match[5])})
#             return dates

#         # Pattern 3: Month Only (Infer Year)
#         # Matches standalone months like "sales in May", "August sales"
#         # Only if NO other date patterns matched
#         standalone_matches = re.findall(rf'\b{months_regex}\b', query_lower)
#         if standalone_matches:
#             unique_months = set(standalone_matches)
#             for m_str in unique_months:
#                 m_num = month_map.get(m_str)
#                 if m_num:
#                     y_inferred = infer_fy_year(m_num)
#                     dates.append({"month_num": m_num, "year": y_inferred})
#             return dates
        
#         return None
    
    
#     def _normalize_intent_keys(self, data: dict) -> dict:
#         """
#         Normalize LLM response keys to match SemanticIntent fields.
#         Handles: daterange→date_range, customdates→custom_dates, etc.
#         """
#         key_map = {
#             "daterange": "date_range",
#             "customdates": "custom_dates",
#             "timegrain": "time_grain",
#             "istrend": "is_trend",
#             "compareto": "compare_to",
#             "orderby": "order_by",
#             "orderdirection": "order_direction",
#         }
        
#         for src, dst in key_map.items():
#             if src in data and dst not in data:
#                 data[dst] = data[src]
        
#         # Normalize date_range variants
#         if "date_range" in data and data["date_range"]:
#             dr = str(data["date_range"]).strip().lower().replace(" ", "_")
#             alias_map = {
#                 "thisweek": "this_week",
#                 "lastweek": "last_week",
#                 "thismonth": "this_month",
#                 "lastmonth": "last_month",
#                 "thisquarter": "this_quarter",
#                 "lastquarter": "last_quarter",
#                 "thisyear": "this_year",
#                 "lastyear": "last_year",
#                 "currentfinancialyear": "current_financial_year",
#                 "lastfinancialyear": "last_financial_year",
#                 "rolling7days": "rolling_7_days",
#                 "rolling30days": "rolling_30_days",
#                 "rolling90days": "rolling_90_days",
#             }
#             data["date_range"] = alias_map.get(dr, dr)
            
#         # =========================================================
#         # DIMENSION NORMALIZATION (Fix LLM Hallucinations)
#         # =========================================================
#         if "dimensions" in data and isinstance(data["dimensions"], list):
#             normalized_dims = []
#             for dim in data["dimensions"]:
#                 # Lowercase and strip
#                 d = str(dim).strip().lower().replace(" ", "_")
                
#                 # Check aliases
#                 if d in self.DIMENSION_ALIASES:
#                     d = self.DIMENSION_ALIASES[d]
                
#                 normalized_dims.append(d)
            
#             data["dimensions"] = normalized_dims
        
#         return data


#     # def _detect_date_range_keywords(self, user_query: str) -> Optional[str]:
#     #     """
#     #     Keyword-based date detection (fallback when LLM fails).
#     #     Returns canonical snake_case format.
#     #     NOTE: q1-q4 are handled by _extract_custom_dates_enhanced(), not here.
#     #     """
#     #     query_lower = user_query.lower()
        
#     #     # Order matters - check specific phrases first!
#     #     date_patterns = [
#     #         # Financial year
#     #         (["current financial year", "current fy", "this fy"], "current_financial_year"),
#     #         (["last financial year", "previous fy", "last fy"], "last_financial_year"),
#     #         (["fytd", "financial year to date"], "fytd"),
            
#     #         # Year on Year (defaults to last 3 FYs)
#     #         (["year on year", "yoy", "year-on-year", "last 3 years", "last 3 year"], "last_3_financial_years"),
#     #         (["quarter on quarter", "qoq", "quarter-on-quarter"], "current_financial_year"),
            
#     #         # Specific periods (with spaces)
#     #         (["last month"], "last_month"),
#     #         (["this month"], "this_month"),
#     #         (["last quarter"], "last_quarter"),
#     #         (["this quarter"], "this_quarter"),
#     #         (["last week"], "last_week"),
#     #         (["this week"], "this_week"),
#     #         (["last year"], "last_financial_year"),
#     #         (["this year"], "current_financial_year"),
#     #         (["yesterday"], "yesterday"),
#     #         (["today"], "today"),
            
#     #         # To-date
#     #         (["mtd", "month to date"], "mtd"),
#     #         (["qtd", "quarter to date"], "qtd"),
#     #         (["ytd", "year to date"], "ytd"),
            
#     #         # Rolling
#     #         (["last 7 days", "past 7 days"], "rolling_7_days"),
#     #         (["last 30 days", "past 30 days"], "rolling_30_days"),
#     #         (["last 90 days", "past 90 days"], "rolling_90_days"),
#     #     ]
        
#     #     for patterns, date_range in date_patterns:
#     #         for pattern in patterns:
#     #             if pattern in query_lower:
#     #                 return date_range
        
#     #     return None
#     def _detect_date_range_keywords(self, user_query: str) -> Optional[str]:
#         """
#         ENHANCED keyword-based date detection with support for:
#         1. Specific years (2024, 2025, etc.)
#         2. Specific quarters (Q1, Q2, etc.)
#         3. Last N periods (last 3 years, last 2 quarters, etc.)
#         4. All existing relative date patterns
        
#         Returns canonical snake_case format.
#         """
#         query_lower = user_query.lower().strip()
        
#         # ========================================
#         # 0. YoY / YEAR-WISE (MUST come before year regex to avoid fy_YYYY mismatch)
#         # ========================================
#         yoy_triggers = ["year on year", "yoy", "year-on-year", "y-o-y",
#                         "year wise", "yearwise", "year-wise", "yearly",
#                         "all years", "year by year", "each year", "per year", "annual trend"]
#         if any(t in query_lower for t in yoy_triggers):
#             return "last_3_financial_years"
        
#         # ========================================
#         # 1. SPECIFIC YEAR PATTERN (Highest Priority)
#         # ========================================
#         # Patterns: "in 2024", "for 2024", "during 2024", "2024"
#         year_patterns = [
#             r'\bin\s+(20\d{2})\b',
#             r'\bfor\s+(20\d{2})\b', 
#             r'\bduring\s+(20\d{2})\b',
#             r'\byear\s+(20\d{2})\b',
#             r'\b(20\d{2})\b'  # Standalone year
#         ]
        
#         for pattern in year_patterns:
#             match = re.search(pattern, query_lower)
#             if match:
#                 year = int(match.group(1))
#                 # Return financial year identifier
#                 return f"fy_{year}"
        
#         # ========================================
#         # 2. SPECIFIC QUARTER PATTERN  
#         # ========================================
#         # Patterns: "Q1", "Q2", "first quarter", "second quarter"
#         quarter_map = {
#             r'\bq1\b': 'q1',
#             r'\bq2\b': 'q2', 
#             r'\bq3\b': 'q3',
#             r'\bq4\b': 'q4',
#             r'\bfirst quarter\b': 'q1',
#             r'\bsecond quarter\b': 'q2',
#             r'\bthird quarter\b': 'q3',
#             r'\bfourth quarter\b': 'q4'
#         }
        
#         for pattern, quarter_id in quarter_map.items():
#             if re.search(pattern, query_lower):
#                 return quarter_id
        
#         # ========================================
#         # 3. LAST N PERIODS (Financial Year Aware)
#         # ========================================
        
#         # Last N financial years
#         last_fy_match = re.search(r'\blast\s+(\d+)\s+(?:financial\s+)?years?\b', query_lower)
#         if last_fy_match:
#             n = int(last_fy_match.group(1))
#             return f"last_{n}_financial_years"
        
#         # Last N quarters (financial quarters)
#         last_q_match = re.search(r'\blast\s+(\d+)\s+quarters?\b', query_lower)
#         if last_q_match:
#             n = int(last_q_match.group(1))
#             return f"last_{n}_quarters"
        
#         # Last N months
#         last_m_match = re.search(r'\blast\s+(\d+)\s+months?\b', query_lower)
#         if last_m_match:
#             n = int(last_m_match.group(1))
#             return f"last_{n}_months"
        
#         # ========================================
#         # 4. EXISTING RELATIVE TIME PATTERNS
#         # ========================================
        
#         # Order matters - check specific phrases first!
#         date_patterns = [
#             # Financial year
#             (["current financial year", "current fy", "this fy"], "current_financial_year"),
#             (["last financial year", "previous fy", "last fy"], "last_financial_year"),
#             (["fytd", "financial year to date"], "fytd"),
            
#             # Year on Year (defaults to last 3 FYs)
#             (["year on year", "yoy", "year-on-year"], "last_3_financial_years"),
#             (["quarter on quarter", "qoq", "quarter-on-quarter"], "current_financial_year"),
            
#             # Specific periods (with spaces)
#             (["last month"], "last_month"),
#             (["this month"], "this_month"),
#             (["last quarter"], "last_quarter"),
#             (["this quarter"], "this_quarter"),
#             (["last week"], "last_week"),
#             (["this week"], "this_week"),
#             (["last year"], "last_financial_year"),
#             (["this year"], "current_financial_year"),
#             (["yesterday"], "yesterday"),
#             (["today"], "today"),
            
#             # To-date
#             (["mtd", "month to date"], "mtd"),
#             (["qtd", "quarter to date"], "qtd"),
#             (["ytd", "year to date"], "ytd"),
            
#             # Rolling
#             (["last 7 days", "past 7 days"], "rolling_7_days"),
#             (["last 30 days", "past 30 days"], "rolling_30_days"),
#             (["last 90 days", "past 90 days"], "rolling_90_days"),
#         ]
        
#         for patterns, date_range in date_patterns:
#             for pattern in patterns:
#                 if pattern in query_lower:
#                     return date_range
        
#         return None


#     # =====================================================
#     # PROMPT BUILDER
#     # =====================================================
    
#     def _build_prompt(self, user_query: str) -> str:
#         return f"""
#     You are a semantic intent extractor for an enterprise sales analytics system.
#     You MUST return a SINGLE valid JSON object.

#     --------------------------- JSON SCHEMA (STRICT) ---------------------------
#     {{
#     "metric": "string",
#     "dimensions": ["string"],
#     "date_range": "string",
#     "custom_dates": [{{"month_num": 4, "year": 2024}}] or null,
#     "filters": object or null,
#     "time_grain": "string or null",
#     "is_trend": boolean,
#     "compare_to": "string or null",
#     "order_by": "string or null",
#     "order_direction": "asc|desc|null",
#     "limit": number or null
#     }}

#     --------------------------- DATE HANDLING (ENHANCED) ---------------------------

#     **1. SPECIFIC YEARS:**
#     - "total sales in 2024" → date_range: "fy_2024"
#     - "sales in 2025" → date_range: "fy_2025"  
#     - "revenue for 2023" → date_range: "fy_2023"

#     **2. SPECIFIC QUARTERS (Financial Year):**
#     - "sales in Q1" → date_range: "q1"
#     - "Q2 sales" → date_range: "q2"
#     - "first quarter" → date_range: "q1"

#     **3. LAST N PERIODS (Financial Year Based):**
#     - "last 3 years" → date_range: "last_3_financial_years"
#     - "last 5 years" → date_range: "last_5_financial_years"
#     - "last 2 quarters" → date_range: "last_2_quarters"
#     - "last 4 quarters" → date_range: "last_4_quarters"
#     - "last 6 months" → date_range: "last_6_months"

#     **4. RELATIVE TIME PERIODS:**
#     - "today" → "today"
#     - "yesterday" → "yesterday"  
#     - "this week" → "this_week"
#     - "last week" → "last_week"
#     - "this month" → "this_month"
#     - "last month" → "last_month"
#     - "this quarter" → "this_quarter"
#     - "last quarter" → "last_quarter"
#     - "this year" OR "current FY" → "current_financial_year"
#     - "last year" OR "last FY" → "last_financial_year"

#     **5. TO-DATE RANGES:**
#     - "MTD" or "month to date" → "mtd"
#     - "QTD" or "quarter to date" → "qtd"
#     - "YTD" or "year to date" → "ytd"
#     - "FYTD" or "financial year to date" → "fytd"

#     **6. ROLLING WINDOWS:**
#     - "last 7 days" → "rolling_7_days"
#     - "last 30 days" → "rolling_30_days"
#     - "last 90 days" → "rolling_90_days"

#     **7. CUSTOM DATE (use custom_dates field):**
#     - "April 2024" → date_range: "custom_range", custom_dates: [{{"month_num": 4, "year": 2024}}]

#     **DEFAULT:** If NO date mentioned → "current_financial_year"

#     **IMPORTANT:** Financial year runs April-March. FY 2024 = Apr 2024 to Mar 2025.

#     --------------------------- EXAMPLES ---------------------------

#     Query: "total sales in 2024"
#     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "fy_2024", "time_grain": null}}

#     Query: "sales in Q1"  
#     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "q1", "time_grain": null}}

#     Query: "last 3 years sales"
#     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_3_financial_years", "time_grain": "year"}}

#     Query: "tower wise sales last 2 quarters"
#     Output: {{"metric": "total_sales", "dimensions": ["tower"], "date_range": "last_2_quarters", "time_grain": "quarter"}}

#     Query: "total sales last month"
#     Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_month", "time_grain": null}}

#     Query: "project wise sales by channel last month"
#     Output: {{"metric": "total_sales", "dimensions": ["sales_org_desc", "dist_channel_desc"], "date_range": "last_month", "time_grain": null}}

#     Query: "sales value this quarter"
#     Output: {{"metric": "sales_value", "dimensions": [], "date_range": "this_quarter", "time_grain": null}}

#     ---------------------------USER QUESTION ---------------------------
#     {user_query}

#     Return JSON ONLY. No explanation.
#     """

#     # =====================================================
#     # SAFE JSON PARSER (NEVER FAILS)
#     # =====================================================
    
#     def _parse_json(self, raw_text: str) -> dict:
#         """
#         Parses LLM output into a safe intent dictionary.
#         Falls back gracefully if output is invalid.
#         """
#         fallback = {
#             "metric": "total_sales",
#             "dimensions": [],
#             "date_range": "current_financial_year",
#             "is_trend": False,
#             "time_grain": None,
#             "compare_to": None,
#             "order_by": None,
#             "order_direction": None,
#             "limit": None,
#         }

#         if not raw_text or not raw_text.strip():
#             return fallback

#         text = raw_text.strip()

#         # Try normal parse
#         try:
#             data = json.loads(text)
#         except Exception:
#             # Try extracting JSON from surrounding text
#             json_match = re.search(r'\{.*\}', text, re.DOTALL)
#             if json_match:
#                 try:
#                     data = json.loads(json_match.group())
#                 except Exception:
#                     return fallback
#             else:
#                 # Try repairing truncated JSON
#                 repaired = text
#                 if repaired.startswith("{"):
#                     if repaired.count('"') % 2 != 0:
#                         repaired += '"'
#                     if not repaired.endswith("}"):
#                         repaired += "}"
#                     try:
#                         data = json.loads(repaired)
#                     except Exception:
#                         return fallback
#                 else:
#                     return fallback

#         # Merge with defaults
#         for key, value in fallback.items():
#             if key not in data or data[key] in ("", None):
#                 data[key] = value

#         # Fix partial date ranges
#         if "date_range" in data and data["date_range"]:
#             date_range = data["date_range"]
            
#             # Handle truncated responses
#             if date_range == "current":
#                 data["date_range"] = "current_financial_year"
#             elif date_range == "last":
#                 data["date_range"] = "last_financial_year"
#             elif date_range == "this":
#                 data["date_range"] = "this_month"  # Best guess

#         return data

















import json
import re
import datetime
from typing import Any, Dict, List, Optional, Tuple
# from app.semantic.intent import SemanticIntent
from semantic.intent import SemanticIntent


class WatsonxSemanticAdapter:
    """
    Adapter responsible ONLY for:
    Natural Language -> SemanticIntent
    Uses Watsonx ModelInference.generate_text()
    
    IMPROVEMENTS:
    1. Comprehensive dimension-to-keyword mapping
    2. Filter value extraction (e.g., "tower 7", "amore", "16th floor")
    3. Better metric selection logic
    4. Enhanced natural language understanding
    5. Custom date parsing
    """
    

    # =====================================================
    # MONTH NAME MAPPINGS FOR DATE PARSING
    # =====================================================
    MONTH_NAMES = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9, "sept": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12,
    }


    # =====================================================
    # SEMANTIC MAPPINGS - FIXED ALL NAMING
    # =====================================================
    
    DIMENSION_KEYWORDS = {
        # Property
        # NOTE: specific product/project names (eden, amore, wave city etc.) are intentionally
        # NOT listed here — they are filter values, not dimension keywords.
        # Only generic grouping words trigger a dimension.
        "sales_group_desc": ["product wise", "product split", "product breakdown"],
        "tower": ["tower", "block"],
        "floor": ["floor wise", "floor split", "floor breakdown", "floor level"],
        "inventory_code": ["inventory code", "inventory"],
        "type_desc": ["unit type", "shop", "office", "bhk"],
        "sector": ["sector"],
        
        # Customer & Sales
        "sold_to_name": ["customer", "sold to", "buyer"],
        "payer_name": ["payer","paying customer","payment by"],
        "customer_type": ["booking status", "customer type", "booked"],  # Column name
        "sales_executive_name": ["sales executive", "salesman"],
        "back_office_executive_name": ["back office"],
        
        # Channel
        "broker_name": ["broker name", "agent"],
        "sub_broker_name": ["sub broker"],
        "dist_channel_desc": ["channel", "distribution"],
        "referral": ["referral", "ref"],
        "consortium_name": ["consortium"],
        
        # Transaction
        "booking_type": ["booking type", "fresh", "relocation"],
        "division_desc": ["division"],
        "sales_group_desc": ["sales group", "group"],
        "sales_office_desc": ["sales office", "office"],
        "sales_org_desc": ["sales organization", "sales org", "organization", "org"],
        "billing_plan": ["billing plan", "payment plan"],
        "billing_block_description": ["billing block", "block reason"],
        
        # Financial
        "loan_bank": ["bank", "loan bank", "lender"],
        "material_pricing_group_desc": ["pricing group", "material group","apartment"],
        "scheme_code": ["scheme"],
        "reason_for_rejection": ["rejection reason", "reason for rejection"],

        # FIX #3: Status breakdown dimensions
        "possession_status": ["possession status", "possession stage", "possession breakdown", "possession wise"],
        "agreement_status": ["agreement status", "agreement stage", "agreement breakdown", "agreement wise"],

        "cancellation_reason": ["cancellation reason","reason", "reason for cancellation", "cancel reason", "why cancelled", "cancellation", "reason cancelled"],
    }

    # METRIC_KEYWORDS = {
    #     "total_sales": ["total sales", "sales count", "number of sales", "sales orders"],
    #     "sales_value": ["sales value", "sales amount", "total revenue", "total amount", "gross sales", "net value", "net amount", "booking amount", "booking value"],
    #     "net_value": ["net value", "net amount"],
    #     "amount_received": ["amount received", "collection", "received", "payment received"],
    #     "amount_demanded": ["amount demanded", "billed", "bill amount", "invoice"],
    #     "collection_percentage": ["collection %", "collection percentage", "collection efficiency"],
    #     "area_sold": ["area sold", "total area", "carpet area"],
    #     "basic_selling_price": ["basic price", "base price"],
    #     "discount": ["discount", "total discount"],
    #     "loan_sanctioned": ["loan sanctioned", "loan amount"],
    # }
    

    METRIC_KEYWORDS = {
        # sales_value MUST come before total_sales so "total sales amount" matches sales_value
        # before "total sales" keyword matches total_sales
        "sales_value": ["total sales amount", "sales value", "sales amount", "total revenue",
                        "total amount", "gross sales", "net value", "net amount",
                        "booking amount", "booking value"],
        "total_sales": ["total sales", "sales count", "number of sales", "sales orders"],
        "net_value": ["net value", "net amount"],
        "amount_received": ["amount received", "collection", "received", "payment received"],
        "amount_demanded": ["amount demanded", "billed", "bill amount", "invoice"],
        "collection_percentage": ["collection %", "collection percentage", "collection efficiency"],
        "area_sold": ["area sold", "total area", "carpet area"],
        "basic_selling_price": ["basic price", "base price"],
        "discount": ["discount", "total discount"],
        "loan_sanctioned": ["loan sanctioned", "loan amount"],
        # CORRECTED: Transfer metrics (counts unique customers - Sold_To_Name)
       # ENHANCED TRANSFER METRICS
                "transferred_sales": [
                    "transferred sales", "transfer count", "transferred customers", 
                    "customers who transferred", "how many transferred",
                    "count of transfers", "transfers customer"
                ],
                "transfer_product_wise": [
                    "product wise transfer", "transfer product wise", 
                    "product transfer", "transfers by product", "product wise transfers",
                    "product-wise transfer", "productwise transfer"
                ],
                "transferred_sales_count": [
                    "transfer orders", "transferred units count", "transfer sales orders",
                    "number of transfer orders", "count of transfer orders"
                ],
                "transferred_sales_value": [
                    "transfer value", "transfer revenue", "value of transfers", 
                    "transferred sales value", "transfer amount", "revenue from transfers"
                ],
                "transfer_recipients": [
                    "customers who received", "transfer recipients", "final payers", 
                    "transferred to", "recipients of transfer", "who received transfer"
                ],
                "non_transferred_sales": [
                    "non transferred sales", "normal sales", 
                    "sales without transfer", "customers without transfer", "regular sales"
                ],
                "transfer_rate": [
                    "transfer rate", "transfer percentage", "transfer %", 
                    "percentage of transfers", "what percent transferred"
                ],

                # ═══════════════════════════════════════════════════════════════
                    # POSSESSION JOURNEY METRICS
                    # ═══════════════════════════════════════════════════════════════
                    "possession_pending_count": [
                        "possession pending", "pending possession", "possession not given",
                        "possession yet to be given", "possession not handed over",
                        "how many possessions pending", "pending possessions"
                    ],
                    "possession_given_count": [
                        "possession given", "possession handed over", "possession completed",
                        "possessions done", "how many possessions given"
                    ],
                    "agreement_pending_count": [
                        "agreement pending", "pending agreement", "agreement not created",
                        "agreement not prepared", "agreements pending"
                    ],
                    "agreement_given_count": [
                        "agreement given", "agreement created", "agreement prepared",
                        "agreement signed", "agreements done"
                    ],
                    "possession_completion_rate": [
                        "possession completion rate", "possession completion %",
                        "percentage of possessions given", "possession completion percentage"
                    ],
                    "possession_status_breakdown": [
                        "possession status", "possession journey", "possession wise breakdown",
                        "status of possession", "possession stages"
                    ],
                    "average_days_to_possession": [
                        "average days to possession", "possession TAT", "time to possession",
                        "how long for possession", "possession turnaround time"
                    ],
                    "average_days_to_agreement": [
                        "average days to agreement", "agreement TAT", "time to agreement",
                        "how long for agreement"
                    ],
                    "possession_pending_value": [
                        "value of pending possessions", "pending possession value",
                        "revenue in pending possessions"
                    ],

                    "cancelled_sales": [
                        "cancelled sales", "total cancelled", "cancellations", "cancelled bookings",
                        "cancelled orders", "how many cancelled", "number of cancellations",
                        "cancelled count", "cancellation reason", "show cancelled", "cancel", "cancelled reason", "reason"
],
                    "cancelled_sales_value": [
                        "cancelled sales value", "cancellation value", "value of cancellations",
                        "cancelled revenue", "value of cancelled bookings"
              ],
    }
    

    

    COMPARISON_KEYWORDS = {
        "mom": ["month on month", "month-on-month", "monthly", "m-o-m", "vs last month"],
        "wow": ["week on week", "week-on-week", "weekly", "w-o-w"],
        "qoq": ["quarter on quarter", "quarter-on-quarter", "quarterly", "q-o-q"],
        "yoy": ["year on year", "year-on-year", "yearly", "y-o-y", "vs last year"],
    }

    TIME_GRAIN_KEYWORDS = {
        "day": ["daily", "day", "by day"],
        "month": ["monthly", "month", "by month", "month wise"],
        "quarter": ["quarterly", "quarter", "by quarter"],
        "year": ["yearly", "year", "by year"],
    }

    DIMENSION_ALIASES = {
        "sales_channel": "dist_channel_desc",
        "channel": "dist_channel_desc",
        "distribution_channel": "dist_channel_desc",
        "product": "sales_group_desc",
        "booking_status": "customer_type",
        "status": "customer_type",
        "customer": "sold_to_name",
        "buyer": "sold_to_name",
        "broker name": "broker_name",
        "agent": "broker_name",
        "salesman": "sales_executive_name",
        "sales_executive": "sales_executive_name",
        "executive": "sales_executive_name",
        "inventory code": "inventory_code",
        "inventory": "inventory_code",
        "unit_type": "type_desc",
        "flat_type": "type_desc",
        "billing_block": "billing_block_description",
        "block_reason": "billing_block_description",
        "sales_org": "sales_org_desc",
        "organization": "sales_org_desc",
        "wave city": "sales_org_desc",
        "bank": "loan_bank",
        "lender": "loan_bank",
        "refferal": "refferal",
        "source": "refferal",
        "scheme": "scheme_code",
        "pricing_group": "material_pricing_group_desc",
        "material_group": "material_pricing_group_desc",
        "sales_office": "sales_office_desc",
        "office": "sales_office_desc",
        "sales_group": "sales_group_desc",
        "group": "sales_group_desc",
        "consortium": "consortium_name",
        "payment_plan": "billing_plan",
    }

    def __init__(self, model):
        """
        model: ibm_watsonx_ai.foundation_models.ModelInference
        """
        self.model = model



     # =====================================================
    # MULTI-QUERY DETECTION (NEW!)
    # =====================================================
    
    # def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
    #     """
    #     Detect if query contains multiple separate requests using "and"
        
    #     Examples:
    #     - "sales for wave city and wave estate" → 2 queries
    #     - "direct and broker sales" → 2 queries
    #     - "april and june sales" → 2 queries
        
    #     Returns list of intents (1 if single query, 2+ if multiple)
    #     """
    #     query_lower = user_query.lower()
        
    #     # Pattern 1: Product names with "and"
    #     product_match = re.search(r'(eden|amore|livork)\s+and\s+(veridia|amore|livork)', query_lower)
    #     if product_match:
    #         base_query = re.sub(r'(eden|amore|livork)\s+and\s+(veridia|amore|livork)', '', user_query, flags=re.IGNORECASE).strip()
    #         product1 = product_match.group(1)
    #         product2 = product_match.group(2)
            
    #         intent1 = self.extract_intent(f"{base_query} for {product1}")
    #         intent2 = self.extract_intent(f"{base_query} for {product2}")
    #         return [intent1, intent2]
        
    #     # Pattern 2: Channel with "and" (direct and broker)
    #     channel_match = re.search(r'(direct|broker|indirect)\s+and\s+(direct|broker|indirect)', query_lower)
    #     if channel_match:
    #         base_query = re.sub(r'(direct|broker|indirect)\s+and\s+(direct|broker|indirect)', '', user_query, flags=re.IGNORECASE).strip()
    #         channel1 = channel_match.group(1)
    #         channel2 = channel_match.group(2)
            
    #         intent1 = self.extract_intent(f"{base_query} {channel1}")
    #         intent2 = self.extract_intent(f"{base_query} {channel2}")
    #         return [intent1, intent2]
        
    #     # Pattern 3: Months with "and" (april and june)
    #     month_and_pattern = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\s+and\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)'
    #     month_match = re.search(month_and_pattern, query_lower)
        
    #     if month_match and ' to ' not in query_lower and ' till ' not in query_lower:
    #         base_query = re.sub(month_and_pattern, '', user_query, flags=re.IGNORECASE).strip()
    #         month1 = month_match.group(1)
    #         month2 = month_match.group(2)
            
    #         intent1 = self.extract_intent(f"{base_query} {month1}")
    #         intent2 = self.extract_intent(f"{base_query} {month2}")
    #         return [intent1, intent2]
        
    #     # Single query
    #     return [self.extract_intent(user_query)]

    #     # Pattern 4: year with "and" (2024 and 2025)

    #             # Pattern 4: year with "and" (2024 and 2025)
    #     year_and_pattern = r'\b(20\d{2})\s+and\s+(20\d{2})\b'
    #     year_match = re.search(year_and_pattern, query_lower)

    #     if year_match:
    #         base_query = re.sub(year_and_pattern, '', user_query, flags=re.IGNORECASE).strip()

    #         year1 = int(year_match.group(1))
    #         year2 = int(year_match.group(2))

    #         # Financial year format: April YYYY to March YYYY+1
    #         fy1 = f"april {year1} to march {year1 + 1}"
    #         fy2 = f"april {year2} to march {year2 + 1}"

    #         intent1 = self.extract_intent(f"{base_query} {fy1}")
    #         intent2 = self.extract_intent(f"{base_query} {fy2}")

    #         return [intent1, intent2]

    #     # Single query
    #     return [self.extract_intent(user_query)]
    
    # running
    # def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
    #     """
    #     Detect if query contains multiple separate requests.
        
    #     Handles TWO types of multi-queries:
    #     1. Dimension-based: "wave city and wave estate" → split by products
    #     2. Date-based: "Q1 and Q2" or "2024 and 2025" → split by dates
        
    #     Returns:
    #         List of SemanticIntent objects (one per query)
    #     """
    #     import re
        
    #     query_lower = user_query.lower()
        
    #     # ========================================
    #     # PRIORITY 1: Check for multi-DATE queries FIRST
    #     # ========================================
        
    #     # Pattern 1: Multiple years (e.g., "2024 and 2025")
    #     year_pattern = r'\b(20\d{2})\b'
    #     years = re.findall(year_pattern, query_lower)
    #     if len(years) > 1:
    #         print(f"[MULTI-QUERY] Detected {len(years)} years: {years}")
    #         intents = []
    #         for year in years:
    #             # Create a modified query for each year
    #             modified_query = re.sub(r'\b20\d{2}(?:\s+and\s+20\d{2})+\b', year, user_query, flags=re.IGNORECASE)
    #             intent = self.extract_intent(modified_query)
    #             intents.append(intent)
    #         return intents
        
    #     # Pattern 2: Multiple quarters (e.g., "Q1 and Q2")
    #     quarter_pattern = r'\bq([1-4])\b'
    #     quarters = re.findall(quarter_pattern, query_lower)
    #     if len(quarters) > 1:
    #         print(f"[MULTI-QUERY] Detected {len(quarters)} quarters: Q{quarters}")
    #         intents = []
    #         for q in quarters:
    #             # Create a modified query for each quarter
    #             modified_query = re.sub(r'\bq[1-4](?:\s+and\s+q[1-4])+\b | \bvs\b|\bv/s\b', f'q{q}', user_query, flags=re.IGNORECASE)
    #             intent = self.extract_intent(modified_query)
    #             intents.append(intent)
    #         return intents
        
    #     # Pattern 3: Multiple months (e.g., "april and may")
    #     month_names = ['january', 'february', 'march', 'april', 'may', 'june', 
    #                   'july', 'august', 'september', 'october', 'november', 'december',
    #                   'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        
    #     found_months = []
    #     for month in month_names:
    #         if month in query_lower:
    #             found_months.append(month)
        
    #     if len(found_months) > 1:
    #         print(f"[MULTI-QUERY] Detected {len(found_months)} months: {found_months}")
    #         intents = []
    #         for month in found_months:
    #             # Create a modified query for each month
    #             month_pattern = '|'.join(found_months)
    #             modified_query = re.sub(f'\\b({month_pattern})(?:\\s+and\\s+({month_pattern}))+\\b', 
    #                                    month, user_query, flags=re.IGNORECASE)
    #             intent = self.extract_intent(modified_query)
    #             intents.append(intent)
    #         return intents
        
    #     # ========================================
    #     # PRIORITY 2: Check for multi-DIMENSION queries
    #     # ========================================
        
    #     # Original dimension-based split logic
    #     # Keywords that indicate dimension-based multi-query
    #     dimension_keywords = {
    #         'product': ['wave city', 'wave estate', 'amore', 'livork'],
    #         'tower': ['tower', 'block'],
    #         'floor': ['floor'],
    #         'type': ['apartment', 'shop', 'plot', 'office'],
    #     }
        
    #     # Check if query contains "and" with dimension values
    #     if ' and ' in query_lower:
    #         for dim_type, values in dimension_keywords.items():
    #             found_values = [v for v in values if v in query_lower]
    #             if len(found_values) >= 2:
    #                 print(f"[MULTI-QUERY] Detected {len(found_values)} {dim_type} values: {found_values}")
    #                 intents = []
    #                 for value in found_values:
    #                     # Create a query for each dimension value
    #                     modified_query = user_query.replace(' and ', ' ').lower()
    #                     # Keep only the current value
    #                     for other_value in found_values:
    #                         if other_value != value:
    #                             modified_query = modified_query.replace(other_value, '')
    #                     modified_query = modified_query.replace('  ', ' ').strip()
                        
    #                     intent = self.extract_intent(modified_query)
    #                     # Add filter for the specific value
    #                     if not intent.filters:
    #                         intent.filters = {}
    #                     intent.filters[dim_type] = value
    #                     intents.append(intent)
    #                 return intents
        
    #     # ========================================
    #     # DEFAULT: Single query
    #     # ========================================
    #     return [self.extract_intent(user_query)]
    


    def _detect_multi_query(self, user_query: str) -> List[SemanticIntent]:
        """
        Detect if a query contains multiple separate requests and split them.

        Decision tree (in order):
        1. vs / versus / v/s  → comparison (handled by _detect_comparison_query)
        2. Range query (from...to, from...till, between...and) → single query always
        3. Explicit AND/comma list of same-type items (years, quarters, months, projects) → multi
        4. Everything else → single query

        Key rule: only split when `and` explicitly joins items of the SAME type.
        e.g. "2024 and 2025", "Q1 and Q2", "april and may", "eden and amore"
        NOT: "from 2023 to 2024", "from june to oct", "between Q1 and Q2"
        """
        import re
        import datetime as _dt

        query_lower = user_query.lower().strip()

        # ──────────────────────────────────────────────────────────────
        # STEP 0: Comparison queries (vs / versus / v/s)
        # ──────────────────────────────────────────────────────────────
        comparison = self._detect_comparison_query(user_query)
        if comparison:
            print(f"[MULTI-QUERY] Comparison: {comparison['type']}")
            intents = []

            if comparison['type'] in ['date_year', 'date_quarter']:
                for i, date_range in enumerate(comparison['date_ranges']):
                    clean = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', user_query, flags=re.IGNORECASE)
                    for item in comparison['items']:
                        clean = re.sub(r'\b' + re.escape(item) + r'\b', '', clean, flags=re.IGNORECASE)
                    clean = re.sub(r'\s+', ' ', clean).strip()
                    intent = self.extract_intent(clean)
                    intent.date_range = date_range
                    intent.original_query = f"{clean} {comparison['items'][i]}"
                    intents.append(intent)
                return intents

            elif comparison['type'] == 'date_month':
                current_year = _dt.datetime.now().year
                year_match = re.search(r'\b(20\d{2})\b', query_lower)
                if year_match:
                    current_year = int(year_match.group(1))
                for i, month_num in enumerate(comparison['month_nums']):
                    clean = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', user_query, flags=re.IGNORECASE)
                    for mn in comparison['items']:
                        clean = re.sub(r'\b' + re.escape(mn) + r'\b', '', clean, flags=re.IGNORECASE)
                    if year_match:
                        clean = re.sub(r'\b' + str(current_year) + r'\b', '', clean)
                    clean = re.sub(r'\s+', ' ', clean).strip()
                    intent = self.extract_intent(clean)
                    intent.date_range = "custom_range"
                    intent.custom_dates = [{"month_num": month_num, "year": current_year}]
                    intent.original_query = f"{clean} {comparison['items'][i]} {current_year}"
                    intents.append(intent)
                return intents

            elif comparison['type'] == 'dimension':
                base = user_query.lower()
                for item in comparison['items']:
                    base = base.replace(item.lower(), '')
                base = re.sub(r'\s+(?:v/s|vs|versus|v\.s)\s+', ' ', base)
                base = re.sub(r'\s+', ' ', base).strip()
                multi_metrics = self._detect_multi_metric(user_query)
                for i, value in enumerate(comparison['items']):
                    intent = self.extract_intent(base)
                    if comparison['dimension'] in intent.dimensions:
                        intent.dimensions.remove(comparison['dimension'])
                    if not intent.filters:
                        intent.filters = {}
                    intent.filters[comparison['dimension']] = value
                    if multi_metrics:
                        intent.metrics = multi_metrics
                        intent.metric = multi_metrics[0]
                    label = comparison.get('labels', comparison['items'])[i]
                    intent.original_query = f"{base} - {label}"
                    intents.append(intent)
                return intents

            elif comparison['type'] == 'generic':
                for item in comparison['items']:
                    intents.append(self.extract_intent(item))
                return intents

        # ──────────────────────────────────────────────────────────────
        # STEP 1: Range queries are ALWAYS single queries
        # Patterns: "from X to Y", "from X till Y", "between X and Y"
        # ──────────────────────────────────────────────────────────────
        range_patterns = [
            r'\bfrom\b.{1,60}\bto\b',
            r'\bfrom\b.{1,60}\btill\b',
            r'\bfrom\b.{1,60}\buntil\b',
            r'\bbetween\b.{1,60}\band\b',
            r'\bthrough\b',
        ]
        if any(re.search(p, query_lower) for p in range_patterns):
            print("[MULTI-QUERY] Range query — single intent")
            return [self.extract_intent(user_query)]

        # ──────────────────────────────────────────────────────────────
        # STEP 2: AND/comma list of YEARS  e.g. "2023 and 2024"
        # ──────────────────────────────────────────────────────────────
        if re.search(r'\b20\d{2}\b(?:\s*,\s*|\s+and\s+)\b20\d{2}\b', query_lower):
            years = re.findall(r'\b(20\d{2})\b', query_lower)
            print(f"[MULTI-QUERY] Year list: {years}")
            intents = []
            for year in years:
                clean = re.sub(r'\b20\d{2}\b(?:\s*,\s*|\s+and\s+)\b20\d{2}\b', year, user_query, flags=re.IGNORECASE)
                intents.append(self.extract_intent(clean))
            return intents

        # ──────────────────────────────────────────────────────────────
        # STEP 3: AND/comma list of QUARTERS  e.g. "Q1 and Q2"
        # ──────────────────────────────────────────────────────────────
        if re.search(r'\bq([1-4])\b(?:\s*,\s*|\s+and\s+)\bq([1-4])\b', query_lower):
            quarters = re.findall(r'\bq([1-4])\b', query_lower)
            print(f"[MULTI-QUERY] Quarter list: Q{quarters}")
            intents = []
            for q in quarters:
                clean = re.sub(r'\bq[1-4]\b(?:\s*,\s*|\s+and\s+)\bq[1-4]\b', f'q{q}', user_query, flags=re.IGNORECASE)
                intents.append(self.extract_intent(clean))
            return intents

        # ──────────────────────────────────────────────────────────────
        # STEP 4: AND/comma list of MONTHS  e.g. "april and may"
        # Only when months are DIRECTLY joined by "and" or ","
        # ──────────────────────────────────────────────────────────────
        _months = ('january|february|march|april|may|june|july|august|september'
                   '|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec')
        if re.search(rf'\b({_months})\b(?:\s*,\s*|\s+and\s+)\b({_months})\b', query_lower):
            found = [m for m in _months.split('|') if re.search(r'\b' + m + r'\b', query_lower)]
            # deduplicate preserving order
            seen, unique = set(), []
            for m in found:
                if m not in seen:
                    seen.add(m); unique.append(m)
            if len(unique) >= 2:
                print(f"[MULTI-QUERY] Month list: {unique}")
                intents = []
                all_re = '|'.join(re.escape(m) for m in unique)
                for month in unique:
                    clean = re.sub(
                        rf'\b({all_re})\b(?:\s*,\s*|\s+and\s+)\b({all_re})\b',
                        month, user_query, flags=re.IGNORECASE
                    )
                    intents.append(self.extract_intent(clean))
                return intents

        # ──────────────────────────────────────────────────────────────
        # STEP 5: AND list of named DIMENSION VALUES
        # e.g. "wave city and wave estate", "broker and direct", "eden and amore"
        # Each entry: dimension_key -> list of (trigger_phrase, filter_value) tuples
        # filter_value is what gets passed to _build_single_value_filter (LIKE match)
        #
        # IMPORTANT: ALL matched trigger phrases are stripped from the base query
        # before passing to extract_intent. This prevents the LLM from misreading
        # project/product names as dimension keywords (e.g. "city" → dimension).
        # The filter is applied explicitly after intent extraction.
        # ──────────────────────────────────────────────────────────────
        if ' and ' in query_lower or ',' in query_lower:
            dim_values = {
                # Project / Sales Org  e.g. "wave city and wave estate"
                'sales_org_desc': [
                    ('wave city', 'Wave City'),
                    ('wave estate', 'Wave Estate'),
                    ('wmcc', 'WMCC'),
                ],
                # Product / Sales Group  e.g. "eden and amore", "fsi and hssc", "veridia and eligo"
                'sales_group_desc': [
                    ('eligo', 'ELIGO'),
                    ('eden', 'EDEN'),
                    ('amore', 'AMORE'),
                    ('livork', 'LIVORK'),
                    ('veridia 7', 'VERIDIA-7'),
                    ('veridia 6', 'VERIDIA-6'),
                    ('veridia 5', 'VERIDIA-5'),
                    ('veridia 4', 'VERIDIA-4'),
                    ('veridia 3', 'VERIDIA-3'),
                    ('veridia', 'VERIDIA'),
                    ('edenia', 'EDENIA'),
                    ('elegantia', 'ELEGANTIA'),
                    ('eminence', 'EMINENCE'),
                    ('irenia', 'IRENIA'),
                    ('trucia', 'TRUCIA'),
                    ('vasilia', 'VASILIA'),
                    ('mayfair park', 'MAYFAIR PARK'),
                    ('mayfair', 'MAYFAIR PARK'),
                    ('harmony greens', 'HARMONY GREENS'),
                    ('wave galleria', 'WAVE GALLERIA'),
                    ('galleria', 'WAVE GALLERIA'),
                    ('wave garden', 'WAVE GARDEN'),
                    ('wave floor 99', 'WAVE FLOOR 99'),
                    ('wave floor 85', 'WAVE FLOOR 85'),
                    ('wave floor', 'WAVE FLOOR'),
                    ('dream homes', 'DREAM HOMES'),
                    ('dream bazaar', 'DREAM BAZAAR'),
                    ('villas', 'VILLAS'),
                    ('armonia villa', 'ARMONIA VILLA'),
                    ('armonia', 'ARMONIA VILLA'),
                    ('business square', 'WAVE BUSSINESS SQUARE'),
                    ('wave estate gh2', 'WAVE ESTATE, GH2 PH2'),
                    ('executive floors', 'EXECUTIVE FLOORS'),
                    ('prime floors', 'PRIME FLOORS'),
                    ('new plots', 'NEW PLOTS'),
                    ('old plots', 'OLD PLOTS'),
                    ('commercial plots', 'COMMERCIAL PLOTS'),
                    ('residential plots', 'PLOTS-RES'),
                    ('fsi', 'FSI'),
                    ('hssc', 'HSSC'),
                    ('institutional', 'INSTITUTIONAL'),
                    ('metro mart', 'METRO MART'),
                    ('swamanorath', 'SWAMANORATH'),
                    ('wave bussiness square', 'WAVE BUSSINESS SQUARE'),
                    ('wbt 1', 'WBT 1'),
                    ('wbt a', 'WBT A'),
                    ('sco', 'SCO'),
                    ('comm booth', 'COMM BOOTH'),
                    ('ews p2', 'EWS_P2'),
                    ('lig p2', 'LIG_P2'),
                ],
                # Distribution Channel  e.g. "broker and direct"
                'dist_channel_desc': [
                    ('broker', 'Broker'),
                    ('direct', 'Direct'),
                    ('walk-in', 'Direct'),
                    ('walkin', 'Direct'),
                    ('channel partner', 'Broker'),
                    ('referral', 'Referral'),
                ],
                # Booking Type  e.g. "fresh and transfer"
                'booking_type': [
                    ('fresh', 'Fresh'),
                    ('transfer', 'Transfer'),
                    ('resale', 'Resale'),
                ],
            }
            for dim, value_tuples in dim_values.items():
                # Find which trigger phrases appear in the query
                matched = []
                seen_filter_vals = set()
                for trigger, filter_val in value_tuples:
                    if re.search(r'\b' + re.escape(trigger) + r'\b', query_lower):
                        if filter_val not in seen_filter_vals:
                            matched.append((trigger, filter_val))
                            seen_filter_vals.add(filter_val)

                if len(matched) >= 2:
                    print(f"[MULTI-QUERY] Dimension AND-list ({dim}): {[m[1] for m in matched]}")
                    intents = []

                    # Build a base query with ALL matched trigger phrases removed.
                    # This prevents the LLM from misinterpreting product/project names
                    # as dimension names (e.g. "city" in "wave city" → dimension error).
                    base_clean = query_lower
                    for trigger, _ in matched:
                        base_clean = re.sub(r'\b' + re.escape(trigger) + r'\b', '', base_clean)
                    base_clean = re.sub(r'\band\b', '', base_clean)
                    base_clean = re.sub(r'\bof\b\s*$|\bof\b\s+(?=and|,|$)', '', base_clean)
                    base_clean = re.sub(r'\s+', ' ', base_clean).strip()

                    # Extract intent once from the clean base (no product names in it)
                    base_intent = self.extract_intent(base_clean)

                    for trigger, filter_val in matched:
                        # Copy base intent and apply this specific filter
                        import copy
                        intent = copy.deepcopy(base_intent)
                        if not intent.filters:
                            intent.filters = {}
                        intent.filters[dim] = filter_val
                        intent.original_query = f"{base_clean} - {filter_val}"
                        intents.append(intent)
                    return intents

        # ──────────────────────────────────────────────────────────────
        # STEP 6: AND list of FLOORS  e.g. "10th floor and 8th floor"
        # Patterns: "Nth floor and Mth floor", "floor N and floor M"
        # ──────────────────────────────────────────────────────────────
        if 'floor' in query_lower and ' and ' in query_lower:
            floor_and_patterns = [
                r'(\d+)(?:st|nd|rd|th)?\s+floor\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor',  # "10th floor and 8th floor"
                r'floor\s+(\d+)\s+and\s+floor\s+(\d+)',                                   # "floor 10 and floor 8"
                r'(\d+)(?:st|nd|rd|th)?\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor',           # "10th and 8th floor"
            ]
            for fpat in floor_and_patterns:
                fm = re.search(fpat, query_lower)
                if fm:
                    floor1, floor2 = fm.group(1), fm.group(2)
                    print(f"[MULTI-QUERY] Floor list: [{floor1}, {floor2}]")
                    intents = []
                    # Build a base query with the floor list removed
                    base_clean = re.sub(fpat, '', query_lower, flags=re.IGNORECASE)
                    base_clean = re.sub(r'\s+', ' ', base_clean).strip()
                    import copy
                    base_intent = self.extract_intent(base_clean)
                    for floor_num in [floor1, floor2]:
                        intent = copy.deepcopy(base_intent)
                        if not intent.filters:
                            intent.filters = {}
                        intent.filters['floor'] = floor_num
                        intent.original_query = f"{base_clean} - floor {floor_num}"
                        intents.append(intent)
                    return intents

        # ──────────────────────────────────────────────────────────────
        # DEFAULT: single query
        # ──────────────────────────────────────────────────────────────
        return [self.extract_intent(user_query)]


    def _detect_multi_date_query(self, query: str) -> List[Dict]:
        """
        Detect if query requests multiple separate time periods.
        
        Examples:
            "total sales in 2024 and 2025" → [{"year": 2024}, {"year": 2025}]
            "sales in Q1 and Q2" → [{"quarter": 1}, {"quarter": 2}]
            "sales in april and may" → [{"month": 4}, {"month": 5}]
            
        
        Returns:
            List of date dictionaries, or empty list if single period
        """
        query_lower = query.lower()
        
        # Pattern 1: Multiple years (e.g., "2024 and 2025", "in 2024, 2025")
        year_pattern = r'\b(20\d{2})\b'
        years = re.findall(year_pattern, query_lower)
        if len(years) > 1:
            return [{"year": int(y)} for y in years]
        
        # Pattern 2: Multiple quarters (e.g., "Q1 and Q2", "Q1, Q2, Q3")
        quarter_pattern = r'\bq([1-4])\b'
        quarters = re.findall(quarter_pattern, query_lower)
        if len(quarters) > 1:
            return [{"quarter": int(q)} for q in quarters]
        
        # Pattern 3: Multiple months (e.g., "april and may", "jan, feb, mar")
        month_names = {
            'january': 1, 'jan': 1,
            'february': 2, 'feb': 2,
            'march': 3, 'mar': 3,
            'april': 4, 'apr': 4,
            'may': 5,
            'june': 6, 'jun': 6,
            'july': 7, 'jul': 7,
            'august': 8, 'aug': 8,
            'september': 9, 'sep': 9, 'sept': 9,
            'october': 10, 'oct': 10,
            'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
        
        found_months = []
        for month_name, month_num in month_names.items():
            if month_name in query_lower:
                found_months.append(month_num)
        
        # Remove duplicates while preserving order
        found_months = list(dict.fromkeys(found_months))
        
        if len(found_months) > 1:
            return [{"month_num": m} for m in found_months]
        
        return []
    

    def _detect_comparison_query(self, user_query: str) -> Optional[Dict]:
        """
        Detect if query is a comparison using "vs", "v/s", or "versus".
        
        Examples:
            "2024 vs 2025" → {"type": "date", "items": ["2024", "2025"]}
            "broker vs direct" → {"type": "dimension", "items": ["broker", "direct"]}
            "Q1 vs Q2" → {"type": "date", "items": ["Q1", "Q2"]}
        
        Returns:
            Dictionary with comparison type and items, or None if not a comparison
        """
        import re
        
        query_lower = user_query.lower()
        
        # Check for comparison keywords
        comparison_patterns = [
            r'\bv/s\b',
            r'\bvs\b',
            r'\bversus\b',
            r'\bv\.s\b',
            r'\bcompare\b'
        ]
        
        has_comparison = False
        for pattern in comparison_patterns:
            if re.search(pattern, query_lower):
                has_comparison = True
                break
        
        if not has_comparison:
            return None
        
        print(f"[COMPARISON] Detected comparison query: {user_query}")
        
        # ========================================
        # Type 1: DATE COMPARISONS
        # ========================================
        
        # Years (2024 vs 2025)
        year_pattern = r'\b(20\d{2})\b'
        years = re.findall(year_pattern, query_lower)
        if len(years) == 2:
            print(f"[COMPARISON] Year comparison: {years}")
            return {
                "type": "date_year",
                "items": years,
                "date_ranges": [f"fy_{y}" for y in years]
            }
        
        # Quarters (Q1 vs Q2)
        quarter_pattern = r'\bq([1-4])\b'
        quarters = re.findall(quarter_pattern, query_lower)
        if len(quarters) == 2:
            print(f"[COMPARISON] Quarter comparison: Q{quarters}")
            return {
                "type": "date_quarter",
                "items": [f"Q{q}" for q in quarters],
                "date_ranges": [f"q{q}" for q in quarters]
            }
        
        # Months (april vs may)
        month_map = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
            'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
            'may': 5, 'june': 6, 'jun': 6,
            'july': 7, 'jul': 7, 'august': 8, 'aug': 8,
            'september': 9, 'sep': 9, 'sept': 9,
            'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
        
        found_months = []
        for month_name, month_num in month_map.items():
            if month_name in query_lower:
                found_months.append((month_name, month_num))
        
        if len(found_months) == 2:
            print(f"[COMPARISON] Month comparison: {[m[0] for m in found_months]}")
            return {
                "type": "date_month",
                "items": [m[0] for m in found_months],
                "month_nums": [m[1] for m in found_months]
            }
        
        # ========================================
        # Type 2: DIMENSION VALUE COMPARISONS
        # ========================================
        
        # Split query by comparison keyword
        # split_pattern = r'\s+(?:v/s|vs|versus|v\.s)\s+'
        # parts = re.split(split_pattern, query_lower, flags=re.IGNORECASE)
        
        # if len(parts) == 2:
        #     left = parts[0].strip()
        #     right = parts[1].strip()
            
        #     # Common dimension value patterns
        #     dimension_patterns = {
        #         # Channel
        #         'dist_channel_desc': {
        #             'broker': ['broker', 'channel partner', 'agent'],
        #             'direct': ['direct', 'walk-in', 'walkin', 'self']
        #         },
        #         # Division
        #         'division_desc': {
        #             'residential': ['residential', 'housing', 'apartment'],
        #             'commercial': ['commercial', 'office', 'shop', 'retail']
        #         },
        #         # Product
        #         'sales_group_desc': {
        #             'wave city': ['wave city', 'wavecity'],
        #             'wave estate': ['wave estate', 'waveestate'],
        #             'amore': ['amore'],
        #             'livork': ['livork']
        #         },
        #         # Booking Type
        #         'booking_type': {
        #             'fresh': ['fresh', 'new booking'],
        #             'transfer': ['transfer', 'transferred']
        #         }
        #     }
            
        #     # Check which dimension this comparison belongs to
        #     for dim_name, value_patterns in dimension_patterns.items():
        #         left_match = None
        #         right_match = None
                
        #         for value_key, patterns in value_patterns.items():
        #             for pattern in patterns:
        #                 if pattern in left:
        #                     left_match = value_key
        #                 if pattern in right:
        #                     right_match = value_key
                
        #         if left_match and right_match:
        #             print(f"[COMPARISON] Dimension comparison: {dim_name} - {left_match} vs {right_match}")
        #             return {
        #                 "type": "dimension",
        #                 "dimension": dim_name,
        #                 "items": [left_match, right_match]
        #             }


        # ========================================
        # Type 2: DIMENSION VALUE COMPARISONS
        # ========================================
        
        # Split query by comparison keyword
        split_pattern = r'\s+(?:v/s|vs|versus|v\.s)\s+'
        parts = re.split(split_pattern, query_lower, flags=re.IGNORECASE)
        
        if len(parts) == 2:
            left = parts[0].strip()
            right = parts[1].strip()
            
            # Common dimension value patterns
            dimension_patterns = {
                # Channel - MOST IMPORTANT
                'dist_channel_desc': {
                    'patterns': {
                        'broker': ['broker', 'channel partner', 'agent', 'brokers'],
                        'direct': ['direct', 'walk-in', 'walkin', 'self', 'walk in'],
                        'Referral': ['referral', 'referred', 'reference', 'ref'],
                    },
                    'priority': 1  # Check this first
                },
                # Division
                'division_desc': {
                    'patterns': {
                        'residential': ['residential', 'housing', 'apartment', 'resi'],
                        'commercial': ['commercial', 'office', 'shop', 'retail', 'comm']
                    },
                    'priority': 2
                },
                # Product
                'sales_group_desc': {
                    'patterns': {
                        'Wave City': ['wave city', 'wavecity'],
                        'Wave Estate': ['wave estate', 'waveestate'],
                        'Amore': ['amore'],
                        'Livork': ['livork']
                    },
                    'priority': 3
                },
                # Booking Type
                'booking_type': {
                    'patterns': {
                        'Fresh': ['fresh', 'new booking', 'new'],
                        'Transfer': ['transfer', 'transferred']
                    },
                    'priority': 4
                }
            }
            
            # Sort by priority
            sorted_patterns = sorted(
                dimension_patterns.items(), 
                key=lambda x: x[1].get('priority', 999)
            )
            
            # Check which dimension this comparison belongs to
            for dim_name, dim_config in sorted_patterns:
                left_match = None
                right_match = None
                
                value_patterns = dim_config['patterns']
                
                for value_key, patterns in value_patterns.items():
                    for pattern in patterns:
                        if pattern in left:
                            left_match = value_key
                        if pattern in right:
                            right_match = value_key
                
                if left_match and right_match:
                    print(f"[COMPARISON] Dimension comparison: {dim_name} - '{left_match}' vs '{right_match}'")
                    return {
                        "type": "dimension",
                        "dimension": dim_name,
                        "items": [left_match, right_match],
                        "labels": [left_match, right_match]
                    }
        
        # ========================================
        # Type 3: GENERIC COMPARISON (fallback)
        # ========================================
        
        # If we detected comparison keywords but couldn't classify the type,
        # treat it as a generic multi-query comparison
        if len(parts) == 2:
            print(f"[COMPARISON] Generic comparison: {parts}")
            return {
                "type": "generic",
                "items": parts
            }
        
        return None
    


    def _detect_cancellation_query(self, user_query: str) -> Optional[SemanticIntent]:
        """
        Detect cancellation-related queries and build the correct intent.

        Always applies:
        - metric: cancelled_sales
        - mandatory filter: Customer_Type = 'cancelled'  (from metric definition)
        - dimension: cancellation_reason (Description column) by default

        Also supports filters by:
        - payer_name, sold_to_name, sales_order, sales_group_desc, project, product
        """
        query_lower = user_query.lower()

        cancellation_triggers = [
            "cancellation reason", "reason for cancellation", "cancel reason",
            "why cancelled", "why was cancelled", "cancelled reason",
            "cancellation reason wise", "reason of cancellation",
        ]

        if not any(trigger in query_lower for trigger in cancellation_triggers):
            return None

        print(f"[CANCELLATION] Detected cancellation reason query")

        # Always group by cancellation reason (Description column)
        dimensions = ["cancellation_reason"]

        # Check for additional dimension filters
        filters = {}

        # By payer
        if any(kw in query_lower for kw in ["payer", "paying customer"]):
            dimensions.append("payer_name")

        # By sold to / customer name
        if any(kw in query_lower for kw in ["sold to", "customer", "buyer"]):
            dimensions.append("sold_to_name")

        # By sales order number — extract it
        order_match = re.search(r'\b(\d{7,12})\b', query_lower)
        if order_match:
            filters["sales_order"] = order_match.group(1)

        # By project (wave city / wave estate / wmcc) — these are FILTER values, not dimensions
        org_filter_map = {"wave city": "Wave City", "wave estate": "Wave Estate", "wmcc": "WMCC"}
        for kw, val in org_filter_map.items():
            if kw in query_lower:
                filters["sales_org_desc"] = val

        # By product / sales group
        # "product wise" / "product wise breakdown" → add sales_group_desc as dimension
        if any(kw in query_lower for kw in ["product wise", "product-wise", "productwise",
                                             "product breakdown", "product split"]):
            dimensions.append("sales_group_desc")
        else:
            # Specific product name → filter value
            product_map = {
                "eden": "EDEN", "amore": "AMORE", "livork": "LIVORK",
                "veridia": "VERIDIA", "eligo": "ELIGO", "edenia": "EDENIA",
                "wave floor": "WAVE FLOOR", "dream homes": "DREAM HOMES",
                "harmony greens": "HARMONY GREENS", "galleria": "WAVE GALLERIA",
            }
            for kw, val in product_map.items():
                if kw in query_lower:
                    filters["sales_group_desc"] = val
                    break

        # By tower
        if "tower" in query_lower:
            dimensions.append("tower")

        # By broker
        if "broker" in query_lower:
            dimensions.append("broker_name")

        # Extract date
        custom_dates, date_range = self._extract_custom_dates_enhanced(user_query)

        intent = SemanticIntent(
            metric="cancelled_sales",
            dimensions=dimensions,
            date_range=date_range or "current_financial_year",
            custom_dates=custom_dates or [],
            filters=filters if filters else None,
            original_query=user_query,
        )

        return intent



    # =====================================================
    # CUSTOM DATE EXTRACTION (NEW!)
    # =====================================================
    
    def _extract_custom_dates_enhanced(self, query: str) -> Tuple[List[Dict], str]:
        """
        Extract custom dates from natural language query with proper Financial Year (FY) handling.
        FY runs from April to March (Apr 2025 - Mar 2026 is current)
        
        Handles:
        1. Date ranges: "from 23 jan to 5 feb", "from 20 jan 2022 to 2 feb 2023"
        2. Specific dates: "20 jan", "23 march 2025"
        3. From patterns: "from may" (may to end of current FY = March)
        4. From-till patterns: "from may till 15 jan", "from sep till 15 feb"
        5. From-to patterns: "from sep to feb", "from april to sep 2024"
        6. Till patterns: "till 13 dec" (april to 13 dec)
        7. Month ranges: "april to june"
        8. Quarters: "q1", "q2", "q3", "q4", "q1 2024", "q4 2023"
        
        Returns: (custom_dates_list, date_range_type)
        """
        query_lower = query.lower().strip()
        today = datetime.datetime.now()
        current_year = today.year
        current_month = today.month
        
        print(f"[EXTRACT_ENHANCED] Input query: '{query}' (lowercased: '{query_lower}')")
        
        # *** CRITICAL: Determine correct Financial Year (Apr-Mar) ***
        # Current date: Feb 9, 2026 → FY is April 2025 - March 2026
        # Logic: If month is Apr-Dec (>=4), FY starts in current year
        #        If month is Jan-Mar (<4), FY started in previous year
        if current_month >= 4:
            fy_start_year = current_year
            fy_end_year = current_year + 1
        else:
            # We're in Jan-Mar, so FY started last year
            fy_start_year = current_year - 1
            fy_end_year = current_year
        
        print(f"[FY_CALC] Today: {today.date()}, Month: {current_month}, FY: {fy_start_year}-{fy_end_year}")
        
        def get_fy_year_for_month(month_num: int, explicit_year: int = None) -> int:
            """
            Get the correct financial year for a given month.
            If explicit_year provided, use it. Otherwise, infer from FY context.
            
            Examples (assuming current FY is 2025-2026, i.e., Apr 2025 - Mar 2026):
            - April-Dec: return 2025 (fy_start_year)
            - Jan-Mar: return 2026 (fy_end_year)
            """
            if explicit_year:
                return explicit_year
            
            # Months Apr(4) through Dec(12) belong to fy_start_year
            # Months Jan(1) through Mar(3) belong to fy_end_year
            if month_num >= 4:
                return fy_start_year
            else:
                return fy_end_year
        
        # ========================================
        # PATTERN 0 (PRIORITY): Financial Quarter (e.g., "q1", "q2", "q3", "q4", "q4 2024", "q1_2025")
        # Check FIRST to avoid conflicts with month patterns
        # FY Quarters: Q1=Apr-Jun(4-6), Q2=Jul-Sep(7-9), Q3=Oct-Dec(10-12), Q4=Jan-Mar(1-3)
        # ========================================
        # PRE-PATTERN 0: "quarter N YYYY" / "quarter N" (written-out quarter)
        # Examples: "quarter 3 2023", "quarter 3 of 2023", "3rd quarter 2022"
        # Covers: "quarter 1", "quarter 2", "1st quarter", "2nd quarter" etc.
        written_quarter_pattern = r'(?:(?:1st|2nd|3rd|4th|first|second|third|fourth)\s+quarter|quarter\s+(?:1|2|3|4|one|two|three|four))(?:\s+(?:of\s+)?(\d{4}))?'
        written_q_match = re.search(written_quarter_pattern, query_lower)
        if written_q_match:
            # Map words to quarter numbers
            q_word_map = {
                "1st": 1, "first": 1, "one": 1, "1": 1,
                "2nd": 2, "second": 2, "two": 2, "2": 2,
                "3rd": 3, "third": 3, "three": 3, "3": 3,
                "4th": 4, "fourth": 4, "four": 4, "4": 4,
            }
            raw = written_q_match.group()
            # Extract the number word
            q_num = None
            for token in re.split(r'\s+', raw):
                if token in q_word_map:
                    q_num = q_word_map[token]
                    break
            year_explicit = int(written_q_match.group(1)) if written_q_match.group(1) else None

            if q_num:
                quarter_months = {1: (4, 6), 2: (7, 9), 3: (10, 12), 4: (1, 3)}
                start_month, end_month = quarter_months[q_num]
                if year_explicit:
                    if q_num in (1, 2, 3):
                        start_year = end_year = year_explicit
                    else:
                        start_year = end_year = year_explicit + 1
                else:
                    if q_num in (1, 2, 3):
                        start_year = end_year = fy_start_year
                    else:
                        start_year = end_year = fy_end_year

                print(f"[PRE-PATTERN 0 - WRITTEN QUARTER] Q{q_num} year={year_explicit or 'current FY'}: {start_month}/{start_year} to {end_month}/{end_year}")
                return [
                    {"month_num": start_month, "year": start_year},
                    {"month_num": end_month, "year": end_year}
                ], "custom_range"

        # ========================================
        # Try pattern with optional space or underscore + year
        quarter_pattern = r'\bq([1-4])(?:\s+(?:of\s+)?(\d{4}))?\b'
        print(f"[PATTERN 0] Searching with regex: {quarter_pattern}")
        quarter_match = re.search(quarter_pattern, query_lower)
        
        if quarter_match:
            q_num = int(quarter_match.group(1))  # 1, 2, 3, or 4
            year_explicit = int(quarter_match.group(2)) if quarter_match.group(2) else None
            
            print(f"[PATTERN 0 - MATCH] YES! Matched: q{q_num}, year_explicit: {year_explicit}, full match: '{quarter_match.group()}'")
            
            # Map quarter to months (based on Financial Year Apr-Mar)
            quarter_months = {
                1: (4, 5, 6),    # Q1: Apr, May, Jun
                2: (7, 8, 9),    # Q2: Jul, Aug, Sep
                3: (10, 11, 12), # Q3: Oct, Nov, Dec
                4: (1, 2, 3),    # Q4: Jan, Feb, Mar
            }
            
            start_month, mid_month, end_month = quarter_months[q_num]
            
            # Determine year(s) for the quarter
            if year_explicit:
                # User specified explicit year (e.g., "q1 2024", "q4 2023")
                # The year refers to the Financial Year (FY)
                # Q1-Q3 of FY 2024 = Apr-Dec 2024
                # Q4 of FY 2024 = Jan-Mar 2025
                if q_num in (1, 2, 3):
                    # Q1-Q3: use exact year (Apr-Dec of that year)
                    start_year = year_explicit
                    end_year = year_explicit
                else:
                    # Q4: Jan-Mar of the NEXT year (Q4 of FY 2024 = Jan-Mar 2025)
                    start_year = year_explicit + 1
                    end_year = year_explicit + 1
            else:
                # No explicit year - use Financial Year context
                # Current FY: Apr fy_start_year - Mar fy_end_year
                if q_num in (1, 2, 3):
                    # Q1-Q3 (Apr-Dec): use FY start year
                    start_year = fy_start_year
                    end_year = fy_start_year
                else:
                    # Q4 (Jan-Mar): use FY end year
                    start_year = fy_end_year
                    end_year = fy_end_year
            
            print(f"[PATTERN 0 - QUARTER] q{q_num}: months {start_month}-{end_month}, year {start_year}-{end_year}")
            print(f"[PATTERN 0 - DATES] Start: month={start_month}/year={start_year}, End: month={end_month}/year={end_year}")
            return [
                {"month_num": start_month, "year": start_year},
                {"month_num": end_month, "year": end_year}
            ], "custom_range"
        else:
            print(f"[PATTERN 0 - NO MATCH] Quarter pattern did not match")
        
        # PATTERN 1: "from DATE to DATE" (e.g., "from 23 jan to 5 feb", "from 20 jan 2022 to 2 feb 2023")
        from_date_to_date = r'from\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+to\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        from_date_to_date_match = re.search(from_date_to_date, query_lower)
        
        if from_date_to_date_match:
            day1 = int(from_date_to_date_match.group(1))
            month1_str = from_date_to_date_match.group(2)
            month1_num = self.MONTH_NAMES.get(month1_str, 1)
            year1_explicit = int(from_date_to_date_match.group(3)) if from_date_to_date_match.group(3) else None
            
            day2 = int(from_date_to_date_match.group(4))
            month2_str = from_date_to_date_match.group(5)
            month2_num = self.MONTH_NAMES.get(month2_str, 12)
            year2_explicit = int(from_date_to_date_match.group(6)) if from_date_to_date_match.group(6) else None
            
            # Intelligent year assignment
            if year1_explicit and year2_explicit:
                year1 = year1_explicit
                year2 = year2_explicit
            elif year1_explicit and not year2_explicit:
                year1 = year1_explicit
                if month2_num < month1_num:
                    year2 = year1 + 1
                else:
                    year2 = year1
            elif year2_explicit and not year1_explicit:
                year2 = year2_explicit
                if month1_num <= month2_num:
                    year1 = year2
                else:
                    year1 = year2 - 1
            else:
                year1 = get_fy_year_for_month(month1_num)
                year2 = get_fy_year_for_month(month2_num)
                if month2_num < month1_num:
                    year2 = year1 + 1
            
            print(f"[PATTERN 1] from date to date: {day1}/{month1_num}/{year1} to {day2}/{month2_num}/{year2}")
            return [
                {"day": day1, "month_num": month1_num, "year": year1},
                {"day": day2, "month_num": month2_num, "year": year2}
            ], "custom_range"
        
        # PATTERN 2: "from MONTH till DATE" (e.g., "from may till 15 jan", "from sep till 15 feb")
        from_month_till_date = r'from\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+till\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        from_month_till_date_match = re.search(from_month_till_date, query_lower)
        
        if from_month_till_date_match:
            month1_str = from_month_till_date_match.group(1)
            month1_num = self.MONTH_NAMES.get(month1_str, 1)
            year1_explicit = int(from_month_till_date_match.group(2)) if from_month_till_date_match.group(2) else None
            
            day2 = int(from_month_till_date_match.group(3))
            month2_str = from_month_till_date_match.group(4)
            month2_num = self.MONTH_NAMES.get(month2_str, 12)
            year2_explicit = int(from_month_till_date_match.group(5)) if from_month_till_date_match.group(5) else None
            
            # Intelligent year assignment
            if year1_explicit and year2_explicit:
                year1 = year1_explicit
                year2 = year2_explicit
            elif year1_explicit and not year2_explicit:
                year1 = year1_explicit
                if month2_num < month1_num:
                    year2 = year1 + 1
                else:
                    year2 = year1
            elif year2_explicit and not year1_explicit:
                year2 = year2_explicit
                if month1_num <= month2_num:
                    year1 = year2
                else:
                    year1 = year2 - 1
            else:
                year1 = get_fy_year_for_month(month1_num)
                year2 = get_fy_year_for_month(month2_num)
                if not year2_explicit and month2_num < month1_num:
                    year2 = year1 + 1
            
            print(f"[PATTERN 2] from month till date: month {month1_num}/{year1} till {day2}/{month2_num}/{year2}")
            return [
                {"month_num": month1_num, "year": year1},
                {"day": day2, "month_num": month2_num, "year": year2}
            ], "custom_range"
        
        # PATTERN 3: "from MONTH to MONTH" (e.g., "from sep to feb", "from april to sep 2024")
        from_month_to_month = r'from\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+to\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        from_month_to_month_match = re.search(from_month_to_month, query_lower)
        
        if from_month_to_month_match:
            month1_str = from_month_to_month_match.group(1)
            month1_num = self.MONTH_NAMES.get(month1_str, 1)
            year1_explicit = int(from_month_to_month_match.group(2)) if from_month_to_month_match.group(2) else None
            
            month2_str = from_month_to_month_match.group(3)
            month2_num = self.MONTH_NAMES.get(month2_str, 12)
            year2_explicit = int(from_month_to_month_match.group(4)) if from_month_to_month_match.group(4) else None
            
            # Intelligent year assignment logic:
            # Priority: explicit years > inferred from one year > FY context
            if year1_explicit and year2_explicit:
                # Both years explicit
                year1 = year1_explicit
                year2 = year2_explicit
            elif year1_explicit and not year2_explicit:
                # Only year1 explicit - infer year2 from month order
                year1 = year1_explicit
                if month2_num < month1_num:
                    # Crosses year boundary (e.g., "from sep 2024 to feb" -> 2024 to 2025)
                    year2 = year1 + 1
                else:
                    year2 = year1
            elif year2_explicit and not year1_explicit:
                # Only year2 explicit - infer year1 from month order
                year2 = year2_explicit
                if month1_num <= month2_num:
                    # Same calendar order (e.g., "from april to sep 2024" -> both 2024)
                    year1 = year2
                else:
                    # Crosses year boundary (e.g., "from sep to april 2024" -> sep 2023 to apr 2024)
                    year1 = year2 - 1
            else:
                # No explicit years - use FY context
                year1 = get_fy_year_for_month(month1_num)
                if month2_num < month1_num:
                    # Crosses FY boundary (e.g., "from oct to feb" = Oct 2025 to Feb 2026)
                    year2 = year1 + 1
                else:
                    year2 = year1
            
            print(f"[PATTERN 3] from month to month: {month1_num}/{year1} to {month2_num}/{year2}")
            return [
                {"month_num": month1_num, "year": year1},
                {"month_num": month2_num, "year": year2}
            ], "custom_range"
        
        # PATTERN 5: "till DATE" or "until DATE" pattern (from start of FY to specified date)
        # MUST come before Pattern 4 (single date) to avoid "13 oct" being grabbed as a lone date
        till_date_pattern = r'(?:till|until)\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        till_date_match = re.search(till_date_pattern, query_lower)
        
        if till_date_match:
            day = int(till_date_match.group(1))
            month_str = till_date_match.group(2)
            month_num = self.MONTH_NAMES.get(month_str, 12)
            year_explicit = int(till_date_match.group(3)) if till_date_match.group(3) else None
            year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
            # Start year: if ending month is Jan-Mar (2026), start is Apr 2025
            start_year = year if month_num >= 4 else year - 1
            
            print(f"[PATTERN 5] till date: Apr/{start_year} till {day}/{month_num}/{year}")
            return [
                {"month_num": 4, "year": start_year},
                {"day": day, "month_num": month_num, "year": year}
            ], "custom_range"
        
        # PATTERN 6A: "from DAY MONTH" pattern (from specific date to end of current FY)
        # Examples: "from 15 sep" → Sep 15 to Mar 31 (end of current FY)
        #           "from 15 sep 2022" → Sep 15 2022 to Mar 31 2023
        # MUST come before Pattern 4 (single date) to avoid "15 sep" being grabbed as a lone date
        from_day_month_pattern = r'\bfrom\s+(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
        from_day_match = re.search(from_day_month_pattern, query_lower)

        if from_day_match:
            day = int(from_day_match.group(1))
            month_str = from_day_match.group(2)
            month_num = self.MONTH_NAMES.get(month_str, 4)
            year_explicit = int(from_day_match.group(3)) if from_day_match.group(3) else None
            year = year_explicit if year_explicit else get_fy_year_for_month(month_num)

            # End: March 31 of the FY that contains the start month
            end_year = year + 1 if month_num >= 4 else year

            print(f"[PATTERN 6A - FROM DAY MONTH] {day}/{month_num}/{year} to 31/3/{end_year}")
            return [
                {"day": day, "month_num": month_num, "year": year},
                {"day": 31, "month_num": 3, "year": end_year}
            ], "custom_range"

        # PATTERN 4: Single specific date (e.g., "20 jan", "5 february 2025", "20 jan 2024", "on 15 sep")
        # Placed AFTER till/from open-range patterns so those take priority.
        day_month_pattern = r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        day_month_match = re.search(day_month_pattern, query_lower)
        
        if day_month_match:
            day = int(day_month_match.group(1))
            month_str = day_month_match.group(2)
            month_num = self.MONTH_NAMES.get(month_str, 1)
            year = int(day_month_match.group(3)) if day_month_match.group(3) else get_fy_year_for_month(month_num)
            
            print(f"[PATTERN 4] single date: {day}/{month_num}/{year}")
            return [{"day": day, "month_num": month_num, "year": year}], "custom_date"

        # PATTERN 6: "from MONTH" pattern (from month to end of current FY)
        # Examples: "from may" (May 2025 to Mar 2026), "from june 2024" (Jun 2024 to Mar 2025)
        # This pattern explicitly looks for the word "from" followed by a month
        from_month_pattern = r'\bfrom\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
        from_match = re.search(from_month_pattern, query_lower)
        
        if from_match:
            month_str = from_match.group(1)
            month_num = self.MONTH_NAMES.get(month_str, 4)
            year_explicit = int(from_match.group(2)) if from_match.group(2) else None
            year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
            # End year: if starting month is Apr-Dec (2025), end is Mar 2026  
            end_year = year + 1 if month_num >= 4 else year
            
            print(f"[PATTERN 6 - FROM MONTH] Matched: '{month_str}' (month={month_num}), Start year: {year}, End year: {end_year}")
            
            return [
                {"month_num": month_num, "year": year},
                {"month_num": 3, "year": end_year}
            ], "custom_range"
        
        # PATTERN 7: "till MONTH" or "until MONTH" pattern (from start of FY to end of that month)
        till_month_pattern = r'(?:till|until)\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        till_month_match = re.search(till_month_pattern, query_lower)
        
        if till_month_match:
            month_str = till_month_match.group(1)
            month_num = self.MONTH_NAMES.get(month_str, 12)
            year_explicit = int(till_month_match.group(2)) if till_month_match.group(2) else None
            year = year_explicit if year_explicit else get_fy_year_for_month(month_num)
            
            # Start year: if ending month is Jan-Mar, start is Apr of previous year
            start_year = year if month_num >= 4 else year - 1
            
            end_date = {"month_num": month_num, "year": year}
            
            return [
                {"month_num": 4, "year": start_year},
                end_date
            ], "custom_range"
        
        # PATTERN 8: Month-to-month range without "to" (e.g., "april - june", "sep - november")
        month_dash_month = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s*-\s*(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        month_dash_match = re.search(month_dash_month, query_lower)
        
        if month_dash_match:
            month1_str = month_dash_match.group(1)
            month1_num = self.MONTH_NAMES.get(month1_str, 1)
            year1_explicit = int(month_dash_match.group(2)) if month_dash_match.group(2) else None
            
            month2_str = month_dash_match.group(3)
            month2_num = self.MONTH_NAMES.get(month2_str, 12)
            year2_explicit = int(month_dash_match.group(4)) if month_dash_match.group(4) else None
            
            # Intelligent year assignment logic
            if year1_explicit and year2_explicit:
                year1 = year1_explicit
                year2 = year2_explicit
            elif year1_explicit and not year2_explicit:
                year1 = year1_explicit
                if month2_num < month1_num:
                    year2 = year1 + 1
                else:
                    year2 = year1
            elif year2_explicit and not year1_explicit:
                year2 = year2_explicit
                if month1_num <= month2_num:
                    year1 = year2
                else:
                    year1 = year2 - 1
            else:
                year1 = get_fy_year_for_month(month1_num)
                if month2_num < month1_num:
                    year2 = year1 + 1
                else:
                    year2 = year1
            
            return [
                {"month_num": month1_num, "year": year1},
                {"month_num": month2_num, "year": year2}
            ], "custom_range"
        
        # PATTERN 9: Month-to-month with "to" (e.g., "april to june", "april to sep 2024")
        month_to_month = r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\s+(?:to)\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?'
        month_to_match = re.search(month_to_month, query_lower)
        
        if month_to_match:
            month1_str = month_to_match.group(1)
            month1_num = self.MONTH_NAMES.get(month1_str, 1)
            year1_explicit = int(month_to_match.group(2)) if month_to_match.group(2) else None
            
            month2_str = month_to_match.group(3)
            month2_num = self.MONTH_NAMES.get(month2_str, 12)
            year2_explicit = int(month_to_match.group(4)) if month_to_match.group(4) else None
            
            # Intelligent year assignment logic
            if year1_explicit and year2_explicit:
                year1 = year1_explicit
                year2 = year2_explicit
            elif year1_explicit and not year2_explicit:
                year1 = year1_explicit
                if month2_num < month1_num:
                    year2 = year1 + 1
                else:
                    year2 = year1
            elif year2_explicit and not year1_explicit:
                year2 = year2_explicit
                if month1_num <= month2_num:
                    year1 = year2
                else:
                    year1 = year2 - 1
            else:
                year1 = get_fy_year_for_month(month1_num)
                if month2_num < month1_num:
                    year2 = year1 + 1
                else:
                    year2 = year1
            
            return [
                {"month_num": month1_num, "year": year1},
                {"month_num": month2_num, "year": year2}
            ], "custom_range"
        
        # PATTERN 10: Single month (e.g., "april 2024", "june")
        single_month_pattern = r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)(?:\s+(\d{4}))?\b'
        single_month_match = re.search(single_month_pattern, query_lower)
        
        if single_month_match:
            month_str = single_month_match.group(1)
            month_num = self.MONTH_NAMES.get(month_str, 1)
            year = int(single_month_match.group(2)) if single_month_match.group(2) else get_fy_year_for_month(month_num)
            
            return [{"month_num": month_num, "year": year}], "custom_range"
        
        return [], "current_financial_year"



    # =====================================================
    # PUBLIC ENTRY POINT
    # =====================================================
    
           
    def _detect_possession_metric(self, user_query: str) -> Optional[str]:
        """
        Detect if query is about possession status/journey.
    
    Possession journey stages:
    1. Sale Done (Document_Date exists)
    2. Agreement Given (Agreement_Date exists)
    3. Possession Given (Possession_Given_On exists)
    
    Args:
        user_query: User's natural language query
    
    Returns:
        Possession metric name or None
    
    Examples:
        "how many possessions are pending" → "possession_pending_count"
        "possession given last month" → "possession_given_count"
        "tower wise possession pending" → "possession_pending_count"
        "possession status breakdown" → "possession_status_breakdown"
        "average days to possession" → "average_days_to_possession"
    """
        query_lower = user_query.lower()
        
        # ────────────────────────────────────────────────────────
        # Check if this is a possession-related query
        # ────────────────────────────────────────────────────────
        possession_keywords = ["possession", "possessions", "handover", "handed over"]
        agreement_keywords = ["agreement", "agreements"]
        
        has_possession = any(kw in query_lower for kw in possession_keywords)
        has_agreement = any(kw in query_lower for kw in agreement_keywords)
        
        if not has_possession and not has_agreement:
            return None
        
        # ────────────────────────────────────────────────────────
        # Possession-related metrics
        # ────────────────────────────────────────────────────────
        if has_possession:
            # Priority 1: Status breakdown
            if any(kw in query_lower for kw in ["status", "journey", "breakdown", "stages", "wise breakdown"]):
                return "possession_status_breakdown"
            
            # Priority 2: Completion rate
            if any(kw in query_lower for kw in ["rate", "%", "percentage", "completion rate"]):
                return "possession_completion_rate"
            
            # Priority 3: TAT/Average days
            if any(kw in query_lower for kw in ["average", "avg", "days", "time", "tat", "turnaround", "how long"]):
                if "agreement" in query_lower:
                    return "average_days_to_agreement"
                elif "total" in query_lower or "cycle" in query_lower:
                    return "average_total_cycle_time"
                else:
                    return "average_days_to_possession"
            
            # Priority 4: Value metrics
            if any(kw in query_lower for kw in ["value", "revenue", "amount", "worth"]):
                if any(kw in query_lower for kw in ["pending", "not given", "yet to"]):
                    return "possession_pending_value"
                else:
                    return "possession_given_value"
            
            # Priority 5: Pending possession
            if any(kw in query_lower for kw in ["pending", "not given", "not handed", "yet to", "awaiting"]):
                return "possession_pending_count"
            
            # Priority 6: Given possession
            if any(kw in query_lower for kw in ["given", "handed over", "completed", "done", "delivered"]):
                return "possession_given_count"
            
            # Default possession metric
            return "possession_pending_count"
    
        # ────────────────────────────────────────────────────────
        # Agreement-related metrics
        # ────────────────────────────────────────────────────────
        if has_agreement:
            # Completion rate
            if any(kw in query_lower for kw in ["rate", "%", "percentage", "completion"]):
                return "agreement_completion_rate"
            
            # TAT
            if any(kw in query_lower for kw in ["average", "avg", "days", "time", "tat", "turnaround"]):
                return "average_days_to_agreement"
            
            # Value
            if any(kw in query_lower for kw in ["value", "revenue", "amount"]):
                if "pending" in query_lower:
                    return "agreement_pending_value"
                else:
                    return "agreement_given_count"
            
            # Pending agreement
            if any(kw in query_lower for kw in ["pending", "not created", "not prepared", "yet to"]):
                return "agreement_pending_count"
            
            # Given agreement
            if any(kw in query_lower for kw in ["given", "created", "prepared", "signed", "done"]):
                return "agreement_given_count"
            
            # Default
            return "agreement_pending_count"
        
        return None


    def _detect_transfer_query(self, user_query: str) -> Optional[str]:
        """
        Intelligently detect if query is about transfers.
        
        Returns appropriate transfer metric or None.
        """
        query_lower = user_query.lower()
        
        # Check if transfer keyword is present
        transfer_indicators = ["transfer", "transferred", "transferring", "transfers"]
        has_transfer = any(indicator in query_lower for indicator in transfer_indicators)
        
        if not has_transfer:
            return None
        
        # Exclude non-metric transfer queries
        exclusions = [
            "transfer policy", "transfer process", "transfer procedure",
            "how to transfer", "transfer ownership", "transfer documentation"
        ]
        if any(excl in query_lower for excl in exclusions):
            return None
        
        # Determine specific transfer metric type
        
        # Priority 1: Transfer rate
        if any(kw in query_lower for kw in ["rate", "percentage", "%", "percent"]):
            return "transfer_rate"
        
        # Priority 2: Transfer value
        if any(kw in query_lower for kw in ["value", "revenue", "amount", "worth"]):
            return "transferred_sales_value"
        
        # Priority 3: Transfer recipients
        if any(kw in query_lower for kw in ["received", "recipients", "final payer", "transferred to"]):
            return "transfer_recipients"
        
        # Priority 4: Non-transferred
        if any(kw in query_lower for kw in ["non transferred", "without transfer", "normal", "regular"]):
            return "non_transferred_sales"
        
        # Priority 5: Transfer orders count
        if any(kw in query_lower for kw in ["orders", "units count", "sales orders"]):
            return "transferred_sales_count"
        
        # Priority 6: Product-wise transfer
        if any(kw in query_lower for kw in ["product", "eden", "sales group", "amore", "livork"]):
            return "transfer_product_wise"
        
        # Default: Customer count
        return "transferred_sales"

    def extract_intent(self, user_query: str) -> SemanticIntent:
        """
        Extract semantic intent from user query.
        
        WORKFLOW:
        1. Call WatsonX LLM for metric, dimensions, date_range
        2. Use keyword fallbacks for dimension inference
        3. Use _extract_filter_values() for precise filter extraction (NOT LLM)
        """
        # 1. Build prompt
        prompt = self._build_prompt(user_query)
        
        print(f"[DEBUG] User Query: {user_query}")
        print(f"[DEBUG] User Query: {user_query}")
        raw_text = self.model.generate_text(prompt)
        print(f"[DEBUG] Watsonx Response: {raw_text}")
        
        # 3. Parse JSON
        intent_dict = self._parse_json(raw_text)
        
        # 4. Normalize keys (daterange -> date_range, etc.)
        intent_dict = self._normalize_intent_keys(intent_dict)
        print(f"[DEBUG] Parsed Intent (from LLM): {intent_dict}")
        


        # query_lower = user_query.lower()
        # status_breakdown = self._detect_status_breakdown_query(query_lower)
        # if status_breakdown:
        #     dimension, metric = status_breakdown
        #     print(f"[STATUS BREAKDOWN] Detected: {dimension}")
        #     return SemanticIntent(
        #         metric=metric,
        #         dimensions=[dimension],
        #         date_range="current_financial_year",
        #         filters={},
        #         original_query=user_query
        #     )
        # FIX #3: Check for status breakdown queries FIRST
        query_lower = user_query.lower()
        status_dim = self._detect_status_breakdown_query(query_lower)
        if status_dim:
            print(f"[STATUS BREAKDOWN] Using dimension: {status_dim}")
            return SemanticIntent(
                metric="total_sales",
                dimensions=[status_dim],
                date_range="current_financial_year",
                filters={},
                original_query=user_query
            )
        


        cancellation_intent = self._detect_cancellation_query(user_query)
        if cancellation_intent:
            return cancellation_intent


        # ============================================================
        # KEYWORD FALLBACKS - Step 1: Date Range
        # ============================================================
        if not intent_dict.get("date_range") or intent_dict["date_range"] == "current_financial_year":
            detected_date = self._detect_date_range_keywords(user_query)
            if detected_date:
                intent_dict["date_range"] = detected_date
                print(f"[FALLBACK] Fixed date_range: {detected_date}")

        # ============================================================
        # TREND QUERY OVERRIDES (MoM, QoQ, YoY)
        # ============================================================
        query_lower = user_query.lower()
        current_dr = intent_dict.get("date_range", "")
        
        # 1. Year on Year (YoY) or Year Wise
        #    Always use the YoY range to include current FY (partial).
        yoy_triggers = ["year on year", "yoy", "year-on-year", "y-o-y",
                        "year wise", "yearwise", "year-wise", "yearly", "all years",
                        "year by year", "each year", "per year", "annual trend",
                        "year by year","year over year","year wise"]
        if any(t in query_lower for t in yoy_triggers):
            intent_dict["date_range"] = "last_3_financial_years_yoy"
            if not intent_dict.get("time_grain"):
                intent_dict["time_grain"] = "year"
            print(f"[OVERRIDE] YoY/year-wise detected -> {intent_dict['date_range']}")

        # 2. Quarter on Quarter (QoQ)
        #    Prefer keeping explicit year/period if already detected (e.g., "qoq 2023", "qoq last year").
        #    Only force current FY when the date range is still default/empty.
        elif "quarter on quarter" in query_lower or "qoq" in query_lower or "quarter-on-quarter" in query_lower or "quarter over quarter" in query_lower or "quarter by quarter" in query_lower or "quarter wise" in query_lower:
            # Only force current FY when no explicit year is already detected.
            # Otherwise, keep the LLM-provided range (e.g., last_financial_year, fy_2023).
            if current_dr in ("", "current_financial_year"):
                intent_dict["date_range"] = "current_financial_year"
                print(f"[OVERRIDE] QoQ detected -> current_financial_year + time_grain=quarter")
            if not intent_dict.get("time_grain"):
                intent_dict["time_grain"] = "quarter"

        # 3. Month on Month (MoM)
        #    If the user explicitly mentions a year/period (e.g., "last year", "2022"), keep that
        #    date range (so "mom last year" / "mom 2022" uses the intended year).
        elif "month on month" in query_lower or "mom" in query_lower or "month-on-month" in query_lower or "month over month" in query_lower or "month by month" in query_lower or "month wise" in query_lower:
            if current_dr in ("", "current_financial_year", "this_month", "this_week", "today"):
                intent_dict["date_range"] = "current_financial_year"
                print(f"[OVERRIDE] MoM detected -> current_financial_year + time_grain=month")
            if not intent_dict.get("time_grain"):
                intent_dict["time_grain"] = "month"
        
        # ============================================================
        # KEYWORD FALLBACKS - Step 2: Dimension Inference
        # ============================================================
        if not intent_dict.get("dimensions"):
            inferred_dims = self._infer_dimensions(user_query)
            if inferred_dims:
                intent_dict["dimensions"] = inferred_dims
                print(f"[FALLBACK] Inferred dimensions: {inferred_dims}")
        
        # Check for bifurcation queries
        if not intent_dict.get("dimensions"):
            bifurcation_dim = self._detect_bifurcation_query(user_query)
            if bifurcation_dim:
                intent_dict["dimensions"] = [bifurcation_dim]
                print(f"[FALLBACK] Bifurcation dimension: {bifurcation_dim}")
        
        # ============================================================
        # KEYWORD FALLBACKS - Step 3: Transfer Detection
        # ============================================================
        if "transfer" in user_query.lower():
            detected_transfer = self._detect_transfer_query(user_query)
            if detected_transfer:
                intent_dict["metric"] = detected_transfer
                print(f"[TRANSFER FALLBACK] Detected transfer metric: {detected_transfer}")
        

        # ============================================================
        # MULTI-METRIC DETECTION: "count and value", "sales count and amount", etc.
        # When user asks for both count and value, set intent.metrics list so
        # sql_builder emits both aggregate columns in a single query.
        # ============================================================
        multi_metrics = self._detect_multi_metric(user_query)
        if multi_metrics:
            intent_dict["metrics"] = multi_metrics
            # Primary metric drives filter resolution; set to first in list
            intent_dict["metric"] = multi_metrics[0]
            print(f"[MULTI-METRIC] Detected: {multi_metrics}")

        # ============================================================
        # HAVING FILTER DETECTION (greater than / less than on metric value)
        # e.g. "sales amount greater than 50000", "value more than 1 lakh"
        # ============================================================
        having_filter = self._detect_having_filter(user_query, intent_dict.get("metric", "total_sales"))
        if having_filter:
            intent_dict["having_filter"] = having_filter
            print(f"[HAVING] Detected having filter: {having_filter}")

        # ============================================================
        # POSSESSION JOURNEY DETECTION
        # ============================================================
        if "possession" in user_query.lower() or "agreement" in user_query.lower():
            detected_possession = self._detect_possession_metric(user_query)
            if detected_possession:
                intent_dict["metric"] = detected_possession
                print(f"[POSSESSION] Detected metric: {detected_possession}")
        # ============================================================
        # CRITICAL FIX: OVERRIDE LLM FILTERS WITH KEYWORD EXTRACTION
        # ============================================================
        # ALWAYS use keyword-based filter extraction (ignore LLM filters)
        dimensions = intent_dict.get("dimensions", [])
        keyword_filters = self._extract_filter_values(user_query, dimensions)
        
        if keyword_filters:
            # Replace LLM filters with keyword-extracted filters
            intent_dict["filters"] = keyword_filters
            print(f"[OVERRIDE] Keyword-extracted filters: {keyword_filters}")
        else:
            # If keyword extraction fails, clear invalid LLM filters
            if "filters" in intent_dict:
                print(f"[WARNING] Clearing invalid LLM filters: {intent_dict['filters']}")
                intent_dict["filters"] = None
        
        # ============================================================
        # METRIC NORMALIZATION
        # ============================================================
        intent_dict["metric"] = self._normalize_metric(
            intent_dict.get("metric", "total_sales"), 
            user_query
        )
        
        # ============================================================
        # TIME GRAIN DETECTION
        # ============================================================
        if not intent_dict.get("time_grain"):
            detected_grain = self._detect_time_grain(user_query)
            if detected_grain:
                intent_dict["time_grain"] = detected_grain
                print(f"[FALLBACK] Time grain: {detected_grain}")
        
        # ============================================================
        # COMPARISON TYPE DETECTION (mom, qoq, yoy, wow)
        # ============================================================
        if not intent_dict.get("compare_to"):
            detected_compare = self._detect_comparison_type(user_query)
            if detected_compare:
                intent_dict["compare_to"] = detected_compare
                intent_dict["is_trend"] = True
                print(f"[FALLBACK] Comparison: {detected_compare}")
                
                # Auto-detect time_grain if not already set
                if not intent_dict.get("time_grain"):
                    intent_dict["time_grain"] = self._suggest_time_grain_for_comparison(detected_compare)

        # ============================================================
        # ENSURE YoY uses multi-year date range (even if LLM or keyword fallback set another range)
        # ============================================================
        if intent_dict.get("compare_to") == "yoy":
            # Force YoY to include current FY + past years
            intent_dict["date_range"] = "last_3_financial_years_yoy"
            if not intent_dict.get("time_grain"):
                intent_dict["time_grain"] = "year"
            print(f"[OVERRIDE] compare_to=yoy -> date_range set to {intent_dict['date_range']}")
        
        # ============================================================
        # CUSTOM DATE EXTRACTION
        # ============================================================
        # Try enhanced date extraction first
        custom_dates_enhanced, date_range_type = self._extract_custom_dates_enhanced(user_query)
        if custom_dates_enhanced:
            custom_dates = custom_dates_enhanced
            intent_dict["date_range"] = date_range_type
            intent_dict["custom_dates"] = custom_dates
            print(f"[DATE EXTRACTION] Enhanced dates found: {custom_dates}, type: {date_range_type}")
        else:
            # Fallback to original method
            custom_dates = self._extract_custom_dates(user_query)
            if custom_dates:
                intent_dict["custom_dates"] = custom_dates
                print(f"[DATE EXTRACTION] Fallback dates found: {custom_dates}")
        
        # ============================================================
        # CLEANUP: STRIP INVALID DIMENSIONS (time grains + unknown LLM hallucinations)
        # ============================================================
        # Full set of valid dimension keys from dimensions.yaml registry
        REGISTRY_DIMENSIONS = {
            'agreement_given_to_customer', 'agreement_prepared_on', 'agreement_status',
            'allotment_letter_date', 'back_office_executive', 'back_office_executive_name',
            'bank_branch', 'bank_branch1', 'billing_block', 'billing_block_changed_on',
            'billing_block_description', 'billing_plan', 'booking_created_by',
            'booking_type', 'broker', 'broker_name', 'cancellation_reason',
            'charge_type', 'co_applicant1', 'co_applicant1_name', 'co_applicant2',
            'co_applicant2_name', 'co_applicant3', 'co_applicant3_name', 'co_applicant4',
            'co_applicant4_name', 'co_applicant5', 'co_applicant5_name',
            'completion_date', 'consortium', 'consortium_name', 'customer_type',
            'description', 'disbursed_date', 'dist_channel_desc', 'distribution_channel',
            'division', 'division_desc', 'document_handed_over_to_cust_or_bank',
            'document_type', 'floor', 'given_to_accounts', 'given_to_dept_head',
            'given_to_dept_manager', 'given_to_finance_director', 'handed_over_to_customer',
            'inventory_code', 'inventory_text', 'loan_bank', 'material', 'material_group',
            'material_pricing_group', 'material_pricing_group_desc', 'noc_date',
            'old_billing_block', 'old_billing_block_description', 'old_booking_no',
            'payer', 'payer_name', 'plc_material', 'plc_material_desc', 'po_number',
            'possession_given_on', 'possession_status', 'product', 'product_desc',
            'project', 'reason', 'reason_for_rejection', 'refferal', 'registry_date',
            'rejection_date', 'sale_organization', 'sales_executive', 'sales_executive_name',
            'sales_group', 'sales_group_desc', 'sales_office', 'sales_office_desc',
            'sales_order', 'sales_org', 'sales_org_desc', 'sanction_letter_provided_by_bank_on',
            'scheme_code', 'sector', 'signed_agreement_received_on', 'signed_by_accounts',
            'signed_by_dept_head', 'signed_by_dept_manager', 'sold_to', 'sold_to_name',
            'sub_broker', 'sub_broker_name', 'tax_class_1', 'tax_class_2', 'tax_class_3',
            'tax_class_4', 'tax_class_5', 'tax_class_6', 'tax_class_7', 'total_transfer',
            'tower', 'tower_desc', 'tpt_and_ptm_given_for_sign', 'tpt_ptm_sign_date',
            'tpt_received_date', 'type', 'type_desc', 'uom',
        }

        if intent_dict.get("dimensions"):
            dims = intent_dict["dimensions"]
            time_grains = set(self.TIME_GRAIN_KEYWORDS.keys())

            valid_dims = []
            for dim in dims:
                if dim in time_grains:
                    print(f"[CORRECTION] Removed '{dim}' from dimensions (time grain)")
                    if not intent_dict.get("time_grain"):
                        intent_dict["time_grain"] = dim
                elif dim not in REGISTRY_DIMENSIONS:
                    print(f"[CORRECTION] Removed invalid dimension '{dim}' (not in registry)")
                else:
                    valid_dims.append(dim)

            intent_dict["dimensions"] = valid_dims

        # ============================================================
        # RETURN CANONICAL INTENT
        # ============================================================
        print(f"[FINAL] Intent: {intent_dict}")
        return SemanticIntent(
            **intent_dict,
            original_query=user_query
        )



    # =====================================================
    # MULTI-METRIC DETECTION
    # =====================================================

    def _detect_multi_metric(self, user_query: str) -> Optional[List[str]]:
        """
        Detect queries asking for both count and value in one response.

        Examples:
            "total sales count and value of eden"        -> ["total_sales", "sales_value"]
            "sales count and amount of tower 7"          -> ["total_sales", "sales_value"]
            "show count and value of wave city"          -> ["total_sales", "sales_value"]
            "count and sales value of 1bhk"              -> ["total_sales", "sales_value"]
            "total sales and amount of broker"           -> ["total_sales", "sales_value"]
        """
        q = user_query.lower()

        count_kws  = ["sales count", "count of sales", "number of sales", "total sales count",
                      "bookings count", "total bookings", "count and value", "count and amount",
                      "count and sales"]
        value_kws  = ["sales value", "sales amount", "amount", "value", "revenue"]

        has_count = any(kw in q for kw in count_kws)
        has_value = any(kw in q for kw in value_kws)

        # Explicit "count and value / count and amount" pattern — clearest signal
        explicit_patterns = [
            # count first
            "count and value", "count and amount", "count and sales value",
            "count and sales amount", "sales count and value", "sales count and amount",
            "total count and value", "total count and amount",
            "number and value", "number and amount",
            # value/amount first (reversed order)
            "value and count", "amount and count", "sales value and count",
            "sales amount and count", "value and sales count", "amount and sales count",
            "total value and count", "total amount and count",
        ]
        if any(p in q for p in explicit_patterns):
            return ["total_sales", "sales_value"]

        # Broader: any combination of a count keyword + value keyword in either order
        count_kws_broad = ["sales count", "total sales count", "count of sales",
                           " count", "count "]   # standalone "count" word
        if has_count and has_value:
            return ["total_sales", "sales_value"]

        # Check standalone "count" word alongside any value keyword
        import re as _re
        if _re.search(r'\bcount\b', q) and has_value:
            return ["total_sales", "sales_value"]

        return None

    # =====================================================
    # HAVING FILTER DETECTION
    # =====================================================

    def _detect_having_filter(self, user_query: str, metric: str) -> Optional[str]:
        """
        Detect aggregate filter conditions from natural language.

        Examples:
            "sales amount greater than 50000"   -> "SUM(\"net_value\") > 50000"
            "value more than 1 lakh"            -> "SUM(\"net_value\") > 100000"
            "sales less than 10000"             -> "SUM(\"net_value\") < 10000"
            "count greater than 5"              -> "COUNT(DISTINCT Sales_Order) > 5"
            "amount at least 50000"             -> "SUM(\"net_value\") >= 50000"
        """
        query_lower = user_query.lower()

        # Decide which aggregate expression to use in HAVING.
        # Rule:
        #   - If the query explicitly mentions a value/amount keyword  -> SUM("net_value")
        #   - If the metric is a count metric AND no value keyword     -> COUNT(DISTINCT Sales_Order)
        #   - Default fallback                                         -> SUM("net_value")
        value_keywords_check = ["value", "amount", "revenue", "net value", "net amount",
                                 "sales value", "sales amount", "booking amount"]
        count_metrics_check  = {"total_sales", "transferred_sales", "transferred_sales_count",
                                 "cancelled_sales", "cancelled_units",
                                 "possession_pending_count", "possession_given_count",
                                 "agreement_pending_count", "agreement_given_count"}

        query_has_value_kw = any(kw in query_lower for kw in value_keywords_check)

        if query_has_value_kw:
            agg_expr = "SUM(Net_Value)"
        elif metric in count_metrics_check:
            agg_expr = "COUNT(DISTINCT Sales_Order)"
        else:
            agg_expr = "SUM(Net_Value)"  # default

        # Operator patterns
        gt_keywords = ["greater than", "more than", "above", "exceeds"]
        lt_keywords = ["less than", "below", "under", "maximum", "at most", "atmost"]
        gte_keywords = ["greater than or equal", "at least", "atleast", "minimum", ">=", "no less than"]
        lte_keywords = ["less than or equal", "at most", "atmost", "maximum", "<="]

        # Parse numeric value — support lakhs and crores
        def parse_amount(text):
            # Match: "50000", "50,000", "50 lakh", "1.5 crore", "1 lakh"
            lakh_match = re.search(r'(\d+(?:\.\d+)?)\s*lakh', text)
            crore_match = re.search(r'(\d+(?:\.\d+)?)\s*crore', text)
            num_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)', text)
            if crore_match:
                return int(float(crore_match.group(1)) * 10_000_000)
            if lakh_match:
                return int(float(lakh_match.group(1)) * 100_000)
            if num_match:
                return int(num_match.group(1).replace(",", ""))
            return None

        for kw in gte_keywords:
            if kw in query_lower:
                val = parse_amount(query_lower.split(kw)[-1])
                if val is not None:
                    return f"{agg_expr} >= {val}"

        for kw in lte_keywords:
            if kw in query_lower:
                val = parse_amount(query_lower.split(kw)[-1])
                if val is not None:
                    return f"{agg_expr} <= {val}"

        for kw in gt_keywords:
            if kw in query_lower:
                val = parse_amount(query_lower.split(kw)[-1])
                if val is not None:
                    return f"{agg_expr} > {val}"

        for kw in lt_keywords:
            if kw in query_lower:
                val = parse_amount(query_lower.split(kw)[-1])
                if val is not None:
                    return f"{agg_expr} < {val}"

        return None

    # =====================================================
    # DIMENSION INFERENCE
    # =====================================================
    
    def _infer_dimensions(self, user_query: str) -> List[str]:
        """
        Infer dimensions from keywords in user query.
        Uses pattern matching with the DIMENSION_KEYWORDS map.
        """
        query_lower = user_query.lower()
        detected = set()
        
        for dim, keywords in self.DIMENSION_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    detected.add(dim)
                    break  # Found this dimension, move to next
        
        return list(detected)

    def _detect_bifurcation_query(self, user_query: str) -> Optional[str]:
        """
        Detect queries that ask for a breakdown/split WITHOUT mentioning dimensions.
        
        Examples:
        - "Booking type wise sales" -> booking_type
        - "Sales by broker" -> broker
        - "Channel split" -> channel
        - "Tower wise sales" -> tower
        """
        query_lower = user_query.lower()
        
        bifurcation_patterns = {
            "booking_type": ["booking type wise", "booking type bifurcation", "booking split"],
            "broker_name": ["broker wise", "broker split", "sales by broker"],
            "cancellation_reason": ["cancellation reason wise", "reason wise cancellation", "cancellation reason breakdown","reason"],
            "dist_channel_desc": ["channel wise", "channel split", "sales by channel"],
            "sales_group_desc": ["product wise", "product split", "sales by product"],
            "Sales_Org_Desc": ["project wise", "project split", "sales by project"],
            "tower": ["tower wise", "tower split", "sales by tower"],
            "division_desc": ["division wise", "division split"],
            "customer_type": ["booking status wise", "customer type wise", "status wise", "booked vs cancelled"],
            "sales_executive_name": ["executive wise", "salesman wise"],
            "floor_desc": ["floor wise", "floor split"],
            "sector": ["sector wise", "sector split"],
            "type_desc": ["unit type wise", "type wise", "product wise"],
            "sales_group_desc": ["sales group wise", "sales group split", "group wise"],
            "sales_office_desc": ["sales office wise", "office wise", "sales office split"],
            "billing_plan": ["billing plan wise", "payment plan wise", "plan wise"],
            "sales_org_desc": ["organization wise", "org wise", "sales org wise"],
            "loan_bank": ["bank wise", "bank split", "lender wise"],
            "billing_block_description": ["billing block wise", "block reason wise"],
            "material_pricing_group_desc": ["pricing group wise", "material group wise"],
            "scheme_code": ["scheme wise", "scheme split"],
            "refferal": ["refferal wise", "refferal split"],
            "reason_for_rejection": ["rejection reason wise"],
        }
        
        for dim, patterns in bifurcation_patterns.items():
            for pattern in patterns:
                if pattern in query_lower:
                    return dim
        
        return None

    # =====================================================
    # FILTER VALUE EXTRACTION
    # =====================================================
    
    def _extract_filter_values(self, user_query: str, dimensions: List[str]) -> Optional[Dict[str, Any]]:
        """
        INTELLIGENT FILTER EXTRACTOR - AUTO-DETECTS FILTERS FROM ALL 121 COLUMNS
        
        Extracts specific filter values from natural language queries for:
        - Projects (50 types)
        - Channels (Broker/Direct/Referral)
        - Divisions (Residential/Commercial/FSI/Institutional)
        - Sales Org (Wave City/Wave Estate/WMCC)
        - Towers (111 types)
        - Floors (51 types)
        - Unit Types (61 types)
        - Loan Banks (55 banks)
        - Sales Executives (154 people)
        - Brokers (774 brokers)
        - Material Pricing Groups (9 categories)
        - Booking Types (6 types)
        - Customer Types (Booked/Cancelled)
        - And 100+ other dimensions
        """
        filters = {}
        query_lower = user_query.lower()
        
        # FIX #2: Try person name extraction FIRST
        for dim_name in ["payer_name", "sold_to_name", "sales_executive_name", "broker_name"]:
            person_name = self._extract_person_name_filter(query_lower, dim_name)
            if person_name:
                filters[dim_name] = person_name
                print(f"[FILTER] Extracted {dim_name}: {person_name}")
        
        # FIX #4 & #5: Try multi-value extraction
        for dim_name in ["floor_desc", "tower", "sales_group_desc"]:
            multi_values = self._extract_multi_values_with_and(query_lower, dim_name)
            if multi_values:
                filters[dim_name] = multi_values
                print(f"[FILTER] Extracted {dim_name}: {multi_values}")


        def extract_multi(patterns: Dict[str, List[str]], dim_key: str):
            found_values = set()
            for value, keywords in patterns.items():
                for keyword in keywords:
                    if keyword in query_lower:
                        # Special check for "sub" to avoid false positives (e.g. "broker" in "sub broker")
                        if dim_key == "dist_channel_desc" and "sub" in query_lower and "sub" not in keyword:
                            continue
                            
                        # IGNORE "WISE" suffix (e.g. "tower wise" should not match "wise")
                        if keyword == "wise":
                            continue
                            
                        found_values.add(value)
                        break
            
            if found_values:
                # If multiple values found, return list. If single, return string.
                # BUT for "broker and direct", we want list.
                # The downstream SQL builder handles lists.
                final_val = list(found_values)
                if len(final_val) == 1:
                    filters[dim_key] = final_val[0]
                else:
                    filters[dim_key] = final_val

        # ==========================================
        # 1. DISTRIBUTION CHANNEL (Dist_Channel_Desc)
        # ==========================================
        channel_patterns = {
            "Broker": ["broker", "agent", "channel broker", "brokerage"],
            "Direct": ["direct", "direct sales", "walk-in", "walk in"],
            "Referral": ["referral", "referred", "reference"]
        }
        # Skip channel filter when the query is asking for broker-name-wise breakdown
        # e.g. "broker name wise total sales" — "broker" here is a dimension, not a channel filter
        if not any(kw in query_lower for kw in ["broker name", "broker wise", "broker name wise"]):
            extract_multi(channel_patterns, "dist_channel_desc")
        
        # ==========================================
        # 2. PROJECT (Project_Desc) - 50 Projects
        # ==========================================
        project_patterns = {
            "AMORE": ["amore"],
            "LIVORK": ["livork"],
            "DREAM HOMES": ["dream homes", "dream home"],
            "DREAM BAZAAR": ["dream bazaar"],
            "EXECUTIVE FLOORS": ["executive floors", "executive floor"],
            "WAVE FLOOR": ["wave floor"],
            "NEW PLOTS": ["new plots", "new plot"],
            "OLD PLOTS": ["old plots", "old plot"],
            "VERIDIA": ["veridia"],
            "VERIDIA-3": ["veridia 3", "veridia-3"],
            "VERIDIA-4": ["veridia 4", "veridia-4"],
            "VERIDIA-5": ["veridia 5", "veridia-5"],
            "VERIDIA-6": ["veridia 6", "veridia-6"],
            "VERIDIA-7": ["veridia 7", "veridia-7"],
            "EDEN": ["eden"],
            "EDENIA": ["edenia"],
            "ELEGANTIA": ["elegantia"],
            "ELIGO": ["eligo"],
            "EMINENCE": ["eminence"],
            "IRENIA": ["irenia"],
            "TRUCIA": ["trucia"],
            "VASILIA": ["vasilia"],
            "MAYFAIR PARK": ["mayfair", "mayfair park"],
            "HARMONY GREENS": ["harmony greens", "harmony green"],
            "WAVE GALLERIA": ["galleria", "wave galleria"],
            "WAVE GARDEN": ["wave garden"],
            "WAVE ESTATE, GH2 PH2": ["wave estate gh2", "gh2 ph2"],
            "WAVE BUSSINESS SQUARE": ["business square", "bussiness square"],
            "WBT 1": ["wbt 1", "wbt1"],
            "WBT A": ["wbt a", "wbta"],
            "PRIME FLOORS": ["prime floors", "prime floor"],
            "VILLAS": ["villas"],
            "ARMONIA VILLA": ["armonia villa", "armonia"],
            "COMM BOOTH": ["comm booth", "commercial booth"],
            "COMMERCIAL PLOTS": ["commercial plots"],
            "PLOTS-COMM": ["plots comm"],
            "PLOTS-RES": ["plots res", "residential plots"],
            "PLOTS-RES-IF": ["plots res if"],
            "SCO": ["sco"],
            "METRO MART": ["metro mart"],
            "SWAMANORATH": ["swamanorath"],
            "FSI": ["fsi project", "fsi"],
            "INSTITUTIONAL": ["institutional project"],
            "EWS_001_(410)": ["ews 410", "ews_001"],
            "EWS_P2": ["ews p2"],
            "LIG_001_(310)": ["lig 310", "lig_001"],
            "LIG_P2": ["lig p2"],
            "HSSC": ["hssc"],
            "WAVE FLOOR 85": ["wave floor 85"],
            "WAVE FLOOR 99": ["wave floor 99"]
        }
        extract_multi(project_patterns, "sales_group_desc")
        
        # ==========================================
        # 3. DIVISION (Division_Desc)
        # ==========================================
        # division_patterns = {
        #     "Residential": ["residential"],
        #     "Commercial": ["commercial"],
        #     "Institutional": ["institutional"],
        #     "FSI": ["fsi division"]
        # }
        # extract_multi(division_patterns, "division_desc")

        # ==========================================
        # 3. DIVISION (Division_Desc)
        # ==========================================
        # NOTE: Only match if "division" keyword is present
        # Otherwise, "residential/commercial" will match both division AND sector
        # if "division" in query_lower:
        if any(keyword in query_lower for keyword in ["division", "residential", "commercial", "institutional"]):
            division_patterns = {
                "Residential": ["residential"],
                "Commercial": ["commercial"],
                "Institutional": ["institutional"],
                "FSI": ["fsi division"]
            }
            extract_multi(division_patterns, "division_desc")
        
        # ==========================================
        # 4. SALES ORGANIZATION (Sales_Org_Desc)
        # ==========================================
        if any(keyword in query_lower for keyword in ["project", "wave city", "wave estate", "wmcc"]):
            sales_org_patterns = {
                "Wave City": ["wave city"],
                "Wave Estate": ["wave estate"],
                "WMCC Sec 32": ["wmcc", "sector 32 org", "sec 32 sales"]
            }
            extract_multi(sales_org_patterns, "sales_org_desc")
        
        # ==========================================
        # 5. SALES OFFICE (Sales_Office_Desc)
        # ==========================================
        sales_office_patterns = {
            "Wave City": ["wave city office"],
            "Wave Estate": ["wave estate office"],
            "WMCC Sec 32": ["wmcc office", "sector 32 office"]
        }
        extract_multi(sales_office_patterns, "sales_office_desc")
        
        # ==========================================
        # 6. TOWER DETECTION (Tower / Tower_Desc)
        # ==========================================
        # if "tower" in query_lower or "block" in query_lower:
        #     # Pattern: "tower 7", "tower A", "block 032", "tower 3JA"
        #     # BUT exclude "tower wise", "tower split" (grouping keywords)
        #     match = re.search(r'(?:tower|block)\s+([a-zA-Z0-9]+)', query_lower)
        #     if match:
        #         tower_val = match.group(1).upper()
        #         # Ignore if it's a grouping keyword
        #         if tower_val.lower() not in ['wise', 'split', 'breakdown', 'group']:
        #             # If numeric, pad with zeros (e.g., "7" -> "007")
        #             if tower_val.isdigit():
        #                 filters["tower"] = tower_val.zfill(3)
        #             else:
        #                 filters["tower_desc"] = tower_val

        # ==========================================
        # 6. TOWER DETECTION (Tower / Tower_Desc) - MULTI-VALUE SUPPORT
        # ==========================================
        if "tower" in query_lower or "block" in query_lower:
            tower_values = []
            
            # Pattern 1: "tower X and tower Y" (explicit tower for each)
            pattern1 = r'(?:tower|block)\s+([a-zA-Z0-9]+)(?:\s+and\s+(?:tower|block)\s+([a-zA-Z0-9]+))?'
            matches = re.findall(pattern1, query_lower)
            
            for match in matches:
                for val in match:
                    if val and val.lower() not in ['wise', 'split', 'breakdown', 'group', 'and']:
                        tower_values.append(val.upper())
            
            # Pattern 2: "tower X and Y" (without repeating "tower")
            pattern2 = r'(?:tower|block)\s+([a-zA-Z0-9]+)\s+and\s+([a-zA-Z0-9]+)(?:\s+(?:tower|block))?'
            matches2 = re.findall(pattern2, query_lower)
            
            for match in matches2:
                for val in match:
                    if val and val.lower() not in ['wise', 'split', 'breakdown', 'group', 'and', 'tower', 'block']:
                        tower_values.append(val.upper())
            
            # Remove duplicates
            tower_values = list(set(tower_values))
            
            if tower_values:
                # If multiple towers, store as list
                if len(tower_values) > 1:
                    filters["tower"] = tower_values
                else:
                    # Single tower - pad if numeric
                    if tower_values[0].isdigit():
                        filters["tower"] = tower_values[0].zfill(3)
                    else:
                        filters["tower"] = tower_values[0]
        
        # ==========================================
        # 7. FLOOR DETECTION (Floor_Desc)
        # ==========================================
        # if "floor" in query_lower:
        #     floor_patterns = [
        #         (r'ground\s+floor', "Ground Floor"),
        #         (r'lower\s+ground\s+floor', "Lower Ground Floor"),
        #         (r'upper\s+ground\s+floor', "Upper Ground Floor"),
        #         (r'basement\s+(\d+)', "Basement {0}"),
        #         (r'podium\s+(\d+)', "Podium {0}"),
        #         (r'(\d+)(?:st|nd|rd|th)\s+floor', "{0}th Floor"),
        #         (r'floor\s+(\d+)', "{0}th Floor"),
        #         (r'g\+(\d+)', "G+{0}")
        #     ]
            
        #     for pattern, floor_format in floor_patterns:
        #         match = re.search(pattern, query_lower)
        #         if match:
        #             if "{0}" in floor_format:
        #                 floor_num = match.group(1)
        #                 if floor_num == "1":
        #                     floor_format = floor_format.replace("{0}th", "1st")
        #                 elif floor_num == "2":
        #                     floor_format = floor_format.replace("{0}th", "2nd")
        #                 elif floor_num == "3":
        #                     floor_format = floor_format.replace("{0}th", "3rd")
        #                 filters["floor_desc"] = floor_format.format(floor_num)
        #             else:
        #                 filters["floor_desc"] = floor_format
        #             break


        # ==========================================
        # 7. FLOOR DETECTION (Floor_Desc) - MULTI-VALUE SUPPORT
        # ==========================================
        if "floor" in query_lower:
            floor_values = []
            
            # Pattern for multiple floors: "15th and 16th floor", "15 and 16 floor"
            multi_floor_pattern = r'(\d+)(?:st|nd|rd|th)?\s+and\s+(\d+)(?:st|nd|rd|th)?\s+floor'
            multi_match = re.search(multi_floor_pattern, query_lower)
            
            if multi_match:
                # Extract both floor numbers
                floor1 = multi_match.group(1)
                floor2 = multi_match.group(2)
                floor_values.extend([floor1, floor2])
            else:
                # Single floor patterns
                floor_patterns = [
                    (r'ground\s+floor', "Ground Floor"),
                    (r'lower\s+ground\s+floor', "Lower Ground Floor"),
                    (r'upper\s+ground\s+floor', "Upper Ground Floor"),
                    (r'basement\s+(\d+)', "Basement {0}"),
                    (r'podium\s+(\d+)', "Podium {0}"),
                    (r'(\d+)(?:st|nd|rd|th)\s+floor', "{0}"),
                    (r'floor\s+(\d+)', "{0}"),
                    (r'g\+(\d+)', "G+{0}")
                ]
                
                for pattern, floor_format in floor_patterns:
                    match = re.search(pattern, query_lower)
                    if match:
                        if "{0}" in floor_format:
                            floor_num = match.group(1)
                            floor_values.append(floor_num)
                        else:
                            # Special floors like "Ground Floor"
                            filters["floor"] = floor_format
                            floor_values = []  # Clear list since we set filters directly
                        break
            
            # If we have numeric floor values, store them
            if floor_values:
                # Remove duplicates
                floor_values = list(set(floor_values))
                
                if len(floor_values) > 1:
                    # Multiple floors - store as list
                    filters["floor"] = floor_values
                else:
                    # Single floor
                    filters["floor"] = floor_values[0]
        
        # ==========================================
        # 8. UNIT TYPE (Type_Desc) - 61 Types
        # ==========================================
        unit_type_patterns = {
            "1 BHK": ["1 bhk", "1bhk", "one bhk"],
            "2 BHK": ["2 bhk", "2bhk", "two bhk"],
            "3 BHK": ["3 bhk", "3bhk", "three bhk"],
            "4 BHK": ["4 bhk", "4bhk", "four bhk"],
            "2 BHK with Study": ["2 bhk with study", "2 bhk study"],
            "3 BHK with Study": ["3 bhk with study", "3 bhk study"],
            "4 BHK with Study": ["4 bhk with study", "4 bhk study"],
            "3 BHK with Servent Qtr": ["3 bhk with servant", "3 bhk servant"],
            "4 BHK with Servent Qtr.": ["4 bhk with servant", "4 bhk servant"],
            "Shop": ["shop", "shops"],
            "Office": ["office", "offices"],
            "Villa": ["villa", "villas"],
            "Duplex": ["duplex"],
            "Penthouse": ["penthouse"],
            "Sky Villa": ["sky villa"],
            # "Plot": ["plot", "plots"],
            "Commercial": ["commercial unit"],
            "Food Court": ["food court"],
            "Commercial Booth": ["commercial booth"]
        }
        extract_multi(unit_type_patterns, "type_desc")
        
        # ==========================================
        # 9. MATERIAL PRICING GROUP (Material_Pricing_Group_Desc)
        # ==========================================
        pricing_group_patterns = {
            "Apartment": ["apartment pricing", "apartment group", "apartment"],
            "Independent Floor": ["independent floor", "independent floors"],
            "Office": ["office pricing"],
            "Shops": ["shops pricing"],
            "Plots": ["plots pricing","plots","plot"],
            "Villas": ["villas pricing"],
            "SCO": ["sco pricing"],
            "FSI": ["fsi pricing"],
            "Institutional Site": ["institutional site"]
        }
        extract_multi(pricing_group_patterns, "material_pricing_group_desc")
        
        # ==========================================
        # 10. SECTOR DETECTION (Sector - numeric)
        # ==========================================
        if "sector" in query_lower:
            match = re.search(r'sector\s+(\d+)', query_lower)
            if match:
                filters["sector"] = match.group(1)
        # ==========================================
        # 10B. SECTOR CATEGORY (Residential/Commercial) - NEW
        # ==========================================
        # NOTE: This detects sector TYPE (residential/commercial)
        # which is different from numeric sector (e.g., "sector 32")
        # sector_category_patterns = {
        #     "Residential": ["residential", "resi"],
        #     "Commercial": ["commercial", "comm"]
        # }
        
        # Check if query contains residential/commercial keywords
        # found_sectors = []
        # for sector_val, keywords in sector_category_patterns.items():
        #     for keyword in keywords:
        #         if keyword in query_lower:
        #             found_sectors.append(sector_val)
        #             break
        
        # if found_sectors:
        #     # If multiple sectors found (e.g., "residential and commercial")
        #     if len(found_sectors) > 1:
        #         # Only override if we haven't already set a numeric sector
        #         if "sector" not in filters:
        #             filters["sector"] = found_sectors
        #     else:
        #         # Single sector - only set if numeric sector wasn't found
        #         if "sector" not in filters:
        #             filters["sector"] = found_sectors[0]
        # ==========================================
        # 11. BOOKING TYPE (Booking_Type)
        # ==========================================
        booking_type_patterns = {
            "Fresh": ["fresh booking", "fresh", "new booking"],
            "Fresh (Indirect)": ["fresh indirect", "indirect fresh"],
            "Relocation": ["relocation", "relocated"],
            "Relocation (Indirect)": ["relocation indirect", "indirect relocation"],
            "Unit Transfer": ["unit transfer booking"],
            "Farmer (Indirect)": ["farmer indirect", "indirect farmer"]
        }
        extract_multi(booking_type_patterns, "booking_type")
        
        # ==========================================
        # 12. CUSTOMER TYPE (Customer_Type) - Booking Status
        # ==========================================
        if "cancelled" in query_lower and "not" not in query_lower:
            filters["customer_type"] = "Cancelled"
        # elif "transferred" in query_lower:
        #     filters["customer_type"] = "Transferred"
        elif "booked" in query_lower and "pre" not in query_lower:
            filters["customer_type"] = "Booked"
        
        # ==========================================
        # 13. LOAN BANK DETECTION (55 Banks)
        # ==========================================
        bank_patterns = {
            "HDF": ["hdfc", "hdfc bank", "housing development"],
            "ICI": ["icici", "icici bank"],
            "SBI": ["sbi", "state bank of india", "state bank"],
            "AXB": ["axis", "axis bank"],
            "PNB": ["pnb", "punjab national", "punjab national bank"],
            "KMB": ["kotak", "kotak mahindra", "kotak bank"],
            "BOI": ["boi", "bank of india"],
            "BOB": ["bob", "bank of baroda", "baroda bank"],
            "CNB": ["canara", "canara bank"],
            "UBI": ["ubi", "union bank", "union bank of india"],
            "YES": ["yes bank", "yes"],
            "IDB": ["idbi", "idbi bank"],
            "PGB": ["punjab gramin bank"],
            "RBL": ["rbl", "rbl bank"],
            "DCH": ["dewan housing", "dhfl", "dch"],
            "L&T": ["l&t", "l&t finance", "larsen toubro"],
            "IDF": ["indiabulls", "indiabulls housing"],
            "LIC": ["lic", "lic housing", "lic housing finance"],
            "J&K": ["j&k", "jammu kashmir bank"],
            "OBC": ["obc", "oriental bank"],
            "IOB": ["iob", "indian overseas bank"],
            "CBI": ["cbi", "central bank of india"],
            "SBH": ["sbh", "state bank hyderabad"],
            "SBP": ["sbp", "state bank patiala"]
        }
        extract_multi(bank_patterns, "loan_bank")
        
        # 14. SALES EXECUTIVE (Sales_Executive_Name)
        # ==========================================
        if "sales executive" in query_lower or "salesman" in query_lower or "executive" in query_lower:
            # Extract name after "sales executive" or "salesman"
            match = re.search(r'(?:sales executive|salesman|executive)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
            if match:
                filters["sales_executive_name"] = match.group(1)
        
        # ==========================================
        # 15. BACK OFFICE EXECUTIVE (Back_Office_Executive_Name)
        # ==========================================
        if "back office" in query_lower:
            match = re.search(r'back office\s+(?:executive\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
            if match:
                filters["back_office_executive_name"] = match.group(1)
        
        # ==========================================
        # 16. BROKER NAME (Broker_Name)
        # ==========================================
        if "broker" in query_lower and "sales" not in query_lower and "channel" not in query_lower:
            # Try to extract broker name
            match = re.search(r'broker\s+([A-Z][A-Z\s&.()]+)', user_query)
            if match:
                filters["broker_name"] = match.group(1).strip()
        
        # ==========================================
        # 17. SUB BROKER NAME (Sub_Broker_Name)
        # ==========================================
        if "sub broker" in query_lower or "sub-broker" in query_lower:
            match = re.search(r'sub[\s-]?broker\s+([A-Z][A-Z\s&.()]+)', user_query)
            if match:
                filters["sub_broker_name"] = match.group(1).strip()
        
        # ==========================================
        # 18. CUSTOMER NAME (Sold_To_Name)
        # ==========================================
        if "customer" in query_lower or "buyer" in query_lower:
            match = re.search(r'(?:customer|buyer)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
            if match:
                filters["sold_to_name"] = match.group(1)
        
        # ==========================================
        # 19. PAYER NAME (Payer_Name)
        # ==========================================
        if "payer" in query_lower:
            match = re.search(r'payer\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
            if match:
                filters["payer_name"] = match.group(1)
        
        # ==========================================
        # 20. BILLING BLOCK (Billing_Block_Description)
        # ==========================================
        billing_block_patterns = {
            "Booking Cancel": ["booking cancel", "cancelled booking"],
            "Cancel - In Process": ["cancel in process", "cancellation in process"],
            "Cancelled/Others": ["cancelled others"],
            "Dormant Sale": ["dormant", "dormant sale"],
            "Hold-Legal Case": ["legal case", "hold legal"],
            "Merger": ["merger"],
            "Relocation": ["relocation block"],
            "Billed in Legacy ERP": ["legacy erp", "billed in legacy"]
        }
        
        for block, keywords in billing_block_patterns.items():
            for keyword in keywords:
                if keyword in query_lower:
                    filters["billing_block_description"] = block
                    break
        
        # Check for "not blocked" or "no billing block"
        if any(phrase in query_lower for phrase in ["not blocked", "no billing block", "unblocked"]):
            filters["billing_block"] = "NULL"
        elif "blocked" in query_lower or "billing block" in query_lower:
            if "billing_block_description" not in filters:
                filters["billing_block"] = "NOT NULL"
        
        # ==========================================
        # 21. REASON FOR REJECTION (Reason_for_Rejection)
        # ==========================================
        if "rejected" in query_lower or "rejection" in query_lower:
            # Check if specific rejection code is mentioned (Z1-ZB)
            match = re.search(r'\b(Z[0-9AB]|00)\b', user_query, re.IGNORECASE)
            if match:
                filters["reason_for_rejection"] = match.group(1).upper()
            else:
                filters["reason_for_rejection"] = "NOT NULL"
        
        # ==========================================
        # 22. SALES ORDER NUMBER (Sales_Order)
        # ==========================================
        if "sales order" in query_lower or re.search(r'\bSO[\s:-]?\d+', user_query, re.IGNORECASE):
            match = re.search(r'(?:SO|sales order)[\s:-]?(\d+)', user_query, re.IGNORECASE)
            if match:
                filters["sales_order"] = match.group(1)
        
        # ==========================================
        # 23. DOCUMENT TYPE (Document_Type)
        # ==========================================
        if "document type" in query_lower:
            match = re.search(r'document type\s+([A-Z0-9]+)', user_query)
            if match:
                filters["document_type"] = match.group(1)
        
        # ==========================================
        # 24. INVENTORY CODE (Inventory_Code)
        # ==========================================
        if "inventory" in query_lower:
            # Match inventory codes like "WC-1234", "INV-001"
            match = re.search(r'(?:unit|inventory)[\s:-]?([A-Z0-9-]+)', user_query, re.IGNORECASE)
            if match:
                filters["inventory_code"] = match.group(1).upper()
        
        # ==========================================
        # 25. UOM (Unit of Measurement)
        # ==========================================
        uom_patterns = {
            "FT2": ["sq ft", "square feet", "sqft", "ft2"],
            "M2": ["sq m", "square meter", "sqm", "m2"],
            "YD2": ["sq yd", "square yard", "sqyd", "yd2"],
            "ACR": ["acre", "acres"]
        }
        
        for uom, keywords in uom_patterns.items():
            for keyword in keywords:
                if keyword in query_lower:
                    filters["uom"] = uom
                    break
        
        # ==========================================
        # 26. MATERIAL GROUP (Material_Group)
        # ==========================================
        material_group_patterns = {
            "ZSDBSP": ["zsdbsp"],
            "ZSDBSP1": ["zsdbsp1"],
            "ZSDBSP4": ["zsdbsp4"],
            "ZSDBSP5": ["zsdbsp5"]
        }
        
        for group, keywords in material_group_patterns.items():
            for keyword in keywords:
                if keyword in query_lower:
                    filters["material_group"] = group
                    break
        
        # ==========================================
        # 27. REFERRAL (Refferal)
        # ==========================================
        if "refferal by" in query_lower or "reffered by" in query_lower:
            match = re.search(r'(?:referral|referred)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
            if match:
                filters["refferal"] = match.group(1)
        
        # ==========================================
        # 28. CO-APPLICANT NAMES
        # ==========================================
        if "co-applicant" in query_lower or "co applicant" in query_lower:
            match = re.search(r'co[\s-]?applicant\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', user_query)
            if match:
                filters["co_applicant1_name"] = match.group(1)
        
        # ==========================================
        # 29. PLC MATERIAL (PLC_Material_Desc)
        # ==========================================
        if "plc" in query_lower:
            match = re.search(r'plc\s+([A-Z0-9/]+)', user_query, re.IGNORECASE)
            if match:
                filters["plc_material_desc"] = match.group(1).upper()
        
        # ==========================================
        # 30. SCHEME CODE (Scheme_Code)
        # ==========================================
        if "scheme" in query_lower:
            match = re.search(r'scheme\s+([A-Z0-9]+)', user_query, re.IGNORECASE)
            if match:
                filters["scheme_code"] = match.group(1).upper()
        
        # ==========================================
        # 31. CONSORTIUM (Consortium_Name)
        # ==========================================
        if "consortium" in query_lower:
            match = re.search(r'consortium\s+([A-Z][A-Z\s&.()]+)', user_query)
            if match:
                filters["consortium_name"] = match.group(1).strip()
        
        # ==========================================
        # 32. CHARGE TYPE (Charge_Type)
        # ==========================================
        if "charge type" in query_lower:
            match = re.search(r'charge type\s+([A-Z][a-z]+)', user_query)
            if match:
                filters["charge_type"] = match.group(1)
        
        # ==========================================
        # 33. TAX CLASSES (Tax_Class_1 to Tax_Class_7)
        # ==========================================
        if "tax class" in query_lower:
            match = re.search(r'tax class\s+(\d+)', query_lower)
            if match:
                tax_num = match.group(1)
                # Extract tax value
                tax_match = re.search(r'tax class\s+\d+\s+([A-Z0-9]+)', user_query, re.IGNORECASE)
                if tax_match:
                    filters[f"tax_class_{tax_num}"] = tax_match.group(1).upper()

        # ==========================================
        # 33. SALES GROUP (Sales_Group / Sales_Group_Desc)
        # ==========================================
        if "sales group" in query_lower or "group" in query_lower:
            # Pattern: "Sales Group 01", "Group 01", "Sales Group 10"
            match = re.search(r'(?:sales )?group\s+(\d+)', query_lower)
            if match:
                group_val = match.group(1)
                # Ensure 2-digit padding if needed (e.g. "1" -> "01")
                filters["sales_group"] = group_val.zfill(2)

        # ==========================================
        # 34. SALES OFFICE (Sales_Office / Sales_Office_Desc)
        # ==========================================
        if "sales office" in query_lower:
            match = re.search(r'sales office\s+([A-Z0-9\s]+)', user_query, re.IGNORECASE)
            if match:
                # Capture the office name (e.g. "Sales Office Noida")
                filters["sales_office_desc"] = match.group(1).strip()

        # ==========================================
        # 35. PAYMENT PLAN / BILLING PLAN (Billing_Plan)
        # ==========================================
        if "plan" in query_lower:
            # Pattern: "CLP Plan", "Down Payment Plan"
            if "clp" in query_lower:
                filters["billing_plan"] = "CLP"
            elif "down payment" in query_lower:
                filters["billing_plan"] = "Down Payment"
            elif "flexi" in query_lower:
                filters["billing_plan"] = "Flexi"

        # ============================================================
        # IMPLICIT GROUPING LOGIC (Moved to end)
        # ============================================================
        # If the user specifies multiple values for a filter (e.g. "Broker and Direct"),
        # AND they ask for "group by" or "wise", we should add that dimension to the grouping.
        
        has_grouping_intent = any(k in query_lower for k in ["group", "wise", "breakdown", "split", "vs"])
        
        if has_grouping_intent:
            for dim, val in filters.items():
                if isinstance(val, list) and len(val) > 1:
                    if dim not in dimensions:
                        dimensions.append(dim)
                        print(f"[INFERENCE] Added {dim} to dimensions due to multi-value filter + grouping intent")

        return filters if filters else None

    # =====================================================
    # METRIC NORMALIZATION
    # =====================================================
    
    def _normalize_metric(self, metric: str, user_query: str) -> str:
        """
        Map natural language to proper metric name.
        Handles aliases and context.
        """
        query_lower = user_query.lower()

        # If a specialist detector (_detect_transfer_query, _detect_possession_metric)
        # already resolved the metric to a known specific metric, trust it and return
        # immediately — skip keyword scanning to avoid false substring matches.
        # e.g. "transferred sales value" contains "sales value" which would otherwise
        # match the sales_value keyword and overwrite the correct transferred_sales_value.
        specialist_metrics = {
            "transferred_sales", "transfer_product_wise", "transferred_sales_count",
            "transferred_sales_value", "transfer_recipients", "non_transferred_sales",
            "transfer_rate",
            "possession_pending_count", "possession_given_count",
            "agreement_pending_count", "agreement_given_count",
            "possession_completion_rate", "possession_status_breakdown",
            "average_days_to_possession", "average_days_to_agreement",
            "average_total_cycle_time", "possession_pending_value",
            "possession_given_value", "agreement_pending_value",
            "tower_wise_possession_pending", "tower_wise_possession_given",
            "floor_wise_possession_pending", "product_wise_possession_pending",
            "cancelled_sales", "cancelled_units", "cancelled_sales_value",
        }
        if metric in specialist_metrics:
            return metric

        # Check keyword matches
        for metric_name, keywords in self.METRIC_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    # Special: "net value" could mean sales_value
                    if metric_name == "net_value":
                        return "sales_value"
                    return metric_name
        
        # Fallback: if metric is already valid, use it
        valid_metrics = [
            "total_sales", "sales_value", "net_value",
            "amount_received", "amount_demanded",
            "collection_percentage", "area_sold",
            # Transfer metrics
            "transferred_sales", "transfer_product_wise", "transferred_sales_count",
            "transferred_sales_value", "transfer_recipients",
            "transfer_rate",
            # Possession metrics
            "possession_pending_count", "possession_given_count",
            "agreement_pending_count", "agreement_given_count",
            "possession_completion_rate", "possession_status_breakdown",
            "average_days_to_possession", "average_days_to_agreement",
            "average_total_cycle_time",
            "possession_pending_value", "possession_given_value", "agreement_pending_value",
            # Dimensional possession metrics
            "tower_wise_possession_pending", "tower_wise_possession_given",
            "floor_wise_possession_pending", "product_wise_possession_pending"
        ]
        
        if metric in valid_metrics:
            return metric
        
        # Default
        return "total_sales"

    # =====================================================
    # TIME GRAIN DETECTION
    # =====================================================
    
    def _detect_time_grain(self, user_query: str) -> Optional[str]:
        """
        Detect time grain from keywords.
        
        Examples:
        - "monthly sales" -> "month"
        - "quarterly breakdown" -> "quarter"
        - "daily trend" -> "day"
        """
        query_lower = user_query.lower()
        
        for grain, keywords in self.TIME_GRAIN_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    return grain
        
        return None
    


    def _extract_person_name_filter(self, query_lower: str, dim_name: str) -> Optional[str]:
        """
        FIX #2: Extract person names from queries.
        
        Examples:
            "total sales of payer sunil" -> "sunil"
            "sales sold to amit hora" -> "amit hora"  
            "sales by sales executive john smith" -> "john smith"
        """
        # Keywords that indicate a person name follows
        person_indicators = {
            "payer_name": ["payer"],
            "sold_to_name": ["sold to", "customer name"],
            "sales_executive_name": ["sales executive", "salesman"],
            "broker_name": ["broker name", "agent"],
        }
        
        if dim_name not in person_indicators:
            return None
        
        indicators = person_indicators[dim_name]
        
        # Words that indicate a grouping/dimension request, NOT a person name
        name_stopwords = {
            'wise', 'split', 'breakdown', 'total', 'sales', 'count', 'number',
            'of', 'by', 'in', 'for', 'and', 'the', 'with', 'report', 'data',
            'summary', 'analysis', 'trend', 'all', 'each', 'per', 'show', 'me'
        }

        for indicator in indicators:
            # Look for "payer X", "sold to X", etc.
            # Match 1-3 words after the indicator
            pattern = rf'{indicator}\s+([a-z]+(?:\s+[a-z]+){{0,2}})'
            match = re.search(pattern, query_lower)
            if match:
                name = match.group(1).strip()
                first_word = name.split()[0].lower()
                # Reject if it starts with a grouping/reporting word — not a person name
                if first_word in name_stopwords:
                    continue
                # Capitalize properly
                return ' '.join(word.capitalize() for word in name.split())
        
        return None


    

    def _extract_multi_values_with_and(self, query_lower: str, dim_name: str) -> Optional[List[str]]:
        """
        FIX #4 & #5: Extract multiple values connected by 'and'.
        
        Examples:
            "15th floor and 16th floor" -> ["15th", "16th"]
            "tower a and tower 7" -> ["a", "7"]
            "eden and amore" -> ["eden", "amore"]
        """
        # Dimension-specific extraction patterns
        patterns = {
            "floor_desc": [
                # Match: "15th floor and 16th floor"
                r'(\d+(?:st|nd|rd|th)?)\s+floor\s+and\s+(\d+(?:st|nd|rd|th)?)\s+floor',
                # Match: "floor 15 and floor 16"
                r'floor\s+(\d+)\s+and\s+floor\s+(\d+)',
                # Match: "15th and 16th floor"
                r'(\d+(?:st|nd|rd|th)?)\s+and\s+(\d+(?:st|nd|rd|th)?)\s+floor',
            ],
            "tower": [
                # Match: "tower a and tower 7"
                r'tower\s+([a-z0-9]+)\s+and\s+tower\s+([a-z0-9]+)',
                # Match: "tower a and 7"
                r'tower\s+([a-z0-9]+)\s+and\s+([a-z0-9]+)',
            ],
            "sales_group_desc": [
                # Match: "eden and amore"
                r'(eden|amore|livork)\s+and\s+(eden|amore|livork)',
            ]
        }
        
        if dim_name not in patterns:
            return None
        
        for pattern in patterns[dim_name]:
            match = re.search(pattern, query_lower)
            if match:
                values = list(match.groups())
                # Clean and deduplicate
                values = [v.strip() for v in values if v]
                return list(dict.fromkeys(values))  # Remove duplicates, preserve order
        
        return None



    # =====================================================
    # COMPARISON TYPE DETECTION
    # =====================================================
    
    def _detect_comparison_type(self, user_query: str) -> Optional[str]:
        """
        Detect time-based comparisons.
        
        Examples:
        - "month on month sales" -> "mom"
        - "year on year growth" -> "yoy"
        """
        query_lower = user_query.lower()
        
        for compare_type, keywords in self.COMPARISON_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    return compare_type
        
        return None

    def _suggest_time_grain_for_comparison(self, compare_to: str) -> str:
        """
        Suggest appropriate time grain for a comparison type.
        """
        grain_map = {
            "mom": "month",
            "wow": "day",  # week is not in Presto, so day for daily data
            "qoq": "quarter",
            "yoy": "year",
        }
        
        return grain_map.get(compare_to, "month")
    

    # def _detect_status_breakdown_query(self, query_lower: str) -> Optional[Tuple[str, str]]:
    #     """
    #     FIX #3: Detect if query is asking for status breakdown.
    #     Returns tuple of (dimension_name, metric_name) or None.
    #     """
    #     # Possession status queries
    #     if any(phrase in query_lower for phrase in ["possession status", "possession breakdown", "possession wise"]):
    #         return ("possession_status", "total_sales")
        
    #     # Agreement status queries  
    #     if any(phrase in query_lower for phrase in ["agreement status", "agreement breakdown", "agreement wise"]):
    #         return ("agreement_status", "total_sales")
        
    #     return None
    def _detect_status_breakdown_query(self, query_lower: str) -> Optional[str]:
        """
        Detect if query is asking for status breakdown.
        Returns the dimension name to use.
        """
        # Possession status queries
        if any(phrase in query_lower for phrase in [
            "possession status", 
            "possession breakdown", 
            "possession wise",
            "possession stage"
        ]):
            return "possession_status"
        
        # Agreement status queries
        if any(phrase in query_lower for phrase in [
            "agreement status", 
            "agreement breakdown", 
            "agreement wise",
            "agreement stage"
        ]):
            return "agreement_status"
        
        return None

    # =====================================================
    # CUSTOM DATE EXTRACTION
    # =====================================================
    
    def _extract_custom_dates(self, user_query: str) -> Optional[List[Dict]]:
        """
        Extract specific dates/months from user query.
        
        Examples:
        - "april 2024" -> [{"month_num": 4, "year": 2024}]
        - "from april to september 2024" -> [{"month_num": 4, "year": 2024}, {"month_num": 9, "year": 2024}]
        - "in may" -> [{"month_num": 5, "year": <inferred_fy_year>}]
        """
        query_lower = user_query.lower()
        dates = []
        
        month_map = {
            'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
            'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
            'aug': 8, 'august': 8, 'sep': 9, 'september': 9, 'oct': 10, 'october': 10,
            'nov': 11, 'november': 11, 'dec': 12, 'december': 12
        }

        months_regex = r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'

        # Helper: Infer year for a month in Current Financial Year
        def infer_fy_year(m_num):
            today = datetime.date.today()
            # *** CRITICAL: Correct FY Logic ***
            # If today is Jan-Mar (month < 4), FY started in PREVIOUS year
            # If today is Apr-Dec (month >= 4), FY started in CURRENT year
            if today.month < 4:
                # We're in Jan-Mar, so FY started last year
                fy_start_year = today.year - 1
            else:
                # We're in Apr-Dec, so FY started this year
                fy_start_year = today.year
                
            # Now determine which year the target month belongs to:
            # Months Apr(4)-Dec(12) belong to fy_start_year
            # Months Jan(1)-Mar(3) belong to fy_start_year + 1
            if m_num >= 4:
                return fy_start_year
            else:
                return fy_start_year + 1

        # Pattern 0: Range "from Month to Month Year" (e.g. "from april to september 2024")
        range_pattern = rf'from\s+{months_regex}\s+to\s+{months_regex}\s+(\d{{4}})'
        range_matches = re.findall(range_pattern, query_lower)
        
        if range_matches:
            for m1, m2, y_str in range_matches:
                y = int(y_str)
                dates.append({"month_num": month_map.get(m1), "year": y})
                dates.append({"month_num": month_map.get(m2), "year": y})
            return dates

        # Pattern 1: Month Year (e.g., "april 2024")
        month_year_pattern = rf'{months_regex}\s+(\d{{4}})'
        matches = re.findall(month_year_pattern, query_lower)
        
        if matches:
            for month_str, year_str in matches:
                month_num = month_map.get(month_str, 1)
                year_num = int(year_str)
                dates.append({
                    "month_num": month_num,
                    "year": year_num
                })
            return dates
        
        # Pattern 2: Full dates (e.g., "2024-04-15", "15/04/2024")
        date_pattern = r'(\d{4})-(\d{2})-(\d{2})|(\d{2})/(\d{2})/(\d{4})'
        date_matches = re.findall(date_pattern, query_lower)
        
        if date_matches:
            for match in date_matches:
                if match[0]:  # YYYY-MM-DD
                    dates.append({"year": int(match[0]), "month_num": int(match[1]), "day": int(match[2])})
                elif match[3]:  # DD/MM/YYYY
                    dates.append({"day": int(match[3]), "month_num": int(match[4]), "year": int(match[5])})
            return dates

        # Pattern 3: Month Only (Infer Year)
        # Matches standalone months like "sales in May", "August sales"
        # Only if NO other date patterns matched
        standalone_matches = re.findall(rf'\b{months_regex}\b', query_lower)
        if standalone_matches:
            unique_months = set(standalone_matches)
            for m_str in unique_months:
                m_num = month_map.get(m_str)
                if m_num:
                    y_inferred = infer_fy_year(m_num)
                    dates.append({"month_num": m_num, "year": y_inferred})
            return dates
        
        return None
    
    
    def _normalize_intent_keys(self, data: dict) -> dict:
        """
        Normalize LLM response keys to match SemanticIntent fields.
        Handles: daterange→date_range, customdates→custom_dates, etc.
        """
        key_map = {
            "daterange": "date_range",
            "customdates": "custom_dates",
            "timegrain": "time_grain",
            "istrend": "is_trend",
            "compareto": "compare_to",
            "orderby": "order_by",
            "orderdirection": "order_direction",
        }
        
        for src, dst in key_map.items():
            if src in data and dst not in data:
                data[dst] = data[src]
        
        # Normalize date_range variants
        if "date_range" in data and data["date_range"]:
            dr = str(data["date_range"]).strip().lower().replace(" ", "_")
            alias_map = {
                "thisweek": "this_week",
                "lastweek": "last_week",
                "thismonth": "this_month",
                "lastmonth": "last_month",
                "thisquarter": "this_quarter",
                "lastquarter": "last_quarter",
                "thisyear": "this_year",
                "lastyear": "last_year",
                "currentfinancialyear": "current_financial_year",
                "lastfinancialyear": "last_financial_year",
                "rolling7days": "rolling_7_days",
                "rolling30days": "rolling_30_days",
                "rolling90days": "rolling_90_days",
            }
            data["date_range"] = alias_map.get(dr, dr)
            
        # =========================================================
        # DIMENSION NORMALIZATION (Fix LLM Hallucinations)
        # =========================================================
        if "dimensions" in data and isinstance(data["dimensions"], list):
            normalized_dims = []
            for dim in data["dimensions"]:
                # Lowercase and strip
                d = str(dim).strip().lower().replace(" ", "_")
                
                # Check aliases
                if d in self.DIMENSION_ALIASES:
                    d = self.DIMENSION_ALIASES[d]
                
                normalized_dims.append(d)
            
            data["dimensions"] = normalized_dims
        
        return data


    # def _detect_date_range_keywords(self, user_query: str) -> Optional[str]:
    #     """
    #     Keyword-based date detection (fallback when LLM fails).
    #     Returns canonical snake_case format.
    #     NOTE: q1-q4 are handled by _extract_custom_dates_enhanced(), not here.
    #     """
    #     query_lower = user_query.lower()
        
    #     # Order matters - check specific phrases first!
    #     date_patterns = [
    #         # Financial year
    #         (["current financial year", "current fy", "this fy"], "current_financial_year"),
    #         (["last financial year", "previous fy", "last fy"], "last_financial_year"),
    #         (["fytd", "financial year to date"], "fytd"),
            
    #         # Year on Year (defaults to last 3 FYs)
    #         (["year on year", "yoy", "year-on-year", "last 3 years", "last 3 year"], "last_3_financial_years"),
    #         (["quarter on quarter", "qoq", "quarter-on-quarter"], "current_financial_year"),
            
    #         # Specific periods (with spaces)
    #         (["last month"], "last_month"),
    #         (["this month"], "this_month"),
    #         (["last quarter"], "last_quarter"),
    #         (["this quarter"], "this_quarter"),
    #         (["last week"], "last_week"),
    #         (["this week"], "this_week"),
    #         (["last year"], "last_financial_year"),
    #         (["this year"], "current_financial_year"),
    #         (["yesterday"], "yesterday"),
    #         (["today"], "today"),
            
    #         # To-date
    #         (["mtd", "month to date"], "mtd"),
    #         (["qtd", "quarter to date"], "qtd"),
    #         (["ytd", "year to date"], "ytd"),
            
    #         # Rolling
    #         (["last 7 days", "past 7 days"], "rolling_7_days"),
    #         (["last 30 days", "past 30 days"], "rolling_30_days"),
    #         (["last 90 days", "past 90 days"], "rolling_90_days"),
    #     ]
        
    #     for patterns, date_range in date_patterns:
    #         for pattern in patterns:
    #             if pattern in query_lower:
    #                 return date_range
        
    #     return None
    def _detect_date_range_keywords(self, user_query: str) -> Optional[str]:
        """
        ENHANCED keyword-based date detection with support for:
        1. Specific years (2024, 2025, etc.)
        2. Specific quarters (Q1, Q2, etc.)
        3. Last N periods (last 3 years, last 2 quarters, etc.)
        4. All existing relative date patterns
        
        Returns canonical snake_case format.
        """
        query_lower = user_query.lower().strip()
        
        # ========================================
        # 0. YoY / YEAR-WISE (MUST come before year regex to avoid fy_YYYY mismatch)
        # ========================================
        yoy_triggers = ["year on year", "yoy", "year-on-year", "y-o-y",
                        "year wise", "yearwise", "year-wise", "yearly",
                        "all years", "year by year", "each year", "per year", "annual trend"]
        if any(t in query_lower for t in yoy_triggers):
            return "last_3_financial_years_yoy"
        
        # ========================================
        # 1. SPECIFIC YEAR PATTERN (Highest Priority)
        # ========================================
        # Patterns: "in 2024", "for 2024", "during 2024", "2024"
        year_patterns = [
            r'\bin\s+(20\d{2})\b',
            r'\bfor\s+(20\d{2})\b', 
            r'\bduring\s+(20\d{2})\b',
            r'\byear\s+(20\d{2})\b',
            r'\b(20\d{2})\b'  # Standalone year
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, query_lower)
            if match:
                year = int(match.group(1))
                # Return financial year identifier
                return f"fy_{year}"
        
        # ========================================
        # 2. SPECIFIC QUARTER PATTERN  
        # ========================================
        # Patterns: "Q1", "Q2", "first quarter", "second quarter"
        quarter_map = {
            r'\bq1\b': 'q1',
            r'\bq2\b': 'q2', 
            r'\bq3\b': 'q3',
            r'\bq4\b': 'q4',
            r'\bfirst quarter\b': 'q1',
            r'\bsecond quarter\b': 'q2',
            r'\bthird quarter\b': 'q3',
            r'\bfourth quarter\b': 'q4'
        }
        
        for pattern, quarter_id in quarter_map.items():
            if re.search(pattern, query_lower):
                return quarter_id
        
        # ========================================
        # 3. LAST N PERIODS (Financial Year Aware)
        # ========================================
        
        # Last N financial years
        last_fy_match = re.search(r'\blast\s+(\d+)\s+(?:financial\s+)?years?\b', query_lower)
        if last_fy_match:
            n = int(last_fy_match.group(1))
            return f"last_{n}_financial_years"
        
        # Last N quarters (financial quarters)
        last_q_match = re.search(r'\blast\s+(\d+)\s+quarters?\b', query_lower)
        if last_q_match:
            n = int(last_q_match.group(1))
            return f"last_{n}_quarters"
        
        # Last N months
        last_m_match = re.search(r'\blast\s+(\d+)\s+months?\b', query_lower)
        if last_m_match:
            n = int(last_m_match.group(1))
            return f"last_{n}_months"
        
        # ========================================
        # 4. EXISTING RELATIVE TIME PATTERNS
        # ========================================
        
        # Order matters - check specific phrases first!
        date_patterns = [
            # Financial year
            (["current financial year", "current fy", "this fy"], "current_financial_year"),
            (["last financial year", "previous fy", "last fy"], "last_financial_year"),
            (["fytd", "financial year to date"], "fytd"),
            
            # Year on Year (defaults to last 3 FYs)
            (["year on year", "yoy", "year-on-year"], "last_3_financial_years"),
            (["quarter on quarter", "qoq", "quarter-on-quarter"], "current_financial_year"),
            
            # Specific periods (with spaces)
            (["last month"], "last_month"),
            (["this month"], "this_month"),
            (["last quarter"], "last_quarter"),
            (["this quarter"], "this_quarter"),
            (["last week"], "last_week"),
            (["this week"], "this_week"),
            (["last year"], "last_financial_year"),
            (["this year"], "current_financial_year"),
            (["yesterday"], "yesterday"),
            (["today"], "today"),
            
            # To-date
            (["mtd", "month to date"], "mtd"),
            (["qtd", "quarter to date"], "qtd"),
            (["ytd", "year to date"], "ytd"),
            
            # Rolling
            (["last 7 days", "past 7 days"], "rolling_7_days"),
            (["last 30 days", "past 30 days"], "rolling_30_days"),
            (["last 90 days", "past 90 days"], "rolling_90_days"),
        ]
        
        for patterns, date_range in date_patterns:
            for pattern in patterns:
                if pattern in query_lower:
                    return date_range
        
        return None


    # =====================================================
    # PROMPT BUILDER
    # =====================================================
    
    def _build_prompt(self, user_query: str) -> str:
        return f"""
    You are a semantic intent extractor for an enterprise sales analytics system.
    You MUST return a SINGLE valid JSON object.

    --------------------------- JSON SCHEMA (STRICT) ---------------------------
    {{
    "metric": "string",
    "dimensions": ["string"],
    "date_range": "string",
    "custom_dates": [{{"month_num": 4, "year": 2024}}] or null,
    "filters": object or null,
    "time_grain": "string or null",
    "is_trend": boolean,
    "compare_to": "string or null",
    "order_by": "string or null",
    "order_direction": "asc|desc|null",
    "limit": number or null
    }}

    --------------------------- DATE HANDLING (ENHANCED) ---------------------------

    **1. SPECIFIC YEARS:**
    - "total sales in 2024" → date_range: "fy_2024"
    - "sales in 2025" → date_range: "fy_2025"  
    - "revenue for 2023" → date_range: "fy_2023"

    **2. SPECIFIC QUARTERS (Financial Year):**
    - "sales in Q1" → date_range: "q1"
    - "Q2 sales" → date_range: "q2"
    - "first quarter" → date_range: "q1"

    **3. LAST N PERIODS (Financial Year Based):**
    - "last 3 years" → date_range: "last_3_financial_years"
    - "last 5 years" → date_range: "last_5_financial_years"
    - "last 2 quarters" → date_range: "last_2_quarters"
    - "last 4 quarters" → date_range: "last_4_quarters"
    - "last 6 months" → date_range: "last_6_months"

    **4. RELATIVE TIME PERIODS:**
    - "today" → "today"
    - "yesterday" → "yesterday"  
    - "this week" → "this_week"
    - "last week" → "last_week"
    - "this month" → "this_month"
    - "last month" → "last_month"
    - "this quarter" → "this_quarter"
    - "last quarter" → "last_quarter"
    - "this year" OR "current FY" → "current_financial_year"
    - "last year" OR "last FY" → "last_financial_year"

    **5. TO-DATE RANGES:**
    - "MTD" or "month to date" → "mtd"
    - "QTD" or "quarter to date" → "qtd"
    - "YTD" or "year to date" → "ytd"
    - "FYTD" or "financial year to date" → "fytd"

    **6. ROLLING WINDOWS:**
    - "last 7 days" → "rolling_7_days"
    - "last 30 days" → "rolling_30_days"
    - "last 90 days" → "rolling_90_days"

    **7. CUSTOM DATE (use custom_dates field):**
    - "April 2024" → date_range: "custom_range", custom_dates: [{{"month_num": 4, "year": 2024}}]

    **DEFAULT:** If NO date mentioned → "current_financial_year"

    **IMPORTANT:** Financial year runs April-March. FY 2024 = Apr 2024 to Mar 2025.

    --------------------------- EXAMPLES ---------------------------

    Query: "total sales in 2024"
    Output: {{"metric": "total_sales", "dimensions": [], "date_range": "fy_2024", "time_grain": null}}

    Query: "sales in Q1"  
    Output: {{"metric": "total_sales", "dimensions": [], "date_range": "q1", "time_grain": null}}

    Query: "last 3 years sales"
    Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_3_financial_years", "time_grain": "year"}}

    Query: "sales year on year"
    Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_3_financial_years_yoy", "time_grain": "year"}}

    Query: "tower wise sales last 2 quarters"
    Output: {{"metric": "total_sales", "dimensions": ["tower"], "date_range": "last_2_quarters", "time_grain": "quarter"}}

    Query: "total sales last month"
    Output: {{"metric": "total_sales", "dimensions": [], "date_range": "last_month", "time_grain": null}}

    Query: "project wise sales by channel last month"
    Output: {{"metric": "total_sales", "dimensions": ["sales_org_desc", "dist_channel_desc"], "date_range": "last_month", "time_grain": null}}

    Query: "sales value this quarter"
    Output: {{"metric": "sales_value", "dimensions": [], "date_range": "this_quarter", "time_grain": null}}

    ---------------------------USER QUESTION ---------------------------
    {user_query}

    Return JSON ONLY. No explanation.
    """

    # =====================================================
    # SAFE JSON PARSER (NEVER FAILS)
    # =====================================================
    
    def _parse_json(self, raw_text: str) -> dict:
        """
        Parses LLM output into a safe intent dictionary.
        Falls back gracefully if output is invalid.
        """
        fallback = {
            "metric": "total_sales",
            "dimensions": [],
            "date_range": "current_financial_year",
            "is_trend": False,
            "time_grain": None,
            "compare_to": None,
            "order_by": None,
            "order_direction": None,
            "limit": None,
        }

        if not raw_text or not raw_text.strip():
            return fallback

        text = raw_text.strip()

        # Try normal parse
        try:
            data = json.loads(text)
        except Exception:
            # Try extracting JSON from surrounding text
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group())
                except Exception:
                    return fallback
            else:
                # Try repairing truncated JSON
                repaired = text
                if repaired.startswith("{"):
                    if repaired.count('"') % 2 != 0:
                        repaired += '"'
                    if not repaired.endswith("}"):
                        repaired += "}"
                    try:
                        data = json.loads(repaired)
                    except Exception:
                        return fallback
                else:
                    return fallback

        # Merge with defaults
        for key, value in fallback.items():
            if key not in data or data[key] in ("", None):
                data[key] = value

        # Fix partial date ranges
        if "date_range" in data and data["date_range"]:
            date_range = data["date_range"]
            
            # Handle truncated responses
            if date_range == "current":
                data["date_range"] = "current_financial_year"
            elif date_range == "last":
                data["date_range"] = "last_financial_year"
            elif date_range == "this":
                data["date_range"] = "this_month"  # Best guess

        return data