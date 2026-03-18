import yaml

dates_data = {
    "Document_Date": {
        "column": "Document_Date",
        "sql": 'TRY(DATE_PARSE(CAST("Document_Date" AS VARCHAR), \'%Y%m%d\'))',
        "supported_filters": [
            "today", "yesterday", "this_week", "last_week",
            "this_month", "last_month", "this_quarter", "last_quarter",
            "this_year", "last_year", "current_financial_year", 
            "last_financial_year", "ytd", "qtd", "mtd", "fytd",
            "rolling_7_days", "rolling_14_days", "rolling_15_days", "rolling_30_days",
            "rolling_60_days", "rolling_90_days", "rolling_6_months", 
            "rolling_12_months", "custom_range", "before_date", "after_date"
        ],
        "supported_grains": ["day", "month", "quarter", "year"]
    },
    "Booking_Created_On": {
        "column": "Booking_Created_On",
        "sql": 'TRY(DATE_PARSE(CAST("Booking_Created_On" AS VARCHAR), \'%Y%m%d\'))',
        "supported_filters": [
            "this_month", "last_month", "this_quarter", "last_quarter",
            "this_year", "last_year", "custom_range"
        ],
        "supported_grains": ["month", "quarter", "year"]
    }
}

with open("app/semantic/model/dates.yaml", "w", encoding="utf-8") as f:
    yaml.dump(dates_data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

print("✅ Fixed dates.yaml successfully!")

# Verify
with open("app/semantic/model/dates.yaml", "r", encoding="utf-8") as f:
    loaded = yaml.safe_load(f)
    print(f"\nKeys loaded: {list(loaded.keys())}")
    
    if "Document_Date" in loaded:
        dd = loaded["Document_Date"]
        print(f"✅ Document_Date has required fields:")
        print(f"   - column: {dd.get('column')}")
        print(f"   - sql: {dd.get('sql')[:50]}...")
        print(f"   - supported_filters: {len(dd.get('supported_filters', []))} filters")
        print(f"   - supported_grains: {dd.get('supported_grains')}")
