# Medallion Architecture Volume Tests

Testy walidacji wolumenów danych w architekturze Medallion (Bronze → Silver → Gold).

## Quick Start

**Lokalnie:**
```bash
cd medallion_tests
pip install -r requirements.txt
pytest tests/test_data_volumes.py -v
```

Raport HTML zostanie wygenerowany automatycznie w `reports/test_report_YYYYMMDD_HHMMSS.html`

**Databricks (inline):**
```python
import pandas as pd
import sqlite3

# Ścieżki - zmień na swoje
BRONZE = "/dbfs/mnt/data/sales.csv"
SILVER_VALID = "/dbfs/mnt/data/silver_sales.csv"  
SILVER_FAULTY = "/dbfs/mnt/data/faulty_sales.csv"

bronze = len(pd.read_csv(BRONZE))
silver_valid = len(pd.read_csv(SILVER_VALID))
silver_faulty = len(pd.read_csv(SILVER_FAULTY))

# Test 1: Bronze == Silver total
assert bronze == silver_valid + silver_faulty, "Data loss!"
print(f"✓ Test 1: Bronze ({bronze}) == Silver ({silver_valid} + {silver_faulty})")

# Test 2: Silver valid == Gold (SQL table)
conn = sqlite3.connect(":memory:")
gold_df = pd.read_csv(SILVER_VALID)
gold_df["total_amount"] = gold_df["quantity"] * gold_df["price"]
gold_df.to_sql("gold_sales_data", conn, index=False)

gold = conn.execute("SELECT COUNT(*) FROM gold_sales_data").fetchone()[0]
assert silver_valid == gold, "Processing error!"
print(f"✓ Test 2: Silver valid ({silver_valid}) == Gold ({gold})")
```

## Struktura projektu

```
medallion_tests/
├── README.md
├── requirements.txt
├── pytest.ini
├── data/
│   ├── sales.csv           # Bronze: 100 rekordów
│   ├── silver_sales.csv    # Silver valid: 90 rekordów
│   └── faulty_sales.csv    # Silver faulty: 10 rekordów
├── tests/
│   ├── conftest.py         # Konfiguracja raportu HTML
│   └── test_data_volumes.py
└── reports/                # (tworzone automatycznie po uruchomieniu testów)
```

## Konfiguracja ścieżek

Edytuj `tests/test_data_volumes.py` - klasa `DataPaths`:

```python
class DataPaths:
    # PRODUCTION (uncomment for Databricks)
    # BRONZE_CSV = "/dbfs/mnt/data/bronze/sales.csv"
    # SILVER_VALID_CSV = "/dbfs/mnt/data/silver/silver_sales.csv"
    # SILVER_FAULTY_CSV = "/dbfs/mnt/data/silver/faulty_sales.csv"
    # GOLD_DB = "/dbfs/mnt/data/gold/gold.db"
    
    # TEST DATA (default)
    BRONZE_CSV = "data/sales.csv"
    SILVER_VALID_CSV = "data/silver_sales.csv"
    SILVER_FAULTY_CSV = "data/faulty_sales.csv"
    GOLD_DB = ":memory:"  # SQLite in-memory
```

## Opis testów

| Test | Walidacja |
|------|-----------|
| `test_bronze_equals_silver_total` | Bronze == Silver valid + Silver faulty |
| `test_silver_valid_equals_gold` | Silver valid == Gold |
| `test_bronze_not_empty` | Bronze > 0 |
| `test_gold_not_empty` | Gold > 0 |
| `test_faulty_records_identified` | Faulty >= 0 |
| `test_no_duplicates_in_gold` | Gold total == Gold distinct |
| `test_gold_has_required_columns` | Wszystkie kolumny obecne |
| `test_gold_total_amount_calculated` | total_amount == quantity * price |

## Oczekiwany output

```
MEDALLION ARCHITECTURE VOLUME SUMMARY
==================================================
Bronze (raw):                 100 records
Silver (valid):                90 records
Silver (faulty):               10 records
Gold (processed):              90 records
--------------------------------------------------
Faulty rate:                10.0%
Data loss:                     0 records
==================================================

============================== 9 passed ==============================
```

## Troubleshooting

**"No module named pandas"** → `pip install pandas`

**Testy nie znajdują CSV** → Uruchom z katalogu `medallion_tests/`
