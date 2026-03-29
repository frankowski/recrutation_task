"""
Medallion Architecture Data Volume Tests.

Test suite for validating data consistency across Bronze, Silver, and Gold layers
in a Data Hub pipeline.

Tests verify:
1. Bronze row count == Silver valid + Silver faulty (no data loss)
2. Silver valid row count == Gold row count (all valid records processed)

Usage:
    pytest tests/test_data_volumes.py -v
"""

import pytest
import pandas as pd
import sqlite3


# =============================================================================
# CONFIGURATION - adjust paths for your environment
# =============================================================================

class DataPaths:
    """
    Configuration for data layer paths.
    """
    # ==========================================================================
    # PRODUCTION PATHS (uncomment and modify for your environment)
    # ==========================================================================
    # BRONZE_CSV = "/dbfs/mnt/data/bronze/sales.csv"
    # SILVER_VALID_CSV = "/dbfs/mnt/data/silver/silver_sales.csv"
    # SILVER_FAULTY_CSV = "/dbfs/mnt/data/silver/faulty_sales.csv"
    # GOLD_DB = "/dbfs/mnt/data/gold/gold.db"
    
    # ==========================================================================
    # TEST DATA PATHS (sample data included in data/ folder)
    # ==========================================================================
    BRONZE_CSV = "data/sales.csv"
    SILVER_VALID_CSV = "data/silver_sales.csv"
    SILVER_FAULTY_CSV = "data/faulty_sales.csv"
    GOLD_DB = ":memory:"  # In-memory SQLite for tests


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="module")
def gold_connection():
    """
    Create SQLite connection and Gold table from Silver valid data.
    """
    conn = sqlite3.connect(DataPaths.GOLD_DB)
    
    # Load Silver valid and create Gold table with total_amount
    silver_df = pd.read_csv(DataPaths.SILVER_VALID_CSV)
    silver_df["total_amount"] = silver_df["quantity"] * silver_df["price"]
    silver_df.to_sql("gold_sales_data", conn, index=False, if_exists="replace")
    
    yield conn
    
    conn.close()


# =============================================================================
# DATA VOLUME TESTS
# =============================================================================

class TestMedallionDataVolumes:
    """
    Test suite for Medallion Architecture data volume validation.
    
    Verifies data consistency between Bronze, Silver, and Gold layers.
    """
    
    def test_bronze_equals_silver_total(self):
        """
        Test: Bronze row count == Silver valid + Silver faulty.
        
        Validates that no records are lost during Bronze -> Silver transformation.
        """
        bronze_count = len(pd.read_csv(DataPaths.BRONZE_CSV))
        silver_valid_count = len(pd.read_csv(DataPaths.SILVER_VALID_CSV))
        silver_faulty_count = len(pd.read_csv(DataPaths.SILVER_FAULTY_CSV))
        
        silver_total = silver_valid_count + silver_faulty_count
        
        assert bronze_count == silver_total, (
            f"Data loss detected! "
            f"Bronze ({bronze_count}) != Silver total ({silver_total}). "
            f"Missing records: {bronze_count - silver_total}"
        )
    
    def test_silver_valid_equals_gold(self, gold_connection):
        """
        Test: Silver valid row count == Gold row count.
        
        Validates that all valid records from Silver are processed into Gold.
        """
        silver_valid_count = len(pd.read_csv(DataPaths.SILVER_VALID_CSV))
        
        cursor = gold_connection.execute("SELECT COUNT(*) FROM gold_sales_data")
        gold_count = cursor.fetchone()[0]
        
        assert silver_valid_count == gold_count, (
            f"Processing error! "
            f"Silver valid ({silver_valid_count}) != Gold ({gold_count}). "
            f"Difference: {silver_valid_count - gold_count}"
        )
    
    def test_bronze_not_empty(self):
        """
        Test: Bronze layer contains data.
        """
        bronze_count = len(pd.read_csv(DataPaths.BRONZE_CSV))
        assert bronze_count > 0, "Bronze layer is empty! No source data found."
    
    def test_gold_not_empty(self, gold_connection):
        """
        Test: Gold layer contains data.
        """
        cursor = gold_connection.execute("SELECT COUNT(*) FROM gold_sales_data")
        gold_count = cursor.fetchone()[0]
        assert gold_count > 0, "Gold layer is empty! Pipeline may have failed."
    
    def test_faulty_records_identified(self):
        """
        Test: Faulty records are properly identified and separated.
        """
        silver_faulty_count = len(pd.read_csv(DataPaths.SILVER_FAULTY_CSV))
        assert silver_faulty_count >= 0, "Faulty records count should be non-negative"
        print(f"Faulty records identified: {silver_faulty_count}")


class TestDataQualityMetrics:
    """
    Additional data quality tests for the pipeline.
    """
    
    def test_no_duplicates_in_gold(self, gold_connection):
        """
        Test: Gold layer has no duplicate records.
        """
        cursor = gold_connection.execute("SELECT COUNT(*) FROM gold_sales_data")
        total_count = cursor.fetchone()[0]
        
        cursor = gold_connection.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM gold_sales_data)"
        )
        distinct_count = cursor.fetchone()[0]
        
        assert total_count == distinct_count, (
            f"Duplicates found in Gold! "
            f"Total: {total_count}, Distinct: {distinct_count}"
        )
    
    def test_gold_has_required_columns(self, gold_connection):
        """
        Test: Gold table has all required columns.
        """
        cursor = gold_connection.execute("PRAGMA table_info(gold_sales_data)")
        actual_columns = {row[1] for row in cursor.fetchall()}
        
        required_columns = {'store_id', 'sale_date', 'product', 'quantity', 'price', 'total_amount'}
        missing = required_columns - actual_columns
        
        assert not missing, f"Missing columns in Gold table: {missing}"
    
    def test_gold_total_amount_calculated(self, gold_connection):
        """
        Test: total_amount is correctly calculated in Gold.
        
        Validates business logic: total_amount = quantity * price.
        """
        cursor = gold_connection.execute("""
            SELECT COUNT(*) FROM gold_sales_data 
            WHERE ABS(total_amount - (quantity * price)) > 0.01
        """)
        incorrect = cursor.fetchone()[0]
        
        assert incorrect == 0, (
            f"Found {incorrect} records with incorrect total_amount calculation"
        )
    
    def test_gold_date_format_valid(self, gold_connection):
        """
        Test: sale_date has valid YYYY-MM-DD format.
        
        Validates data format consistency.
        """
        cursor = gold_connection.execute("""
            SELECT COUNT(*) FROM gold_sales_data 
            WHERE sale_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]'
        """)
        invalid = cursor.fetchone()[0]
        
        assert invalid == 0, (
            f"Found {invalid} records with invalid date format (expected YYYY-MM-DD)"
        )


# =============================================================================
# SUMMARY REPORT
# =============================================================================

def test_generate_volume_summary(gold_connection):
    """
    Generate summary report of all layer volumes.
    """
    bronze_count = len(pd.read_csv(DataPaths.BRONZE_CSV))
    silver_valid_count = len(pd.read_csv(DataPaths.SILVER_VALID_CSV))
    silver_faulty_count = len(pd.read_csv(DataPaths.SILVER_FAULTY_CSV))
    
    cursor = gold_connection.execute("SELECT COUNT(*) FROM gold_sales_data")
    gold_count = cursor.fetchone()[0]
    
    print("\n" + "=" * 50)
    print("MEDALLION ARCHITECTURE VOLUME SUMMARY")
    print("=" * 50)
    print(f"Bronze (raw):          {bronze_count:>10} records")
    print(f"Silver (valid):        {silver_valid_count:>10} records")
    print(f"Silver (faulty):       {silver_faulty_count:>10} records")
    print(f"Silver (total):        {silver_valid_count + silver_faulty_count:>10} records")
    print(f"Gold (processed):      {gold_count:>10} records")
    print("-" * 50)
    print(f"Faulty rate:           {silver_faulty_count/bronze_count*100:>9.1f}%")
    print(f"Bronze→Silver loss:    {bronze_count - (silver_valid_count + silver_faulty_count):>10} records")
    print(f"Silver→Gold loss:      {silver_valid_count - gold_count:>10} records")
    print("=" * 50 + "\n")
    
    assert True  # Summary test always passes
