import pytest
import os
import sys

# Add parent directory to path to find python modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import fx_store
except ImportError:
    print("Warning: fx_store not found. Skipping binding tests.")
    fx_store = None

@pytest.mark.skipif(fx_store is None, reason="fx_store extension not built")
class TestFxStoreBindings:
    def test_store_creation(self):
        store = fx_store.PyFxStore()
        assert store is not None
        
    def test_import_and_query(self, tmp_path):
        store = fx_store.PyFxStore()
        
        # Create a dummy CSV file
        csv_content = """Date,Open,High,Low,Close,Volume
20230101 120000,1.10000,1.10050,1.09950,1.10020,100
20230101 120100,1.10020,1.10080,1.10010,1.10060,150
"""
        csv_file = tmp_path / "EURUSD.csv"
        csv_file.write_text(csv_content)
        
        # Import
        store.import_csv(str(csv_file), "EURUSD")
        
        # Query
        # 2023-01-01 12:00:00 = 1672574400
        start_ts = 1672574400 * 1_000_000_000
        end_ts = start_ts + 600 * 1_000_000_000 # 10 minutes later
        
        records = store.query_range("EURUSD", start_ts, end_ts)
        
        assert len(records) == 2
        assert records[0].open == 1.10000
        assert records[0].volume == 100
        assert records[1].close == 1.10060
        
    def test_get_symbols(self):
        store = fx_store.PyFxStore()
        # Note: Symbols are shared in the static/global store if implemented that way,
        # or local to the instance. The Rust implementation uses `Arc<FxStore>` but `new` creates a fresh one.
        # So it should be empty initially.
        assert len(store.get_symbols()) == 0
