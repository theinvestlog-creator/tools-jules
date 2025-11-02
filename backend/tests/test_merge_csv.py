from backend.util.io_safe import merge_incremental

def test_merge_empty_existing():
    """Test merging into an empty list of existing rows."""
    existing = []
    new = [{"date": "2023-01-01", "close": "100"}]
    merged = merge_incremental(existing, new)
    assert merged == [{"date": "2023-01-01", "close": "100"}]

def test_merge_empty_new():
    """Test merging with an empty list of new rows."""
    existing = [{"date": "2023-01-01", "close": "100"}]
    new = []
    merged = merge_incremental(existing, new)
    assert merged == [{"date": "2023-01-01", "close": "100"}]

def test_merge_no_overlap():
    """Test merging with no overlapping dates."""
    existing = [{"date": "2023-01-01", "close": "100"}]
    new = [{"date": "2023-01-02", "close": "101"}]
    merged = merge_incremental(existing, new)
    assert merged == [
        {"date": "2023-01-01", "close": "100"},
        {"date": "2023-01-02", "close": "101"},
    ]

def test_overwrite_last_five_days():
    """Test that the last 5 days of existing data are overwritten."""
    existing = [
        {"date": f"2023-01-{i:02d}", "close": str(100 + i)} for i in range(1, 11)
    ]
    # New data overlaps with the last 3 days of existing data and adds 2 new days
    new = [
        {"date": "2023-01-08", "close": "999"}, # Overwrite
        {"date": "2023-01-10", "close": "888"}, # Overwrite
        {"date": "2023-01-11", "close": "777"}, # New
    ]

    merged = merge_incremental(existing, new)

    merged_map = {row["date"]: row for row in merged}

    assert len(merged) == 11
    assert merged_map["2023-01-07"]["close"] == "107" # Unchanged
    assert merged_map["2023-01-08"]["close"] == "999" # Overwritten
    assert merged_map["2023-01-09"]["close"] == "109" # Unchanged
    assert merged_map["2023-01-10"]["close"] == "888" # Overwritten
    assert merged_map["2023-01-11"]["close"] == "777" # New
    assert merged[-1]["date"] == "2023-01-11" # Check sorting

def test_deduplication_and_sorting():
    """Test that duplicates are removed and the list is sorted."""
    existing = [
        {"date": "2023-01-01", "close": "100"},
        {"date": "2023-01-03", "close": "102"},
    ]
    new = [
        {"date": "2023-01-02", "close": "101"},
        {"date": "2023-01-01", "close": "100"}, # Duplicate
    ]

    merged = merge_incremental(existing, new)

    assert len(merged) == 3
    assert merged[0]["date"] == "2023-01-01"
    assert merged[1]["date"] == "2023-01-02"
    assert merged[2]["date"] == "2023-01-03"
