from backend.pipelines.build_indicators import _calculate_price_ratio

def test_calculate_price_ratio():
    """
    Tests the calculation of a price ratio between two synthetic ticker data sets.
    """
    ticker1_rows = [
        {"date": "2023-01-01", "close": "100.0"},
        {"date": "2023-01-02", "close": "101.0"},
        {"date": "2023-01-03", "close": "102.0"},
        {"date": "2023-01-05", "close": "104.0"},
    ]
    ticker2_rows = [
        {"date": "2023-01-02", "close": "50.0"},
        {"date": "2023-01-03", "close": "51.5"},
        {"date": "2023-01-04", "close": "52.0"},
        {"date": "2023-01-05", "close": "0"}, # Test for division by zero
    ]

    series = _calculate_price_ratio(ticker1_rows, ticker2_rows)

    # Only 2 dates should align: 01-02 and 01-03. 01-05 has a zero close.
    assert len(series) == 2

    # --- Check data for the first aligned date ---
    assert series[0]["date"] == "2023-01-02"
    # 101.0 / 50.0 = 2.02
    assert abs(series[0]["ratio"] - 2.02) < 1e-9

    # --- Check data for the second aligned date ---
    assert series[1]["date"] == "2023-01-03"
    # 102.0 / 51.5 = 1.9805825... -> rounded to 1.980583
    assert abs(series[1]["ratio"] - 1.980583) < 1e-9

def test_no_common_dates():
    """Tests the case where there are no common dates between the tickers."""
    ticker1_rows = [{"date": "2023-01-01", "close": "100.0"}]
    ticker2_rows = [{"date": "2023-01-02", "close": "50.0"}]

    series = _calculate_price_ratio(ticker1_rows, ticker2_rows)
    assert len(series) == 0
