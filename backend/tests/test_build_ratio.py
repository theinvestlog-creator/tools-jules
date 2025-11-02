from backend.pipelines.build_indicators import (
    _calculate_price_ratio,
    _aggregate_to_monthly,
)


def test_calculate_price_ratio_with_adj_close():
    """
    Tests the calculation of a price ratio using the adj_close field.
    """
    ticker1_rows = [
        {"date": "2023-01-01", "adj_close": "100.0"},
        {"date": "2023-01-02", "adj_close": "101.0"},
        {"date": "2023-01-03", "adj_close": "102.0"},
        {"date": "2023-01-05", "adj_close": "104.0"},
    ]
    ticker2_rows = [
        {"date": "2023-01-02", "adj_close": "50.0"},
        {"date": "2023-01-03", "adj_close": "51.5"},
        {"date": "2023-01-04", "adj_close": "52.0"},
        {"date": "2023-01-05", "adj_close": "0"},  # Test for division by zero
    ]

    series = _calculate_price_ratio(ticker1_rows, ticker2_rows)

    assert len(series) == 2
    assert series[0]["date"] == "2023-01-02"
    assert abs(series[0]["ratio"] - 2.02) < 1e-9
    assert series[1]["date"] == "2023-01-03"
    assert abs(series[1]["ratio"] - 1.980583) < 1e-9


def test_no_common_dates():
    """Tests the case where there are no common dates between the tickers."""
    ticker1_rows = [{"date": "2023-01-01", "adj_close": "100.0"}]
    ticker2_rows = [{"date": "2023-01-02", "adj_close": "50.0"}]

    series = _calculate_price_ratio(ticker1_rows, ticker2_rows)
    assert len(series) == 0


def test_aggregate_to_monthly():
    """
    Tests the aggregation of a daily series to a monthly one.
    """
    daily_series = [
        {"date": "2023-01-15", "ratio": 1.5},
        {"date": "2023-01-31", "ratio": 1.6},  # Last for Jan
        {"date": "2023-02-01", "ratio": 1.7},
        {"date": "2023-02-28", "ratio": 1.8},  # Last for Feb
        {"date": "2023-03-01", "ratio": 1.9},  # Only for Mar
    ]

    monthly_series = _aggregate_to_monthly(daily_series)

    assert len(monthly_series) == 3
    assert monthly_series[0]["date"] == "2023-01-31"
    assert monthly_series[0]["ratio"] == 1.6
    assert monthly_series[1]["date"] == "2023-02-28"
    assert monthly_series[1]["ratio"] == 1.8
    assert monthly_series[2]["date"] == "2023-03-01"
    assert monthly_series[2]["ratio"] == 1.9
