import csv
import os
from pathlib import Path
from typing import Dict, List, Union

def atomic_write_text(path: Union[str, Path], text: str):
    """Writes text to a file atomically."""
    path = Path(path)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(temp_path, path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise

def read_csv_rows(path: Union[str, Path]) -> List[Dict[str, str]]:
    """Reads a CSV file into a list of dictionaries."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def merge_incremental(existing_rows: List[Dict[str, str]], new_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Merges new rows into an existing list of rows, overwriting the last 5 trading days.
    """
    if not new_rows:
        return existing_rows

    merged_dict = {row["date"]: row for row in existing_rows}
    new_rows_dict = {row["date"]: row for row in new_rows}

    # Overwrite last 5 trading days from existing rows with new data
    if existing_rows:
        last_five_dates = sorted([row["date"] for row in existing_rows])[-5:]
        for date in last_five_dates:
            if date in new_rows_dict:
                merged_dict[date] = new_rows_dict[date]

    # Add new rows
    merged_dict.update(new_rows_dict)

    merged_list = sorted(merged_dict.values(), key=lambda x: x["date"])
    return merged_list
