from datetime import datetime
from zoneinfo import ZoneInfo

BERLIN_TZ = ZoneInfo("Europe/Berlin")

def get_berlin_now() -> datetime:
    """Returns the current time in Europe/Berlin timezone."""
    return datetime.now(BERLIN_TZ)

def to_iso_date(dt: datetime) -> str:
    """Formats a datetime object to an ISO YYYY-MM-DD string."""
    return dt.strftime("%Y-%m-%d")
