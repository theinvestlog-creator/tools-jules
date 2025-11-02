import json
from typing import Any, Dict

def log_structured(data: Dict[str, Any]):
    """Prints a structured log message as a JSON string."""
    print(json.dumps(data))
