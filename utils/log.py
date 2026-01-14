from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Optional, Dict


LOG_DIR = "logs"
LOG_PATH = os.path.join(LOG_DIR, "pipeline.log")


def log_event(stage: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Append a structured log line to logs/pipeline.log.

    Each line is a JSON object:
        {
          "ts": "...",
          "stage": "PPP_INGEST",
          "message": "ppp_clean built",
          "extra": { ... }
        }
    """
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        rec: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "message": message,
        }
        if extra is not None:
            rec["extra"] = extra

        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        # Logging must never crash the app; fail silently.
        pass
