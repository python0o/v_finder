from datetime import datetime, timezone

def utcnow():
    # timezone-aware UTC (avoids datetime.utcnow deprecation)
    return datetime.now(timezone.utc)