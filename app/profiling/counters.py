from collections import Counter
import logging

log = logging.getLogger("perf")
COUNTS = Counter()

def hit(key: str, trace_id: str = "-"):
    COUNTS[key] += 1
    n = COUNTS[key]
    if n in (10, 50, 100, 200, 500) or n % 1000 == 0:
        log.warning("[%s] HIT x%d: %s", trace_id, n, key)
