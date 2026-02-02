import time
import logging
from contextlib import contextmanager

log = logging.getLogger("perf")

@contextmanager
def timer(label: str, trace_id: str = "-"):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = (time.perf_counter() - t0) * 1000
        log.info("[%s] %s: %.1f ms", trace_id, label, dt)
