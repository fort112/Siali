import time
import logging
from aiogram import BaseMiddleware

log = logging.getLogger("perf")

class PerfMiddleware(BaseMiddleware):
    def __init__(self, slow_ms: int = 200):
        self.slow_ms = slow_ms

    async def __call__(self, handler, event, data):
        update = data.get("event_update")
        trace_id = f"u{getattr(update, 'update_id', 'na')}"
        data["trace_id"] = trace_id

        t0 = time.perf_counter()
        try:
            return await handler(event, data)
        finally:
            dt = (time.perf_counter() - t0) * 1000
            lvl = logging.WARNING if dt >= self.slow_ms else logging.INFO
            log.log(lvl, "[%s] update %.1f ms | %s", trace_id, dt, type(event).__name__)
