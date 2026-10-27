"""Client-side tracing before any bridge transport or formula routing."""
import datetime
import functools
import itertools
import os
import sys
import threading
import time

_SEQUENCE = itertools.count(1)
_LOCK = threading.Lock()


def _summary(value, depth=0):
    if depth > 2:
        return "..."
    if isinstance(value, dict):
        items = list(value.items())[:12]
        return {str(k): ("<redacted>" if any(token in str(k).lower()
                for token in ("password", "token", "secret", "account"))
                else _summary(v, depth + 1)) for k, v in items}
    if isinstance(value, (list, tuple)):
        return {"count": len(value), "sample": [_summary(v, depth + 1) for v in value[:3]]}
    if isinstance(value, str):
        return value[:120] + ("..." if len(value) > 120 else "")
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return "<%s>" % type(value).__name__


def _emit(message):
    # Diagnostics must survive a blocked request and must not affect trading.
    try:
        with _LOCK:
            stamp = datetime.datetime.now().isoformat(timespec="milliseconds")
            print("%s [bridge_call] %s" % (stamp, message), flush=True)
    except Exception:
        pass


def trace_rpc_call(func):
    @functools.wraps(func)
    def traced(self, method, params=None, *args, **kwargs):
        if not getattr(self, "trace_calls", False):
            return func(self, method, params, *args, **kwargs)
        call_id = "%s-%s" % (os.getpid(), next(_SEQUENCE))
        started = time.perf_counter()
        frame = sys._getframe(1)
        callers = []
        try:
            while frame is not None and len(callers) < 3:
                path = frame.f_code.co_filename
                if os.path.dirname(path) != os.path.dirname(__file__):
                    callers.append("%s:%s:%s" % (path, frame.f_lineno, frame.f_code.co_name))
                frame = frame.f_back
        finally:
            del frame
        prefix = "id=%s method=%s transport=%s thread=%s" % (
            call_id, method, getattr(self, "transport_name", "unknown"),
            threading.current_thread().name)
        _emit("BEGIN %s params=%s caller=%s" % (
            prefix, repr(_summary(params))[:1600], " <- ".join(callers)))
        try:
            result = func(self, method, params, *args, **kwargs)
        except BaseException as exc:
            _emit("ERROR %s elapsed_ms=%.1f error_type=%s" % (
                prefix, (time.perf_counter() - started) * 1000, type(exc).__name__))
            raise
        _emit("END %s elapsed_ms=%.1f result_type=%s" % (
            prefix, (time.perf_counter() - started) * 1000, type(result).__name__))
        return result
    return traced
