"""Bounded console diagnostics for historical market-data RPC (Python 3.6)."""
import datetime
import functools
import os
import sys
import threading
import time
from contextlib import contextmanager

_LOCAL = threading.local()


def is_history(request):
    params = request.get("params") or {}
    return (str(request.get("method") or "").startswith("download_history_data")
            or (request.get("method") in ("get_market_data_ex", "get_market_data", "get_local_data")
                and params.get("period") == "tick"))


def emit(stage, event, **fields):
    request = getattr(_LOCAL, "request", None)
    if request is None:
        return
    try:
        params = request.get("params") or {}
        codes = params.get("stock_list") or params.get("stock_code") or []
        if isinstance(codes, str):
            codes = [codes]
        detail = " ".join("%s=%s" % (key, value) for key, value in sorted(fields.items()))
        print("%s [history_rpc] request_id=%s method=%s stage=%s event=%s "
              "pid=%s thread=%s codes=%s sample=%s period=%s start=%s end=%s %s" % (
                  datetime.datetime.now().isoformat(), request.get("request_id", "?"),
                  request.get("method"), stage, event, os.getpid(),
                  threading.current_thread().name, len(codes), repr(codes[:2]),
                  params.get("period", ""), params.get("start_time", ""),
                  params.get("end_time", ""), detail), flush=True)
    except Exception:
        # Diagnostics must never stop the strategy or replace the RPC exception.
        pass


@contextmanager
def phase(stage, **fields):
    started = time.perf_counter()
    cpu_started = time.process_time()
    thread_clock = getattr(time, "thread_time", None)
    thread_started = thread_clock() if thread_clock else None
    emit(stage, "BEGIN", **fields)
    try:
        yield
    except BaseException as exc:
        emit(stage, "ERROR", elapsed_ms=round((time.perf_counter()-started)*1000, 1),
             error_type=type(exc).__name__, **fields)
        raise
    else:
        timing = {"process_cpu_ms": round((time.process_time()-cpu_started)*1000, 1)}
        if thread_clock is not None:
            timing["thread_cpu_ms"] = round((thread_clock()-thread_started)*1000, 1)
        emit(stage, "END", elapsed_ms=round((time.perf_counter()-started)*1000, 1),
             **dict(fields, **timing))


def trace_request(stage):
    def decorate(func):
        @functools.wraps(func)
        def wrapped(self, request, *args, **kwargs):
            if not is_history(request or {}):
                return func(self, request, *args, **kwargs)
            previous = getattr(_LOCAL, "request", None)
            _LOCAL.request = request
            try:
                fields = {}
                received = request.get("_history_received_at")
                if stage == "server_dispatch" and received is not None:
                    fields["queue_ms"] = round((time.time()-received)*1000, 1)
                with phase(stage, **fields):
                    return func(self, request, *args, **kwargs)
            finally:
                _LOCAL.request = previous
        return wrapped
    return decorate


def trace_stage(stage):
    def decorate(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            with phase(stage):
                return func(*args, **kwargs)
        return wrapped
    return decorate


def received(request):
    if not is_history(request):
        return
    request["_history_received_at"] = time.time()
    previous = getattr(_LOCAL, "request", None)
    _LOCAL.request = request
    try:
        emit("server_receive", "RECEIVED")
    finally:
        _LOCAL.request = previous


def _type_name(value):
    kind = type(value)
    return kind.__module__ + "." + kind.__name__


def conversion_phase(stage, value):
    """Bounded metadata only: never stringify a complete market payload."""
    fields = {"input_type": _type_name(value)}
    if isinstance(value, (dict, list, tuple)):
        fields["size"] = len(value)
        sample = [next(iter(value.values()))] if isinstance(value, dict) and value else value[:1] if not isinstance(value, dict) else []
        if sample:
            fields["sample_type"] = _type_name(sample[0])
    return phase(stage, **fields)


def convert_history_result(value, converter):
    """Split only ordinary JSON containers; keep leaf conversion unchanged.

    Maximum three container levels and six timed list blocks. This avoids
    per-scalar tracing overhead while locating slow records or columns.
    """
    if getattr(_LOCAL, "request", None) is None:
        return converter(value)
    emit("conversion_runtime", "INFO", python=sys.version.split()[0],
         trace=_type_name(sys.gettrace()), profile=_type_name(sys.getprofile()),
         switch_interval=sys.getswitchinterval())

    def visit(item, depth, path):
        with conversion_phase("convert_" + path, item):
            if type(item) is dict and depth < 3 and len(item) <= 16:
                return {str(key): visit(child, depth + 1, path + "_" + str(pos))
                        for pos, (key, child) in enumerate(item.items())}
            if type(item) is list and len(item) > 5000:
                result = []
                block = max(5000, (len(item) + 5) // 6)
                for start in range(0, len(item), block):
                    with phase("conversion_records_block", start=start,
                               stop=min(start + block, len(item)), path=path):
                        result.extend(converter(item[start:start + block]))
                return result
            return converter(item)
    return visit(value, 0, "root")
