"""Bounded console diagnostics for historical market-data RPC (Python 3.6)."""
import datetime
import functools
import os
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
    emit(stage, "BEGIN", **fields)
    try:
        yield
    except BaseException as exc:
        emit(stage, "ERROR", elapsed_ms=round((time.perf_counter()-started)*1000, 1),
             error_type=type(exc).__name__, **fields)
        raise
    else:
        emit(stage, "END", elapsed_ms=round((time.perf_counter()-started)*1000, 1), **fields)


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
