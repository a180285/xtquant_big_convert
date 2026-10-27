import concurrent.futures
import re
import runpy
from pathlib import Path

trace = runpy.run_path(str(Path(__file__).parents[2] / "src" /
                          "bigqmt_signal_trader" / "rpc_call_trace.py"))


class Client:
    trace_calls = True
    transport_name = "pipe"

    @trace["trace_rpc_call"]
    def call(self, method, params=None, **kwargs):
        if method == "fail":
            raise TimeoutError("unchanged")
        return params


def test_concurrent_calls_have_matching_begin_and_end(capsys):
    client = Client()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda n: client.call("details", {"codes": list(range(n))}), range(12)))
    assert len(results) == 12
    lines = capsys.readouterr().out.splitlines()
    begins = [re.search(r"id=(\S+)", line).group(1) for line in lines if " BEGIN " in line]
    ends = [re.search(r"id=(\S+)", line).group(1) for line in lines if " END " in line]
    assert len(set(begins)) == 12
    assert sorted(begins) == sorted(ends)
    assert all("method=details" in line and "transport=pipe" in line for line in lines)
    assert any("rpc_call_trace_test.py:" in line for line in lines)


def test_error_preserves_exception_and_redacts_params(capsys):
    client = Client()
    try:
        client.call("fail", {"account_id": "sensitive-account", "password": "secret-value"})
    except TimeoutError as exc:
        assert str(exc) == "unchanged"
    else:
        raise AssertionError("exception swallowed")
    output = capsys.readouterr().out
    assert " BEGIN " in output and " ERROR " in output
    assert "error_type=TimeoutError" in output
    assert "sensitive-account" not in output and "secret-value" not in output
    assert " END " not in output


def test_disabled_logging_does_not_change_result(capsys):
    client = Client()
    client.trace_calls = False
    value = {"test": 1}
    assert client.call("read", value) is value
    assert not capsys.readouterr().out
