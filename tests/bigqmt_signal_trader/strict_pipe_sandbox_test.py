import ast
import builtins
from pathlib import Path


def test_strict_pipe_logging_uses_no_files_or_network(monkeypatch, capsys):
    source = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader" / "logging_setup.py"
    code = compile(source.read_bytes(), str(source), "exec")
    monkeypatch.setenv("BIGQMT_NO_FILE_IO", "1")
    monkeypatch.setenv("BIGQMT_LOG_TO_STDOUT", "1")
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        assert name not in ("logging", "logging.handlers", "socket")
        return original_import(name, *args, **kwargs)

    def denied_open(*args, **kwargs):
        raise AssertionError("file I/O forbidden")

    namespace = {"__name__": "strict_pipe_logger", "__builtins__":
                 dict(vars(builtins), __import__=guarded_import, open=denied_open)}
    exec(code, namespace)
    namespace["get_logger"]("strict").info("pipe sandbox ready")
    assert "pipe sandbox ready" in capsys.readouterr().out


def test_order_journal_is_guarded_in_strict_sandbox():
    source = Path(__file__).parents[2] / "src" / "bigqmt_signal_trader_strategy.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    guards = [node for node in ast.walk(tree) if isinstance(node, ast.If)
              and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                      and call.func.attr == "enable_order_identity_persistence"
                      for statement in node.body for call in ast.walk(statement))]
    assert len(guards) == 1
    guard = compile(ast.Expression(guards[0].test), "<guard>", "eval")
    class Environment:
        environ = {"BIGQMT_NO_FILE_IO": "1"}
    assert not eval(guard, {"_store_redis": None, "os": Environment})
    Environment.environ = {}
    assert eval(guard, {"_store_redis": None, "os": Environment})
