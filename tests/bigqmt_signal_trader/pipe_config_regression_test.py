import ast
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).parents[2]


def test_pipe_client_defaults_to_no_formula_connection():
    code = """
from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient
client = BigQmtRpcClient(account_id='offline', redis_config={
    'transport':'pipe', 'formula_server':{}})
assert client.formula_server_config['enabled'] is False
assert not client._formula_router().supports('get_stock_list_in_sector')
"""
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), BIGQMT_FORMULA_ENABLED="1",
               BIGQMT_CLIENT_CONFIG_MODULE="missing_offline_test_config")
    subprocess.run([sys.executable, "-c", code], env=env, check=True,
                   capture_output=True, text=True)


def test_runtime_forwards_warmup_setting_including_pipe_default():
    tree = ast.parse((ROOT / "src" / "bigqmt_signal_trader_redis_rpc_runtime.py").read_text(encoding="utf-8"))
    values = [value for node in ast.walk(tree) if isinstance(node, ast.Dict)
              for key, value in zip(node.keys, node.values)
              if isinstance(key, ast.Constant) and key.value == "warm_context_data"]
    assert len(values) == 1
    expression = compile(ast.Expression(values[0]), "<warmup config>", "eval")
    for transport, config, expected in (
            ("pipe", {}, False), ("redis", {}, True),
            ("pipe", {"warm_context_data": False}, False),
            ("redis", {"warm_context_data": False}, False)):
        assert eval(expression, {"RPC_TRANSPORT": transport,
                                 "BIGQMT_REDIS_CONFIG": config}) is expected
