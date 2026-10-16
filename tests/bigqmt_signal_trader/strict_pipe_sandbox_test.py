import ast
import builtins
import os
import subprocess
import sys
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


PUSH_CHECK = "import sys\nimport ast\nimport builtins\nimport ctypes\nfrom ctypes import wintypes\nfrom pathlib import Path\nimport os\nimport threading\nimport time\n\npath=Path(sys.argv[1])\ncode=compile(path.read_bytes(),str(path),'exec')\noriginal_import=builtins.__import__\noriginal_open=builtins.open\nviolations=[]\ndef guard(name,*args,**kwargs):\n if name in ('socket','redis','zmq','logging.handlers') or name.startswith(('redis.','zmq.')):\n  violations.append(name)\n  raise AssertionError('Forbidden import: '+name)\n return original_import(name,*args,**kwargs)\ndef denied(*args,**kwargs):\n violations.append('open')\n raise AssertionError('File I/O forbidden')\nbuiltins.__import__=guard\nbuiltins.open=denied\nns={'__name__':'pipe_push_check','__file__':str(path)}\ntry:\n exec(code,ns)\n assert ns['BIGQMT_REDIS_CONFIG']['quote_push']['enabled'] is True\n strategy=ns['_load_local_module']('bigqmt_signal_trader_strategy')\n class Context:\n  def subscribe_whole_quote(self,codes,callback):\n   self.callback=callback\n   return 1\n  def unsubscribe_quote(self,handle):\n   self.closed=handle\n ctx=Context()\n config={'quote_push':ns['BIGQMT_REDIS_CONFIG']['quote_push'],\n         'rpc':{'pipe':{'pipe_name':'qmt_push_test_'+str(os.getpid())}}}\n manager,publisher=strategy._build_quote_subscription_service(ctx,config,'pipe','test',None)\n rpc=ns['_load_local_module']('bigqmt_signal_trader.redis_rpc')\n handler=rpc.BigQmtRpcHandlers.__new__(rpc.BigQmtRpcHandlers)\n handler.quote_subscription_manager=manager\n reply=handler._handle_subscribe_whole_quote({'client_id':'test','sub_id':'test','codes':['510500.SH']})\n assert reply['push_endpoint']==publisher.bind_address\n push=ns['_load_local_module']('bigqmt_signal_trader.transports.pipe_push_channel')\n subscriber=push.PipeQuotePushChannel(connect_address=reply['push_endpoint'])\n received=[]\n event=threading.Event()\n def on_message(topic,data):\n  received.append((topic,data))\n  event.set()\n publisher.start_publisher()\n subscriber.start_subscriber([reply['topic']],on_message)\n try:\n  deadline=time.monotonic()+5\n  while publisher.publisher_subscriber_count()==0 and time.monotonic()<deadline:\n   time.sleep(0.02)\n  assert publisher.publisher_subscriber_count()==1,'events pipe did not connect'\n  ctx.callback({'510500.SH':{'lastPrice':5.5}})\n  assert event.wait(3),'no quote callback through events pipe'\n  assert received[0][1]['510500.SH']['lastPrice']==5.5,received\n  handler._handle_unsubscribe_whole_quote({'client_id':'test','sub_id':'test'})\n  assert ctx.closed==1\n finally:\n  subscriber.stop()\n  publisher.stop()\n assert not violations,violations\n print('PASS: subscription RPC and quote callback over real Windows events pipe; no socket/Redis/ZMQ/file open')\nfinally:\n builtins.__import__=original_import\n builtins.open=original_open\n"


def test_built_pipe_strategy_subscribes_and_pushes_without_network_or_files(tmp_path):
    root = Path(__file__).parents[2]
    output = tmp_path / "pipe_strategy.py"
    env = dict(os.environ, BIGQMT_BUILD_OUT=str(output), PYTHONDONTWRITEBYTECODE="1")
    subprocess.run([sys.executable, "-B", str(root / "tools" / "build_pipe_single_file_flat.py")],
                   env=env, check=True, capture_output=True, text=True)
    result = subprocess.run([sys.executable, "-c", PUSH_CHECK, str(output)],
                            check=True, capture_output=True, text=True, timeout=20)
    assert "PASS: subscription RPC and quote callback" in result.stdout
