import queue
import concurrent.futures
import unittest
from unittest.mock import patch

from bigqmt_signal_trader.history_trace import phase, received, trace_request
from bigqmt_signal_trader.transports.pipe_transport import NamedPipeTransport


class HistoryTraceTest(unittest.TestCase):
    def test_real_dispatch_logs_native_conversion_and_publish(self):
        from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
        from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService
        from test_redis_rpc import FakeRedis, FakePositionProvider
        class Context:
            def get_market_data_ex_ori(self, *args, **kwargs):
                return {}
        provider = BigQmtMarketDataProvider(Context(), native_xtdata_enabled=False)
        handlers = BigQmtRpcHandlers(account_id="acct", market_data=provider,
                                     position_provider=FakePositionProvider(), order_gateway=None,
                                     allow_order_methods=False)
        service = RedisPubSubRpcService(FakeRedis(), handlers, account_id="acct")
        request = {"request_id": "integration-1", "account_id": "acct",
                   "method": "get_market_data_ex", "params": {
                       "period": "tick", "stock_list": ["10012309.SHO"],
                       "field_list": ["askPrice", "bidPrice"]}}
        with patch("builtins.print") as output:
            received(request)
            response = service.process_request(request)
        self.assertTrue(response["ok"], response["error"])
        lines = "\n".join(c.args[0] for c in output.call_args_list)
        for stage in ("server_receive", "server_dispatch", "server_handler",
                      "qmt_market_read", "qmt_api", "server_to_jsonable", "server_publish"):
            self.assertIn("stage="+stage, lines)
        self.assertIn("api=get_market_data_ex_ori", lines)

    def test_concurrent_requests_keep_ids_and_do_not_print_secrets(self):
        class Server:
            @trace_request("server_dispatch")
            def handle(self, request):
                with phase("qmt_market_read"):
                    return request["request_id"]
        def run(n):
            request = {"request_id": "req-%s" % n, "method": "get_market_data_ex",
                       "account_id": "secret-account", "params": {
                           "period": "tick", "stock_list": ["10012309.SHO"],
                           "password": "secret-password"}}
            received(request)
            return Server().handle(request)
        with patch("builtins.print") as output:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                self.assertEqual(list(pool.map(run, range(6))), ["req-%s" % n for n in range(6)])
        lines = [call.args[0] for call in output.call_args_list]
        for n in range(6):
            matching = [s for s in lines if "request_id=req-%s " % n in s]
            self.assertEqual(len(matching), 5)
            self.assertTrue(any("queue_ms=" in s for s in matching))
        self.assertNotIn("secret-account", "\n".join(lines))
        self.assertNotIn("secret-password", "\n".join(lines))

    def test_pipe_timeout_names_wait_stage_and_preserves_exception(self):
        transport = object.__new__(NamedPipeTransport)
        failure = TimeoutError("sentinel")
        request = {"request_id": "timeout-1", "method": "download_history_data2",
                   "params": {"stock_list": ["10012309.SHO"], "period": "tick"}}
        with patch.object(transport, "_client_handle", return_value=1), patch.object(
            transport, "_write"
        ), patch.object(transport, "_read_with_watchdog", side_effect=failure), patch("builtins.print") as output:
            with self.assertRaises(TimeoutError) as caught:
                transport.send_request(request, 60)
        self.assertIs(caught.exception, failure)
        lines = "\n".join(call.args[0] for call in output.call_args_list)
        self.assertIn("stage=client_wait_response event=ERROR", lines)
        self.assertIn("timeout_seconds=60", lines)
        self.assertNotIn("stage=client_decode", lines)

    def test_queued_reply_and_actual_write_have_separate_stages(self):
        transport = object.__new__(NamedPipeTransport)
        outbox = queue.Queue()
        request = {"request_id": "reply-1", "method": "get_market_data_ex",
                   "params": {"period": "tick"}, "_pipe_handle": 1, "_pipe_outbox": outbox}
        with patch.object(transport, "_cancel_read"), patch.object(transport, "_write") as write, patch("builtins.print") as output:
            transport.send_response(request, {"ok": True})
            write.assert_not_called()
            transport._write_reply(request, 1, outbox.get_nowait())
        write.assert_called_once()
        lines = "\n".join(c.args[0] for c in output.call_args_list)
        self.assertIn("stage=server_outbox event=QUEUED", lines)
        self.assertIn("stage=server_write event=END", lines)
        self.assertIn("bytes=", lines)

    def test_logging_failure_does_not_change_rpc(self):
        class Server:
            @trace_request("server_dispatch")
            def handle(self, request):
                return 42
        with patch("builtins.print", side_effect=OSError("console unavailable")):
            self.assertEqual(Server().handle({"method": "download_history_data2"}), 42)

    def test_non_history_is_silent(self):
        class Server:
            @trace_request("server_dispatch")
            def handle(self, request):
                with phase("handler"):
                    return 42
        with patch("builtins.print") as output:
            self.assertEqual(Server().handle({"method": "submit_order"}), 42)
        output.assert_not_called()
