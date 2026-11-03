import contextlib
import io
import unittest
from bigqmt_signal_trader.history_trace import convert_history_result, trace_request
from bigqmt_signal_trader.redis_rpc import to_jsonable


class ConversionTraceTest(unittest.TestCase):
    def test_large_records_preserve_values_and_emit_bounded_blocks(self):
        payload = {"code": {"records": [{"ask": [1.0, float("nan")]} for _ in range(31000)]}}
        @trace_request("test")
        def run(self, request):
            return convert_history_result(payload, to_jsonable)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            actual = run(None, {"method": "get_market_data_ex", "params": {"period": "tick"}})
        self.assertEqual(actual, to_jsonable(payload))
        logs = output.getvalue()
        self.assertEqual(logs.count("stage=conversion_records_block event=BEGIN"), 6)
        self.assertIn("process_cpu_ms=", logs)
        self.assertIn("stage=conversion_runtime", logs)

    def test_without_history_keeps_original_converter(self):
        value = {"x": [1, 2]}
        calls = []
        def convert(item):
            calls.append(item)
            return item
        self.assertIs(convert_history_result(value, convert), value)
        self.assertEqual(calls, [value])

    def test_dataframe_conversion_preserves_envelope(self):
        import pandas as pd
        frame = pd.DataFrame({"price": [1.0, 2.0]})
        result = to_jsonable(frame)
        self.assertEqual(result["records"], [{"index": 0, "price": 1.0}, {"index": 1, "price": 2.0}])


if __name__ == "__main__":
    unittest.main()
