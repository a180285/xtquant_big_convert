"""The mounted flat shell must forward native download functions to RPC."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]


class FlatDownloadBindingTest(unittest.TestCase):
    def test_shell_forwards_downloads_from_its_own_namespace(self):
        builder = ROOT / "tools/build_no_redis_single_file_flat.py"
        tree = ast.parse(builder.read_text(encoding="utf-8"))
        template = next(
            ast.literal_eval(node.value) for node in tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "FLAT_TEMPLATE"
                    for t in node.targets)
        )
        shell = ast.parse(template.replace("__MODULE_FUNCS_BLOCK__", ""))
        binding = next(
            node for node in shell.body if isinstance(node, ast.Try)
            and any(isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Attribute)
                    and child.func.attr == "bind_runtime_api"
                    for child in ast.walk(node))
        )
        calls = []
        native = lambda *args: None
        namespace = {name: native for name in (
            "download_history_data", "download_history_data2", "down_history_data")}

        def capture(actual):
            self.assertIs(actual, namespace)
            return {name: value for name, value in actual.items()
                    if name.startswith(("download_", "down_"))}

        namespace.update(
            _strategy=SimpleNamespace(capture_qmt_injected_funcs=capture),
            _runtime=SimpleNamespace(bind_runtime_api=lambda **kw: calls.append(kw)),
        )
        exec(compile(ast.Module(body=[binding], type_ignores=[]), "shell", "exec"),
             namespace)
        self.assertEqual(len(calls), 1)
        for name in ("download_history_data", "download_history_data2", "down_history_data"):
            self.assertIs(calls[0]["extra_funcs"][name], native)

    def test_packaged_builder_matches_source(self):
        self.assertEqual(
            (ROOT / "tools/build_no_redis_single_file_flat.py").read_bytes(),
            (ROOT / "src/bigqmt_signal_trader/_singlefile/"
             "build_no_redis_single_file_flat.py.txt").read_bytes(),
        )


if __name__ == "__main__":
    unittest.main()
