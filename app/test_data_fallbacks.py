import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


APP_DIR = Path(__file__).resolve().parent
FILTER_PATH = APP_DIR / "filter.py"
LP_REGISTRY_PATH = APP_DIR / "lp_registry.py"


class DataFallbackImportTests(unittest.TestCase):
    def _load_module(self, alias: str, module_path: Path):
        spec = importlib.util.spec_from_file_location(alias, module_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules[alias] = module
        try:
            spec.loader.exec_module(module)
            return module
        finally:
            sys.modules.pop(alias, None)

    def _missing_file_open(self, missing_path: Path):
        original_open = Path.open

        def _open(path_obj, *args, **kwargs):
            if Path(path_obj) == missing_path:
                raise FileNotFoundError(str(missing_path))
            return original_open(path_obj, *args, **kwargs)

        return _open

    def test_missing_addresses_json_falls_back_to_empty_address_book(self) -> None:
        missing_path = APP_DIR.parent / "data" / "addresses.json"
        with patch("pathlib.Path.open", new=self._missing_file_open(missing_path)):
            module = self._load_module("filter_missing_addresses_json_test", FILTER_PATH)

        self.assertEqual({}, module.ADDRESS_BOOK)
        self.assertEqual(set(), module.ALL_WATCH_ADDRESSES)
        self.assertEqual(set(), module.WATCH_ADDRESSES)

    def test_missing_lp_pools_json_falls_back_to_empty_lp_pool_book(self) -> None:
        missing_path = APP_DIR.parent / "data" / "lp_pools.json"
        with patch("pathlib.Path.open", new=self._missing_file_open(missing_path)):
            module = self._load_module("lp_registry_missing_pools_json_test", LP_REGISTRY_PATH)

        self.assertEqual([], module.LP_POOL_BOOK)
        self.assertEqual({}, module.LP_POOLS)
        self.assertEqual({}, module.ACTIVE_LP_POOLS)
        self.assertFalse(module.is_lp_pool("0xpool"))
        self.assertIsNone(module.get_lp_pool_meta("0xpool"))


if __name__ == "__main__":
    unittest.main()
