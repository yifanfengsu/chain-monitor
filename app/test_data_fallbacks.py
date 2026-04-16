import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


APP_DIR = Path(__file__).resolve().parent
FILTER_PATH = APP_DIR / "filter.py"
LP_REGISTRY_PATH = APP_DIR / "lp_registry.py"
CLMM_REGISTRY_PATH = APP_DIR / "clmm_registry.py"
CLMM_POSITION_STATE_PATH = APP_DIR / "clmm_position_state.py"


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
        self.assertEqual({}, module.ENTITY_BOOK)
        self.assertEqual({}, module.ADDRESS_ALIASES)

    def test_missing_optional_entity_books_fall_back_cleanly(self) -> None:
        missing_entities = APP_DIR.parent / "data" / "entities.json"
        missing_aliases = APP_DIR.parent / "data" / "address_aliases.json"
        original_open = Path.open

        def _open(path_obj, *args, **kwargs):
            if Path(path_obj) in {missing_entities, missing_aliases}:
                raise FileNotFoundError(str(path_obj))
            return original_open(path_obj, *args, **kwargs)

        with patch("pathlib.Path.open", new=_open):
            module = self._load_module("filter_missing_optional_books_test", FILTER_PATH)

        self.assertEqual({}, module.ENTITY_BOOK)
        self.assertEqual({}, module.ADDRESS_ALIASES)
        self.assertIn("wallet_function", module.get_address_meta(""))
        self.assertIn("entity_attribution_strength", module.get_address_meta(""))

    def test_missing_lp_pools_json_falls_back_to_empty_lp_pool_book(self) -> None:
        missing_path = APP_DIR.parent / "data" / "lp_pools.json"
        with patch("pathlib.Path.open", new=self._missing_file_open(missing_path)):
            module = self._load_module("lp_registry_missing_pools_json_test", LP_REGISTRY_PATH)

        self.assertEqual([], module.LP_POOL_BOOK)
        self.assertEqual({}, module.LP_POOLS)
        self.assertEqual({}, module.ACTIVE_LP_POOLS)
        self.assertFalse(module.is_lp_pool("0xpool"))
        self.assertIsNone(module.get_lp_pool_meta("0xpool"))

    def test_missing_clmm_managers_json_falls_back_to_empty_registry(self) -> None:
        missing_path = APP_DIR.parent / "data" / "clmm_managers.json"
        with patch("pathlib.Path.open", new=self._missing_file_open(missing_path)):
            module = self._load_module("clmm_registry_missing_managers_json_test", CLMM_REGISTRY_PATH)

        self.assertEqual([], module.CLMM_MANAGER_BOOK)
        self.assertEqual({}, module.CLMM_MANAGERS_BY_CHAIN)
        self.assertFalse(module.is_clmm_position_manager("0xmanager"))
        self.assertIsNone(module.get_clmm_manager_meta("0xmanager"))

    def test_missing_clmm_position_cache_falls_back_to_empty_runtime_state(self) -> None:
        missing_path = APP_DIR.parent / "data" / "clmm_positions.cache.json"
        with patch("pathlib.Path.open", new=self._missing_file_open(missing_path)):
            module = self._load_module("clmm_position_state_missing_cache_test", CLMM_POSITION_STATE_PATH)

        self.assertEqual({}, module.POSITION_STATES)
        self.assertEqual([], module.POSITION_ACTION_HISTORY)
        self.assertEqual("", module.build_position_key("", "ethereum", "0xmanager", "1"))
        self.assertEqual([], module.recent_position_actions())


if __name__ == "__main__":
    unittest.main()
