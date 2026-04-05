"""
兼容层：
保留历史导入路径 state_store.StateStore，内部转发到新的 state_manager.StateManager。
"""

from state_manager import StateManager as StateStore  # noqa: F401
