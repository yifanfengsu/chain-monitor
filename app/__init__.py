from pathlib import Path
import sys


APP_DIR = Path(__file__).resolve().parent
APP_DIR_TEXT = str(APP_DIR)
if APP_DIR_TEXT not in sys.path:
    sys.path.insert(0, APP_DIR_TEXT)
