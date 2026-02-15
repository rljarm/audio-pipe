#!/usr/bin/env python3

from pathlib import Path
from runpy import run_path


if __name__ == "__main__":
    target = Path(__file__).with_name("audio_converter 2.py")
    if not target.exists():
        raise FileNotFoundError(f"Missing required script: {target}")
    run_path(str(target), run_name="__main__")
