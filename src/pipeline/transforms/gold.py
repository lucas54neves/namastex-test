from __future__ import annotations

from .gold_macro import build_gold_macro
from .silver import add_gold_segments, build_gold

__all__ = ["add_gold_segments", "build_gold", "build_gold_macro"]
