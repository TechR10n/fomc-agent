"""Deprecated module.

Use:
  python -m src.transforms.to_processed
"""

from __future__ import annotations

from src.transforms.to_processed import main, to_processed as to_silver, to_processed_multi as to_silver_multi

__all__ = ["main", "to_silver", "to_silver_multi"]


if __name__ == "__main__":
    main()
