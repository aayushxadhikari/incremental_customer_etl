"""Entry point for the incremental customer ETL.

Usage:
    python main.py
"""

import logging

from config.settings import Settings
from src.pipeline import run


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    run(settings)


if __name__ == "__main__":
    main()
