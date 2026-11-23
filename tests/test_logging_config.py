import logging

import pytest

from common.logging_config import configure_logging


def test_configure_logging_accepts_named_level() -> None:
    previous_level = logging.root.level
    try:
        configure_logging("DEBUG")
        assert logging.root.level == logging.DEBUG
    finally:
        logging.root.setLevel(previous_level)


def test_configure_logging_rejects_unknown_level() -> None:
    with pytest.raises(ValueError, match="Unknown logging level"):
        configure_logging("verbose")
