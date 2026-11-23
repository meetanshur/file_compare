"""Application logging configuration."""

import logging


def configure_logging(level: str | int = "INFO") -> None:
    """Configure standard application logging with a consistent format."""
    if isinstance(level, str):
        resolved_level = logging.getLevelNamesMapping().get(level.upper())
        if not isinstance(resolved_level, int):
            raise ValueError(f"Unknown logging level: {level}")
    elif isinstance(level, int):
        resolved_level = level
    else:
        raise TypeError("Logging level must be a string or integer")

    logging.basicConfig(
        level=resolved_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger().setLevel(resolved_level)
