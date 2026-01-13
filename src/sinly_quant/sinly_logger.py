"""Lightweight project logging utilities for the sinly_quant package.

This module provides a simple, centralized logger configuration you can
reuse across notebooks, scripts, and library code.

Usage
-----

    from sinly_quant.project_log import get_logger

    logger = get_logger(__name__)
    logger.info("Starting data load...")

You can also call ``setup_logging`` explicitly at application startup
if you want to override defaults (log level, path, etc.).
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
from typing import Optional

from .constants import CATALOG_PATH

# Default log directory under the project root. We reuse CATALOG_PATH's
# parent as an anchor to avoid guessing the installation location.
PROJECT_ROOT = Path(CATALOG_PATH).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "project.log"


def setup_logging(level: int = logging.INFO, log_to_file: bool = True) -> None:
    """Configure root logging for the project.

    This is idempotent and safe to call multiple times; subsequent calls
    will not add duplicate handlers.

    Args:
        level: Logging level for the root logger (e.g. ``logging.INFO``).
        log_to_file: Whether to also log to a rotating file under ``logs/``.
    """

    root = logging.getLogger()
    if getattr(root, "_sinly_quant_logging_configured", False):
        return

    root.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_fmt)
    root.addHandler(console_handler)

    # Optional file handler
    if log_to_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(console_fmt)
        root.addHandler(file_handler)

    # Mark as configured to avoid double configuration
    root._sinly_quant_logging_configured = True  # type: ignore[attr-defined]


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger configured for the sinly_quant project.

    If logging has not yet been configured via :func:`setup_logging`, it
    will be configured with default settings on first use.

    Args:
        name: Logger name. If ``None``, the root logger is returned.

    Returns:
        A :class:`logging.Logger` instance.
    """

    root = logging.getLogger()
    if not getattr(root, "_sinly_quant_logging_configured", False):
        setup_logging()

    return logging.getLogger(name)


__all__ = ["get_logger", "setup_logging", "LOG_DIR", "LOG_FILE"]

