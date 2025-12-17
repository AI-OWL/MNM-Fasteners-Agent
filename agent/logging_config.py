"""
Logging configuration for the MNM Agent.
Uses loguru for enhanced logging capabilities.
"""

import sys
from pathlib import Path
from loguru import logger
from typing import Optional

from agent.config import AgentConfig


def setup_logging(config: AgentConfig, console: bool = True) -> None:
    """
    Configure logging for the agent.
    
    Args:
        config: Agent configuration
        console: Whether to output to console (disable for Windows service)
    """
    
    # Remove default handler
    logger.remove()
    
    # Log format
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    simple_format = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}"
    
    # Console output (for development)
    if console:
        logger.add(
            sys.stdout,
            format=log_format,
            level=config.log_level,
            colorize=True,
        )
    
    # File output
    log_path = Path(config.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        str(log_path),
        format=simple_format,
        level=config.log_level,
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        enqueue=True,  # Thread-safe
    )
    
    # Error file (separate file for errors only)
    error_log_path = log_path.parent / "error.log"
    logger.add(
        str(error_log_path),
        format=simple_format,
        level="ERROR",
        rotation="10 MB",
        retention="60 days",
        compression="zip",
        enqueue=True,
    )
    
    # Sentry integration (optional)
    if config.sentry_dsn:
        try:
            import sentry_sdk
            sentry_sdk.init(
                dsn=config.sentry_dsn,
                traces_sample_rate=0.1,
                environment="production",
            )
            logger.info("Sentry error tracking enabled")
        except ImportError:
            logger.warning("Sentry SDK not installed, error tracking disabled")
    
    logger.info(f"Logging initialized - Level: {config.log_level}, File: {log_path}")


class TaskLogger:
    """Context logger for task execution."""
    
    def __init__(self, task_id: str, task_type: str):
        self.task_id = task_id
        self.task_type = task_type
        self._logger = logger.bind(task_id=task_id, task_type=task_type)
    
    def info(self, message: str, **kwargs):
        self._logger.info(f"[Task:{self.task_id[:8]}] {message}", **kwargs)
    
    def debug(self, message: str, **kwargs):
        self._logger.debug(f"[Task:{self.task_id[:8]}] {message}", **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._logger.warning(f"[Task:{self.task_id[:8]}] {message}", **kwargs)
    
    def error(self, message: str, **kwargs):
        self._logger.error(f"[Task:{self.task_id[:8]}] {message}", **kwargs)
    
    def exception(self, message: str, **kwargs):
        self._logger.exception(f"[Task:{self.task_id[:8]}] {message}", **kwargs)

