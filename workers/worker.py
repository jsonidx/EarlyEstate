"""Worker entrypoint. Run with: python workers/worker.py"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Ensure project root is on PYTHONPATH when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)

from app.jobs.runner import run_worker_loop


async def main() -> None:
    await run_worker_loop()


if __name__ == "__main__":
    asyncio.run(main())
