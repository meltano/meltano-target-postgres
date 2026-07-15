"""Stdlib logging + a tiny metrics shim, replacing the original's singer-python dependency."""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s - %(levelname)s - %(message)s",
)

LOGGER = logging.getLogger("target_postgres")


class Counter:
    """Minimal stand-in for singer-python's metrics.Counter context manager."""

    def __init__(self, metric: str, tags: dict | None = None):
        self.metric = metric
        self.tags = tags or {}
        self.value = 0

    def increment(self, amount: int = 1) -> None:
        self.value += amount

    def __enter__(self) -> "Counter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        LOGGER.info(
            'METRIC: {"type": "counter", "metric": "%s", "value": %s, "tags": %s}',
            self.metric,
            self.value,
            self.tags,
        )
        return False
