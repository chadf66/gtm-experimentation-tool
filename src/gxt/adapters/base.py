"""Base adapter interface for warehouses."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable


class WarehouseAdapter(ABC):
    """Abstract base class for warehouse adapters."""

    @abstractmethod
    def execute(self, sql: str) -> Any:
        """Execute a SQL statement in the warehouse and return result."""

    @abstractmethod
    def insert_rows(self, table: str, rows: Iterable[Dict[str, Any]]):
        """Insert rows into a table/ledger."""

    def hash_bucket_sql(self, column: str, salt: str = "", precision: int = 1_000_000) -> str:
        """Return a SQL expression (as a string) that computes a deterministic bucket in [0,1)

        The default implementation uses a portable SHA256 -> int approach where supported.
        Adapters should override with a more efficient native fingerprint when available.

        Example return value: "(ABS(FARM_FINGERPRINT(CAST({col} AS STRING))) % {p})/{p}.0"
        """
        # Generic SQL using SHA256 hex if the database supports TO_HEX(SHA256(...))
        # This is a safe fallback but adapters (e.g., BigQuery) should override.
        col = column
        p = precision
        if salt:
            # concat in SQL; adapters may need to change syntax per dialect
            col = f"CONCAT(CAST({column} AS STRING),'::','{salt}')"
        else:
            col = f"CAST({column} AS STRING)"

        # Note: many warehouses support SHA256/TO_HEX; this expression may need adapter tuning.
        return f"(CAST(TO_HEX(SHA256({col})) AS BIGNUMERIC) % {p})/{p}.0"

    def qualify_table(self, dataset: str, table: str) -> str:
        """Return a qualified identifier for a dataset/table pair.

        Default fallback: return a plain dot-separated identifier. Adapters
        should override to apply proper quoting and project/schema prefixes.
        """
        return f"{dataset}.{table}"

