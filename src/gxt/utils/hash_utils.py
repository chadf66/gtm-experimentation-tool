"""Deprecated hashing utilities (removed).

This module was intentionally removed to enforce warehouse-side hashing.
If you see this file present, it has been replaced with a deprecation stub.
Please use adapter-provided SQL hashing expressions instead.
"""

raise ImportError(
    """
    src.gxt.utils.hash_utils has been removed. Use adapter.hash_bucket_sql or a
    warehouse-side hashing function instead.
    """
)
