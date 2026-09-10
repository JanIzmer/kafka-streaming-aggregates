"""Postgres sink: aggregates, dedup ledger, late events, dead letters."""

from orders_stream.store.repository import Repository

__all__ = ["Repository"]
