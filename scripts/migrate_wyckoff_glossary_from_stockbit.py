#!/usr/bin/env python3
"""Salin stockbit.wyckoff_glossary → invezgood.wyckoff_glossary."""

from __future__ import annotations

import os
import sys


def cql_str(value: str | None) -> str:
    if value is None:
        return "null"
    return "'" + str(value).replace("'", "''") + "'"


def main() -> int:
    try:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
    except ImportError:
        print("Install: pip install cassandra-driver", file=sys.stderr)
        return 1

    uri = os.environ.get("SCYLLA_URI", "127.0.0.1:9042")
    user = os.environ.get("SCYLLA_USER", "cassandra")
    password = os.environ.get("SCYLLA_PASSWORD", "")

    host, port_str = uri.rsplit(":", 1) if ":" in uri else (uri, "9042")
    auth = PlainTextAuthProvider(username=user, password=password) if user else None
    cluster = Cluster([host], port=int(port_str), auth_provider=auth)
    session = cluster.connect()

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS invezgood.wyckoff_glossary (
            name text PRIMARY KEY,
            long_name text,
            description text,
            urutan_tampil int,
            jenis text,
            phase text
        )
        """
    )

    rows = session.execute(
        "SELECT name, long_name, description, urutan_tampil, jenis, phase "
        "FROM stockbit.wyckoff_glossary"
    )
    insert = session.prepare(
        "INSERT INTO invezgood.wyckoff_glossary "
        "(name, long_name, description, urutan_tampil, jenis, phase) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )

    count = 0
    for row in rows:
        session.execute(
            insert,
            (
                row.name,
                row.long_name,
                row.description,
                row.urutan_tampil,
                row.jenis,
                row.phase,
            ),
        )
        count += 1

    cluster.shutdown()
    print(f"OK: copied {count} row(s) to invezgood.wyckoff_glossary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
