
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, Any

# DDL: skapar tabellen om den inte finns.

DDL = """
create table if not exists fact_cars (
    url              text primary key,
    regnr            text unique,
    model_year       integer check (model_year is null or model_year >= 1900),
    price_sek        integer check (price_sek is null or price_sek >= 0),
    odometer_km      integer check (odometer_km is null or odometer_km >= 0),
    fuel             text,
    body_type        text,
    horsepower       integer check (horsepower is null or horsepower >= 0),
    price_per_1000km real    check (price_per_1000km is null or price_per_1000km >= 0),
    load_ts          text not null
);
-- Index för snabbare sökning på registreringsnummer
create unique index if not exists ux_fact_cars_regnr on fact_cars(regnr);
"""

# UPSERT: vid konflikter på url uppdateras fälten med "excluded" värden (SQL standard).
UPSERT_SQL = """
insert into fact_cars (
    url, regnr, model_year, price_sek, odometer_km, fuel, body_type, horsepower, price_per_1000km, load_ts
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(url) do update set
    regnr=excluded.regnr,
    model_year=excluded.model_year,
    price_sek=excluded.price_sek,
    odometer_km=excluded.odometer_km,
    fuel=excluded.fuel,
    body_type=excluded.body_type,
    horsepower=excluded.horsepower,
    price_per_1000km=excluded.price_per_1000km,
    load_ts=excluded.load_ts;
"""

def get_conn(db_path: Path) -> sqlite3.Connection:
    """
    Öppna en SQLite-anslutning med rimliga prestanda-inställningar.
    """
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("pragma journal_mode=WAL;")
    conn.execute("pragma synchronous=NORMAL;")
    conn.execute("pragma foreign_keys=on;")
    return conn

def init_schema(conn: sqlite3.Connection) -> None:
    """Skapa tabellen (och index) om de inte finns."""
    with conn:
        conn.executescript(DDL)

def upsert_cars(conn: sqlite3.Connection, rows: Iterable[Tuple[Any, ...]]) -> int:
    """
    Kör idempotent UPSERT för alla rader.
    Returnerar antalet rader som skickades till UPSERT.
    """
    rows_list = list(rows)
    if not rows_list:
        return 0
    with conn:
        conn.executemany(UPSERT_SQL, rows_list)
    return len(rows_list)
