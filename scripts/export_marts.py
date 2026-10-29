"""Export the seven dbt marts from Snowflake to local CSVs for reconciliation.

Reconciliation runs on Databricks, which can't reach this Snowflake account
directly on Free Edition, so the dbt side of the comparison travels as files:
this script dumps ANALYTICS.MARTS.* to reconcile_exports/, and those CSVs are
uploaded to a Unity Catalog volume and read by 04_reconcile.py.

Connects with the same key-pair credentials dbt uses (~/.dbt/profiles.yml,
profile `restaurant_pipeline`, its default target), so there's no second copy
of connection config to drift. Read-only: SELECT only.

Output format is fixed so Spark can parse it against the gold contract
schemas with no guessing:
  - timestamps  -> 'YYYY-MM-DD HH:MM:SS', no timezone (all mart timestamps
                   are TIMESTAMP_NTZ; fails loudly on sub-second values
                   rather than silently truncating them)
  - dates       -> 'YYYY-MM-DD'
  - booleans    -> true / false
  - decimals    -> exact string from Python's Decimal (keeps the scale, e.g. 12.50)
  - floats      -> repr(), the shortest string that round-trips the double
  - NULL        -> empty field
  - header      -> lowercase column names, matching the gold tables
  - line ending -> \n (csv's default \r\n would leave a stray \r on the last field)

Run from the repo root:
    venv/bin/python scripts/export_marts.py
"""

import csv
import datetime
import decimal
from pathlib import Path

import snowflake.connector
import yaml
from cryptography.hazmat.primitives import serialization

PROFILES_PATH = Path.home() / ".dbt" / "profiles.yml"
PROFILE_NAME = "restaurant_pipeline"
DATABASE = "ANALYTICS"
SCHEMA = "MARTS"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "reconcile_exports"

# Same seven marts, same names as the gold tables in 03_gold.py.
MARTS = [
    "dim_restaurants",
    "dim_menu_items",
    "dim_customers",
    "fct_orders",
    "fct_revenue_by_restaurant_category_month",
    "fct_item_demand_by_hour",
    "fct_price_ratio_by_tier",
]


def load_dbt_target() -> dict:
    """Return the active target's settings from ~/.dbt/profiles.yml."""
    profiles = yaml.safe_load(PROFILES_PATH.read_text())
    profile = profiles[PROFILE_NAME]
    return profile["outputs"][profile["target"]]


def load_private_key(path: str) -> bytes:
    """Read the unencrypted PKCS#8 .p8 key and return DER bytes for the connector."""
    key = serialization.load_pem_private_key(Path(path).expanduser().read_bytes(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def format_value(value):
    """Render one Snowflake value in the fixed CSV format described above."""
    if value is None:
        return ""
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return "true" if value else "false"
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            raise ValueError(f"unexpected timezone-aware timestamp: {value!r}")
        if value.microsecond:
            raise ValueError(f"sub-second timestamp would be truncated: {value!r}")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return str(value)


def export_mart(cursor, mart: str) -> int:
    """SELECT * from one mart, write it to OUTPUT_DIR/<mart>.csv, return the row count."""
    cursor.execute(f"SELECT * FROM {DATABASE}.{SCHEMA}.{mart.upper()}")
    header = [d[0].lower() for d in cursor.description]
    rows = cursor.fetchall()
    with open(OUTPUT_DIR / f"{mart}.csv", "w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        for row in rows:
            writer.writerow([format_value(v) for v in row])
    return len(rows)


def main() -> None:
    target = load_dbt_target()
    OUTPUT_DIR.mkdir(exist_ok=True)

    conn = snowflake.connector.connect(
        account=target["account"],
        user=target["user"],
        role=target["role"],
        warehouse=target["warehouse"],
        database=DATABASE,
        schema=SCHEMA,
        private_key=load_private_key(target["private_key_path"]),
    )
    try:
        cursor = conn.cursor()
        for mart in MARTS:
            count = export_mart(cursor, mart)
            print(f"{mart:<42} {count:>7,} rows -> reconcile_exports/{mart}.csv")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
