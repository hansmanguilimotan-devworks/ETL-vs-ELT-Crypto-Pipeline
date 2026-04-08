"""
Website B — ELT: Extract from CoinGecko, Load raw into staging, Transform in SQL.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, flash, redirect, render_template, url_for

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "elt_assets.db"
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
FETCH_LIMIT = 10
COINGECKO_DEMO_API_KEY = "CG-cxyaiamN2gnb2zA7kCu8uBAx"
MOCK_DATA = [
    {
        "id": "bitcoin",
        "name": "Bitcoin",
        "symbol": "btc",
        "market_cap_rank": 1,
        "current_price": 64231.5678,
        "market_cap": 1200000000,
        "total_volume": 35000000,
        "price_change_percentage_24h": 1.22,
    },
    {
        "id": "ethereum",
        "name": "Ethereum",
        "symbol": "eth",
        "market_cap_rank": 2,
        "current_price": 3456.1234,
        "market_cap": 400000000,
        "total_volume": 18000000,
        "price_change_percentage_24h": 0.88,
    },
    {
        "id": "tether",
        "name": "Tether",
        "symbol": "usdt",
        "market_cap_rank": 3,
        "current_price": 1.0001,
        "market_cap": 100000000,
        "total_volume": 9000000,
        "price_change_percentage_24h": 0.01,
    },
]

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)
app.secret_key = "elt-demo-secret-change-in-production"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
api_status = "Fallback Data"


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_assets (
                staging_id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                rank_raw TEXT,
                symbol TEXT,
                name TEXT,
                price_usd_raw TEXT,
                market_cap_usd_raw TEXT,
                volume_24h_raw TEXT,
                change_24h_raw TEXT,
                loaded_at TEXT NOT NULL
            )
            """
        )
        has_final = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='final_assets'"
        ).fetchone()
        has_prod = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='production_data'"
        ).fetchone()
        if has_final and not has_prod:
            conn.execute("ALTER TABLE final_assets RENAME TO production_data")
        elif has_final and has_prod:
            conn.execute("DROP TABLE final_assets")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS production_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                rank_num INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                price_usd REAL NOT NULL,
                market_cap_usd REAL NOT NULL,
                market_share_pct REAL NOT NULL,
                volume_24h_usd REAL,
                transformed_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def extract_raw() -> list[dict]:
    global api_status
    try:
        response = requests.get(
            COINGECKO_URL,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": FETCH_LIMIT,
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h",
            },
            headers={"x-cg-demo-api-key": COINGECKO_DEMO_API_KEY},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        api_status = "Connected"
        return list(payload or [])
    except requests.RequestException as exc:
        logger.exception("CoinGecko extract failed, using fallback data: %s", exc)
        api_status = "Fallback Data"
        return list(MOCK_DATA)


def load_staging(raw_items: list[dict], batch_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("DELETE FROM staging_assets")
        rows = [
            (
                batch_id,
                item.get("market_cap_rank") or item.get("rank"),
                item.get("symbol"),
                item.get("name"),
                item.get("current_price") or item.get("priceUsd"),
                item.get("market_cap") or item.get("marketCapUsd"),
                item.get("total_volume") or item.get("volumeUsd24Hr"),
                item.get("price_change_percentage_24h")
                or item.get("changePercent24Hr"),
                now,
            )
            for item in raw_items
        ]
        conn.executemany(
            """
            INSERT INTO staging_assets (
                batch_id, rank_raw, symbol, name, price_usd_raw,
                market_cap_usd_raw, volume_24h_raw, change_24h_raw, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def transform_sql(batch_id: str) -> None:
    """Transform in DB: INSERT INTO production_data SELECT ... cleans staging rows."""
    now = datetime.now(timezone.utc).isoformat()
    batch_sql = batch_id.replace("'", "''")
    now_sql = now.replace("'", "''")
    transform_script = f"""
    DELETE FROM production_data;

    INSERT INTO production_data (
        batch_id, rank_num, symbol, name, price_usd, market_cap_usd,
        market_share_pct, volume_24h_usd, transformed_at
    )
    SELECT
        '{batch_sql}' AS batch_id,
        CAST(COALESCE(NULLIF(TRIM(rank_raw), ''), '0') AS INTEGER) AS rank_num,
        COALESCE(symbol, '') AS symbol,
        COALESCE(name, '') AS name,
        ROUND(CAST(COALESCE(NULLIF(TRIM(price_usd_raw), ''), '0') AS REAL), 2) AS price_usd,
        ROUND(CAST(COALESCE(NULLIF(TRIM(market_cap_usd_raw), ''), '0') AS REAL), 2) AS market_cap_usd,
        ROUND(
            CASE
                WHEN (
                    SELECT SUM(CAST(COALESCE(NULLIF(TRIM(market_cap_usd_raw), ''), '0') AS REAL))
                    FROM staging_assets
                    WHERE batch_id = '{batch_sql}'
                ) > 0
                THEN 100.0 * CAST(COALESCE(NULLIF(TRIM(market_cap_usd_raw), ''), '0') AS REAL)
                    / (
                        SELECT SUM(CAST(COALESCE(NULLIF(TRIM(market_cap_usd_raw), ''), '0') AS REAL))
                        FROM staging_assets
                        WHERE batch_id = '{batch_sql}'
                    )
                ELSE 0
            END,
            2
        ) AS market_share_pct,
        ROUND(CAST(COALESCE(NULLIF(TRIM(volume_24h_raw), ''), '0') AS REAL), 2) AS volume_24h_usd,
        '{now_sql}' AS transformed_at
    FROM staging_assets
    WHERE batch_id = '{batch_sql}'
    ORDER BY rank_num ASC;
    """
    with get_db() as conn:
        conn.executescript(transform_script)
        conn.commit()


def load_dashboard_context() -> dict:
    with get_db() as conn:
        sc = conn.execute(
            "SELECT COUNT(*) AS c FROM staging_assets"
        ).fetchone()
        staging_count = int(sc["c"]) if sc else 0
        cur = conn.execute(
            """
            SELECT batch_id, rank_num, symbol, name, price_usd, market_cap_usd,
                   market_share_pct, volume_24h_usd, transformed_at
            FROM production_data
            ORDER BY rank_num ASC
            """
        )
        table_rows = [dict(row) for row in cur.fetchall()]

    if not table_rows:
        return {
            "pipeline_mode": "ELT",
            "table_rows": [],
            "chart_labels": [],
            "chart_values": [],
            "last_updated": None,
            "row_count": 0,
            "top_symbol": None,
            "staging_count": staging_count,
            "api_status": api_status,
        }

    top_n = 10
    sorted_by_share = sorted(
        table_rows, key=lambda r: r["market_share_pct"], reverse=True
    )[:top_n]
    top_sum = sum(r["market_share_pct"] for r in sorted_by_share)
    others_share = max(0.0, 100.0 - top_sum)

    chart_labels = [r["symbol"] for r in sorted_by_share]
    chart_values = [round(r["market_share_pct"], 2) for r in sorted_by_share]
    if others_share >= 0.01:
        chart_labels.append("Others")
        chart_values.append(round(others_share, 2))

    last_updated = table_rows[0].get("transformed_at") if table_rows else None
    return {
        "pipeline_mode": "ELT",
        "table_rows": table_rows,
        "chart_labels": chart_labels,
        "chart_values": chart_values,
        "last_updated": last_updated,
        "row_count": len(table_rows),
        "top_symbol": sorted_by_share[0]["symbol"] if sorted_by_share else None,
        "staging_count": staging_count,
        "api_status": api_status,
    }


@app.route("/")
def dashboard():
    ctx = load_dashboard_context()
    return render_template("dashboard.html", **ctx)


@app.route("/sync", methods=["POST"])
def sync():
    try:
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        raw = extract_raw()
        load_staging(raw, batch_id)
        transform_sql(batch_id)
        flash(
            f"ELT complete: loaded {len(raw)} raw rows to staging; "
            "INSERT INTO production_data SELECT … filled the production table.",
            "success",
        )
    except requests.RequestException as e:
        flash(f"Network error during extract: {e}", "danger")
    except Exception as e:  # noqa: BLE001
        flash(f"ELT failed: {e}", "danger")
    return redirect(url_for("dashboard"))


init_db()

if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 5002)))
