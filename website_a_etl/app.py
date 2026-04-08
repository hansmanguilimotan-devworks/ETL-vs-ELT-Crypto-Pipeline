"""
Website A — ETL: Extract from CoinGecko, Transform in Python, Load into SQLite.
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
DB_PATH = APP_DIR / "etl_assets.db"
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
    },
    {
        "id": "ethereum",
        "name": "Ethereum",
        "symbol": "eth",
        "market_cap_rank": 2,
        "current_price": 3456.1234,
        "market_cap": 400000000,
        "total_volume": 18000000,
    },
    {
        "id": "tether",
        "name": "Tether",
        "symbol": "usdt",
        "market_cap_rank": 3,
        "current_price": 1.0001,
        "market_cap": 100000000,
        "total_volume": 9000000,
    },
]

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)
app.secret_key = "etl-demo-secret-change-in-production"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
api_status = "Fallback Data"


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        has_assets = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='assets'"
        ).fetchone()
        has_structured = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='structured_data'"
        ).fetchone()
        if has_assets and not has_structured:
            conn.execute("ALTER TABLE assets RENAME TO structured_data")
        elif has_assets and has_structured:
            conn.execute("DROP TABLE assets")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS structured_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rank_num INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                price_usd REAL NOT NULL,
                market_cap_usd REAL NOT NULL,
                market_share_pct REAL NOT NULL,
                volume_24h_usd REAL,
                updated_at TEXT NOT NULL
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


def fetch_and_transform() -> list[dict]:
    rows_raw = extract_raw()
    if not rows_raw:
        rows_raw = list(MOCK_DATA)

    total_mcap = 0.0
    parsed: list[dict] = []
    for item in rows_raw:
        try:
            mcap = float(item.get("market_cap") or item.get("marketCapUsd") or 0)
        except (TypeError, ValueError):
            mcap = 0.0
        try:
            price = float(item.get("current_price") or item.get("priceUsd") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            vol = float(item.get("total_volume") or item.get("volumeUsd24Hr") or 0)
        except (TypeError, ValueError):
            vol = 0.0
        try:
            rank = int(item.get("market_cap_rank") or item.get("rank") or 0)
        except (TypeError, ValueError):
            rank = 0
        parsed.append(
            {
                "rank": rank,
                "symbol": str(item.get("symbol") or "").strip().upper(),
                "name": str(item.get("name") or "").strip().title(),
                "price": price,
                "mcap": mcap,
                "volume": vol,
            }
        )
        total_mcap += mcap

    out: list[dict] = []
    for p in parsed:
        share = (100.0 * p["mcap"] / total_mcap) if total_mcap > 0 else 0.0
        out.append(
            {
                "rank_num": p["rank"],
                "symbol": p["symbol"],
                "name": p["name"],
                "price_usd": round(p["price"], 2),
                "market_cap_usd": round(p["mcap"], 2),
                "market_share_pct": round(share, 2),
                "volume_24h_usd": round(p["volume"], 2) if p["volume"] else None,
            }
        )
    out.sort(key=lambda x: x["rank_num"])
    return out


def load_transformed(rows: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("DELETE FROM structured_data")
        conn.executemany(
            """
            INSERT INTO structured_data (
                rank_num, symbol, name, price_usd, market_cap_usd,
                market_share_pct, volume_24h_usd, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["rank_num"],
                    r["symbol"],
                    r["name"],
                    r["price_usd"],
                    r["market_cap_usd"],
                    r["market_share_pct"],
                    r["volume_24h_usd"],
                    now,
                )
                for r in rows
            ],
        )
        conn.commit()


def load_dashboard_context() -> dict:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT rank_num, symbol, name, price_usd, market_cap_usd,
                   market_share_pct, volume_24h_usd, updated_at
            FROM structured_data
            ORDER BY rank_num ASC
            """
        )
        table_rows = [dict(row) for row in cur.fetchall()]

    if not table_rows:
        return {
            "pipeline_mode": "ETL",
            "table_rows": [],
            "chart_labels": [],
            "chart_values": [],
            "last_updated": None,
            "row_count": 0,
            "top_symbol": None,
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

    last_updated = table_rows[0].get("updated_at") if table_rows else None
    return {
        "pipeline_mode": "ETL",
        "table_rows": table_rows,
        "chart_labels": chart_labels,
        "chart_values": chart_values,
        "last_updated": last_updated,
        "row_count": len(table_rows),
        "top_symbol": sorted_by_share[0]["symbol"] if sorted_by_share else None,
        "api_status": api_status,
    }


@app.route("/")
def dashboard():
    ctx = load_dashboard_context()
    return render_template("dashboard.html", **ctx)


@app.route("/sync", methods=["POST"])
def sync():
    try:
        rows = fetch_and_transform()
        load_transformed(rows)
        flash(
            f"ETL complete: transformed {len(rows)} assets in Python and loaded.",
            "success",
        )
    except requests.RequestException as e:
        flash(f"Network error during extract: {e}", "danger")
    except Exception as e:  # noqa: BLE001
        flash(f"ETL failed: {e}", "danger")
    return redirect(url_for("dashboard"))


init_db()

if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 5001)))
