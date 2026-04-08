from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, send_file

APP_DIR = Path(__file__).resolve().parent

app = Flask(__name__)


@app.route("/")
def landing():
    return send_file(APP_DIR / "index.html")


if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 5000)))
