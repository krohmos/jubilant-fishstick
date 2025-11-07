from __future__ import annotations

import logging

from flask import Flask, Response, jsonify, render_template

from harborpi.core.database import get_db_connection

log = logging.getLogger(__name__)

# Create the Flask application instance
app = Flask(__name__)

# --- UI Route ---


@app.route("/")
def index() -> str:
    """
    Serves the main (and only) HTML page.

    Returns:
        The rendered 'index.html' template.
    """
    return render_template("index.html")


# --- API Endpoints ---


@app.route("/api/v1/latest")
def get_latest_sample() -> Response:
    """
    Fetches the most recent 'sample' from the database.

    This provides the real-time dashboard data (speed, course, etc.).

    Returns:
        A JSON response containing the latest sample data.
    """
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        # Find the most recent sample that has a position
        cursor.execute(
            "SELECT * FROM samples WHERE lat IS NOT NULL AND lon IS NOT NULL "
            "ORDER BY ts_utc DESC LIMIT 1"
        )
        row = cursor.fetchone()
        db_conn.close()

        if row:
            return jsonify(dict(row))
        return jsonify({"error": "No data"}), 404

    except Exception as e:
        log.error(f"API Error in /api/v1/latest: {e}")
        return jsonify({"error": "Database query failed"}), 500


@app.route("/api/v1/logbook")
def get_logbook_entries() -> Response:
    """
    Fetches all logbook 'entries' from the database, newest first.

    Returns:
        A JSON response containing a list of all logbook entries.
    """
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        cursor.execute("SELECT * FROM entries ORDER BY ts_utc DESC")
        rows = cursor.fetchall()
        db_conn.close()

        # Convert list of sqlite3.Row objects to a list of dicts
        entries = [dict(row) for row in rows]
        return jsonify(entries)

    except Exception as e:
        log.error(f"API Error in /api/v1/logbook: {e}")
        return jsonify({"error": "Database query failed"}), 500
