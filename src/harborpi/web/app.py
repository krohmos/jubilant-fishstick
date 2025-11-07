from __future__ import annotations

import json
import logging
import time

from flask import Flask, Response, jsonify, render_template, request

from harborpi.core.database import get_db_connection
from harborpi.utils.astronomy import get_moon_phase

log = logging.getLogger(__name__)

# Create the Flask application instance
app = Flask(__name__)


# --- UI Route ---
@app.route("/")
def index() -> str:
    """Serves the main (and only) HTML page."""
    return render_template("index.html")


# --- API Endpoints ---


@app.route("/api/v1/logbook", methods=["GET"])
def get_logbook_entries() -> Response:
    """
    Fetches all logbook 'entries' from the database, newest first.
    """
    # ... (existing code from before) ...
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        cursor.execute("SELECT * FROM entries ORDER BY ts_utc DESC")
        rows = cursor.fetchall()
        db_conn.close()

        entries = [dict(row) for row in rows]
        return jsonify(entries)

    except Exception as e:
        log.error(f"API Error in /api/v1/logbook: {e}")
        return jsonify({"error": "Database query failed"}), 500


@app.route("/api/v1/logbook", methods=["POST"])
def create_logbook_entry() -> Response:
    """
    Creates a new, manual logbook entry.

    Expects a JSON payload with 'notes' and optional 'lat', 'lon'.
    """
    data = request.json
    if not data or "notes" not in data:
        return jsonify({"error": "Missing 'notes' field"}), 400

    try:
        ts = int(time.time())
        lat = data.get("lat", 0.0)  # Default to 0,0 if not provided
        lon = data.get("lon", 0.0)
        notes = str(data["notes"])

        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        cursor.execute(
            """
            INSERT INTO entries (ts_utc, lat, lon, status, notes)
            VALUES (?, ?, ?, 'manual', ?)
            """,
            (ts, lat, lon, notes),
        )
        db_conn.commit()

        # Return the newly created entry
        entry_id = cursor.lastrowid
        cursor.execute("SELECT * FROM entries WHERE id = ?", (entry_id,))
        new_entry = dict(cursor.fetchone())
        db_conn.close()

        return jsonify(new_entry), 201  # 201 Created

    except Exception as e:
        log.error(f"API Error creating manual entry: {e}")
        return jsonify({"error": "Database insert failed"}), 500


@app.route("/api/v1/latest")
def get_latest_sample() -> Response:
    """
    Fetches the most recent sample, latest logbook entry (for weather/place),
    and current moon phase.

    This provides the real-time dashboard data.
    """
    response_data = {}
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()

        # 1. Get latest sensor sample
        cursor.execute(
            "SELECT * FROM samples WHERE lat IS NOT NULL AND lon IS NOT NULL "
            "ORDER BY ts_utc DESC LIMIT 1"
        )
        sample_row = cursor.fetchone()
        if sample_row:
            response_data["sample"] = dict(sample_row)

        # 2. Get latest logbook entry (for place/weather)
        cursor.execute(
            "SELECT place_name, wx_json FROM entries " "ORDER BY ts_utc DESC LIMIT 1"
        )
        entry_row = cursor.fetchone()
        if entry_row:
            response_data["location"] = {"place_name": entry_row["place_name"]}
            if entry_row["wx_json"]:
                response_data["weather"] = json.loads(entry_row["wx_json"])
            else:
                response_data["weather"] = None
        else:
            response_data["location"] = None
            response_data["weather"] = None

        db_conn.close()

        # 3. Get current moon phase
        response_data["moon"] = get_moon_phase(int(time.time()))

        return jsonify(response_data)

    except Exception as e:
        log.error(f"API Error in /api/v1/latest: {e}")
        return jsonify({"error": "Database query failed"}), 500


@app.route("/api/v1/logbook", methods=["GET"])
def get_logbook_entries() -> Response:
    """
    Fetches all logbook 'entries' and injects the moon phase
    for each entry's date.
    """
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        cursor.execute("SELECT * FROM entries ORDER BY ts_utc DESC")
        rows = cursor.fetchall()
        db_conn.close()

        # Convert list of sqlite3.Row objects to a list of dicts
        entries = []
        for row in rows:
            entry = dict(row)
            # Inject moon phase for the entry's date
            entry["moon"] = get_moon_phase(entry["ts_utc"])
            # Parse wx_json string into an object
            if entry["wx_json"]:
                entry["wx_json"] = json.loads(entry["wx_json"])
            entries.append(entry)

        return jsonify(entries)

    except Exception as e:
        log.error(f"API Error in /api/v1/logbook: {e}")
        return jsonify({"error": "Database query failed"}), 500
