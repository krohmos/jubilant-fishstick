from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Dict, Optional, Tuple

from harborpi.core.database import get_db_connection
from harborpi.utils.config import settings

log = logging.getLogger(__name__)

# Define our logbook states
STATUS_ANCHORED = "anchored"
STATUS_UNDERWAY = "underway"


class LogbookInterpreter:
    """
    Analyzes the 'samples' table to derive 'entries' for the logbook.

    This class implements the core state machine logic (e.g., detecting
    when the vessel stops to anchor or starts moving).
    """

    def __init__(self, db_conn: sqlite3.Connection) -> None:
        """
        Initializes the interpreter with a database connection.

        Args:
            db_conn: An active SQLite3 connection.
        """
        self.db_conn = db_conn

    def _get_last_entry(self) -> Dict[str, Any] | None:
        """
        Fetches the most recent entry from the 'entries' table.

        Returns:
            A dictionary representing the last logbook entry, or None
            if the 'entries' table is empty.
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT * FROM entries ORDER BY ts_utc DESC LIMIT 1")
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            log.error(f"Failed to get last entry: {e}")
            return None

    def _get_samples_since(self, timestamp: int) -> Tuple[int, float] | None:
        """
        Analyzes all samples since a given timestamp.

        Args:
            timestamp: The UNIX epoch timestamp to query after.

        Returns:
            A tuple of (sample_count, avg_speed_kn) if samples are found,
            otherwise None.
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "SELECT COUNT(*), AVG(speed_kn) FROM samples WHERE ts_utc > ?",
                (timestamp,),
            )
            row = cursor.fetchone()
            if row and row[0] > 0:
                return int(row[0]), float(row[1])
            return None
        except sqlite3.Error as e:
            log.error(f"Failed to get sample analytics: {e}")
            return None

    def _get_latest_sample(self) -> Dict[str, Any] | None:
        """
        Fetches the most recent valid sample from the 'samples' table.
        Used to get the lat/lon for a new entry.

        Returns:
            A dictionary of the latest sample, or None if no valid
            sample (with lat/lon) exists.
        """
        try:
            cursor = self.db_conn.cursor()
            # Find the most recent sample that has a position
            cursor.execute(
                "SELECT * FROM samples WHERE lat IS NOT NULL AND lon IS NOT NULL "
                "ORDER BY ts_utc DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            log.error(f"Failed to get latest sample: {e}")
            return None

    def _create_entry(self, status: str, ts_utc: int, lat: float, lon: float) -> None:
        """
        Inserts a new, automatically-generated entry into the 'entries' table.

        Args:
            status: The new status to log (e.g., 'anchored', 'underway').
            ts_utc: The timestamp for this entry.
            lat: The latitude for this entry.
            lon: The longitude for this entry.
        """
        log.info(f"Creating new logbook entry: {status} at {ts_utc}")
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                INSERT INTO entries (ts_utc, lat, lon, status)
                VALUES (?, ?, ?, ?)
                """,
                (ts_utc, lat, lon, status),
            )
            self.db_conn.commit()
        except sqlite3.Error as e:
            log.error(f"Failed to create new entry: {e}")
            self.db_conn.rollback()

    def run_job(self) -> None:
        """
        Executes one analysis cycle. This is the main method called
        by the scheduler.

        It checks the last known state and analyzes new samples to
        determine if a state transition (e.g., Underway -> Anchored)
        has occurred.
        """
        log.debug("Interpreter job running...")

        last_entry = self._get_last_entry()

        # Determine the time range to analyze
        if last_entry:
            last_entry_time = int(last_entry["ts_utc"])
            last_status = str(last_entry["status"])
        else:
            # No entries yet. Default to 'underway' and check last 15 min.
            last_entry_time = int(time.time()) - (settings.ANCHOR_MINUTES * 60)
            last_status = STATUS_UNDERWAY  # Assume 'underway' at first run

        # Calculate time window to check
        # We check a window *ending* now, and *starting* N minutes ago.
        # This is more robust than `_get_samples_since`
        window_start_ts = int(time.time()) - (settings.ANCHOR_MINUTES * 60)

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                "SELECT AVG(speed_kn) FROM samples WHERE ts_utc >= ?",
                (window_start_ts,),
            )
            row = cursor.fetchone()
        except sqlite3.Error as e:
            log.error(f"Failed to get moving average speed: {e}")
            return

        if not row or row[0] is None:
            log.debug("No samples found in analysis window. Skipping.")
            return

        avg_speed_kn = float(row[0])
        now_ts = int(time.time())

        # --- The State Machine ---

        # 1. Check for ANCHOR event
        if last_status == STATUS_UNDERWAY:
            if avg_speed_kn < settings.ANCHOR_SPEED_KN:
                # Speed has been low for the *entire* ANCHOR_MINUTES window
                log.info(
                    f"Anchor event detected. Avg speed {avg_speed_kn:.2f} kn "
                    f"is below threshold {settings.ANCHOR_SPEED_KN} kn."
                )
                latest_sample = self._get_latest_sample()
                if latest_sample:
                    self._create_entry(
                        status=STATUS_ANCHORED,
                        ts_utc=now_ts,
                        lat=latest_sample["lat"],
                        lon=latest_sample["lon"],
                    )

        # 2. Check for DEPARTURE event
        elif last_status == STATUS_ANCHORED:
            # Use a higher speed threshold to trigger departure
            # This creates hysteresis and avoids flip-flopping
            departure_speed_kn = settings.ANCHOR_SPEED_KN + 0.5

            if avg_speed_kn > departure_speed_kn:
                log.info(
                    f"Departure event detected. Avg speed {avg_speed_kn:.2f} kn "
                    f"is above threshold {departure_speed_kn} kn."
                )
                latest_sample = self._get_latest_sample()
                if latest_sample:
                    self._create_entry(
                        status=STATUS_UNDERWAY,
                        ts_utc=now_ts,
                        lat=latest_sample["lat"],
                        lon=latest_sample["lon"],
                    )

        else:
            # Status is 'docked', 'manual', etc.
            # The interpreter *only* toggles between 'underway' and 'anchored'.
            # It will not interfere with manual entries.
            log.debug(f"Last status is '{last_status}'. No automated action.")


def run_interpreter_scheduler(stop_event: threading.Event) -> None:
    """
    Runs the LogbookInterpreter on a fixed schedule.

    This function is designed to be run in its own thread.

    Args:
        stop_event: A threading.Event used to signal the loop to stop.
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    log.info("Logbook interpreter scheduler starting...")

    # We use a BlockingScheduler because this function runs in its
    # own dedicated thread.
    scheduler = BlockingScheduler()
    db_conn = get_db_connection()
    interpreter = LogbookInterpreter(db_conn)

    # Schedule the job to run every 5 minutes
    scheduler.add_job(interpreter.run_job, "interval", minutes=5, id="interpreter_job")

    # Run an initial check 15 seconds after startup
    scheduler.add_job(
        interpreter.run_job,
        "date",
        run_date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 15)),
        id="initial_run",
    )

    # This loop allows the scheduler to be stopped gracefully
    while not stop_event.is_set():
        if not scheduler.running:
            scheduler.start()
        time.sleep(1)  # Wait for the stop event

    log.info("Interpreter scheduler stopping...")
    scheduler.shutdown(wait=False)
    db_conn.close()
