from __future__ import annotations

import logging
import sqlite3
import threading
import time
from typing import Any, Dict

import pynmea2  # type: ignore
import serial  # type: ignore

from harborpi.core.database import get_db_connection
from harborpi.utils.config import settings

log = logging.getLogger(__name__)

# The specific columns in the 'samples' table we are allowed to write to.
# This prevents malformed sensor data from breaking the SQL query.
VALID_SAMPLE_COLUMNS = [
    "ts_utc",
    "lat",
    "lon",
    "speed_kn",
    "course_deg",
    "heading_mag",
    "pressure_hpa",
    "temp_c",
]


class GpsSerialSensor:
    """
    Handles reading and parsing NMEA 0183 data from a serial GPS device.

    This class focuses on extracting the most critical navigation data from
    RMC (Recommended Minimum Navigation Information) sentences.
    """

    def __init__(self, device_path: str, baud_rate: int = 9600) -> None:
        """
        Initializes the serial connection.

        Args:
            device_path: The filesystem path to the serial device (e.g., /dev/ttyUSB0).
            baud_rate: The baud rate for the serial connection.

        Raises:
            serial.SerialException: If the device cannot be opened.
        """
        self.device_path = device_path
        self.serial_conn = serial.Serial(device_path, baudrate=baud_rate, timeout=2.0)
        log.info(f"Opened serial connection to GPS at {self.device_path}")

    def read(self) -> Dict[str, Any] | None:
        """
        Reads one line from the serial port and attempts to parse it as an RMC sentence.

        Returns:
            A dictionary with navigation data if a valid RMC sentence with an
            active fix ('A') is found. Returns None otherwise.
        """
        try:
            # Read and decode one line, ignoring any bytes that aren't valid ASCII
            line = self.serial_conn.readline().decode("ascii", errors="ignore").strip()

            if not line:
                return None  # Timeout, no data

            # Parse the NMEA sentence
            msg = pynmea2.parse(line, check=True)

            # We only care about RMC sentences with an Active ('A') fix.
            # 'V' (Void) means no valid fix.
            if isinstance(msg, pynmea2.RMC) and msg.status == "A":
                return {
                    "lat": msg.latitude,
                    "lon": msg.longitude,
                    "speed_kn": msg.spd_over_grnd,
                    "course_deg": msg.true_course,
                }

            # Other valid NMEA sentence types (GGA, VTG, etc.) are ignored
            return None

        except serial.SerialException as e:
            # Device unplugged or other hardware error
            log.error(f"Serial error reading from GPS: {e}")
            # Close and attempt to reopen connection on next call
            self.serial_conn.close()
            time.sleep(5)  # Avoid rapid-fire reconnection attempts
            try:
                self.serial_conn.open()
            except serial.SerialException as reopen_e:
                log.error(f"Failed to reopen serial port: {reopen_e}")
            return None
        except pynmea2.ParseError as e:
            # Corrupt or incomplete NMEA sentence
            log.warning(f"Failed to parse NMEA sentence: {e}")
            return None


def _insert_sample(db_conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    """
    Inserts a single sensor sample into the 'samples' table.

    This function dynamically builds the query based on keys present in the
    data dictionary, filtered by the VALID_SAMPLE_COLUMNS constant.

    Args:
        db_conn: An active SQLite3 connection.
        data: A dictionary of sensor readings. Must include 'ts_utc'.
    """
    # Filter data to only include keys that match table columns
    sql_data = {
        k: data[k] for k in VALID_SAMPLE_COLUMNS if k in data and data[k] is not None
    }

    if "ts_utc" not in sql_data:
        log.error("Sensor data missing 'ts_utc'. Skipping insert.")
        return

    cols = ", ".join(sql_data.keys())
    placeholders = ", ".join(["?" for _ in sql_data])
    values = tuple(sql_data.values())

    sql = f"INSERT OR IGNORE INTO samples ({cols}) VALUES ({placeholders})"

    try:
        cursor = db_conn.cursor()
        cursor.execute(sql, values)
        db_conn.commit()
    except sqlite3.Error as e:
        log.error(f"Database error inserting sample: {e}. SQL: {sql}")
        db_conn.rollback()


def run_acquisition_loop(stop_event: threading.Event) -> None:
    """
    The main, high-reliability acquisition loop.

    This function runs in its own thread. It continuously polls the GPS
    sensor, timestamps the data, and writes it to the 'samples' table.
    It is designed to never crash.

    Args:
        stop_event: A threading.Event used to signal the loop to stop.
    """
    log.info("Acquisition thread starting...")

    try:
        sensor = GpsSerialSensor(settings.GPS_DEVICE)
        db_conn = get_db_connection()
    except Exception as e:
        log.critical(
            f"Failed to initialize sensor or database: {e}. "
            "Acquisition thread is stopping."
        )
        return

    while not stop_event.is_set():
        try:
            start_time = time.monotonic()

            # 1. Read Sensor
            sensor_data = sensor.read()

            if sensor_data:
                # 2. Add Timestamp
                sensor_data["ts_utc"] = int(time.time())

                # 3. Write to DB
                _insert_sample(db_conn, sensor_data)

            # 4. Sleep to maintain a ~1Hz loop
            elapsed = time.monotonic() - start_time
            sleep_duration = max(0, 1.0 - elapsed)  # 1.0 second loop interval

            # Use event.wait() instead of time.sleep()
            # This makes the loop exit immediately when the event is set.
            if stop_event.wait(timeout=sleep_duration):
                break  # Stop event was set, exit loop

        except Exception as e:
            # This is the "never die" failsafe.
            log.critical(f"Unhandled exception in acquisition loop: {e}", exc_info=True)
            # Wait 5 seconds before retrying to avoid spamming logs
            if stop_event.wait(timeout=5.0):
                break  # Stop event was set during error wait

    db_conn.close()
    log.info("Acquisition loop stopped.")
