from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from wsgiref.simple_server import make_server

from harborpi.core.acquisition import run_acquisition_loop
from harborpi.core.database import create_schema, get_db_connection
from harborpi.core.interpreter import run_interpreter_scheduler
from harborpi.utils.config import settings
from harborpi.web.app import app

# --- Global Stop Event ---
# This event is used to signal all background threads to stop.
stop_event = threading.Event()


# --- Logging Setup ---
def setup_logging() -> None:
    """Configures the root logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        handlers=[
            logging.FileHandler(settings.LOG_PATH),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger(__name__)
    log.info("--- HarborPI Application Starting ---")
    log.info(f"Using database at: {settings.DATABASE_PATH}")


# --- Graceful Shutdown Handler ---
def signal_handler(sig: int, frame: Any) -> None:
    """
    Handles SIGINT (Ctrl+C) and SIGTERM.

    Sets the global stop_event, which signals background threads
    to terminate their loops.
    """
    log = logging.getLogger(__name__)
    log.warning("Shutdown signal received. Stopping services...")
    stop_event.set()


# --- Main `run` function ---
def run() -> None:
    """
    The main application entry point.

    1. Sets up logging.
    2. Registers signal handlers.
    3. Initializes the database.
    4. Starts background threads for Acquisition and Interpreter.
    5. Starts the Flask web server in the main thread.
    6. Waits for shutdown signal.
    """
    setup_logging()
    log = logging.getLogger(__name__)

    # Register signal handlers for graceful exit
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 1. Initialize Database
    try:
        db_conn = get_db_connection()
        create_schema(db_conn)
        db_conn.close()
    except Exception as e:
        log.critical(f"Failed to initialize database: {e}. Exiting.")
        sys.exit(1)

    # 2. Start Acquisition Thread
    log.info("Starting acquisition service thread...")
    acq_thread = threading.Thread(
        target=run_acquisition_loop, args=(stop_event,), name="AcquisitionThread"
    )
    acq_thread.daemon = True  # Dies if main thread dies
    acq_thread.start()

    # 3. Start Interpreter Thread
    log.info("Starting interpreter service thread...")
    interp_thread = threading.Thread(
        target=run_interpreter_scheduler, args=(stop_event,), name="InterpreterThread"
    )
    interp_thread.daemon = True
    interp_thread.start()

    # 4. Start Flask Web Server (in the main thread)
    log.info(
        f"Starting Flask web server at "
        f"http://{settings.SERVER_HOST}:{settings.SERVER_PORT}"
    )

    # Use a production-ready WSGI server instead of app.run()
    # We use wsgiref.simple_server here for simplicity,
    # but Gunicorn or Waitress would be better.
    try:
        with make_server(settings.SERVER_HOST, settings.SERVER_PORT, app) as httpd:

            # This loop runs until the stop_event is set.
            # We must use a short timeout for handle_request() so the
            # loop can check stop_event.is_set().
            httpd.timeout = 1.0
            while not stop_event.is_set():
                httpd.handle_request()  # Process one request

            log.info("Web server shutting down.")

    except Exception as e:
        log.error(f"Web server failed: {e}", exc_info=True)

    # 5. Wait for threads to join
    log.info("Waiting for background threads to stop...")
    acq_thread.join(timeout=5.0)
    interp_thread.join(timeout=5.0)
    log.info("--- HarborPI Application Stopped ---")
