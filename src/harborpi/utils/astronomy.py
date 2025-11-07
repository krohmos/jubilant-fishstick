from __future__ import annotations

import math
from datetime import datetime, timezone

import ephem  # type: ignore


def get_moon_phase(ts_utc: int) -> dict[str, str | float]:
    """
    Calculates the moon phase for a given UTC timestamp.

    Args:
        ts_utc: The UNIX epoch timestamp.

    Returns:
        A dictionary containing the phase percentage (illumination)
        and a descriptive name.
    """
    try:
        dt = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
        moon = ephem.Moon(dt)

        # Get illumination percentage
        illumination = moon.phase / 100.0  # As a float 0.0 to 1.0

        # Determine phase name
        # These are approximate values
        # ephem.next_new_moon, .next_full_moon, etc. are more accurate
        # but this is faster for a simple display.
        percent = illumination * 100
        if percent < 1:
            name = "New Moon"
        elif percent < 24:
            name = "Waxing Crescent"
        elif percent < 26:
            name = "First Quarter"
        elif percent < 49:
            name = "Waxing Gibbous"
        elif percent < 51:
            name = "Full Moon"
        elif percent < 74:
            name = "Waning Gibbous"
        elif percent < 76:
            name = "Third Quarter"
        elif percent <= 100:
            name = "Waning Crescent"

        return {
            "illumination": round(illumination, 2),
            "phase_name": name,
            "phase_percent": round(percent, 0),
        }
    except Exception:
        return {"illumination": 0, "phase_name": "Unknown", "phase_percent": 0}
