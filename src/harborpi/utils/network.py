from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

import requests

from harborpi.utils.config import settings

log = logging.getLogger(__name__)


def get_weather_and_location(
    lat: float, lon: float
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetches weather and reverse-geocoded place name from OpenWeatherMap.

    Uses the "One Call" API (v3.0) for weather and the Geocoding API
    for reverse geocoding.

    Args:
        lat: Latitude
        lon: Longitude

    Returns:
        A tuple of (place_name, weather_json). Both are None on failure.
    """
    place_name = _get_location_name(lat, lon)
    weather_data = _get_weather_data(lat, lon)

    weather_json = json.dumps(weather_data) if weather_data else None

    return place_name, weather_json


def _get_location_name(lat: float, lon: float) -> Optional[str]:
    """Fetches reverse-geocoded place name."""
    if not settings.OWM_API_KEY:
        log.warning("No OWM_API_KEY. Skipping reverse geocoding.")
        return None

    url = "http://api.openweathermap.org/geo/1.0/reverse"
    params = {
        "lat": lat,
        "lon": lon,
        "limit": 1,
        "appid": settings.OWM_API_KEY,
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses
        data = response.json()

        if data and isinstance(data, list) and len(data) > 0:
            item = data[0]
            name = item.get("name", "Unknown")
            country = item.get("country", "")
            return f"{name}, {country}"

        return "Unknown Location"

    except requests.exceptions.RequestException as e:
        log.error(f"Failed to get location name: {e}")
        return None


def _get_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Fetches current weather data."""
    if not settings.OWM_API_KEY:
        log.warning("No OWM_API_KEY. Skipping weather fetch.")
        return None

    url = "https://api.openweathermap.org/data/3.0/onecall"
    params = {
        "lat": lat,
        "lon": lon,
        "exclude": "minutely,hourly,daily,alerts",
        "units": "metric",  # Use Celsius, m/s
        "appid": settings.OWM_API_KEY,
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # We only care about the 'current' block
        if "current" in data:
            return data["current"]
        return None

    except requests.exceptions.RequestException as e:
        log.error(f"Failed to get weather data: {e}")
        return None
