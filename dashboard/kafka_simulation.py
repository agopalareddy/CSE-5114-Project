"""
Kafka-Powered Live Game Simulation
===================================
Helper module for the Live Simulation page that integrates with
Kafka streaming pipeline for real-time game replay.

This module provides:
1. API client for controlling the simulation producer
2. Functions to read streamed pitches from Snowflake
3. The main page_live_simulation function for Streamlit
"""

import os
import time
import requests
import streamlit as st
import pandas as pd

# Kafka simulation API URL (FastAPI server running simulation_producer.py)
SIMULATION_API_URL = os.getenv("SIMULATION_API_URL", "http://localhost:8000")


def get_simulation_status():
    """Get current simulation status from the API."""
    try:
        response = requests.get(f"{SIMULATION_API_URL}/status", timeout=2)
        if response.status_code == 200:
            return response.json()
    except requests.exceptions.RequestException:
        pass
    return None


def start_kafka_simulation(game_pk: int, speed: float):
    """Start a Kafka-powered simulation."""
    try:
        response = requests.post(
            f"{SIMULATION_API_URL}/start",
            json={"game_pk": game_pk, "speed": speed},
            timeout=5,
        )
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def pause_kafka_simulation():
    """Pause the current simulation."""
    try:
        response = requests.post(f"{SIMULATION_API_URL}/pause", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def resume_kafka_simulation():
    """Resume a paused simulation."""
    try:
        response = requests.post(f"{SIMULATION_API_URL}/resume", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def stop_kafka_simulation():
    """Stop the current simulation."""
    try:
        response = requests.post(f"{SIMULATION_API_URL}/stop", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def update_simulation_speed(speed: float):
    """Update simulation playback speed."""
    try:
        response = requests.post(
            f"{SIMULATION_API_URL}/speed", json={"speed": speed}, timeout=2
        )
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def get_simulation_pitches_query(game_pk: int) -> str:
    """Return SQL query to get simulation pitches from Snowflake."""
    # SIMULATION_PITCHES was created with uppercase column names (Snowflake default)
    return f"""
    SELECT 
        GAME_PK,
        AT_BAT_NUMBER,
        PITCH_NUMBER,
        INNING,
        INNING_TOPBOT,
        HOME_TEAM,
        AWAY_TEAM,
        HOME_SCORE,
        AWAY_SCORE,
        BATTER,
        PITCHER,
        PLAYER_NAME as PITCHER_NAME,
        PITCH_TYPE,
        PITCH_NAME,
        RELEASE_SPEED,
        PLATE_X,
        PLATE_Z,
        BALLS,
        STRIKES,
        OUTS_WHEN_UP as OUTS,
        TYPE as PITCH_RESULT,
        EVENTS,
        DESCRIPTION,
        SIMULATION_TIMESTAMP,
        LOADED_AT
    FROM SIMULATION_PITCHES
    WHERE GAME_PK = {game_pk}
      AND LOADED_AT > DATEADD(hour, -2, CURRENT_TIMESTAMP())
    ORDER BY AT_BAT_NUMBER, PITCH_NUMBER
    """


def is_kafka_available() -> tuple:
    """
    Check if Kafka simulation API is available.

    Returns:
        tuple: (is_available: bool, status: dict or None)
    """
    status = get_simulation_status()
    return (status is not None, status)
