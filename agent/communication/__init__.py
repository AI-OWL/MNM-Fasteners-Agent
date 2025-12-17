"""
Communication layer for the MNM Agent.
Handles WebSocket and HTTP polling connections to the backend.
"""

from agent.communication.websocket_client import WebSocketClient
from agent.communication.polling_client import PollingClient
from agent.communication.connection_manager import ConnectionManager

__all__ = ["WebSocketClient", "PollingClient", "ConnectionManager"]

