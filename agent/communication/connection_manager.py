"""
Connection manager that handles both WebSocket and polling communication.
Provides automatic failover between connection methods.
"""

import asyncio
from datetime import datetime
from typing import Callable, Optional
from loguru import logger

from agent.config import AgentConfig
from agent.models import Task, TaskResult, AgentStatus
from agent.communication.websocket_client import WebSocketClient
from agent.communication.polling_client import PollingClient


class ConnectionManager:
    """
    Manages communication with the backend.
    
    Primary: WebSocket for real-time communication
    Fallback: HTTP polling when WebSocket fails
    
    Automatically switches between methods based on connection status.
    """
    
    def __init__(
        self,
        config: AgentConfig,
        on_task_received: Callable[[Task], None],
    ):
        self.config = config
        self.on_task_received = on_task_received
        
        self._ws_client: Optional[WebSocketClient] = None
        self._polling_client: Optional[PollingClient] = None
        
        self._using_websocket = True
        self._running = False
        self._connected = False
        
        # Statistics
        self._ws_connect_attempts = 0
        self._ws_connect_failures = 0
        self._last_message_received: Optional[datetime] = None
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def connection_type(self) -> str:
        return "websocket" if self._using_websocket else "polling"
    
    def _on_ws_connected(self):
        """Called when WebSocket connects."""
        self._connected = True
        self._using_websocket = True
        logger.info("WebSocket connection established")
        
        # Stop polling if it was running
        if self._polling_client:
            self._polling_client.stop()
    
    def _on_ws_disconnected(self):
        """Called when WebSocket disconnects."""
        self._connected = False
        self._ws_connect_failures += 1
        logger.warning("WebSocket disconnected")
        
        # Check if we should fall back to polling
        if self._ws_connect_failures >= 3 and self.config.polling_enabled:
            logger.info("Falling back to HTTP polling")
            self._using_websocket = False
            asyncio.create_task(self._start_polling())
    
    def _handle_task(self, task: Task):
        """Handle incoming task from either connection method."""
        self._last_message_received = datetime.utcnow()
        self.on_task_received(task)
    
    async def _start_websocket(self):
        """Start WebSocket client."""
        self._ws_client = WebSocketClient(
            config=self.config,
            on_task_received=self._handle_task,
            on_connected=self._on_ws_connected,
            on_disconnected=self._on_ws_disconnected,
        )
        
        self._ws_connect_attempts += 1
        await self._ws_client.run()
    
    async def _start_polling(self):
        """Start polling client."""
        self._polling_client = PollingClient(
            config=self.config,
            on_task_received=self._handle_task,
        )
        
        self._connected = True
        await self._polling_client.run()
    
    async def send_task_result(self, result: TaskResult):
        """Send task result to backend."""
        if self._using_websocket and self._ws_client:
            await self._ws_client.send_task_result(result)
        elif self._polling_client:
            await self._polling_client.submit_result(result)
        else:
            logger.error("No connection available to send result")
    
    async def send_status_update(self, status: AgentStatus):
        """Send status update to backend."""
        status.connection_type = self.connection_type
        
        if self._using_websocket and self._ws_client:
            await self._ws_client.send_status_update(status)
        elif self._polling_client:
            await self._polling_client.send_heartbeat(status)
    
    async def start(self):
        """Start the connection manager."""
        self._running = True
        logger.info("Starting connection manager")
        
        # Try WebSocket first
        if self.config.backend_ws_url:
            try:
                await self._start_websocket()
            except Exception as e:
                logger.error(f"WebSocket failed: {e}")
                
                # Fall back to polling
                if self.config.polling_enabled:
                    logger.info("Falling back to polling")
                    self._using_websocket = False
                    await self._start_polling()
        else:
            # No WebSocket URL, use polling
            if self.config.polling_enabled:
                self._using_websocket = False
                await self._start_polling()
            else:
                raise RuntimeError("No communication method configured")
    
    async def stop(self):
        """Stop all connections."""
        self._running = False
        
        if self._ws_client:
            await self._ws_client.disconnect()
        
        if self._polling_client:
            self._polling_client.stop()
            await self._polling_client.close()
        
        self._connected = False
        logger.info("Connection manager stopped")
    
    def get_stats(self) -> dict:
        """Get connection statistics."""
        return {
            "connection_type": self.connection_type,
            "is_connected": self._connected,
            "ws_connect_attempts": self._ws_connect_attempts,
            "ws_connect_failures": self._ws_connect_failures,
            "last_message_received": (
                self._last_message_received.isoformat()
                if self._last_message_received else None
            ),
        }

