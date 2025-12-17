"""
WebSocket client for real-time communication with the backend.
Handles connection, reconnection, and message passing.
"""

import asyncio
import json
from datetime import datetime
from typing import Callable, Optional, Any
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode
from loguru import logger
import jwt

from agent.config import AgentConfig
from agent.models import (
    AgentMessage,
    ServerMessage,
    MessageType,
    Task,
    TaskResult,
    AgentStatus,
)


class WebSocketClient:
    """
    WebSocket client for maintaining persistent connection to the backend.
    
    Features:
    - Automatic reconnection with exponential backoff
    - Heartbeat to keep connection alive
    - JWT authentication
    - Message queuing during disconnection
    """
    
    def __init__(
        self,
        config: AgentConfig,
        on_task_received: Optional[Callable[[Task], None]] = None,
        on_connected: Optional[Callable[[], None]] = None,
        on_disconnected: Optional[Callable[[], None]] = None,
    ):
        self.config = config
        self.on_task_received = on_task_received
        self.on_connected = on_connected
        self.on_disconnected = on_disconnected
        
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._running = False
        self._reconnect_delay = 1  # Start with 1 second
        self._max_reconnect_delay = 60
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._pending_messages: list[AgentMessage] = []
        
        # Tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._send_task: Optional[asyncio.Task] = None
    
    @property
    def is_connected(self) -> bool:
        return self._connected and self._websocket is not None
    
    def _generate_auth_token(self) -> str:
        """Generate JWT token for authentication."""
        payload = {
            "agent_id": self.config.agent_id,
            "iat": datetime.utcnow().timestamp(),
            "exp": datetime.utcnow().timestamp() + 3600,  # 1 hour expiry
        }
        return jwt.encode(payload, self.config.agent_secret, algorithm="HS256")
    
    def _get_ws_url(self) -> str:
        """Get WebSocket URL with authentication."""
        token = self._generate_auth_token()
        return f"{self.config.backend_ws_url}?token={token}&agent_id={self.config.agent_id}"
    
    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        try:
            url = self._get_ws_url()
            logger.info(f"Connecting to WebSocket: {self.config.backend_ws_url}")
            
            self._websocket = await websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
                max_size=10 * 1024 * 1024,  # 10MB max message size
            )
            
            self._connected = True
            self._reconnect_delay = 1  # Reset delay on successful connection
            
            logger.info("WebSocket connected successfully")
            
            # Send registration message
            await self._send_registration()
            
            if self.on_connected:
                self.on_connected()
            
            return True
            
        except InvalidStatusCode as e:
            logger.error(f"WebSocket connection rejected: {e.status_code}")
            return False
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            return False
    
    async def disconnect(self):
        """Close WebSocket connection gracefully."""
        self._running = False
        self._connected = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._receive_task:
            self._receive_task.cancel()
        if self._send_task:
            self._send_task.cancel()
        
        if self._websocket:
            await self._websocket.close()
            self._websocket = None
        
        logger.info("WebSocket disconnected")
    
    async def _send_registration(self):
        """Send registration message to backend."""
        from agent import __version__
        
        message = AgentMessage(
            message_type=MessageType.REGISTER,
            agent_id=self.config.agent_id,
            payload={
                "version": __version__,
                "capabilities": [
                    "create_sales_order",
                    "batch_create_orders",
                    "sync_orders",
                    "customer_management",
                ],
                "sage50_configured": bool(self.config.sage50_company_path),
            },
        )
        
        await self._send_message(message)
        logger.debug("Registration message sent")
    
    async def _send_message(self, message: AgentMessage):
        """Send a message through the WebSocket."""
        if not self._websocket:
            # Queue message for later
            self._pending_messages.append(message)
            logger.warning("WebSocket not connected, message queued")
            return
        
        try:
            data = message.model_dump_json()
            await self._websocket.send(data)
            logger.debug(f"Sent message: {message.message_type}")
        except ConnectionClosed:
            self._pending_messages.append(message)
            logger.warning("Connection closed while sending, message queued")
    
    async def send_task_result(self, result: TaskResult):
        """Send task result to backend."""
        message = AgentMessage(
            message_type=MessageType.TASK_RESULT,
            agent_id=self.config.agent_id,
            payload=result.model_dump(),
        )
        await self._send_message(message)
    
    async def send_status_update(self, status: AgentStatus):
        """Send status update to backend."""
        message = AgentMessage(
            message_type=MessageType.STATUS_UPDATE,
            agent_id=self.config.agent_id,
            payload=status.model_dump(),
        )
        await self._send_message(message)
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self._running:
            try:
                if self._connected:
                    message = AgentMessage(
                        message_type=MessageType.HEARTBEAT,
                        agent_id=self.config.agent_id,
                        payload={"timestamp": datetime.utcnow().isoformat()},
                    )
                    await self._send_message(message)
                    logger.debug("Heartbeat sent")
                
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def _receive_loop(self):
        """Receive and process messages from backend."""
        while self._running:
            try:
                if not self._websocket:
                    await asyncio.sleep(1)
                    continue
                
                raw_message = await self._websocket.recv()
                await self._handle_message(raw_message)
                
            except ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                self._connected = False
                if self.on_disconnected:
                    self.on_disconnected()
                await self._reconnect()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
    
    async def _handle_message(self, raw_message: str):
        """Process incoming message from backend."""
        try:
            data = json.loads(raw_message)
            message = ServerMessage(**data)
            
            logger.debug(f"Received message: {message.message_type}")
            
            if message.message_type == MessageType.TASK:
                task = Task(**message.payload)
                logger.info(f"Received task: {task.task_type} ({task.task_id})")
                
                if self.on_task_received:
                    self.on_task_received(task)
                    
            elif message.message_type == MessageType.CANCEL_TASK:
                task_id = message.payload.get("task_id")
                logger.info(f"Received cancel request for task: {task_id}")
                # Handle task cancellation
                
            elif message.message_type == MessageType.ACK:
                logger.debug(f"Received ACK for: {message.payload.get('message_id')}")
                
            elif message.message_type == MessageType.CONFIG_UPDATE:
                logger.info("Received config update request")
                # Handle config update
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff."""
        while self._running and not self._connected:
            logger.info(f"Reconnecting in {self._reconnect_delay} seconds...")
            await asyncio.sleep(self._reconnect_delay)
            
            if await self.connect():
                # Send any queued messages
                for msg in self._pending_messages:
                    await self._send_message(msg)
                self._pending_messages.clear()
                break
            
            # Exponential backoff
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._max_reconnect_delay
            )
    
    async def run(self):
        """Main run loop for the WebSocket client."""
        self._running = True
        
        # Initial connection
        await self.connect()
        
        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._receive_task = asyncio.create_task(self._receive_loop())
        
        try:
            # Wait for tasks to complete (they won't unless cancelled)
            await asyncio.gather(
                self._heartbeat_task,
                self._receive_task,
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self.disconnect()

