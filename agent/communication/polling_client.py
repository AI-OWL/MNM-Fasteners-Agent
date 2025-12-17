"""
HTTP Polling client for fallback communication with the backend.
Used when WebSocket connection is not available or fails.
"""

import asyncio
from datetime import datetime
from typing import Callable, Optional, Any
import aiohttp
from loguru import logger

from agent.config import AgentConfig
from agent.models import (
    Task,
    TaskResult,
    AgentStatus,
    TaskStatus,
)


class PollingClient:
    """
    HTTP Polling client for backend communication.
    
    Features:
    - Configurable polling interval
    - Automatic retry with backoff
    - Task fetching and result submission
    """
    
    def __init__(
        self,
        config: AgentConfig,
        on_task_received: Optional[Callable[[Task], None]] = None,
    ):
        self.config = config
        self.on_task_received = on_task_received
        
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_poll: Optional[datetime] = None
        self._consecutive_errors = 0
    
    @property
    def base_url(self) -> str:
        return self.config.backend_api_url
    
    def _get_headers(self) -> dict[str, str]:
        """Get HTTP headers for API requests."""
        return {
            "Authorization": f"Bearer {self.config.backend_api_key}",
            "X-Agent-ID": self.config.agent_id,
            "X-Agent-Secret": self.config.agent_secret,
            "Content-Type": "application/json",
        }
    
    async def _ensure_session(self):
        """Ensure HTTP session is created."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self._get_headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            )
    
    async def close(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def register(self) -> bool:
        """Register agent with backend."""
        await self._ensure_session()
        
        from agent import __version__
        
        try:
            url = f"{self.base_url}/agents/register"
            payload = {
                "agent_id": self.config.agent_id,
                "version": __version__,
                "capabilities": [
                    "create_sales_order",
                    "batch_create_orders",
                    "sync_orders",
                ],
            }
            
            async with self._session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info("Agent registered successfully via polling")
                    return True
                else:
                    error = await response.text()
                    logger.error(f"Registration failed: {response.status} - {error}")
                    return False
                    
        except Exception as e:
            logger.error(f"Registration error: {e}")
            return False
    
    async def fetch_tasks(self) -> list[Task]:
        """Fetch pending tasks from backend."""
        await self._ensure_session()
        
        try:
            url = f"{self.base_url}/agents/{self.config.agent_id}/tasks"
            
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    tasks = [Task(**t) for t in data.get("tasks", [])]
                    
                    if tasks:
                        logger.info(f"Fetched {len(tasks)} pending task(s)")
                    
                    self._consecutive_errors = 0
                    return tasks
                    
                elif response.status == 204:
                    # No tasks available
                    self._consecutive_errors = 0
                    return []
                    
                else:
                    error = await response.text()
                    logger.warning(f"Task fetch failed: {response.status} - {error}")
                    self._consecutive_errors += 1
                    return []
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching tasks: {e}")
            self._consecutive_errors += 1
            return []
        except Exception as e:
            logger.error(f"Error fetching tasks: {e}")
            self._consecutive_errors += 1
            return []
    
    async def submit_result(self, result: TaskResult) -> bool:
        """Submit task result to backend."""
        await self._ensure_session()
        
        try:
            url = f"{self.base_url}/agents/{self.config.agent_id}/tasks/{result.task_id}/result"
            payload = result.model_dump(mode="json")
            
            async with self._session.post(url, json=payload) as response:
                if response.status in (200, 201):
                    logger.info(f"Task result submitted: {result.task_id}")
                    return True
                else:
                    error = await response.text()
                    logger.error(f"Result submission failed: {response.status} - {error}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error submitting result: {e}")
            return False
    
    async def send_heartbeat(self, status: AgentStatus) -> bool:
        """Send heartbeat/status update to backend."""
        await self._ensure_session()
        
        try:
            url = f"{self.base_url}/agents/{self.config.agent_id}/heartbeat"
            payload = status.model_dump(mode="json")
            
            async with self._session.post(url, json=payload) as response:
                return response.status == 200
                
        except Exception as e:
            logger.debug(f"Heartbeat error: {e}")
            return False
    
    async def acknowledge_task(self, task_id: str) -> bool:
        """Acknowledge receipt of a task."""
        await self._ensure_session()
        
        try:
            url = f"{self.base_url}/agents/{self.config.agent_id}/tasks/{task_id}/ack"
            
            async with self._session.post(url) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Error acknowledging task: {e}")
            return False
    
    def _get_poll_interval(self) -> int:
        """Get polling interval with backoff on errors."""
        base_interval = self.config.polling_interval
        
        if self._consecutive_errors > 0:
            # Exponential backoff: 30s -> 60s -> 120s -> 240s (max)
            backoff = min(2 ** self._consecutive_errors, 8)
            return base_interval * backoff
        
        return base_interval
    
    async def run(self):
        """Main polling loop."""
        self._running = True
        
        logger.info(f"Starting polling client (interval: {self.config.polling_interval}s)")
        
        # Initial registration
        await self.register()
        
        while self._running:
            try:
                # Fetch tasks
                tasks = await self.fetch_tasks()
                self._last_poll = datetime.utcnow()
                
                # Process tasks
                for task in tasks:
                    # Acknowledge receipt
                    await self.acknowledge_task(task.task_id)
                    
                    # Notify handler
                    if self.on_task_received:
                        self.on_task_received(task)
                
                # Wait for next poll
                interval = self._get_poll_interval()
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(self._get_poll_interval())
        
        await self.close()
        logger.info("Polling client stopped")
    
    def stop(self):
        """Stop the polling loop."""
        self._running = False

