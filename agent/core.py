"""
Core agent service that orchestrates all components.
This is the main entry point for the agent.
"""

import asyncio
import signal
from datetime import datetime
from typing import Optional
from loguru import logger
import psutil

from agent import __version__
from agent.config import AgentConfig, init_config
from agent.logging_config import setup_logging
from agent.models import Task, TaskResult, AgentStatus
from agent.communication import ConnectionManager
from agent.sage50 import Sage50Connector
from agent.executor import TaskQueue, TaskExecutor


class MNMAgent:
    """
    Main agent service class.
    
    Orchestrates:
    - Communication with backend (WebSocket/Polling)
    - Task queue management
    - Task execution
    - Sage 50 integration
    - Status reporting
    """
    
    def __init__(self, config: Optional[AgentConfig] = None):
        self.config = config or init_config()
        
        # Components (initialized in start())
        self._connection: Optional[ConnectionManager] = None
        self._task_queue: Optional[TaskQueue] = None
        self._executor: Optional[TaskExecutor] = None
        self._sage_connector: Optional[Sage50Connector] = None
        
        # State
        self._running = False
        self._started_at: Optional[datetime] = None
        self._tasks_completed = 0
        self._tasks_failed = 0
        
        # Background tasks
        self._status_task: Optional[asyncio.Task] = None
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    def _on_task_received(self, task: Task):
        """Handle incoming task from backend."""
        logger.info(f"Received task: {task.task_type} ({task.task_id[:8]})")
        
        # Add to queue (fire and forget)
        asyncio.create_task(self._task_queue.enqueue(task))
    
    async def _on_task_result(self, result: TaskResult):
        """Handle task execution result."""
        # Update stats
        if result.status == "completed":
            self._tasks_completed += 1
        else:
            self._tasks_failed += 1
        
        # Send result to backend
        if self._connection:
            await self._connection.send_task_result(result)
    
    def _get_status(self) -> AgentStatus:
        """Get current agent status."""
        # Get system metrics
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return AgentStatus(
            agent_id=self.config.agent_id,
            version=__version__,
            status="online" if self._running else "offline",
            connected_at=self._started_at,
            last_heartbeat=datetime.utcnow(),
            connection_type=self._connection.connection_type if self._connection else "none",
            current_task=self._executor.current_task_id if self._executor else None,
            tasks_completed=self._tasks_completed,
            tasks_failed=self._tasks_failed,
            sage_connected=self._sage_connector.is_connected if self._sage_connector else False,
            sage_company=self._sage_connector.company_name if self._sage_connector else None,
            sage_version=self._sage_connector.sage_version if self._sage_connector else None,
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            disk_free_gb=disk.free / (1024**3),
        )
    
    async def _status_reporter(self):
        """Periodically report status to backend."""
        while self._running:
            try:
                if self._connection and self._connection.is_connected:
                    status = self._get_status()
                    await self._connection.send_status_update(status)
                
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Status reporter error: {e}")
                await asyncio.sleep(30)
    
    async def start(self):
        """Start the agent service."""
        logger.info(f"Starting MNM Agent v{__version__}")
        logger.info(f"Agent ID: {self.config.agent_id}")
        
        # Validate configuration
        errors = self.config.validate()
        if errors:
            for error in errors:
                logger.error(f"Config error: {error}")
            raise RuntimeError("Invalid configuration")
        
        self._running = True
        self._started_at = datetime.utcnow()
        
        try:
            # Initialize Sage 50 connector
            self._sage_connector = Sage50Connector(self.config)
            
            # Try to connect to Sage 50
            try:
                self._sage_connector.connect()
                logger.info(f"Connected to Sage 50: {self._sage_connector.company_name}")
            except Exception as e:
                logger.warning(f"Sage 50 connection failed: {e}")
                logger.warning("Agent will continue but Sage operations will fail")
            
            # Initialize task queue
            self._task_queue = TaskQueue(self.config)
            await self._task_queue.initialize()
            
            # Initialize task executor
            self._executor = TaskExecutor(
                config=self.config,
                task_queue=self._task_queue,
                sage_connector=self._sage_connector,
                on_result=self._on_task_result,
            )
            
            # Initialize connection manager
            self._connection = ConnectionManager(
                config=self.config,
                on_task_received=self._on_task_received,
            )
            
            # Start executor
            await self._executor.start()
            
            # Start status reporter
            self._status_task = asyncio.create_task(self._status_reporter())
            
            # Start connection (this blocks until stopped)
            logger.info("Agent started successfully")
            await self._connection.start()
            
        except Exception as e:
            logger.exception(f"Agent startup failed: {e}")
            raise
    
    async def stop(self):
        """Stop the agent service."""
        logger.info("Stopping agent...")
        self._running = False
        
        # Cancel status reporter
        if self._status_task:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
        
        # Stop executor
        if self._executor:
            await self._executor.stop()
        
        # Stop connection
        if self._connection:
            await self._connection.stop()
        
        # Disconnect from Sage 50
        if self._sage_connector:
            self._sage_connector.disconnect()
        
        logger.info("Agent stopped")
    
    def run(self):
        """Run the agent (blocking)."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Setup signal handlers
        def signal_handler():
            logger.info("Received shutdown signal")
            loop.create_task(self.stop())
        
        try:
            # Windows doesn't support add_signal_handler well
            # so we use a different approach
            import sys
            if sys.platform != 'win32':
                loop.add_signal_handler(signal.SIGTERM, signal_handler)
                loop.add_signal_handler(signal.SIGINT, signal_handler)
        except NotImplementedError:
            pass
        
        try:
            loop.run_until_complete(self.start())
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            loop.run_until_complete(self.stop())
        finally:
            loop.close()


def run_agent(config_file: Optional[str] = None):
    """
    Run the MNM Agent.
    
    Args:
        config_file: Path to configuration file
    """
    # Initialize configuration
    config = init_config(config_file)
    
    # Setup logging
    setup_logging(config, console=True)
    
    # Create and run agent
    agent = MNMAgent(config)
    agent.run()

