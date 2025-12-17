"""
Task queue for managing pending tasks.
Provides priority-based task scheduling with persistence.
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
from collections import defaultdict
from loguru import logger

from agent.config import AgentConfig
from agent.models import Task, TaskStatus, TaskPriority


class TaskQueue:
    """
    Priority queue for tasks.
    
    Features:
    - Priority-based ordering (critical > high > normal > low)
    - Persistence to disk for crash recovery
    - Task deduplication
    - Scheduled task support
    """
    
    PRIORITY_ORDER = {
        TaskPriority.CRITICAL: 0,
        TaskPriority.HIGH: 1,
        TaskPriority.NORMAL: 2,
        TaskPriority.LOW: 3,
    }
    
    def __init__(self, config: AgentConfig):
        self.config = config
        
        # Queues per priority level
        self._queues: dict[TaskPriority, asyncio.Queue] = {
            TaskPriority.CRITICAL: asyncio.Queue(),
            TaskPriority.HIGH: asyncio.Queue(),
            TaskPriority.NORMAL: asyncio.Queue(),
            TaskPriority.LOW: asyncio.Queue(),
        }
        
        # Task tracking
        self._tasks: dict[str, Task] = {}  # task_id -> Task
        self._task_status: dict[str, TaskStatus] = {}  # task_id -> status
        
        # Persistence
        self._queue_file = config.data_dir / "task_queue.json"
        
        # Statistics
        self._stats = defaultdict(int)
    
    async def initialize(self):
        """Initialize queue and load persisted tasks."""
        await self._load_from_disk()
        logger.info(f"Task queue initialized with {len(self._tasks)} pending tasks")
    
    async def enqueue(self, task: Task) -> bool:
        """
        Add a task to the queue.
        
        Args:
            task: Task to enqueue
            
        Returns:
            True if task was added, False if duplicate
        """
        # Check for duplicate
        if task.task_id in self._tasks:
            logger.warning(f"Duplicate task ignored: {task.task_id}")
            return False
        
        # Store task
        self._tasks[task.task_id] = task
        self._task_status[task.task_id] = TaskStatus.PENDING
        
        # Add to appropriate priority queue
        priority = TaskPriority(task.priority)
        await self._queues[priority].put(task.task_id)
        
        # Update stats
        self._stats["enqueued"] += 1
        
        # Persist
        await self._save_to_disk()
        
        logger.info(
            f"Task enqueued: {task.task_type} ({task.task_id[:8]}) "
            f"priority={task.priority}"
        )
        
        return True
    
    async def dequeue(self, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Get the next task from the queue.
        
        Tasks are returned in priority order.
        
        Args:
            timeout: Maximum time to wait (None = wait forever)
            
        Returns:
            Next task or None if timeout
        """
        # Check queues in priority order
        for priority in [
            TaskPriority.CRITICAL,
            TaskPriority.HIGH,
            TaskPriority.NORMAL,
            TaskPriority.LOW,
        ]:
            queue = self._queues[priority]
            
            if not queue.empty():
                try:
                    task_id = queue.get_nowait()
                    task = self._tasks.get(task_id)
                    
                    if task:
                        self._task_status[task_id] = TaskStatus.IN_PROGRESS
                        self._stats["dequeued"] += 1
                        return task
                        
                except asyncio.QueueEmpty:
                    continue
        
        # No tasks in any queue - wait on normal priority queue
        if timeout is not None:
            try:
                task_id = await asyncio.wait_for(
                    self._queues[TaskPriority.NORMAL].get(),
                    timeout=timeout
                )
                task = self._tasks.get(task_id)
                
                if task:
                    self._task_status[task_id] = TaskStatus.IN_PROGRESS
                    self._stats["dequeued"] += 1
                    return task
                    
            except asyncio.TimeoutError:
                return None
        else:
            # Wait forever
            task_id = await self._queues[TaskPriority.NORMAL].get()
            task = self._tasks.get(task_id)
            
            if task:
                self._task_status[task_id] = TaskStatus.IN_PROGRESS
                self._stats["dequeued"] += 1
                return task
        
        return None
    
    def complete_task(self, task_id: str, success: bool = True):
        """Mark a task as completed."""
        if task_id in self._tasks:
            del self._tasks[task_id]
            
            if success:
                self._task_status[task_id] = TaskStatus.COMPLETED
                self._stats["completed"] += 1
            else:
                self._task_status[task_id] = TaskStatus.FAILED
                self._stats["failed"] += 1
            
            logger.debug(f"Task completed: {task_id[:8]} success={success}")
    
    async def requeue(self, task: Task, delay_seconds: int = 0):
        """
        Requeue a task (e.g., for retry).
        
        Args:
            task: Task to requeue
            delay_seconds: Optional delay before task becomes available
        """
        self._task_status[task.task_id] = TaskStatus.RETRYING
        self._stats["requeued"] += 1
        
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        
        # Re-add to queue
        priority = TaskPriority(task.priority)
        await self._queues[priority].put(task.task_id)
        
        logger.debug(f"Task requeued: {task.task_id[:8]}")
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a pending task.
        
        Returns:
            True if task was cancelled
        """
        if task_id in self._tasks:
            status = self._task_status.get(task_id)
            
            if status == TaskStatus.PENDING:
                del self._tasks[task_id]
                self._task_status[task_id] = TaskStatus.CANCELLED
                self._stats["cancelled"] += 1
                logger.info(f"Task cancelled: {task_id[:8]}")
                return True
            else:
                logger.warning(
                    f"Cannot cancel task {task_id[:8]} - status: {status}"
                )
        
        return False
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        return self._tasks.get(task_id)
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the status of a task."""
        return self._task_status.get(task_id)
    
    @property
    def pending_count(self) -> int:
        """Number of pending tasks."""
        return len(self._tasks)
    
    @property
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._tasks) == 0
    
    def get_stats(self) -> dict:
        """Get queue statistics."""
        return {
            "pending": self.pending_count,
            "enqueued": self._stats["enqueued"],
            "dequeued": self._stats["dequeued"],
            "completed": self._stats["completed"],
            "failed": self._stats["failed"],
            "cancelled": self._stats["cancelled"],
            "requeued": self._stats["requeued"],
        }
    
    async def _save_to_disk(self):
        """Persist queue state to disk."""
        try:
            self._queue_file.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                "tasks": {
                    task_id: task.model_dump(mode="json")
                    for task_id, task in self._tasks.items()
                },
                "saved_at": datetime.utcnow().isoformat(),
            }
            
            with open(self._queue_file, "w") as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to persist queue: {e}")
    
    async def _load_from_disk(self):
        """Load queue state from disk."""
        if not self._queue_file.exists():
            return
        
        try:
            with open(self._queue_file) as f:
                data = json.load(f)
            
            for task_id, task_data in data.get("tasks", {}).items():
                task = Task(**task_data)
                self._tasks[task_id] = task
                self._task_status[task_id] = TaskStatus.PENDING
                
                priority = TaskPriority(task.priority)
                await self._queues[priority].put(task_id)
            
            logger.info(f"Loaded {len(self._tasks)} tasks from disk")
            
        except Exception as e:
            logger.error(f"Failed to load queue from disk: {e}")
    
    async def clear(self):
        """Clear all pending tasks."""
        self._tasks.clear()
        
        for queue in self._queues.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        
        # Delete persistence file
        if self._queue_file.exists():
            self._queue_file.unlink()
        
        logger.info("Task queue cleared")

