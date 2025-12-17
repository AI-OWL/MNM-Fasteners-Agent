"""Tests for task queue."""

import pytest
import asyncio
from pathlib import Path
import tempfile

from agent.config import AgentConfig
from agent.models import Task, TaskType, TaskPriority
from agent.executor.task_queue import TaskQueue


@pytest.fixture
def config():
    """Create test configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = AgentConfig(
            agent_id="test-agent",
            agent_secret="test-secret",
            backend_api_key="test-key",
            sage50_company_path="C:\\Test",
        )
        cfg.data_dir = Path(tmpdir)
        yield cfg


@pytest.fixture
async def task_queue(config):
    """Create test task queue."""
    queue = TaskQueue(config)
    await queue.initialize()
    yield queue
    await queue.clear()


class TestTaskQueue:
    """Tests for TaskQueue."""
    
    @pytest.mark.asyncio
    async def test_enqueue_dequeue(self, task_queue):
        """Test basic enqueue and dequeue."""
        task = Task(
            task_type=TaskType.HEALTH_CHECK,
            payload={},
        )
        
        # Enqueue
        result = await task_queue.enqueue(task)
        assert result is True
        assert task_queue.pending_count == 1
        
        # Dequeue
        retrieved = await task_queue.dequeue(timeout=1.0)
        assert retrieved is not None
        assert retrieved.task_id == task.task_id
    
    @pytest.mark.asyncio
    async def test_priority_ordering(self, task_queue):
        """Test tasks are dequeued by priority."""
        # Add tasks in reverse priority order
        low = Task(task_type=TaskType.HEALTH_CHECK, priority=TaskPriority.LOW)
        normal = Task(task_type=TaskType.HEALTH_CHECK, priority=TaskPriority.NORMAL)
        high = Task(task_type=TaskType.HEALTH_CHECK, priority=TaskPriority.HIGH)
        critical = Task(task_type=TaskType.HEALTH_CHECK, priority=TaskPriority.CRITICAL)
        
        await task_queue.enqueue(low)
        await task_queue.enqueue(normal)
        await task_queue.enqueue(high)
        await task_queue.enqueue(critical)
        
        # Should come out in priority order
        first = await task_queue.dequeue(timeout=1.0)
        assert first.priority == "critical"
        
        second = await task_queue.dequeue(timeout=1.0)
        assert second.priority == "high"
        
        third = await task_queue.dequeue(timeout=1.0)
        assert third.priority == "normal"
        
        fourth = await task_queue.dequeue(timeout=1.0)
        assert fourth.priority == "low"
    
    @pytest.mark.asyncio
    async def test_duplicate_detection(self, task_queue):
        """Test duplicate tasks are rejected."""
        task = Task(
            task_type=TaskType.HEALTH_CHECK,
            payload={},
        )
        
        # First enqueue succeeds
        result1 = await task_queue.enqueue(task)
        assert result1 is True
        
        # Second enqueue fails (duplicate)
        result2 = await task_queue.enqueue(task)
        assert result2 is False
        
        # Still only one task
        assert task_queue.pending_count == 1
    
    @pytest.mark.asyncio
    async def test_task_completion(self, task_queue):
        """Test marking tasks as complete."""
        task = Task(
            task_type=TaskType.HEALTH_CHECK,
            payload={},
        )
        
        await task_queue.enqueue(task)
        await task_queue.dequeue(timeout=1.0)
        
        # Complete the task
        task_queue.complete_task(task.task_id, success=True)
        
        # Task should be removed
        assert task_queue.pending_count == 0
        assert task_queue.get_task(task.task_id) is None
    
    @pytest.mark.asyncio
    async def test_timeout_returns_none(self, task_queue):
        """Test dequeue timeout returns None."""
        result = await task_queue.dequeue(timeout=0.1)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_stats(self, task_queue):
        """Test queue statistics."""
        task = Task(
            task_type=TaskType.HEALTH_CHECK,
            payload={},
        )
        
        await task_queue.enqueue(task)
        await task_queue.dequeue(timeout=1.0)
        task_queue.complete_task(task.task_id, success=True)
        
        stats = task_queue.get_stats()
        
        assert stats["enqueued"] == 1
        assert stats["dequeued"] == 1
        assert stats["completed"] == 1

