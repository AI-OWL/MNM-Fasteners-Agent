"""
Task executor for processing tasks from the queue.
Handles task execution, retries, and result reporting.
"""

import asyncio
from datetime import datetime
from typing import Callable, Optional, Any
from loguru import logger

from agent.config import AgentConfig
from agent.models import (
    Task,
    TaskResult,
    TaskType,
    TaskStatus,
    SalesOrder,
    Customer,
)
from agent.executor.task_queue import TaskQueue
from agent.sage50.connector import Sage50Connector, Sage50Error
from agent.sage50.operations import Sage50Operations
from agent.logging_config import TaskLogger


class TaskExecutor:
    """
    Executes tasks from the queue.
    
    Features:
    - Async task execution
    - Automatic retry with backoff
    - Timeout handling
    - Result callback for reporting
    """
    
    def __init__(
        self,
        config: AgentConfig,
        task_queue: TaskQueue,
        sage_connector: Sage50Connector,
        on_result: Optional[Callable[[TaskResult], Any]] = None,
    ):
        self.config = config
        self.task_queue = task_queue
        self.sage_connector = sage_connector
        self.sage_ops = Sage50Operations(sage_connector)
        self.on_result = on_result
        
        self._running = False
        self._current_task: Optional[Task] = None
        self._worker_task: Optional[asyncio.Task] = None
        
        # Task handlers registry
        self._handlers: dict[TaskType, Callable] = {
            TaskType.CREATE_SALES_ORDER: self._handle_create_sales_order,
            TaskType.GET_SALES_ORDER: self._handle_get_sales_order,
            TaskType.CREATE_CUSTOMER: self._handle_create_customer,
            TaskType.GET_CUSTOMER: self._handle_get_customer,
            TaskType.SEARCH_CUSTOMERS: self._handle_search_customers,
            TaskType.GET_PRODUCT: self._handle_get_product,
            TaskType.SEARCH_PRODUCTS: self._handle_search_products,
            TaskType.BATCH_CREATE_ORDERS: self._handle_batch_create_orders,
            TaskType.SYNC_ORDERS: self._handle_sync_orders,
            TaskType.SYNC_AMAZON_TO_SAGE: self._handle_sync_amazon,
            TaskType.SYNC_EBAY_TO_SAGE: self._handle_sync_ebay,
            TaskType.SYNC_SHOPIFY_TO_SAGE: self._handle_sync_shopify,
            TaskType.DAILY_MORNING_SYNC: self._handle_daily_sync,
            TaskType.DAILY_NOON_SYNC: self._handle_daily_sync,
            TaskType.FULL_SYNC: self._handle_full_sync,
            TaskType.HEALTH_CHECK: self._handle_health_check,
            TaskType.GET_SAGE_STATUS: self._handle_get_sage_status,
        }
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def current_task_id(self) -> Optional[str]:
        return self._current_task.task_id if self._current_task else None
    
    async def start(self):
        """Start the task executor."""
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("Task executor started")
    
    async def stop(self):
        """Stop the task executor."""
        self._running = False
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Task executor stopped")
    
    async def _worker_loop(self):
        """Main worker loop that processes tasks."""
        while self._running:
            try:
                # Get next task with timeout
                task = await self.task_queue.dequeue(timeout=5.0)
                
                if task:
                    await self._execute_task(task)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(1)
    
    async def _execute_task(self, task: Task):
        """Execute a single task."""
        self._current_task = task
        task_logger = TaskLogger(task.task_id, task.task_type)
        
        task_logger.info(f"Starting execution")
        started_at = datetime.utcnow()
        
        result = TaskResult(
            task_id=task.task_id,
            status=TaskStatus.IN_PROGRESS,
            started_at=started_at,
            attempt_number=1,
        )
        
        try:
            # Get handler for task type
            handler = self._handlers.get(TaskType(task.task_type))
            
            if not handler:
                raise ValueError(f"Unknown task type: {task.task_type}")
            
            # Execute with timeout
            try:
                task_result = await asyncio.wait_for(
                    handler(task, task_logger),
                    timeout=task.timeout_seconds or self.config.task_timeout
                )
                
                result.status = TaskStatus.COMPLETED
                result.result = task_result
                
                task_logger.info("Completed successfully")
                
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Task timed out after {task.timeout_seconds}s"
                )
            
        except Sage50Error as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.error_code = "SAGE50_ERROR"
            task_logger.error(f"Sage 50 error: {e}")
            
        except TimeoutError as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.error_code = "TIMEOUT"
            task_logger.error(f"Timeout: {e}")
            
        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.error_code = "EXECUTION_ERROR"
            task_logger.exception(f"Execution error: {e}")
        
        finally:
            # Calculate duration
            completed_at = datetime.utcnow()
            result.completed_at = completed_at
            result.duration_ms = int(
                (completed_at - started_at).total_seconds() * 1000
            )
            
            # Update queue
            self.task_queue.complete_task(
                task.task_id,
                success=(result.status == TaskStatus.COMPLETED)
            )
            
            # Handle retry if failed
            if result.status == TaskStatus.FAILED:
                await self._handle_retry(task, result, task_logger)
            
            # Report result
            if self.on_result:
                try:
                    await self._report_result(result)
                except Exception as e:
                    task_logger.error(f"Failed to report result: {e}")
            
            self._current_task = None
    
    async def _handle_retry(
        self,
        task: Task,
        result: TaskResult,
        task_logger: TaskLogger
    ):
        """Handle task retry logic."""
        if result.attempt_number < task.max_retries:
            delay = self.config.retry_delay_seconds * result.attempt_number
            task_logger.info(
                f"Scheduling retry {result.attempt_number + 1}/{task.max_retries} "
                f"in {delay}s"
            )
            
            # Increment attempt counter
            # Note: In a real implementation, you'd want to track this properly
            await self.task_queue.requeue(task, delay_seconds=delay)
    
    async def _report_result(self, result: TaskResult):
        """Report task result via callback."""
        if self.on_result:
            if asyncio.iscoroutinefunction(self.on_result):
                await self.on_result(result)
            else:
                self.on_result(result)
    
    # ===== Task Handlers =====
    
    async def _handle_create_sales_order(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Create a sales order in Sage 50."""
        order_data = task.payload.get("order")
        
        if not order_data:
            raise ValueError("Missing 'order' in task payload")
        
        order = SalesOrder(**order_data)
        log.info(f"Creating order for {order.platform_order_id}")
        
        # Run in executor to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.create_sales_order,
            order
        )
        
        return result
    
    async def _handle_get_sales_order(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Get a sales order from Sage 50."""
        order_ref = task.payload.get("order_ref")
        
        if not order_ref:
            raise ValueError("Missing 'order_ref' in task payload")
        
        log.info(f"Getting order: {order_ref}")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.find_sales_order,
            order_ref
        )
        
        if result is None:
            return {"found": False, "order_ref": order_ref}
        
        return {"found": True, **result}
    
    async def _handle_create_customer(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Create a customer in Sage 50."""
        customer_data = task.payload.get("customer")
        
        if not customer_data:
            raise ValueError("Missing 'customer' in task payload")
        
        customer = Customer(**customer_data)
        log.info(f"Creating customer: {customer.account_ref}")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.create_customer,
            customer
        )
        
        return result
    
    async def _handle_get_customer(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Get a customer from Sage 50."""
        account_ref = task.payload.get("account_ref")
        
        if not account_ref:
            raise ValueError("Missing 'account_ref' in task payload")
        
        log.info(f"Getting customer: {account_ref}")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.find_customer,
            account_ref
        )
        
        if result is None:
            return {"found": False, "account_ref": account_ref}
        
        return {"found": True, **result}
    
    async def _handle_search_customers(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Search for customers."""
        query = task.payload.get("query", "")
        limit = task.payload.get("limit", 50)
        
        log.info(f"Searching customers: {query}")
        
        # Note: This would need to be implemented in Sage50Operations
        # For now, return empty result
        return {"results": [], "query": query}
    
    async def _handle_get_product(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Get a product from Sage 50."""
        sku = task.payload.get("sku")
        
        if not sku:
            raise ValueError("Missing 'sku' in task payload")
        
        log.info(f"Getting product: {sku}")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.find_product,
            sku
        )
        
        if result is None:
            return {"found": False, "sku": sku}
        
        return {"found": True, **result}
    
    async def _handle_search_products(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Search for products."""
        query = task.payload.get("query", "")
        limit = task.payload.get("limit", 50)
        
        log.info(f"Searching products: {query}")
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None,
            self.sage_ops.search_products,
            query,
            limit
        )
        
        return {"results": results, "count": len(results)}
    
    async def _handle_batch_create_orders(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Create multiple orders in batch."""
        orders_data = task.payload.get("orders", [])
        stop_on_error = task.payload.get("stop_on_error", False)
        
        if not orders_data:
            return {"error": "No orders provided"}
        
        orders = [SalesOrder(**o) for o in orders_data]
        log.info(f"Batch creating {len(orders)} orders")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.batch_create_orders,
            orders,
            stop_on_error
        )
        
        return result.model_dump()
    
    async def _handle_sync_orders(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Sync orders from ecommerce platform to Sage 50."""
        orders_data = task.payload.get("orders", [])
        platform = task.payload.get("platform", "unknown")
        
        log.info(f"Syncing {len(orders_data)} orders from {platform}")
        
        if not orders_data:
            return {
                "synced": 0,
                "failed": 0,
                "skipped": 0,
                "platform": platform,
            }
        
        orders = [SalesOrder(**o) for o in orders_data]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.batch_create_orders,
            orders,
            False  # Don't stop on error
        )
        
        return {
            "synced": result.successful,
            "failed": result.failed,
            "skipped": result.skipped,
            "platform": platform,
            "created_order_refs": result.created_order_refs,
            "failed_orders": result.failed_orders,
        }
    
    async def _handle_sync_amazon(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Sync Amazon orders to Sage 50."""
        return await self._handle_platform_sync("amazon", task, log)
    
    async def _handle_sync_ebay(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Sync eBay orders to Sage 50."""
        return await self._handle_platform_sync("ebay", task, log)
    
    async def _handle_sync_shopify(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Sync Shopify orders to Sage 50."""
        return await self._handle_platform_sync("shopify", task, log)
    
    async def _handle_platform_sync(
        self, platform: str, task: Task, log: TaskLogger
    ) -> dict:
        """Sync a platform's orders to Sage 50."""
        from agent.sync_service import SyncService
        
        days_back = task.payload.get("days_back", 30)
        log.info(f"Syncing {platform.upper()} orders (last {days_back} days)")
        
        # Create sync service with Sage SDK
        sync_service = SyncService(self.config, self.sage_ops)
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            sync_service.sync_platform,
            platform,
            days_back
        )
        
        return result
    
    async def _handle_daily_sync(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Run daily sync for all platforms."""
        return await self._handle_full_sync(task, log)
    
    async def _handle_full_sync(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Sync all platforms to Sage 50."""
        from agent.sync_service import SyncService
        
        days_back = task.payload.get("days_back", 30)
        log.info(f"Running full sync (last {days_back} days)")
        
        # Create sync service with Sage SDK
        sync_service = SyncService(self.config, self.sage_ops)
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            sync_service.sync_all_platforms,
            days_back
        )
        
        return result
    
    async def _handle_health_check(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Perform health check."""
        log.info("Running health check")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sage_ops.health_check
        )
        
        return result
    
    async def _handle_get_sage_status(
        self, task: Task, log: TaskLogger
    ) -> dict:
        """Get Sage 50 connection status."""
        log.info("Getting Sage 50 status")
        
        return {
            "connected": self.sage_connector.is_connected,
            "company": self.sage_connector.company_name,
            "version": self.sage_connector.sage_version,
        }

