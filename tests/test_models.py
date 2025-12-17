"""Tests for data models."""

import pytest
from datetime import datetime

from agent.models import (
    Task,
    TaskResult,
    TaskType,
    TaskStatus,
    TaskPriority,
    SalesOrder,
    SalesOrderLine,
    Customer,
)


class TestTask:
    """Tests for Task model."""
    
    def test_task_creation(self):
        """Test basic task creation."""
        task = Task(
            task_type=TaskType.CREATE_SALES_ORDER,
            payload={"order": {"ref": "123"}},
        )
        
        assert task.task_id is not None
        assert task.task_type == "create_sales_order"
        assert task.priority == "normal"
        assert task.payload == {"order": {"ref": "123"}}
    
    def test_task_with_priority(self):
        """Test task with custom priority."""
        task = Task(
            task_type=TaskType.SYNC_ORDERS,
            priority=TaskPriority.HIGH,
        )
        
        assert task.priority == "high"
    
    def test_task_serialization(self):
        """Test task JSON serialization."""
        task = Task(
            task_type=TaskType.HEALTH_CHECK,
            payload={"test": True},
        )
        
        json_data = task.model_dump_json()
        assert "health_check" in json_data


class TestTaskResult:
    """Tests for TaskResult model."""
    
    def test_result_creation(self):
        """Test basic result creation."""
        result = TaskResult(
            task_id="test-123",
            status=TaskStatus.COMPLETED,
            result={"success": True},
        )
        
        assert result.task_id == "test-123"
        assert result.status == "completed"
        assert result.result == {"success": True}
    
    def test_failed_result(self):
        """Test failed result with error."""
        result = TaskResult(
            task_id="test-456",
            status=TaskStatus.FAILED,
            error="Connection timeout",
            error_code="TIMEOUT",
        )
        
        assert result.status == "failed"
        assert result.error == "Connection timeout"


class TestSalesOrder:
    """Tests for SalesOrder model."""
    
    def test_order_creation(self):
        """Test sales order creation."""
        order = SalesOrder(
            order_ref="ORD-001",
            customer_ref="CUST001",
            order_date=datetime.now(),
            platform="amazon",
            platform_order_id="111-2222-3333",
            lines=[
                SalesOrderLine(
                    sku="BOLT-M8",
                    description="M8 Bolt",
                    quantity=100,
                    unit_price=0.15,
                )
            ],
        )
        
        assert order.order_ref == "ORD-001"
        assert len(order.lines) == 1
        assert order.lines[0].sku == "BOLT-M8"
    
    def test_order_with_shipping(self):
        """Test order with shipping details."""
        order = SalesOrder(
            order_ref="ORD-002",
            customer_ref="CUST001",
            order_date=datetime.now(),
            platform="ebay",
            platform_order_id="12345",
            lines=[
                SalesOrderLine(
                    sku="NUT-M8",
                    description="M8 Nut",
                    quantity=50,
                    unit_price=0.05,
                )
            ],
            delivery_name="John Smith",
            delivery_postcode="SW1A 1AA",
            shipping_cost=5.99,
        )
        
        assert order.delivery_name == "John Smith"
        assert order.shipping_cost == 5.99


class TestCustomer:
    """Tests for Customer model."""
    
    def test_customer_creation(self):
        """Test customer creation."""
        customer = Customer(
            account_ref="AMAZON001",
            company_name="Amazon UK",
            email="marketplace@amazon.co.uk",
        )
        
        assert customer.account_ref == "AMAZON001"
        assert customer.country == "GB"  # default
        assert customer.currency == "GBP"  # default

