"""
Data models for the M&M 2.0 Agent.
Defines the structure of tasks, messages, and data transformations.

M&M 2.0 Flow:
1. Pull orders from Amazon/eBay/Shopify
2. Pull data from Sage Quantum
3. Clean/format data
4. Pull tracking from FedEx/UPS
5. Upload formatted data + tracking back to platforms
6. Generate email reports
"""

from enum import Enum
from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, Field
import uuid


class TaskType(str, Enum):
    """Types of tasks the agent can execute."""
    
    # === Data Pull Operations ===
    PULL_SAGE_ORDERS = "pull_sage_orders"
    PULL_SAGE_PRODUCTS = "pull_sage_products"
    PULL_SAGE_CUSTOMERS = "pull_sage_customers"
    PULL_SAGE_INVENTORY = "pull_sage_inventory"
    
    # === Data Push to Sage ===
    PUSH_ORDERS_TO_SAGE = "push_orders_to_sage"
    UPDATE_SAGE_INVENTORY = "update_sage_inventory"
    
    # === Tracking Operations ===
    PULL_TRACKING_INFO = "pull_tracking_info"
    FORMAT_TRACKING_DATA = "format_tracking_data"
    
    # === Data Formatting ===
    FORMAT_FOR_AMAZON = "format_for_amazon"
    FORMAT_FOR_EBAY = "format_for_ebay"
    FORMAT_FOR_SHOPIFY = "format_for_shopify"
    CLEAN_SPREADSHEET = "clean_spreadsheet"
    VALIDATE_DATA = "validate_data"
    
    # === Sync Operations ===
    SYNC_AMAZON_TO_SAGE = "sync_amazon_to_sage"
    SYNC_EBAY_TO_SAGE = "sync_ebay_to_sage"
    SYNC_SHOPIFY_TO_SAGE = "sync_shopify_to_sage"
    SYNC_TRACKING_TO_PLATFORMS = "sync_tracking_to_platforms"
    
    # === Batch Operations ===
    DAILY_MORNING_SYNC = "daily_morning_sync"
    DAILY_NOON_SYNC = "daily_noon_sync"
    FULL_SYNC = "full_sync"
    
    # === Report Generation ===
    GENERATE_SYNC_REPORT = "generate_sync_report"
    GENERATE_ERROR_REPORT = "generate_error_report"
    SEND_EMAIL_REPORT = "send_email_report"
    
    # === Utility ===
    HEALTH_CHECK = "health_check"
    GET_SAGE_STATUS = "get_sage_status"
    TEST_CONNECTION = "test_connection"


class TaskStatus(str, Enum):
    """Status of a task."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    WAITING_REVIEW = "waiting_review"  # For errors that need manual review


class TaskPriority(str, Enum):
    """Task priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class Platform(str, Enum):
    """Supported ecommerce platforms."""
    AMAZON = "amazon"
    EBAY = "ebay"
    SHOPIFY = "shopify"
    SAGE_QUANTUM = "sage_quantum"


class Task(BaseModel):
    """Represents a task to be executed by the agent."""
    
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: TaskType
    priority: TaskPriority = TaskPriority.NORMAL
    payload: dict[str, Any] = Field(default_factory=dict)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    timeout_seconds: int = 600  # 10 minutes for data operations
    max_retries: int = 3
    
    # Tracking
    correlation_id: Optional[str] = None  # Links to backend request
    source: str = "backend"
    
    # M&M specific
    platform: Optional[Platform] = None
    requires_review: bool = False  # If true, errors go to review queue
    
    class Config:
        use_enum_values = True


class TaskResult(BaseModel):
    """Result of a task execution."""
    
    task_id: str
    status: TaskStatus
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    
    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    
    # Data stats
    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    
    # Errors for review
    errors_for_review: list[dict] = Field(default_factory=list)
    
    # Retry info
    attempt_number: int = 1
    
    class Config:
        use_enum_values = True


class MessageType(str, Enum):
    """Types of WebSocket messages."""
    
    # Agent -> Server
    REGISTER = "register"
    HEARTBEAT = "heartbeat"
    TASK_RESULT = "task_result"
    STATUS_UPDATE = "status_update"
    DATA_UPLOAD = "data_upload"  # Agent sending pulled data to server
    ERROR_REPORT = "error_report"
    LOG = "log"
    
    # Server -> Agent
    TASK = "task"
    CANCEL_TASK = "cancel_task"
    CONFIG_UPDATE = "config_update"
    COMMAND = "command"
    ACK = "ack"
    DATA_REQUEST = "data_request"  # Server requesting data pull


class AgentMessage(BaseModel):
    """Message sent from agent to server."""
    
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    message_type: MessageType
    agent_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    payload: dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class ServerMessage(BaseModel):
    """Message received from server."""
    
    message_id: str
    message_type: MessageType
    timestamp: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class AgentStatus(BaseModel):
    """Current status of the agent."""
    
    agent_id: str
    version: str
    status: str = "online"
    
    # Connection
    connected_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    connection_type: str = "websocket"
    
    # Tasks
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_pending_review: int = 0
    
    # Sage Quantum
    sage_connected: bool = False
    sage_company: Optional[str] = None
    sage_version: Optional[str] = None
    
    # Last sync times
    last_amazon_sync: Optional[datetime] = None
    last_ebay_sync: Optional[datetime] = None
    last_shopify_sync: Optional[datetime] = None
    
    # System
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    disk_free_gb: Optional[float] = None


# ===== M&M 2.0 Data Models =====

class OrderLine(BaseModel):
    """A line item in an order."""
    
    sku: str
    description: str
    quantity: int
    unit_price: float
    unit_of_measure: str = "each"  # each, box, pack, etc.
    discount_percent: float = 0.0
    tax_amount: float = 0.0
    
    # Platform-specific IDs
    amazon_asin: Optional[str] = None
    ebay_item_id: Optional[str] = None
    shopify_variant_id: Optional[str] = None


class Order(BaseModel):
    """Order data that can flow between platforms."""
    
    # Internal reference
    internal_id: Optional[str] = None
    
    # Platform references
    amazon_order_id: Optional[str] = None
    ebay_order_id: Optional[str] = None
    shopify_order_id: Optional[str] = None
    sage_order_ref: Optional[str] = None
    
    # Order details
    order_date: datetime
    status: str = "pending"
    
    # Customer
    customer_id: Optional[str] = None  # Customer ID in Sage (e.g., "Amazon", "ECOMMERCE TEST")
    customer_name: str
    customer_email: Optional[str] = None
    customer_phone: Optional[str] = None
    
    # Shipping address
    ship_name: Optional[str] = None
    ship_address_1: Optional[str] = None
    ship_address_2: Optional[str] = None
    ship_city: Optional[str] = None
    ship_state: Optional[str] = None
    ship_postcode: Optional[str] = None
    ship_country: str = "GB"
    ship_method: Optional[str] = None  # FedEx, UPS, Royal Mail, etc.
    
    # Line items
    lines: list[OrderLine] = Field(default_factory=list)
    
    # Totals
    subtotal: float = 0.0
    shipping_cost: float = 0.0
    tax_total: float = 0.0
    total: float = 0.0
    
    # Tracking
    tracking_number: Optional[str] = None
    carrier: Optional[str] = None  # fedex, ups, etc.
    ship_date: Optional[datetime] = None
    
    # Source tracking
    source_platform: Platform
    needs_sync_to: list[Platform] = Field(default_factory=list)


class TrackingInfo(BaseModel):
    """Tracking information from carriers."""
    
    tracking_number: str
    carrier: str  # fedex, ups, usps, royal_mail, etc.
    
    # Status
    status: str  # in_transit, delivered, exception, etc.
    status_detail: Optional[str] = None
    
    # Dates
    ship_date: Optional[datetime] = None
    estimated_delivery: Optional[datetime] = None
    actual_delivery: Optional[datetime] = None
    
    # Location
    current_location: Optional[str] = None
    
    # Links to orders
    order_ids: list[str] = Field(default_factory=list)
    
    # Raw carrier response
    carrier_response: Optional[dict] = None


class Product(BaseModel):
    """Product/inventory data."""
    
    sku: str
    title: str
    description: Optional[str] = None
    
    # Identifiers
    amazon_asin: Optional[str] = None
    ebay_item_id: Optional[str] = None
    shopify_product_id: Optional[str] = None
    sage_stock_code: Optional[str] = None
    
    # Pricing
    price: float = 0.0
    cost: float = 0.0
    
    # Inventory
    quantity_available: int = 0
    quantity_reserved: int = 0
    reorder_level: int = 0
    
    # Attributes
    weight: Optional[float] = None
    dimensions: Optional[str] = None
    category: Optional[str] = None


class Customer(BaseModel):
    """Customer data for Sage 50."""
    
    # Core identifiers
    customer_id: Optional[str] = None
    account_ref: Optional[str] = None  # Sage account reference
    
    # Contact info
    name: str
    company: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    
    # Billing address
    billing_address_1: Optional[str] = None
    billing_address_2: Optional[str] = None
    billing_city: Optional[str] = None
    billing_state: Optional[str] = None
    billing_postcode: Optional[str] = None
    billing_country: Optional[str] = None
    
    # Shipping address
    shipping_address_1: Optional[str] = None
    shipping_address_2: Optional[str] = None
    shipping_city: Optional[str] = None
    shipping_state: Optional[str] = None
    shipping_postcode: Optional[str] = None
    shipping_country: Optional[str] = None
    
    # Platform identifiers
    amazon_customer_id: Optional[str] = None
    ebay_buyer_id: Optional[str] = None
    shopify_customer_id: Optional[str] = None


class SpreadsheetData(BaseModel):
    """Represents spreadsheet data for transformation."""
    
    filename: Optional[str] = None
    sheet_name: Optional[str] = None
    
    # Column mappings
    columns: list[str] = Field(default_factory=list)
    
    # Data rows
    rows: list[dict[str, Any]] = Field(default_factory=list)
    
    # Validation
    row_count: int = 0
    error_count: int = 0
    errors: list[dict] = Field(default_factory=list)


class DataValidationError(BaseModel):
    """Represents a data validation error for review."""
    
    error_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    row_number: Optional[int] = None
    column: Optional[str] = None
    value: Optional[str] = None
    error_type: str  # missing, invalid_format, duplicate, etc.
    message: str
    suggestion: Optional[str] = None
    
    # Original data for reference
    original_row: Optional[dict] = None
    
    # Can auto-fix?
    auto_fixable: bool = False
    auto_fix_value: Optional[str] = None


class SyncReport(BaseModel):
    """Report of a sync operation."""
    
    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    report_type: str  # daily_morning, daily_noon, manual, etc.
    
    # Timing
    started_at: datetime
    completed_at: Optional[datetime] = None
    
    # Platforms synced
    platforms_synced: list[str] = Field(default_factory=list)
    
    # Results per platform
    amazon_results: Optional[dict] = None
    ebay_results: Optional[dict] = None
    shopify_results: Optional[dict] = None
    sage_results: Optional[dict] = None
    
    # Overall stats
    total_orders_processed: int = 0
    total_products_synced: int = 0
    total_tracking_updated: int = 0
    
    # Errors
    errors_count: int = 0
    errors_for_review: list[DataValidationError] = Field(default_factory=list)
    
    # Success summary
    success_summary: str = ""
    
    # Email sent?
    email_sent: bool = False
    email_recipients: list[str] = Field(default_factory=list)


class ColumnMapping(BaseModel):
    """Mapping rule for spreadsheet columns."""
    
    source_column: str
    target_column: str
    platform: Platform
    
    # Transformation
    transform_type: Optional[str] = None  # uppercase, lowercase, date_format, etc.
    transform_params: Optional[dict] = None
    
    # Validation
    required: bool = False
    default_value: Optional[str] = None
    validation_regex: Optional[str] = None


class PlatformFormat(BaseModel):
    """Format requirements for a platform."""
    
    platform: Platform
    file_type: str = "csv"  # csv, xlsx, flat_file
    
    # Required columns
    required_columns: list[str] = Field(default_factory=list)
    
    # Column order
    column_order: list[str] = Field(default_factory=list)
    
    # Column mappings
    mappings: list[ColumnMapping] = Field(default_factory=list)
    
    # Validation rules
    date_format: str = "%Y-%m-%d"
    decimal_places: int = 2
    text_encoding: str = "utf-8"
