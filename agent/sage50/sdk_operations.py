"""
Sage 50 SDK (SDO) Operations.
Automatic import/export using Sage Data Objects COM interface.

This allows FULLY AUTOMATIC operations - no manual CSV import needed!

Requirements:
- Sage 50 installed on the machine
- pywin32 installed (pip install pywin32)
- Sage 50 NOT running (or run as same user)

What SDO Can Do:
- Create sales orders automatically
- Create/update customers
- Read all orders, products, customers
- Update order status and tracking
"""

from typing import Optional, Any
from datetime import datetime
from loguru import logger

try:
    import win32com.client
    import pythoncom
    HAS_COM = True
except ImportError:
    HAS_COM = False

from agent.config import AgentConfig
from agent.models import Order, OrderLine, Customer, Product, Platform


class SageSDKError(Exception):
    """Error in Sage SDK operation."""
    pass


class SageSDK:
    """
    Direct Sage 50 SDK (SDO) interface.
    
    This provides AUTOMATIC operations - no manual import/export needed.
    
    Usage:
        sdk = SageSDK(config)
        sdk.connect()
        
        # Create order automatically
        sdk.create_sales_order(order)
        
        # Read unshipped orders
        orders = sdk.get_unshipped_orders()
        
        sdk.disconnect()
    """
    
    # SDO ProgIDs to try (different Sage versions)
    SDO_PROGIDS = [
        "SageDataObject50.SDOEngine",
        "SageDataObject50v29.SDOEngine",  # 2024
        "SageDataObject50v28.SDOEngine",  # 2023
        "SageDataObject50v27.SDOEngine",  # 2022
    ]
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self._engine = None
        self._workspace = None
        self._connected = False
        self._com_initialized = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def _init_com(self):
        """Initialize COM threading."""
        if not self._com_initialized:
            pythoncom.CoInitialize()
            self._com_initialized = True
    
    def _cleanup_com(self):
        """Cleanup COM."""
        if self._com_initialized:
            pythoncom.CoUninitialize()
            self._com_initialized = False
    
    def connect(self) -> bool:
        """
        Connect to Sage 50 via SDK.
        
        Note: Sage 50 should NOT be running, or must be run as same user.
        """
        if not HAS_COM:
            raise SageSDKError("pywin32 not installed. Run: pip install pywin32")
        
        self._init_com()
        
        # Try different ProgIDs
        for prog_id in self.SDO_PROGIDS:
            try:
                logger.info(f"Trying Sage SDK: {prog_id}")
                self._engine = win32com.client.Dispatch(prog_id)
                logger.info(f"Successfully created: {prog_id}")
                break
            except Exception as e:
                logger.debug(f"Failed {prog_id}: {e}")
                continue
        
        if not self._engine:
            raise SageSDKError(
                "Could not create Sage SDK object. "
                "Make sure Sage 50 is installed."
            )
        
        # Connect to company data
        data_path = self.config.sage50_company_path
        if not data_path:
            raise SageSDKError("SAGE_COMPANY_PATH not configured")
        
        try:
            # Create workspace and connect
            self._workspace = self._engine.Workspaces.Add("MNMAgent")
            
            self._workspace.Connect(
                data_path,
                self.config.sage50_username or "",
                self.config.sage50_password or "",
                "MNMAgent"
            )
            
            self._connected = True
            logger.info(f"Connected to Sage 50 via SDK: {data_path}")
            
            return True
            
        except Exception as e:
            raise SageSDKError(f"Failed to connect to Sage: {e}")
    
    def disconnect(self):
        """Disconnect from Sage."""
        try:
            if self._workspace:
                self._workspace.Disconnect()
                self._workspace = None
            self._engine = None
            self._connected = False
            logger.info("Disconnected from Sage SDK")
        except Exception as e:
            logger.warning(f"Error disconnecting: {e}")
        finally:
            self._cleanup_com()
    
    # ===== READ OPERATIONS =====
    
    def get_unshipped_orders(self) -> list[Order]:
        """
        Get all unshipped orders from Sage.
        
        Returns orders where:
        - Order status is not Complete
        - Items not fully despatched
        """
        if not self._connected:
            raise SageSDKError("Not connected to Sage")
        
        orders = []
        
        try:
            # Get SalesOrder record set
            sales_orders = self._workspace.CreateObject("SalesOrder")
            
            # Find all orders
            sales_orders.FindFirst()
            
            while not sales_orders.EOF:
                # Check if order is not complete
                status = getattr(sales_orders, 'OrderStatus', 0)
                despatch_status = getattr(sales_orders, 'DespatchStatus', 0)
                
                # Status 2 = Complete, DespatchStatus 2 = Fully Despatched
                if status != 2 and despatch_status != 2:
                    order = self._parse_sales_order(sales_orders)
                    orders.append(order)
                
                sales_orders.FindNext()
            
            logger.info(f"Found {len(orders)} unshipped orders via SDK")
            return orders
            
        except Exception as e:
            logger.error(f"Error reading orders: {e}")
            raise SageSDKError(f"Failed to read orders: {e}")
    
    def _parse_sales_order(self, record) -> Order:
        """Parse a SalesOrder record into Order model."""
        # Get basic fields
        order = Order(
            sage_order_ref=str(getattr(record, 'OrderNumber', '')),
            order_date=getattr(record, 'OrderDate', datetime.now()),
            customer_name=getattr(record, 'Name', ''),
            customer_email=getattr(record, 'Email', ''),
            customer_phone=getattr(record, 'Telephone', ''),
            
            ship_name=getattr(record, 'DeliveryName', '') or getattr(record, 'Name', ''),
            ship_address_1=getattr(record, 'DeliveryAddress1', '') or getattr(record, 'Address1', ''),
            ship_address_2=getattr(record, 'DeliveryAddress2', '') or getattr(record, 'Address2', ''),
            ship_city=getattr(record, 'DeliveryAddress3', '') or getattr(record, 'Address3', ''),
            ship_postcode=getattr(record, 'DeliveryAddress5', '') or getattr(record, 'Address5', ''),
            
            subtotal=float(getattr(record, 'NetAmount', 0) or 0),
            tax_total=float(getattr(record, 'TaxAmount', 0) or 0),
            total=float(getattr(record, 'GrossAmount', 0) or 0),
            
            tracking_number=getattr(record, 'CourierNumber', '') or '',
            carrier=getattr(record, 'CourierName', '') or '',
            
            source_platform=Platform.SAGE_QUANTUM,
        )
        
        # Try to get line items
        try:
            items = record.Items
            for i in range(items.Count):
                item = items.Item(i)
                line = OrderLine(
                    sku=getattr(item, 'StockCode', ''),
                    description=getattr(item, 'Description', ''),
                    quantity=int(getattr(item, 'Quantity', 0) or 0),
                    unit_price=float(getattr(item, 'UnitPrice', 0) or 0),
                )
                order.lines.append(line)
        except Exception:
            pass
        
        return order
    
    # ===== CREATE OPERATIONS (Automatic Import!) =====
    
    def create_sales_order(self, order: Order) -> dict:
        """
        Create a sales order in Sage 50 AUTOMATICALLY.
        
        No manual import needed - order appears in Sage instantly!
        
        Args:
            order: Order data to create
            
        Returns:
            Dict with sage_order_ref and success status
        """
        if not self._connected:
            raise SageSDKError("Not connected to Sage")
        
        try:
            # Create new SalesOrder record
            sales_order = self._workspace.CreateObject("SalesOrder")
            sales_order.AddNew()
            
            # Get or create customer account
            account_ref = self._get_or_create_customer_account(order)
            
            # Set header fields
            sales_order.Fields("ACCOUNT_REF").Value = account_ref
            sales_order.Fields("NAME").Value = order.customer_name[:60]
            sales_order.Fields("ADDRESS_1").Value = (order.ship_address_1 or "")[:60]
            sales_order.Fields("ADDRESS_2").Value = (order.ship_address_2 or "")[:60]
            sales_order.Fields("ADDRESS_3").Value = (order.ship_city or "")[:60]
            sales_order.Fields("ADDRESS_5").Value = (order.ship_postcode or "")[:10]
            
            if order.customer_email:
                sales_order.Fields("E_MAIL").Value = order.customer_email[:60]
            if order.customer_phone:
                sales_order.Fields("TELEPHONE").Value = order.customer_phone[:30]
            
            # Store platform reference in notes
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            platform = str(order.source_platform).replace("Platform.", "")
            
            if platform_id:
                sales_order.Fields("NOTES_1").Value = f"{platform}: {platform_id}"[:60]
            
            sales_order.Fields("ORDER_DATE").Value = order.order_date
            
            # Add line items
            items = sales_order.Items
            for line in order.lines:
                item = items.Add()
                item.Fields("STOCK_CODE").Value = line.sku[:30]
                item.Fields("DESCRIPTION").Value = line.description[:60]
                item.Fields("QTY_ORDER").Value = line.quantity
                item.Fields("UNIT_PRICE").Value = line.unit_price
            
            # Add shipping as line item if present
            if order.shipping_cost > 0:
                ship_item = items.Add()
                ship_item.Fields("STOCK_CODE").Value = "CARRIAGE"
                ship_item.Fields("DESCRIPTION").Value = "Shipping"
                ship_item.Fields("QTY_ORDER").Value = 1
                ship_item.Fields("UNIT_PRICE").Value = order.shipping_cost
            
            # Save the order
            sales_order.Update()
            
            # Get the assigned order number
            sage_order_ref = str(sales_order.Fields("ORDER_NUMBER").Value)
            
            logger.info(f"Created Sage order {sage_order_ref} for {platform_id}")
            
            return {
                "success": True,
                "sage_order_ref": sage_order_ref,
                "platform_order_id": platform_id,
                "message": "Order created automatically in Sage",
            }
            
        except Exception as e:
            logger.error(f"Failed to create order: {e}")
            raise SageSDKError(f"Failed to create order: {e}")
    
    def _get_or_create_customer_account(self, order: Order) -> str:
        """Get existing customer account or create new one."""
        # Generate account ref from customer name
        name = order.customer_name or "CUSTOMER"
        account_ref = name[:8].upper().replace(" ", "")
        
        # Add platform prefix
        if order.amazon_order_id:
            account_ref = "AMZ" + account_ref[:5]
        elif order.ebay_order_id:
            account_ref = "EBY" + account_ref[:5]
        elif order.shopify_order_id:
            account_ref = "SHP" + account_ref[:5]
        
        # Check if exists
        try:
            customer = self._workspace.CreateObject("SalesRecord")
            if customer.Find("ACCOUNT_REF", account_ref):
                return account_ref
        except Exception:
            pass
        
        # Create new customer
        try:
            customer = self._workspace.CreateObject("SalesRecord")
            customer.AddNew()
            
            customer.Fields("ACCOUNT_REF").Value = account_ref
            customer.Fields("NAME").Value = order.customer_name[:60]
            customer.Fields("ADDRESS_1").Value = (order.ship_address_1 or "")[:60]
            customer.Fields("ADDRESS_2").Value = (order.ship_address_2 or "")[:60]
            customer.Fields("ADDRESS_3").Value = (order.ship_city or "")[:60]
            customer.Fields("ADDRESS_5").Value = (order.ship_postcode or "")[:10]
            
            if order.customer_email:
                customer.Fields("E_MAIL").Value = order.customer_email[:60]
            
            customer.Update()
            
            logger.info(f"Created customer account: {account_ref}")
            
        except Exception as e:
            logger.warning(f"Could not create customer: {e}")
        
        return account_ref
    
    def update_order_tracking(
        self,
        sage_order_ref: str,
        tracking_number: str,
        carrier: str,
    ) -> bool:
        """
        Update tracking info on an existing order.
        
        Args:
            sage_order_ref: Sage order number
            tracking_number: Tracking number to set
            carrier: Carrier name (FedEx, UPS, etc.)
            
        Returns:
            True if updated successfully
        """
        if not self._connected:
            raise SageSDKError("Not connected to Sage")
        
        try:
            sales_order = self._workspace.CreateObject("SalesOrder")
            
            if sales_order.Find("ORDER_NUMBER", sage_order_ref):
                sales_order.Edit()
                sales_order.Fields("COURIER_NUMBER").Value = tracking_number[:30]
                sales_order.Fields("COURIER_NAME").Value = carrier[:30]
                sales_order.Update()
                
                logger.info(f"Updated tracking for order {sage_order_ref}")
                return True
            else:
                logger.warning(f"Order not found: {sage_order_ref}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to update tracking: {e}")
            return False
    
    # ===== PRODUCT OPERATIONS =====
    
    def get_products(self, limit: int = 500) -> list[Product]:
        """Get products/stock items from Sage."""
        if not self._connected:
            raise SageSDKError("Not connected to Sage")
        
        products = []
        
        try:
            stock = self._workspace.CreateObject("StockRecord")
            stock.FindFirst()
            
            count = 0
            while not stock.EOF and count < limit:
                product = Product(
                    sku=getattr(stock, 'StockCode', ''),
                    sage_stock_code=getattr(stock, 'StockCode', ''),
                    title=getattr(stock, 'Description', ''),
                    price=float(getattr(stock, 'SalesPrice', 0) or 0),
                    cost=float(getattr(stock, 'CostPrice', 0) or 0),
                    quantity_available=int(getattr(stock, 'QtyInStock', 0) or 0),
                )
                products.append(product)
                
                stock.FindNext()
                count += 1
            
            return products
            
        except Exception as e:
            logger.error(f"Error reading products: {e}")
            return []
    
    # ===== HEALTH CHECK =====
    
    def test_connection(self) -> dict:
        """Test SDK connection."""
        result = {
            "success": False,
            "message": "",
            "sdk_available": HAS_COM,
        }
        
        if not HAS_COM:
            result["message"] = "pywin32 not installed"
            return result
        
        try:
            self.connect()
            result["success"] = True
            result["message"] = "SDK connection successful"
            
            # Try to count orders
            try:
                orders = self.get_unshipped_orders()
                result["unshipped_orders"] = len(orders)
            except Exception:
                pass
            
            self.disconnect()
            
        except SageSDKError as e:
            result["message"] = str(e)
        except Exception as e:
            result["message"] = f"Error: {e}"
        
        return result

