"""
Sage 50 Accounting (US) SDK Operations.
Uses PeachtreeAccounting COM interface for Sage 50 US versions.

This allows FULLY AUTOMATIC operations - no manual CSV import needed!

Requirements:
- Sage 50 Accounting installed on the machine (US version)
- pythonnet installed (pip install pythonnet) - preferred
- OR pywin32 installed (pip install pywin32) - fallback
- Sage 50 NOT running (or run as same user)

What the SDK Can Do:
- Create sales orders automatically
- Create/update customers
- Read all orders, products, customers
- Update order status and tracking
"""

from typing import Optional, Any
from datetime import datetime
from pathlib import Path
import tempfile
import xml.etree.ElementTree as ET
from loguru import logger

# Try pythonnet first (works better with .NET Interop assemblies)
HAS_PYTHONNET = False
HAS_COM = False

try:
    import clr
    HAS_PYTHONNET = True
    logger.debug("pythonnet available")
except ImportError:
    pass

try:
    import win32com.client
    import pythoncom
    HAS_COM = True
    logger.debug("win32com available")
except ImportError:
    pass

from agent.config import AgentConfig
from agent.models import Order, OrderLine, Customer, Product, Platform


class SageSDKError(Exception):
    """Error in Sage SDK operation."""
    pass


class SageSDK:
    """
    Direct Sage 50 Accounting (US/Peachtree) interface.
    
    This provides AUTOMATIC operations - no manual import/export needed.
    Uses PeachtreeAccounting.Login COM object.
    
    Usage:
        sdk = SageSDK(config)
        sdk.connect()
        
        # Create order automatically
        sdk.create_sales_order(order)
        
        # Read unshipped orders
        orders = sdk.get_unshipped_orders()
        
        sdk.disconnect()
    """
    
    # Peachtree ProgIDs to try (different versions)
    PEACHTREE_PROGIDS = [
        "PeachtreeAccounting.Login.31",   # 2024 (v31)
        "PeachtreeAccounting.Login.30",   # 2023 (v30)
        "PeachtreeAccounting.Login.29",   # 2022 (v29)
        "PeachtreeAccounting.Login",      # Default/latest
    ]
    
    # Legacy SDO ProgIDs (for UK Sage 50 Accounts)
    SDO_PROGIDS = [
        "SageDataObject50.SDOEngine",
        "SageDataObject50v29.SDOEngine",
        "SageDataObject50v28.SDOEngine",
    ]
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self._login = None
        self._company = None
        self._connected = False
        self._com_initialized = False
        self._api_type = None  # "peachtree" or "sdo"
        self._company_was_already_open = False  # Don't close if user had it open
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def _init_com(self):
        """Initialize COM threading."""
        if HAS_COM and not self._com_initialized:
            pythoncom.CoInitialize()
            self._com_initialized = True
    
    def _cleanup_com(self):
        """Cleanup COM."""
        if self._com_initialized:
            try:
                pythoncom.CoUninitialize()
            except:
                pass
            self._com_initialized = False
    
    def _connect_pythonnet(self) -> bool:
        """Connect using pythonnet (.NET Interop) - preferred method."""
        import clr
        
        # Add reference to Sage Interop DLL
        dll_paths = [
            r"C:\Program Files (x86)\Sage\Peachtree\Interop.PeachwServer.dll",
            r"C:\Program Files\Sage\Peachtree\Interop.PeachwServer.dll",
        ]
        
        dll_loaded = False
        for dll_path in dll_paths:
            try:
                clr.AddReference(dll_path)
                dll_loaded = True
                logger.info(f"Loaded Sage DLL: {dll_path}")
                break
            except Exception as e:
                logger.debug(f"Could not load {dll_path}: {e}")
                continue
        
        if not dll_loaded:
            raise SageSDKError("Could not find Interop.PeachwServer.dll")
        
        # Import and create Login object
        # IMPORTANT: Must cast to Application to expose all methods!
        from Interop.PeachwServer import Login, Application
        login = Login()
        self._login = login
        self._api_type = "peachtree"
        
        # Get credentials
        username = self.config.sage50_username or "Peachtree Software"
        password = self.config.sage50_password or "9E5643PCU118X6C"
        
        logger.info(f"Connecting with pythonnet (user={username})...")
        
        # Get Application object and cast to proper type
        obj = login.GetApplication(username, password)
        
        if obj is None:
            raise SageSDKError("GetApplication returned None")
        
        # Cast to Application to expose all methods
        app = Application(obj)
        logger.info(f"Got Application object: {type(app)}")
        
        # Check if company is already open (user has Sage running)
        if app.get_CompanyIsOpen():
            self._company_was_already_open = True
            logger.info(f"Company already open: {app.get_CurrentCompanyName()} (will NOT close on disconnect)")
        else:
            # Open company if path provided
            self._company_was_already_open = False
            data_path = self.config.sage50_company_path
            if data_path:
                logger.info(f"Opening company: {data_path}")
                app.OpenCompany(data_path)
                logger.info(f"Company opened: {app.get_CurrentCompanyName()}")
        
        self._company = app
        self._connected = True
        logger.info("Connected to Sage 50 via pythonnet!")
        return True
    
    def connect(self) -> bool:
        """
        Connect to Sage 50 via SDK.
        
        Tries pythonnet first (better for .NET Interop), then win32com fallback.
        Note: Sage 50 should NOT be running, or must be run as same user.
        """
        # Try pythonnet first (works better with .NET Interop assemblies)
        if HAS_PYTHONNET:
            try:
                return self._connect_pythonnet()
            except Exception as e:
                logger.warning(f"pythonnet connection failed: {e}, trying win32com...")
        
        # Fall back to win32com
        if not HAS_COM:
            raise SageSDKError("Neither pythonnet nor pywin32 installed. Run: pip install pythonnet")
        
        self._init_com()
        
        # Try Peachtree (US) first
        for prog_id in self.PEACHTREE_PROGIDS:
            try:
                logger.info(f"Trying Peachtree: {prog_id}")
                self._login = win32com.client.Dispatch(prog_id)
                self._api_type = "peachtree"
                logger.info(f"Successfully created: {prog_id}")
                break
            except Exception as e:
                logger.debug(f"Failed {prog_id}: {e}")
                continue
        
        # If Peachtree failed, try SDO (UK)
        if not self._login:
            for prog_id in self.SDO_PROGIDS:
                try:
                    logger.info(f"Trying SDO: {prog_id}")
                    self._login = win32com.client.Dispatch(prog_id)
                    self._api_type = "sdo"
                    logger.info(f"Successfully created: {prog_id}")
                    break
                except Exception as e:
                    logger.debug(f"Failed {prog_id}: {e}")
                    continue
        
        if not self._login:
            raise SageSDKError(
                "Could not create Sage SDK object. "
                "Make sure Sage 50 is installed and closed."
            )
        
        # Connect to company data
        data_path = self.config.sage50_company_path
        if not data_path:
            raise SageSDKError("SAGE_COMPANY_PATH not configured")
        
        try:
            if self._api_type == "peachtree":
                return self._connect_peachtree(data_path)
            else:
                return self._connect_sdo(data_path)
        except Exception as e:
            raise SageSDKError(f"Failed to connect to Sage: {e}")
    
    def _connect_peachtree(self, data_path: str) -> bool:
        """Connect using Peachtree API (US)."""
        try:
            logger.info(f"Opening Peachtree company: {data_path}")
            
            # Per Sage 50 SDK docs, the flow is:
            # 1. Login.GetApplication(COM_Username, COM_Password) - third-party access creds
            # 2. App.OpenCompany(path) or App.OpenCompanySecure(path, user, pass)
            
            # COM/Third-party credentials (from "Access From Outside Sage 50" setting)
            com_username = self.config.sage50_username or "Peachtree"
            com_password = self.config.sage50_password or ""
            
            # Try 0: Connect to ALREADY RUNNING Sage 50 instance (skip auth if user is logged in)
            running_prog_ids = [
                # Sage 50 names (current branding)
                "Sage.Application",
                "Sage50.Application",
                "Sage50Accounting.Application",
                "Sage 50.Application",
                "Sage.50.Application",
                # Peachtree names (legacy, but still used internally)
                "Peachtree.Application",
                "PeachtreeAccounting.Application",
                "PeachtreeAccounting.Application.31",
                "PeachtreeAccounting.Application.30",
            ]
            
            for prog_id in running_prog_ids:
                try:
                    logger.debug(f"Trying to connect to running instance: {prog_id}")
                    import win32com.client
                    
                    # GetActiveObject connects to an existing running COM instance
                    app = win32com.client.GetActiveObject(prog_id)
                    
                    if app:
                        app_type = str(type(app))
                        logger.info(f"Connected to running Sage 50 via {prog_id}! Type: {app_type}")
                        
                        # Check if it has the methods we need
                        if hasattr(app, 'Customers') or hasattr(app, 'SalesOrders'):
                            self._company = app
                            self._connected = True
                            logger.info("Successfully attached to running Sage 50!")
                            return True
                        else:
                            logger.debug("Running instance doesn't have expected methods")
                            
                except Exception as e0:
                    logger.debug(f"Could not connect to {prog_id}: {e0}")
            
            # Try 1: Standard GetApplication approach
            try:
                logger.debug(f"Step 1: GetApplication with COM credentials (user={com_username})...")
                app = self._login.GetApplication(com_username, com_password)
                
                # Verify we got an Application object, not None or Login
                if app is None:
                    raise Exception("GetApplication returned None")
                
                app_type = str(type(app))
                logger.info(f"Got object of type: {app_type}")
                
                # Check if we actually got an Application (should have OpenCompany)
                if 'ILogin' in app_type:
                    logger.warning("GetApplication returned Login object instead of Application!")
                    raise Exception("GetApplication returned Login instead of Application")
                
                methods = [m for m in dir(app) if not m.startswith('_')]
                logger.debug(f"App methods: {methods[:20]}...")
                
                # Verify this is an Application by checking for expected methods
                if not hasattr(app, 'OpenCompany') and not hasattr(app, 'Customers'):
                    logger.warning(f"Object missing expected Application methods")
                    raise Exception("Object doesn't appear to be an Application")
                
                # Step 2: Open the company
                logger.debug(f"Step 2: Opening company at {data_path}...")
                
                # Check if company uses passwords
                if hasattr(app, 'CheckCompanyUsesPasswords'):
                    try:
                        uses_passwords = app.CheckCompanyUsesPasswords(data_path)
                        logger.debug(f"Company uses passwords: {uses_passwords}")
                        
                        if uses_passwords:
                            # Need company-level credentials
                            # Try with the same credentials or empty
                            logger.debug("Trying OpenCompanySecure...")
                            app.OpenCompanySecure(data_path, com_username, com_password)
                        else:
                            logger.debug("Trying OpenCompany (no auth needed)...")
                            app.OpenCompany(data_path)
                    except Exception as e_check:
                        logger.debug(f"CheckCompanyUsesPasswords failed: {e_check}, trying OpenCompany...")
                        app.OpenCompany(data_path)
                elif hasattr(app, 'OpenCompany'):
                    logger.debug("Trying OpenCompany...")
                    app.OpenCompany(data_path)
                else:
                    logger.debug("No OpenCompany method, assuming already connected")
                
                self._company = app
                self._connected = True
                logger.info(f"Connected and company opened: {data_path}")
                return True
                
            except Exception as e1:
                logger.error(f"SDK connection failed: {e1}")
                logger.debug(f"Full error: {e1}")
            
            # Try 2: EnsureDispatch for early binding with GetApplication
            try:
                logger.debug("Trying EnsureDispatch (early binding)...")
                login = win32com.client.gencache.EnsureDispatch("PeachtreeAccounting.Login.31")
                methods = [m for m in dir(login) if not m.startswith('_')]
                logger.debug(f"EnsureDispatch Login methods: {methods[:15]}...")
                
                # Must use GetApplication to get the Application object
                if hasattr(login, 'GetApplication'):
                    logger.debug(f"Calling GetApplication with ({com_username}, ***)")
                    app = login.GetApplication(com_username, com_password)
                    
                    if app is None:
                        raise Exception("EnsureDispatch GetApplication returned None")
                    
                    app_type = str(type(app))
                    logger.debug(f"EnsureDispatch got: {app_type}")
                    
                    if 'ILogin' in app_type:
                        raise Exception("EnsureDispatch GetApplication returned Login instead of App")
                    
                    # Open company
                    if hasattr(app, 'OpenCompany'):
                        logger.debug(f"Opening company: {data_path}")
                        app.OpenCompany(data_path)
                    
                    self._login = login
                    self._company = app
                    self._connected = True
                    logger.info(f"Connected via EnsureDispatch.GetApplication: {data_path}")
                    return True
                    
            except Exception as e2:
                logger.debug(f"EnsureDispatch failed: {e2}")
            
            # Try 3: Direct late binding with credentials
            try:
                logger.debug("Trying direct late binding with GetApplication...")
                login = win32com.client.Dispatch("PeachtreeAccounting.Login")
                app = login.GetApplication(com_username, com_password)
                
                if app and 'ILogin' not in str(type(app)):
                    if hasattr(app, 'OpenCompany'):
                        app.OpenCompany(data_path)
                    self._login = login
                    self._company = app
                    self._connected = True
                    logger.info(f"Connected via late binding: {data_path}")
                    return True
            except Exception as e3:
                logger.debug(f"Late binding failed: {e3}")
            
            # Try 4: Use LoginSelector.GetLogin()
            try:
                logger.debug("Trying LoginSelector.GetLogin approach...")
                selector = win32com.client.Dispatch("PeachtreeAccounting.LoginSelector")
                methods = [m for m in dir(selector) if not m.startswith('_')]
                logger.debug(f"LoginSelector methods: {methods}")
                
                # Try to get login from selector
                login = selector.GetLogin()
                if login:
                    self._login = login
                    # Must pass credentials to GetApplication
                    app = login.GetApplication(com_username, com_password)
                    
                    if app and 'ILogin' not in str(type(app)):
                        if hasattr(app, 'OpenCompany'):
                            app.OpenCompany(data_path)
                        self._company = app
                        self._connected = True
                        logger.info(f"Connected via LoginSelector.GetLogin: {data_path}")
                        return True
            except Exception as e4:
                logger.debug(f"LoginSelector.GetLogin failed: {e4}")
            
            # Try 5: ptWEB12.PeachtreeStorageAdapter
            try:
                logger.debug("Trying PeachtreeStorageAdapter approach...")
                adapter = win32com.client.Dispatch("ptWEB12.PeachtreeStorageAdapter")
                methods = [m for m in dir(adapter) if not m.startswith('_')]
                logger.debug(f"StorageAdapter methods: {methods}")
                self._company = adapter
                self._connected = True
                logger.info(f"Connected via StorageAdapter")
                return True
            except Exception as e5:
                logger.debug(f"StorageAdapter failed: {e5}")
            
            raise SageSDKError(
                f"Could not open Peachtree company. Tried multiple methods.\n"
                f"Path: {data_path}\n"
                f"Make sure:\n"
                f"  1. Sage 50 is completely closed\n"
                f"  2. You've opened this company in Sage at least once\n"
                f"  3. You have permissions to access the folder"
            )
            
        except SageSDKError:
            raise
        except Exception as e:
            logger.error(f"Peachtree connection failed: {e}")
            raise SageSDKError(f"Failed to open company: {e}")
    
    def _connect_sdo(self, data_path: str) -> bool:
        """Connect using SDO API (UK)."""
        try:
            # Create workspace and connect
            self._company = self._login.Workspaces.Add("MNMAgent")
            
            self._company.Connect(
                data_path,
                self.config.sage50_username or "",
                self.config.sage50_password or "",
                "MNMAgent"
            )
            
            self._connected = True
            logger.info(f"Connected to Sage 50 via SDO: {data_path}")
            return True
            
        except Exception as e:
            logger.error(f"SDO connection failed: {e}")
            raise SageSDKError(f"Failed to connect: {e}")
    
    def disconnect(self):
        """
        Disconnect from Sage.
        
        IMPORTANT: We NEVER close the company - the automation is designed
        to run while the user has Sage open. Closing would log them out.
        We just release our reference to the objects.
        """
        try:
            if self._api_type == "peachtree":
                # NEVER call Close() - it logs out the user!
                # Just release our reference
                logger.info("Releasing Sage connection (leaving company open)")
                self._company = None
            else:  # sdo
                if self._company:
                    try:
                        self._company.Disconnect()
                    except:
                        pass
                    self._company = None
            
            self._login = None
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
        - Items not fully shipped
        """
        if not self._connected:
            raise SageSDKError("Not connected to Sage")
        
        if self._api_type == "peachtree":
            return self._get_unshipped_orders_peachtree()
        else:
            return self._get_unshipped_orders_sdo()
    
    def _get_unshipped_orders_peachtree(self) -> list[Order]:
        """Get unshipped orders using Peachtree API (US)."""
        orders = []
        
        try:
            sales_orders = self._company.SalesOrders
            
            for so in sales_orders:
                try:
                    # Check if order is not fully shipped
                    # In Peachtree, we check if any line has unshipped quantity
                    is_unshipped = False
                    
                    for line in so.Lines:
                        qty_ordered = getattr(line, 'Quantity', 0) or 0
                        qty_shipped = getattr(line, 'QuantityShipped', 0) or 0
                        if qty_ordered > qty_shipped:
                            is_unshipped = True
                            break
                    
                    if is_unshipped:
                        order = self._parse_sales_order_peachtree(so)
                        orders.append(order)
                        
                except Exception as e:
                    logger.debug(f"Error parsing order: {e}")
                    continue
            
            logger.info(f"Found {len(orders)} unshipped orders via Peachtree")
            return orders
            
        except Exception as e:
            logger.error(f"Error reading orders: {e}")
            raise SageSDKError(f"Failed to read orders: {e}")
    
    def _get_unshipped_orders_sdo(self) -> list[Order]:
        """Get unshipped orders using SDO API (UK)."""
        orders = []
        
        try:
            # Get SalesOrder record set
            sales_orders = self._company.CreateObject("SalesOrder")
            
            # Find all orders
            sales_orders.FindFirst()
            
            while not sales_orders.EOF:
                # Check if order is not complete
                status = getattr(sales_orders, 'OrderStatus', 0)
                despatch_status = getattr(sales_orders, 'DespatchStatus', 0)
                
                # Status 2 = Complete, DespatchStatus 2 = Fully Despatched
                if status != 2 and despatch_status != 2:
                    order = self._parse_sales_order_sdo(sales_orders)
                    orders.append(order)
                
                sales_orders.FindNext()
            
            logger.info(f"Found {len(orders)} unshipped orders via SDO")
            return orders
            
        except Exception as e:
            logger.error(f"Error reading orders: {e}")
            raise SageSDKError(f"Failed to read orders: {e}")
    
    def _parse_sales_order_peachtree(self, record) -> Order:
        """Parse a Peachtree SalesOrder into Order model."""
        order = Order(
            sage_order_ref=str(getattr(record, 'ReferenceNumber', '') or ''),
            order_date=getattr(record, 'Date', None) or datetime.now(),
            customer_name=getattr(record, 'ShipToName', '') or '',
            
            ship_name=getattr(record, 'ShipToName', '') or '',
            ship_address_1=getattr(record, 'ShipToAddress1', '') or '',
            ship_address_2=getattr(record, 'ShipToAddress2', '') or '',
            ship_city=getattr(record, 'ShipToCity', '') or '',
            ship_state=getattr(record, 'ShipToState', '') or '',
            ship_postcode=getattr(record, 'ShipToZip', '') or '',
            
            source_platform=Platform.SAGE_QUANTUM,
        )
        
        # Calculate totals and get lines
        total = 0.0
        try:
            for line_item in record.Lines:
                qty = float(getattr(line_item, 'Quantity', 0) or 0)
                price = float(getattr(line_item, 'UnitPrice', 0) or 0)
                total += qty * price
                
                line = OrderLine(
                    sku=getattr(line_item, 'ItemID', '') or '',
                    description=getattr(line_item, 'Description', '') or '',
                    quantity=int(qty),
                    unit_price=price,
                )
                order.lines.append(line)
        except Exception:
            pass
        
        order.total = total
        return order
    
    def _parse_sales_order_sdo(self, record) -> Order:
        """Parse a SDO SalesOrder record into Order model."""
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
        
        if self._api_type == "peachtree":
            return self._create_sales_order_peachtree(order)
        else:
            return self._create_sales_order_sdo(order)
    
    def _create_sales_order_peachtree(self, order: Order) -> dict:
        """
        Create sales order using Peachtree API (US) via XML Import.
        
        Based on the Sage 50 SDK sample code (frmNewCustInvoices.cs):
        1. Create XML file with invoice data
        2. Use CreateImporter() to import
        3. SDK validates and creates the invoice
        """
        try:
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            platform = str(order.source_platform).replace("Platform.", "")
            
            # Customer ID is ALWAYS just the platform name: Amazon, eBay, or Shopify
            # The actual buyer name goes in Ship To Name on the invoice
            # All orders for a platform accumulate under that single customer
            if order.amazon_order_id:
                customer_id = "Amazon"
            elif order.ebay_order_id:
                customer_id = "eBay"
            elif order.shopify_order_id:
                customer_id = "Shopify"
            else:
                customer_id = platform  # Fallback to platform name
            
            logger.debug(f"Using customer ID: {customer_id} (Ship To: {order.customer_name})")
            
            # Generate invoice number from platform order ID
            invoice_number = platform_id[:20] if platform_id else f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Use Item IDs in invoices (items must exist in Sage inventory)
            # Default to True since inventory is imported
            use_item_ids = getattr(self.config, 'sage_use_item_ids', True)
            if use_item_ids:
                logger.debug("Using Item IDs in invoice (items must exist in Sage)")
            else:
                logger.debug("Simple mode: Using description + GL account (no Item IDs)")
            
            # Create XML file for import
            xml_path = self._create_invoice_xml(order, customer_id, invoice_number, use_item_ids)
            
            # Import using SDK
            result = self._import_sales_journal(xml_path)
            
            # Clean up temp file
            try:
                Path(xml_path).unlink()
            except Exception:
                pass
            
            if result["success"]:
                logger.info(f"Created Sage invoice {invoice_number} for {platform_id}")
                return {
                    "success": True,
                    "sage_order_ref": invoice_number,
                    "platform_order_id": platform_id,
                    "message": "Order imported to Sage via SDK",
                }
            else:
                raise SageSDKError(result.get("error", "Import failed"))
            
        except Exception as e:
            logger.error(f"Failed to create order (Peachtree): {e}")
            raise SageSDKError(f"Failed to create order: {e}")
    
    # NOTE: No customer creation/modification logic. Customers must be pre-created:
    # - Amazon (ID: Amazon, Name: Amazon)
    # - eBay (ID: eBay, Name: eBay)  
    # - Shopify (ID: Shopify, Name: Shopify)
    # All orders just add up under these platform customers. The actual buyer name
    # goes in the "Ship To Name" field on each invoice, not as a customer record.
    
    def _ensure_item_exists(self, item_id: str, description: str = "") -> str:
        """
        Check if item exists in Sage, create if not.
        Returns the item ID.
        """
        if not item_id or item_id in ("ITEM", "UNKNOWN"):
            return item_id
        
        # Check if item exists
        try:
            if self._item_exists(item_id):
                logger.debug(f"Item {item_id} exists")
                return item_id
        except Exception as e:
            logger.debug(f"Could not check if item exists: {e}")
        
        # Item doesn't exist - create it
        logger.info(f"Item {item_id} not found, creating...")
        try:
            self._create_item(item_id, description)
            logger.info(f"Created item: {item_id}")
        except Exception as e:
            logger.warning(f"Could not create item {item_id}: {e}")
        
        return item_id
    
    def _item_exists(self, item_id: str) -> bool:
        """Check if an item exists in Sage. Returns False to always try creation."""
        # Skip check - just try to create. If item exists, import will handle it.
        # This avoids complex exporter setup issues.
        return False
    
    def _create_item(self, item_id: str, description: str = ""):
        """Create a new item in Sage via XML import."""
        if not HAS_PYTHONNET or not self._company:
            raise SageSDKError("pythonnet not available for item creation")
        
        from Interop.PeachwServer import PeachwIEObj, PeachwIEFileType
        
        # Use correct enum: peachwIEObjInventoryItemsList
        
        # Create item XML
        root = ET.Element("PAW_ItemList")
        root.set("xmlns:paw", "urn:schemas-peachtree-com/paw8.02-datatypes")
        root.set("xmlns:xsi", "http://www.w3.org/2000/10/XMLSchema-instance")
        
        item = ET.SubElement(root, "PAW_Item")
        
        # Item ID
        id_elem = ET.SubElement(item, "ItemID")
        id_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        id_elem.text = item_id[:20]
        
        # Description
        ET.SubElement(item, "Description").text = (description or item_id)[:160]
        
        # Item Class - Non-stock (service item that doesn't track inventory)
        ET.SubElement(item, "ItemClass").text = "Non-stock"
        
        # Default price (0 - will use price from invoice)
        ET.SubElement(item, "SalesPrice1").text = "0.00"
        
        # GL Account for sales
        sales_account_id = getattr(self.config, 'sage_sales_account', None) or "4100"
        gl_acct = ET.SubElement(item, "GLSalesAccount")
        gl_acct.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        gl_acct.text = sales_account_id
        
        # Write to temp file
        temp_path = Path(tempfile.gettempdir()) / f"item_{item_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xml"
        tree = ET.ElementTree(root)
        tree.write(str(temp_path), encoding="utf-8", xml_declaration=True)
        
        try:
            # Import item using correct enum - wrap with Import class like customer import
            from Interop.PeachwServer import Import
            importer = Import(self._company.CreateImporter(PeachwIEObj.peachwIEObjInventoryItemsList))
            importer.SetFilename(str(temp_path))
            importer.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
            importer.Import()
            
            logger.debug(f"Item {item_id} created successfully")
            
        finally:
            try:
                temp_path.unlink()
            except:
                pass
    
    def _create_invoice_xml(self, order: Order, customer_id: str, invoice_number: str, use_item_ids: bool = False) -> str:
        """
        Create XML file in Peachtree format for import.
        
        Args:
            order: Order data
            customer_id: Sage customer ID
            invoice_number: Invoice number
            use_item_ids: If True (production mode), include Item_ID in lines.
                         If False (simple mode), use description + GL account only.
        
        Format based on Sage SDK sample code.
        Note: Amounts are NEGATIVE for sales (credits to income accounts).
        """
        # Calculate totals
        subtotal = sum(line.quantity * line.unit_price for line in order.lines)
        if order.shipping_cost > 0:
            subtotal += order.shipping_cost
        total = subtotal  # Add tax if needed
        
        # Create XML structure
        root = ET.Element("PAW_Invoices")
        root.set("xmlns:paw", "urn:schemas-peachtree-com/paw8.02-datatypes")
        root.set("xmlns:xsi", "http://www.w3.org/2000/10/XMLSchema-instance")
        root.set("xmlns:xsd", "http://www.w3.org/2000/10/XMLSchema-datatypes")
        
        # Create invoice element
        invoice = ET.SubElement(root, "PAW_Invoice")
        invoice.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:receipt")
        
        # Customer ID
        cust_id_elem = ET.SubElement(invoice, "Customer_ID")
        cust_id_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        cust_id_elem.text = customer_id
        
        # Customer Name (the platform name)
        ET.SubElement(invoice, "Customer_Name").text = customer_id
        
        # Date
        date_elem = ET.SubElement(invoice, "Date")
        date_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:date")
        date_elem.text = order.order_date.strftime("%m/%d/%Y") if order.order_date else datetime.now().strftime("%m/%d/%Y")
        
        # Invoice Number
        ET.SubElement(invoice, "Invoice_Number").text = invoice_number
        
        # Ship To - Name first, then address (matching Sage's field order)
        ship_to_name = (order.customer_name or order.ship_name or "")[:40]
        logger.info(f"Setting Ship To Name: '{ship_to_name}'")
        ET.SubElement(invoice, "Name").text = ship_to_name  # Simple "Name" element
        ET.SubElement(invoice, "Line1").text = (order.ship_address_1 or "")[:40]
        ET.SubElement(invoice, "Line2").text = (order.ship_address_2 or "")[:40]
        ET.SubElement(invoice, "City").text = (order.ship_city or "")[:25]
        ET.SubElement(invoice, "State").text = (order.ship_state or "")[:2]
        ET.SubElement(invoice, "Zip").text = (order.ship_postcode or "")[:12]
        
        # AR Account - configurable, default 1100
        ar_account_id = getattr(self.config, 'sage_ar_account', None) or "1100"
        ar_acct = ET.SubElement(invoice, "Accounts_Receivable_Account")
        ar_acct.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        ar_acct.text = ar_account_id
        
        # AR Amount (total)
        ET.SubElement(invoice, "Accounts_Receivable_Amount").text = f"{total:.2f}"
        
        # Credit Memo Type
        ET.SubElement(invoice, "CreditMemoType").text = "FALSE"
        
        # Number of distributions (line items)
        num_distributions = len(order.lines)
        if order.shipping_cost > 0:
            num_distributions += 1
        ET.SubElement(invoice, "Number_of_Distributions").text = str(num_distributions)
        
        # Sales Lines
        sales_lines = ET.SubElement(invoice, "SalesLines")
        
        # GL Account for sales - configurable, default 4100
        sales_account_id = getattr(self.config, 'sage_sales_account', None) or "4100"
        logger.info(f"Using GL accounts - AR: {ar_account_id}, Sales: {sales_account_id}")
        logger.info(f"Order has {len(order.lines)} line items")
        
        for line in order.lines:
            logger.debug(f"  Line: qty={line.quantity}, price={line.unit_price}, sku={line.sku}")
            sales_line = ET.SubElement(sales_lines, "SalesLine")
            
            ET.SubElement(sales_line, "Quantity").text = str(line.quantity)
            
            if use_item_ids:
                # Production mode: Use actual Item ID (items must exist in Sage)
                item_id = (line.sku or "ITEM")[:20]
                logger.info(f"  Adding Item_ID: '{item_id}' to invoice")
                
                item_id_elem = ET.SubElement(sales_line, "Item_ID")
                item_id_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
                item_id_elem.text = item_id
                
                ET.SubElement(sales_line, "Description").text = (line.description or "")[:160]
            else:
                # Simple mode: Include SKU in description (no Item_ID lookup)
                desc_with_sku = f"{line.sku}: {line.description}" if line.sku else line.description
                ET.SubElement(sales_line, "Description").text = (desc_with_sku or "Sale")[:160]
                logger.debug(f"  Simple mode - no Item_ID, desc: {desc_with_sku[:50]}")
            
            # GL Account for sales
            gl_acct = ET.SubElement(sales_line, "GL_Account")
            gl_acct.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
            gl_acct.text = sales_account_id
            
            # Unit Price (NEGATIVE for sales)
            ET.SubElement(sales_line, "Unit_Price").text = f"{-line.unit_price:.2f}"
            
            # Tax Type: 1 = Non-taxable (per spec)
            ET.SubElement(sales_line, "Tax_Type").text = "1"
            
            # Amount (NEGATIVE for sales)
            line_amount = line.quantity * line.unit_price
            ET.SubElement(sales_line, "Amount").text = f"{-line_amount:.2f}"
        
        # Add shipping line if present
        if order.shipping_cost > 0:
            ship_line = ET.SubElement(sales_lines, "SalesLine")
            
            ET.SubElement(ship_line, "Quantity").text = "1"
            
            ship_item = ET.SubElement(ship_line, "Item_ID")
            ship_item.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
            ship_item.text = "SHIPPING"
            
            ET.SubElement(ship_line, "Description").text = "Shipping & Handling"
            
            ship_gl = ET.SubElement(ship_line, "GL_Account")
            ship_gl.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
            ship_gl.text = sales_account_id  # Use same sales account for shipping
            
            ET.SubElement(ship_line, "Unit_Price").text = f"{-order.shipping_cost:.2f}"
            ET.SubElement(ship_line, "Tax_Type").text = "1"  # Non-taxable per spec
            ET.SubElement(ship_line, "Sales_Tax_ID").text = "exempt"  # Per spec
            ET.SubElement(ship_line, "Amount").text = f"{-order.shipping_cost:.2f}"
        
        # Write to temp file
        temp_dir = tempfile.gettempdir()
        xml_path = Path(temp_dir) / f"sage_import_{datetime.now().strftime('%Y%m%d%H%M%S')}.xml"
        
        tree = ET.ElementTree(root)
        tree.write(str(xml_path), encoding="utf-8", xml_declaration=True)
        
        # Log the XML content for debugging
        with open(xml_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        logger.debug(f"Created import XML: {xml_path}")
        logger.debug(f"XML content:\n{xml_content}")
        return str(xml_path)
    
    def _import_sales_journal(self, xml_path: str) -> dict:
        """
        Import sales journal entries from XML file using SDK.
        
        Uses CreateImporter() with PeachwIEObj.peachwIEObjSalesJournal.
        """
        if not self._connected or not self._company:
            return {"success": False, "error": "Not connected to Sage"}
        
        try:
            # Import the enums from PeachwServer
            if HAS_PYTHONNET:
                import clr
                from Interop.PeachwServer import (
                    PeachwIEObj, 
                    PeachwIEObjSalesJournalField,
                    PeachwIEFileType,
                    Import
                )
                
                # Create importer for Sales Journal
                importer_obj = self._company.CreateImporter(PeachwIEObj.peachwIEObjSalesJournal)
                importer = Import(importer_obj) if importer_obj else None
                
                if not importer:
                    return {"success": False, "error": "Failed to create importer"}
                
                # Clear and set up field list
                importer.ClearImportFieldList()
                
                # Add fields to import (from C# sample)
                fields = [
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_CustomerId,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_CustomerName,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_Date,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_InvoiceNumber,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ShipToName,  # Ship To Name
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ShipToAddressLine1,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ShipToAddressLine2,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ShipToCity,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ShipToState,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ShipToZip,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ARAccountId,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ARAmount,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_IsCreditMemo,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_NumberOfDistributions,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_Quantity,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_ItemId,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_Description,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_GLAccountId,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_UnitPrice,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_TaxType,
                    PeachwIEObjSalesJournalField.peachwIEObjSalesJournalField_Amount,
                ]
                
                for field in fields:
                    importer.AddToImportFieldList(int(field))
                
                # Set file info
                importer.SetFilename(xml_path)
                importer.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
                
                # Perform import
                importer.Import()
                
                logger.info(f"Successfully imported from {xml_path}")
                return {"success": True}
                
            else:
                # Fallback using win32com
                # Create importer - PeachwIEObj.peachwIEObjSalesJournal = 0
                importer = self._company.CreateImporter(0)  # 0 = SalesJournal
                
                importer.ClearImportFieldList()
                
                # Field IDs from Sage SDK documentation
                sales_journal_fields = [
                    0,   # CustomerId
                    1,   # CustomerName
                    2,   # Date
                    3,   # InvoiceNumber
                    4,   # ShipToAddressLine1
                    5,   # ShipToAddressLine2
                    6,   # ShipToCity
                    7,   # ShipToState
                    8,   # ShipToZip
                    9,   # ARAccountId
                    10,  # ARAmount
                    11,  # IsCreditMemo
                    12,  # NumberOfDistributions
                    13,  # Quantity
                    14,  # ItemId
                    15,  # Description
                    16,  # GLAccountId
                    17,  # UnitPrice
                    18,  # TaxType
                    19,  # Amount
                ]
                
                for field_id in sales_journal_fields:
                    importer.AddToImportFieldList(field_id)
                
                # Set file info - peachwIEFileTypeXML = 1
                importer.SetFilename(xml_path)
                importer.SetFileType(1)
                
                # Import
                importer.Import()
                
                return {"success": True}
                
        except Exception as e:
            logger.error(f"Import failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _create_sales_order_sdo(self, order: Order) -> dict:
        """Create sales order using SDO API (UK)."""
        try:
            # Create new SalesOrder record
            sales_order = self._company.CreateObject("SalesOrder")
            sales_order.AddNew()
            
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            platform = str(order.source_platform).replace("Platform.", "")
            
            # Customer account is just the platform name (Amazon, eBay, Shopify)
            if order.amazon_order_id:
                account_ref = "Amazon"
            elif order.ebay_order_id:
                account_ref = "eBay"
            elif order.shopify_order_id:
                account_ref = "Shopify"
            else:
                account_ref = platform
            
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
                "message": "Order created automatically in Sage (SDO)",
            }
            
        except Exception as e:
            logger.error(f"Failed to create order (SDO): {e}")
            raise SageSDKError(f"Failed to create order: {e}")
    
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
            "sdk_available": HAS_COM or HAS_PYTHONNET,
        }
        
        if not HAS_COM and not HAS_PYTHONNET:
            result["message"] = "Neither pythonnet nor pywin32 installed"
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
    
    def test_import(self) -> dict:
        """
        Test the import functionality by creating a sample invoice.
        
        This creates a test invoice with one line item to verify
        the SDK import is working correctly.
        """
        if not self._connected:
            return {"success": False, "error": "Not connected to Sage"}
        
        try:
            # Create a simple test order
            from agent.models import Order, OrderLine, Platform
            
            test_order = Order(
                order_date=datetime.now(),
                customer_name="SDK Test Customer",
                ship_name="SDK Test Customer",
                ship_address_1="123 Test Street",
                ship_city="Test City",
                ship_state="TX",
                ship_postcode="12345",
                source_platform=Platform.SAGE_QUANTUM,
                amazon_order_id=f"TEST-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            )
            
            # Add test line item
            test_order.lines.append(OrderLine(
                sku="TEST-ITEM",
                description="SDK Import Test Item",
                quantity=1,
                unit_price=10.00,
            ))
            
            # Try to import
            result = self._create_sales_order_peachtree(test_order)
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def import_orders_from_excel(self, excel_path: str) -> dict:
        """
        Import orders from an Excel file (Amazon/eBay/Shopify format).
        
        Expected columns:
        - Date of Order, E-Commerce Order#, Sales Order#, Ship Date
        - Amount, Qty, Unit of Measure, Unit Price, Item ID
        - Customer ID, Ship to Name, Address Line 1, Address Line 2
        - City, State, Zipcode, Receivable amount
        - # of Line Items Ordered, GL Amount, Tax Type, Description
        
        Args:
            excel_path: Path to the Excel file
            
        Returns:
            Dict with success count, failed count, and details
        """
        if not self._connected:
            return {"success": False, "error": "Not connected to Sage"}
        
        try:
            import pandas as pd
            
            # Read Excel file
            df = pd.read_excel(excel_path)
            logger.info(f"Read {len(df)} rows from {excel_path}")
            logger.debug(f"Columns: {list(df.columns)}")
            
            results = {
                "success": True,
                "total_rows": len(df),
                "imported": 0,
                "failed": 0,
                "errors": [],
            }
            
            # Group by order number (E-Commerce Order# or Sales Order#)
            order_col = None
            for col in ['E-Commerce Order#', 'Sales Order#', 'Order Number', 'Order#']:
                if col in df.columns:
                    order_col = col
                    break
            
            if not order_col:
                # Treat each row as a separate order
                order_col = 'row_index'
                df['row_index'] = range(len(df))
            
            logger.info(f"Using order column: {order_col}")
            logger.debug(f"Order column values: {df[order_col].tolist()}")
            
            # Handle NaN values - replace with row index
            for idx in df.index:
                if pd.isna(df.loc[idx, order_col]):
                    df.loc[idx, order_col] = f"ROW-{idx}"
            
            unique_orders = df[order_col].nunique()
            logger.info(f"Found {unique_orders} unique orders to process")
            
            # Process each unique order
            for order_id, order_rows in df.groupby(order_col):
                logger.info(f"Processing order: {order_id} ({len(order_rows)} rows)")
                try:
                    order = self._parse_excel_order(order_id, order_rows)
                    logger.debug(f"Parsed order: customer={order.customer_name}, lines={len(order.lines)}")
                    result = self._create_sales_order_peachtree(order)
                    
                    if result.get("success"):
                        results["imported"] += 1
                        logger.info(f"Imported order {order_id}")
                    else:
                        results["failed"] += 1
                        results["errors"].append(f"{order_id}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append(f"{order_id}: {str(e)}")
                    logger.error(f"Failed to import order {order_id}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"Excel import failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _parse_excel_order(self, order_id, rows) -> 'Order':
        """Parse Excel rows into an Order object."""
        import pandas as pd
        from agent.models import Order, OrderLine, Platform
        
        # Get first row for header info
        first_row = rows.iloc[0]
        columns = list(rows.columns)
        
        # Helper to find column by partial match (case-insensitive)
        def find_col(search_terms):
            for term in search_terms:
                for col in columns:
                    if term.lower() in str(col).lower():
                        return col
            return None
        
        # Helper to get value safely
        def get_val(row, col, default=''):
            if col and col in row.index:
                val = row[col]
                if pd.notna(val):
                    return str(val).strip()
            return default
        
        # Find column mappings (supports both original format and API format)
        date_col = find_col(['Date of Order', 'Order Date', 'Date'])
        cust_col = find_col(['Customer ID', 'CustomerID', 'Cust ID', 'Platform'])  # API uses 'Platform'
        name_col = find_col(['Ship to Name', 'ShipToName', 'Customer Name', 'Name'])
        addr1_col = find_col(['Address Line 1', 'AddressLine1', 'Address1', 'Address', 'Ship To Address'])
        addr2_col = find_col(['Address Line 2', 'AddressLine2', 'Address2'])
        city_col = find_col(['City'])
        state_col = find_col(['State'])
        zip_col = find_col(['Zipcode', 'Zip', 'ZipCode', 'Postal'])
        qty_col = find_col(['Qty', 'Quantity'])
        price_col = find_col(['Unit Price', 'UnitPrice', 'Price'])
        item_col = find_col(['Item ID', 'ItemID', 'SKU', 'Item', 'Product'])  # SKU before Item
        desc_col = find_col(['Description', 'Desc', 'Item Description'])
        amount_col = find_col(['Amount', 'Receivable amount', 'Total', 'Receivable Amount'])
        phone_col = find_col(['Customer Phone #', 'Customer Phone', 'Phone', 'Phone #', 'Telephone'])
        platform_order_col = find_col(['Platform Order ID', 'Platform Order'])  # API's order ID column
        
        logger.info(f"Column mappings: Qty='{qty_col}', Price='{price_col}', Item='{item_col}', Desc='{desc_col}'")
        logger.info(f"Column mappings: Customer ID='{cust_col}', Name='{name_col}', Amount='{amount_col}'")
        
        # Parse date
        order_date = datetime.now()
        if date_col:
            try:
                order_date = pd.to_datetime(first_row[date_col])
            except:
                pass
        
        # Get platform from Customer ID column (Amazon/Shopify/eBay)
        # This becomes the Customer ID in Sage (one customer per platform)
        platform = get_val(first_row, cust_col) if cust_col else 'Amazon'
        if platform and platform.endswith('.0'):
            platform = platform[:-2]
        if not platform:
            platform = 'Amazon'
        
        # Determine platform from the Customer ID column (Amazon, eBay, Shopify)
        platform_lower = platform.lower() if platform else 'amazon'
        if 'ebay' in platform_lower:
            source_platform = Platform.EBAY
        elif 'shopify' in platform_lower:
            source_platform = Platform.SHOPIFY
        else:
            source_platform = Platform.AMAZON
        
        # Get customer name (this goes in Ship To Name field on the invoice)
        customer_name = get_val(first_row, name_col)[:40] if name_col else ''
        
        logger.info(f"Order {order_id}: Platform={source_platform}, Ship To Name='{customer_name}'")
        
        # Get phone number
        customer_phone = get_val(first_row, phone_col)[:20] if phone_col else ''
        
        # Create order
        order = Order(
            order_date=order_date,
            customer_name=customer_name,
            customer_phone=customer_phone,
            ship_name=customer_name,
            ship_address_1=get_val(first_row, addr1_col)[:40] if addr1_col else '',
            ship_address_2=get_val(first_row, addr2_col)[:40] if addr2_col else '',
            ship_city=get_val(first_row, city_col)[:25] if city_col else '',
            ship_state=get_val(first_row, state_col)[:2] if state_col else '',
            ship_postcode=get_val(first_row, zip_col)[:12] if zip_col else '',
            source_platform=source_platform,
        )
        
        # Set platform order ID based on which platform this order came from
        ecom_order = str(order_id) if order_id else None
        if ecom_order:
            if source_platform == Platform.EBAY:
                order.ebay_order_id = ecom_order
            elif source_platform == Platform.SHOPIFY:
                order.shopify_order_id = ecom_order
            else:
                order.amazon_order_id = ecom_order
        
        # Parse line items
        logger.debug(f"Parsing {len(rows)} rows for line items")
        for idx, row in rows.iterrows():
            logger.debug(f"  Row {idx}: Qty={row.get(qty_col) if qty_col else 'N/A'}, Price={row.get(price_col) if price_col else 'N/A'}, Item={row.get(item_col) if item_col else 'N/A'}")
            qty = 1
            if qty_col:
                try:
                    qty = int(float(row[qty_col])) if pd.notna(row[qty_col]) else 1
                except:
                    qty = 1
            
            unit_price = 0.0
            if price_col:
                try:
                    unit_price = float(row[price_col]) if pd.notna(row[price_col]) else 0.0
                except:
                    unit_price = 0.0
            
            item_id = ""
            if item_col:
                try:
                    val = row[item_col]
                    logger.debug(f"    Raw Item ID value: '{val}' (type: {type(val).__name__})")
                    if pd.notna(val):
                        item_id = str(val).strip()[:20]
                        # Clean up float-like IDs
                        if item_id.endswith('.0'):
                            item_id = item_id[:-2]
                except Exception as e:
                    logger.debug(f"    Item ID read error: {e}")
            
            description = ""
            if desc_col:
                try:
                    val = row[desc_col]
                    if pd.notna(val):
                        description = str(val).strip()[:160]
                except:
                    pass
            
            logger.debug(f"  Parsed line: item={item_id}, qty={qty}, price={unit_price}, desc={description[:30] if description else 'N/A'}")
            
            # Add line if we have meaningful data (qty > 0 and either price, item, or description)
            if qty > 0 and (unit_price > 0 or item_id or description):
                order.lines.append(OrderLine(
                    sku=item_id if item_id else "ITEM",
                    description=description if description else f"Order {order_id} item",
                    quantity=qty,
                    unit_price=unit_price,
                ))
                logger.debug(f"    -> Added line item")
        
        # Set total from Amount column
        if amount_col:
            try:
                order.total = float(first_row[amount_col]) if pd.notna(first_row[amount_col]) else 0.0
            except:
                pass
        
        # Fallback: if no line items but we have Amount, create a single line
        if len(order.lines) == 0 and order.total > 0:
            logger.info(f"No line items parsed, creating fallback line from Amount: {order.total}")
            order.lines.append(OrderLine(
                sku="SALE",
                description=f"Order {order_id}",
                quantity=1,
                unit_price=order.total,
            ))
        
        logger.info(f"Order {order_id}: {len(order.lines)} lines, platform={order.source_platform}, total={order.total}")
        
        return order

