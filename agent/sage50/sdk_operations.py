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
        password = self.config.sage50_password or ""
        
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
            
            # Use customer ID from order if set (e.g., from Excel import)
            # Otherwise generate one
            customer_id = getattr(order, '_sage_customer_id', None)
            if not customer_id:
                name = order.customer_name or "CUSTOMER"
                customer_id = name[:14].upper().replace(" ", "")
                if order.amazon_order_id:
                    customer_id = "AMZ-" + customer_id[:10]
                elif order.ebay_order_id:
                    customer_id = "EBY-" + customer_id[:10]
                elif order.shopify_order_id:
                    customer_id = "SHP-" + customer_id[:10]
            
            # Generate invoice number from platform order ID
            invoice_number = platform_id[:20] if platform_id else f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Ensure customer exists (auto-create if needed)
            customer_id = self._ensure_customer_exists(customer_id, order)
            
            # Create XML file for import
            xml_path = self._create_invoice_xml(order, customer_id, invoice_number)
            
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
    
    def _ensure_customer_exists(self, customer_id: str, order: Order) -> str:
        """
        Check if customer exists in Sage, create if not.
        
        Returns the customer ID (may be modified if auto-created).
        """
        if not customer_id:
            # Generate customer ID from name
            name = order.customer_name or "CUSTOMER"
            customer_id = name[:14].upper().replace(" ", "")
        
        # Try to check if customer exists by attempting an export filter
        # If that fails, we'll try to create the customer
        try:
            if self._customer_exists(customer_id):
                logger.debug(f"Customer {customer_id} exists")
                return customer_id
        except Exception as e:
            logger.debug(f"Could not check if customer exists: {e}")
        
        # Customer doesn't exist - create them
        logger.info(f"Customer {customer_id} not found, creating...")
        try:
            self._create_customer(customer_id, order)
            logger.info(f"Created customer: {customer_id}")
        except Exception as e:
            logger.warning(f"Could not create customer {customer_id}: {e}")
            # Continue anyway - the import might still work if customer exists
        
        return customer_id
    
    def _customer_exists(self, customer_id: str) -> bool:
        """Check if a customer ID exists in Sage."""
        if not HAS_PYTHONNET or not self._company:
            return False  # Assume exists if we can't check
        
        try:
            from Interop.PeachwServer import Export, PeachwIEObj, PeachwIEFileType, PeachwIEObjCustomerListField, PeachwIEFilterOperation
            
            # Create exporter for customer list with filter
            exporter = Export(self._company.CreateExporter(PeachwIEObj.peachwIEObjCustomerList))
            exporter.ClearExportFieldList()
            exporter.AddToExportFieldList(int(PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerId))
            
            # Filter by customer ID
            exporter.SetFilterValue(
                int(PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerId),
                PeachwIEFilterOperation.peachwIEFilterOperationEqual,
                customer_id,
                customer_id
            )
            
            # Export to temp file
            temp_path = Path(tempfile.gettempdir()) / f"cust_check_{customer_id}.xml"
            exporter.SetFilename(str(temp_path))
            exporter.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
            exporter.Export()
            
            # Check if any results
            if temp_path.exists():
                content = temp_path.read_text()
                temp_path.unlink()
                # If the XML contains the customer ID, they exist
                return customer_id in content
            
            return False
            
        except Exception as e:
            logger.debug(f"Customer check failed: {e}")
            return False  # Assume doesn't exist
    
    def _create_customer(self, customer_id: str, order: Order):
        """Create a new customer in Sage via XML import."""
        if not HAS_PYTHONNET or not self._company:
            raise SageSDKError("pythonnet not available for customer creation")
        
        from Interop.PeachwServer import Import, PeachwIEObj, PeachwIEFileType, PeachwIEObjCustomerListField
        
        # Create customer XML
        root = ET.Element("PAW_Customers")
        root.set("xmlns:paw", "urn:schemas-peachtree-com/paw8.02-datatypes")
        root.set("xmlns:xsi", "http://www.w3.org/2000/10/XMLSchema-instance")
        
        customer = ET.SubElement(root, "PAW_Customer")
        
        # Customer ID
        id_elem = ET.SubElement(customer, "ID")
        id_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
        id_elem.text = customer_id
        
        # Customer Name
        ET.SubElement(customer, "Name").text = (order.customer_name or customer_id)[:40]
        
        # Bill To Address
        bill_to = ET.SubElement(customer, "BillToAddress")
        ET.SubElement(bill_to, "Line1").text = (order.ship_address_1 or "")[:40]
        ET.SubElement(bill_to, "Line2").text = (order.ship_address_2 or "")[:40]
        ET.SubElement(bill_to, "City").text = (order.ship_city or "")[:25]
        ET.SubElement(bill_to, "State").text = (order.ship_state or "")[:2]
        ET.SubElement(bill_to, "Zip").text = (order.ship_postcode or "")[:12]
        
        # Ship To Address (same as bill to)
        ship_to = ET.SubElement(customer, "ShipToAddress")
        ET.SubElement(ship_to, "Line1").text = (order.ship_address_1 or "")[:40]
        ET.SubElement(ship_to, "Line2").text = (order.ship_address_2 or "")[:40]
        ET.SubElement(ship_to, "City").text = (order.ship_city or "")[:25]
        ET.SubElement(ship_to, "State").text = (order.ship_state or "")[:2]
        ET.SubElement(ship_to, "Zip").text = (order.ship_postcode or "")[:12]
        
        # Contact info
        if order.customer_email:
            ET.SubElement(customer, "E_Mail").text = order.customer_email[:50]
        if order.customer_phone:
            ET.SubElement(customer, "Telephone1").text = order.customer_phone[:20]
        
        # Write to temp file
        temp_path = Path(tempfile.gettempdir()) / f"customer_{customer_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xml"
        tree = ET.ElementTree(root)
        tree.write(str(temp_path), encoding="utf-8", xml_declaration=True)
        
        try:
            # Import customer
            importer = Import(self._company.CreateImporter(PeachwIEObj.peachwIEObjCustomerList))
            importer.ClearImportFieldList()
            
            # Add fields
            fields = [
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerId,
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerName,
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerBillToAddressLine1,
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerBillToAddressLine2,
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerBillToCity,
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerBillToState,
                PeachwIEObjCustomerListField.peachwIEObjCustomerListField_CustomerBillToZip,
            ]
            
            for field in fields:
                importer.AddToImportFieldList(int(field))
            
            importer.SetFilename(str(temp_path))
            importer.SetFileType(PeachwIEFileType.peachwIEFileTypeXML)
            importer.Import()
            
        finally:
            # Clean up
            try:
                temp_path.unlink()
            except:
                pass
    
    def _create_invoice_xml(self, order: Order, customer_id: str, invoice_number: str) -> str:
        """
        Create XML file in Peachtree format for import.
        
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
        
        # Customer Name
        ET.SubElement(invoice, "Customer_Name").text = (order.customer_name or "")[:40]
        
        # Date
        date_elem = ET.SubElement(invoice, "Date")
        date_elem.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:date")
        date_elem.text = order.order_date.strftime("%m/%d/%Y") if order.order_date else datetime.now().strftime("%m/%d/%Y")
        
        # Invoice Number
        ET.SubElement(invoice, "Invoice_Number").text = invoice_number
        
        # Ship To Address
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
        
        for line in order.lines:
            sales_line = ET.SubElement(sales_lines, "SalesLine")
            
            ET.SubElement(sales_line, "Quantity").text = str(line.quantity)
            
            item_id = ET.SubElement(sales_line, "Item_ID")
            item_id.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
            item_id.text = (line.sku or "ITEM")[:20]
            
            ET.SubElement(sales_line, "Description").text = (line.description or "")[:160]
            
            # GL Account for sales - configurable, default 4100
            sales_account_id = getattr(self.config, 'sage_sales_account', None) or "4100"
            gl_acct = ET.SubElement(sales_line, "GL_Account")
            gl_acct.set("{http://www.w3.org/2000/10/XMLSchema-instance}type", "paw:ID")
            gl_acct.text = sales_account_id
            
            # Unit Price (NEGATIVE for sales)
            ET.SubElement(sales_line, "Unit_Price").text = f"{-line.unit_price:.2f}"
            
            # Tax Type (2 = Taxable, 1 = Non-taxable)
            ET.SubElement(sales_line, "Tax_Type").text = "2"
            
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
            ET.SubElement(ship_line, "Tax_Type").text = "1"  # Shipping usually non-taxable
            ET.SubElement(ship_line, "Amount").text = f"{-order.shipping_cost:.2f}"
        
        # Write to temp file
        temp_dir = tempfile.gettempdir()
        xml_path = Path(temp_dir) / f"sage_import_{datetime.now().strftime('%Y%m%d%H%M%S')}.xml"
        
        tree = ET.ElementTree(root)
        tree.write(str(xml_path), encoding="utf-8", xml_declaration=True)
        
        logger.debug(f"Created import XML: {xml_path}")
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
            
            # Get or create customer account
            account_ref = self._get_or_create_customer_sdo(order)
            
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            platform = str(order.source_platform).replace("Platform.", "")
            
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
    
    def _get_or_create_customer_peachtree(self, order: Order) -> str:
        """Get or create customer using Peachtree API (US)."""
        # Generate customer ID from name
        name = order.customer_name or "CUSTOMER"
        customer_id = name[:14].upper().replace(" ", "")
        
        # Add platform prefix
        if order.amazon_order_id:
            customer_id = "AMZ-" + customer_id[:10]
        elif order.ebay_order_id:
            customer_id = "EBY-" + customer_id[:10]
        elif order.shopify_order_id:
            customer_id = "SHP-" + customer_id[:10]
        
        # Check if customer exists
        try:
            customers = self._company.Customers
            existing = customers.Find(customer_id)
            if existing:
                return customer_id
        except Exception:
            pass
        
        # Create new customer
        try:
            customers = self._company.Customers
            new_customer = customers.Add()
            
            new_customer.ID = customer_id
            new_customer.Name = order.customer_name[:40] if order.customer_name else ""
            new_customer.BillToAddress1 = (order.ship_address_1 or "")[:40]
            new_customer.BillToAddress2 = (order.ship_address_2 or "")[:40]
            new_customer.BillToCity = (order.ship_city or "")[:25]
            new_customer.BillToState = (order.ship_state or "")[:2]
            new_customer.BillToZip = (order.ship_postcode or "")[:12]
            
            if order.customer_email:
                new_customer.Email = order.customer_email[:50]
            if order.customer_phone:
                new_customer.Telephone1 = order.customer_phone[:20]
            
            new_customer.Save()
            logger.info(f"Created customer: {customer_id}")
            
        except Exception as e:
            logger.warning(f"Could not create customer: {e}")
        
        return customer_id
    
    def _get_or_create_customer_sdo(self, order: Order) -> str:
        """Get or create customer using SDO API (UK)."""
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
            customer = self._company.CreateObject("SalesRecord")
            if customer.Find("ACCOUNT_REF", account_ref):
                return account_ref
        except Exception:
            pass
        
        # Create new customer
        try:
            customer = self._company.CreateObject("SalesRecord")
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
            
            # Process each unique order
            for order_id, order_rows in df.groupby(order_col):
                try:
                    order = self._parse_excel_order(order_id, order_rows)
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
        
        # Parse date
        order_date = datetime.now()
        for date_col in ['Date of Order', 'Order Date', 'Date']:
            if date_col in rows.columns and pd.notna(first_row.get(date_col)):
                try:
                    order_date = pd.to_datetime(first_row[date_col])
                except:
                    pass
                break
        
        # Get customer ID
        customer_id = None
        for cust_col in ['Customer ID', 'CustomerID', 'Cust ID']:
            if cust_col in rows.columns and pd.notna(first_row.get(cust_col)):
                customer_id = str(first_row[cust_col])
                break
        
        # Create order
        order = Order(
            order_date=order_date,
            customer_name=str(first_row.get('Ship to Name', ''))[:40] if pd.notna(first_row.get('Ship to Name')) else '',
            ship_name=str(first_row.get('Ship to Name', ''))[:40] if pd.notna(first_row.get('Ship to Name')) else '',
            ship_address_1=str(first_row.get('Address Line 1', ''))[:40] if pd.notna(first_row.get('Address Line 1')) else '',
            ship_address_2=str(first_row.get('Address Line 2', ''))[:40] if pd.notna(first_row.get('Address Line 2')) else '',
            ship_city=str(first_row.get('City', ''))[:25] if pd.notna(first_row.get('City')) else '',
            ship_state=str(first_row.get('State', ''))[:2] if pd.notna(first_row.get('State')) else '',
            ship_postcode=str(first_row.get('Zipcode', first_row.get('Zip', '')))[:12] if pd.notna(first_row.get('Zipcode', first_row.get('Zip'))) else '',
            source_platform=Platform.AMAZON,  # Default, can be detected from order format
        )
        
        # Set platform order ID
        ecom_order = str(order_id) if order_id else None
        if ecom_order:
            order.amazon_order_id = ecom_order  # Use as reference
        
        # Store customer ID for import (will use existing customer)
        order._sage_customer_id = customer_id
        
        # Parse line items
        for _, row in rows.iterrows():
            qty = 1
            for qty_col in ['Qty', 'Quantity', 'QTY']:
                if qty_col in rows.columns and pd.notna(row.get(qty_col)):
                    try:
                        qty = int(float(row[qty_col]))
                    except:
                        qty = 1
                    break
            
            unit_price = 0.0
            for price_col in ['Unit Price', 'UnitPrice', 'Price']:
                if price_col in rows.columns and pd.notna(row.get(price_col)):
                    try:
                        unit_price = float(row[price_col])
                    except:
                        unit_price = 0.0
                    break
            
            item_id = ""
            for item_col in ['Item ID', 'ItemID', 'Item', 'SKU']:
                if item_col in rows.columns and pd.notna(row.get(item_col)):
                    item_id = str(row[item_col])[:20]
                    break
            
            description = ""
            for desc_col in ['Description', 'Desc', 'Item Description']:
                if desc_col in rows.columns and pd.notna(row.get(desc_col)):
                    description = str(row[desc_col])[:160]
                    break
            
            if qty > 0 and (unit_price > 0 or item_id):
                order.lines.append(OrderLine(
                    sku=item_id,
                    description=description,
                    quantity=qty,
                    unit_price=unit_price,
                ))
        
        # Set total from Amount or Receivable amount
        for amt_col in ['Amount', 'Receivable amount', 'Total']:
            if amt_col in rows.columns:
                try:
                    order.total = float(first_row[amt_col])
                except:
                    pass
                break
        
        return order

