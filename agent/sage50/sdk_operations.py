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
        
        # Open company if path provided
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
        """Disconnect from Sage."""
        try:
            if self._api_type == "peachtree":
                if self._company:
                    try:
                        self._company.Close()
                    except:
                        pass
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
        """Create sales order using Peachtree API (US)."""
        try:
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            platform = str(order.source_platform).replace("Platform.", "")
            
            # Get or create customer
            customer_id = self._get_or_create_customer_peachtree(order)
            
            # Create sales order
            sales_orders = self._company.SalesOrders
            new_order = sales_orders.Add()
            
            # Set customer
            new_order.CustomerID = customer_id
            
            # Set addresses
            new_order.ShipToName = order.customer_name[:40] if order.customer_name else ""
            new_order.ShipToAddress1 = (order.ship_address_1 or "")[:40]
            new_order.ShipToAddress2 = (order.ship_address_2 or "")[:40]
            new_order.ShipToCity = (order.ship_city or "")[:25]
            new_order.ShipToState = (order.ship_state or "")[:2]
            new_order.ShipToZip = (order.ship_postcode or "")[:12]
            
            # Set date
            new_order.Date = order.order_date
            
            # Set reference (platform order ID)
            if platform_id:
                new_order.CustomerPurchaseOrder = f"{platform}:{platform_id}"[:20]
            
            # Add line items
            for line in order.lines:
                order_line = new_order.Lines.Add()
                order_line.ItemID = line.sku[:20] if line.sku else ""
                order_line.Description = line.description[:160] if line.description else ""
                order_line.Quantity = line.quantity
                order_line.UnitPrice = line.unit_price
            
            # Add shipping as line item if present
            if order.shipping_cost > 0:
                ship_line = new_order.Lines.Add()
                ship_line.ItemID = "SHIPPING"
                ship_line.Description = "Shipping & Handling"
                ship_line.Quantity = 1
                ship_line.UnitPrice = order.shipping_cost
            
            # Save
            new_order.Save()
            
            # Get order number
            sage_order_ref = str(new_order.ReferenceNumber) if hasattr(new_order, 'ReferenceNumber') else "NEW"
            
            logger.info(f"Created Sage order {sage_order_ref} for {platform_id}")
            
            return {
                "success": True,
                "sage_order_ref": sage_order_ref,
                "platform_order_id": platform_id,
                "message": "Order created automatically in Sage (Peachtree)",
            }
            
        except Exception as e:
            logger.error(f"Failed to create order (Peachtree): {e}")
            raise SageSDKError(f"Failed to create order: {e}")
    
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

