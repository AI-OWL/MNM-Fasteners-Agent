r"""
Sage 50 Quantum Connector (UK Version).
Handles connection to Sage 50 Accounts via ODBC or SDO (COM).

Sage 50 Data Locations:
- Default: C:\ProgramData\Sage\Accounts\{YEAR}\
- Company files: ACCDATA folder, or Company.001, etc.
- ODBC DSN: Usually "SageLine50v{XX}" (e.g., SageLine50v29)

Accessing Unshipped Orders:
- Via ODBC: Query SALES_ORDER table where ORDER_STATUS != 'Complete'
- Via SDO: Access SalesOrders collection and filter
- Via Export: Export from Sage UI to CSV, then read file
"""

import os
from typing import Optional, Any
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from loguru import logger

# Windows COM imports
try:
    import win32com.client
    import pythoncom
    HAS_COM = True
except ImportError:
    HAS_COM = False
    logger.warning("pywin32 not available - COM integration disabled")

# ODBC imports
try:
    import pyodbc
    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False
    logger.warning("pyodbc not available - ODBC integration disabled")

from agent.config import AgentConfig


class Sage50Error(Exception):
    """Base exception for Sage 50 errors."""
    pass


class Sage50ConnectionError(Sage50Error):
    """Raised when connection to Sage 50 fails."""
    pass


class Sage50OperationError(Sage50Error):
    """Raised when a Sage 50 operation fails."""
    pass


class Sage50Connector:
    r"""
    Manages connection to Sage 50 Quantum (UK).
    
    Connection Methods:
    1. ODBC - Direct database queries (best for reading data)
    2. SDO (COM) - Sage Data Objects API (best for creating records)
    3. File-based - Import/Export via CSV files
    
    Common Data Locations:
    - C:\ProgramData\Sage\Accounts\2024\Company.001\ACCDATA
    - ODBC DSN: "SageLine50v29" (version 29), "SageLine50v28", etc.
    """
    
    # SDO ProgIDs for different Sage 50 versions
    # UK Sage 50 ProgIDs
    SDO_PROGIDS = [
        "SageDataObject50.SDOEngine",      # Standard
        "SageDataObject50v29.SDOEngine",   # Version 29 (2024)
        "SageDataObject50v28.SDOEngine",   # Version 28 (2023)
        "SageDataObject50v27.SDOEngine",   # Version 27 (2022)
    ]
    
    # US Peachtree/Sage 50 Accounting ProgIDs
    PEACHTREE_PROGIDS = [
        "PeachtreeAccounting.Login.31",    # Sage 50 2024
        "PeachtreeAccounting.Login.30",    # Sage 50 2023
        "PeachtreeAccounting.Login",       # Generic
        "PeachwServer.Login",              # Alternative
    ]
    
    # Common ODBC DSN names
    ODBC_DSNS = [
        "SageLine50v29",
        "SageLine50v28", 
        "SageLine50v27",
        "Sage Line 50",
        "Sage50",
    ]
    
    def __init__(self, config: AgentConfig):
        self.config = config
        
        self._connection: Optional[Any] = None
        self._connection_type: Optional[str] = None  # 'odbc', 'com', 'file'
        self._connected = False
        self._company_name: Optional[str] = None
        self._sage_version: Optional[str] = None
        self._data_path: Optional[str] = None
        
        # Thread safety for COM
        self._com_initialized = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def company_name(self) -> Optional[str]:
        return self._company_name
    
    @property
    def sage_version(self) -> Optional[str]:
        return self._sage_version
    
    @property
    def connection_type(self) -> Optional[str]:
        return self._connection_type
    
    def _init_com(self):
        """Initialize COM for the current thread."""
        if not HAS_COM:
            return False
        
        if not self._com_initialized:
            pythoncom.CoInitialize()
            self._com_initialized = True
        return True
    
    def _cleanup_com(self):
        """Cleanup COM for the current thread."""
        if self._com_initialized:
            pythoncom.CoUninitialize()
            self._com_initialized = False
    
    def find_sage_data_path(self) -> Optional[str]:
        """
        Attempt to find where Sage 50 data is stored.
        
        Returns:
            Path to Sage data directory, or None if not found
        """
        # Common locations to check
        possible_paths = [
            # Configured path
            self.config.sage50_company_path,
            
            # ProgramData locations (Sage 50 2020+)
            r"C:\ProgramData\Sage\Accounts\2024",
            r"C:\ProgramData\Sage\Accounts\2023",
            r"C:\ProgramData\Sage\Accounts\2022",
            r"C:\ProgramData\Sage\Accounts",
            
            # Program Files (older versions)
            r"C:\Program Files (x86)\Sage\Accounts",
            r"C:\Program Files\Sage\Accounts",
            
            # User Documents
            os.path.expanduser(r"~\Documents\Sage\Accounts"),
        ]
        
        for path in possible_paths:
            if path and os.path.exists(path):
                # Look for ACCDATA or company files
                if os.path.exists(os.path.join(path, "ACCDATA")):
                    return path
                # Check for Company.001 etc.
                for item in os.listdir(path):
                    item_path = os.path.join(path, item)
                    if os.path.isdir(item_path):
                        if os.path.exists(os.path.join(item_path, "ACCDATA")):
                            return item_path
        
        return None
    
    def find_odbc_dsn(self) -> Optional[str]:
        """Find available Sage 50 ODBC DSN."""
        if not HAS_ODBC:
            return None
        
        # Check configured DSN first
        if self.config.sage50_odbc_dsn:
            return self.config.sage50_odbc_dsn
        
        # Try to find installed DSNs
        try:
            import winreg
            
            # Check 64-bit DSNs
            for reg_path in [
                r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources",
                r"SOFTWARE\WOW6432Node\ODBC\ODBC.INI\ODBC Data Sources",
            ]:
                try:
                    key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
                    i = 0
                    while True:
                        try:
                            name, value, _ = winreg.EnumValue(key, i)
                            if "sage" in name.lower():
                                return name
                            i += 1
                        except WindowsError:
                            break
                    winreg.CloseKey(key)
                except WindowsError:
                    continue
                    
        except Exception as e:
            logger.debug(f"Could not search registry for DSN: {e}")
        
        return None
    
    def connect(self) -> bool:
        """
        Connect to Sage 50 using the best available method.
        
        Tries in order:
        1. ODBC (if DSN configured or found)
        2. COM/SDO
        3. File-based
        """
        # Try ODBC first
        dsn = self.find_odbc_dsn()
        if dsn and HAS_ODBC:
            try:
                return self._connect_odbc(dsn)
            except Exception as e:
                logger.warning(f"ODBC connection failed: {e}")
        
        # Try COM/SDO
        if HAS_COM:
            try:
                return self._connect_com()
            except Exception as e:
                logger.warning(f"COM connection failed: {e}")
        
        # File-based fallback
        data_path = self.find_sage_data_path()
        if data_path:
            return self._connect_file_based(data_path)
        
        raise Sage50ConnectionError(
            "Could not connect to Sage 50. Please configure:\n"
            "- SAGE_ODBC_DSN (e.g., 'SageLine50v29')\n"
            "- or SAGE_COMPANY_PATH (path to Sage data folder)"
        )
    
    def _connect_odbc(self, dsn: str) -> bool:
        """Connect via ODBC."""
        logger.info(f"Connecting to Sage 50 via ODBC: {dsn}")
        
        try:
            conn_string = f"DSN={dsn}"
            
            # Add credentials if provided
            if self.config.sage50_username:
                conn_string += f";UID={self.config.sage50_username}"
            if self.config.sage50_password:
                conn_string += f";PWD={self.config.sage50_password}"
            
            # Add data path if provided
            if self.config.sage50_company_path:
                conn_string += f";DIR={self.config.sage50_company_path}"
            
            self._connection = pyodbc.connect(conn_string, timeout=30)
            self._connection_type = "odbc"
            self._connected = True
            
            # Get company info
            try:
                cursor = self._connection.cursor()
                cursor.execute("SELECT COMPANY_NAME FROM COMPANY")
                row = cursor.fetchone()
                self._company_name = row[0] if row else dsn
            except Exception:
                self._company_name = dsn
            
            self._sage_version = f"Sage 50 (ODBC: {dsn})"
            
            logger.info(f"Connected to Sage 50: {self._company_name}")
            return True
            
        except pyodbc.Error as e:
            raise Sage50ConnectionError(f"ODBC error: {e}")
    
    def _connect_com(self) -> bool:
        """Connect via COM/SDO."""
        self._init_com()
        
        logger.info("Connecting to Sage 50 via COM/SDO...")
        
        data_path = self.config.sage50_company_path or self.find_sage_data_path()
        username = self.config.sage50_username or "Peachtree"
        password = self.config.sage50_password or ""
        
        # Try 1: Peachtree/US version (Sage 50 Accounting)
        for prog_id in self.PEACHTREE_PROGIDS:
            try:
                logger.debug(f"Trying Peachtree: {prog_id}")
                login = win32com.client.Dispatch(prog_id)
                
                # Get Application object
                app = login.GetApplication(username, password)
                
                if app and 'ILogin' not in str(type(app)):
                    # Open company if path provided
                    if data_path and hasattr(app, 'OpenCompany'):
                        app.OpenCompany(data_path)
                    
                    self._connection = app
                    self._connection_type = "com"
                    self._connected = True
                    self._sage_version = f"Sage 50 Accounting ({prog_id})"
                    self._data_path = data_path
                    self._company_name = "Sage 50 Accounting"
                    
                    logger.info(f"Connected to Sage 50 Accounting via {prog_id}")
                    return True
                    
            except Exception as e:
                logger.debug(f"Peachtree {prog_id} failed: {e}")
                continue
        
        # Try 2: UK SDO version
        sdo_engine = None
        for prog_id in self.SDO_PROGIDS:
            try:
                sdo_engine = win32com.client.Dispatch(prog_id)
                self._sage_version = f"Sage 50 ({prog_id})"
                break
            except Exception:
                continue
        
        if not sdo_engine:
            raise Sage50ConnectionError("Could not create Sage SDO object")
        
        try:
            if data_path:
                # Open company
                ws = sdo_engine.Workspaces.Add("Main")
                ws.Connect(
                    data_path,
                    username,
                    password,
                    "Main"
                )
                self._connection = ws
                self._data_path = data_path
            else:
                self._connection = sdo_engine
            
            self._connection_type = "com"
            self._connected = True
            
            # Get company name
            try:
                self._company_name = self._connection.Company.Name
            except Exception:
                self._company_name = "Sage 50"
            
            logger.info(f"Connected to Sage 50 via COM: {self._company_name}")
            return True
            
        except Exception as e:
            raise Sage50ConnectionError(f"COM error: {e}")
    
    def _connect_file_based(self, data_path: str) -> bool:
        """Set up file-based integration."""
        logger.info(f"Using file-based Sage integration: {data_path}")
        
        self._data_path = data_path
        self._connection_type = "file"
        self._connected = True
        self._company_name = os.path.basename(data_path)
        self._sage_version = "Sage 50 (File-based)"
        
        # Create import/export directories
        import_dir = os.path.join(data_path, "MNM_Import")
        export_dir = os.path.join(data_path, "MNM_Export")
        
        os.makedirs(import_dir, exist_ok=True)
        os.makedirs(export_dir, exist_ok=True)
        
        return True
    
    def disconnect(self):
        """Disconnect from Sage 50."""
        try:
            if self._connection_type == "odbc" and self._connection:
                self._connection.close()
            elif self._connection_type == "com" and self._connection:
                try:
                    self._connection.Disconnect()
                except Exception:
                    pass
            
            self._connection = None
            self._connected = False
            self._connection_type = None
            
            logger.info("Disconnected from Sage 50")
            
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")
        finally:
            self._cleanup_com()
    
    @contextmanager
    def session(self):
        """Context manager for Sage sessions."""
        try:
            if not self._connected:
                self.connect()
            yield self._connection
        finally:
            pass
    
    def execute_query(self, query: str, params: tuple = ()) -> list[dict]:
        """
        Execute a SQL query (ODBC only).
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of row dictionaries
        """
        if self._connection_type != "odbc":
            raise Sage50OperationError("SQL queries require ODBC connection")
        
        cursor = self._connection.cursor()
        cursor.execute(query, params)
        
        columns = [col[0] for col in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return results
    
    def get_unshipped_orders_query(self) -> str:
        """
        Get SQL query for unshipped orders.
        
        Sage 50 stores sales orders in SALES_ORDER and SALES_ORDER_ITEM tables.
        """
        return """
            SELECT 
                so.ORDER_NUMBER,
                so.ORDER_DATE,
                so.ACCOUNT_REF,
                so.NAME as CUSTOMER_NAME,
                so.ADDRESS_1,
                so.ADDRESS_2,
                so.ADDRESS_3,
                so.ADDRESS_4,
                so.ADDRESS_5 as POSTCODE,
                so.NOTES_1,
                so.NOTES_2,
                so.NOTES_3,
                so.TOTAL_NET,
                so.TOTAL_TAX,
                so.TOTAL_GROSS,
                so.ORDER_STATUS,
                so.DESPATCH_STATUS
            FROM SALES_ORDER so
            WHERE so.ORDER_STATUS <> 2  -- 2 = Complete
              AND so.DESPATCH_STATUS <> 2  -- Not fully despatched
            ORDER BY so.ORDER_DATE DESC
        """
    
    def get_order_items_query(self, order_number: str) -> str:
        """Get SQL query for order line items."""
        return f"""
            SELECT 
                soi.ORDER_NUMBER,
                soi.ITEM_NUMBER,
                soi.STOCK_CODE,
                soi.DESCRIPTION,
                soi.QTY_ORDER,
                soi.QTY_DELIVERED,
                soi.UNIT_PRICE,
                soi.NET_AMOUNT,
                soi.TAX_AMOUNT
            FROM SALES_ORDER_ITEM soi
            WHERE soi.ORDER_NUMBER = '{order_number}'
            ORDER BY soi.ITEM_NUMBER
        """
    
    def get_company_info(self) -> dict:
        """Get information about the connected company."""
        if not self._connected:
            return {"connected": False}
        
        info = {
            "connected": True,
            "connection_type": self._connection_type,
            "company_name": self._company_name,
            "sage_version": self._sage_version,
            "data_path": self._data_path,
        }
        
        # Try to get more details via ODBC
        if self._connection_type == "odbc":
            try:
                cursor = self._connection.cursor()
                cursor.execute("SELECT * FROM COMPANY")
                row = cursor.fetchone()
                if row:
                    columns = [col[0] for col in cursor.description]
                    for i, col in enumerate(columns):
                        if col in ["COMPANY_NAME", "ADDRESS_1", "ADDRESS_2", 
                                   "ADDRESS_3", "POSTCODE", "TELEPHONE", "VAT_REG_NO"]:
                            info[col.lower()] = row[i]
            except Exception:
                pass
        
        return info
    
    def test_connection(self) -> dict:
        """Test connection to Sage 50."""
        result = {
            "success": False,
            "message": "",
            "connection_type": None,
            "details": {},
            "unshipped_orders_count": 0,
        }
        
        try:
            # Check what's available
            result["odbc_available"] = HAS_ODBC
            result["com_available"] = HAS_COM
            result["found_dsn"] = self.find_odbc_dsn()
            result["found_data_path"] = self.find_sage_data_path()
            
            if self._connected:
                result["success"] = True
                result["message"] = "Already connected"
                result["connection_type"] = self._connection_type
                result["details"] = self.get_company_info()
            else:
                self.connect()
                result["success"] = True
                result["message"] = "Connection successful"
                result["connection_type"] = self._connection_type
                result["details"] = self.get_company_info()
            
            # Try to count unshipped orders
            if self._connection_type == "odbc":
                try:
                    cursor = self._connection.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) FROM SALES_ORDER 
                        WHERE ORDER_STATUS <> 2 AND DESPATCH_STATUS <> 2
                    """)
                    row = cursor.fetchone()
                    result["unshipped_orders_count"] = row[0] if row else 0
                except Exception as e:
                    result["unshipped_orders_query_error"] = str(e)
            
        except Sage50ConnectionError as e:
            result["message"] = str(e)
        except Exception as e:
            result["message"] = f"Error: {e}"
        
        return result
    
    @property
    def import_path(self) -> str:
        """Path for import files."""
        if self._data_path:
            return os.path.join(self._data_path, "MNM_Import")
        return ""
    
    @property
    def export_path(self) -> str:
        """Path for export files."""
        if self._data_path:
            return os.path.join(self._data_path, "MNM_Export")
        return ""
