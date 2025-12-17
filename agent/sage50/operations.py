"""
Sage 50 Quantum Operations.
High-level operations for pulling/pushing data from/to Sage 50.

Key Operations for M&M 2.0:
- Pull unshipped orders (orders without tracking)
- Pull all orders for a date range
- Push orders from ecommerce to Sage
- Export data to CSV for upload to platforms
"""

from typing import Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import csv
from loguru import logger

from agent.sage50.connector import (
    Sage50Connector,
    Sage50Error,
    Sage50OperationError,
)
from agent.models import (
    Order,
    OrderLine,
    Product,
    Platform,
    SpreadsheetData,
)


class Sage50Operations:
    """
    High-level operations for Sage 50.
    
    Main operations:
    - pull_unshipped_orders() - Get orders that need tracking
    - pull_orders() - Get orders for date range
    - push_order() - Create order in Sage from ecommerce
    - export_for_platform() - Format data for Amazon/eBay upload
    """
    
    def __init__(self, connector: Sage50Connector):
        self.connector = connector
    
    def ensure_connected(self):
        """Ensure we're connected to Sage 50."""
        if not self.connector.is_connected:
            self.connector.connect()
    
    # ===== UNSHIPPED ORDERS (Main M&M 2.0 Operation) =====
    
    def pull_unshipped_orders(self) -> list[Order]:
        """
        Pull all unshipped orders from Sage 50.
        
        These are orders that:
        - Have status != Complete (2)
        - Have despatch status != Fully Despatched (2)
        
        Returns:
            List of Order objects without tracking info
        """
        self.ensure_connected()
        
        orders = []
        
        if self.connector.connection_type == "odbc":
            orders = self._pull_unshipped_odbc()
        elif self.connector.connection_type == "file":
            orders = self._pull_unshipped_file()
        else:
            logger.warning("Cannot pull orders - need ODBC or file-based connection")
        
        logger.info(f"Found {len(orders)} unshipped orders")
        return orders
    
    def _pull_unshipped_odbc(self) -> list[Order]:
        """Pull unshipped orders via ODBC."""
        query = """
            SELECT 
                so.ORDER_NUMBER,
                so.ORDER_DATE,
                so.ACCOUNT_REF,
                so.NAME,
                so.ADDRESS_1,
                so.ADDRESS_2,
                so.ADDRESS_3,
                so.ADDRESS_4,
                so.ADDRESS_5,
                so.C_ADDRESS_1,
                so.C_ADDRESS_2,
                so.C_ADDRESS_3,
                so.C_ADDRESS_4,
                so.C_ADDRESS_5,
                so.CONTACT_NAME,
                so.TELEPHONE,
                so.E_MAIL,
                so.NOTES_1,
                so.NOTES_2,
                so.NOTES_3,
                so.TOTAL_NET,
                so.TOTAL_TAX,
                so.TOTAL_GROSS,
                so.FOREIGN_NET,
                so.FOREIGN_TAX,
                so.FOREIGN_GROSS,
                so.COURIER_NAME,
                so.COURIER_NUMBER
            FROM SALES_ORDER so
            WHERE so.ORDER_STATUS <> 2
              AND (so.DESPATCH_STATUS IS NULL OR so.DESPATCH_STATUS <> 2)
            ORDER BY so.ORDER_DATE DESC
        """
        
        try:
            rows = self.connector.execute_query(query)
            
            orders = []
            for row in rows:
                # Check if already has tracking in notes
                notes = f"{row.get('NOTES_1', '')} {row.get('NOTES_2', '')} {row.get('NOTES_3', '')}"
                
                # Parse platform order ID from notes if present
                amazon_id = None
                ebay_id = None
                shopify_id = None
                
                if "amazon" in notes.lower() or "amz" in notes.lower():
                    # Try to extract Amazon order ID
                    import re
                    match = re.search(r'(\d{3}-\d{7}-\d{7})', notes)
                    if match:
                        amazon_id = match.group(1)
                
                if "ebay" in notes.lower():
                    import re
                    match = re.search(r'(\d{2}-\d{5}-\d{5})', notes)
                    if match:
                        ebay_id = match.group(1)
                
                order = Order(
                    sage_order_ref=str(row.get('ORDER_NUMBER', '')),
                    amazon_order_id=amazon_id,
                    ebay_order_id=ebay_id,
                    shopify_order_id=shopify_id,
                    order_date=row.get('ORDER_DATE') or datetime.now(),
                    customer_name=row.get('NAME', ''),
                    customer_email=row.get('E_MAIL', ''),
                    customer_phone=row.get('TELEPHONE', ''),
                    
                    # Delivery address (or customer address as fallback)
                    ship_name=row.get('C_ADDRESS_1') or row.get('NAME', ''),
                    ship_address_1=row.get('C_ADDRESS_2') or row.get('ADDRESS_1', ''),
                    ship_address_2=row.get('C_ADDRESS_3') or row.get('ADDRESS_2', ''),
                    ship_city=row.get('C_ADDRESS_4') or row.get('ADDRESS_3', ''),
                    ship_state=row.get('C_ADDRESS_5') or row.get('ADDRESS_4', ''),
                    ship_postcode=row.get('ADDRESS_5', ''),
                    
                    # Totals
                    subtotal=float(row.get('TOTAL_NET', 0) or 0),
                    tax_total=float(row.get('TOTAL_TAX', 0) or 0),
                    total=float(row.get('TOTAL_GROSS', 0) or 0),
                    
                    # Tracking (if already set)
                    carrier=row.get('COURIER_NAME', ''),
                    tracking_number=row.get('COURIER_NUMBER', ''),
                    
                    status="unshipped",
                    source_platform=Platform.SAGE_QUANTUM,
                )
                
                # Get line items
                order.lines = self._get_order_items(str(row.get('ORDER_NUMBER', '')))
                
                orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Error pulling unshipped orders: {e}")
            raise Sage50OperationError(f"Failed to pull unshipped orders: {e}")
    
    def _get_order_items(self, order_number: str) -> list[OrderLine]:
        """Get line items for an order."""
        query = f"""
            SELECT 
                STOCK_CODE,
                DESCRIPTION,
                QTY_ORDER,
                QTY_DELIVERED,
                UNIT_PRICE,
                NET_AMOUNT,
                TAX_AMOUNT
            FROM SALES_ORDER_ITEM
            WHERE ORDER_NUMBER = ?
            ORDER BY ITEM_NUMBER
        """
        
        try:
            rows = self.connector.execute_query(query, (order_number,))
            
            lines = []
            for row in rows:
                line = OrderLine(
                    sku=row.get('STOCK_CODE', ''),
                    description=row.get('DESCRIPTION', ''),
                    quantity=int(row.get('QTY_ORDER', 0) or 0),
                    unit_price=float(row.get('UNIT_PRICE', 0) or 0),
                )
                lines.append(line)
            
            return lines
            
        except Exception:
            return []
    
    def _pull_unshipped_file(self) -> list[Order]:
        """Pull unshipped orders from export file."""
        export_path = Path(self.connector.export_path)
        
        # Look for exported orders file
        possible_files = [
            export_path / "unshipped_orders.csv",
            export_path / "orders_export.csv",
            export_path / "sales_orders.csv",
        ]
        
        for filepath in possible_files:
            if filepath.exists():
                return self._parse_orders_csv(filepath)
        
        logger.warning(f"No order export file found in {export_path}")
        logger.info("To use file-based import, export orders from Sage to CSV")
        return []
    
    def _parse_orders_csv(self, filepath: Path) -> list[Order]:
        """Parse orders from CSV file."""
        orders = []
        
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    order = Order(
                        sage_order_ref=row.get('ORDER_NUMBER') or row.get('Order Number', ''),
                        order_date=self._parse_date(row.get('ORDER_DATE') or row.get('Date', '')),
                        customer_name=row.get('NAME') or row.get('Customer', ''),
                        customer_email=row.get('E_MAIL') or row.get('Email', ''),
                        ship_address_1=row.get('ADDRESS_1') or row.get('Address 1', ''),
                        ship_city=row.get('ADDRESS_3') or row.get('Town', ''),
                        ship_postcode=row.get('ADDRESS_5') or row.get('Postcode', ''),
                        total=float(row.get('TOTAL_GROSS') or row.get('Total', 0) or 0),
                        status="unshipped",
                        source_platform=Platform.SAGE_QUANTUM,
                    )
                    orders.append(order)
                except Exception as e:
                    logger.warning(f"Error parsing order row: {e}")
        
        return orders
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date from various formats."""
        if not date_str:
            return datetime.now()
        
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        return datetime.now()
    
    # ===== PULL ALL ORDERS =====
    
    def pull_orders(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Order]:
        """Pull orders for a date range."""
        self.ensure_connected()
        
        if self.connector.connection_type != "odbc":
            logger.warning("Date-range queries require ODBC connection")
            return []
        
        query = """
            SELECT 
                ORDER_NUMBER, ORDER_DATE, ACCOUNT_REF, NAME,
                ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5,
                E_MAIL, TELEPHONE, TOTAL_NET, TOTAL_TAX, TOTAL_GROSS,
                ORDER_STATUS, DESPATCH_STATUS, COURIER_NAME, COURIER_NUMBER
            FROM SALES_ORDER
            WHERE 1=1
        """
        params = []
        
        if start_date:
            query += " AND ORDER_DATE >= ?"
            params.append(start_date)
        if end_date:
            query += " AND ORDER_DATE <= ?"
            params.append(end_date)
        
        query += f" ORDER BY ORDER_DATE DESC"
        
        if limit:
            # Note: TOP syntax varies by ODBC driver
            query = query.replace("SELECT", f"SELECT TOP {limit}")
        
        try:
            rows = self.connector.execute_query(query, tuple(params))
            
            orders = []
            for row in rows:
                order = Order(
                    sage_order_ref=str(row.get('ORDER_NUMBER', '')),
                    order_date=row.get('ORDER_DATE') or datetime.now(),
                    customer_name=row.get('NAME', ''),
                    customer_email=row.get('E_MAIL', ''),
                    ship_address_1=row.get('ADDRESS_1', ''),
                    ship_city=row.get('ADDRESS_3', ''),
                    ship_postcode=row.get('ADDRESS_5', ''),
                    total=float(row.get('TOTAL_GROSS', 0) or 0),
                    tracking_number=row.get('COURIER_NUMBER', ''),
                    carrier=row.get('COURIER_NAME', ''),
                    source_platform=Platform.SAGE_QUANTUM,
                )
                orders.append(order)
            
            return orders
            
        except Exception as e:
            logger.error(f"Error pulling orders: {e}")
            return []
    
    # ===== PUSH ORDER TO SAGE =====
    
    def push_order(self, order: Order) -> dict:
        """
        Push an order from ecommerce to Sage 50.
        
        For file-based: Creates import CSV file
        For ODBC: Would need INSERT (but Sage ODBC is usually read-only)
        For COM: Use SDO to create order
        """
        self.ensure_connected()
        
        if self.connector.connection_type == "file":
            return self._push_order_file(order)
        else:
            # Most Sage 50 ODBC connections are read-only
            # Create file for import instead
            return self._push_order_file(order)
    
    def _push_order_file(self, order: Order) -> dict:
        """Create import file for Sage."""
        import_path = Path(self.connector.import_path)
        import_path.mkdir(parents=True, exist_ok=True)
        
        import_file = import_path / "orders_to_import.csv"
        
        # Check if file exists
        file_exists = import_file.exists()
        
        with open(import_file, 'a', newline='', encoding='utf-8') as f:
            fieldnames = [
                'ACCOUNT_REF', 'NAME', 'ADDRESS_1', 'ADDRESS_2', 'ADDRESS_3',
                'ADDRESS_4', 'ADDRESS_5', 'E_MAIL', 'TELEPHONE',
                'NOTES_1', 'NOTES_2', 'NOTES_3',
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            # Combine platform IDs into notes
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            platform = str(order.source_platform).replace("Platform.", "")
            
            writer.writerow({
                'ACCOUNT_REF': order.customer_name[:8].upper().replace(" ", ""),
                'NAME': order.customer_name,
                'ADDRESS_1': order.ship_address_1 or '',
                'ADDRESS_2': order.ship_address_2 or '',
                'ADDRESS_3': order.ship_city or '',
                'ADDRESS_4': order.ship_state or '',
                'ADDRESS_5': order.ship_postcode or '',
                'E_MAIL': order.customer_email or '',
                'TELEPHONE': order.customer_phone or '',
                'NOTES_1': f"{platform}: {platform_id}" if platform_id else '',
                'NOTES_2': f"Total: {order.total}",
                'NOTES_3': order.tracking_number or '',
            })
        
        logger.info(f"Order added to import file: {import_file}")
        
        return {
            "success": True,
            "message": "Order added to import file",
            "import_file": str(import_file),
            "platform_order_id": platform_id,
        }
    
    # ===== EXPORT FOR PLATFORMS =====
    
    def export_for_tracking_upload(
        self,
        orders: list[Order],
        platform: Platform,
    ) -> SpreadsheetData:
        """
        Format orders with tracking for platform upload.
        
        Creates the file format that Amazon/eBay needs to update tracking.
        """
        if platform == Platform.AMAZON:
            return self._format_amazon_tracking(orders)
        elif platform == Platform.EBAY:
            return self._format_ebay_tracking(orders)
        elif platform == Platform.SHOPIFY:
            return self._format_shopify_tracking(orders)
        else:
            return self._format_generic_tracking(orders)
    
    def _format_amazon_tracking(self, orders: list[Order]) -> SpreadsheetData:
        """Format for Amazon Seller Central shipment confirmation."""
        columns = [
            "order-id", "order-item-id", "quantity", "ship-date",
            "carrier-code", "carrier-name", "tracking-number", "ship-method"
        ]
        
        rows = []
        for order in orders:
            if order.amazon_order_id and order.tracking_number:
                rows.append({
                    "order-id": order.amazon_order_id,
                    "order-item-id": "",
                    "quantity": sum(line.quantity for line in order.lines) if order.lines else 1,
                    "ship-date": (order.ship_date or datetime.now()).strftime("%Y-%m-%d"),
                    "carrier-code": self._amazon_carrier_code(order.carrier),
                    "carrier-name": order.carrier or "",
                    "tracking-number": order.tracking_number,
                    "ship-method": "Standard",
                })
        
        return SpreadsheetData(
            filename=f"amazon_shipment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            columns=columns,
            rows=rows,
            row_count=len(rows),
        )
    
    def _format_ebay_tracking(self, orders: list[Order]) -> SpreadsheetData:
        """Format for eBay File Exchange."""
        columns = ["*Action", "*OrderID", "ShippingCarrier", "ShipmentTrackingNumber"]
        
        rows = []
        for order in orders:
            if order.ebay_order_id and order.tracking_number:
                rows.append({
                    "*Action": "Update",
                    "*OrderID": order.ebay_order_id,
                    "ShippingCarrier": self._ebay_carrier_code(order.carrier),
                    "ShipmentTrackingNumber": order.tracking_number,
                })
        
        return SpreadsheetData(
            filename=f"ebay_tracking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            columns=columns,
            rows=rows,
            row_count=len(rows),
        )
    
    def _format_shopify_tracking(self, orders: list[Order]) -> SpreadsheetData:
        """Format for Shopify fulfillment."""
        columns = ["Name", "Fulfillment Status", "Tracking Number", "Tracking Company"]
        
        rows = []
        for order in orders:
            if order.shopify_order_id and order.tracking_number:
                rows.append({
                    "Name": order.shopify_order_id,
                    "Fulfillment Status": "fulfilled",
                    "Tracking Number": order.tracking_number,
                    "Tracking Company": order.carrier or "",
                })
        
        return SpreadsheetData(
            filename=f"shopify_fulfillment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            columns=columns,
            rows=rows,
            row_count=len(rows),
        )
    
    def _format_generic_tracking(self, orders: list[Order]) -> SpreadsheetData:
        """Generic format."""
        columns = ["Order Ref", "Platform", "Platform Order ID", "Tracking", "Carrier", "Ship Date"]
        
        rows = []
        for order in orders:
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            rows.append({
                "Order Ref": order.sage_order_ref,
                "Platform": str(order.source_platform).replace("Platform.", ""),
                "Platform Order ID": platform_id or "",
                "Tracking": order.tracking_number or "",
                "Carrier": order.carrier or "",
                "Ship Date": (order.ship_date or datetime.now()).strftime("%Y-%m-%d"),
            })
        
        return SpreadsheetData(
            filename=f"tracking_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            columns=columns,
            rows=rows,
            row_count=len(rows),
        )
    
    def _amazon_carrier_code(self, carrier: Optional[str]) -> str:
        """Map carrier to Amazon code."""
        if not carrier:
            return "Other"
        
        mapping = {
            "fedex": "FedEx",
            "ups": "UPS",
            "usps": "USPS",
            "dhl": "DHL",
            "royal mail": "Royal Mail",
            "parcelforce": "Parcelforce",
            "hermes": "Hermes",
            "yodel": "Yodel",
        }
        return mapping.get(carrier.lower(), "Other")
    
    def _ebay_carrier_code(self, carrier: Optional[str]) -> str:
        """Map carrier to eBay code."""
        if not carrier:
            return "Other"
        
        mapping = {
            "fedex": "FedEx",
            "ups": "UPS",
            "usps": "USPS",
            "dhl": "DHL",
            "royal mail": "Royal Mail",
            "parcelforce": "Parcelforce Worldwide",
            "hermes": "Hermes",
            "yodel": "Yodel",
        }
        return mapping.get(carrier.lower(), "Other")
    
    # ===== HEALTH CHECK =====
    
    def health_check(self) -> dict:
        """Perform health check."""
        result = {
            "status": "unknown",
            "connected": False,
            "connection_type": None,
            "company": None,
            "unshipped_orders": 0,
        }
        
        try:
            self.ensure_connected()
            result["connected"] = True
            result["connection_type"] = self.connector.connection_type
            result["company"] = self.connector.company_name
            
            # Count unshipped orders
            try:
                orders = self.pull_unshipped_orders()
                result["unshipped_orders"] = len(orders)
            except Exception as e:
                result["order_query_error"] = str(e)
            
            result["status"] = "healthy"
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
        
        return result
