"""
Data Formatter.
Formats data for specific platform requirements.

M&M 2.0 Requirements:
- Amazon Seller Central format (Inventory → Add Products via Upload)
- eBay File Exchange format
- Shopify CSV format
"""

import csv
import io
from typing import Any, Optional
from datetime import datetime
from pathlib import Path
from loguru import logger

from agent.models import SpreadsheetData, Platform, ColumnMapping, PlatformFormat


class DataFormatter:
    """
    Formats spreadsheet data for platform uploads.
    
    Supports:
    - Amazon Seller Central flat files
    - eBay File Exchange CSV
    - Shopify product/order CSV
    """
    
    # Amazon required columns for different file types
    AMAZON_INVENTORY_COLUMNS = [
        "sku", "product-id", "product-id-type", "price", "minimum-seller-allowed-price",
        "maximum-seller-allowed-price", "item-condition", "quantity", "add-delete",
        "will-ship-internationally", "expedited-shipping", "standard-plus",
        "item-note", "fulfillment-center-id", "product-tax-code", "leadtime-to-ship"
    ]
    
    AMAZON_SHIPMENT_COLUMNS = [
        "order-id", "order-item-id", "quantity", "ship-date", "carrier-code",
        "carrier-name", "tracking-number", "ship-method"
    ]
    
    # eBay File Exchange columns
    EBAY_LISTING_COLUMNS = [
        "Action", "ItemID", "Title", "Description", "Format", "Duration",
        "StartPrice", "BuyItNowPrice", "Quantity", "Category", "StoreCategory",
        "PayPalAccepted", "PayPalEmailAddress", "ShippingType", "ShippingService-1:Option",
        "ShippingService-1:Cost", "DispatchTimeMax"
    ]
    
    EBAY_SHIPPING_COLUMNS = [
        "*Action", "*OrderID", "ShippingCarrier", "ShipmentTrackingNumber"
    ]
    
    # Shopify columns
    SHOPIFY_PRODUCT_COLUMNS = [
        "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type",
        "Tags", "Published", "Option1 Name", "Option1 Value", "Variant SKU",
        "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty",
        "Variant Inventory Policy", "Variant Fulfillment Service", "Variant Price",
        "Variant Compare At Price", "Variant Requires Shipping", "Variant Taxable",
        "Image Src", "Status"
    ]
    
    def __init__(self):
        self._format_stats = {
            "rows_formatted": 0,
            "columns_mapped": 0,
            "files_created": 0,
        }
    
    def format_for_platform(
        self,
        data: SpreadsheetData,
        platform: Platform,
        file_type: str = "inventory",
    ) -> SpreadsheetData:
        """
        Format data for a specific platform.
        
        Args:
            data: Source spreadsheet data
            platform: Target platform
            file_type: Type of file (inventory, shipment, etc.)
            
        Returns:
            Formatted spreadsheet data
        """
        logger.info(f"Formatting {len(data.rows)} rows for {platform} ({file_type})")
        
        if platform == Platform.AMAZON:
            return self._format_for_amazon(data, file_type)
        elif platform == Platform.EBAY:
            return self._format_for_ebay(data, file_type)
        elif platform == Platform.SHOPIFY:
            return self._format_for_shopify(data, file_type)
        else:
            return data
    
    def _format_for_amazon(self, data: SpreadsheetData, file_type: str) -> SpreadsheetData:
        """Format data for Amazon Seller Central."""
        if file_type == "shipment":
            columns = self.AMAZON_SHIPMENT_COLUMNS
            mapper = self._map_to_amazon_shipment
        else:
            columns = self.AMAZON_INVENTORY_COLUMNS
            mapper = self._map_to_amazon_inventory
        
        formatted_rows = []
        for row in data.rows:
            formatted_row = mapper(row)
            formatted_rows.append(formatted_row)
            self._format_stats["rows_formatted"] += 1
        
        return SpreadsheetData(
            filename=f"amazon_{file_type}_{datetime.now().strftime('%Y%m%d')}.txt",
            columns=columns,
            rows=formatted_rows,
            row_count=len(formatted_rows),
        )
    
    def _map_to_amazon_inventory(self, row: dict) -> dict:
        """Map row to Amazon inventory format."""
        return {
            "sku": self._get_value(row, ["SKU", "sku", "StockCode", "ProductCode"]),
            "product-id": self._get_value(row, ["ASIN", "asin", "EAN", "UPC"]),
            "product-id-type": "1" if self._get_value(row, ["ASIN", "asin"]) else "4",  # 1=ASIN, 4=EAN
            "price": self._format_price(self._get_value(row, ["Price", "price", "SalesPrice"])),
            "quantity": self._get_value(row, ["Quantity", "qty", "Stock", "QtyAvailable"]),
            "item-condition": "11",  # New
            "add-delete": "a",  # Add
            "will-ship-internationally": "y",
            "expedited-shipping": "n",
            "leadtime-to-ship": "2",
        }
    
    def _map_to_amazon_shipment(self, row: dict) -> dict:
        """Map row to Amazon shipment confirmation format."""
        return {
            "order-id": self._get_value(row, ["OrderID", "order_id", "AmazonOrderID"]),
            "order-item-id": self._get_value(row, ["OrderItemID", "order_item_id"]),
            "quantity": self._get_value(row, ["Quantity", "qty", "ShipQuantity"]),
            "ship-date": self._format_date(self._get_value(row, ["ShipDate", "ship_date", "Date"])),
            "carrier-code": self._map_carrier_amazon(self._get_value(row, ["Carrier", "carrier"])),
            "carrier-name": self._get_value(row, ["Carrier", "carrier", "CarrierName"]),
            "tracking-number": self._get_value(row, ["TrackingNumber", "tracking", "Tracking"]),
            "ship-method": "Standard",
        }
    
    def _format_for_ebay(self, data: SpreadsheetData, file_type: str) -> SpreadsheetData:
        """Format data for eBay File Exchange."""
        if file_type == "shipment":
            columns = self.EBAY_SHIPPING_COLUMNS
            mapper = self._map_to_ebay_shipment
        else:
            columns = self.EBAY_LISTING_COLUMNS
            mapper = self._map_to_ebay_listing
        
        formatted_rows = []
        for row in data.rows:
            formatted_row = mapper(row)
            formatted_rows.append(formatted_row)
            self._format_stats["rows_formatted"] += 1
        
        return SpreadsheetData(
            filename=f"ebay_{file_type}_{datetime.now().strftime('%Y%m%d')}.csv",
            columns=columns,
            rows=formatted_rows,
            row_count=len(formatted_rows),
        )
    
    def _map_to_ebay_listing(self, row: dict) -> dict:
        """Map row to eBay listing format."""
        return {
            "Action": "Add",
            "ItemID": "",
            "Title": self._get_value(row, ["Title", "Description", "ProductName"])[:80],
            "Description": self._get_value(row, ["Description", "LongDescription"]),
            "Format": "FixedPrice",
            "Duration": "GTC",  # Good Till Cancelled
            "StartPrice": self._format_price(self._get_value(row, ["Price", "SalesPrice"])),
            "Quantity": self._get_value(row, ["Quantity", "Stock"]),
            "Category": self._get_value(row, ["Category", "EbayCategory"]),
        }
    
    def _map_to_ebay_shipment(self, row: dict) -> dict:
        """Map row to eBay shipping update format."""
        return {
            "*Action": "Update",
            "*OrderID": self._get_value(row, ["OrderID", "EbayOrderID", "order_id"]),
            "ShippingCarrier": self._map_carrier_ebay(self._get_value(row, ["Carrier", "carrier"])),
            "ShipmentTrackingNumber": self._get_value(row, ["TrackingNumber", "tracking"]),
        }
    
    def _format_for_shopify(self, data: SpreadsheetData, file_type: str) -> SpreadsheetData:
        """Format data for Shopify CSV import."""
        columns = self.SHOPIFY_PRODUCT_COLUMNS
        
        formatted_rows = []
        for row in data.rows:
            formatted_row = self._map_to_shopify_product(row)
            formatted_rows.append(formatted_row)
            self._format_stats["rows_formatted"] += 1
        
        return SpreadsheetData(
            filename=f"shopify_products_{datetime.now().strftime('%Y%m%d')}.csv",
            columns=columns,
            rows=formatted_rows,
            row_count=len(formatted_rows),
        )
    
    def _map_to_shopify_product(self, row: dict) -> dict:
        """Map row to Shopify product format."""
        title = self._get_value(row, ["Title", "Description", "ProductName"])
        
        return {
            "Handle": self._slugify(title),
            "Title": title,
            "Body (HTML)": self._get_value(row, ["Description", "LongDescription"]),
            "Vendor": self._get_value(row, ["Vendor", "Brand", "Manufacturer"]),
            "Type": self._get_value(row, ["Type", "Category"]),
            "Tags": self._get_value(row, ["Tags", "Keywords"]),
            "Published": "TRUE",
            "Variant SKU": self._get_value(row, ["SKU", "sku"]),
            "Variant Inventory Qty": self._get_value(row, ["Quantity", "Stock"]),
            "Variant Price": self._format_price(self._get_value(row, ["Price", "SalesPrice"])),
            "Status": "active",
        }
    
    # Helper methods
    
    def _get_value(self, row: dict, keys: list[str], default: str = "") -> str:
        """Get value from row using multiple possible keys."""
        for key in keys:
            if key in row and row[key]:
                return str(row[key])
        return default
    
    def _format_price(self, value: str) -> str:
        """Format price value."""
        try:
            price = float(value.replace("£", "").replace("$", "").replace(",", ""))
            return f"{price:.2f}"
        except (ValueError, AttributeError):
            return "0.00"
    
    def _format_date(self, value: str) -> str:
        """Format date to YYYY-MM-DD."""
        if not value:
            return datetime.now().strftime("%Y-%m-%d")
        
        # Try to parse and reformat
        try:
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        except Exception:
            pass
        
        return value
    
    def _slugify(self, text: str) -> str:
        """Create URL-safe slug from text."""
        import re
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text)
        return text[:50]
    
    def _map_carrier_amazon(self, carrier: str) -> str:
        """Map carrier name to Amazon carrier code."""
        carrier_map = {
            "fedex": "FedEx",
            "ups": "UPS",
            "usps": "USPS",
            "dhl": "DHL",
            "royal mail": "Royal Mail",
            "parcelforce": "Parcelforce",
        }
        return carrier_map.get(carrier.lower(), "Other")
    
    def _map_carrier_ebay(self, carrier: str) -> str:
        """Map carrier name to eBay carrier code."""
        carrier_map = {
            "fedex": "FedEx",
            "ups": "UPS",
            "usps": "USPS",
            "dhl": "DHL",
            "royal mail": "Royal Mail",
            "parcelforce": "Parcelforce Worldwide",
        }
        return carrier_map.get(carrier.lower(), "Other")
    
    def to_csv(self, data: SpreadsheetData) -> str:
        """Convert spreadsheet data to CSV string."""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data.columns)
        writer.writeheader()
        writer.writerows(data.rows)
        return output.getvalue()
    
    def save_csv(self, data: SpreadsheetData, path: Path) -> str:
        """Save spreadsheet data to CSV file."""
        filepath = path / data.filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.columns)
            writer.writeheader()
            writer.writerows(data.rows)
        
        self._format_stats["files_created"] += 1
        logger.info(f"Saved: {filepath}")
        
        return str(filepath)
    
    def get_stats(self) -> dict:
        """Get formatting statistics."""
        return self._format_stats.copy()

