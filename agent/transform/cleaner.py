"""
Data Cleaner.
Cleans and standardizes spreadsheet data.

M&M 2.0 Requirements:
- Fix common errors like duplicates, missing info
- Standardize dates, prices, text formatting
"""

import re
from typing import Any, Optional
from datetime import datetime
from loguru import logger

from agent.models import SpreadsheetData, DataValidationError


class DataCleaner:
    """
    Cleans spreadsheet data for platform uploads.
    
    Operations:
    - Remove duplicates
    - Standardize text (trim, case)
    - Format dates consistently
    - Format prices/numbers
    - Fill missing required fields
    - Remove invalid characters
    """
    
    def __init__(self):
        self._cleaning_stats = {
            "rows_cleaned": 0,
            "duplicates_removed": 0,
            "values_standardized": 0,
            "blanks_filled": 0,
        }
    
    def clean(self, data: SpreadsheetData) -> SpreadsheetData:
        """
        Clean spreadsheet data.
        
        Args:
            data: Raw spreadsheet data
            
        Returns:
            Cleaned spreadsheet data
        """
        logger.info(f"Cleaning {len(data.rows)} rows")
        
        # Reset stats
        self._cleaning_stats = {
            "rows_cleaned": 0,
            "duplicates_removed": 0,
            "values_standardized": 0,
            "blanks_filled": 0,
        }
        
        cleaned_rows = []
        seen_keys = set()
        
        for row in data.rows:
            # Clean each row
            cleaned_row = self._clean_row(row)
            
            # Check for duplicates (using SKU or order ID as key)
            row_key = self._get_row_key(cleaned_row)
            if row_key and row_key in seen_keys:
                self._cleaning_stats["duplicates_removed"] += 1
                continue
            
            if row_key:
                seen_keys.add(row_key)
            
            cleaned_rows.append(cleaned_row)
            self._cleaning_stats["rows_cleaned"] += 1
        
        logger.info(
            f"Cleaning complete: {self._cleaning_stats['rows_cleaned']} rows, "
            f"{self._cleaning_stats['duplicates_removed']} duplicates removed"
        )
        
        return SpreadsheetData(
            filename=data.filename,
            sheet_name=data.sheet_name,
            columns=data.columns,
            rows=cleaned_rows,
            row_count=len(cleaned_rows),
        )
    
    def _clean_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Clean a single row."""
        cleaned = {}
        
        for key, value in row.items():
            cleaned_key = self._clean_column_name(key)
            cleaned_value = self._clean_value(key, value)
            cleaned[cleaned_key] = cleaned_value
        
        return cleaned
    
    def _clean_column_name(self, name: str) -> str:
        """Standardize column names."""
        if not name:
            return name
        
        # Remove extra whitespace
        name = " ".join(name.split())
        
        # Standard mappings
        name_mappings = {
            "sku": "SKU",
            "product code": "SKU",
            "stock code": "SKU",
            "item number": "SKU",
            "qty": "Quantity",
            "quantity": "Quantity",
            "price": "Price",
            "unit price": "Price",
            "description": "Description",
            "product name": "Description",
            "title": "Description",
        }
        
        return name_mappings.get(name.lower(), name)
    
    def _clean_value(self, column: str, value: Any) -> Any:
        """Clean a single value based on column type."""
        if value is None:
            return ""
        
        # Convert to string for processing
        str_value = str(value).strip()
        
        column_lower = column.lower()
        
        # Price columns
        if any(term in column_lower for term in ["price", "cost", "total", "amount"]):
            return self._clean_price(str_value)
        
        # Quantity columns
        if any(term in column_lower for term in ["qty", "quantity", "stock"]):
            return self._clean_quantity(str_value)
        
        # Date columns
        if any(term in column_lower for term in ["date", "time", "created", "updated"]):
            return self._clean_date(str_value)
        
        # SKU columns - uppercase, no special chars
        if any(term in column_lower for term in ["sku", "code", "asin", "item"]):
            return self._clean_sku(str_value)
        
        # Email columns
        if "email" in column_lower:
            return self._clean_email(str_value)
        
        # Phone columns
        if "phone" in column_lower or "tel" in column_lower:
            return self._clean_phone(str_value)
        
        # Postcode/ZIP
        if any(term in column_lower for term in ["postcode", "zip", "postal"]):
            return self._clean_postcode(str_value)
        
        # Default: clean text
        return self._clean_text(str_value)
    
    def _clean_price(self, value: str) -> float:
        """Clean price value."""
        if not value:
            return 0.0
        
        # Remove currency symbols and whitespace
        cleaned = re.sub(r'[£$€¥\s,]', '', value)
        
        try:
            return round(float(cleaned), 2)
        except ValueError:
            return 0.0
    
    def _clean_quantity(self, value: str) -> int:
        """Clean quantity value."""
        if not value:
            return 0
        
        # Remove any non-numeric characters except minus
        cleaned = re.sub(r'[^\d\-]', '', value)
        
        try:
            return int(float(cleaned))
        except ValueError:
            return 0
    
    def _clean_date(self, value: str) -> str:
        """Clean and standardize date value to YYYY-MM-DD format."""
        if not value:
            return ""
        
        # Common date formats to try
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%d %b %Y",
            "%d %B %Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(value.strip(), fmt)
                self._cleaning_stats["values_standardized"] += 1
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # Return original if can't parse
        return value
    
    def _clean_sku(self, value: str) -> str:
        """Clean SKU value."""
        if not value:
            return ""
        
        # Uppercase and remove problematic characters
        cleaned = value.upper().strip()
        cleaned = re.sub(r'[\t\n\r]', '', cleaned)
        
        return cleaned
    
    def _clean_email(self, value: str) -> str:
        """Clean email value."""
        if not value:
            return ""
        
        # Lowercase and strip
        cleaned = value.lower().strip()
        
        # Basic validation
        if "@" not in cleaned or "." not in cleaned:
            return ""
        
        return cleaned
    
    def _clean_phone(self, value: str) -> str:
        """Clean phone number."""
        if not value:
            return ""
        
        # Keep only digits and +
        cleaned = re.sub(r'[^\d+]', '', value)
        
        return cleaned
    
    def _clean_postcode(self, value: str) -> str:
        """Clean postcode/ZIP."""
        if not value:
            return ""
        
        # Uppercase and standardize spacing for UK postcodes
        cleaned = value.upper().strip()
        
        # UK postcode format: add space if missing
        if re.match(r'^[A-Z]{1,2}\d{1,2}[A-Z]?\d[A-Z]{2}$', cleaned):
            # Insert space before last 3 characters
            cleaned = cleaned[:-3] + " " + cleaned[-3:]
        
        return cleaned
    
    def _clean_text(self, value: str) -> str:
        """Clean general text value."""
        if not value:
            return ""
        
        # Strip whitespace
        cleaned = value.strip()
        
        # Normalize whitespace
        cleaned = " ".join(cleaned.split())
        
        # Remove control characters
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        return cleaned
    
    def _get_row_key(self, row: dict) -> Optional[str]:
        """
        Get a unique key for the row for duplicate detection.
        
        Uses SKU, order ID, or other identifier.
        """
        # Try common identifier columns
        for key in ["SKU", "sku", "OrderID", "order_id", "ASIN", "ItemNumber"]:
            if key in row and row[key]:
                return str(row[key])
        
        return None
    
    def get_stats(self) -> dict:
        """Get cleaning statistics."""
        return self._cleaning_stats.copy()

