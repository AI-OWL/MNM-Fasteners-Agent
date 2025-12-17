"""
Data Validator.
Validates data before platform upload.

M&M 2.0 Requirements:
- Check for missing required fields
- Validate formats (dates, prices, SKUs)
- Report errors for manual review
"""

import re
from typing import Any, Optional
from datetime import datetime
from loguru import logger

from agent.models import SpreadsheetData, DataValidationError, Platform


class DataValidator:
    """
    Validates spreadsheet data for platform requirements.
    
    Features:
    - Required field validation
    - Format validation (dates, prices, etc.)
    - Platform-specific rules
    - Error reporting for review
    """
    
    # Required fields by platform
    AMAZON_REQUIRED = ["sku", "quantity"]
    EBAY_REQUIRED = ["Title", "StartPrice", "Quantity"]
    SHOPIFY_REQUIRED = ["Handle", "Title", "Variant SKU"]
    
    def __init__(self):
        self._validation_errors: list[DataValidationError] = []
    
    def validate(
        self,
        data: SpreadsheetData,
        platform: Optional[Platform] = None,
    ) -> tuple[bool, list[DataValidationError]]:
        """
        Validate spreadsheet data.
        
        Args:
            data: Spreadsheet data to validate
            platform: Target platform for platform-specific rules
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        self._validation_errors = []
        
        logger.info(f"Validating {len(data.rows)} rows")
        
        for idx, row in enumerate(data.rows):
            row_num = idx + 2  # Account for header row
            
            # General validations
            self._validate_row_general(row, row_num)
            
            # Platform-specific validations
            if platform == Platform.AMAZON:
                self._validate_amazon(row, row_num)
            elif platform == Platform.EBAY:
                self._validate_ebay(row, row_num)
            elif platform == Platform.SHOPIFY:
                self._validate_shopify(row, row_num)
        
        is_valid = len(self._validation_errors) == 0
        
        # Update data with error count
        data.error_count = len(self._validation_errors)
        data.errors = [e.model_dump() for e in self._validation_errors]
        
        logger.info(f"Validation complete: {len(self._validation_errors)} errors found")
        
        return is_valid, self._validation_errors
    
    def _add_error(
        self,
        row_number: int,
        column: str,
        value: Any,
        error_type: str,
        message: str,
        suggestion: Optional[str] = None,
        auto_fixable: bool = False,
        auto_fix_value: Optional[str] = None,
        original_row: Optional[dict] = None,
    ):
        """Add a validation error."""
        error = DataValidationError(
            row_number=row_number,
            column=column,
            value=str(value) if value else None,
            error_type=error_type,
            message=message,
            suggestion=suggestion,
            auto_fixable=auto_fixable,
            auto_fix_value=auto_fix_value,
            original_row=original_row,
        )
        self._validation_errors.append(error)
    
    def _validate_row_general(self, row: dict, row_num: int):
        """Run general validations on a row."""
        
        # Check for completely empty rows
        if all(not v for v in row.values()):
            self._add_error(
                row_number=row_num,
                column="*",
                value=None,
                error_type="empty_row",
                message="Row is completely empty",
                suggestion="Remove this row or add data",
            )
            return
        
        # Validate price fields
        for key, value in row.items():
            key_lower = key.lower()
            
            if any(term in key_lower for term in ["price", "cost", "total"]):
                if value and not self._is_valid_price(value):
                    self._add_error(
                        row_number=row_num,
                        column=key,
                        value=value,
                        error_type="invalid_price",
                        message=f"Invalid price format: {value}",
                        suggestion="Use numeric format like 19.99",
                        auto_fixable=True,
                        auto_fix_value=self._fix_price(value),
                    )
            
            # Validate quantity fields
            if any(term in key_lower for term in ["qty", "quantity", "stock"]):
                if value and not self._is_valid_quantity(value):
                    self._add_error(
                        row_number=row_num,
                        column=key,
                        value=value,
                        error_type="invalid_quantity",
                        message=f"Invalid quantity: {value}",
                        suggestion="Use whole number",
                    )
            
            # Validate date fields
            if any(term in key_lower for term in ["date", "created", "updated"]):
                if value and not self._is_valid_date(value):
                    self._add_error(
                        row_number=row_num,
                        column=key,
                        value=value,
                        error_type="invalid_date",
                        message=f"Invalid date format: {value}",
                        suggestion="Use YYYY-MM-DD format",
                    )
            
            # Validate email fields
            if "email" in key_lower:
                if value and not self._is_valid_email(value):
                    self._add_error(
                        row_number=row_num,
                        column=key,
                        value=value,
                        error_type="invalid_email",
                        message=f"Invalid email: {value}",
                    )
    
    def _validate_amazon(self, row: dict, row_num: int):
        """Amazon-specific validation rules."""
        # Check required fields
        for field in self.AMAZON_REQUIRED:
            if not self._get_value(row, [field, field.upper(), field.lower()]):
                self._add_error(
                    row_number=row_num,
                    column=field,
                    value=None,
                    error_type="missing_required",
                    message=f"Missing required field for Amazon: {field}",
                    original_row=row,
                )
        
        # SKU length check (Amazon max 40 chars)
        sku = self._get_value(row, ["sku", "SKU"])
        if sku and len(sku) > 40:
            self._add_error(
                row_number=row_num,
                column="sku",
                value=sku,
                error_type="too_long",
                message=f"SKU exceeds Amazon's 40 character limit ({len(sku)} chars)",
                suggestion="Shorten SKU to 40 characters",
            )
        
        # ASIN validation (if present)
        asin = self._get_value(row, ["ASIN", "asin"])
        if asin and not re.match(r'^B[0-9A-Z]{9}$', asin):
            self._add_error(
                row_number=row_num,
                column="ASIN",
                value=asin,
                error_type="invalid_format",
                message=f"Invalid ASIN format: {asin}",
                suggestion="ASIN should start with B followed by 9 alphanumeric characters",
            )
    
    def _validate_ebay(self, row: dict, row_num: int):
        """eBay-specific validation rules."""
        # Check required fields
        for field in self.EBAY_REQUIRED:
            if not self._get_value(row, [field, field.lower()]):
                self._add_error(
                    row_number=row_num,
                    column=field,
                    value=None,
                    error_type="missing_required",
                    message=f"Missing required field for eBay: {field}",
                    original_row=row,
                )
        
        # Title length check (eBay max 80 chars)
        title = self._get_value(row, ["Title", "title"])
        if title and len(title) > 80:
            self._add_error(
                row_number=row_num,
                column="Title",
                value=title[:50] + "...",
                error_type="too_long",
                message=f"Title exceeds eBay's 80 character limit ({len(title)} chars)",
                suggestion="Shorten title to 80 characters",
                auto_fixable=True,
                auto_fix_value=title[:80],
            )
    
    def _validate_shopify(self, row: dict, row_num: int):
        """Shopify-specific validation rules."""
        # Check required fields
        for field in self.SHOPIFY_REQUIRED:
            if not self._get_value(row, [field, field.lower().replace(" ", "_")]):
                self._add_error(
                    row_number=row_num,
                    column=field,
                    value=None,
                    error_type="missing_required",
                    message=f"Missing required field for Shopify: {field}",
                    original_row=row,
                )
        
        # Handle validation (URL-safe)
        handle = self._get_value(row, ["Handle", "handle"])
        if handle and not re.match(r'^[a-z0-9-]+$', handle):
            self._add_error(
                row_number=row_num,
                column="Handle",
                value=handle,
                error_type="invalid_format",
                message="Handle must be lowercase alphanumeric with hyphens",
                suggestion="Use lowercase letters, numbers, and hyphens only",
                auto_fixable=True,
                auto_fix_value=self._slugify(handle),
            )
    
    # Helper methods
    
    def _get_value(self, row: dict, keys: list[str]) -> Optional[str]:
        """Get value from row using multiple possible keys."""
        for key in keys:
            if key in row and row[key]:
                return str(row[key])
        return None
    
    def _is_valid_price(self, value: str) -> bool:
        """Check if value is a valid price."""
        try:
            cleaned = re.sub(r'[£$€,\s]', '', str(value))
            float(cleaned)
            return True
        except (ValueError, TypeError):
            return False
    
    def _fix_price(self, value: str) -> str:
        """Attempt to fix a price value."""
        try:
            cleaned = re.sub(r'[£$€,\s]', '', str(value))
            return f"{float(cleaned):.2f}"
        except (ValueError, TypeError):
            return "0.00"
    
    def _is_valid_quantity(self, value: str) -> bool:
        """Check if value is a valid quantity."""
        try:
            cleaned = re.sub(r'[,\s]', '', str(value))
            int(float(cleaned))
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_valid_date(self, value: str) -> bool:
        """Check if value is a valid date."""
        formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"]
        for fmt in formats:
            try:
                datetime.strptime(str(value).strip(), fmt)
                return True
            except ValueError:
                continue
        return False
    
    def _is_valid_email(self, value: str) -> bool:
        """Check if value is a valid email."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(value).strip()))
    
    def _slugify(self, text: str) -> str:
        """Create URL-safe slug from text."""
        text = str(text).lower()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text)
        return text
    
    def get_errors_for_review(self) -> list[DataValidationError]:
        """Get errors that need manual review (not auto-fixable)."""
        return [e for e in self._validation_errors if not e.auto_fixable]
    
    def get_auto_fixable_errors(self) -> list[DataValidationError]:
        """Get errors that can be auto-fixed."""
        return [e for e in self._validation_errors if e.auto_fixable]
    
    def get_error_summary(self) -> dict:
        """Get summary of validation errors."""
        error_counts = {}
        for error in self._validation_errors:
            error_counts[error.error_type] = error_counts.get(error.error_type, 0) + 1
        
        return {
            "total_errors": len(self._validation_errors),
            "errors_by_type": error_counts,
            "auto_fixable": len(self.get_auto_fixable_errors()),
            "needs_review": len(self.get_errors_for_review()),
        }

