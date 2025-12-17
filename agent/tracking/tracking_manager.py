"""
Tracking Manager.
Coordinates tracking lookups across multiple carriers.
"""

from typing import Optional
from datetime import datetime
from loguru import logger

from agent.config import AgentConfig
from agent.models import TrackingInfo, Order
from agent.tracking.carrier_api import (
    CarrierAPI,
    FedExAPI,
    UPSAPI,
    RoyalMailAPI,
    GenericCarrierAPI,
)


class TrackingManager:
    """
    Manages tracking lookups across multiple carriers.
    
    Features:
    - Auto-detect carrier from tracking number format
    - Batch tracking lookups
    - Caching of results
    """
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self._carriers: dict[str, CarrierAPI] = {}
        self._cache: dict[str, TrackingInfo] = {}
        
        self._initialize_carriers()
    
    def _initialize_carriers(self):
        """Initialize carrier APIs from config."""
        # FedEx
        if self.config.fedex_client_id and self.config.fedex_client_secret:
            self._carriers["fedex"] = FedExAPI(
                client_id=self.config.fedex_client_id,
                client_secret=self.config.fedex_client_secret,
                account_number=self.config.fedex_account_number,
            )
            logger.info("FedEx API configured")
        
        # UPS
        if self.config.ups_client_id and self.config.ups_client_secret:
            self._carriers["ups"] = UPSAPI(
                client_id=self.config.ups_client_id,
                client_secret=self.config.ups_client_secret,
            )
            logger.info("UPS API configured")
        
        # Royal Mail
        if self.config.royal_mail_client_id:
            self._carriers["royal_mail"] = RoyalMailAPI(
                client_id=self.config.royal_mail_client_id,
                client_secret=self.config.royal_mail_client_secret,
            )
            logger.info("Royal Mail API configured")
        
        # Generic fallback
        self._carriers["other"] = GenericCarrierAPI()
    
    def detect_carrier(self, tracking_number: str) -> str:
        """
        Detect carrier from tracking number format.
        
        Common formats:
        - FedEx: 12-34 digits, starts with specific prefixes
        - UPS: 1Z followed by 16 chars, or 18 digits
        - USPS: 20-22 digits, or starts with specific prefixes
        - Royal Mail: Various UK formats
        """
        tracking = tracking_number.strip().upper()
        
        # UPS: 1Z + 16 characters
        if tracking.startswith("1Z") and len(tracking) == 18:
            return "ups"
        
        # UPS: 18 digit tracking numbers
        if len(tracking) == 18 and tracking.isdigit():
            return "ups"
        
        # FedEx: 12 or 15 or 20 or 22 digits
        if len(tracking) in [12, 15, 20, 22] and tracking.isdigit():
            return "fedex"
        
        # FedEx Door Tag: DT + 12 digits
        if tracking.startswith("DT") and len(tracking) == 14:
            return "fedex"
        
        # Royal Mail: UK domestic formats
        if len(tracking) in [13, 16] and tracking[:2].isalpha():
            return "royal_mail"
        
        # USPS: Various formats
        if len(tracking) in [20, 22, 26, 34] and tracking.isdigit():
            return "usps"
        
        return "other"
    
    async def get_tracking(
        self,
        tracking_number: str,
        carrier: Optional[str] = None,
        use_cache: bool = True,
    ) -> Optional[TrackingInfo]:
        """
        Get tracking information for a shipment.
        
        Args:
            tracking_number: The tracking number
            carrier: Optional carrier name (auto-detected if not provided)
            use_cache: Whether to use cached results
            
        Returns:
            TrackingInfo or None if not found
        """
        # Check cache
        cache_key = f"{carrier or 'auto'}:{tracking_number}"
        if use_cache and cache_key in self._cache:
            logger.debug(f"Cache hit for {tracking_number}")
            return self._cache[cache_key]
        
        # Detect carrier if not specified
        if not carrier:
            carrier = self.detect_carrier(tracking_number)
            logger.debug(f"Detected carrier: {carrier}")
        
        # Get carrier API
        api = self._carriers.get(carrier, self._carriers.get("other"))
        
        if not api:
            logger.warning(f"No API configured for carrier: {carrier}")
            return None
        
        # Lookup tracking
        try:
            info = await api.get_tracking(tracking_number)
            
            if info:
                # Cache result
                self._cache[cache_key] = info
                logger.info(f"Tracking {tracking_number}: {info.status}")
            
            return info
            
        except Exception as e:
            logger.error(f"Tracking lookup failed for {tracking_number}: {e}")
            return None
    
    async def get_tracking_batch(
        self,
        tracking_numbers: list[tuple[str, Optional[str]]],  # (number, carrier)
    ) -> dict[str, TrackingInfo]:
        """
        Get tracking information for multiple shipments.
        
        Args:
            tracking_numbers: List of (tracking_number, carrier) tuples
            
        Returns:
            Dict mapping tracking numbers to TrackingInfo
        """
        import asyncio
        
        results = {}
        
        # Process in batches to avoid rate limits
        batch_size = 10
        
        for i in range(0, len(tracking_numbers), batch_size):
            batch = tracking_numbers[i:i + batch_size]
            
            tasks = [
                self.get_tracking(num, carrier)
                for num, carrier in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for (num, _), result in zip(batch, batch_results):
                if isinstance(result, TrackingInfo):
                    results[num] = result
                elif isinstance(result, Exception):
                    logger.error(f"Batch tracking error for {num}: {result}")
            
            # Small delay between batches
            if i + batch_size < len(tracking_numbers):
                await asyncio.sleep(0.5)
        
        return results
    
    async def update_order_tracking(self, order: Order) -> Order:
        """
        Update an order with tracking information.
        
        Args:
            order: Order with tracking_number and carrier
            
        Returns:
            Updated order with tracking status
        """
        if not order.tracking_number:
            return order
        
        info = await self.get_tracking(order.tracking_number, order.carrier)
        
        if info:
            order.carrier = info.carrier
            if info.ship_date:
                order.ship_date = info.ship_date
            
            # Update status based on tracking
            if info.status == "delivered":
                order.status = "delivered"
            elif info.status == "in_transit":
                order.status = "shipped"
            elif info.status == "exception":
                order.status = "delivery_exception"
        
        return order
    
    async def update_orders_tracking(self, orders: list[Order]) -> list[Order]:
        """Update tracking for multiple orders."""
        # Collect tracking numbers that need lookup
        tracking_to_lookup = [
            (order.tracking_number, order.carrier)
            for order in orders
            if order.tracking_number
        ]
        
        if not tracking_to_lookup:
            return orders
        
        # Batch lookup
        tracking_results = await self.get_tracking_batch(tracking_to_lookup)
        
        # Update orders
        for order in orders:
            if order.tracking_number and order.tracking_number in tracking_results:
                info = tracking_results[order.tracking_number]
                order.carrier = info.carrier
                if info.ship_date:
                    order.ship_date = info.ship_date
        
        return orders
    
    def clear_cache(self):
        """Clear the tracking cache."""
        self._cache.clear()
        logger.info("Tracking cache cleared")

