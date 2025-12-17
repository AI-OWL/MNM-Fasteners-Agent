"""
Carrier API integrations for tracking information.
Supports FedEx, UPS, USPS, Royal Mail, etc.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import aiohttp
from loguru import logger

from agent.models import TrackingInfo


class CarrierAPI(ABC):
    """Base class for carrier API integrations."""
    
    @abstractmethod
    async def get_tracking(self, tracking_number: str) -> Optional[TrackingInfo]:
        """Get tracking information for a shipment."""
        pass
    
    @abstractmethod
    def get_carrier_name(self) -> str:
        """Get the carrier name."""
        pass


class FedExAPI(CarrierAPI):
    """
    FedEx Track API integration.
    
    Requires FedEx Developer credentials:
    - Client ID
    - Client Secret
    """
    
    AUTH_URL = "https://apis.fedex.com/oauth/token"
    TRACK_URL = "https://apis.fedex.com/track/v1/trackingnumbers"
    
    def __init__(self, client_id: str, client_secret: str, account_number: str = ""):
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_number = account_number
        self._access_token: Optional[str] = None
        self._token_expires: Optional[datetime] = None
    
    def get_carrier_name(self) -> str:
        return "fedex"
    
    async def _get_access_token(self) -> str:
        """Get or refresh OAuth access token."""
        if self._access_token and self._token_expires and datetime.now() < self._token_expires:
            return self._access_token
        
        async with aiohttp.ClientSession() as session:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            
            async with session.post(
                self.AUTH_URL,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self._access_token = result["access_token"]
                    # Token typically expires in 1 hour
                    from datetime import timedelta
                    self._token_expires = datetime.now() + timedelta(seconds=result.get("expires_in", 3600) - 60)
                    return self._access_token
                else:
                    error = await resp.text()
                    raise Exception(f"FedEx auth failed: {error}")
    
    async def get_tracking(self, tracking_number: str) -> Optional[TrackingInfo]:
        """Get tracking information from FedEx."""
        try:
            token = await self._get_access_token()
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "X-locale": "en_US",
                }
                
                payload = {
                    "includeDetailedScans": True,
                    "trackingInfo": [
                        {
                            "trackingNumberInfo": {
                                "trackingNumber": tracking_number
                            }
                        }
                    ]
                }
                
                async with session.post(
                    self.TRACK_URL,
                    json=payload,
                    headers=headers
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_response(tracking_number, data)
                    else:
                        error = await resp.text()
                        logger.warning(f"FedEx tracking failed: {error}")
                        return None
                        
        except Exception as e:
            logger.error(f"FedEx API error: {e}")
            return None
    
    def _parse_response(self, tracking_number: str, data: dict) -> Optional[TrackingInfo]:
        """Parse FedEx API response."""
        try:
            results = data.get("output", {}).get("completeTrackResults", [])
            if not results:
                return None
            
            track_result = results[0].get("trackResults", [{}])[0]
            
            latest_status = track_result.get("latestStatusDetail", {})
            status_code = latest_status.get("code", "")
            
            # Map FedEx status codes
            status_map = {
                "DE": "delivered",
                "IT": "in_transit",
                "PU": "picked_up",
                "OD": "out_for_delivery",
                "EX": "exception",
            }
            
            status = status_map.get(status_code, "unknown")
            
            # Parse dates
            ship_date = None
            delivered_date = None
            
            date_times = track_result.get("dateAndTimes", [])
            for dt in date_times:
                if dt.get("type") == "SHIP":
                    ship_date = datetime.fromisoformat(dt.get("dateTime", "").replace("Z", "+00:00"))
                elif dt.get("type") == "ACTUAL_DELIVERY":
                    delivered_date = datetime.fromisoformat(dt.get("dateTime", "").replace("Z", "+00:00"))
            
            return TrackingInfo(
                tracking_number=tracking_number,
                carrier="fedex",
                status=status,
                status_detail=latest_status.get("description", ""),
                ship_date=ship_date,
                actual_delivery=delivered_date,
                carrier_response=track_result,
            )
            
        except Exception as e:
            logger.error(f"Error parsing FedEx response: {e}")
            return None


class UPSAPI(CarrierAPI):
    """
    UPS Tracking API integration.
    
    Requires UPS Developer credentials:
    - Client ID
    - Client Secret
    """
    
    AUTH_URL = "https://onlinetools.ups.com/security/v1/oauth/token"
    TRACK_URL = "https://onlinetools.ups.com/api/track/v1/details"
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: Optional[str] = None
        self._token_expires: Optional[datetime] = None
    
    def get_carrier_name(self) -> str:
        return "ups"
    
    async def _get_access_token(self) -> str:
        """Get or refresh OAuth access token."""
        if self._access_token and self._token_expires and datetime.now() < self._token_expires:
            return self._access_token
        
        import base64
        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        
        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            }
            
            async with session.post(
                self.AUTH_URL,
                data={"grant_type": "client_credentials"},
                headers=headers
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self._access_token = result["access_token"]
                    from datetime import timedelta
                    self._token_expires = datetime.now() + timedelta(seconds=result.get("expires_in", 3600) - 60)
                    return self._access_token
                else:
                    error = await resp.text()
                    raise Exception(f"UPS auth failed: {error}")
    
    async def get_tracking(self, tracking_number: str) -> Optional[TrackingInfo]:
        """Get tracking information from UPS."""
        try:
            token = await self._get_access_token()
            
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "transId": f"track-{tracking_number}",
                    "transactionSrc": "MNMAgent",
                }
                
                url = f"{self.TRACK_URL}/{tracking_number}"
                
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._parse_response(tracking_number, data)
                    else:
                        error = await resp.text()
                        logger.warning(f"UPS tracking failed: {error}")
                        return None
                        
        except Exception as e:
            logger.error(f"UPS API error: {e}")
            return None
    
    def _parse_response(self, tracking_number: str, data: dict) -> Optional[TrackingInfo]:
        """Parse UPS API response."""
        try:
            shipment = data.get("trackResponse", {}).get("shipment", [{}])[0]
            package = shipment.get("package", [{}])[0]
            
            activity = package.get("activity", [{}])[0]
            status_type = activity.get("status", {}).get("type", "")
            
            # Map UPS status codes
            status_map = {
                "D": "delivered",
                "I": "in_transit",
                "P": "picked_up",
                "M": "manifest",
                "X": "exception",
            }
            
            status = status_map.get(status_type, "unknown")
            
            return TrackingInfo(
                tracking_number=tracking_number,
                carrier="ups",
                status=status,
                status_detail=activity.get("status", {}).get("description", ""),
                carrier_response=data,
            )
            
        except Exception as e:
            logger.error(f"Error parsing UPS response: {e}")
            return None


class RoyalMailAPI(CarrierAPI):
    """Royal Mail Tracking API integration."""
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
    
    def get_carrier_name(self) -> str:
        return "royal_mail"
    
    async def get_tracking(self, tracking_number: str) -> Optional[TrackingInfo]:
        """Get tracking information from Royal Mail."""
        # Royal Mail API implementation would go here
        # For now, return basic info
        return TrackingInfo(
            tracking_number=tracking_number,
            carrier="royal_mail",
            status="unknown",
            status_detail="Royal Mail tracking lookup not implemented",
        )


class GenericCarrierAPI(CarrierAPI):
    """
    Generic carrier that just stores tracking info without API lookup.
    Used when carrier API is not configured.
    """
    
    def __init__(self, carrier_name: str = "other"):
        self._carrier_name = carrier_name
    
    def get_carrier_name(self) -> str:
        return self._carrier_name
    
    async def get_tracking(self, tracking_number: str) -> Optional[TrackingInfo]:
        """Return basic tracking info without API lookup."""
        return TrackingInfo(
            tracking_number=tracking_number,
            carrier=self._carrier_name,
            status="unknown",
            status_detail="No API configured for this carrier",
        )

