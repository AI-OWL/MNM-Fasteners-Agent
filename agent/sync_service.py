"""
Sync Service for MNM Fasteners Agent.

Fetches sales order Excel files from the Railway API 
and imports them into Sage 50.
"""

import requests
from pathlib import Path
from datetime import datetime
from typing import Optional
from loguru import logger

from agent.config import AgentConfig


# Railway API Base URL
DEFAULT_API_URL = "https://web-production-a7918.up.railway.app"


class SyncService:
    """
    Service to sync orders from platforms (Amazon/Shopify/eBay) to Sage 50.
    
    Flow:
    1. Call Railway API to get Excel file with unshipped orders
    2. Save Excel file locally
    3. Import into Sage 50 using SageSDK
    """
    
    def __init__(self, config: AgentConfig, sage_sdk=None):
        self.config = config
        self.sage_sdk = sage_sdk
        self.api_url = getattr(config, 'api_base_url', None) or DEFAULT_API_URL
        self.output_dir = Path(getattr(config, 'sync_output_dir', './sales_orders'))
        self.output_dir.mkdir(exist_ok=True)
    
    def set_sage_sdk(self, sdk):
        """Set the Sage SDK instance for imports."""
        self.sage_sdk = sdk
    
    def fetch_orders(self, platform: str, days_back: int = 30) -> Optional[Path]:
        """
        Fetch sales orders Excel file from the API.
        
        Args:
            platform: 'amazon', 'shopify', or 'ebay'
            days_back: Number of days to look back
            
        Returns:
            Path to downloaded file, or None if no orders found
        """
        platform = platform.lower()
        if platform not in ['amazon', 'shopify', 'ebay']:
            logger.error(f"Invalid platform: {platform}")
            return None
        
        endpoint = f"{self.api_url}/api/generate/{platform}"
        logger.info(f"Fetching {platform} orders from {endpoint}...")
        
        try:
            response = requests.post(
                endpoint,
                params={"days_back": days_back},
                timeout=120  # 2 minute timeout for large orders
            )
            
            if response.status_code == 200:
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{platform}_sales_orders_{timestamp}.xlsx"
                filepath = self.output_dir / filename
                
                # Save file
                with open(filepath, "wb") as f:
                    f.write(response.content)
                
                # Log stats from headers
                orders_processed = response.headers.get("X-Orders-Processed", "unknown")
                logger.info(f"✅ {platform.upper()}: {orders_processed} orders -> {filepath}")
                
                return filepath
                
            elif response.status_code == 404:
                logger.info(f"ℹ️ {platform.upper()}: No unshipped orders found")
                return None
                
            else:
                try:
                    error = response.json().get("detail", "Unknown error")
                except:
                    error = response.text[:200]
                logger.error(f"❌ {platform.upper()}: Error - {error}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ {platform.upper()}: Request timed out")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ {platform.upper()}: Connection error - {e}")
            return None
    
    def import_to_sage(self, excel_path: Path) -> dict:
        """
        Import an Excel file into Sage 50.
        
        Args:
            excel_path: Path to the Excel file
            
        Returns:
            Dict with import results
        """
        if not self.sage_sdk:
            return {"success": False, "error": "Sage SDK not connected"}
        
        if not excel_path.exists():
            return {"success": False, "error": f"File not found: {excel_path}"}
        
        logger.info(f"Importing {excel_path} to Sage...")
        
        try:
            result = self.sage_sdk.import_orders_from_excel(str(excel_path))
            return result
        except Exception as e:
            logger.error(f"Import failed: {e}")
            return {"success": False, "error": str(e)}
    
    def sync_platform(self, platform: str, days_back: int = 30) -> dict:
        """
        Full sync: fetch orders and import to Sage.
        
        Args:
            platform: 'amazon', 'shopify', or 'ebay'
            days_back: Number of days to look back
            
        Returns:
            Dict with sync results
        """
        result = {
            "platform": platform,
            "success": False,
            "orders_fetched": 0,
            "orders_imported": 0,
            "errors": [],
        }
        
        # Step 1: Fetch from API
        excel_path = self.fetch_orders(platform, days_back)
        
        if not excel_path:
            result["message"] = "No orders to import"
            result["success"] = True  # Not an error if no orders
            return result
        
        result["excel_file"] = str(excel_path)
        
        # Step 2: Import to Sage
        import_result = self.import_to_sage(excel_path)
        
        if import_result.get("success"):
            result["success"] = True
            result["orders_imported"] = import_result.get("imported", 0)
            result["orders_failed"] = import_result.get("failed", 0)
            result["message"] = f"Imported {result['orders_imported']} orders"
            
            if import_result.get("errors"):
                result["errors"] = import_result["errors"]
        else:
            result["success"] = False
            result["error"] = import_result.get("error", "Import failed")
        
        return result
    
    def sync_all_platforms(self, days_back: int = 30) -> dict:
        """
        Sync all platforms: Amazon, Shopify, eBay.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dict with results for each platform
        """
        logger.info("=" * 50)
        logger.info("MNM Fasteners - Full Sync")
        logger.info("=" * 50)
        
        results = {
            "success": True,
            "platforms": {},
            "total_imported": 0,
            "total_failed": 0,
        }
        
        for platform in ["amazon", "shopify", "ebay"]:
            logger.info(f"\n--- Syncing {platform.upper()} ---")
            platform_result = self.sync_platform(platform, days_back)
            results["platforms"][platform] = platform_result
            
            if platform_result.get("success"):
                results["total_imported"] += platform_result.get("orders_imported", 0)
                results["total_failed"] += platform_result.get("orders_failed", 0)
            else:
                results["success"] = False
        
        logger.info("\n" + "=" * 50)
        logger.info("Sync Summary:")
        logger.info(f"  Total imported: {results['total_imported']}")
        logger.info(f"  Total failed: {results['total_failed']}")
        logger.info("=" * 50)
        
        return results


def run_sync(platforms: list[str] = None, days_back: int = 30):
    """
    Standalone function to run sync.
    
    Args:
        platforms: List of platforms to sync, or None for all
        days_back: Number of days to look back
    """
    from agent.config import init_config
    from agent.sage50.sdk_operations import SageSDK
    
    config = init_config()
    sdk = SageSDK(config)
    
    # Connect to Sage
    logger.info("Connecting to Sage 50...")
    if not sdk.connect():
        logger.error("Failed to connect to Sage 50")
        return {"success": False, "error": "Sage connection failed"}
    
    try:
        # Create sync service
        service = SyncService(config, sdk)
        
        if platforms:
            # Sync specific platforms
            results = {"platforms": {}}
            for platform in platforms:
                result = service.sync_platform(platform, days_back)
                results["platforms"][platform] = result
            return results
        else:
            # Sync all
            return service.sync_all_platforms(days_back)
    
    finally:
        sdk.disconnect()


if __name__ == "__main__":
    import sys
    
    # Parse command line args
    platforms = None
    days_back = 30
    
    for arg in sys.argv[1:]:
        if arg.lower() in ['amazon', 'shopify', 'ebay']:
            if platforms is None:
                platforms = []
            platforms.append(arg.lower())
        elif arg.isdigit():
            days_back = int(arg)
    
    # Run sync
    from agent.logging_config import setup_logging
    from agent.config import init_config
    
    config = init_config()
    setup_logging(config, console=True)
    
    result = run_sync(platforms, days_back)
    print(f"\nResult: {result}")

