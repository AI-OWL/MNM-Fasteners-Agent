#!/usr/bin/env python
"""
Test script to fetch real orders from the backend API and optionally import to Sage.

Usage:
    python test_api_import.py --platform ebay                    # Fetch eBay orders
    python test_api_import.py --platform amazon                  # Fetch Amazon orders
    python test_api_import.py --platform shopify                 # Fetch Shopify orders
    python test_api_import.py --platform ebay --days 7           # Last 7 days
    python test_api_import.py --platform ebay --import-to-sage   # Fetch and import
    python test_api_import.py --platform ebay --fetch-only       # Just download (default)
"""

import argparse
import requests
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")

# Railway API Base URL
API_URL = "https://web-production-a7918.up.railway.app"


def fetch_orders(platform: str, days_back: int = 30) -> Path | None:
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
        logger.error(f"Invalid platform: {platform}. Must be 'amazon', 'shopify', or 'ebay'")
        return None
    
    endpoint = f"{API_URL}/api/generate/{platform}"
    logger.info(f"Fetching {platform.upper()} orders from {endpoint}...")
    logger.info(f"Looking back {days_back} days...")
    
    try:
        response = requests.post(
            endpoint,
            params={"days_back": days_back},
            timeout=120  # 2 minute timeout for large orders
        )
        
        if response.status_code == 200:
            # Create output directory
            output_dir = Path("./sales_orders")
            output_dir.mkdir(exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{platform}_orders_{timestamp}.xlsx"
            filepath = output_dir / filename
            
            # Save file
            with open(filepath, "wb") as f:
                f.write(response.content)
            
            # Log stats from headers
            orders_processed = response.headers.get("X-Orders-Processed", "unknown")
            file_size = len(response.content)
            
            logger.info(f"✅ SUCCESS!")
            logger.info(f"   Platform: {platform.upper()}")
            logger.info(f"   Orders: {orders_processed}")
            logger.info(f"   File: {filepath}")
            logger.info(f"   Size: {file_size:,} bytes")
            
            return filepath
            
        elif response.status_code == 404:
            logger.info(f"ℹ️ No unshipped orders found for {platform.upper()}")
            return None
            
        else:
            try:
                error = response.json().get("detail", "Unknown error")
            except:
                error = response.text[:500]
            logger.error(f"❌ Error fetching {platform.upper()} orders: {error}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error(f"❌ Request timed out (120s)")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Connection error: {e}")
        return None


def import_to_sage(filepath: Path) -> dict:
    """Import the Excel file to Sage 50."""
    logger.info(f"Importing {filepath} to Sage 50...")
    
    try:
        from agent.config import init_config
        from agent.sage50.sdk_operations import SageSDK
        
        config = init_config()
        sdk = SageSDK(config)
        
        logger.info("Connecting to Sage 50...")
        sdk.connect()
        logger.info(f"Connected to: {sdk._company_name}")
        
        result = sdk.import_orders_from_excel(str(filepath))
        
        sdk.disconnect()
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to import: {e}")
        return {"success": False, "error": str(e)}


def preview_excel(filepath: Path, max_rows: int = 10):
    """Preview the Excel file contents."""
    try:
        import pandas as pd
        
        df = pd.read_excel(filepath)
        
        print("\n" + "=" * 60)
        print("EXCEL FILE PREVIEW")
        print("=" * 60)
        print(f"File: {filepath}")
        print(f"Total rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        # Count unique orders
        order_cols = ['E-Commerce Order#', 'Order #', 'Order ID', 'Order Number']
        for col in order_cols:
            if col in df.columns:
                unique_orders = df[col].nunique()
                print(f"Unique orders ({col}): {unique_orders}")
                break
        
        print(f"\nFirst {min(max_rows, len(df))} rows:")
        print(df.head(max_rows).to_string())
        print("=" * 60 + "\n")
        
    except Exception as e:
        logger.error(f"Failed to preview Excel: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch orders from backend API and optionally import to Sage")
    parser.add_argument("--platform", "-p", required=True, choices=["amazon", "shopify", "ebay"],
                        help="Platform to fetch orders from")
    parser.add_argument("--days", "-d", type=int, default=30,
                        help="Days to look back (default: 30)")
    parser.add_argument("--import-to-sage", "-i", action="store_true",
                        help="Import the fetched orders to Sage 50")
    parser.add_argument("--preview", action="store_true", default=True,
                        help="Preview the Excel file contents (default: True)")
    parser.add_argument("--no-preview", action="store_true",
                        help="Skip Excel preview")
    
    args = parser.parse_args()
    
    print("\n" + "=" * 60)
    print(f"FETCHING {args.platform.upper()} ORDERS FROM BACKEND API")
    print("=" * 60 + "\n")
    
    # Fetch orders
    filepath = fetch_orders(args.platform, args.days)
    
    if not filepath:
        print("\n❌ No orders fetched. Exiting.")
        return
    
    # Preview
    if args.preview and not args.no_preview:
        preview_excel(filepath)
    
    # Import to Sage if requested
    if args.import_to_sage:
        print("\n" + "=" * 60)
        print("IMPORTING TO SAGE 50")
        print("=" * 60 + "\n")
        
        result = import_to_sage(filepath)
        
        print("\n" + "=" * 60)
        print("IMPORT RESULT")
        print("=" * 60)
        print(f"Success: {result.get('success', False)}")
        print(f"Imported: {result.get('imported', 0)}")
        print(f"Failed: {result.get('failed', 0)}")
        
        if result.get('errors'):
            print("Errors:")
            for err in result['errors'][:10]:  # Show first 10 errors
                print(f"  - {err}")
        print("=" * 60 + "\n")
    else:
        print(f"\n✅ File saved to: {filepath}")
        print("   Run with --import-to-sage to import to Sage 50")


if __name__ == "__main__":
    main()

