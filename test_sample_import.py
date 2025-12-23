"""
Test script to generate random sample orders and import them into Sage 50.
Uses the same field names as actual reports but with randomized content.
Customer IDs are always Amazon, eBay, or Shopify.
"""

import random
import string
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from loguru import logger

# Sample data for randomization
FIRST_NAMES = [
    "John", "Jane", "Michael", "Sarah", "David", "Emily", "James", "Emma",
    "Robert", "Olivia", "William", "Sophia", "Thomas", "Isabella", "Daniel",
    "Mia", "Christopher", "Charlotte", "Matthew", "Amelia", "Andrew", "Harper"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd", "Elm Blvd",
    "Washington Ave", "Park Place", "Lake View Dr", "Forest Rd", "River St",
    "Highland Ave", "Sunset Blvd", "Spring St", "Valley Rd", "Mountain View"
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "Seattle", "Denver", "Boston"
]

STATES = [
    "NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"
]

# Sample products (fasteners theme)
PRODUCTS = [
    {"sku": "BOLT-HEX-14", "desc": "Hex Bolt 1/4-20 x 1\"", "price": 0.25},
    {"sku": "BOLT-HEX-38", "desc": "Hex Bolt 3/8-16 x 1.5\"", "price": 0.45},
    {"sku": "BOLT-HEX-12", "desc": "Hex Bolt 1/2-13 x 2\"", "price": 0.75},
    {"sku": "NUT-HEX-14", "desc": "Hex Nut 1/4-20", "price": 0.08},
    {"sku": "NUT-HEX-38", "desc": "Hex Nut 3/8-16", "price": 0.12},
    {"sku": "NUT-HEX-12", "desc": "Hex Nut 1/2-13", "price": 0.18},
    {"sku": "WASHER-FL-14", "desc": "Flat Washer 1/4\"", "price": 0.05},
    {"sku": "WASHER-FL-38", "desc": "Flat Washer 3/8\"", "price": 0.07},
    {"sku": "WASHER-LK-14", "desc": "Lock Washer 1/4\"", "price": 0.06},
    {"sku": "SCREW-WD-8", "desc": "Wood Screw #8 x 1\"", "price": 0.10},
    {"sku": "SCREW-WD-10", "desc": "Wood Screw #10 x 1.5\"", "price": 0.12},
    {"sku": "SCREW-SH-14", "desc": "Sheet Metal Screw #14 x 1\"", "price": 0.15},
    {"sku": "ANCHOR-EXP-14", "desc": "Expansion Anchor 1/4\"", "price": 0.35},
    {"sku": "ANCHOR-EXP-38", "desc": "Expansion Anchor 3/8\"", "price": 0.55},
    {"sku": "RIVET-POP-18", "desc": "Pop Rivet 1/8\" Aluminum", "price": 0.08},
    {"sku": "RIVET-POP-316", "desc": "Pop Rivet 3/16\" Steel", "price": 0.12},
]

PLATFORMS = ["Amazon", "eBay", "Shopify"]


def generate_order_id(platform: str) -> str:
    """Generate a realistic order ID for the platform."""
    if platform == "Amazon":
        # Amazon format: 111-1234567-1234567
        return f"{random.randint(100,999)}-{random.randint(1000000,9999999)}-{random.randint(1000000,9999999)}"
    elif platform == "eBay":
        # eBay format: 12-12345-12345
        return f"{random.randint(10,99)}-{random.randint(10000,99999)}-{random.randint(10000,99999)}"
    else:  # Shopify
        # Shopify format: #1234
        return f"#{random.randint(1000, 9999)}"


def generate_phone() -> str:
    """Generate a random US phone number."""
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"


def generate_zipcode() -> str:
    """Generate a random US zipcode."""
    return f"{random.randint(10000, 99999)}"


def generate_sample_orders(num_orders: int = 10) -> pd.DataFrame:
    """
    Generate random sample orders matching the expected Excel format.
    
    Columns match the actual Amazon Invoice Report format:
    - Customer ID (Platform name: Amazon, eBay, Shopify)
    - E-Commerce Order#
    - Date of Order
    - Ship to Name
    - Address Line 1
    - Address Line 2
    - City
    - State
    - Zipcode
    - Customer Phone #
    - Item ID
    - Description
    - Qty
    - Unit Price
    - Receivable amount
    """
    rows = []
    
    for i in range(num_orders):
        # Pick random platform
        platform = random.choice(PLATFORMS)
        order_id = generate_order_id(platform)
        
        # Random customer
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        customer_name = f"{first_name} {last_name}"
        
        # Random address
        street_num = random.randint(100, 9999)
        street = random.choice(STREET_NAMES)
        address1 = f"{street_num} {street}"
        address2 = random.choice(["", "", "", f"Apt {random.randint(1, 999)}", f"Suite {random.randint(100, 999)}"])
        city = random.choice(CITIES)
        state = random.choice(STATES)
        zipcode = generate_zipcode()
        phone = generate_phone()
        
        # Random date within last 7 days
        days_ago = random.randint(0, 7)
        order_date = (datetime.now() - timedelta(days=days_ago)).strftime("%m/%d/%Y")
        
        # Random number of line items (1-3)
        num_items = random.randint(1, 3)
        selected_products = random.sample(PRODUCTS, num_items)
        
        for product in selected_products:
            qty = random.randint(1, 100)
            unit_price = product["price"]
            amount = round(qty * unit_price, 2)
            
            row = {
                "Customer ID": platform,
                "E-Commerce Order#": order_id,
                "Date of Order": order_date,
                "Ship to Name": customer_name,
                "Address Line 1": address1,
                "Address Line 2": address2,
                "City": city,
                "State": state,
                "Zipcode": zipcode,
                "Customer Phone #": phone,
                "Item ID": product["sku"],
                "Description": product["desc"],
                "Qty": qty,
                "Unit Price": unit_price,
                "Receivable amount": amount,
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    return df


def save_sample_excel(df: pd.DataFrame, filename: str = None) -> Path:
    """Save the sample orders to an Excel file."""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sample_orders_{timestamp}.xlsx"
    
    output_dir = Path("./sales_orders")
    output_dir.mkdir(exist_ok=True)
    
    filepath = output_dir / filename
    df.to_excel(filepath, index=False)
    logger.info(f"Saved sample orders to: {filepath}")
    
    return filepath


def import_sample_orders(excel_path: Path) -> dict:
    """Import the sample orders into Sage 50."""
    from agent.config import init_config
    from agent.sage50.sdk_operations import SageSDK, SageSDKError
    
    config = init_config()
    sdk = SageSDK(config)
    
    try:
        logger.info("Connecting to Sage 50...")
        sdk.connect()
        logger.info("Connected to Sage 50!")
        
        result = sdk.import_orders_from_excel(str(excel_path))
        
        return result
        
    except SageSDKError as e:
        logger.error(f"Sage SDK Error: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return {"success": False, "error": str(e)}
    finally:
        sdk.disconnect()


def main():
    """Generate sample orders and import to Sage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate and import sample orders to Sage 50")
    parser.add_argument("--count", "-n", type=int, default=5, help="Number of orders to generate (default: 5)")
    parser.add_argument("--platform", "-p", choices=["amazon", "ebay", "shopify", "mixed"], default="mixed",
                        help="Platform for orders (default: mixed)")
    parser.add_argument("--generate-only", "-g", action="store_true", help="Only generate Excel, don't import")
    parser.add_argument("--import-file", "-f", type=str, help="Import existing Excel file instead of generating")
    
    args = parser.parse_args()
    
    # If importing existing file
    if args.import_file:
        excel_path = Path(args.import_file)
        if not excel_path.exists():
            logger.error(f"File not found: {excel_path}")
            return
        
        logger.info(f"Importing from existing file: {excel_path}")
        result = import_sample_orders(excel_path)
        print("\n" + "="*50)
        print("IMPORT RESULT:")
        print("="*50)
        print(f"Success: {result.get('success', False)}")
        print(f"Imported: {result.get('imported', 0)}")
        print(f"Failed: {result.get('failed', 0)}")
        if result.get('errors'):
            print(f"Errors: {result.get('errors')}")
        return
    
    # Generate sample orders
    logger.info(f"Generating {args.count} sample orders...")
    
    # Override platforms if specified
    global PLATFORMS
    if args.platform != "mixed":
        PLATFORMS = [args.platform.capitalize()]
    
    df = generate_sample_orders(args.count)
    
    # Show preview
    print("\n" + "="*50)
    print("SAMPLE ORDERS PREVIEW:")
    print("="*50)
    print(f"Total rows: {len(df)}")
    print(f"Unique orders: {df['E-Commerce Order#'].nunique()}")
    print(f"Platforms: {df['Customer ID'].unique().tolist()}")
    print("\nFirst few rows:")
    print(df[['Customer ID', 'E-Commerce Order#', 'Ship to Name', 'Item ID', 'Qty', 'Unit Price']].head(10).to_string())
    
    # Save to Excel
    excel_path = save_sample_excel(df)
    print(f"\nSaved to: {excel_path}")
    
    if args.generate_only:
        print("\n--generate-only specified, skipping import.")
        return
    
    # Import to Sage
    print("\n" + "="*50)
    print("IMPORTING TO SAGE 50...")
    print("="*50)
    
    result = import_sample_orders(excel_path)
    
    print("\n" + "="*50)
    print("IMPORT RESULT:")
    print("="*50)
    print(f"Success: {result.get('success', False)}")
    print(f"Imported: {result.get('imported', 0)}")
    print(f"Failed: {result.get('failed', 0)}")
    if result.get('errors'):
        print("Errors:")
        for err in result.get('errors', []):
            print(f"  - {err}")


if __name__ == "__main__":
    main()

