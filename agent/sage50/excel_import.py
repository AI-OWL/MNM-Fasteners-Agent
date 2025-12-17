"""
Excel Import via Sage SDK.
Reads an Excel file of orders and imports them into Sage 50 automatically.

This is the KEY test for M&M 2.0 - proving we can take an Excel/CSV
of orders and automatically create them in Sage.

Usage:
    python -m agent.sage50.excel_import sample_orders.xlsx
    python -m agent.sage50.excel_import --test  # Use sample data
"""

import click
from pathlib import Path
from datetime import datetime
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

# Try to import pandas/openpyxl for Excel reading
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

import csv
from agent.config import init_config
from agent.models import Order, OrderLine, Platform
from agent.sage50.sdk_operations import SageSDK, SageSDKError, HAS_COM


def create_sample_excel():
    """Create a sample Excel file for testing."""
    sample_path = Path("sample_orders.xlsx")
    
    if HAS_PANDAS:
        # Create with pandas
        data = {
            "Platform": ["Amazon", "eBay", "Amazon", "Shopify", "eBay"],
            "Order ID": ["111-2222222-3333333", "12-34567-89012", "111-3333333-4444444", "#1001", "12-55555-66666"],
            "Order Date": ["2024-01-15", "2024-01-15", "2024-01-14", "2024-01-14", "2024-01-13"],
            "Customer Name": ["John Smith", "Jane Doe", "Bob Wilson", "Alice Brown", "Charlie Davis"],
            "Email": ["john@email.com", "jane@email.com", "bob@email.com", "alice@email.com", "charlie@email.com"],
            "Address 1": ["123 Main Street", "456 Oak Avenue", "789 High Street", "321 Pine Road", "654 Elm Lane"],
            "City": ["London", "Manchester", "Birmingham", "Leeds", "Liverpool"],
            "Postcode": ["SW1A 1AA", "M1 1AA", "B1 1AA", "LS1 1AA", "L1 1AA"],
            "SKU": ["BOLT-M8X50", "NUT-M8", "SCREW-M6X25", "WASHER-M10", "BOLT-M10X60"],
            "Description": ["M8 x 50mm Hex Bolt", "M8 Hex Nut", "M6 x 25mm Screw", "M10 Flat Washer", "M10 x 60mm Bolt"],
            "Quantity": [100, 200, 50, 500, 75],
            "Unit Price": [0.15, 0.05, 0.08, 0.02, 0.25],
            "Shipping": [5.99, 3.99, 4.99, 2.99, 6.99],
            "Total": [20.99, 13.99, 8.99, 12.99, 25.74],
        }
        
        df = pd.DataFrame(data)
        df.to_excel(sample_path, index=False)
        console.print(f"[green]✅ Created: {sample_path}[/green]")
    else:
        # Create as CSV instead
        sample_path = Path("sample_orders.csv")
        with open(sample_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Platform", "Order ID", "Order Date", "Customer Name", "Email", 
                           "Address 1", "City", "Postcode", "SKU", "Description", 
                           "Quantity", "Unit Price", "Shipping", "Total"])
            writer.writerow(["Amazon", "111-2222222-3333333", "2024-01-15", "John Smith", "john@email.com",
                           "123 Main Street", "London", "SW1A 1AA", "BOLT-M8X50", "M8 x 50mm Hex Bolt",
                           100, 0.15, 5.99, 20.99])
            writer.writerow(["eBay", "12-34567-89012", "2024-01-15", "Jane Doe", "jane@email.com",
                           "456 Oak Avenue", "Manchester", "M1 1AA", "NUT-M8", "M8 Hex Nut",
                           200, 0.05, 3.99, 13.99])
            writer.writerow(["Amazon", "111-3333333-4444444", "2024-01-14", "Bob Wilson", "bob@email.com",
                           "789 High Street", "Birmingham", "B1 1AA", "SCREW-M6X25", "M6 x 25mm Screw",
                           50, 0.08, 4.99, 8.99])
        console.print(f"[green]✅ Created: {sample_path}[/green]")
    
    return sample_path


def read_orders_from_file(filepath: Path) -> list[Order]:
    """Read orders from Excel or CSV file."""
    orders = []
    
    if filepath.suffix.lower() in ['.xlsx', '.xls']:
        if not HAS_PANDAS:
            raise Exception("pandas required for Excel files. Install: pip install pandas openpyxl")
        df = pd.read_excel(filepath)
    else:
        if HAS_PANDAS:
            df = pd.read_csv(filepath)
        else:
            # Read with csv module
            with open(filepath, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            # Convert to orders
            for row in rows:
                order = parse_row_to_order(row)
                orders.append(order)
            return orders
    
    # Convert DataFrame to orders
    for _, row in df.iterrows():
        order = parse_row_to_order(row.to_dict())
        orders.append(order)
    
    return orders


def parse_row_to_order(row: dict) -> Order:
    """
    Parse a row dict into an Order object.
    
    Supports two formats:
    1. Amazon Invoice Report format (actual production data)
    2. Simple test format (for sample data)
    """
    
    # === DETECT FORMAT ===
    # Amazon invoice format uses "E-Commerce Order#" and "Customer ID"
    # Simple format uses "Platform" and "Order ID"
    is_amazon_format = 'E-Commerce Order#' in row or 'Customer ID' in row
    
    if is_amazon_format:
        return parse_amazon_invoice_row(row)
    else:
        return parse_simple_row(row)


def parse_amazon_invoice_row(row: dict) -> Order:
    """
    Parse Amazon Invoice Report format.
    
    Columns:
    - Date of Order, E-Commerce Order#, Sales Order#, Ship Date, Amount, Qty
    - Unit of Measure, Unit Price, Item ID, Sales Order Proposal, Customer ID
    - Ship to Name, Address Line 1, Address Line 2, City, State, Zipcode
    - Receivable amount, # of Line Items Ordered, GL Amount, Tax Type
    - U/M Stocking, Account Receivable Amount, Sales Tax ID, Description
    - Ship Via, Customer Phone #
    """
    
    # Determine platform from Customer ID column (e.g. "Amazon", "eBay")
    customer_id = str(row.get('Customer ID', '')).lower()
    ecommerce_order = str(row.get('E-Commerce Order#', ''))
    
    if 'amazon' in customer_id:
        source_platform = Platform.AMAZON
        amazon_id = ecommerce_order
        ebay_id = None
        shopify_id = None
    elif 'ebay' in customer_id:
        source_platform = Platform.EBAY
        amazon_id = None
        ebay_id = ecommerce_order
        shopify_id = None
    elif 'shopify' in customer_id:
        source_platform = Platform.SHOPIFY
        amazon_id = None
        ebay_id = None
        shopify_id = ecommerce_order
    else:
        source_platform = Platform.SAGE_QUANTUM
        amazon_id = ebay_id = shopify_id = None
    
    # Parse order date (format: MM/DD/YYYY or YYYY-MM-DD)
    date_str = row.get('Date of Order', '')
    order_date = parse_date(date_str)
    
    # Parse ship date if present
    ship_date_str = row.get('Ship Date', '')
    ship_date = parse_date(ship_date_str) if ship_date_str else None
    
    # Get Sage Sales Order # if already assigned
    sage_order_ref = str(row.get('Sales Order#', '')) if row.get('Sales Order#') else None
    
    # Parse amounts
    total_amount = safe_float(row.get('Amount', 0))
    unit_price = safe_float(row.get('Unit Price', 0))
    quantity = safe_int(row.get('Qty', 1))
    
    # Build address (handle potential NaN values)
    address_1 = safe_str(row.get('Address Line 1', ''))
    address_2 = safe_str(row.get('Address Line 2', ''))
    city = safe_str(row.get('City', ''))
    state = safe_str(row.get('State', ''))
    zipcode = safe_str(row.get('Zipcode', ''))
    
    # Create order
    order = Order(
        amazon_order_id=amazon_id,
        ebay_order_id=ebay_id,
        shopify_order_id=shopify_id,
        sage_order_ref=sage_order_ref,
        order_date=order_date,
        ship_date=ship_date,
        customer_name=safe_str(row.get('Ship to Name', '')),
        customer_phone=safe_str(row.get('Customer Phone #', '')),
        ship_address_1=address_1,
        ship_address_2=address_2,
        ship_city=city,
        ship_state=state,
        ship_postcode=zipcode,
        ship_method=safe_str(row.get('Ship Via', '')),
        total=total_amount,
        source_platform=source_platform,
        lines=[
            OrderLine(
                sku=safe_str(row.get('Item ID', '')),
                description=safe_str(row.get('Description', '')),
                quantity=quantity,
                unit_price=unit_price,
                unit_of_measure=safe_str(row.get('Unit of Measure', 'each')),
            )
        ]
    )
    
    return order


def parse_simple_row(row: dict) -> Order:
    """Parse simple test format (Platform, Order ID, etc.)."""
    
    # Determine platform
    platform_str = str(row.get('Platform', '')).lower()
    if 'amazon' in platform_str:
        source_platform = Platform.AMAZON
        amazon_id = row.get('Order ID', '')
        ebay_id = None
        shopify_id = None
    elif 'ebay' in platform_str:
        source_platform = Platform.EBAY
        amazon_id = None
        ebay_id = row.get('Order ID', '')
        shopify_id = None
    elif 'shopify' in platform_str:
        source_platform = Platform.SHOPIFY
        amazon_id = None
        ebay_id = None
        shopify_id = row.get('Order ID', '')
    else:
        source_platform = Platform.SAGE_QUANTUM
        amazon_id = ebay_id = shopify_id = None
    
    # Parse date
    order_date = parse_date(row.get('Order Date', ''))
    
    # Create order
    order = Order(
        amazon_order_id=amazon_id,
        ebay_order_id=ebay_id,
        shopify_order_id=shopify_id,
        order_date=order_date,
        customer_name=str(row.get('Customer Name', '')),
        customer_email=str(row.get('Email', '')),
        ship_address_1=str(row.get('Address 1', '')),
        ship_city=str(row.get('City', '')),
        ship_postcode=str(row.get('Postcode', '')),
        shipping_cost=safe_float(row.get('Shipping', 0)),
        total=safe_float(row.get('Total', 0)),
        source_platform=source_platform,
        lines=[
            OrderLine(
                sku=str(row.get('SKU', '')),
                description=str(row.get('Description', '')),
                quantity=safe_int(row.get('Quantity', 1)),
                unit_price=safe_float(row.get('Unit Price', 0)),
            )
        ]
    )
    
    return order


def parse_date(date_str) -> datetime:
    """Parse date from various formats."""
    if not date_str or (isinstance(date_str, float) and str(date_str) == 'nan'):
        return datetime.now()
    
    if isinstance(date_str, datetime):
        return date_str
    
    date_str = str(date_str).strip()
    
    # Try common formats
    formats = [
        '%m/%d/%Y',      # 12/15/2025
        '%Y-%m-%d',      # 2025-12-15
        '%d/%m/%Y',      # 15/12/2025
        '%Y/%m/%d',      # 2025/12/15
        '%d-%m-%Y',      # 15-12-2025
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return datetime.now()


def safe_float(value, default=0.0) -> float:
    """Safely convert value to float, handling NaN and None."""
    if value is None:
        return default
    if isinstance(value, float) and str(value) == 'nan':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=1) -> int:
    """Safely convert value to int, handling NaN and None."""
    if value is None:
        return default
    if isinstance(value, float) and str(value) == 'nan':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_str(value, default='') -> str:
    """Safely convert value to string, handling NaN and None."""
    if value is None:
        return default
    if isinstance(value, float) and str(value) == 'nan':
        return default
    return str(value).strip()


def import_orders_to_sage(orders: list[Order], dry_run: bool = False) -> dict:
    """
    Import orders into Sage 50 via SDK.
    
    Args:
        orders: List of orders to import
        dry_run: If True, don't actually create orders (just test)
        
    Returns:
        Results dict with success/failure counts
    """
    results = {
        "total": len(orders),
        "success": 0,
        "failed": 0,
        "created_refs": [],
        "errors": [],
    }
    
    if dry_run:
        console.print("[yellow]🔸 DRY RUN - Not actually creating orders[/yellow]\n")
        for order in orders:
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id
            console.print(f"  Would create: {platform_id} - {order.customer_name} - £{order.total:.2f}")
            results["success"] += 1
        return results
    
    # Connect to Sage via SDK
    config = init_config()
    
    if not config.sage50_company_path:
        raise SageSDKError(
            "SAGE_COMPANY_PATH not configured!\n"
            "Run: python find_sage.py\n"
            "Then add the path to config.env"
        )
    
    sdk = SageSDK(config)
    
    try:
        console.print("[yellow]🔌 Connecting to Sage 50...[/yellow]")
        sdk.connect()
        console.print("[green]✅ Connected![/green]\n")
        
        for order in orders:
            platform_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id or "N/A"
            
            try:
                console.print(f"  Creating: {platform_id} - {order.customer_name}...", end=" ")
                
                result = sdk.create_sales_order(order)
                
                if result.get("success"):
                    sage_ref = result.get("sage_order_ref", "?")
                    console.print(f"[green]✅ {sage_ref}[/green]")
                    results["success"] += 1
                    results["created_refs"].append(sage_ref)
                else:
                    console.print(f"[red]❌ Failed[/red]")
                    results["failed"] += 1
                    results["errors"].append({"order": platform_id, "error": "Unknown error"})
                    
            except Exception as e:
                console.print(f"[red]❌ {e}[/red]")
                results["failed"] += 1
                results["errors"].append({"order": platform_id, "error": str(e)})
        
    finally:
        sdk.disconnect()
    
    return results


@click.command()
@click.argument('filepath', required=False, type=click.Path(exists=True))
@click.option('--test', is_flag=True, help='Create and use sample data')
@click.option('--dry-run', is_flag=True, help='Show what would be imported without doing it')
def main(filepath: Optional[str], test: bool, dry_run: bool):
    """
    Import orders from Excel/CSV into Sage 50 via SDK.
    
    Examples:
        python -m agent.sage50.excel_import orders.xlsx
        python -m agent.sage50.excel_import --test
        python -m agent.sage50.excel_import --test --dry-run
    """
    console.print(Panel.fit(
        "[bold blue]Sage 50 Excel Import Test[/bold blue]\n"
        "Import orders from Excel directly into Sage via SDK",
        title="📥 M&M 2.0"
    ))
    
    # Check requirements
    if not HAS_COM:
        console.print("[red]❌ pywin32 not installed![/red]")
        console.print("Run: pip install pywin32")
        return
    
    # Get or create file
    if test:
        console.print("\n[yellow]📝 Creating sample order file...[/yellow]")
        filepath = create_sample_excel()
    elif not filepath:
        console.print("\n[red]Please specify a file or use --test[/red]")
        console.print("Usage:")
        console.print("  python -m agent.sage50.excel_import orders.xlsx")
        console.print("  python -m agent.sage50.excel_import --test")
        return
    else:
        filepath = Path(filepath)
    
    # Read orders
    console.print(f"\n[yellow]📖 Reading orders from: {filepath}[/yellow]")
    orders = read_orders_from_file(Path(filepath))
    
    console.print(f"[green]Found {len(orders)} orders[/green]\n")
    
    # Show preview
    table = Table(title="Orders to Import")
    table.add_column("Platform")
    table.add_column("Order ID")
    table.add_column("Customer")
    table.add_column("Total")
    
    for order in orders:
        platform = str(order.source_platform).replace("Platform.", "")
        order_id = order.amazon_order_id or order.ebay_order_id or order.shopify_order_id or "-"
        table.add_row(
            platform,
            str(order_id)[:20],
            order.customer_name[:20],
            f"£{order.total:.2f}"
        )
    
    console.print(table)
    
    # Confirm import
    if not dry_run:
        if not click.confirm("\n🚀 Import these orders into Sage 50?"):
            console.print("[yellow]Cancelled[/yellow]")
            return
    
    # Do the import
    console.print("\n[bold]Importing orders...[/bold]\n")
    
    try:
        results = import_orders_to_sage(orders, dry_run=dry_run)
        
        # Show results
        console.print("\n" + "="*50)
        console.print(Panel.fit(
            f"[green]✅ Success: {results['success']}[/green]\n"
            f"[red]❌ Failed: {results['failed']}[/red]\n"
            f"[cyan]📋 Total: {results['total']}[/cyan]",
            title="Results"
        ))
        
        if results["created_refs"]:
            console.print("\n[green]Created Sage Orders:[/green]")
            for ref in results["created_refs"]:
                console.print(f"  • {ref}")
        
        if results["errors"]:
            console.print("\n[red]Errors:[/red]")
            for err in results["errors"]:
                console.print(f"  • {err['order']}: {err['error']}")
                
    except SageSDKError as e:
        console.print(f"\n[red]❌ Sage SDK Error:[/red]")
        console.print(f"   {e}")
        console.print("\n[yellow]💡 Make sure:[/yellow]")
        console.print("   1. SAGE_COMPANY_PATH is set in config.env")
        console.print("   2. Sage 50 is closed (or same Windows user)")
        console.print("   3. pywin32 is installed: pip install pywin32")


if __name__ == "__main__":
    main()

