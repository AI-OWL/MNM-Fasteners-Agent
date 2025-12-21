"""
Demo/Test mode for M&M 2.0 Agent.
Allows testing agent functionality without connecting to the server.

Usage:
    python -m agent.demo              # Run all demos
    python -m agent.demo --report     # Generate sample report
    python -m agent.demo --format     # Test data formatting
    python -m agent.demo --email      # Test email sending
"""

import asyncio
import click
from datetime import datetime, timedelta
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from agent import __version__
from agent.config import init_config, AgentConfig
from agent.models import (
    Order, OrderLine, Product, Platform, SpreadsheetData,
    SyncReport, DataValidationError, TrackingInfo
)
from agent.transform import DataCleaner, DataFormatter, DataValidator
from agent.reports import ReportGenerator, EmailSender
from agent.sage50.connector import Sage50Connector, HAS_COM, HAS_ODBC

console = Console()


# ===== Sample Data =====

def get_sample_orders() -> list[Order]:
    """Generate sample orders for testing."""
    return [
        Order(
            amazon_order_id="111-2222222-3333333",
            order_date=datetime.now() - timedelta(days=1),
            customer_name="John Smith",
            customer_email="john@example.com",
            ship_name="John Smith",
            ship_address_1="123 Main Street",
            ship_city="London",
            ship_postcode="SW1A 1AA",
            ship_country="GB",
            lines=[
                OrderLine(sku="BOLT-M8X50", description="M8 x 50mm Hex Bolt", quantity=100, unit_price=0.15),
                OrderLine(sku="NUT-M8", description="M8 Hex Nut", quantity=100, unit_price=0.05),
            ],
            subtotal=20.00,
            shipping_cost=5.99,
            tax_total=4.00,
            total=29.99,
            tracking_number="1Z999AA10123456784",
            carrier="ups",
            source_platform=Platform.AMAZON,
        ),
        Order(
            ebay_order_id="12-34567-89012",
            order_date=datetime.now() - timedelta(days=1),
            customer_name="Jane Doe",
            customer_email="jane@example.com",
            ship_name="Jane Doe",
            ship_address_1="456 Oak Avenue",
            ship_city="Manchester",
            ship_postcode="M1 1AA",
            ship_country="GB",
            lines=[
                OrderLine(sku="SCREW-M6X25", description="M6 x 25mm Machine Screw", quantity=50, unit_price=0.08),
            ],
            subtotal=4.00,
            shipping_cost=3.99,
            tax_total=0.80,
            total=8.79,
            tracking_number="794644790132",
            carrier="fedex",
            source_platform=Platform.EBAY,
        ),
        Order(
            shopify_order_id="#1001",
            order_date=datetime.now(),
            customer_name="Bob Wilson",
            customer_email="bob@example.com",
            ship_name="Bob Wilson",
            ship_address_1="789 High Street",
            ship_city="Birmingham",
            ship_postcode="B1 1AA",
            ship_country="GB",
            lines=[
                OrderLine(sku="WASHER-M10", description="M10 Flat Washer", quantity=200, unit_price=0.02),
                OrderLine(sku="BOLT-M10X60", description="M10 x 60mm Hex Bolt", quantity=50, unit_price=0.25),
            ],
            subtotal=16.50,
            shipping_cost=4.99,
            tax_total=3.30,
            total=24.79,
            source_platform=Platform.SHOPIFY,
        ),
    ]


def get_sample_spreadsheet() -> SpreadsheetData:
    """Generate sample spreadsheet data with some errors for testing."""
    return SpreadsheetData(
        filename="sample_products.csv",
        columns=["SKU", "Description", "Price", "Quantity", "Date Added"],
        rows=[
            {"SKU": "BOLT-M8X50", "Description": "M8 x 50mm Hex Bolt", "Price": "0.15", "Quantity": "100", "Date Added": "2024-01-15"},
            {"SKU": "nut-m8", "Description": "M8 Hex Nut", "Price": "£0.05", "Quantity": "200", "Date Added": "15/01/2024"},
            {"SKU": "SCREW-M6X25", "Description": "M6 x 25mm Machine Screw", "Price": "invalid", "Quantity": "50", "Date Added": "2024-01-16"},
            {"SKU": "", "Description": "Missing SKU Product", "Price": "1.00", "Quantity": "10", "Date Added": "2024-01-17"},
            {"SKU": "WASHER-M10", "Description": "M10 Flat Washer" * 10, "Price": "0.02", "Quantity": "five hundred", "Date Added": ""},
            {"SKU": "BOLT-M8X50", "Description": "Duplicate SKU", "Price": "0.20", "Quantity": "50", "Date Added": "2024-01-18"},
        ],
        row_count=6,
    )


def get_sample_sync_results() -> dict:
    """Generate sample sync results for report testing."""
    return {
        "started_at": datetime.now() - timedelta(minutes=15),
        "platforms": ["amazon", "ebay", "shopify"],
        "amazon": {
            "orders": 18,
            "success": 17,
            "failed": 1,
        },
        "ebay": {
            "orders": 10,
            "success": 10,
            "failed": 0,
        },
        "shopify": {
            "orders": 5,
            "success": 5,
            "failed": 0,
        },
        "sage": {
            "imported": 33,
            "exported": 15,
        },
        "tracking_updated": 22,
        "errors": [
            {
                "row_number": 5,
                "column": "CustomerRef",
                "error_type": "missing_required",
                "message": "Missing required field: CustomerRef",
            }
        ]
    }


# ===== Demo Functions =====

def demo_data_cleaning():
    """Demonstrate data cleaning functionality."""
    console.print(Panel.fit("[bold blue]Data Cleaning Demo[/bold blue]"))
    
    # Get sample data with issues
    data = get_sample_spreadsheet()
    
    console.print(f"\n[yellow]Original Data ({len(data.rows)} rows):[/yellow]")
    table = Table()
    for col in data.columns:
        table.add_column(col)
    for row in data.rows:
        table.add_row(*[str(row.get(col, ""))[:30] for col in data.columns])
    console.print(table)
    
    # Clean the data
    cleaner = DataCleaner()
    cleaned = cleaner.clean(data)
    
    console.print(f"\n[green]Cleaned Data ({len(cleaned.rows)} rows):[/green]")
    table = Table()
    for col in cleaned.columns:
        table.add_column(col)
    for row in cleaned.rows:
        table.add_row(*[str(row.get(col, ""))[:30] for col in cleaned.columns])
    console.print(table)
    
    stats = cleaner.get_stats()
    console.print(f"\n[cyan]Cleaning Stats:[/cyan]")
    console.print(f"  • Rows cleaned: {stats['rows_cleaned']}")
    console.print(f"  • Duplicates removed: {stats['duplicates_removed']}")
    console.print(f"  • Values standardized: {stats['values_standardized']}")


def demo_data_validation():
    """Demonstrate data validation functionality."""
    console.print(Panel.fit("[bold blue]Data Validation Demo[/bold blue]"))
    
    data = get_sample_spreadsheet()
    validator = DataValidator()
    
    is_valid, errors = validator.validate(data, Platform.AMAZON)
    
    console.print(f"\n[yellow]Validation Result:[/yellow] {'✅ Valid' if is_valid else '❌ Invalid'}")
    
    if errors:
        console.print(f"\n[red]Errors Found ({len(errors)}):[/red]")
        table = Table()
        table.add_column("Row")
        table.add_column("Column")
        table.add_column("Error")
        table.add_column("Auto-Fix?")
        
        for err in errors[:10]:
            table.add_row(
                str(err.row_number or "-"),
                err.column or "-",
                err.message[:50],
                "✅" if err.auto_fixable else "❌"
            )
        console.print(table)
        
        summary = validator.get_error_summary()
        console.print(f"\n[cyan]Summary:[/cyan]")
        console.print(f"  • Total errors: {summary['total_errors']}")
        console.print(f"  • Auto-fixable: {summary['auto_fixable']}")
        console.print(f"  • Needs review: {summary['needs_review']}")


def demo_data_formatting():
    """Demonstrate data formatting for different platforms."""
    console.print(Panel.fit("[bold blue]Data Formatting Demo[/bold blue]"))
    
    orders = get_sample_orders()
    formatter = DataFormatter()
    
    # Format for Amazon
    console.print("\n[yellow]Amazon Shipment Format:[/yellow]")
    amazon_data = formatter.format_for_platform(
        SpreadsheetData(rows=[{
            "OrderID": o.amazon_order_id,
            "TrackingNumber": o.tracking_number,
            "Carrier": o.carrier,
            "ShipDate": datetime.now().strftime("%Y-%m-%d"),
        } for o in orders if o.amazon_order_id]),
        Platform.AMAZON,
        "shipment"
    )
    
    table = Table(title=amazon_data.filename)
    for col in amazon_data.columns[:6]:
        table.add_column(col)
    for row in amazon_data.rows:
        table.add_row(*[str(row.get(col, ""))[:20] for col in amazon_data.columns[:6]])
    console.print(table)
    
    # Format for eBay
    console.print("\n[yellow]eBay Shipping Format:[/yellow]")
    ebay_data = formatter.format_for_platform(
        SpreadsheetData(rows=[{
            "OrderID": o.ebay_order_id,
            "TrackingNumber": o.tracking_number,
            "Carrier": o.carrier,
        } for o in orders if o.ebay_order_id]),
        Platform.EBAY,
        "shipment"
    )
    
    table = Table(title=ebay_data.filename)
    for col in ebay_data.columns:
        table.add_column(col)
    for row in ebay_data.rows:
        table.add_row(*[str(row.get(col, "")) for col in ebay_data.columns])
    console.print(table)


def demo_report_generation():
    """Demonstrate report generation."""
    console.print(Panel.fit("[bold blue]Report Generation Demo[/bold blue]"))
    
    # Generate sample sync results
    sync_results = get_sample_sync_results()
    
    # Generate report
    generator = ReportGenerator(output_dir=Path("demo_reports"))
    report = generator.generate_sync_report(sync_results, "daily_morning")
    
    # Display text report
    console.print("\n[yellow]Text Report:[/yellow]")
    console.print(Panel(report.success_summary))
    
    # Save report
    filepath = generator.save_report(report)
    console.print(f"\n[green]Report saved to: {filepath}[/green]")
    
    # Generate HTML version
    html = generator.generate_html_report(report)
    html_path = Path("demo_reports") / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    html_path.write_text(html, encoding='utf-8')
    console.print(f"[green]HTML report saved to: {html_path}[/green]")
    
    return report


def demo_email_report(report: SyncReport = None):
    """Demonstrate email sending (if SMTP configured)."""
    console.print(Panel.fit("[bold blue]Email Report Demo[/bold blue]"))
    
    config = init_config()
    
    if not config.smtp_host:
        console.print("[yellow]⚠️ SMTP not configured in config.env[/yellow]")
        console.print("To test email, add these to your config.env:")
        console.print("  SMTP_HOST=smtp.gmail.com")
        console.print("  SMTP_PORT=587")
        console.print("  SMTP_USERNAME=your-email@gmail.com")
        console.print("  SMTP_PASSWORD=your-app-password")
        console.print("  REPORT_RECIPIENTS=recipient@example.com")
        return
    
    sender = EmailSender(config)
    
    # Test connection
    console.print("\n[yellow]Testing SMTP connection...[/yellow]")
    result = sender.test_connection()
    
    if result["success"]:
        console.print("[green]✅ SMTP connection successful![/green]")
        
        # Generate report if not provided
        if not report:
            generator = ReportGenerator()
            report = generator.generate_sync_report(get_sample_sync_results(), "test")
        
        # Ask before sending
        if click.confirm("Send test email?"):
            if sender.send_sync_report(report):
                console.print("[green]✅ Email sent successfully![/green]")
            else:
                console.print("[red]❌ Failed to send email[/red]")
    else:
        console.print(f"[red]❌ SMTP error: {result['message']}[/red]")


def demo_sage_connection():
    """Test Sage 50 connection and show unshipped orders."""
    console.print(Panel.fit("[bold blue]Sage 50 Connection Demo[/bold blue]"))
    
    config = init_config()
    connector = Sage50Connector(config)
    
    # First, show what we can find
    console.print("\n[yellow]🔍 Searching for Sage 50...[/yellow]")
    
    found_path = connector.find_sage_data_path()
    found_dsn = connector.find_odbc_dsn()
    
    table = Table(title="Sage 50 Detection")
    table.add_column("Check", style="cyan")
    table.add_column("Result", style="green")
    
    table.add_row("ODBC Available", "✅ Yes" if HAS_ODBC else "❌ No (install pyodbc)")
    table.add_row("COM/SDK Available", "✅ Yes" if HAS_COM else "❌ No (install pywin32)")
    table.add_row("Found ODBC DSN", found_dsn or "[dim]Not found[/dim]")
    table.add_row("Found Data Path", found_path or "[dim]Not found[/dim]")
    table.add_row("Configured Path", config.sage50_company_path or "[dim]Not set[/dim]")
    table.add_row("Configured DSN", config.sage50_odbc_dsn or "[dim]Not set[/dim]")
    
    console.print(table)
    
    # Try to connect
    console.print("\n[yellow]🔌 Testing connection...[/yellow]")
    result = connector.test_connection()
    
    if result["success"]:
        console.print(f"[green]✅ Connected to Sage 50![/green]")
        console.print(f"   Company: {result['details'].get('company_name', 'Unknown')}")
        console.print(f"   Connection: {result['connection_type']}")
        
        if result.get("unshipped_orders_count"):
            console.print(f"\n[cyan]📦 Unshipped Orders: {result['unshipped_orders_count']}[/cyan]")
        
        # Try to pull unshipped orders
        if click.confirm("\nWould you like to see unshipped orders?"):
            from agent.sage50.operations import Sage50Operations
            ops = Sage50Operations(connector)
            
            console.print("\n[yellow]Pulling unshipped orders...[/yellow]")
            orders = ops.pull_unshipped_orders()
            
            if orders:
                table = Table(title=f"Unshipped Orders ({len(orders)})")
                table.add_column("Order #")
                table.add_column("Date")
                table.add_column("Customer")
                table.add_column("Total")
                table.add_column("Tracking")
                
                for order in orders[:20]:  # Show first 20
                    table.add_row(
                        order.sage_order_ref or "-",
                        order.order_date.strftime("%Y-%m-%d") if order.order_date else "-",
                        (order.customer_name or "")[:25],
                        f"£{order.total:.2f}",
                        order.tracking_number or "[dim]None[/dim]",
                    )
                
                console.print(table)
                
                if len(orders) > 20:
                    console.print(f"[dim]... and {len(orders) - 20} more[/dim]")
            else:
                console.print("[yellow]No unshipped orders found[/yellow]")
    else:
        console.print(f"[red]❌ Connection failed: {result['message']}[/red]")
        
        console.print("\n[yellow]💡 To configure Sage 50:[/yellow]")
        console.print("1. Find your Sage data folder:")
        console.print("   - Usually: C:\\ProgramData\\Sage\\Accounts\\2024\\")
        console.print("   - Or: C:\\Program Files (x86)\\Sage\\Accounts\\")
        console.print("")
        console.print("2. Add to config.env:")
        console.print("   SAGE_COMPANY_PATH=C:\\ProgramData\\Sage\\Accounts\\2024\\Company.001")
        console.print("")
        console.print("3. Or use ODBC:")
        console.print("   - Open 'ODBC Data Sources' in Windows")
        console.print("   - Look for 'SageLine50v29' or similar")
        console.print("   - Add to config.env: SAGE_ODBC_DSN=SageLine50v29")


def demo_full_workflow():
    """Run a complete demo workflow."""
    console.print(Panel.fit(
        f"[bold cyan]M&M 2.0 Agent Demo v{__version__}[/bold cyan]\n"
        "Complete workflow demonstration",
        title="🚀 Full Demo"
    ))
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        
        # Step 1: Data Cleaning
        task = progress.add_task("Step 1: Cleaning sample data...", total=None)
        demo_data_cleaning()
        progress.remove_task(task)
        
        console.print("\n" + "="*60 + "\n")
        
        # Step 2: Validation
        task = progress.add_task("Step 2: Validating data...", total=None)
        demo_data_validation()
        progress.remove_task(task)
        
        console.print("\n" + "="*60 + "\n")
        
        # Step 3: Formatting
        task = progress.add_task("Step 3: Formatting for platforms...", total=None)
        demo_data_formatting()
        progress.remove_task(task)
        
        console.print("\n" + "="*60 + "\n")
        
        # Step 4: Report Generation
        task = progress.add_task("Step 4: Generating report...", total=None)
        report = demo_report_generation()
        progress.remove_task(task)
    
    console.print("\n[bold green]✅ Demo complete![/bold green]")
    console.print("\nGenerated files are in the 'demo_reports' folder.")
    
    return report


# ===== CLI =====

@click.command()
@click.option('--report', is_flag=True, help='Generate sample report only')
@click.option('--format', 'format_demo', is_flag=True, help='Test data formatting only')
@click.option('--validate', is_flag=True, help='Test data validation only')
@click.option('--clean', is_flag=True, help='Test data cleaning only')
@click.option('--email', is_flag=True, help='Test email sending')
@click.option('--sage', is_flag=True, help='Test Sage connection')
@click.option('--all', 'run_all', is_flag=True, help='Run all demos')
def main(report, format_demo, validate, clean, email, sage, run_all):
    """
    M&M 2.0 Agent Demo Mode
    
    Run demos to test agent functionality without connecting to the server.
    """
    console.print(Panel.fit(
        f"[bold blue]M&M 2.0 Agent v{__version__}[/bold blue]\n"
        "Demo/Test Mode",
        title="🧪 Demo"
    ))
    
    # If no specific option, run all
    if not any([report, format_demo, validate, clean, email, sage, run_all]):
        run_all = True
    
    if run_all:
        generated_report = demo_full_workflow()
        if click.confirm("\nWould you like to test email sending?"):
            demo_email_report(generated_report)
        if click.confirm("\nWould you like to test Sage connection?"):
            demo_sage_connection()
    else:
        if clean:
            demo_data_cleaning()
        if validate:
            demo_data_validation()
        if format_demo:
            demo_data_formatting()
        if report:
            demo_report_generation()
        if email:
            demo_email_report()
        if sage:
            demo_sage_connection()


if __name__ == "__main__":
    main()

