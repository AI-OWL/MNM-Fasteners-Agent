"""
Command-line interface for the MNM Agent.
Provides commands for running, installing, and managing the agent.
"""

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from pathlib import Path

from agent import __version__

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="MNM Agent")
def cli():
    """MNM Fasteners Agent - Sage 50 Integration Service"""
    pass


@cli.command()
@click.option(
    "--config", "-c",
    type=click.Path(exists=True),
    help="Path to configuration file"
)
def run(config):
    """Run the agent in foreground mode."""
    console.print(Panel.fit(
        f"[bold blue]MNM Fasteners Agent v{__version__}[/bold blue]\n"
        "Press Ctrl+C to stop",
        title="Starting Agent"
    ))
    
    from agent.core import run_agent
    run_agent(config)


@cli.command()
def install():
    """Install the agent as a Windows service."""
    console.print("[bold]Installing Windows Service...[/bold]")
    
    try:
        from agent.windows_service import install_service
        if install_service():
            console.print("[green]✓ Service installed successfully![/green]")
        else:
            console.print("[red]✗ Service installation failed[/red]")
    except ImportError:
        console.print("[red]Error: pywin32 is required for Windows service[/red]")
        console.print("Install it with: pip install pywin32")


@cli.command()
def uninstall():
    """Uninstall the Windows service."""
    console.print("[bold]Uninstalling Windows Service...[/bold]")
    
    try:
        from agent.windows_service import uninstall_service
        if uninstall_service():
            console.print("[green]✓ Service uninstalled successfully![/green]")
        else:
            console.print("[red]✗ Service uninstallation failed[/red]")
    except ImportError:
        console.print("[red]Error: pywin32 is required[/red]")


@cli.command()
def start():
    """Start the Windows service."""
    try:
        from agent.windows_service import start_service
        if start_service():
            console.print("[green]✓ Service started[/green]")
        else:
            console.print("[red]✗ Failed to start service[/red]")
    except ImportError:
        console.print("[red]Error: pywin32 is required[/red]")


@cli.command()
def stop():
    """Stop the Windows service."""
    try:
        from agent.windows_service import stop_service
        if stop_service():
            console.print("[green]✓ Service stopped[/green]")
        else:
            console.print("[red]✗ Failed to stop service[/red]")
    except ImportError:
        console.print("[red]Error: pywin32 is required[/red]")


@cli.command()
def status():
    """Show agent and service status."""
    console.print(Panel.fit(
        f"[bold]MNM Fasteners Agent v{__version__}[/bold]",
        title="Status"
    ))
    
    # Service status
    try:
        from agent.windows_service import service_status
        svc_status = service_status()
        if svc_status:
            color = "green" if svc_status == "Running" else "yellow"
            console.print(f"Windows Service: [{color}]{svc_status}[/{color}]")
    except ImportError:
        console.print("Windows Service: [dim]Not available[/dim]")
    
    # Configuration
    from agent.config import AgentConfig
    config = AgentConfig.from_env()
    
    table = Table(title="Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Agent ID", config.agent_id)
    table.add_row("Backend URL", config.backend_ws_url)
    table.add_row("Polling Enabled", str(config.polling_enabled))
    table.add_row("Polling Interval", f"{config.polling_interval}s")
    table.add_row("Sage 50 Path", config.sage50_company_path or "[dim]Not set[/dim]")
    table.add_row("Log File", config.log_file)
    
    console.print(table)


@cli.command()
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def test_sage(verbose):
    """Test connection to Sage 50."""
    console.print("[bold]Testing Sage 50 Connection...[/bold]")
    
    from agent.config import init_config
    from agent.sage50 import Sage50Connector
    
    config = init_config()
    connector = Sage50Connector(config)
    
    result = connector.test_connection()
    
    if result["success"]:
        console.print("[green]✓ Connection successful![/green]")
        
        if verbose and result.get("details"):
            table = Table(title="Sage 50 Details")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")
            
            for key, value in result["details"].items():
                table.add_row(key, str(value))
            
            console.print(table)
    else:
        console.print(f"[red]✗ Connection failed: {result['message']}[/red]")


@cli.command()
def test_backend():
    """Test connection to backend server."""
    import asyncio
    import aiohttp
    
    console.print("[bold]Testing Backend Connection...[/bold]")
    
    from agent.config import init_config
    config = init_config()
    
    async def test():
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{config.backend_api_url}/health"
                headers = {
                    "Authorization": f"Bearer {config.backend_api_key}",
                    "X-Agent-ID": config.agent_id,
                }
                
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        console.print("[green]✓ Backend connection successful![/green]")
                        return True
                    else:
                        console.print(
                            f"[red]✗ Backend returned status {resp.status}[/red]"
                        )
                        return False
                        
        except aiohttp.ClientError as e:
            console.print(f"[red]✗ Connection error: {e}[/red]")
            return False
        except Exception as e:
            console.print(f"[red]✗ Error: {e}[/red]")
            return False
    
    asyncio.run(test())


@cli.command()
@click.argument("config_path", type=click.Path())
def init(config_path):
    """Initialize configuration file."""
    config_path = Path(config_path)
    
    if config_path.exists():
        if not click.confirm(f"{config_path} already exists. Overwrite?"):
            return
    
    template = '''# MNM Fasteners Agent Configuration

# Agent Identity
AGENT_ID=mnm-agent-001
AGENT_SECRET=your-secret-key-here

# Backend Server Connection
BACKEND_URL=wss://api.yourbackend.com/agent/ws
BACKEND_API_URL=https://api.yourbackend.com/api/v1
BACKEND_API_KEY=your-api-key-here

# Polling Configuration (fallback if WebSocket fails)
POLLING_INTERVAL=30
POLLING_ENABLED=true

# Sage 50 Configuration
SAGE50_COMPANY_PATH=C:\\ProgramData\\Sage\\Accounts\\2024\\Company.001
SAGE50_USERNAME=
SAGE50_PASSWORD=

# Logging
LOG_LEVEL=INFO
LOG_FILE=C:\\ProgramData\\MNMAgent\\logs\\agent.log

# Agent Settings
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=5
HEARTBEAT_INTERVAL=60
TASK_TIMEOUT=300
'''
    
    config_path.write_text(template, encoding='utf-8')
    console.print(f"[green]✓ Configuration file created: {config_path}[/green]")
    console.print("\nEdit this file with your settings, then run:")
    console.print(f"  mnm-agent --config {config_path} run")


@cli.command()
def logs():
    """View recent logs."""
    from agent.config import AgentConfig
    config = AgentConfig.from_env()
    
    log_file = Path(config.log_file)
    
    if not log_file.exists():
        console.print(f"[yellow]Log file not found: {log_file}[/yellow]")
        return
    
    console.print(f"[bold]Recent logs from {log_file}:[/bold]\n")
    
    # Read last 50 lines
    with open(log_file, "r") as f:
        lines = f.readlines()
        recent = lines[-50:] if len(lines) > 50 else lines
        
        for line in recent:
            # Color based on log level
            if "ERROR" in line:
                console.print(f"[red]{line.rstrip()}[/red]")
            elif "WARNING" in line:
                console.print(f"[yellow]{line.rstrip()}[/yellow]")
            elif "INFO" in line:
                console.print(f"[green]{line.rstrip()}[/green]")
            else:
                console.print(line.rstrip())


@cli.command()
@click.option('--report', is_flag=True, help='Generate sample report')
@click.option('--format', 'format_demo', is_flag=True, help='Test data formatting')
@click.option('--email', is_flag=True, help='Test email sending')
@click.option('--sage', is_flag=True, help='Test Sage connection')
def demo(report, format_demo, email, sage):
    """
    Run demo mode to test functionality without server.
    
    This allows testing the agent's features locally.
    """
    import sys
    from agent import demo as demo_module
    
    # Build args for demo module
    args = ['demo']
    if report:
        args.append('--report')
    if format_demo:
        args.append('--format')
    if email:
        args.append('--email')
    if sage:
        args.append('--sage')
    if not any([report, format_demo, email, sage]):
        args.append('--all')
    
    sys.argv = args
    demo_module.main(standalone_mode=False)


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()

