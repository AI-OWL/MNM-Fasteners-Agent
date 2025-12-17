"""
Windows Service wrapper for the MNM Agent.
Allows the agent to run as a Windows service.
"""

import sys
import os
import time
import asyncio
from pathlib import Path
from loguru import logger

# Windows service imports
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False


from agent import __version__
from agent.config import init_config
from agent.logging_config import setup_logging
from agent.core import MNMAgent


class MNMAgentService(win32serviceutil.ServiceFramework):
    """
    Windows Service class for MNM Agent.
    
    Service Name: MNMFastenersAgent
    Display Name: MNM Fasteners Agent
    Description: Bridges ecommerce platforms with Sage 50
    """
    
    _svc_name_ = "MNMFastenersAgent"
    _svc_display_name_ = "MNM Fasteners Agent"
    _svc_description_ = (
        "MNM Fasteners Agent - Bridges ecommerce platforms "
        "(Amazon, eBay) with Sage 50 Accounts"
    )
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.agent = None
        self.loop = None
    
    def SvcStop(self):
        """Called when the service is asked to stop."""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        
        # Signal the agent to stop
        if self.agent and self.loop:
            self.loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(self.agent.stop())
            )
        
        win32event.SetEvent(self.stop_event)
    
    def SvcDoRun(self):
        """Called when the service is starting."""
        try:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            
            self.main()
            
        except Exception as e:
            servicemanager.LogErrorMsg(f"Service failed: {e}")
            raise
    
    def main(self):
        """Main service entry point."""
        # Determine config file location
        # Look in: service directory, ProgramData, or working directory
        config_paths = [
            Path(__file__).parent.parent / "config.env",
            Path(r"C:\ProgramData\MNMAgent\config.env"),
            Path("config.env"),
        ]
        
        config_file = None
        for path in config_paths:
            if path.exists():
                config_file = str(path)
                break
        
        # Initialize configuration
        config = init_config(config_file)
        
        # Setup logging (no console for service)
        setup_logging(config, console=False)
        
        logger.info(f"MNM Agent Service v{__version__} starting")
        
        # Create agent
        self.agent = MNMAgent(config)
        
        # Create event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            # Run agent
            self.loop.run_until_complete(self.agent.start())
            
        except Exception as e:
            logger.exception(f"Agent error: {e}")
            servicemanager.LogErrorMsg(f"Agent error: {e}")
            
        finally:
            # Cleanup
            if self.agent:
                self.loop.run_until_complete(self.agent.stop())
            self.loop.close()
            
            logger.info("MNM Agent Service stopped")


def install_service():
    """Install the Windows service."""
    if not HAS_WIN32:
        print("Error: pywin32 is not installed")
        print("Install it with: pip install pywin32")
        return False
    
    try:
        # Get the path to this script
        script_path = os.path.abspath(__file__)
        
        print(f"Installing MNM Fasteners Agent service...")
        print(f"Script path: {script_path}")
        
        # Install service
        win32serviceutil.InstallService(
            MNMAgentService._svc_name_,
            MNMAgentService._svc_display_name_,
            description=MNMAgentService._svc_description_,
            startType=win32service.SERVICE_AUTO_START,
            exeName=sys.executable,
            exeArgs=f'"{script_path}" --service',
        )
        
        print("Service installed successfully!")
        print(f"Service name: {MNMAgentService._svc_name_}")
        print("\nTo start the service, run:")
        print(f"  net start {MNMAgentService._svc_name_}")
        
        return True
        
    except Exception as e:
        print(f"Failed to install service: {e}")
        return False


def uninstall_service():
    """Uninstall the Windows service."""
    if not HAS_WIN32:
        print("Error: pywin32 is not installed")
        return False
    
    try:
        print(f"Stopping service {MNMAgentService._svc_name_}...")
        
        try:
            win32serviceutil.StopService(MNMAgentService._svc_name_)
            time.sleep(2)
        except Exception:
            pass  # Service might not be running
        
        print(f"Uninstalling service {MNMAgentService._svc_name_}...")
        win32serviceutil.RemoveService(MNMAgentService._svc_name_)
        
        print("Service uninstalled successfully!")
        return True
        
    except Exception as e:
        print(f"Failed to uninstall service: {e}")
        return False


def start_service():
    """Start the Windows service."""
    if not HAS_WIN32:
        print("Error: pywin32 is not installed")
        return False
    
    try:
        win32serviceutil.StartService(MNMAgentService._svc_name_)
        print(f"Service {MNMAgentService._svc_name_} started")
        return True
    except Exception as e:
        print(f"Failed to start service: {e}")
        return False


def stop_service():
    """Stop the Windows service."""
    if not HAS_WIN32:
        print("Error: pywin32 is not installed")
        return False
    
    try:
        win32serviceutil.StopService(MNMAgentService._svc_name_)
        print(f"Service {MNMAgentService._svc_name_} stopped")
        return True
    except Exception as e:
        print(f"Failed to stop service: {e}")
        return False


def service_status():
    """Get service status."""
    if not HAS_WIN32:
        print("Error: pywin32 is not installed")
        return None
    
    try:
        status = win32serviceutil.QueryServiceStatus(MNMAgentService._svc_name_)
        
        state_map = {
            win32service.SERVICE_STOPPED: "Stopped",
            win32service.SERVICE_START_PENDING: "Start Pending",
            win32service.SERVICE_STOP_PENDING: "Stop Pending",
            win32service.SERVICE_RUNNING: "Running",
            win32service.SERVICE_CONTINUE_PENDING: "Continue Pending",
            win32service.SERVICE_PAUSE_PENDING: "Pause Pending",
            win32service.SERVICE_PAUSED: "Paused",
        }
        
        state = state_map.get(status[1], f"Unknown ({status[1]})")
        print(f"Service: {MNMAgentService._svc_name_}")
        print(f"Status: {state}")
        
        return state
        
    except Exception as e:
        print(f"Service not found or error: {e}")
        return None


if __name__ == '__main__':
    if HAS_WIN32:
        if len(sys.argv) == 1:
            # No arguments - run as service
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(MNMAgentService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            # Handle command line arguments
            win32serviceutil.HandleCommandLine(MNMAgentService)
    else:
        print("Windows service support requires pywin32")
        print("Install it with: pip install pywin32")

