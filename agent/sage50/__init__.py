r"""
Sage 50 Quantum (UK) integration module.
Provides interface to Sage 50 Accounts via ODBC, SDK (COM), or file-based.

Connection Methods:
1. SDK (SDO) - BEST: Automatic import/export, no manual steps
2. ODBC - Read-only database access
3. File-based - Manual CSV import/export

Data typically stored at:
- C:\ProgramData\Sage\Accounts\{YEAR}\
- ODBC DSN: SageLine50v{XX} (e.g., SageLine50v29 for 2024)
"""

from agent.sage50.connector import Sage50Connector, Sage50Error, Sage50ConnectionError
from agent.sage50.operations import Sage50Operations
from agent.sage50.sdk_operations import SageSDK, SageSDKError

__all__ = [
    "Sage50Connector", 
    "Sage50Operations",
    "Sage50Error",
    "Sage50ConnectionError",
    "SageSDK",
    "SageSDKError",
]
