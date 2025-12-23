"""
Configuration management for the M&M 2.0 Agent.
Handles loading settings from environment variables and config files.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv


@dataclass
class AgentConfig:
    """Main configuration class for the M&M 2.0 agent."""
    
    # Agent Identity
    agent_id: str = "mnm-agent-001"
    agent_secret: str = ""
    
    # Backend Connection
    backend_ws_url: str = "wss://api.yourbackend.com/agent/ws"
    backend_api_url: str = "https://api.yourbackend.com/api/v1"
    backend_api_key: str = ""
    
    # Polling (fallback)
    polling_enabled: bool = True
    polling_interval: int = 30  # seconds
    
    # === Sage Quantum ===
    sage50_company_path: str = ""
    sage50_username: str = ""
    sage50_password: str = ""
    sage50_odbc_dsn: str = ""  # ODBC Data Source Name
    sage_ar_account: str = "1100"  # Accounts Receivable GL account
    sage_sales_account: str = "4100"  # Sales/Income GL account
    sage_use_item_ids: bool = True  # True = use Item IDs (items must exist), False = simple mode (description only)
    
    # === Carrier API Credentials ===
    # FedEx
    fedex_client_id: str = ""
    fedex_client_secret: str = ""
    fedex_account_number: str = ""
    
    # UPS
    ups_client_id: str = ""
    ups_client_secret: str = ""
    
    # Royal Mail
    royal_mail_client_id: str = ""
    royal_mail_client_secret: str = ""
    
    # === Email/SMTP ===
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from_email: str = ""
    report_recipients: list[str] = field(default_factory=list)
    
    # === Scheduling ===
    # Daily sync times (24h format, e.g., "08:00" and "12:00")
    morning_sync_time: str = "08:00"
    noon_sync_time: str = "12:00"
    sync_enabled: bool = True
    
    # Logging
    log_level: str = "INFO"
    log_file: str = r"C:\ProgramData\MNMAgent\logs\agent.log"
    
    # Monitoring
    sentry_dsn: Optional[str] = None
    
    # Agent Settings
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 5
    heartbeat_interval: int = 60
    task_timeout: int = 600  # 10 minutes for data operations
    
    # Runtime paths
    data_dir: Path = field(default_factory=lambda: Path(r"C:\ProgramData\MNMAgent\data"))
    export_dir: Path = field(default_factory=lambda: Path(r"C:\ProgramData\MNMAgent\exports"))
    import_dir: Path = field(default_factory=lambda: Path(r"C:\ProgramData\MNMAgent\imports"))
    
    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "AgentConfig":
        """Load configuration from environment variables."""
        
        # Try to load from .env file
        if env_file:
            load_dotenv(env_file)
        else:
            # Try common locations
            for env_path in ["config.env", ".env", "../config.env"]:
                if Path(env_path).exists():
                    load_dotenv(env_path)
                    break
        
        # Parse report recipients
        recipients_str = os.getenv("REPORT_RECIPIENTS", "")
        recipients = [r.strip() for r in recipients_str.split(",") if r.strip()]
        
        return cls(
            # Agent Identity
            agent_id=os.getenv("AGENT_ID", "mnm-agent-001"),
            agent_secret=os.getenv("AGENT_SECRET", ""),
            
            # Backend
            backend_ws_url=os.getenv("BACKEND_URL", "wss://api.yourbackend.com/agent/ws"),
            backend_api_url=os.getenv("BACKEND_API_URL", "https://api.yourbackend.com/api/v1"),
            backend_api_key=os.getenv("BACKEND_API_KEY", ""),
            
            # Polling
            polling_enabled=os.getenv("POLLING_ENABLED", "true").lower() == "true",
            polling_interval=int(os.getenv("POLLING_INTERVAL", "30")),
            
            # Sage Quantum
            sage50_company_path=os.getenv("SAGE_COMPANY_PATH", os.getenv("SAGE50_COMPANY_PATH", "")),
            sage50_username=os.getenv("SAGE_USERNAME", os.getenv("SAGE50_USERNAME", "")),
            sage50_password=os.getenv("SAGE_PASSWORD", os.getenv("SAGE50_PASSWORD", "")),
            sage50_odbc_dsn=os.getenv("SAGE_ODBC_DSN", ""),
            sage_ar_account=os.getenv("SAGE_AR_ACCOUNT", "1100"),
            sage_sales_account=os.getenv("SAGE_SALES_ACCOUNT", "4100"),
            sage_use_item_ids=os.getenv("SAGE_USE_ITEM_IDS", "true").lower() in ("true", "1", "yes"),
            
            # FedEx
            fedex_client_id=os.getenv("FEDEX_CLIENT_ID", ""),
            fedex_client_secret=os.getenv("FEDEX_CLIENT_SECRET", ""),
            fedex_account_number=os.getenv("FEDEX_ACCOUNT_NUMBER", ""),
            
            # UPS
            ups_client_id=os.getenv("UPS_CLIENT_ID", ""),
            ups_client_secret=os.getenv("UPS_CLIENT_SECRET", ""),
            
            # Royal Mail
            royal_mail_client_id=os.getenv("ROYAL_MAIL_CLIENT_ID", ""),
            royal_mail_client_secret=os.getenv("ROYAL_MAIL_CLIENT_SECRET", ""),
            
            # Email
            smtp_host=os.getenv("SMTP_HOST", ""),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_username=os.getenv("SMTP_USERNAME", ""),
            smtp_password=os.getenv("SMTP_PASSWORD", ""),
            smtp_from_email=os.getenv("SMTP_FROM_EMAIL", ""),
            report_recipients=recipients,
            
            # Scheduling
            morning_sync_time=os.getenv("MORNING_SYNC_TIME", "08:00"),
            noon_sync_time=os.getenv("NOON_SYNC_TIME", "12:00"),
            sync_enabled=os.getenv("SYNC_ENABLED", "true").lower() == "true",
            
            # Logging
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", r"C:\ProgramData\MNMAgent\logs\agent.log"),
            
            # Monitoring
            sentry_dsn=os.getenv("SENTRY_DSN"),
            
            # Agent Settings
            max_retry_attempts=int(os.getenv("MAX_RETRY_ATTEMPTS", "3")),
            retry_delay_seconds=int(os.getenv("RETRY_DELAY_SECONDS", "5")),
            heartbeat_interval=int(os.getenv("HEARTBEAT_INTERVAL", "60")),
            task_timeout=int(os.getenv("TASK_TIMEOUT", "600")),
        )
    
    def ensure_directories(self):
        """Create necessary directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.import_dir.mkdir(parents=True, exist_ok=True)
        Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
    
    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.agent_secret:
            errors.append("AGENT_SECRET is required")
        if not self.backend_api_key:
            errors.append("BACKEND_API_KEY is required")
        
        # Sage connection - need either company path or ODBC DSN
        if not self.sage50_company_path and not self.sage50_odbc_dsn:
            errors.append("Either SAGE_COMPANY_PATH or SAGE_ODBC_DSN is required")
        
        # Email - warning if not configured
        if not self.smtp_host:
            errors.append("Warning: SMTP not configured - email reports disabled")
        
        return errors


# Global config instance
_config: Optional[AgentConfig] = None


def get_config() -> AgentConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = AgentConfig.from_env()
    return _config


def init_config(env_file: Optional[str] = None) -> AgentConfig:
    """Initialize configuration from environment."""
    global _config
    _config = AgentConfig.from_env(env_file)
    _config.ensure_directories()
    return _config
