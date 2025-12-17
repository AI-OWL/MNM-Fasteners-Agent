"""
Email Sender.
Sends sync reports via email.
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from loguru import logger

from agent.config import AgentConfig
from agent.models import SyncReport
from agent.reports.report_generator import ReportGenerator


class EmailSender:
    """
    Sends email reports.
    
    Supports:
    - SMTP with TLS
    - HTML and plain text emails
    - Multiple recipients
    """
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self.report_generator = ReportGenerator()
    
    def send_sync_report(
        self,
        report: SyncReport,
        recipients: Optional[list[str]] = None,
    ) -> bool:
        """
        Send sync report via email.
        
        Args:
            report: The sync report to send
            recipients: List of email addresses (uses config if not provided)
            
        Returns:
            True if email sent successfully
        """
        recipients = recipients or self.config.report_recipients
        
        if not recipients:
            logger.warning("No email recipients configured")
            return False
        
        if not self.config.smtp_host:
            logger.warning("SMTP not configured, cannot send email")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"M&M 2.0 Sync Report - {report.report_type.replace('_', ' ').title()}"
            msg["From"] = self.config.smtp_from_email
            msg["To"] = ", ".join(recipients)
            
            # Plain text version
            text_part = MIMEText(report.success_summary, "plain")
            msg.attach(text_part)
            
            # HTML version
            html_content = self.report_generator.generate_html_report(report)
            html_part = MIMEText(html_content, "html")
            msg.attach(html_part)
            
            # Send email
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls(context=context)
                
                if self.config.smtp_username and self.config.smtp_password:
                    server.login(self.config.smtp_username, self.config.smtp_password)
                
                server.sendmail(
                    self.config.smtp_from_email,
                    recipients,
                    msg.as_string()
                )
            
            logger.info(f"Sync report emailed to {len(recipients)} recipient(s)")
            
            # Update report
            report.email_sent = True
            report.email_recipients = recipients
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
    
    def send_error_alert(
        self,
        error_message: str,
        error_details: Optional[dict] = None,
        recipients: Optional[list[str]] = None,
    ) -> bool:
        """
        Send error alert email.
        
        Args:
            error_message: Main error message
            error_details: Additional error details
            recipients: Email recipients
            
        Returns:
            True if sent successfully
        """
        recipients = recipients or self.config.report_recipients
        
        if not recipients or not self.config.smtp_host:
            return False
        
        try:
            msg = MIMEMultipart()
            msg["Subject"] = "⚠️ M&M 2.0 Agent Error Alert"
            msg["From"] = self.config.smtp_from_email
            msg["To"] = ", ".join(recipients)
            
            # Build error message
            body = f"""
M&M 2.0 Agent Error Alert

Error: {error_message}

Agent ID: {self.config.agent_id}
Time: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
            if error_details:
                body += "Details:\n"
                for key, value in error_details.items():
                    body += f"  {key}: {value}\n"
            
            msg.attach(MIMEText(body, "plain"))
            
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls(context=context)
                
                if self.config.smtp_username and self.config.smtp_password:
                    server.login(self.config.smtp_username, self.config.smtp_password)
                
                server.sendmail(
                    self.config.smtp_from_email,
                    recipients,
                    msg.as_string()
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send error alert: {e}")
            return False
    
    def test_connection(self) -> dict:
        """Test SMTP connection."""
        result = {
            "success": False,
            "message": "",
        }
        
        if not self.config.smtp_host:
            result["message"] = "SMTP not configured"
            return result
        
        try:
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls(context=context)
                
                if self.config.smtp_username:
                    server.login(self.config.smtp_username, self.config.smtp_password)
                
                result["success"] = True
                result["message"] = "SMTP connection successful"
                
        except Exception as e:
            result["message"] = f"SMTP error: {e}"
        
        return result

