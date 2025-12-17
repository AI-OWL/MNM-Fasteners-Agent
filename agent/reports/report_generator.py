"""
Report Generator.
Creates summary reports of sync operations.
"""

from datetime import datetime
from typing import Optional
from pathlib import Path
from loguru import logger

from agent.models import SyncReport, DataValidationError


class ReportGenerator:
    """
    Generates sync reports for M&M 2.0.
    
    Report types:
    - Daily sync summary
    - Error report
    - Full detailed report
    """
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or Path("reports")
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_sync_report(
        self,
        sync_results: dict,
        report_type: str = "daily",
    ) -> SyncReport:
        """
        Generate a sync report.
        
        Args:
            sync_results: Results from sync operations
            report_type: Type of report (daily_morning, daily_noon, manual)
            
        Returns:
            SyncReport object
        """
        report = SyncReport(
            report_type=report_type,
            started_at=sync_results.get("started_at", datetime.utcnow()),
            completed_at=datetime.utcnow(),
            platforms_synced=sync_results.get("platforms", []),
        )
        
        # Amazon results
        if "amazon" in sync_results:
            report.amazon_results = sync_results["amazon"]
            report.total_orders_processed += sync_results["amazon"].get("orders", 0)
        
        # eBay results
        if "ebay" in sync_results:
            report.ebay_results = sync_results["ebay"]
            report.total_orders_processed += sync_results["ebay"].get("orders", 0)
        
        # Shopify results
        if "shopify" in sync_results:
            report.shopify_results = sync_results["shopify"]
            report.total_orders_processed += sync_results["shopify"].get("orders", 0)
        
        # Sage results
        if "sage" in sync_results:
            report.sage_results = sync_results["sage"]
        
        # Tracking updates
        report.total_tracking_updated = sync_results.get("tracking_updated", 0)
        
        # Errors
        errors = sync_results.get("errors", [])
        report.errors_count = len(errors)
        report.errors_for_review = [
            DataValidationError(**e) if isinstance(e, dict) else e
            for e in errors
        ]
        
        # Generate summary
        report.success_summary = self._generate_summary(report)
        
        return report
    
    def _generate_summary(self, report: SyncReport) -> str:
        """Generate human-readable summary."""
        lines = []
        
        # Header
        lines.append(f"M&M 2.0 Sync Report - {report.report_type.replace('_', ' ').title()}")
        lines.append(f"Completed: {report.completed_at.strftime('%Y-%m-%d %H:%M')}")
        lines.append("")
        
        # Overview
        lines.append("📊 SYNC OVERVIEW")
        lines.append(f"  • Orders Processed: {report.total_orders_processed}")
        lines.append(f"  • Products Synced: {report.total_products_synced}")
        lines.append(f"  • Tracking Updated: {report.total_tracking_updated}")
        lines.append("")
        
        # Platform details
        if report.amazon_results:
            lines.append("🛒 AMAZON")
            lines.append(f"  • Orders: {report.amazon_results.get('orders', 0)}")
            lines.append(f"  • Success: {report.amazon_results.get('success', 0)}")
            lines.append(f"  • Failed: {report.amazon_results.get('failed', 0)}")
            lines.append("")
        
        if report.ebay_results:
            lines.append("🏷️ EBAY")
            lines.append(f"  • Orders: {report.ebay_results.get('orders', 0)}")
            lines.append(f"  • Success: {report.ebay_results.get('success', 0)}")
            lines.append(f"  • Failed: {report.ebay_results.get('failed', 0)}")
            lines.append("")
        
        if report.shopify_results:
            lines.append("🛍️ SHOPIFY")
            lines.append(f"  • Orders: {report.shopify_results.get('orders', 0)}")
            lines.append(f"  • Success: {report.shopify_results.get('success', 0)}")
            lines.append(f"  • Failed: {report.shopify_results.get('failed', 0)}")
            lines.append("")
        
        if report.sage_results:
            lines.append("📁 SAGE QUANTUM")
            lines.append(f"  • Imported: {report.sage_results.get('imported', 0)}")
            lines.append(f"  • Exported: {report.sage_results.get('exported', 0)}")
            lines.append("")
        
        # Errors
        if report.errors_count > 0:
            lines.append(f"⚠️ ERRORS ({report.errors_count})")
            for error in report.errors_for_review[:10]:  # Show first 10
                lines.append(f"  • Row {error.row_number}: {error.message}")
            if report.errors_count > 10:
                lines.append(f"  ... and {report.errors_count - 10} more")
            lines.append("")
        else:
            lines.append("✅ No errors!")
            lines.append("")
        
        return "\n".join(lines)
    
    def generate_html_report(self, report: SyncReport) -> str:
        """Generate HTML version of report for email."""
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; color: #333; }}
        .header {{ background: #2563eb; color: white; padding: 20px; border-radius: 8px; }}
        .section {{ margin: 20px 0; padding: 15px; background: #f8fafc; border-radius: 8px; }}
        .section h3 {{ margin-top: 0; color: #1e40af; }}
        .stat {{ display: inline-block; margin: 10px; padding: 10px 20px; background: white; border-radius: 4px; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #2563eb; }}
        .stat-label {{ font-size: 12px; color: #64748b; }}
        .success {{ color: #16a34a; }}
        .error {{ color: #dc2626; }}
        .error-list {{ background: #fef2f2; padding: 10px; border-radius: 4px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
        th {{ background: #f1f5f9; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>M&M 2.0 Sync Report</h1>
        <p>{report.report_type.replace('_', ' ').title()} - {report.completed_at.strftime('%Y-%m-%d %H:%M')}</p>
    </div>
    
    <div class="section">
        <h3>📊 Sync Overview</h3>
        <div class="stat">
            <div class="stat-value">{report.total_orders_processed}</div>
            <div class="stat-label">Orders Processed</div>
        </div>
        <div class="stat">
            <div class="stat-value">{report.total_products_synced}</div>
            <div class="stat-label">Products Synced</div>
        </div>
        <div class="stat">
            <div class="stat-value">{report.total_tracking_updated}</div>
            <div class="stat-label">Tracking Updated</div>
        </div>
    </div>
"""
        
        # Platform sections
        if report.amazon_results:
            html += self._platform_section_html("Amazon", "🛒", report.amazon_results)
        
        if report.ebay_results:
            html += self._platform_section_html("eBay", "🏷️", report.ebay_results)
        
        if report.shopify_results:
            html += self._platform_section_html("Shopify", "🛍️", report.shopify_results)
        
        if report.sage_results:
            html += self._platform_section_html("Sage Quantum", "📁", report.sage_results)
        
        # Errors section
        if report.errors_count > 0:
            html += f"""
    <div class="section">
        <h3 class="error">⚠️ Errors ({report.errors_count})</h3>
        <div class="error-list">
            <table>
                <tr><th>Row</th><th>Column</th><th>Error</th></tr>
"""
            for error in report.errors_for_review[:20]:
                html += f"""
                <tr>
                    <td>{error.row_number or '-'}</td>
                    <td>{error.column or '-'}</td>
                    <td>{error.message}</td>
                </tr>
"""
            html += """
            </table>
        </div>
    </div>
"""
        else:
            html += """
    <div class="section">
        <h3 class="success">✅ All operations completed successfully!</h3>
    </div>
"""
        
        html += """
</body>
</html>
"""
        return html
    
    def _platform_section_html(self, name: str, emoji: str, results: dict) -> str:
        """Generate HTML section for a platform."""
        success = results.get('success', 0)
        failed = results.get('failed', 0)
        total = results.get('orders', 0) or (success + failed)
        
        return f"""
    <div class="section">
        <h3>{emoji} {name}</h3>
        <table>
            <tr><td>Total</td><td><strong>{total}</strong></td></tr>
            <tr><td>Successful</td><td class="success">{success}</td></tr>
            <tr><td>Failed</td><td class="error">{failed}</td></tr>
        </table>
    </div>
"""
    
    def save_report(self, report: SyncReport) -> Path:
        """Save report to file."""
        timestamp = report.completed_at.strftime("%Y%m%d_%H%M%S")
        filename = f"sync_report_{report.report_type}_{timestamp}.txt"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            f.write(report.success_summary)
        
        logger.info(f"Report saved: {filepath}")
        return filepath

