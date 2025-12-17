# M&M 2.0 Agent

A local agent service that runs on your client's Windows machine to facilitate communication between your cloud backend and their **Sage Quantum** system.

## 🎯 Project Overview

**M&M 2.0** is an automated system that:

1. **Pulls data from Amazon/eBay/Shopify** - Daily order exports
2. **Cleans and formats spreadsheet data** - AI-powered column mapping and standardization
3. **Syncs with Sage Quantum** - Push orders to and pull data from internal ERP
4. **Pulls tracking from FedEx/UPS** - Get shipment status and tracking numbers
5. **Uploads tracking back to platforms** - Update Amazon/eBay/Shopify with tracking info
6. **Sends email reports** - Daily summary of sync operations

This **Agent** runs on the client's Windows device (where Sage Quantum is installed) and acts as a bridge between your cloud backend and their local Sage system.

```
┌─────────────────────┐                ┌──────────────────────┐                ┌─────────────────┐
│   Your Backend      │ ◄────WSS────► │    M&M 2.0 Agent     │ ◄───SDK/ODBC──►│  Sage Quantum   │
│   (Cloud Server)    │                │  (Client's Windows)  │                │ (Client Device) │
│                     │                │                      │                │                 │
│ • Amazon SP API     │                │ • Receives tasks     │                │ • Orders        │
│ • eBay API          │                │ • Executes Sage ops  │                │ • Products      │
│ • Shopify API       │                │ • Formats data       │                │ • Customers     │
│ • Task management   │                │ • Returns results    │                │ • Inventory     │
└─────────────────────┘                └──────────────────────┘                └─────────────────┘
                                                │
                                                ▼
                                       ┌──────────────────┐
                                       │  FedEx/UPS APIs  │
                                       │  (Tracking Info) │
                                       └──────────────────┘
```

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Sage Quantum Integration** | Pull/push orders, products, customers via SDK, ODBC, or file import |
| **Data Transformation** | Clean, validate, and format data for Amazon/eBay/Shopify requirements |
| **Tracking Integration** | Pull tracking info from FedEx, UPS, Royal Mail |
| **Twice Daily Sync** | Automated sync at 8:00 AM and 12:00 PM (configurable) |
| **Email Reports** | HTML summary reports sent to configured recipients |
| **Error Handling** | Errors flagged for review instead of auto-fixing |
| **Windows Service** | Runs as background service, survives reboots |
| **Real-time Communication** | WebSocket with HTTP polling fallback |

## 📦 Installation

### Prerequisites

- Windows 10/11 or Windows Server
- Python 3.10+
- Sage Quantum installed (with SDK or ODBC access)
- Administrator access

### Quick Install

```powershell
# 1. Clone repository
git clone https://github.com/your-org/MNM-Fasteners-Agent.git
cd MNM-Fasteners-Agent

# 2. Run installer (as Administrator)
.\scripts\install.ps1

# 3. Configure
notepad C:\ProgramData\MNMAgent\config.env

# 4. Test connections
mnm-agent test-sage
mnm-agent test-backend

# 5. Install and start service
mnm-agent install
mnm-agent start
```

## 🔧 Configuration

Edit `C:\ProgramData\MNMAgent\config.env`:

```env
# Agent Identity
AGENT_ID=mnm-client-001
AGENT_SECRET=secure-secret-here

# Backend Connection
BACKEND_URL=wss://api.mmautomation.com/agent/ws
BACKEND_API_KEY=your-api-key

# Sage Quantum (choose one)
SAGE_COMPANY_PATH=C:\Sage\Company.001
# OR
SAGE_ODBC_DSN=SageQuantum

# Carrier APIs (for tracking)
FEDEX_CLIENT_ID=your-fedex-id
FEDEX_CLIENT_SECRET=your-fedex-secret
UPS_CLIENT_ID=your-ups-id
UPS_CLIENT_SECRET=your-ups-secret

# Email Reports
SMTP_HOST=smtp.gmail.com
SMTP_USERNAME=reports@company.com
SMTP_PASSWORD=app-password
REPORT_RECIPIENTS=team@company.com

# Schedule
MORNING_SYNC_TIME=08:00
NOON_SYNC_TIME=12:00
```

## 📋 Supported Operations

### Data Sync
| Task | Description |
|------|-------------|
| `pull_sage_orders` | Pull orders from Sage Quantum |
| `push_orders_to_sage` | Push ecommerce orders to Sage |
| `pull_sage_products` | Pull product/inventory data |
| `sync_amazon_to_sage` | Full Amazon → Sage sync |
| `sync_ebay_to_sage` | Full eBay → Sage sync |
| `sync_shopify_to_sage` | Full Shopify → Sage sync |

### Data Formatting
| Task | Description |
|------|-------------|
| `clean_spreadsheet` | Remove duplicates, fix formatting |
| `format_for_amazon` | Convert to Amazon upload format |
| `format_for_ebay` | Convert to eBay File Exchange format |
| `format_for_shopify` | Convert to Shopify CSV format |
| `validate_data` | Check for errors before upload |

### Tracking
| Task | Description |
|------|-------------|
| `pull_tracking_info` | Get tracking from FedEx/UPS |
| `sync_tracking_to_platforms` | Upload tracking to Amazon/eBay |

### Reports
| Task | Description |
|------|-------------|
| `generate_sync_report` | Create sync summary |
| `send_email_report` | Email report to recipients |

## 🔄 Daily Sync Flow

The agent runs two scheduled syncs daily:

### Morning Sync (8:00 AM)
1. Pull unshipped orders from Amazon/eBay/Shopify
2. Format and push to Sage Quantum
3. Pull tracking numbers from Sage
4. Get tracking status from FedEx/UPS
5. Upload tracking to platforms
6. Send summary report

### Noon Sync (12:00 PM)
- Same flow as morning sync
- Catches any orders missed in morning

## 📊 Report Example

```
M&M 2.0 Sync Report - Daily Morning
Completed: 2024-01-15 08:45

📊 SYNC OVERVIEW
  • Orders Processed: 28
  • Products Synced: 0
  • Tracking Updated: 15

🛒 AMAZON
  • Orders: 18
  • Success: 18
  • Failed: 0

🏷️ EBAY
  • Orders: 10
  • Success: 9
  • Failed: 1

⚠️ ERRORS (1)
  • Row 5: Missing required field: CustomerRef

✅ Report emailed to: team@company.com
```

## 🖥️ CLI Commands

```powershell
# Run in foreground (for testing)
mnm-agent run

# Service management
mnm-agent install      # Install Windows service
mnm-agent uninstall    # Remove service
mnm-agent start        # Start service
mnm-agent stop         # Stop service
mnm-agent status       # Show status

# Testing
mnm-agent test-sage    # Test Sage connection
mnm-agent test-backend # Test backend connection

# Utilities
mnm-agent logs         # View recent logs
mnm-agent init config.env  # Create config template
```

## 📁 Project Structure

```
MNM-Fasteners-Agent/
├── agent/
│   ├── communication/     # WebSocket & polling
│   ├── sage50/           # Sage Quantum integration
│   ├── tracking/         # FedEx/UPS APIs
│   ├── transform/        # Data cleaning & formatting
│   ├── reports/          # Report generation & email
│   ├── executor/         # Task queue & execution
│   ├── core.py           # Main agent service
│   ├── cli.py            # Command-line interface
│   └── windows_service.py # Windows service wrapper
├── server/               # Example backend API
├── scripts/              # Install/uninstall scripts
└── tests/                # Unit tests
```

## 🔒 Security

- **JWT Authentication** for WebSocket connections
- **API Keys** for REST endpoints
- **TLS/SSL** for all communication
- **Local credentials** - Sage credentials never leave the device
- **Encrypted config** support (optional)

## 📚 Integration with Your Backend

Your backend needs to implement these endpoints:

### WebSocket
- `ws://api.yourserver.com/agent/ws` - Real-time task communication

### REST (Polling Fallback)
- `GET /api/v1/agents/{id}/tasks` - Fetch pending tasks
- `POST /api/v1/agents/{id}/tasks/{taskId}/result` - Submit results
- `POST /api/v1/agents/{id}/heartbeat` - Health check

See `server/api_example.py` for a complete FastAPI implementation.

## 🐛 Troubleshooting

### Agent won't connect to Sage
1. Verify Sage Quantum is installed and running
2. Check `SAGE_COMPANY_PATH` or `SAGE_ODBC_DSN` is correct
3. Run `mnm-agent test-sage -v` for detailed output

### No tracking info returned
1. Verify carrier API credentials are correct
2. Check tracking number format is valid
3. Some carriers have API rate limits

### Email reports not sending
1. Check SMTP settings in config
2. For Gmail, use an "App Password" not your regular password
3. Verify `REPORT_RECIPIENTS` has valid emails

### Service won't start
1. Check Windows Event Viewer for errors
2. Run `mnm-agent run` to see console output
3. Ensure Python is in system PATH

## 📄 License

MIT License - See LICENSE file for details.

---

**M&M 2.0** - Automating ecommerce data flow since 2024
