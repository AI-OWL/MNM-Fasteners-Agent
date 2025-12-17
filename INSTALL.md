# M&M 2.0 Agent Installation Guide

## 📋 Prerequisites

Before installing, make sure the client's PC has:

1. **Windows 10/11** or Windows Server 2016+
2. **Python 3.10+** installed ([Download Python](https://www.python.org/downloads/))
3. **Sage Quantum** installed with data accessible
4. **Administrator access** to the PC

## 🚀 Installation Steps

### Step 1: Get the Agent Files

**Option A: Clone from Git**
```powershell
git clone https://github.com/your-org/MNM-Fasteners-Agent.git
cd MNM-Fasteners-Agent
```

**Option B: Download ZIP**
1. Download the ZIP file from your repository
2. Extract to `C:\MNMAgent\` or similar location

### Step 2: Run the Installer

Open **PowerShell as Administrator** and run:

```powershell
cd C:\path\to\MNM-Fasteners-Agent
.\scripts\install.ps1
```

This will:
- Create a Python virtual environment
- Install all dependencies
- Create necessary directories
- Set up the configuration file

### Step 3: Configure the Agent

Edit the configuration file:

```powershell
notepad C:\ProgramData\MNMAgent\config.env
```

**Required settings:**
```env
# Your unique agent ID (you provide this to the client)
AGENT_ID=client-001
AGENT_SECRET=secret-key-from-you

# Your backend server
BACKEND_URL=wss://api.yourserver.com/agent/ws
BACKEND_API_KEY=api-key-from-you

# Sage Quantum path (client needs to find this)
SAGE_COMPANY_PATH=C:\ProgramData\Sage\Accounts\Company.001
```

### Step 4: Test the Connection

```powershell
# Test Sage connection
mnm-agent test-sage

# Test backend connection
mnm-agent test-backend

# Run full demo (no server needed)
python -m agent.demo
```

### Step 5: Install as Windows Service

```powershell
# Install the service
mnm-agent install

# Start the service
mnm-agent start

# Check status
mnm-agent status
```

## 🧪 Testing Without Server

You can test all functionality locally without connecting to your server:

```powershell
# Run all demos
python -m agent.demo

# Just test report generation
python -m agent.demo --report

# Just test data formatting
python -m agent.demo --format

# Just test data validation
python -m agent.demo --validate

# Test Sage connection (requires Sage)
python -m agent.demo --sage

# Test email sending (requires SMTP config)
python -m agent.demo --email
```

## 📁 Where Files Are Located

| Location | Purpose |
|----------|---------|
| `C:\Program Files\MNMAgent\` | Agent installation |
| `C:\ProgramData\MNMAgent\config.env` | Configuration file |
| `C:\ProgramData\MNMAgent\logs\` | Log files |
| `C:\ProgramData\MNMAgent\data\` | Task queue persistence |
| `C:\ProgramData\MNMAgent\exports\` | Exported files |

## 🔧 Troubleshooting

### "Python not found"
Install Python 3.10+ from [python.org](https://www.python.org/downloads/) and check "Add to PATH" during installation.

### "Sage connection failed"
1. Make sure Sage Quantum is installed
2. Find the correct company path (usually in `C:\ProgramData\Sage\`)
3. The path should point to the `.001` company file

### "Service won't start"
1. Run `mnm-agent run` to see error messages
2. Check logs: `mnm-agent logs`
3. Make sure config.env has all required settings

### "Backend connection failed"
1. Check internet connectivity
2. Verify `BACKEND_URL` is correct
3. Check firewall allows outbound connections

## 📞 Support

If you encounter issues:
1. Check the logs: `C:\ProgramData\MNMAgent\logs\agent.log`
2. Run demo mode to test locally: `python -m agent.demo`
3. Contact support with agent ID and log excerpts

