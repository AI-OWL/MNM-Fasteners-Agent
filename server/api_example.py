"""
Example API endpoints for agent communication.

This shows what your backend needs to implement to communicate
with the MNM Agent. You can integrate this into your existing
FastAPI/Flask/etc. backend.

Endpoints needed:
1. WebSocket: /agent/ws - Real-time communication
2. REST: /api/v1/agents/... - Polling fallback
"""

from datetime import datetime
from typing import Optional
import asyncio
import json
import uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Header
from fastapi.security import HTTPBearer
from pydantic import BaseModel
import jwt

app = FastAPI(title="MNM Agent Backend API")
security = HTTPBearer()

# Configuration (in production, load from env/config)
SECRET_KEY = "your-secret-key"
AGENT_SECRETS = {
    "mnm-agent-001": "agent-001-secret",
    # Add more agents as needed
}


# ===== Models =====

class TaskPayload(BaseModel):
    """Task to send to agent."""
    task_type: str
    payload: dict
    priority: str = "normal"
    timeout_seconds: int = 300


class TaskResult(BaseModel):
    """Result received from agent."""
    task_id: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None
    duration_ms: Optional[int] = None


class AgentStatus(BaseModel):
    """Agent status update."""
    agent_id: str
    status: str
    connection_type: str
    tasks_completed: int
    tasks_failed: int
    sage_connected: bool


# ===== In-memory stores (use Redis/DB in production) =====

connected_agents: dict[str, WebSocket] = {}
pending_tasks: dict[str, dict] = {}  # agent_id -> {task_id: task}
task_results: dict[str, TaskResult] = {}  # task_id -> result
agent_statuses: dict[str, AgentStatus] = {}


# ===== Authentication =====

def verify_agent_token(token: str) -> Optional[str]:
    """Verify agent JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload.get("agent_id")
    except jwt.InvalidTokenError:
        return None


def verify_api_key(
    authorization: str = Header(...),
    x_agent_id: str = Header(...),
    x_agent_secret: str = Header(...),
) -> str:
    """Verify API key for REST endpoints."""
    expected_secret = AGENT_SECRETS.get(x_agent_id)
    if not expected_secret or expected_secret != x_agent_secret:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return x_agent_id


# ===== WebSocket Endpoint =====

@app.websocket("/agent/ws")
async def agent_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for agent communication.
    
    Protocol:
    1. Agent connects with JWT token
    2. Agent sends registration message
    3. Server sends tasks, agent sends results
    4. Periodic heartbeats maintain connection
    """
    await websocket.accept()
    
    agent_id = None
    
    try:
        # Wait for registration message
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=30)
        message = json.loads(raw)
        
        if message.get("message_type") != "register":
            await websocket.close(code=4001, reason="Expected registration")
            return
        
        agent_id = message.get("agent_id")
        
        # Verify agent
        if agent_id not in AGENT_SECRETS:
            await websocket.close(code=4003, reason="Unknown agent")
            return
        
        # Store connection
        connected_agents[agent_id] = websocket
        print(f"Agent connected: {agent_id}")
        
        # Send acknowledgment
        await websocket.send_json({
            "message_id": str(uuid.uuid4()),
            "message_type": "ack",
            "timestamp": datetime.utcnow().isoformat(),
            "payload": {"status": "registered"},
        })
        
        # Initialize pending tasks queue for this agent
        if agent_id not in pending_tasks:
            pending_tasks[agent_id] = {}
        
        # Main message loop
        while True:
            raw = await websocket.receive_text()
            message = json.loads(raw)
            
            msg_type = message.get("message_type")
            
            if msg_type == "heartbeat":
                # Update last seen
                pass
                
            elif msg_type == "task_result":
                # Store task result
                result = TaskResult(**message.get("payload", {}))
                task_results[result.task_id] = result
                
                # Remove from pending
                pending_tasks[agent_id].pop(result.task_id, None)
                
                print(f"Task {result.task_id} completed: {result.status}")
                
            elif msg_type == "status_update":
                # Store agent status
                status = AgentStatus(**message.get("payload", {}))
                agent_statuses[agent_id] = status
    
    except WebSocketDisconnect:
        print(f"Agent disconnected: {agent_id}")
    except asyncio.TimeoutError:
        await websocket.close(code=4008, reason="Registration timeout")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        if agent_id:
            connected_agents.pop(agent_id, None)


async def send_task_to_agent(agent_id: str, task: dict) -> bool:
    """Send a task to a connected agent."""
    websocket = connected_agents.get(agent_id)
    if not websocket:
        return False
    
    try:
        await websocket.send_json({
            "message_id": str(uuid.uuid4()),
            "message_type": "task",
            "timestamp": datetime.utcnow().isoformat(),
            "payload": task,
        })
        return True
    except Exception as e:
        print(f"Failed to send task: {e}")
        return False


# ===== REST API Endpoints (Polling Fallback) =====

@app.post("/api/v1/agents/register")
async def register_agent(
    payload: dict,
    agent_id: str = Depends(verify_api_key),
):
    """Register agent (polling mode)."""
    return {"status": "registered", "agent_id": agent_id}


@app.get("/api/v1/agents/{agent_id}/tasks")
async def get_pending_tasks(
    agent_id: str,
    verified_agent: str = Depends(verify_api_key),
):
    """Get pending tasks for agent (polling mode)."""
    if agent_id != verified_agent:
        raise HTTPException(status_code=403, detail="Agent ID mismatch")
    
    tasks = list(pending_tasks.get(agent_id, {}).values())
    
    if not tasks:
        return {"tasks": []}
    
    return {"tasks": tasks}


@app.post("/api/v1/agents/{agent_id}/tasks/{task_id}/ack")
async def acknowledge_task(
    agent_id: str,
    task_id: str,
    verified_agent: str = Depends(verify_api_key),
):
    """Acknowledge task receipt (polling mode)."""
    return {"status": "acknowledged"}


@app.post("/api/v1/agents/{agent_id}/tasks/{task_id}/result")
async def submit_task_result(
    agent_id: str,
    task_id: str,
    result: TaskResult,
    verified_agent: str = Depends(verify_api_key),
):
    """Submit task result (polling mode)."""
    task_results[task_id] = result
    pending_tasks.get(agent_id, {}).pop(task_id, None)
    
    return {"status": "received"}


@app.post("/api/v1/agents/{agent_id}/heartbeat")
async def agent_heartbeat(
    agent_id: str,
    status: AgentStatus,
    verified_agent: str = Depends(verify_api_key),
):
    """Receive heartbeat from agent."""
    agent_statuses[agent_id] = status
    return {"status": "ok"}


@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ===== Admin Endpoints (for your backend to use) =====

@app.post("/api/v1/admin/tasks/create")
async def create_task(
    agent_id: str,
    task: TaskPayload,
):
    """
    Create a new task for an agent.
    
    This would be called by your backend when you want to
    send orders to Sage 50.
    """
    task_id = str(uuid.uuid4())
    
    task_data = {
        "task_id": task_id,
        "task_type": task.task_type,
        "priority": task.priority,
        "payload": task.payload,
        "timeout_seconds": task.timeout_seconds,
        "created_at": datetime.utcnow().isoformat(),
    }
    
    # Try WebSocket first
    if agent_id in connected_agents:
        success = await send_task_to_agent(agent_id, task_data)
        if success:
            pending_tasks.setdefault(agent_id, {})[task_id] = task_data
            return {"task_id": task_id, "delivery": "websocket"}
    
    # Fall back to queue for polling
    pending_tasks.setdefault(agent_id, {})[task_id] = task_data
    return {"task_id": task_id, "delivery": "queued"}


@app.get("/api/v1/admin/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Get task status and result."""
    result = task_results.get(task_id)
    
    if result:
        return {
            "task_id": task_id,
            "status": result.status,
            "result": result.result,
            "error": result.error,
        }
    
    # Check if pending
    for agent_tasks in pending_tasks.values():
        if task_id in agent_tasks:
            return {"task_id": task_id, "status": "pending"}
    
    raise HTTPException(status_code=404, detail="Task not found")


@app.get("/api/v1/admin/agents")
async def list_agents():
    """List all known agents and their status."""
    agents = []
    
    for agent_id, status in agent_statuses.items():
        agents.append({
            "agent_id": agent_id,
            "status": status.status,
            "connected": agent_id in connected_agents,
            "connection_type": status.connection_type,
            "sage_connected": status.sage_connected,
            "tasks_completed": status.tasks_completed,
            "tasks_failed": status.tasks_failed,
        })
    
    return {"agents": agents}


# ===== Example: Creating an order sync task =====

async def sync_orders_to_sage(agent_id: str, orders: list[dict]) -> str:
    """
    Example function to sync orders to Sage 50 via agent.
    
    Call this from your order processing pipeline.
    
    Args:
        agent_id: Target agent ID
        orders: List of order data in the expected format
        
    Returns:
        Task ID for tracking
    """
    task = TaskPayload(
        task_type="sync_orders",
        payload={
            "orders": orders,
            "platform": "amazon",  # or "ebay"
        },
        priority="high",
    )
    
    result = await create_task(agent_id, task)
    return result["task_id"]


# Example order format:
EXAMPLE_ORDER = {
    "order_ref": "AMZ-123456",
    "customer_ref": "AMAZON001",
    "order_date": "2024-01-15T10:30:00Z",
    "platform": "amazon",
    "platform_order_id": "111-2222222-3333333",
    "lines": [
        {
            "sku": "BOLT-M8X50",
            "description": "M8 x 50mm Hex Bolt",
            "quantity": 100,
            "unit_price": 0.15,
        },
        {
            "sku": "NUT-M8",
            "description": "M8 Hex Nut",
            "quantity": 100,
            "unit_price": 0.05,
        },
    ],
    "delivery_name": "John Smith",
    "delivery_address_1": "123 Main Street",
    "delivery_city": "London",
    "delivery_postcode": "SW1A 1AA",
    "shipping_cost": 5.99,
}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

