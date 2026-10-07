"""Web API for Pipeline Copilot: the same agent as the terminal chat, over HTTP.

    uv run pipeline-copilot-api        # http://127.0.0.1:8000

Public: the showcase (saved, reviewed investigations, read-only).
Owner only (password login): live chat and the saved conversation list.
POST /api/chat streams the run as Server-Sent Events (one JSON event per step).
"""
import asyncio
import json
import os
from collections import defaultdict
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from pipeline_copilot import auth, monitor
from pipeline_copilot.config import PROJECT_ROOT
from pipeline_copilot.runtime import list_threads, load_thread, new_thread_id, open_agent, run_turn
from pipeline_copilot.showcase import get_showcase, list_showcase

FRONTEND_DIST = PROJECT_ROOT / "frontend" / "dist"


# --- Start the agent (MCP servers + memory) once, for the app's whole lifetime ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with open_agent() as agent:
        app.state.agent = agent
        # The health monitor runs in the background when enabled (hosted instance)
        app.state.monitor = monitor.MonitorState(
            enabled=monitor.enabled(),
            interval_minutes=int(os.environ.get("MONITOR_INTERVAL_MINUTES", "30")),
        )
        task = asyncio.create_task(monitor.monitor_loop(agent, app.state.monitor)) if monitor.enabled() else None
        yield
        if task:
            task.cancel()


app = FastAPI(title="Pipeline Copilot", lifespan=lifespan)

# One run at a time per conversation: two overlapping runs on the same thread
# would interleave their messages in the saved history.
_thread_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    thread_id: str | None = Field(default=None, pattern=r"^chat-[0-9-]+$")


class LoginRequest(BaseModel):
    password: str = Field(min_length=1, max_length=200)


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event, default=str)}\n\n"


def _is_owner(request: Request) -> bool:
    # No password configured = local use: everything open (main() refuses a
    # public bind in that case, so this never applies to a hosted instance).
    return not auth.auth_enabled() or auth.is_valid_session(request.cookies.get(auth.COOKIE))


# --- Guard for owner-only endpoints ---
def require_owner(request: Request) -> None:
    if not _is_owner(request):
        raise HTTPException(401, "Sign in to use live mode.")


# ---------------- public ----------------

@app.get("/api/health")
async def health(request: Request) -> dict:
    agent = app.state.agent
    servers: dict[str, list[str]] = {}
    for tool, server in agent.tool_servers.items():
        servers.setdefault(server, []).append(tool)
    return {"status": "ok", "model": agent.settings.llm_model,
            "tools": [t.name for t in agent.tools], "servers": servers,
            "auth_enabled": auth.auth_enabled(), "owner": _is_owner(request)}


@app.get("/api/showcase")
async def showcase_list() -> list[dict]:
    return list_showcase()


@app.get("/api/showcase/{slug}")
async def showcase_item(slug: str) -> dict:
    item = get_showcase(slug)
    if item is None:
        raise HTTPException(404, "No such showcase investigation")
    return item


@app.post("/api/login")
async def login(req: LoginRequest, request: Request, response: Response) -> dict:
    client = request.client.host if request.client else "unknown"
    if auth.login_blocked(client):
        raise HTTPException(429, "Too many attempts. Try again in 15 minutes.")
    if not auth.check_password(client, req.password):
        raise HTTPException(401, "Wrong password.")
    response.set_cookie(
        auth.COOKIE, auth.new_session_token(), max_age=auth.SESSION_SECONDS,
        httponly=True,                              # page scripts can't read it
        secure=request.url.scheme == "https",       # HTTPS-only when hosted
        samesite="strict",                          # not sent on cross-site requests
    )
    return {"owner": True}


@app.post("/api/logout")
async def logout(response: Response) -> dict:
    response.delete_cookie(auth.COOKIE)
    return {"owner": False}


# ---------------- owner only ----------------

@app.get("/api/threads", dependencies=[Depends(require_owner)])
async def threads() -> list[dict]:
    return await list_threads(app.state.agent)


@app.get("/api/threads/{thread_id}", dependencies=[Depends(require_owner)])
async def thread(thread_id: str) -> dict:
    data = await load_thread(app.state.agent, thread_id)
    if not data["turns"]:
        raise HTTPException(404, f"No conversation {thread_id}")
    return data


@app.post("/api/chat", dependencies=[Depends(require_owner)])
async def chat(req: ChatRequest) -> StreamingResponse:
    thread_id = req.thread_id or new_thread_id()
    lock = _thread_locks[thread_id]
    if lock.locked():
        raise HTTPException(409, "This conversation is still answering the previous question.")

    async def stream():
        async with lock:
            # First event tells the browser which conversation this is (new ones get an id here)
            yield _sse({"type": "thread", "thread_id": thread_id})
            async for event in run_turn(app.state.agent, thread_id, req.message):
                yield _sse(event)
            yield _sse({"type": "done"})

    # no-cache + no proxy buffering, so each event reaches the browser immediately
    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/monitor", dependencies=[Depends(require_owner)])
async def monitor_status() -> dict:
    return app.state.monitor.public()


_monitor_lock = asyncio.Lock()


@app.post("/api/monitor/run", dependencies=[Depends(require_owner)])
async def monitor_run() -> dict:
    # Run a monitoring pass now (same logic as the schedule; one at a time)
    if _monitor_lock.locked():
        raise HTTPException(409, "A check is already running.")
    async with _monitor_lock:
        await monitor.run_once(app.state.agent, app.state.monitor)
    return app.state.monitor.public()


# --- Serve the built React app, if it exists (npm run build in frontend/) ---
# Mounted last so /api/* routes take priority.
if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")


def main() -> None:
    host = os.environ.get("COPILOT_HOST", "127.0.0.1")
    # Fail safe: never serve beyond this machine without a password set
    if host != "127.0.0.1" and not auth.auth_enabled():
        raise SystemExit("Refusing to listen on a public interface without COPILOT_PASSWORD set.")
    # proxy_headers: behind Caddy, trust X-Forwarded-Proto/For so HTTPS and the
    # client IP (used for login rate limits) are seen correctly
    uvicorn.run(app, host=host, port=8000, proxy_headers=True, forwarded_allow_ips="*")


if __name__ == "__main__":
    main()
