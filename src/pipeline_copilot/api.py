"""Web API for Pipeline Copilot: the same agent as the terminal chat, over HTTP.

    uv run pipeline-copilot-api        # http://127.0.0.1:8000

POST /api/chat streams the run as Server-Sent Events (one JSON event per step).
Binds to localhost only: there is no login, and the agent can read the pipeline's
data and logs.
"""
import asyncio
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from pipeline_copilot.config import PROJECT_ROOT
from pipeline_copilot.runtime import list_threads, load_thread, new_thread_id, open_agent, run_turn

FRONTEND_DIST = PROJECT_ROOT / "frontend" / "dist"


# --- Start the agent (MCP servers + memory) once, for the app's whole lifetime ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with open_agent() as agent:
        app.state.agent = agent
        yield


app = FastAPI(title="Pipeline Copilot", lifespan=lifespan)

# One run at a time per conversation: two overlapping runs on the same thread
# would interleave their messages in the saved history.
_thread_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    thread_id: str | None = Field(default=None, pattern=r"^chat-[0-9-]+$")


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event, default=str)}\n\n"


@app.get("/api/health")
async def health() -> dict:
    agent = app.state.agent
    servers: dict[str, list[str]] = {}
    for tool, server in agent.tool_servers.items():
        servers.setdefault(server, []).append(tool)
    return {"status": "ok", "model": agent.settings.llm_model,
            "tools": [t.name for t in agent.tools], "servers": servers}


@app.get("/api/threads")
async def threads() -> list[dict]:
    return await list_threads(app.state.agent)


@app.get("/api/threads/{thread_id}")
async def thread(thread_id: str) -> dict:
    data = await load_thread(app.state.agent, thread_id)
    if not data["turns"]:
        raise HTTPException(404, f"No conversation {thread_id}")
    return data


@app.post("/api/chat")
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


# --- Serve the built React app, if it exists (npm run build in frontend/) ---
# Mounted last so /api/* routes take priority.
if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")


def main() -> None:
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
