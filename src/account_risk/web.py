"""Stage UI for the three demos (The Builders Foundry look): `uv run account-risk web` → http://localhost:8000

Same agents, same workflow, same tools as the console demos. The browser receives a stream of events
(tool calls, agent messages, approval requests) and sends approvals back.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from agent_framework import (
    Agent,
    AgentExecutorResponse,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    Content,
    Message,
)
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from account_risk import console as ui
from account_risk.agents import create_risk_agent
from account_risk.app import friendly_error
from account_risk.config import ROOT, Settings, get_settings
from account_risk.runner import _show_hosted_tool_calls, auto_approve, needs_human
from account_risk.telemetry import setup_telemetry, trace_id, tracer
from account_risk.tools.actions import outbox_entries
from account_risk.workflow import AccountRiskWorkflow, build_workflow

log = logging.getLogger("account_risk.web")
STATIC = Path(__file__).parent / "static"
Emit = Callable[[dict[str, Any]], None]
_current_demo: ContextVar[str] = ContextVar("current_demo", default="demo1")


class Stage:
    """Everything one presenter needs between clicks."""

    settings: Settings
    tracing: str
    risk_agent: Agent
    risk_session: AgentSession
    workflow: AccountRiskWorkflow | None = None
    workflow_resources: AsyncExitStack | None = None
    pending: dict[str, tuple[str, asyncio.Future[bool]]] = {}  # approval id -> (demo, decision)
    running: dict[str, asyncio.Task[None]] = {}  # the latest run of each demo
    locks: dict[str, asyncio.Lock] = {"demo1": asyncio.Lock(), "demo2": asyncio.Lock(), "demo3": asyncio.Lock()}


stage = Stage()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    stage.settings = get_settings()
    stage.tracing = setup_telemetry(stage.settings)
    stage.risk_agent = create_risk_agent(stage.settings)
    async with stage.risk_agent:  # starts / connects both MCP tools once
        stage.risk_session = stage.risk_agent.create_session()
        yield
        await _close_workflow()


app = FastAPI(title="The Builders Foundry · Building Intelligent Agents with Microsoft Agent Framework", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# ============================================================================ plumbing


def _stream(demo: str, job: Callable[[Emit], Awaitable[None]], replace: bool = False) -> StreamingResponse:
    """Run `job` in the background and stream every event it emits to the browser.

    Demos are independent (Demo 2 waiting for an approval never blocks Demo 1). Within one demo, requests
    queue up; with `replace=True` a new run stops the previous one and rejects its pending approvals.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    emit: Emit = queue.put_nowait

    async def run() -> None:
        ui.set_sink(emit)
        _current_demo.set(demo)
        try:
            async with stage.locks[demo]:
                await _traced(demo, job, emit)
        except asyncio.CancelledError:  # stopped while still queued behind the previous run
            emit({"type": "error", "message": "Stopped: a new request was started.", "hint": ""})
            emit({"type": "done", "trace_id": None})

    if replace:
        _stop(demo)
    stage.running[demo] = asyncio.create_task(run())

    async def events() -> AsyncIterator[str]:
        while True:
            event = await queue.get()
            yield f"data: {json.dumps(event, default=str, ensure_ascii=False)}\n\n"
            if event["type"] == "done":
                return

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


async def _traced(demo: str, job: Callable[[Emit], Awaitable[None]], emit: Emit) -> None:
    with tracer.start_as_current_span(f"account_risk.web.{demo}") as span:
        try:
            await asyncio.wait_for(job(emit), stage.settings.run_timeout_seconds * 5)
        except asyncio.CancelledError:
            emit({"type": "error", "message": "Stopped: a new request was started.", "hint": ""})
        except Exception as e:  # noqa: BLE001 - shown as a friendly card, details in logs/demo.log
            log.exception("Web job failed")
            message, hint = friendly_error(e)
            emit({"type": "error", "message": message, "hint": hint})
        finally:
            emit({"type": "done", "trace_id": trace_id(span)})


def _stop(demo: str) -> None:
    """Re-running a demo: reject its approvals still waiting and stop its previous run."""
    for key, (owner, future) in list(stage.pending.items()):
        if owner == demo and not future.done():
            future.set_result(False)
            stage.pending.pop(key)
    task = stage.running.get(demo)
    if task and not task.done():
        task.cancel()


async def _approve(emit: Emit, agent: str, request: Content) -> Content:
    """Show an approval card in the browser and wait for the presenter's click."""
    call = request.function_call
    approval_id = uuid.uuid4().hex
    future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
    stage.pending[approval_id] = (_current_demo.get(), future)
    emit({"type": "approval", "id": approval_id, "agent": agent, "tool": call.name,
          "args": call.parse_arguments() or {}})
    approved = await future
    emit({"type": "approval_result", "id": approval_id, "approved": approved})
    return request.to_function_approval_response(approved=approved)


async def _agent_turn(emit: Emit, agent: Agent, label: str, text: str, session: AgentSession) -> None:
    """One question to one agent, looping while it waits for approvals."""
    pending: Any = text
    before = len(outbox_entries())
    while True:
        emit({"type": "status", "agent": label, "state": "working"})
        response: AgentResponse = await asyncio.wait_for(agent.run(pending, session=session),
                                                         stage.settings.run_timeout_seconds)
        _show_hosted_tool_calls(label, (c for m in response.messages for c in m.contents))
        if response.text:
            emit({"type": "message", "agent": label, "text": response.text})
        requests = [r for r in response.user_input_requests if r.function_call is not None]
        if not requests:
            break
        if needs_human(label):
            emit({"type": "status", "agent": label, "state": "waiting"})
            pending = Message("user", list(await asyncio.gather(*(_approve(emit, label, r) for r in requests))))
        else:
            pending = Message("user", [auto_approve(label, r) for r in requests])
    emit({"type": "status", "agent": label, "state": "done"})
    _emit_outbox(emit, before)


def _emit_outbox(emit: Emit, before: int) -> None:
    for entry in outbox_entries()[before:]:
        emit({"type": "outbox", "entry": entry})


# ============================================================================ pages and info


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/info")
async def info() -> dict[str, Any]:
    s = stage.settings
    m = s.models
    return {
        "mode": s.demo_mode, "tracing": stage.tracing, "today": s.demo_today.isoformat(),
        "models": {"orchestrator": m.orchestrator, "data_analyst": m.data_analyst, "work_context": m.work_context,
                   "action": m.action, "data_analyst_alt": m.data_analyst_alt},
    }


@app.get("/api/sources")
async def sources() -> dict[str, Any]:
    """Demo 1, steps 1–2: the Fabric IQ ontology and the tools each source exposes."""
    result: dict[str, Any] = {}
    for tool in stage.risk_agent.mcp_tools:
        entry: dict[str, Any] = {"tools": [{"name": f.name, "description": (f.description or "").split("\n")[0]}
                                           for f in tool.functions]}
        if any(f.name == "list_ontology_entity_types" for f in tool.functions):
            raw = await tool.call_tool("list_ontology_entity_types")
            text = raw if isinstance(raw, str) else " ".join(getattr(c, "text", "") or "" for c in raw)
            entry["ontology"] = json.loads(text)
        result[tool.name] = entry
    s = stage.settings
    fabric = "mock MCP server (mocks/fabric_iq_mcp.py)" if s.fabric_iq_is_mock else "Fabric IQ (live)"
    work = "mock MCP server (mocks/work_iq_mcp.py)" if s.work_iq_is_mock else "Work IQ (live, Foundry connection)"
    result["endpoint"] = f"Fabric IQ: {fabric} · Work IQ: {work}"
    return result


@app.get("/api/code")
async def code() -> dict[str, str]:
    path = ROOT / "src/account_risk/agents/risk_agent.py"
    return {"path": str(path.relative_to(ROOT)), "code": path.read_text()}


@app.get("/api/outbox")
async def outbox() -> list[dict[str, Any]]:
    return outbox_entries()


class Ask(BaseModel):
    text: str


class Decision(BaseModel):
    id: str
    approved: bool


@app.post("/api/approve")
async def approve(decision: Decision) -> dict[str, bool]:
    entry = stage.pending.pop(decision.id, None)
    if entry and not entry[1].done():
        entry[1].set_result(decision.approved)
    return {"ok": entry is not None}


# ============================================================================ Demo 1


@app.post("/api/demo1/ask")
async def demo1_ask(ask: Ask) -> StreamingResponse:
    return _stream("demo1", lambda emit: _agent_turn(emit, stage.risk_agent, "risk_agent", ask.text, stage.risk_session))


@app.post("/api/demo1/reset")
async def demo1_reset() -> dict[str, bool]:
    stage.risk_session = stage.risk_agent.create_session()
    return {"ok": True}


# ============================================================================ Demo 2


async def _close_workflow() -> None:
    if stage.workflow_resources:
        try:
            await stage.workflow_resources.aclose()
        except Exception:  # noqa: BLE001 - a stale MCP process must not block the next run
            log.warning("Closing the previous workflow failed", exc_info=True)
    stage.workflow, stage.workflow_resources = None, None


@app.post("/api/demo2/run")
async def demo2_run(ask: Ask) -> StreamingResponse:
    async def job(emit: Emit) -> None:
        await _close_workflow()
        stage.workflow = build_workflow(stage.settings)
        stage.workflow_resources = AsyncExitStack()
        for agent in stage.workflow.agents:
            await stage.workflow_resources.enter_async_context(agent)

        before = len(outbox_entries())
        updates: dict[str, list[AgentResponseUpdate]] = defaultdict(list)
        started: set[str] = set()
        stream = stage.workflow.workflow.run(ask.text, stream=True)
        while True:
            approvals = await _workflow_events(emit, stream, updates, started)
            if not approvals:
                break
            responses = {rid: auto_approve(agent, req) for rid, (agent, req) in approvals.items() if not needs_human(agent)}
            human = {rid: (agent, req) for rid, (agent, req) in approvals.items() if needs_human(agent)}
            if human:
                emit({"type": "status", "agent": "action", "state": "waiting"})  # all approval cards at once
                decisions = await asyncio.gather(*(_approve(emit, agent, req) for agent, req in human.values()))
                responses |= dict(zip(human, decisions, strict=True))
            emit({"type": "status", "agent": "action", "state": "working"})
            stream = stage.workflow.workflow.run(stream=True, responses=responses)
        _emit_outbox(emit, before)

    return _stream("demo2", job, replace=True)


async def _workflow_events(emit: Emit, stream: Any, updates: dict[str, list[AgentResponseUpdate]],
                           started: set[str]) -> dict[str, tuple[str, Content]]:
    approvals: dict[str, tuple[str, Content]] = {}
    async for event in stream:
        agent = event.executor_id or ""
        if event.type == "executor_invoked" and agent in ui.AGENTS and agent not in started:
            started.add(agent)
            emit({"type": "status", "agent": agent, "state": "working"})
        elif event.type in ("output", "intermediate"):
            if isinstance(event.data, AgentResponseUpdate):
                updates[agent].append(event.data)
                _show_hosted_tool_calls(agent, event.data.contents)
            elif isinstance(event.data, str) and agent == "orchestrator":
                emit({"type": "message", "agent": "orchestrator", "title": "The plan", "text": event.data})
        elif event.type == "executor_completed" and agent in ui.AGENTS:
            text = AgentResponse.from_updates(updates.pop(agent)).text if updates.get(agent) else ""
            if not text:  # fall back to the response the executor sent downstream
                text = next((m.agent_response.text for m in event.data or [] if isinstance(m, AgentExecutorResponse)), "")
            if text:
                emit({"type": "message", "agent": agent, "text": text})
            emit({"type": "status", "agent": agent, "state": "done"})
        elif event.type == "request_info" and isinstance(event.data, Content):
            if event.data.type == "function_approval_request":
                approvals[event.request_id] = (event.source_executor_id or agent, event.data)
        elif event.type == "failed":
            raise RuntimeError(str(event.details))
    return approvals


@app.post("/api/demo2/followup")
async def demo2_followup(ask: Ask) -> StreamingResponse:
    async def job(emit: Emit) -> None:
        if not stage.workflow:
            raise RuntimeError("Run the workflow first, then ask a follow-up.")
        await _agent_turn(emit, stage.workflow.action_agent, "action", ask.text, stage.workflow.action_session)

    return _stream("demo2", job)


# ============================================================================ Demo 3


@app.post("/api/demo3/run")
async def demo3_run() -> StreamingResponse:
    """Run evals/run_evals.py --quick and stream its console output line by line."""

    async def job(emit: Emit) -> None:
        env = {**os.environ, "COLUMNS": "110", "PYTHONUNBUFFERED": "1"}
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(ROOT / "evals" / "run_evals.py"), "--quick",
            cwd=str(ROOT), env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        assert process.stdout is not None
        async for line in process.stdout:
            emit({"type": "line", "text": line.decode(errors="replace").rstrip("\n")})
        await process.wait()

    return _stream("demo3", job, replace=True)


def serve(port: int = 8000) -> None:
    import uvicorn

    ui.configure(verbose=False, pause=False, show_tools=True)
    print(f"Stage UI → http://localhost:{port}")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
