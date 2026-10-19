# Building Intelligent Agents with Microsoft Agent Framework — live demos

**The Builders Foundry · Software without hype.** — London, 19 October 2026

- Third talk, 7:35 PM · Renato Romão (Microsoft MVP & MCT)
- Event: https://www.meetup.com/the-builders-foundry/events/316554194/

Account Risk Agent: *"Which key accounts are at risk this week, why — and what should I do?"*
Microsoft Agent Framework + Microsoft Foundry, with Fabric IQ (the business) and Work IQ (the people).

## Quick start

```bash
uv sync
cp .env.example .env        # set FOUNDRY_PROJECT_ENDPOINT and FOUNDRY_MODEL
az login
uv run account-risk web     # stage UI with the three demos → http://localhost:8000
```

`DEMO_MODE=mock` runs everything on local mock MCP servers with synthetic data (only a model endpoint is needed).

| Demo | Browser | Terminal |
|---|---|---|
| 1 · Fabric IQ + Work IQ Agent | tab 1 | `uv run account-risk demo1` |
| 2 · Multi-Agent + Human Approval | tab 2 | `uv run account-risk demo2` |
| 3 · Evals & Tracing in Foundry | tab 3 | `uv run python evals/run_evals.py --quick` |

## Layout

- `src/account_risk/agents/` — Risk agent (Demo 1), Orchestrator, Data Analyst, Work Context, Action
- `src/account_risk/workflow.py` — plan → concurrent specialists → Action agent with tool approval
- `src/account_risk/tools/` — mock vs live wiring (only place that looks at `DEMO_MODE`)
- `src/account_risk/web.py`, `static/` — stage UI
- `mocks/` — mock Fabric IQ and Work IQ MCP servers + synthetic data
- `evals/` — dataset, Foundry evaluators, model comparison
- `fabric/` — the scenario tables as CSV and as a Fabric Warehouse SQL script
- `scripts/deploy_hosted_agent.py` — deploy the Demo 1 agent to Foundry Hosted Agents

Fabric IQ, Work IQ, Foundry Hosted Agents' Python hosting package and DevUI are preview (October 2026).
