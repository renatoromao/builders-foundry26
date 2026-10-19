"""Deploy the Demo 1 agent to Foundry Agent Service as a hosted agent (code deployment, no Docker).

Follows the Python path of https://learn.microsoft.com/azure/foundry/agents/quickstarts/quickstart-hosted-agent:
zip the source, `create_version_from_code`, wait until the version is active, route 100% of traffic to it,
then invoke it once.

    uv run python scripts/deploy_hosted_agent.py --stage-only   # build hosting/build/ and the zip only
    uv run python scripts/deploy_hosted_agent.py                # deploy (creates a billable hosted agent version)

The hosted copy runs in the same DEMO_MODE as your .env (mock mode ships the mock MCP servers inside the bundle).
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "hosting" / "build"
ZIP = ROOT / "hosting" / "account-risk-agent.zip"
FORWARDED_SETTINGS = [
    "DEMO_MODE", "ORCHESTRATOR_MODEL", "AGENT_TEMPERATURE", "DEMO_TODAY", "ENABLE_SENSITIVE_DATA",
    "FABRIC_IQ_MCP_URL", "FABRIC_IQ_CONNECTION_ID", "FABRIC_IQ_TOKEN_SCOPE",
    "WORK_IQ_MCP_URL", "WORK_IQ_CONNECTION_ID", "WORK_IQ_TOKEN_SCOPE",
    "APPLICATIONINSIGHTS_CONNECTION_STRING",
]


def stage() -> Path:
    """hosting/build = main.py + requirements.txt at the root, plus the account_risk and mocks packages."""
    shutil.rmtree(BUILD, ignore_errors=True)
    BUILD.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", "_generate.py")
    shutil.copytree(ROOT / "src" / "account_risk", BUILD / "account_risk", ignore=ignore)
    shutil.copytree(ROOT / "mocks", BUILD / "mocks", ignore=ignore)
    (BUILD / "main.py").write_text("from account_risk.hosting import main\n\nmain()\n")
    requirements = subprocess.run(
        ["uv", "export", "--no-dev", "--extra", "hosting", "--no-emit-project", "--no-hashes",
         "--format", "requirements-txt", "--quiet"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout
    (BUILD / "requirements.txt").write_text(requirements)

    with zipfile.ZipFile(ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in BUILD.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(BUILD))
    print(f"Staged {BUILD.relative_to(ROOT)} and {ZIP.relative_to(ROOT)} ({ZIP.stat().st_size // 1024} KB)")
    return ZIP


def deploy(zip_path: Path, agent_name: str) -> None:
    from azure.ai.projects import AIProjectClient
    from azure.ai.projects.models import (
        AgentEndpointConfig,
        CodeConfiguration,
        CodeDependencyResolution,
        FixedRatioVersionSelectionRule,
        HostedAgentDefinition,
        ProtocolConfiguration,
        ProtocolVersionRecord,
        ResponsesProtocolConfiguration,
        VersionSelector,
    )
    from azure.identity import DefaultAzureCredential

    from account_risk.config import get_settings

    settings = get_settings()
    env = {"FOUNDRY_PROJECT_ENDPOINT": settings.foundry_project_endpoint, "FOUNDRY_MODEL": settings.models.orchestrator}
    env |= {name: value for name in FORWARDED_SETTINGS if (value := _env(name))}

    with (
        zip_path.open("rb") as code,
        DefaultAzureCredential() as credential,
        AIProjectClient(endpoint=settings.foundry_project_endpoint, credential=credential) as project,
    ):
        created = project.agents.create_version_from_code(
            agent_name=agent_name,
            description="Douro Cellars Account Risk Agent (The Builders Foundry London 2026 demo).",
            definition=HostedAgentDefinition(
                cpu="0.5",
                memory="1Gi",
                code_configuration=CodeConfiguration(
                    runtime="python_3_14",
                    entry_point=["python", "main.py"],
                    dependency_resolution=CodeDependencyResolution.REMOTE_BUILD,
                ),
                environment_variables=env,
                protocol_versions=[ProtocolVersionRecord(protocol="responses", version="2.0.0")],
            ),
            code=code,
        )
        print(f"Created hosted agent {agent_name} version {created.version}; waiting for it to become active…")
        for attempt in range(60):
            time.sleep(10)
            status = project.agents.get_version(agent_name=agent_name, agent_version=created.version)["status"]
            print(f"  status: {status} ({attempt + 1}/60)")
            if status == "active":
                break
            if status == "failed":
                sys.exit("Hosted agent provisioning failed: check the build logs in the Foundry portal.")

        project.agents.update_details(
            agent_name=agent_name,
            agent_endpoint=AgentEndpointConfig(
                version_selector=VersionSelector(version_selection_rules=[
                    FixedRatioVersionSelectionRule(agent_version=created.version, traffic_percentage=100),
                ]),
                protocol_configuration=ProtocolConfiguration(responses=ResponsesProtocolConfiguration()),
            ),
        )
        with project.get_openai_client(agent_name=agent_name) as client:
            answer = client.responses.create(input="Which key accounts are at risk this week? One line each.")
        print(f"\nHosted agent says:\n{answer.output_text}\n")
        print("Next: Foundry portal → Build → Agents → this agent → Publish → Teams and Microsoft 365 Copilot "
              "(see docs/HOSTING_AND_PUBLISHING.md).")


def _env(name: str) -> str | None:
    return os.environ.get(name) or None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stage-only", action="store_true", help="build the bundle and zip, deploy nothing")
    parser.add_argument("--agent-name", default="douro-account-risk-agent")
    args = parser.parse_args()
    bundle = stage()
    if not args.stage_only:
        deploy(bundle, args.agent_name)
