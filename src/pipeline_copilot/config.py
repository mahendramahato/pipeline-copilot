"""Settings for pipeline-copilot, loaded once from .env."""
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from pipeline_copilot.llm import API_KEY_ENV, provider_for

# --- Load .env into os.environ ---
# Values already set in your shell win over .env (override=False is the
# default), so you can try another model for a single run without editing
# the file:   LLM_MODEL=claude-sonnet-5-5 uv run ...
load_dotenv()

# --- Project root ---
# Paths are resolved from here, so they work no matter which folder a process
# (e.g. an MCP server subprocess) was started from.
PROJECT_ROOT = Path(__file__).resolve().parents[2]    # src/pipeline_copilot/config.py → project root


# --- Required settings fail loudly at startup ---
# A missing value raises here, with the variable's name, instead of turning
# into a confusing None error deep inside an Airflow or Claude call.
def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing {name}. Set it in .env (see .env.example).")
    return value


# --- Agent settings: only what the agent process itself needs ---
@dataclass(frozen=True)
class AgentSettings:
    llm_model: str
    guardrail_model: str
    memory_db: Path
    # Remediation: off unless ACTIONS_ENABLED=true. When off, fixes are still
    # proposed (and shown), but never offered for one-click execution.
    actions_enabled: bool = False
    validation_poll_seconds: float = 15       # how often to re-check a run after a fix
    validation_timeout_seconds: float = 900   # give up waiting after this long
    container_settle_seconds: float = 90      # after a restart, wait before re-checking


def load_agent_settings() -> AgentSettings:
    llm_model = os.environ.get("LLM_MODEL", "claude-opus-5-5")
    # Small, fast model for classification: far cheaper per question than the agent
    guardrail_model = os.environ.get("GUARDRAIL_MODEL", "claude-haiku-4-5")
    # Each provider's SDK reads its own key; we only check the keys of the
    # providers actually in use exist (e.g. no OpenAI key needed for Claude-only).
    for model in {llm_model, guardrail_model}:
        _require(API_KEY_ENV[provider_for(model)])
    return AgentSettings(
        llm_model=llm_model,
        guardrail_model=guardrail_model,
        # Conversation checkpoints. Relative to the project root, like .chroma/
        memory_db=PROJECT_ROOT / os.environ.get("MEMORY_DB", "memory.sqlite"),
        actions_enabled=actions_enabled(),
        validation_poll_seconds=float(os.environ.get("VALIDATION_POLL_SECONDS", "15")),
        validation_timeout_seconds=float(os.environ.get("VALIDATION_TIMEOUT_SECONDS", "900")),
        container_settle_seconds=float(os.environ.get("CONTAINER_SETTLE_SECONDS", "90")),
    )


def actions_enabled() -> bool:
    return os.environ.get("ACTIONS_ENABLED", "").strip().lower() in ("1", "true", "yes")


# --- Airflow settings: only the Airflow MCP server loads these ---
# repr=False keeps the password out of prints and tracebacks.
@dataclass(frozen=True)
class AirflowSettings:
    base_url: str
    username: str
    password: str = field(repr=False)


def load_airflow_settings() -> AirflowSettings:
    return AirflowSettings(
        base_url=_require("AIRFLOW_BASE_URL").rstrip("/"),
        username=_require("AIRFLOW_USERNAME"),
        password=_require("AIRFLOW_PASSWORD"),
    )


# --- Airflow ACTIONS settings: only the actions MCP server loads these ---
# A different Airflow user from the read-only one: its role may clear tasks and
# trigger runs of daily_lake_maintenance, nothing else.
def load_airflow_actions_settings() -> AirflowSettings:
    return AirflowSettings(
        base_url=_require("AIRFLOW_BASE_URL").rstrip("/"),
        username=_require("AIRFLOW_ACTIONS_USERNAME"),
        password=_require("AIRFLOW_ACTIONS_PASSWORD"),
    )


# --- Athena settings: only the Athena MCP server loads these ---
# No secrets here: AWS keys stay in ~/.aws/credentials, selected by profile.
@dataclass(frozen=True)
class AthenaSettings:
    aws_profile: str
    aws_region: str
    workgroup: str
    database: str


def load_athena_settings() -> AthenaSettings:
    return AthenaSettings(
        aws_profile=_require("AWS_PROFILE"),
        aws_region=_require("AWS_REGION"),
        workgroup=_require("ATHENA_WORKGROUP"),
        database=_require("ATHENA_DATABASE"),
    )


# --- Knowledge base (RAG) settings ---
@dataclass(frozen=True)
class KnowledgeSettings:
    knowledge_dir: Path
    chroma_path: Path


def load_knowledge_settings() -> KnowledgeSettings:
    return KnowledgeSettings(
        knowledge_dir=PROJECT_ROOT / "knowledge",
        chroma_path=PROJECT_ROOT / os.environ.get("CHROMA_PATH", ".chroma"),
    )
