"""Settings for pipeline-copilot, loaded once from .env."""
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# --- Load .env into os.environ ---
# Values already set in your shell win over .env (override=False is the
# default), so you can try another model for a single run without editing
# the file:   LLM_MODEL=claude-sonnet-5-5 uv run ...
load_dotenv()


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


# --- Airflow settings: only the Airflow MCP server loads these ---
# repr=False keeps the password out of prints and tracebacks.
@dataclass(frozen=True)
class AirflowSettings:
    base_url: str
    username: str
    password: str = field(repr=False)


def load_agent_settings() -> AgentSettings:
    # The SDK reads ANTHROPIC_API_KEY itself; we only check it exists.
    _require("ANTHROPIC_API_KEY")
    return AgentSettings(llm_model=os.environ.get("LLM_MODEL", "claude-opus-5-5"))


def load_airflow_settings() -> AirflowSettings:
    return AirflowSettings(
        base_url=_require("AIRFLOW_BASE_URL").rstrip("/"),
        username=_require("AIRFLOW_USERNAME"),
        password=_require("AIRFLOW_PASSWORD"),
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

# --- Guardrail model ---
@dataclass(frozen=True)
class AgentSettings:
    llm_model: str
    guardrail_model: str


def load_agent_settings() -> AgentSettings:
    _require("ANTHROPIC_API_KEY")
    return AgentSettings(
        llm_model=os.environ.get("LLM_MODEL", "claude-opus-5-5"),
        # Small, fast model for classification: ~100x cheaper per question than the agent
        guardrail_model=os.environ.get("GUARDRAIL_MODEL", "claude-haiku-4-5"),
    )

# --- Knowledge base (RAG) settings ---
# Paths are resolved from the project root, so they work no matter which
# folder a process (e.g. an MCP server subprocess) was started from.
PROJECT_ROOT = Path(__file__).resolve().parents[2]    # src/pipeline_copilot/config.py → project root


@dataclass(frozen=True)
class KnowledgeSettings:
    knowledge_dir: Path
    chroma_path: Path


def load_knowledge_settings() -> KnowledgeSettings:
    return KnowledgeSettings(
        knowledge_dir=PROJECT_ROOT / "knowledge",
        chroma_path=PROJECT_ROOT / os.environ.get("CHROMA_PATH", ".chroma"),
    )
