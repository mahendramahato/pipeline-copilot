"""Settings for pipeline-copilot, loaded once from .env."""
import os
from dataclasses import dataclass, field

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
