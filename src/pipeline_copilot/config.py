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


# --- The settings object ---
# frozen=True: settings can't be changed by accident while the agent runs.
# repr=False on the password: print(settings) or a traceback will show
# every field EXCEPT the password, so it never leaks into logs.
@dataclass(frozen=True)
class Settings:
    llm_model: str
    airflow_base_url: str
    airflow_username: str
    airflow_password: str = field(repr=False)


def load_settings() -> Settings:
    # ANTHROPIC_API_KEY isn't stored on Settings: the Anthropic SDK reads it
    # from the environment by itself. We only check it exists, so a missing
    # key fails here rather than on the first question you ask the agent.
    _require("ANTHROPIC_API_KEY")

    return Settings(
        llm_model=os.environ.get("LLM_MODEL", "claude-opus-5-5"),
        # rstrip("/") so joining paths later never produces "//api/v2"
        airflow_base_url=_require("AIRFLOW_BASE_URL").rstrip("/"),
        airflow_username=_require("AIRFLOW_USERNAME"),
        airflow_password=_require("AIRFLOW_PASSWORD"),
    )
