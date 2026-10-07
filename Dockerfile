# --- Stage 1: build the React app into static files ---
FROM node:22-slim AS ui
WORKDIR /ui
# Install exactly the versions in package-lock.json (npm ci = clean install)
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# --- Stage 2: the app (API + agent + MCP servers, which run as subprocesses) ---
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

# Run as an unprivileged user. Its uid should match the VM user that owns the
# mounted secret files (.env.deploy, ~/.aws-copilot, both chmod 600), so it can
# read them: set APP_UID (e.g. 1001 on the Oracle VM). /data (a volume) holds
# conversations and the vector store, so it must belong to that user too.
ARG APP_UID=1000
RUN useradd --create-home --uid ${APP_UID} app && mkdir /data /app && chown app /data /app
USER app
WORKDIR /app

# Dependencies first so code-only changes reuse this cached layer
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY --chown=app pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project
COPY --chown=app src/ src/
RUN uv sync --frozen --no-dev

COPY --chown=app knowledge/ knowledge/
COPY --chown=app showcase/ showcase/
COPY --chown=app --from=ui /ui/dist frontend/dist
ENV PATH="/app/.venv/bin:$PATH"

# Download the embedding model now, so the container starts without fetching it
RUN python -c "from chromadb.utils.embedding_functions import DefaultEmbeddingFunction as E; E()(['warm up'])"

# 0.0.0.0 = reachable from other containers (Caddy). No port is published:
# Caddy, on the same Docker network, is the only way in. The API refuses to
# start on this address unless COPILOT_PASSWORD is set.
ENV COPILOT_HOST=0.0.0.0
EXPOSE 8000

# Rebuild the runbook index from knowledge/ (incident memory is kept), then serve
CMD ["sh", "-c", "python -m pipeline_copilot.knowledge_base > /dev/null && exec pipeline-copilot-api"]
