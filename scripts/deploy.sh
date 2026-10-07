#!/usr/bin/env bash
# Deploys the latest Pipeline Copilot on the server. GitHub Actions runs this over
# SSH after CI passes on main:
#   cd ~/pipeline-copilot && OLD=$(git rev-parse HEAD) \
#     && git pull --ff-only && bash scripts/deploy.sh "$OLD"
# The argument is the commit the server was on before the pull, for the log.
set -euo pipefail
cd "$(dirname "$0")/.."

OLD_COMMIT="${1:?usage: deploy.sh <commit before pull>}"
echo "Deploying $(git rev-parse --short HEAD) (was $(git rev-parse --short "$OLD_COMMIT"))"
echo "Changed files:"
CHANGED="$(git diff --name-only "$OLD_COMMIT" HEAD)"
echo "${CHANGED:-  (none)}" | sed 's/^/  /'

# Rebuild the image (cached layers keep code-only changes quick) and recreate the
# container if anything changed. On start it re-indexes knowledge/ (runbooks);
# conversations and incident memory live in the /data volume and are kept.
docker compose up -d --build

# --- Smoke test ---
# The API must answer inside the container (up to 2 min: startup launches the
# three MCP servers and rebuilds the runbook index).
for _ in $(seq 1 60); do
  if docker compose exec -T copilot python -c \
      "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=5)" \
      > /dev/null 2>&1; then
    echo "Deploy OK: copilot healthy"
    # The previous image is now unused: free its disk space (dangling images only)
    docker image prune -f > /dev/null
    exit 0
  fi
  sleep 2
done
echo "FAILED: copilot did not become healthy"
docker compose logs --tail 60 copilot
exit 1
