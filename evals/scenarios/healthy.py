"""Nothing is broken. Tests that the agent doesn't invent an incident."""
from datetime import datetime, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)


def build() -> Scenario:
    return Scenario(
        id="healthy_control",
        question="Is the curated data up to date this morning? Anything I should worry about?",
        world=healthy_world(NOW),
        expected_category="no_problem_found",
        must_mention=[],
        must_not_mention=[],
        runbook_covered=True,          # the overview doc describes what normal looks like
        notes="A false alarm here is a failure: an agent that cries wolf gets ignored.",
    )
