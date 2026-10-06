"""One eval case: a question, a simulated world, and what a correct diagnosis contains."""
from dataclasses import dataclass, field

from evals.world import World


@dataclass
class Scenario:
    id: str
    question: str                    # what the user asks; must NOT give the answer away
    world: World
    expected_category: str           # Diagnosis.category of a correct answer
    must_mention: list[str]          # ALL must appear in root_cause (case-insensitive)
    must_not_mention: list[str] = field(default_factory=list)   # red herrings: wrong blame
    runbook_covered: bool = True     # is there a runbook for this failure? Reported separately
    notes: str = ""
