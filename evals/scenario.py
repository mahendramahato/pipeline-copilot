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
    # Acceptable fix plans. Each option is a list of action matchers: a plan matches an
    # option when every matcher is met by a proposed action (matching only the keys
    # given) and every proposed action meets a matcher. [[]] = "propose nothing".
    fix_options: list[list[dict]] = field(default_factory=lambda: [[]])
    # What an honest validation reports after the fix. "partially_resolved" when the fix
    # helps but the root cause stays (e.g. a restart while the VM is still out of memory).
    expected_outcome: str = "resolved"
    notes: str = ""

    @property
    def expects_fix(self) -> bool:
        return any(self.fix_options)
