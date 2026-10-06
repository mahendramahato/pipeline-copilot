"""Tests for the evidence grounding check. No LLM, no AWS: pure function."""
from pipeline_copilot.output_guardrail import check_grounding

TOOL_OUTPUTS = [
    ("get_registered_partitions", "raw_seismic: 4 registered partitions (2026-09-27 to 2026-09-30)\n2026-09-27, ..."),
    ("run_query", "date | n\n2026-09-30 | 72\n(4 rows; scanned 0.00 MB)"),
]


def make_diagnosis(evidence: list[dict], confidence: str = "high") -> dict:
    return {"summary": "s", "category": "silent_data_gap", "root_cause": "r", "evidence": evidence,
            "impact": "i", "suggested_fix": "f", "runbooks_used": [], "unverified": [],
            "confidence": confidence}


def ev(tool: str, quote: str) -> dict:
    return {"tool": tool, "quote": quote, "meaning": "m"}


def test_exact_quote_is_grounded():
    d = check_grounding(make_diagnosis([ev("get_registered_partitions", "4 registered partitions (2026-09-27 to 2026-09-30)")]), TOOL_OUTPUTS)
    assert d["evidence"][0]["grounded"] and d["confidence"] == "high"

def test_reflowed_whitespace_still_grounded():
    # the multi-line quote from the real run, with the line break turned into spaces
    d = check_grounding(make_diagnosis([ev("run_query", "2026-09-30 | 72   (4 rows; scanned 0.00 MB)")]), TOOL_OUTPUTS)
    assert d["evidence"][0]["grounded"]

def test_paraphrase_is_not_grounded():
    d = check_grounding(make_diagnosis([ev("run_query", "2026-09-30 had 72 rows")]), TOOL_OUTPUTS)
    assert not d["evidence"][0]["grounded"]

def test_changed_number_is_not_grounded():
    # the dangerous case: looks exactly like real output, one digit off
    d = check_grounding(make_diagnosis([ev("run_query", "2026-09-30 | 27")]), TOOL_OUTPUTS)
    assert not d["evidence"][0]["grounded"]

def test_quote_from_wrong_tool_is_not_grounded():
    d = check_grounding(make_diagnosis([ev("get_task_log", "2026-09-30 | 72")]), TOOL_OUTPUTS)
    assert not d["evidence"][0]["grounded"]

def test_empty_quote_is_not_grounded():
    d = check_grounding(make_diagnosis([ev("run_query", "   ")]), TOOL_OUTPUTS)
    assert not d["evidence"][0]["grounded"]

def test_partly_ungrounded_caps_high_to_medium():
    d = check_grounding(make_diagnosis([
        ev("run_query", "2026-09-30 | 72"),          # real
        ev("run_query", "2026-10-01 | 0"),           # invented
    ]), TOOL_OUTPUTS)
    assert d["confidence"] == "medium"
    assert d["grounding"] == {"checked": 2, "ungrounded": 1, "confidence_lowered_from": "high"}

def test_nothing_grounded_forces_low():
    d = check_grounding(make_diagnosis([ev("run_query", "invented")], confidence="medium"), TOOL_OUTPUTS)
    assert d["confidence"] == "low"

def test_no_evidence_forces_low():
    assert check_grounding(make_diagnosis([]), TOOL_OUTPUTS)["confidence"] == "low"

def test_low_confidence_is_never_raised():
    d = check_grounding(make_diagnosis([ev("run_query", "2026-09-30 | 72")], confidence="low"), TOOL_OUTPUTS)
    assert d["confidence"] == "low"
