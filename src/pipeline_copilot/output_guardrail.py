"""Output guardrail: every evidence quote must really appear in a tool result.

Pure function, no LLM: the check is deterministic code, so a confident-sounding
but invented quote can't pass. It proves evidence EXISTS. It can't prove the
reasoning drawn from that evidence is right.
"""
import re


# Collapse runs of whitespace (newlines included): an LLM copying text may
# reflow line breaks, which isn't fabrication. Every other character must match.
def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def check_grounding(diagnosis: dict, tool_outputs: list[tuple[str, str]]) -> dict:
    """Return a copy of `diagnosis` with each evidence item marked grounded or not,
    and confidence lowered if evidence is ungrounded.

    tool_outputs: (tool_name, output_text) for every tool result in this turn.
    """
    outputs_by_tool: dict[str, list[str]] = {}
    for name, text in tool_outputs:
        outputs_by_tool.setdefault(name, []).append(_norm(text))

    # A quote is grounded only if it appears in the output of the SAME tool it
    # cites: citing a tool that was never called counts as ungrounded too.
    evidence = []
    for item in diagnosis["evidence"]:
        quote = _norm(item["quote"])
        grounded = bool(quote) and any(quote in out for out in outputs_by_tool.get(item["tool"], []))
        evidence.append({**item, "grounded": grounded})

    n_grounded = sum(e["grounded"] for e in evidence)
    confidence = diagnosis["confidence"]
    if n_grounded == 0:
        confidence = "low"                       # nothing verified → can't be confident
    elif n_grounded < len(evidence) and confidence == "high":
        confidence = "medium"                    # some claims unverified → cap it

    return {
        **diagnosis,
        "evidence": evidence,
        "confidence": confidence,
        "grounding": {
            "checked": len(evidence),
            "ungrounded": len(evidence) - n_grounded,
            "confidence_lowered_from": diagnosis["confidence"] if confidence != diagnosis["confidence"] else None,
        },
    }
