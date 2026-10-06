"""Score one eval run: did the diagnosis get it right, and was it grounded?

Pure code, no LLM judge: the same diagnosis always gets the same score.
"""
from evals.scenario import Scenario

OPUS_PRICE = (4.00, 20.00)   # $ per 1M input / output tokens, Claude Opus 5.5


def score(s: Scenario, final_state: dict) -> dict:
    messages = final_state["messages"]
    tools_used = [c["name"] for m in messages for c in (getattr(m, "tool_calls", None) or [])]
    usage = [m.usage_metadata for m in messages if getattr(m, "usage_metadata", None)]
    tokens_in = sum(u["input_tokens"] for u in usage)
    tokens_out = sum(u["output_tokens"] for u in usage)

    base = {
        "scenario": s.id,
        "runbook_covered": s.runbook_covered,
        "intent": final_state.get("intent"),
        "tool_calls": len(tools_used),
        "tools_used": tools_used,
        # agent-loop tokens only: excludes the Haiku guardrail and the diagnose call
        "cost_usd": round((tokens_in * OPUS_PRICE[0] + tokens_out * OPUS_PRICE[1]) / 1e6, 3),
    }

    d = final_state.get("diagnosis")
    if d is None:
        if s.expected_category == "no_problem_found":
            # Healthy system, answered without raising an incident: that's the right outcome
            return {**base, "correct": True, "grounded": True, "confidently_wrong": False,
                    "category_got": "(no incident)", "confidence": "-"}
        # Routed as a plain question (or refused): a real failure a user would hit
        return {**base, "correct": False, "failure": "no diagnosis: not routed as an incident"}

    text = f"{d['summary']} {d['root_cause']}".lower()
    checks = {
        "category_ok": d["category"] == s.expected_category,
        "mentions_ok": all(w.lower() in text for w in s.must_mention),
        "no_red_herring": not any(w.lower() in text for w in s.must_not_mention),
        "grounded": d["grounding"]["ungrounded"] == 0,
    }
    correct = checks["category_ok"] and checks["mentions_ok"] and checks["no_red_herring"]
    return {
        **base, **checks,
        "correct": correct,
        "category_got": d["category"],
        "confidence": d["confidence"],
        # the dangerous kind of wrong: an on-call engineer would act on it
        "confidently_wrong": (not correct) and d["confidence"] == "high",
        "root_cause": d["root_cause"],
    }
