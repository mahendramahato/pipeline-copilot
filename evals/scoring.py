"""Score one eval run: did the diagnosis get it right, and was it grounded?

Pure code, no LLM judge: the same diagnosis always gets the same score.
"""
from evals.scenario import Scenario

# $ per 1M tokens: (input, output, cached input read, cache write), standard tier.
# Claude cache writes cost 1.25x input; OpenAI has no write surcharge.
# Check provider pricing pages before relying on these.
PRICES = {
    "claude-opus-5-5":   (4.00, 20.00, 0.20, 5.00),
    "claude-sonnet-5-5": (2.00, 10.00, 0.20, 2.50),
    "gpt-6-astra":       (10.00, 50.00, 1.00, 10.00),
    "gpt-6.1-sol":       (2.00, 10.00, 0.10, 2.00),
    "gpt-6-sol":         (2.00, 10.00, 0.20, 2.00),
    "gpt-5.6-sol":       (4.00, 20.00, 0.40, 4.00),
    "gpt-5.6-terra":     (2.00, 12.00, 0.20, 2.00),
}


def _cost(u: dict, price: tuple) -> float:
    # input_tokens includes cached tokens; split them out so caching shows in the cost
    p_in, p_out, p_read, p_write = price
    d = u.get("input_token_details") or {}
    read, write = d.get("cache_read", 0), d.get("cache_creation", 0)
    uncached = u["input_tokens"] - read - write
    return (uncached * p_in + read * p_read + write * p_write + u["output_tokens"] * p_out) / 1e6


def score(s: Scenario, final_state: dict, model: str) -> dict:
    messages = final_state["messages"]
    tools_used = [c["name"] for m in messages for c in (getattr(m, "tool_calls", None) or [])]
    usage = [m.usage_metadata for m in messages if getattr(m, "usage_metadata", None)]
    price = PRICES.get(model)

    base = {
        "scenario": s.id,
        "runbook_covered": s.runbook_covered,
        "intent": final_state.get("intent"),
        "tool_calls": len(tools_used),
        "tools_used": tools_used,
        # agent-loop tokens only: excludes the guardrail and the diagnose call.
        # None when the model isn't in PRICES (cost unknown, not zero).
        "cost_usd": round(sum(_cost(u, price) for u in usage), 3) if price else None,
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
