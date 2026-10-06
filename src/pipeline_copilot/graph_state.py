"""The graph's state: the conversation plus per-turn routing and results."""
from langgraph.graph import MessagesState


# MessagesState gives us `messages` with its append reducer. The extra fields
# have no reducer, so each update simply REPLACES the previous value; the
# guardrail resets them at the start of every turn.
class AgentState(MessagesState):
    intent: str | None          # "question" or "incident", set by the guardrail
    diagnosis: dict | None      # a Diagnosis as a dict (plain JSON is easy to checkpoint)
