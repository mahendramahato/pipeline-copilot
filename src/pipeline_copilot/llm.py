"""Chat model factory: the provider is picked from the model name, so switching
providers is just LLM_MODEL / GUARDRAIL_MODEL in .env (plus that provider's API key).
Each provider's SDK reads its own key from the environment.
"""
from langchain_core.language_models import BaseChatModel


def provider_for(model: str) -> str:
    if model.startswith("claude-"):
        return "anthropic"
    if model.startswith(("gpt-", "o1", "o3", "o4")):
        return "openai"
    raise ValueError(f"Unknown provider for model {model!r}: expected a claude-* or gpt-* model name.")


API_KEY_ENV = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}


def chat_model(model: str, max_tokens: int) -> BaseChatModel:
    # Imports are local so a provider's package only loads when it's used
    if provider_for(model) == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model, max_tokens=max_tokens)
    from langchain_openai import ChatOpenAI
    # Responses API: newer OpenAI reasoning models (e.g. gpt-5.6-*) only accept
    # function tools there, not on the older /v1/chat/completions endpoint.
    return ChatOpenAI(model=model, max_tokens=max_tokens, use_responses_api=True)
