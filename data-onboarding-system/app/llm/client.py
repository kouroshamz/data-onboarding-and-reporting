"""Abstract LLM client and provider factory."""

from __future__ import annotations

import abc
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from loguru import logger


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LLMResponse:
    """Normalised response from any provider."""
    content: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    cached: bool = False
    cost_usd: float = 0.0


@dataclass
class LLMUsageEntry:
    """Single call usage record."""
    layer: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    cost_usd: float
    cached: bool = False
    timestamp: str = ""


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseLLMClient(abc.ABC):
    """Provider-agnostic LLM interface."""

    @abc.abstractmethod
    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        temperature: float = 0.1,
        max_tokens: int = 2000,
        response_json: bool = True,
    ) -> LLMResponse:
        """Send messages and return structured response."""

    @abc.abstractmethod
    def name(self) -> str:
        """Provider display name."""


# ---------------------------------------------------------------------------
# OpenAI provider
# ---------------------------------------------------------------------------

class OpenAIClient(BaseLLMClient):
    """OpenAI / Azure OpenAI provider."""

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        base_url: Optional[str] = None,
    ):
        try:
            import openai
        except ImportError:
            raise ImportError("openai package required: pip install openai")
        kwargs: Dict[str, Any] = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = openai.OpenAI(**kwargs)
        self._model = model

    def name(self) -> str:
        return f"openai/{self._model}"

    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        temperature: float = 0.1,
        max_tokens: int = 2000,
        response_json: bool = True,
    ) -> LLMResponse:
        kwargs: Dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_json:
            kwargs["response_format"] = {"type": "json_object"}

        t0 = time.time()
        resp = self._client.chat.completions.create(**kwargs)
        latency = (time.time() - t0) * 1000

        usage = resp.usage
        return LLMResponse(
            content=resp.choices[0].message.content or "",
            model=self._model,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            latency_ms=round(latency, 1),
        )


# ---------------------------------------------------------------------------
# Anthropic provider
# ---------------------------------------------------------------------------

class AnthropicClient(BaseLLMClient):
    """Anthropic Claude provider."""

    def __init__(
        self,
        api_key: str,
        model: str = "claude-3-5-haiku-20241022",
        base_url: Optional[str] = None,
    ):
        try:
            import anthropic
        except ImportError:
            raise ImportError("anthropic package required: pip install anthropic")
        kwargs: Dict[str, Any] = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = anthropic.Anthropic(**kwargs)
        self._model = model

    def name(self) -> str:
        return f"anthropic/{self._model}"

    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        temperature: float = 0.1,
        max_tokens: int = 2000,
        response_json: bool = True,
    ) -> LLMResponse:
        # Anthropic uses system message separately
        system_msg = ""
        chat_msgs = []
        for m in messages:
            if m["role"] == "system":
                system_msg = m["content"]
            else:
                chat_msgs.append(m)

        if response_json and system_msg:
            system_msg += "\n\nIMPORTANT: Respond ONLY with valid JSON."

        t0 = time.time()
        resp = self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_msg or "Respond only with valid JSON.",
            messages=chat_msgs,
        )
        latency = (time.time() - t0) * 1000

        content = resp.content[0].text if resp.content else ""
        return LLMResponse(
            content=content,
            model=self._model,
            input_tokens=resp.usage.input_tokens if resp.usage else 0,
            output_tokens=resp.usage.output_tokens if resp.usage else 0,
            latency_ms=round(latency, 1),
        )


# ---------------------------------------------------------------------------
# Local / Ollama provider (OpenAI-compatible API)
# ---------------------------------------------------------------------------

class LocalClient(BaseLLMClient):
    """Local LLM via OpenAI-compatible endpoint (Ollama, vLLM, etc.)."""

    def __init__(
        self,
        base_url: str = "http://localhost:11434/v1",
        model: str = "llama3.2",
        api_key: str = "local",
    ):
        try:
            import openai
        except ImportError:
            raise ImportError("openai package required for local provider")
        self._client = openai.OpenAI(api_key=api_key, base_url=base_url)
        self._model = model

    def name(self) -> str:
        return f"local/{self._model}"

    def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        temperature: float = 0.1,
        max_tokens: int = 2000,
        response_json: bool = True,
    ) -> LLMResponse:
        kwargs: Dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        # Not all local servers support response_format
        try:
            if response_json:
                kwargs["response_format"] = {"type": "json_object"}
            t0 = time.time()
            resp = self._client.chat.completions.create(**kwargs)
            latency = (time.time() - t0) * 1000
        except Exception:
            # Retry without response_format
            kwargs.pop("response_format", None)
            t0 = time.time()
            resp = self._client.chat.completions.create(**kwargs)
            latency = (time.time() - t0) * 1000

        usage = resp.usage
        return LLMResponse(
            content=resp.choices[0].message.content or "",
            model=self._model,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            latency_ms=round(latency, 1),
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_llm_client(
    provider: str,
    model: str,
    api_key: str = "",
    base_url: Optional[str] = None,
) -> BaseLLMClient:
    """Create an LLM client for the given provider."""
    p = provider.lower().strip()
    if p == "openai":
        return OpenAIClient(api_key=api_key, model=model, base_url=base_url)
    elif p == "anthropic":
        return AnthropicClient(api_key=api_key, model=model, base_url=base_url)
    elif p in ("local", "ollama"):
        return LocalClient(
            base_url=base_url or "http://localhost:11434/v1",
            model=model,
            api_key=api_key or "local",
        )
    else:
        raise ValueError(f"Unknown LLM provider: {provider!r}. Use openai|anthropic|local")
