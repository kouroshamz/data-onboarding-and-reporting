"""LLM integration for data onboarding pipeline.

Three layers:
  L1 — Type Inspector:   detect hidden/misclassified types in string columns
  L2 — Insight Detector: surface anomalies and good-to-know facts
  L3 — Report Advisor:   decide content layout and section narratives
"""

from app.llm.client import BaseLLMClient, create_llm_client
from app.llm.service import LLMService

__all__ = ["BaseLLMClient", "create_llm_client", "LLMService"]
