from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from google import genai  # pip install google-genai


class LLMUnavailableError(RuntimeError):
    pass


@dataclass
class LLMClient:
    """
    Gemini client wrapper (Google GenAI SDK).
    - complete(prompt) -> str
    - extract_json(text) -> dict
    """
    model: str = "gemini-2.5-flash"
    api_key: Optional[str] = None
    _client: Any = None

    @staticmethod
    def from_env(model: str = "gemini-2.5-flash") -> "LLMClient":
        # If both are set, GOOGLE_API_KEY can take precedence (per SDK docs),
        # but we accept either.
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise LLMUnavailableError("Set GEMINI_API_KEY or GOOGLE_API_KEY environment variable.")
        return LLMClient(model=model, api_key=api_key)

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        if not self.api_key:
            raise LLMUnavailableError("Missing api_key.")
        # Official SDK client
        self._client = genai.Client(api_key=self.api_key)

    def complete(self, prompt: str) -> str:
        """
        Returns model output as plain text.
        """
        self._ensure_client()

        resp = self._client.models.generate_content(
            model=self.model,
            contents=prompt,
        )

        # SDK returns a response object; .text is the simplest way to get the final text.
        text = getattr(resp, "text", None)
        if not text:
            # Fallback: stringify full response if needed
            text = str(resp)
        return text

    def extract_json(self, text: str) -> Dict[str, Any]:
        """
        Extract JSON dict from model output.
        Supports:
        - pure JSON
        - JSON wrapped in ```json ... ```
        - extra text around JSON
        """
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Empty LLM response, cannot extract JSON.")

        # 1) Try direct parse
        try:
            return json.loads(text)
        except Exception:
            pass

        # 2) Try fenced block ```json ... ```
        fence = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if fence:
            return json.loads(fence.group(1))

        # 3) Try first {...} block
        brace = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if brace:
            return json.loads(brace.group(1))

        raise ValueError("Could not extract JSON from LLM output.")