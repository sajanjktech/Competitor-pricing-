# llm_client.py

import os
import json
from config import get_llm_client

client = get_llm_client()

MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")


def call_llm(system_prompt: str, user_payload: dict):
    """Call Azure LLM enforcing STRICT JSON output."""

    # IMPORTANT FIX:
    # Use ensure_ascii=False when sending JSON to LLM
    # so characters like “ ’ ” are not escaped as \u2019 
    user_json = json.dumps(user_payload, ensure_ascii=False)

    response = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        response_format={"type": "json_object"},  # Azure-supported
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_json}
        ]
    )

    # IMPORTANT FIX:
    # Ensure LLM output is parsed with UTF-8 characters intact
    return json.loads(response.choices[0].message.content)
