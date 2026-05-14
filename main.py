import os
import asyncio
import httpx
from fastapi import FastAPI
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
client = Anthropic()

AGENTPHONE_API_KEY = os.getenv("AGENTPHONE_API_KEY")
AGENTPHONE_AGENT_ID = os.getenv("AGENTPHONE_AGENT_ID")

# In-memory call context — keyed by callId, lives for duration of one call
call_context: dict = {}


@app.get("/")
async def health():
    return {"status": "gtm-agent running"}


@app.post("/webhook/call")
async def handle_call(event: dict):
    call_id = event.get("callId")
    event_type = event.get("type")

    print(f"[{event_type}] callId={call_id}")

    if event_type == "call.started":
        call_context[call_id] = {
            "history": [],
            "lead": {"name": "Test Lead", "company": "Test Co"},
        }
        return {"text": "Hey, this is Alex — I'm testing an AI sales agent. Can you say something back to me?"}

    elif event_type == "agent.message":
        ctx = call_context.get(call_id, {"history": [], "lead": {}})
        user_text = event.get("content", "")
        ctx["history"].append({"role": "user", "content": user_text})

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            system=(
                "You are a friendly sales agent doing a quick intro call. "
                "Keep every response under 2 sentences. Be warm and conversational."
            ),
            messages=ctx["history"],
        )

        reply = response.content[0].text
        ctx["history"].append({"role": "assistant", "content": reply})
        call_context[call_id] = ctx

        print(f"  user: {user_text}")
        print(f"  agent: {reply}")

        return {"text": reply}

    elif event_type == "call.ended":
        print(f"[call.ended] callId={call_id}")
        call_context.pop(call_id, None)
        return {}

    return {}


@app.post("/trigger-call")
async def trigger_call(to: str):
    """Fire a test call to any phone number."""
    async with httpx.AsyncClient() as http:
        resp = await http.post(
            "https://api.agentphone.ai/v1/calls",
            json={"agentId": AGENTPHONE_AGENT_ID, "toNumber": to},
            headers={"Authorization": f"Bearer {AGENTPHONE_API_KEY}"},
        )
    return resp.json()
