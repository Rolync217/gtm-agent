import os
import asyncio
import httpx
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
client = Anthropic()

AGENTPHONE_API_KEY = os.getenv("AGENTPHONE_API_KEY", "").strip()
AGENTPHONE_AGENT_ID = os.getenv("AGENTPHONE_AGENT_ID", "").strip()
CAL_COM_API_KEY = os.getenv("CAL_COM_API_KEY", "").strip()
CAL_EVENT_URL = os.getenv("CAL_EVENT_URL", "https://cal.com/abhinav-anand-xdbyff/30-mins-discovery-call")
ZEPTOMAIL_TOKEN = os.getenv("SEND_MAIL_TOKEN_1", "").strip()

DEMO_PHONE = "+12142184795"
DEMO_EMAIL = "anandabhinav217@gmail.com"

call_context: dict = {}

# ---------------------------------------------------------------------------
# Demo lead profile
# ---------------------------------------------------------------------------
DEMO_LEAD = {
    "name": "Abhinav",
    "company": "Dataflow AI",
    "role": "Co-founder & CTO",
    "background": (
        "Raised a $1.2M pre-seed 4 months ago. Has 8 paying customers, all from warm intros. "
        "Tried Apollo for 6 weeks, got 1.8% reply rate, gave up. "
        "Describes his ICP as 'basically any B2B company that automates workflows.' "
        "Still spending 10+ hours a week doing sales himself."
    ),
}


# ---------------------------------------------------------------------------
# Cal.com — fetch real available slots at call start
# ---------------------------------------------------------------------------
async def get_available_slots() -> list[str]:
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            r = await http.get(
                "https://api.cal.com/v1/event-types",
                params={"apiKey": CAL_COM_API_KEY},
            )
            event_types = r.json().get("event_types", [])
            if not event_types:
                return []

            event_type_id = None
            for et in event_types:
                slug = et.get("slug", "")
                if "discovery" in slug or "30" in slug:
                    event_type_id = et["id"]
                    break
            if not event_type_id:
                event_type_id = event_types[0]["id"]

            now = datetime.now(timezone.utc)
            end = now + timedelta(days=7)

            r = await http.get(
                "https://api.cal.com/v1/slots",
                params={
                    "apiKey": CAL_COM_API_KEY,
                    "eventTypeId": event_type_id,
                    "startTime": now.isoformat(),
                    "endTime": end.isoformat(),
                },
            )
            slots_data = r.json().get("slots", {})

            readable = []
            for date_key in sorted(slots_data.keys()):
                for slot in slots_data[date_key]:
                    raw = slot.get("time", "")
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    ct = dt - timedelta(hours=5)
                    readable.append(ct.strftime("%A %b %d at %-I:%M %p CT"))
                    if len(readable) >= 3:
                        break
                if len(readable) >= 3:
                    break

            return readable
    except Exception as e:
        print(f"[cal.com] slot fetch failed: {e}")
        return []


def build_system_prompt(lead: dict, slots: list[str]) -> str:
    slots_text = (
        ", ".join(slots) if slots
        else "Tuesday or Wednesday this week (check cal.com link for exact times)"
    )
    return f"""You are Alex — direct, energetic, and sharp. You work for Rolync, an AI-powered GTM service that helps early-stage founders go from zero pipeline to qualified meetings on their calendar.

You're calling {lead['name']}, {lead['role']} at {lead['company']}.

INTEL:
{lead['background']}

YOUR GOAL: Qualify them, then book a 30-min discovery call.

QUALIFICATION — they're a fit if:
- Outbound hasn't worked (low reply rates, wrong targeting, gave up on tools)
- All customers came through warm intros and they need to break out of that
- They don't have a sharp, tested ICP
- They're doing 5+ hrs/week of sales themselves

CALL FLOW:
1. Open by referencing their specific situation — not a generic intro
2. Ask one sharp question at a time to surface the pain
3. When they show interest, pitch the outcome: "We figure out exactly who your best customers are, then book meetings with them. You pay when meetings happen."
4. Close: ask if they're open to a 30-min call
5. When they say YES, say: "Perfect — I'm pulling up the calendar right now. I have {slots_text}. Which of those works?"
6. When they confirm a time, say: "Got it. I have anandabhinav217@gmail.com on file — should I send the booking link there?"
7. When they confirm the email, say: "Done — sending it over right now."

RULES:
- Max 2-3 sentences per turn — this is a phone call
- Use {lead['name']}'s name naturally, not every turn
- If they push back once, acknowledge and redirect to the outcome
- If they're genuinely not interested after 2 tries, exit gracefully: "No worries at all — good luck with the pipeline. If it ever becomes a pain point, you know where to find us." Then say "Take care!" and end.
- You already know their background. Don't ask questions you already know the answer to.
- Once you've sent the booking link and they've acknowledged it, say "Perfect — talk soon, {lead['name']}!" and end the call. Do not keep talking.
"""


# ---------------------------------------------------------------------------
# Email — booking link via ZeptoMail
# ---------------------------------------------------------------------------
async def send_email_booking_link(to_email: str):
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.post(
                "https://api.zeptomail.com/v1.1/email",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": ZEPTOMAIL_TOKEN,
                },
                json={
                    "from": {"address": "abhinav.anand@rolync.com", "name": "Alex | Rolync"},
                    "to": [{"email_address": {"address": to_email, "name": DEMO_LEAD["name"]}}],
                    "subject": "Your 30-min discovery call with Rolync",
                    "htmlbody": (
                        f"<p>Hey {DEMO_LEAD['name']},</p>"
                        f"<p>Great speaking with you! Here's your booking link for our 30-min discovery call:</p>"
                        f"<p><a href='{CAL_EVENT_URL}'>{CAL_EVENT_URL}</a></p>"
                        f"<p>Talk soon,<br><strong>Abhinav Anand</strong><br>Founder & CEO, Rolync<br>+16514444766</p>"
                    ),
                },
            )
        print(f"[email] sent to {to_email} → {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"[email] failed: {e}")


def booking_confirmed(reply: str) -> bool:
    signals = ["sending it over", "sent it over", "sending you", "sent you",
               "sending the link", "sent the link"]
    return any(s in reply.lower() for s in signals)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def health():
    return {"status": "gtm-agent running"}


@app.post("/webhook/call")
async def handle_call(event: dict):
    event_type = event.get("event")
    channel = event.get("channel", "voice")
    data = event.get("data", {})
    call_id = data.get("callId")

    print(f"[{event_type}] channel={channel} callId={call_id}")

    if event_type == "agent.message" and channel == "voice":
        ctx = call_context.get(call_id)
        if not ctx:
            return {}

        user_text = data.get("transcript", "")
        ctx["history"].append({"role": "user", "content": user_text})
        print(f"  user: {user_text}")

        system = build_system_prompt(ctx["lead"], ctx["slots"])
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system=system,
            messages=ctx["history"],
        )
        reply = response.content[0].text
        ctx["history"].append({"role": "assistant", "content": reply})
        call_context[call_id] = ctx

        print(f"  agent: {reply}")

        if booking_confirmed(reply) and not ctx["booking_sent"]:
            ctx["booking_sent"] = True
            asyncio.create_task(send_email_booking_link(ctx.get("email", DEMO_EMAIL)))
            print(f"  [email booking link] → {ctx.get('email', DEMO_EMAIL)}")

        hangup_signals = ["talk soon", "take care", "goodbye", "disconnecting", "have a good"]
        should_hangup = ctx["booking_sent"] and any(s in reply.lower() for s in hangup_signals)

        return {"text": reply, "hangup": should_hangup}

    elif event_type == "agent.call_ended":
        transcript = data.get("transcript", [])
        duration = data.get("durationSeconds")
        sentiment = data.get("userSentiment", "unknown")
        successful = data.get("callSuccessful", False)

        print(f"[call_ended] callId={call_id} duration={duration}s sentiment={sentiment} success={successful}")
        for turn in transcript:
            print(f"  {turn.get('role')}: {turn.get('content')}")

        call_context.pop(call_id, None)
        return {}

    return {}


@app.post("/trigger-call")
async def trigger_call(to: str = DEMO_PHONE):
    slots = await get_available_slots()

    opening = (
        f"Hey {DEMO_LEAD['name']}! This is Alex from Rolync. "
        f"I was looking at Dataflow AI — congrats on the pre-seed raise. "
        f"I work with founders at exactly your stage on a pretty specific problem. "
        f"You got 60 seconds?"
    )

    async with httpx.AsyncClient() as http:
        resp = await http.post(
            "https://api.agentphone.ai/v1/calls",
            json={
                "agentId": AGENTPHONE_AGENT_ID,
                "toNumber": to,
                "initialGreeting": opening,
            },
            headers={"Authorization": f"Bearer {AGENTPHONE_API_KEY}"},
        )
    result = resp.json()
    call_id = result.get("id")

    if call_id:
        call_context[call_id] = {
            "history": [{"role": "assistant", "content": opening}],
            "lead": DEMO_LEAD,
            "slots": slots,
            "phone": to,
            "email": DEMO_EMAIL,
            "booking_sent": False,
        }

    print(f"[trigger-call] → {to} | callId={call_id} | slots={slots}")
    return result
