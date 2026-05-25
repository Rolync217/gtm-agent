import os
import asyncio
import httpx
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
client = Anthropic()

# ── Env vars ──────────────────────────────────────────────────────────────────
AGENTPHONE_API_KEY  = os.getenv("AGENTPHONE_API_KEY", "").strip()
AGENTPHONE_AGENT_ID = os.getenv("AGENTPHONE_AGENT_ID", "").strip()
CAL_COM_API_KEY     = os.getenv("CAL_COM_API_KEY", "").strip()
CAL_EVENT_URL       = os.getenv("CAL_EVENT_URL", "https://cal.com/abhinav-anand-xdbyff/30-mins-discovery-call")
ZEPTOMAIL_TOKEN     = os.getenv("SEND_MAIL_TOKEN_1", "").strip()
APOLLO_API_KEY      = os.getenv("APOLLO_API_KEY", "").strip()
FIRECRAWL_API_KEY   = os.getenv("FIRECRAWL_API_KEY", "").strip()

DEMO_PHONE = "+12142184795"
DEMO_EMAIL = "anandabhinav217@gmail.com"

# ── In-memory state ───────────────────────────────────────────────────────────
call_context:    dict = {}
icp_history:     dict = {}
icp_locked:      dict = {}
leads_store:     dict = {}
pipeline_queues: dict = {}
call_streams:    dict = {}

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


# ── Utilities ─────────────────────────────────────────────────────────────────
def extract_json(text: str) -> dict:
    text = text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    return json.loads(text)


# ── MCP tool imports ──────────────────────────────────────────────────────────
from mcp_server.tools.firecrawl_v2 import search_web as _fc_search, scrape_web_page as _fc_scrape
from mcp_server.tools.yc_algolia import search_yc_companies
from mcp_server.tools.linkdapi import (
    search_people as _ld_search_people,
    get_profile as _ld_get_profile,
    get_profile_posts as _ld_get_posts,
    find_company_id as _ld_company_id,
    get_company_posts as _ld_company_posts,
)
from mcp_server.tools.apollo import find_person_by_name as _ap_find_person


# ── Agent tool definitions ────────────────────────────────────────────────────
AGENT_TOOLS = [
    {
        "name": "search_yc_companies",
        "description": "Search HN Algolia for YC companies matching ICP keywords. Call this first to get a list of candidates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keywords": {
                    "type": "array", "items": {"type": "string"},
                    "description": "2-4 product-category keywords from the ICP, e.g. ['saas', 'b2b', 'workflow']"
                },
                "batches": {
                    "type": "array", "items": {"type": "string"},
                    "description": "YC batches to search, defaults to ['W25', 'S24', 'W24']"
                },
            },
            "required": ["keywords"],
        },
    },
    {
        "name": "scrape_website",
        "description": "Scrape a company website to understand what they build and who they serve.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Full URL to scrape"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "search_web",
        "description": "Search the web via Firecrawl. Use for founder Twitter/X posts, news articles, funding announcements, or any public signals.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query string"},
                "limit": {"type": "integer", "description": "Number of results, 1-10, default 5"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_linkedin_people",
        "description": "Search LinkedIn for a person by name + company. Returns username and URN needed for other LinkedIn tools.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Full name to search, e.g. 'Jane Smith'"},
                "company": {"type": "string", "description": "Company name filter"},
                "title": {"type": "string", "description": "Title filter, e.g. 'founder' or 'ceo'"},
            },
            "required": ["keyword"],
        },
    },
    {
        "name": "get_linkedin_profile",
        "description": "Get a founder's full LinkedIn profile: headline, summary, experience, follower count.",
        "input_schema": {
            "type": "object",
            "properties": {
                "username": {"type": "string", "description": "LinkedIn username from search_linkedin_people"},
            },
            "required": ["username"],
        },
    },
    {
        "name": "get_founder_linkedin_posts",
        "description": "Get a founder's recent LinkedIn posts. Reveals what they're thinking about, pain points, announcements.",
        "input_schema": {
            "type": "object",
            "properties": {
                "urn": {"type": "string", "description": "Profile URN from search_linkedin_people or get_linkedin_profile"},
                "limit": {"type": "integer", "description": "Max posts to return, default 5"},
            },
            "required": ["urn"],
        },
    },
    {
        "name": "get_company_linkedin_posts",
        "description": "Get a company's recent LinkedIn posts. Good for understanding their messaging, product focus, and stage.",
        "input_schema": {
            "type": "object",
            "properties": {
                "company_name": {"type": "string", "description": "Company name to look up"},
            },
            "required": ["company_name"],
        },
    },
    {
        "name": "find_founder_contact",
        "description": "Find a founder's email and contact info via Apollo. Use after you know their first and last name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "first_name": {"type": "string"},
                "last_name": {"type": "string"},
                "organization_name": {"type": "string", "description": "Company name"},
                "domain": {"type": "string", "description": "Company website domain, e.g. acme.com"},
            },
            "required": ["first_name", "last_name"],
        },
    },
    {
        "name": "submit_lead",
        "description": "Submit the researched lead. Call this when you have enough context for a real cold call. This ends the research.",
        "input_schema": {
            "type": "object",
            "properties": {
                "company": {"type": "string"},
                "website": {"type": "string"},
                "batch": {"type": "string", "description": "YC batch e.g. W25"},
                "contact_name": {"type": "string", "description": "Founder full name"},
                "contact_role": {"type": "string", "description": "Founder title"},
                "contact_email": {"type": "string"},
                "contact_linkedin": {"type": "string"},
                "icp_score": {"type": "integer", "description": "Your ICP fit score 0-100"},
                "research_summary": {"type": "string", "description": "2-3 sentences on why this company fits the ICP"},
                "call_brief": {"type": "string", "description": "3-4 sentences of specific intel for the cold call: what they build, stage, likely pain, one signal to reference in opening"},
            },
            "required": ["company", "contact_name", "icp_score", "research_summary", "call_brief"],
        },
    },
]


# ── Agent tool executor ───────────────────────────────────────────────────────
async def execute_tool(name: str, inputs: dict) -> str:
    if name == "search_yc_companies":
        result = await asyncio.to_thread(
            search_yc_companies, inputs["keywords"], inputs.get("batches")
        )
        return json.dumps(result)

    elif name == "scrape_website":
        result = await asyncio.to_thread(_fc_scrape, inputs["url"])
        return json.dumps({"markdown": (result.get("markdown") or "")[:3000]})

    elif name == "search_web":
        result = await asyncio.to_thread(_fc_search, inputs["query"], inputs.get("limit", 5))
        return json.dumps(result)

    elif name == "search_linkedin_people":
        result = await asyncio.to_thread(
            _ld_search_people, inputs["keyword"], inputs.get("company", ""), inputs.get("title", "")
        )
        return json.dumps(result)

    elif name == "get_linkedin_profile":
        result = await asyncio.to_thread(_ld_get_profile, inputs["username"])
        return json.dumps(result)

    elif name == "get_founder_linkedin_posts":
        result = await asyncio.to_thread(_ld_get_posts, inputs["urn"], inputs.get("limit", 5))
        return json.dumps(result)

    elif name == "get_company_linkedin_posts":
        company_id = await asyncio.to_thread(_ld_company_id, inputs["company_name"])
        if not company_id:
            return json.dumps({"error": "Company not found on LinkedIn"})
        result = await asyncio.to_thread(_ld_company_posts, company_id)
        return json.dumps(result)

    elif name == "find_founder_contact":
        result = await asyncio.to_thread(
            _ap_find_person,
            inputs["first_name"], inputs["last_name"],
            inputs.get("organization_name", ""), inputs.get("domain", ""),
        )
        return json.dumps(result or {"error": "Not found in Apollo"})

    return json.dumps({"error": f"Unknown tool: {name}"})


# ── Pipeline ──────────────────────────────────────────────────────────────────
AGENT_SYSTEM = """You are a GTM research agent. Your job: find ONE company that fits the ICP, research them deeply, then call submit_lead.

ICP:
{icp}

GOAL: Build enough context about the founder and company to have a real, specific cold call — not a generic pitch.

SUGGESTED WORKFLOW (adapt as needed):
1. search_yc_companies with the ICP keywords to get candidates
2. Pick the most promising company from the results based on description
3. scrape_website to understand what they actually build
4. search_linkedin_people to find the founder, then get_linkedin_profile + get_founder_linkedin_posts
5. get_company_linkedin_posts for company signals and messaging
6. search_web for founder Twitter/X posts, news, funding announcements
7. find_founder_contact (Apollo) to get their email
8. submit_lead with everything you found

RULES:
- Research only 1 company — the best ICP fit from search results
- The call_brief must be specific: what they build, their stage, one signal that proves you did homework
- You have enough when you can fill submit_lead confidently"""


async def run_pipeline(session_id: str, icp: dict):
    queue = pipeline_queues[session_id]

    async def emit(etype: str, **kwargs):
        await queue.put({"type": etype, **kwargs})

    await emit("status", message="Agent starting research...")

    system = AGENT_SYSTEM.format(icp=json.dumps(icp, indent=2))
    messages: list[dict] = [{"role": "user", "content": "Find and research one qualified company now."}]
    lead_submitted = False

    for iteration in range(25):
        if lead_submitted:
            break

        def _call():
            return client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4096,
                system=system,
                tools=AGENT_TOOLS,
                messages=messages,
            )

        try:
            resp = await asyncio.to_thread(_call)
        except Exception as e:
            await emit("status", message=f"Agent error: {e}")
            break

        messages.append({"role": "assistant", "content": resp.content})

        if resp.stop_reason == "end_turn":
            break

        tool_results = []
        for block in resp.content:
            if not hasattr(block, "type") or block.type != "tool_use":
                continue

            name = block.name
            inputs = block.input

            # Emit a human-readable status for each tool call
            status_map = {
                "search_yc_companies": f"Searching YC for: {', '.join(inputs.get('keywords', []))}",
                "scrape_website":      f"Scraping {inputs.get('url', '')}...",
                "search_web":          f"Searching: {inputs.get('query', '')}",
                "search_linkedin_people": f"Looking up {inputs.get('keyword', '')} on LinkedIn...",
                "get_linkedin_profile":   f"Fetching LinkedIn profile @{inputs.get('username', '')}...",
                "get_founder_linkedin_posts": "Fetching founder's LinkedIn posts...",
                "get_company_linkedin_posts": f"Fetching {inputs.get('company_name', '')} LinkedIn posts...",
                "find_founder_contact": f"Finding contact for {inputs.get('first_name', '')} {inputs.get('last_name', '')} via Apollo...",
                "submit_lead":         f"Submitting lead: {inputs.get('company', '')}",
            }
            if name in status_map:
                await emit("status", message=status_map[name])

            if name == "submit_lead":
                lead_id = str(uuid.uuid4())
                lead = {
                    "id":               lead_id,
                    "company":          inputs.get("company", ""),
                    "website":          inputs.get("website", ""),
                    "batch":            inputs.get("batch", ""),
                    "contact_name":     inputs.get("contact_name", ""),
                    "contact_role":     inputs.get("contact_role", "Founder"),
                    "contact_email":    inputs.get("contact_email", ""),
                    "contact_linkedin": inputs.get("contact_linkedin", ""),
                    "icp_score":        inputs.get("icp_score", 0),
                    "research_summary": inputs.get("research_summary", ""),
                    "call_brief":       inputs.get("call_brief", ""),
                    "status":           "sourced",
                }
                leads_store[lead_id] = lead
                await emit("lead", lead=lead)
                lead_submitted = True
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps({"ok": True, "lead_id": lead_id}),
                })
            else:
                try:
                    result = await execute_tool(name, inputs)
                except Exception as e:
                    result = json.dumps({"error": str(e)})
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

    await emit("done", count=1 if lead_submitted else 0)


# ── ICP discovery ─────────────────────────────────────────────────────────────
ICP_SYSTEM = """You are a sharp GTM consultant helping a founder define their Ideal Customer Profile.

Ask ONE focused question per turn to extract:
- What the product does and its core value
- Who their best current customers are (industry, company size, role)
- The specific pain it solves
- Target company stage and size
- What a good vs bad customer looks like

After 4-5 exchanges, when you have a clear picture, respond with ONLY this (no other text):

LOCKED_ICP
{"industry": "...", "stage": "Seed / Series A", "company_size": "1-50 employees", "target_roles": ["co-founder", "ceo"], "pain_points": ["...", "..."], "hn_keywords": ["word1", "word2", "word3"]}

hn_keywords: 2-4 words describing what the TARGET COMPANIES BUILD (their product category), not the pain you solve for them. These appear in Launch HN post titles like "AI-powered [keyword]" or "[keyword] platform for [audience]". Example: if targeting B2B SaaS founders, use ["saas", "b2b", "workflow"] not ["outbound", "pipeline"]."""


@app.post("/icp/message")
async def icp_message(body: dict):
    session_id = body.get("session_id") or str(uuid.uuid4())
    icp_history.setdefault(session_id, [])
    icp_history[session_id].append({"role": "user", "content": body.get("message", "")})

    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=ICP_SYSTEM,
        messages=icp_history[session_id],
    )
    reply = resp.content[0].text
    icp_history[session_id].append({"role": "assistant", "content": reply})

    locked, icp_data, display = False, None, reply
    if "LOCKED_ICP" in reply:
        try:
            icp_data = extract_json(reply.split("LOCKED_ICP")[1].strip())
            icp_locked[session_id] = icp_data
            locked = True
            display = "Perfect — I have everything I need. Here's your ICP. Ready to find leads."
        except Exception as e:
            print(f"[icp] parse error: {e}")

    return {"session_id": session_id, "reply": display, "locked": locked, "icp": icp_data}


@app.post("/icp/preset")
async def icp_preset(body: dict):
    """Skip ICP chat — load a pre-built ICP directly and return a session ready for sourcing."""
    icp = body.get("icp")
    if not icp:
        return {"error": "icp required"}
    session_id = str(uuid.uuid4())
    icp_locked[session_id] = icp
    icp_history[session_id] = []
    return {"session_id": session_id, "icp": icp}


@app.post("/leads/source")
async def trigger_sourcing(body: dict):
    session_id = body.get("session_id")
    icp = icp_locked.get(session_id)
    if not icp:
        return {"error": "ICP not locked"}
    pipeline_queues[session_id] = asyncio.Queue()
    asyncio.create_task(run_pipeline(session_id, icp))
    return {"ok": True}


@app.get("/leads/stream/{session_id}")
async def leads_stream(session_id: str):
    async def generate():
        for _ in range(10):
            if session_id in pipeline_queues:
                break
            await asyncio.sleep(0.5)
        q = pipeline_queues.get(session_id)
        if not q:
            yield f"data: {json.dumps({'type': 'error'})}\n\n"
            return
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=180.0)
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("type") == "done":
                    break
            except asyncio.TimeoutError:
                break

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/leads")
async def get_leads():
    return list(leads_store.values())


# ── Cal.com ───────────────────────────────────────────────────────────────────
async def get_available_slots() -> list[str]:
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            r = await http.get("https://api.cal.com/v1/event-types", params={"apiKey": CAL_COM_API_KEY})
            event_types = r.json().get("event_types", [])
            if not event_types:
                return []
            eid = next(
                (e["id"] for e in event_types if "discovery" in e.get("slug", "") or "30" in e.get("slug", "")),
                event_types[0]["id"],
            )
            now = datetime.now(timezone.utc)
            r = await http.get("https://api.cal.com/v1/slots", params={
                "apiKey": CAL_COM_API_KEY, "eventTypeId": eid,
                "startTime": now.isoformat(), "endTime": (now + timedelta(days=7)).isoformat(),
            })
            readable = []
            for dk in sorted(r.json().get("slots", {}).keys()):
                for slot in r.json()["slots"][dk]:
                    dt = datetime.fromisoformat(slot["time"].replace("Z", "+00:00")) - timedelta(hours=5)
                    readable.append(dt.strftime("%A %b %d at %-I:%M %p CT"))
                    if len(readable) >= 3:
                        break
                if len(readable) >= 3:
                    break
            return readable
    except Exception as e:
        print(f"[cal] {e}")
        return []


# ── Call agent ────────────────────────────────────────────────────────────────
def build_system_prompt(lead: dict, slots: list[str]) -> str:
    slots_text = ", ".join(slots) if slots else "Tuesday or Wednesday this week"
    return f"""You are Alex — direct, energetic, sharp. You work for Rolync, an AI GTM service that helps early-stage founders get qualified meetings on their calendar.

You're calling {lead['name']}, {lead['role']} at {lead['company']}.

INTEL:
{lead['background']}

YOUR GOAL: Qualify them, then book a 30-min discovery call.

QUALIFICATION — they're a fit if:
- Outbound hasn't worked or they haven't started it
- All customers came from warm intros and they need to break out
- They're doing 5+ hrs/week of sales themselves

CALL FLOW:
1. Open by referencing their specific situation — not a generic intro
2. Ask one sharp question at a time to surface the pain
3. When they show interest: "We figure out exactly who your best customers are, then book meetings with them. You pay when meetings happen."
4. Close: ask if they're open to a 30-min call
5. When YES: "Perfect — I have {slots_text}. Which works?"
6. When they confirm a time: "Got it. I have {DEMO_EMAIL} on file — should I send the booking link there?"
7. When they confirm email: "Done — sending it over right now."

RULES:
- Max 2-3 sentences per turn — this is a phone call
- If they push back once, acknowledge and redirect to the outcome
- If genuinely not interested after 2 tries: "No worries — good luck with the pipeline. Take care!" then end.
- Once booking sent and acknowledged: "Perfect — talk soon, {lead['name']}!" and end."""


async def send_email_booking_link(to_email: str):
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            await http.post(
                "https://api.zeptomail.com/v1.1/email",
                headers={"Accept": "application/json", "Content-Type": "application/json",
                         "Authorization": ZEPTOMAIL_TOKEN},
                json={
                    "from": {"address": "abhinav.anand@rolync.com", "name": "Rolync"},
                    "to": [{"email_address": {"address": to_email, "name": "Founder"}}],
                    "subject": "Your 30-min discovery call with Rolync",
                    "htmlbody": f"<p>Here's your booking link: <a href='{CAL_EVENT_URL}'>{CAL_EVENT_URL}</a></p><p>— Abhinav, Rolync</p>",
                },
            )
    except Exception as e:
        print(f"[email] {e}")


async def send_sms_booking_link(to_number: str):
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            await http.post(
                "https://api.agentphone.ai/v1/messages",
                headers={"Authorization": f"Bearer {AGENTPHONE_API_KEY}", "Content-Type": "application/json"},
                json={"agent_id": AGENTPHONE_AGENT_ID, "to_number": to_number,
                      "body": f"Here's your booking link: {CAL_EVENT_URL}"},
            )
    except Exception as e:
        print(f"[sms] {e}")


def booking_confirmed(reply: str) -> bool:
    return any(s in reply.lower() for s in
               ["sending it over", "sent it over", "sending you", "sent you", "sending the link", "sent the link"])


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def health():
    return {"status": "gtm-agent running"}


@app.get("/app", response_class=HTMLResponse)
async def serve_app():
    with open("static/index.html") as f:
        return HTMLResponse(content=f.read())


@app.post("/leads/{lead_id}/call")
async def call_lead(lead_id: str):
    lead = leads_store.get(lead_id)
    if not lead:
        return {"error": "Lead not found"}

    slots = await get_available_slots()
    first = lead["contact_name"].split()[0] if lead["contact_name"] else "there"
    opening = (
        f"Hey {first}! This is Alex from Rolync. I was just looking at {lead['company']} — "
        f"really interesting what you're building. Got 60 seconds?"
    )

    async with httpx.AsyncClient(timeout=10.0) as http:
        resp = await http.post(
            "https://api.agentphone.ai/v1/calls",
            json={"agentId": AGENTPHONE_AGENT_ID, "toNumber": DEMO_PHONE, "initialGreeting": opening},
            headers={"Authorization": f"Bearer {AGENTPHONE_API_KEY}"},
        )
    result = resp.json()
    call_id = result.get("id")

    if call_id:
        lead_profile = {
            "name": first,
            "company": lead["company"],
            "role": lead["contact_role"],
            "background": lead["call_brief"],
        }
        call_context[call_id] = {
            "history": [{"role": "assistant", "content": opening}],
            "lead": lead_profile,
            "slots": slots,
            "phone": DEMO_PHONE,
            "email": DEMO_EMAIL,
            "booking_sent": False,
        }
        call_streams[call_id] = asyncio.Queue()
        await call_streams[call_id].put({"role": "assistant", "content": opening})
        leads_store[lead_id]["status"] = "called"
        leads_store[lead_id]["call_id"] = call_id

    return {"call_id": call_id}


@app.get("/call/{call_id}/stream")
async def call_stream(call_id: str):
    async def generate():
        for _ in range(30):
            if call_id in call_streams:
                break
            await asyncio.sleep(0.5)
        q = call_streams.get(call_id)
        if not q:
            return
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=300.0)
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("hangup"):
                    break
            except asyncio.TimeoutError:
                break

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/webhook/call")
async def handle_call(event: dict):
    etype = event.get("event")
    channel = event.get("channel", "voice")
    data = event.get("data", {})
    call_id = data.get("callId")
    print(f"[{etype}] channel={channel} callId={call_id}")

    if etype == "agent.message" and channel == "voice":
        ctx = call_context.get(call_id)
        if not ctx:
            return {}

        user_text = data.get("transcript", "")
        ctx["history"].append({"role": "user", "content": user_text})
        print(f"  user: {user_text}")

        if call_id in call_streams:
            asyncio.create_task(call_streams[call_id].put({"role": "user", "content": user_text}))

        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system=build_system_prompt(ctx["lead"], ctx["slots"]),
            messages=ctx["history"],
        )
        reply = resp.content[0].text
        ctx["history"].append({"role": "assistant", "content": reply})
        call_context[call_id] = ctx
        print(f"  agent: {reply}")

        if booking_confirmed(reply) and not ctx["booking_sent"]:
            ctx["booking_sent"] = True
            asyncio.create_task(send_sms_booking_link(ctx.get("phone", DEMO_PHONE)))
            asyncio.create_task(send_email_booking_link(ctx.get("email", DEMO_EMAIL)))

        hangup_signals = ["talk soon", "take care", "goodbye", "disconnecting", "have a good"]
        should_hangup = ctx["booking_sent"] and any(s in reply.lower() for s in hangup_signals)

        if call_id in call_streams:
            asyncio.create_task(call_streams[call_id].put({
                "role": "assistant", "content": reply, "hangup": should_hangup
            }))

        return {"text": reply, "hangup": should_hangup}

    elif etype == "agent.call_ended":
        print(f"[call_ended] callId={call_id}")
        if call_id in call_streams:
            asyncio.create_task(call_streams[call_id].put({"hangup": True, "role": "system", "content": ""}))
        call_context.pop(call_id, None)
        return {}

    return {}


@app.post("/trigger-call")
async def trigger_call(to: str = DEMO_PHONE):
    slots = await get_available_slots()
    opening = (
        f"Hey {DEMO_LEAD['name']}! This is Alex from Rolync. "
        f"I was looking at Dataflow AI — congrats on the pre-seed raise. "
        f"You got 60 seconds?"
    )
    async with httpx.AsyncClient() as http:
        resp = await http.post(
            "https://api.agentphone.ai/v1/calls",
            json={"agentId": AGENTPHONE_AGENT_ID, "toNumber": to, "initialGreeting": opening},
            headers={"Authorization": f"Bearer {AGENTPHONE_API_KEY}"},
        )
    result = resp.json()
    call_id = result.get("id")
    if call_id:
        call_context[call_id] = {
            "history": [{"role": "assistant", "content": opening}],
            "lead": DEMO_LEAD, "slots": slots, "phone": to, "email": DEMO_EMAIL, "booking_sent": False,
        }
    return result
