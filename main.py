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
LINKD_API_KEY       = os.getenv("linkdAPI", "").strip()

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


def parse_launch_hn(title: str) -> dict | None:
    m = re.match(r"Launch HN:\s*(.+?)\s*\(YC\s*([WS]\d{2})\)\s*[–—-]+\s*(.+)", title, re.IGNORECASE)
    if not m:
        return None
    return {"name": m.group(1).strip(), "batch": m.group(2).upper(), "description": m.group(3).strip()}


def clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&#x2F;", "/", text)
    text = re.sub(r"&amp;", "&", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_domain(url: str) -> str:
    if not url:
        return ""
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    return url.split("/")[0].split("?")[0]


# ── External API clients ──────────────────────────────────────────────────────
async def search_yc_for_icp(hn_keywords: list[str]) -> list[dict]:
    companies: list[dict] = []
    seen: set[str] = set()

    # Build queries: one broad per batch + one per keyword per batch
    # Broad catches all recent YC companies; keywords boost relevant ones to the top
    queries: list[str] = []
    for batch in ["W25", "S24", "W24"]:
        queries.append(f"Launch HN {batch}")          # broad
        for kw in hn_keywords[:3]:
            queries.append(f"Launch HN {batch} {kw}") # keyword-specific

    async with httpx.AsyncClient(timeout=15.0) as http:
        for query in queries:
            try:
                resp = await http.get(
                    "https://hn.algolia.com/api/v1/search",
                    params={"query": query, "tags": "story", "hitsPerPage": 20},
                )
                hits = resp.json().get("hits", [])
            except Exception as e:
                print(f"[yc] query='{query}': {e}")
                continue

            for hit in hits:
                parsed = parse_launch_hn(hit.get("title", ""))
                if not parsed or parsed["name"].lower() in seen:
                    continue
                seen.add(parsed["name"].lower())
                story = clean_html(hit.get("story_text", "") or "")
                companies.append({
                    "name": parsed["name"],
                    "website": hit.get("url", ""),
                    "description": parsed["description"],
                    "story": story[:400],
                    "batch": parsed["batch"],
                })

    print(f"[yc] {len(companies)} companies across {len(queries)} queries")
    return companies


async def firecrawl_scrape(url: str) -> str:
    if not url or not FIRECRAWL_API_KEY:
        return ""
    try:
        async with httpx.AsyncClient(timeout=20.0) as http:
            resp = await http.post(
                "https://api.firecrawl.dev/v1/scrape",
                headers={"Authorization": f"Bearer {FIRECRAWL_API_KEY}"},
                json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
            )
            return resp.json().get("data", {}).get("markdown", "")[:2000]
    except Exception as e:
        print(f"[firecrawl] {url}: {e}")
        return ""


async def apollo_find_founder(domain: str) -> dict | None:
    if not domain or not APOLLO_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            resp = await http.post(
                "https://api.apollo.io/api/v1/mixed_people/search",
                json={
                    "api_key": APOLLO_API_KEY,
                    "q_organization_domains": [domain],
                    "person_titles": ["founder", "co-founder", "ceo", "chief executive officer"],
                    "page": 1,
                    "per_page": 3,
                },
            )
            people = resp.json().get("people", [])
        if not people:
            return None
        for p in people:
            if "founder" in (p.get("title") or "").lower():
                return p
        return people[0]
    except Exception as e:
        print(f"[apollo] {domain}: {e}")
        return None


# ── LinkdAPI ──────────────────────────────────────────────────────────────────
_linkdapi_last: float = 0.0

async def _linkdapi_get(http: httpx.AsyncClient, path: str, params: dict) -> dict:
    global _linkdapi_last
    elapsed = asyncio.get_event_loop().time() - _linkdapi_last
    if elapsed < 9.0:                      # stay under 7 req/min
        await asyncio.sleep(9.0 - elapsed)
    resp = await http.get(
        f"https://linkdapi.com{path}",
        params=params,
        headers={"X-linkdapi-apikey": LINKD_API_KEY},
    )
    _linkdapi_last = asyncio.get_event_loop().time()
    return resp.json()


async def linkdapi_company_intel(company_name: str, founder_linkedin_url: str = "") -> str:
    if not LINKD_API_KEY:
        return ""
    signals: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=12.0) as http:
            # Step 1: find company ID
            data = await _linkdapi_get(http, "/api/v1/search/companies", {"keyword": company_name})
            items = (data.get("data") or {})
            if isinstance(items, dict):
                items = items.get("items") or []
            if not items:
                return ""
            company_id = items[0].get("companyID") or items[0].get("id") or ""
            if not company_id:
                return ""

            # Step 2: company LinkedIn posts
            data = await _linkdapi_get(http, "/api/v1/companies/company/posts", {"id": str(company_id)})
            posts_raw = data.get("data") or []
            if isinstance(posts_raw, dict):
                posts_raw = posts_raw.get("data") or posts_raw.get("items") or []
            post_texts = []
            for p in posts_raw[:5]:
                t = p.get("text") or p.get("commentary") or p.get("description") or ""
                if t and len(t) > 20:
                    post_texts.append(t[:250])
            if post_texts:
                signals.append("LinkedIn posts: " + " | ".join(post_texts))

            # Step 3: founder profile if LinkedIn URL available
            if founder_linkedin_url and "/in/" in founder_linkedin_url:
                username = founder_linkedin_url.rstrip("/").split("/in/")[-1].split("?")[0].rstrip("/")
                if username:
                    data = await _linkdapi_get(http, "/api/v1/profile/overview", {"username": username})
                    p = data.get("data") or {}
                    headline = p.get("headline") or p.get("title") or ""
                    about = p.get("about") or p.get("summary") or ""
                    if headline or about:
                        signals.append(f"Founder profile: {headline} | {about[:300]}")

    except Exception as e:
        print(f"[linkdapi] {company_name}: {e}")

    return "\n".join(signals)


async def score_and_brief(company: dict, website: str, contact: dict, linkedin: str, icp: dict) -> dict:
    prompt = f"""ICP:
{json.dumps(icp, indent=2)}

Company: {company['name']} (YC {company.get('batch', '')})
Description: {company.get('description', '')}
Website content: {website[:500]}
LinkedIn signals: {linkedin[:600] if linkedin else 'none'}
Contact: {contact.get('first_name', '')} {contact.get('last_name', '')}, {contact.get('title', '')}

Return JSON only:
{{
  "score": <int 0-100>,
  "reasoning": "<2 sentences: why this is or isn't an ICP fit, cite LinkedIn signals if useful>",
  "call_brief": "<3 sentences of cold call intel: what they do, stage, most likely pain point>"
}}"""
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return extract_json(resp.content[0].text)
    except Exception as e:
        print(f"[score] {company['name']}: {e}")
        return {"score": 0, "reasoning": "scoring failed", "call_brief": company.get("description", "")}


# ── Pipeline ──────────────────────────────────────────────────────────────────
async def run_pipeline(session_id: str, icp: dict):
    queue = pipeline_queues[session_id]

    async def emit(etype: str, **kwargs):
        await queue.put({"type": etype, **kwargs})

    kw = icp.get("hn_keywords", ["saas", "sales"])
    await emit("status", message=f"Searching YC W25/S24/W24 for: {', '.join(kw)}")

    companies = await search_yc_for_icp(kw)
    await emit("status", message=f"Found {len(companies)} YC companies. Finding the best match...")

    good: list[dict] = []
    checked = 0

    for company in companies:
        if len(good) >= 1 or checked >= 5:
            break
        checked += 1
        name = company["name"]
        url = company.get("website", "")
        domain = extract_domain(url)

        await emit("status", message=f"Researching {name}...")
        website = await firecrawl_scrape(url)

        await emit("status", message=f"Finding founder at {name} via Apollo...")
        contact = await apollo_find_founder(domain)
        if not contact:
            await emit("status", message=f"  ↳ No founder found, skipping")
            continue

        cname = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
        await emit("status", message=f"  ↳ Found {cname} ({contact.get('title', '')})")

        await emit("status", message=f"Checking LinkedIn signals for {name}...")
        linkedin_intel = await linkdapi_company_intel(name, contact.get("linkedin_url", ""))
        if linkedin_intel:
            preview = linkedin_intel[:120].replace("\n", " ")
            await emit("status", message=f"  ↳ {preview}...")
        else:
            await emit("status", message=f"  ↳ No LinkedIn data found")

        await emit("status", message=f"Scoring ICP fit for {name}...")
        scored = await score_and_brief(company, website, contact, linkedin_intel, icp)
        score = scored.get("score", 0)
        await emit("status", message=f"  ↳ {score}/100 — {scored.get('reasoning', '')}")

        if score < 55:
            await emit("status", message=f"  ↳ Below threshold, skipping")
            continue

        lead_id = str(uuid.uuid4())
        lead = {
            "id": lead_id,
            "company": name,
            "website": url,
            "batch": company.get("batch", ""),
            "contact_name": cname,
            "contact_role": contact.get("title", "Founder"),
            "contact_email": contact.get("email", ""),
            "icp_score": score,
            "research_summary": scored.get("reasoning", ""),
            "call_brief": scored.get("call_brief", ""),
            "status": "sourced",
        }
        leads_store[lead_id] = lead
        good.append(lead)
        await emit("lead", lead=lead)

    await emit("done", count=len(good))


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
