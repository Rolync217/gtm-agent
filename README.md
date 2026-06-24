# GTM Agent

An AI-powered outbound pipeline that finds YC startup leads, researches them deeply, cold calls them, and books a calendar meeting — fully automated, no SDR required.

Built with FastAPI + Claude + Firecrawl + AgentPhone. Deployed on Railway.

---

## What It Does

Three stages, end to end:

1. **ICP Definition** — Chat with an AI consultant to define your Ideal Customer Profile. After 4–5 exchanges it locks a structured ICP (industry, stage, pain points, HN keywords). Or skip the chat and load one directly.

2. **Research Pipeline** — A Claude Sonnet agent runs a tool loop: searches YC's database, scrapes company websites, pulls LinkedIn profiles and posts, finds the founder's email via Apollo. It surfaces the single best-fit company and writes a call brief — 3–4 sentences of specific intel to open a cold call with.

3. **AI Voice Call** — One click dials the founder. AgentPhone runs the call; every utterance from the human hits your webhook, Claude Haiku generates the reply in real time, and AgentPhone speaks it. The agent (Alex from Rolync) qualifies the lead, offers calendar slots, and closes for a 30-min discovery call. When the lead agrees, a real Cal.com booking is created and a calendar invite is emailed automatically.

---

## Architecture

```
User → ICP Chat → locked ICP
                     ↓
              Research Agent (Claude Sonnet)
              → YC search → website scrape → LinkedIn → Apollo
                     ↓
                Lead card (stored in memory, streamed to UI via SSE)
                     ↓
              Call initiated (AgentPhone)
                     ↓
              Human speaks → webhook → Claude Haiku → AgentPhone speaks
                     ↓
              Lead agrees → Cal.com booking + ZeptoMail calendar invite
```

The backend is a single FastAPI process (`main.py`). All state lives in memory — a restart clears it. The frontend is a single-page app served at `/app`.

---

## Stack

| Layer | Technology |
|---|---|
| Backend | FastAPI + Python |
| AI reasoning | Anthropic Claude Sonnet 4.6 (research loop) |
| AI voice | Anthropic Claude Haiku 4.5 (real-time call responses) |
| Phone calls | [AgentPhone](https://agentphone.ai) |
| Web research | [Firecrawl](https://firecrawl.dev) |
| LinkedIn data | [LinkdAPI](https://linkdapi.com) |
| Email lookup | [Apollo](https://apollo.io) |
| Lead sourcing | YC HN Algolia index |
| Calendar | [Cal.com](https://cal.com) |
| Email delivery | [ZeptoMail](https://zeptomail.com) |
| Deployment | [Railway](https://railway.app) |

---

## Local Setup

### 1. Clone and install

```bash
git clone https://github.com/Rolync217/gtm-agent.git
cd gtm-agent
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your keys:

```env
# Anthropic
ANTHROPIC_API_KEY=your_key_here

# AgentPhone — makes the actual phone calls
AGENTPHONE_API_KEY=your_key_here
AGENTPHONE_AGENT_ID=your_agent_id_here

# Firecrawl — web scraping and search
FIRECRAWL_API_KEY=your_key_here

# Apollo — email lookup
APOLLO_API_KEY=your_key_here

# LinkdAPI — LinkedIn profiles and posts
LINKDAPI_API_KEY=your_key_here

# Cal.com — calendar availability and booking
CAL_COM_API_KEY=your_key_here
CAL_EVENT_URL=https://cal.com/your-username/30-mins-discovery-call

# ZeptoMail — sends the calendar invite email
SEND_MAIL_TOKEN_1=your_key_here

# Demo — phone number and email for the voice call
DEMO_PHONE=+10000000000
DEMO_EMAIL=your@email.com
```

### 3. Run

```bash
uvicorn main:app --reload
```

Open `http://localhost:8000/app` to use the UI.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/icp/message` | Send a message to the ICP discovery chat |
| `POST` | `/icp/preset` | Skip chat — load an ICP directly |
| `POST` | `/leads/source` | Start the research pipeline for a locked ICP |
| `GET` | `/leads/stream/{session_id}` | SSE stream of pipeline events |
| `GET` | `/leads` | List all sourced leads |
| `POST` | `/leads/{lead_id}/call` | Initiate a phone call for a lead |
| `GET` | `/call/{call_id}/stream` | SSE stream of live call transcript |
| `POST` | `/webhook/call` | AgentPhone webhook (receives call events) |
| `POST` | `/leads/seed-demo` | Plant a demo lead instantly (skips research) |
| `GET` | `/app` | Serves the frontend |

---

## Research Tools Available to the Agent

| Tool | What it does |
|---|---|
| `search_yc_companies` | Searches YC's HN Algolia index by ICP keywords and batch |
| `scrape_website` | Scrapes a company website — returns cleaned markdown |
| `search_web` | General web search via Firecrawl (news, Twitter posts, funding) |
| `search_linkedin_people` | Finds a founder by name on LinkedIn |
| `get_linkedin_profile` | Full LinkedIn profile: headline, bio, experience, follower count |
| `get_founder_linkedin_posts` | Recent posts from the founder — reveals active pain points |
| `get_company_linkedin_posts` | Recent posts from the company page |
| `find_founder_contact` | Email lookup via Apollo |
| `submit_lead` | Finalises the lead with ICP score, research summary, and call brief |

---

## Deployment (Railway)

The `Procfile` and `runtime.txt` are already configured for Railway.

```
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

Push to `main` to trigger an auto-deploy. Set all `.env` keys as Railway environment variables in the project settings.

For AgentPhone to send call events, point the webhook URL in your AgentPhone dashboard to:

```
https://your-railway-domain.up.railway.app/webhook/call
```

---

## Project Structure

```
gtm-agent/
├── main.py                    # FastAPI app — all routes, agent loop, call handler
├── mcp_server/
│   └── tools/
│       ├── apollo.py          # Apollo email lookup
│       ├── firecrawl_v2.py    # Firecrawl search + scrape
│       ├── linkdapi.py        # LinkedIn people/company data
│       └── yc_algolia.py      # YC company search
├── static/
│   └── index.html             # Single-page frontend
├── .env.example               # Environment variable template
├── requirements.txt
├── Procfile                   # Railway process definition
└── runtime.txt                # Python version pin
```

---

## License

MIT
