# LinkdAPI Reference

Base URL: `https://linkdapi.com`
Auth header: `X-linkdapi-apikey: <key>`
Response envelope: `{ "success": true, "data": { ... } }`

## Rate Limits
| Tier | Credits | Req/min |
|------|---------|---------|
| Testing | 0–99 | 7 |
| Hobby | 100–9,999 | 30 |

## Credit Costs
Most endpoints: **1 credit**
Exceptions: `GET /api/v1/profile/overview` — **2 credits**, `GET /api/v1/companies/company/info` — **2 credits**

---

## Endpoints Used in GTM Agent

### 1. Search People
`GET /api/v1/search/people`

Find a founder by name + company.

| Param | Type | Notes |
|-------|------|-------|
| `keyword` | string | Full name, e.g. `"John Smith"` |
| `currentCompany` | string | Company name filter |
| `title` | string | e.g. `"founder"`, `"ceo"` |
| `count` | int | 1–50, default 10 |

Response `data` shape:
```json
{
  "items": [
    {
      "urn": "ACoAA...",
      "username": "johnsmith",
      "firstName": "John",
      "lastName": "Smith",
      "headline": "Co-founder at Acme",
      "profilePicture": "https://...",
      "currentCompany": { "name": "Acme", "urn": "..." }
    }
  ]
}
```

---

### 2. Get Full Profile
`GET /api/v1/profile/full?username={username}`
**1 credit**

Returns full profile including summary, headline, experience, followerCount.

Key response fields:
```json
{
  "urn": "ACoAA...",
  "username": "johnsmith",
  "firstName": "John",
  "lastName": "Smith",
  "headline": "Co-founder & CEO at Acme",
  "summary": "Building AI tools for...",
  "followerCount": 1200,
  "connectionsCount": 500,
  "isHiring": false,
  "currentPositions": [
    { "title": "Co-founder & CEO", "companyName": "Acme", "startYear": 2023 }
  ]
}
```

---

### 3. Get Founder Posts
`GET /api/v1/posts/all?urn={profile_urn}`
**1 credit**

First request returns up to 100 posts; subsequent pages return 20.

Response `data` shape:
```json
{
  "data": [
    {
      "urn": "7...",
      "text": "Post content here...",
      "totalReactionCount": 45,
      "commentsCount": 12,
      "publishedAt": 1715000000000
    }
  ],
  "cursor": "..."
}
```

---

### 4. Find Company ID
`GET /api/v1/companies/name-lookup?query={company_name}`
**1 credit**

Response `data` shape:
```json
{
  "items": [
    {
      "id": 12345678,
      "name": "Acme Inc",
      "universalName": "acme-inc",
      "followerCount": 800
    }
  ]
}
```

---

### 5. Get Company Posts
`GET /api/v1/companies/company/posts?id={company_id}`
**1 credit**

Same shape as profile posts. Use `id` from name-lookup.

Response `data` shape:
```json
{
  "data": [
    {
      "urn": "7...",
      "commentary": "Post text here...",
      "totalReactionCount": 20,
      "publishedAt": 1715000000000
    }
  ]
}
```

---

## Usage Notes
- Always use `profile/full` (1 credit) over `profile/overview` (2 credits) — full has more fields
- `posts/all` `urn` param = the profile URN from search/profile results (e.g. `ACoAA...`)
- `companies/company/posts` `id` param = integer company ID from `name-lookup`
- On 429: back off 9s (Testing tier) or 2s (Hobby tier)
