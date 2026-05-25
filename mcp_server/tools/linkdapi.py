"""mcp_server/tools/linkdapi.py

LinkdAPI wrapper for LinkedIn research.
See docs/linkdapi.md for endpoint reference and credit costs.

Auth: X-linkdapi-apikey header from LINKD_API_KEY env var (env key: linkdAPI).
Rate limit: 7 req/min on Testing tier — enforced via _rate_limit().
"""

import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://linkdapi.com"
_MIN_INTERVAL = 9.0  # seconds between calls (stays under 7/min)
_last_call: float = 0.0


def _key() -> str:
    k = os.getenv("linkdAPI", "")
    if not k:
        raise RuntimeError("linkdAPI env var not set")
    return k


def _get(path: str, params: dict) -> dict:
    """Rate-limited GET. Returns parsed response dict or {} on error."""
    global _last_call
    elapsed = time.monotonic() - _last_call
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)

    try:
        resp = httpx.get(
            f"{_BASE}{path}",
            params=params,
            headers={"X-linkdapi-apikey": _key()},
            timeout=12.0,
        )
        _last_call = time.monotonic()
        data = resp.json()
        if not data.get("success"):
            logger.warning("linkdapi %s: success=false — %s", path, data)
        return data
    except Exception as exc:
        _last_call = time.monotonic()
        logger.warning("linkdapi %s: %s", path, exc)
        return {}


def search_people(keyword: str, company: str = "", title: str = "", count: int = 3) -> list[dict]:
    """Search LinkedIn for people by name and optional company/title filter.

    Args:
        keyword: Full name to search, e.g. "Jane Smith"
        company: Optional current company name filter
        title:   Optional title keyword, e.g. "founder"
        count:   Number of results (1–10)

    Returns list of {urn, username, firstName, lastName, headline, currentCompany}.
    """
    params: dict = {"keyword": keyword, "count": min(count, 10)}
    if company:
        params["currentCompany"] = company
    if title:
        params["title"] = title

    data = _get("/api/v1/search/people", params)
    items = (data.get("data") or {})
    if isinstance(items, dict):
        items = items.get("items") or []
    if not isinstance(items, list):
        return []

    results = []
    for p in items:
        results.append({
            "urn": p.get("urn", ""),
            "username": p.get("username", ""),
            "firstName": p.get("firstName", ""),
            "lastName": p.get("lastName", ""),
            "headline": p.get("headline", ""),
            "currentCompany": (p.get("currentCompany") or {}).get("name", ""),
        })
    return results


def get_profile(username: str) -> dict:
    """Get full LinkedIn profile for a username.

    Returns {urn, username, firstName, lastName, headline, summary,
             followerCount, currentPositions, isHiring}.
    1 credit.
    """
    data = _get("/api/v1/profile/full", {"username": username})
    p = data.get("data") or {}
    if isinstance(p, dict) and "urn" not in p:
        # some responses nest under data.data
        p = p.get("data") or p

    positions = []
    for pos in (p.get("currentPositions") or p.get("position") or [])[:3]:
        positions.append({
            "title": pos.get("title", ""),
            "company": pos.get("companyName", "") or (pos.get("company") or {}).get("name", ""),
        })

    return {
        "urn": p.get("urn", ""),
        "username": p.get("username", username),
        "firstName": p.get("firstName", ""),
        "lastName": p.get("lastName", ""),
        "headline": p.get("headline", ""),
        "summary": (p.get("summary") or p.get("about") or "")[:500],
        "followerCount": p.get("followerCount", 0),
        "isHiring": p.get("isHiring", False),
        "currentPositions": positions,
    }


def get_profile_posts(urn: str, limit: int = 5) -> list[dict]:
    """Get recent LinkedIn posts by a person's URN.

    Args:
        urn:   Profile URN from search_people() or get_profile()
        limit: Max posts to return (first request returns up to 100)

    Returns list of {text, reactionCount, commentsCount, publishedAt}.
    1 credit.
    """
    data = _get("/api/v1/posts/all", {"urn": urn})
    raw = (data.get("data") or {})
    if isinstance(raw, dict):
        raw = raw.get("data") or []
    if not isinstance(raw, list):
        return []

    posts = []
    for p in raw[:limit]:
        text = p.get("text") or p.get("commentary") or p.get("description") or ""
        if not text or len(text) < 20:
            continue
        posts.append({
            "text": text[:400],
            "reactionCount": p.get("totalReactionCount", 0),
            "commentsCount": p.get("commentsCount", 0),
            "publishedAt": p.get("publishedAt", 0),
        })
    return posts


def find_company_id(company_name: str) -> int | None:
    """Look up a LinkedIn company ID by name.

    Returns integer company ID, or None if not found.
    1 credit.
    """
    data = _get("/api/v1/companies/name-lookup", {"query": company_name})
    items = (data.get("data") or {})
    if isinstance(items, dict):
        items = items.get("items") or []
    if not isinstance(items, list) or not items:
        return None
    return items[0].get("id") or items[0].get("companyID")


def get_company_posts(company_id: int, limit: int = 5) -> list[dict]:
    """Get recent LinkedIn posts from a company page.

    Args:
        company_id: Integer ID from find_company_id()
        limit:      Max posts to return

    Returns list of {text, reactionCount, publishedAt}.
    1 credit.
    """
    data = _get("/api/v1/companies/company/posts", {"id": str(company_id)})
    raw = (data.get("data") or {})
    if isinstance(raw, dict):
        raw = raw.get("data") or raw.get("items") or []
    if not isinstance(raw, list):
        return []

    posts = []
    for p in raw[:limit]:
        text = p.get("commentary") or p.get("text") or p.get("description") or ""
        if not text or len(text) < 20:
            continue
        posts.append({
            "text": text[:400],
            "reactionCount": p.get("totalReactionCount", 0),
            "publishedAt": p.get("publishedAt", 0),
        })
    return posts
