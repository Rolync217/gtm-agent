"""mcp_server/tools/apollo.py

Apollo.io wrapper for finding founder contact info by name.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://api.apollo.io/api/v1"


def _key() -> str:
    k = os.getenv("APOLLO_API_KEY", "")
    if not k:
        raise RuntimeError("APOLLO_API_KEY env var not set")
    return k


def find_person_by_name(
    first_name: str,
    last_name: str,
    organization_name: str = "",
    domain: str = "",
) -> dict | None:
    """Match a person in Apollo by name + company.

    Uses /people/match — returns the single best match with email,
    phone, LinkedIn URL, and title if Apollo has them.

    Args:
        first_name:        Founder's first name
        last_name:         Founder's last name
        organization_name: Company name (improves match accuracy)
        domain:            Company website domain (improves match accuracy)

    Returns dict with {first_name, last_name, title, email, phone,
    linkedin_url, organization_name} or None if no match.
    """
    payload: dict = {
        "api_key": _key(),
        "first_name": first_name,
        "last_name": last_name,
        "reveal_personal_emails": True,
    }
    if organization_name:
        payload["organization_name"] = organization_name
    if domain:
        payload["domain"] = domain

    try:
        with httpx.Client(timeout=10.0) as http:
            resp = http.post(f"{_BASE}/people/match", json=payload)
        person = resp.json().get("person")
        if not person:
            return None

        org = person.get("organization") or {}
        return {
            "first_name": person.get("first_name", ""),
            "last_name": person.get("last_name", ""),
            "title": person.get("title", ""),
            "email": person.get("email", ""),
            "phone": (person.get("phone_numbers") or [{}])[0].get("raw_number", ""),
            "linkedin_url": person.get("linkedin_url", ""),
            "organization_name": org.get("name", organization_name),
        }
    except Exception as exc:
        logger.warning("apollo find_person_by_name %s %s: %s", first_name, last_name, exc)
        return None


def search_people_by_company(domain: str, count: int = 3) -> list[dict]:
    """Search Apollo for founders at a company by domain.

    Prefers titles containing 'founder'; falls back to first result.

    Returns list of {first_name, last_name, title, email, linkedin_url}.
    """
    try:
        with httpx.Client(timeout=10.0) as http:
            resp = http.post(
                f"{_BASE}/mixed_people/search",
                json={
                    "api_key": _key(),
                    "q_organization_domains": [domain],
                    "person_titles": ["founder", "co-founder", "ceo", "chief executive officer"],
                    "page": 1,
                    "per_page": count,
                },
            )
        people = resp.json().get("people", [])
        results = []
        for p in people:
            results.append({
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "title": p.get("title", ""),
                "email": p.get("email", ""),
                "linkedin_url": p.get("linkedin_url", ""),
            })
        return results
    except Exception as exc:
        logger.warning("apollo search_people_by_company %s: %s", domain, exc)
        return []
