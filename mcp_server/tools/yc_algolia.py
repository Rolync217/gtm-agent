"""mcp_server/tools/yc_algolia.py

Searches HN Algolia for YC Launch HN posts matching ICP keywords.
Returns structured company list.
"""

import logging
import re

import httpx

logger = logging.getLogger(__name__)

_HN_SEARCH_URL = "https://hn.algolia.com/api/v1/search"


def _parse_launch_hn(title: str) -> dict | None:
    m = re.match(r"Launch HN:\s*(.+?)\s*\(YC\s*([WS]\d{2})\)\s*[–—-]+\s*(.+)", title, re.IGNORECASE)
    if not m:
        return None
    return {"name": m.group(1).strip(), "batch": m.group(2).upper(), "description": m.group(3).strip()}


def _clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&#x2F;", "/", text)
    text = re.sub(r"&amp;", "&", text)
    return re.sub(r"\s+", " ", text).strip()


def search_yc_companies(keywords: list[str], batches: list[str] | None = None) -> list[dict]:
    """Search HN Algolia for YC companies matching ICP keywords.

    Runs broad batch queries + per-keyword queries. Returns deduplicated list.

    Args:
        keywords: ICP product-category keywords, e.g. ["saas", "b2b", "workflow"]
        batches:  YC batches to search, defaults to ["W25", "S24", "W24"]

    Returns list of {name, website, batch, description, story} dicts.
    """
    batches = batches or ["W25", "S24", "W24"]
    queries: list[str] = []
    for batch in batches:
        queries.append(f"Launch HN {batch}")
        for kw in keywords[:3]:
            queries.append(f"Launch HN {batch} {kw}")

    companies: list[dict] = []
    seen: set[str] = set()

    with httpx.Client(timeout=15.0) as http:
        for query in queries:
            try:
                resp = http.get(_HN_SEARCH_URL, params={"query": query, "tags": "story", "hitsPerPage": 20})
                hits = resp.json().get("hits", [])
            except Exception as exc:
                logger.warning("yc search query='%s': %s", query, exc)
                continue

            for hit in hits:
                parsed = _parse_launch_hn(hit.get("title", ""))
                if not parsed or parsed["name"].lower() in seen:
                    continue
                seen.add(parsed["name"].lower())
                story = _clean_html(hit.get("story_text", "") or "")
                companies.append({
                    "name": parsed["name"],
                    "website": hit.get("url", ""),
                    "batch": parsed["batch"],
                    "description": parsed["description"],
                    "story": story[:400],
                })

    logger.info("yc_algolia: %d companies from %d queries", len(companies), len(queries))
    return companies
