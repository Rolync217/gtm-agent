"""pipeline_v2/mcp_server/tools/firecrawl_v2.py

Firecrawl Python SDK tool wrappers for the pipeline v2 MCP server.
All functions load FIRECRAWL_API_KEY from the environment.
These are the primary web tools for the agent — replaces Tavily and
the old httpx scraper.

Tools:
  search_web             — search the web, return snippets
  scrape_web_page        — scrape a single URL to markdown
  search_web_and_scrape  — search + return full page markdown per result
  map_site               — discover all URLs on a site
  crawl_site             — crawl an entire site (blocking)
  interact_with_page     — single-shot browser actions (click, fill, scroll) then scrape
  continue_interaction   — continue interacting with a scrape-bound browser context (act → observe → act)
  stop_interaction       — stop the scrape-bound interactive session when done
  open_browser_session   — open a persistent Playwright browser session (optional named profile)
  run_in_browser         — execute Python/JS Playwright code in the session
  close_browser_session  — close the session when done
  crawl_site_streaming   — async streaming crawl via WebSockets

Error handling contract:
  Dict-returning tools:  return {"error": str(exc), ...} on failure — never raise.
  List-returning tools:  return [] on failure — never raise.
"""

import asyncio
import logging
import os
from typing import Optional

from firecrawl import AsyncFirecrawl, Firecrawl

logger = logging.getLogger(__name__)


def _resolve_metadata(meta_obj) -> dict:
    """Extract metadata dict from various formats (None, dict, or Pydantic model)."""
    if meta_obj is None:
        return {}
    if isinstance(meta_obj, dict):
        return meta_obj
    if hasattr(meta_obj, "model_dump"):
        return meta_obj.model_dump(exclude_none=True)
    return {}


def _client() -> Firecrawl:
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise RuntimeError("FIRECRAWL_API_KEY not set in environment")
    return Firecrawl(api_key=api_key)


def _async_client() -> AsyncFirecrawl:
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise RuntimeError("FIRECRAWL_API_KEY not set in environment")
    return AsyncFirecrawl(api_key=api_key)


def _doc_to_dict(p) -> dict:
    """Convert a Document or dict-like page object to a plain dict with standard keys."""
    if isinstance(p, dict):
        meta = p.get("metadata", {}) or {}
        if not isinstance(meta, dict):
            # DocumentMetadata Pydantic model inside a dict — shouldn't happen, but guard it
            meta = meta.model_dump(exclude_none=True) if hasattr(meta, "model_dump") else {}
        return {
            "url": meta.get("source_url") or p.get("url", "") or "",
            "title": meta.get("title", ""),
            "markdown": p.get("markdown", "") or "",
            "html": p.get("html", "") or "",
            "metadata": meta,
        }
    # Pydantic Document
    meta_dict = _resolve_metadata(p.metadata)
    url = meta_dict.get("source_url") or getattr(p, "url", "") or ""
    return {
        "url": url,
        "title": meta_dict.get("title", ""),
        "markdown": p.markdown or "",
        "html": p.html or "",
        "metadata": meta_dict,
    }


def _search_result_to_dict(item) -> dict:
    """Convert a SearchResultWeb/SearchResultNews/LinkResult or dict to a plain dict."""
    if isinstance(item, dict):
        return item
    return {
        "url": getattr(item, "url", "") or "",
        "title": getattr(item, "title", "") or "",
        "description": getattr(item, "description", "") or "",
    }


def search_web(query: str, limit: int = 5) -> list[dict]:
    """Search the web via Firecrawl.

    Returns list of {title, url, description} dicts. Returns [] on error.
    """
    try:
        result = _client().search(query, limit=limit)
        # SDK returns SearchData (Pydantic) with .web / .news / .images attributes.
        # Fall back to dict access for forward-compatibility.
        if hasattr(result, "web"):
            hits = result.web or []
        elif hasattr(result, "get"):
            hits = result.get("data", result.get("web", []))
        else:
            hits = []
        return [_search_result_to_dict(h) for h in hits]
    except Exception as exc:
        logger.warning("search_web failed: %s", exc)
        return []


def _extract_doc_fields(result, url: str) -> dict:
    """Extract markdown/html/metadata from a Document (Pydantic) or dict."""
    if isinstance(result, dict):
        raw_meta = _resolve_metadata(result.get("metadata", {}))
        return {
            "url": url,
            "markdown": result.get("markdown", "") or "",
            "html": result.get("html", "") or "",
            "metadata": raw_meta,
            "scrape_id": raw_meta.get("scrape_id"),
            "error": None,
        }
    # Pydantic Document
    meta_dict = _resolve_metadata(result.metadata)
    return {
        "url": url,
        "markdown": result.markdown or "",
        "html": result.html or "",
        "metadata": meta_dict,
        "scrape_id": meta_dict.get("scrape_id"),
        "error": None,
    }


def scrape_web_page(url: str, formats: Optional[list[str]] = None) -> dict:
    """Scrape a single URL and return structured content.

    Returns dict with keys: url, markdown, html, metadata, error.
    """
    formats = formats or ["markdown"]
    try:
        result = _client().scrape(url, formats=formats)
        return _extract_doc_fields(result, url)
    except Exception as exc:
        return {"url": url, "markdown": "", "html": "", "metadata": {}, "error": str(exc)}


def search_web_and_scrape(query: str, limit: int = 3) -> list[dict]:
    """Search the web and return full markdown content for each result.

    Returns list of {url, title, markdown} dicts. Returns [] on error.
    """
    try:
        fc = _client()
        search_result = fc.search(query, limit=limit)
        # SDK returns SearchData (Pydantic) with .web attribute; fall back to dict access.
        if hasattr(search_result, "web"):
            hits = search_result.web or []
        elif hasattr(search_result, "get"):
            hits = search_result.get("data", search_result.get("web", []))
        else:
            hits = []
        # Each hit is a SearchResultWeb Pydantic model or dict; extract .url either way.
        urls = [
            (h.url if hasattr(h, "url") else h.get("url"))
            for h in hits
            if (h.url if hasattr(h, "url") else h.get("url"))
        ]
        if not urls:
            return []
        batch = fc.batch_scrape(urls, formats=["markdown"])
        # batch is BatchScrapeJob (Pydantic) with .data = List[Document]
        pages = batch.data if hasattr(batch, "data") else []
        return [
            {
                "url": _doc_to_dict(p)["url"],
                "title": _doc_to_dict(p)["title"],
                "markdown": _doc_to_dict(p)["markdown"],
            }
            for p in pages
        ]
    except Exception as exc:
        logger.warning("search_web_and_scrape failed: %s", exc)
        return []


def map_site(url: str, limit: int = 20, search: Optional[str] = None) -> list[str]:
    """Discover and list all URLs on a website.

    Args:
        url: Root URL to map.
        limit: Max number of URLs to return.
        search: Optional keyword to filter discovered URLs.

    Returns list of URL strings. Returns [] on error.
    """
    try:
        kwargs: dict = {"limit": limit}
        if search:
            kwargs["search"] = search
        result = _client().map(url=url, **kwargs)
        # SDK returns MapData (Pydantic) with .links = List[LinkResult] where each
        # LinkResult has a .url attribute (not a plain string).
        if hasattr(result, "links"):
            links = result.links or []
            return [
                (item.url if hasattr(item, "url") else item)
                for item in links
            ]
        # Fallback for dict-style response
        return result.get("links", [])
    except Exception as exc:
        logger.warning("map_site failed: %s", exc)
        return []


def crawl_site(url: str, limit: int = 10) -> list[dict]:
    """Crawl a website (blocking) and return all pages as markdown.

    Returns list of {url, markdown} dicts. Returns [] on error.
    """
    try:
        result = _client().crawl(url, limit=limit, scrape_options={"formats": ["markdown"]})
        # SDK returns CrawlJob (Pydantic) with .data = List[Document]
        pages = result.data if hasattr(result, "data") else result.get("data", [])
        return [
            {
                "url": _doc_to_dict(p)["url"],
                "markdown": _doc_to_dict(p)["markdown"],
            }
            for p in pages
        ]
    except Exception as exc:
        logger.warning("crawl_site failed: %s", exc)
        return []


def interact_with_page(
    url: str,
    actions: list[dict],
    formats: Optional[list[str]] = None,
) -> dict:
    """Interact with a webpage via browser automation, then scrape the result.

    Actions are executed in order before the final scrape. Supports:
      {"type": "click", "selector": "button.load-more"}
      {"type": "fill", "selector": "input[name='email']", "value": "user@example.com"}
      {"type": "scroll", "direction": "down", "amount": 500}
      {"type": "wait", "milliseconds": 2000}

    Returns dict with keys: url, markdown, html, metadata, error.
    """
    formats = formats or ["markdown"]
    try:
        result = _client().scrape(url, formats=formats, actions=actions)
        return _extract_doc_fields(result, url)
    except Exception as exc:
        return {"url": url, "markdown": "", "html": "", "metadata": {}, "error": str(exc)}


def open_browser_session(profile_name: Optional[str] = None) -> dict:
    """Open a persistent Playwright browser session.

    Args:
        profile_name: Optional named profile. When provided, the session starts
                      with previously saved state (cookies, localStorage) and
                      writes changes back on close.

    Returns {session_id, cdp_url, live_view_url, error}.
    """
    try:
        kwargs: dict = {}
        if profile_name:
            kwargs["profile"] = {"name": profile_name, "save_changes": True}
        session = _client().browser(**kwargs)
        return {
            "session_id": session.id,
            "cdp_url": getattr(session, "cdp_url", None),
            "live_view_url": getattr(session, "live_view_url", None),
            "error": None,
        }
    except Exception as exc:
        return {"session_id": None, "cdp_url": None, "live_view_url": None, "error": str(exc)}


def run_in_browser(session_id: str, code: str, language: str = "python") -> dict:
    """Execute Playwright code in an open browser session.

    Args:
        session_id: Value from open_browser_session()["session_id"].
        code: Python (default) or Node.js Playwright code.
        language: "python" (default) or "node"

    Returns {stdout, result, error}.
    """
    try:
        result = _client().browser_execute(session_id, code=code, language=language)
        return {
            "stdout": getattr(result, "stdout", "") or getattr(result, "result", ""),
            "result": getattr(result, "result", ""),
            "error": None,
        }
    except Exception as exc:
        return {"stdout": "", "result": "", "error": str(exc)}


def close_browser_session(session_id: str) -> dict:
    """Close an open browser session.

    Always call this when done — sessions consume credits while open.
    Returns {closed, error}.
    """
    try:
        _client().delete_browser(session_id)
        return {"closed": True, "error": None}
    except Exception as exc:
        return {"closed": False, "error": str(exc)}


def continue_interaction(
    scrape_job_id: str,
    code: str,
    language: str = "python",
) -> dict:
    """Continue interacting with the browser context from a prior scrape job.

    Use for act → observe → act flows. scrape_job_id is the "scrape_id" field
    in the return dict from scrape_web_page() or interact_with_page().

    Returns {stdout, result, error}. Call stop_interaction(scrape_job_id) when done.
    """
    try:
        result = _client().interact(scrape_job_id, code=code, language=language, timeout=60)
        return {
            "stdout": getattr(result, "stdout", "") or "",
            "result": getattr(result, "result", ""),
            "error": None,
        }
    except Exception as exc:
        return {"stdout": "", "result": "", "error": str(exc)}


def stop_interaction(scrape_job_id: str) -> dict:
    """Stop a scrape-bound interactive session.

    Returns {stopped, error}.
    """
    try:
        _client().stop_interaction(scrape_job_id)
        return {"stopped": True, "error": None}
    except Exception as exc:
        return {"stopped": False, "error": str(exc)}


async def _crawl_streaming(url: str, limit: int) -> list[dict]:
    fc = _async_client()
    started = await fc.start_crawl(url, limit=limit)
    snapshots: list[dict] = []
    async for snapshot in fc.watcher(started.id, kind="crawl", poll_interval=2, timeout=120):
        snapshots.append({
            "status": snapshot.status,
            "completed": snapshot.completed,
            "total": snapshot.total,
            "pages": [
                {
                    "url": _doc_to_dict(p)["url"],
                    "markdown": _doc_to_dict(p)["markdown"],
                }
                for p in (snapshot.data or [])
            ],
        })
        if snapshot.status in ("completed", "failed"):
            break
    return snapshots


def crawl_site_streaming(url: str, limit: int = 5) -> list[dict]:
    """Crawl a website with real-time WebSocket updates (blocking wrapper).

    Returns list of snapshot dicts, each with:
      status, completed, total, pages (list of {url, markdown}).
    Returns [] on error.
    """
    try:
        return asyncio.run(_crawl_streaming(url, limit))
    except Exception as exc:
        logger.warning("crawl_site_streaming failed: %s", exc)
        return []
