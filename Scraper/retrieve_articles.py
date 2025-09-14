import os
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, HttpUrl, ValidationError
import hashlib

load_dotenv()

class Article(BaseModel):
    """Pydantic model representing a single search result article."""
    id: str
    query_id: str
    title: str
    description: str
    url: HttpUrl
    
    @staticmethod
    def make_id(url: str, query_id: str) -> str:
        return hashlib.md5(f"{url}-{query_id}".encode()).hexdigest()


def extract_json(data: Dict[str, Any], query_id: str) -> List[Article]:
    """Extract a list of Article objects from Google Custom Search JSON.

    Accepts either a raw JSON string or a pre-parsed dict in the format
    returned by the Google Custom Search API. Only items with both a
    title and link are converted; invalid URLs are skipped.
    """
    results: List[Article] = []
    
    items = data.get("items")
    
    if items == None:
        return results
    
    for item in items:
        if not isinstance(item, dict):
            continue

        title = item.get("title") or ""
        link = item.get("link") or item.get("url") or ""
        description = (
            item.get("snippet")
                or item.get("htmlSnippet")
                or item.get("description")
                 or ""
        )
        if not title or not link:
            continue
        id = Article.make_id(link, query_id)
        try:
            results.append(Article(id = id, query_id = query_id, title=title, description=description, url=link))
        except ValidationError:
                # Skip items with invalid URL format
            continue

    return results

def get_articles(query: str, query_id: str) -> List[Article]:
    """Query Google Custom Search and return a list of Articles.

    Note: requires `GOOGLE_SEARCH_ENGINE_API_KEY` in environment.
    """
    api_key = os.getenv("GOOGLE_SEARCH_ENGINE_API_KEY")
    if not api_key:
        return []

    request_url = (
        f"https://www.googleapis.com/customsearch/v1?key={api_key}&cx=d5b275046e0124b12&q={query}"
    ) 
    response = requests.get(request_url)
    return extract_json(response.json(), query_id)
    
    
    
