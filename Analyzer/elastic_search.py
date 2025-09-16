from elasticsearch import Elasticsearch, helpers
from typing import Any, Iterable
from dotenv import load_dotenv
import os

load_dotenv()

INDEX = os.getenv("ES_INDEX", "articles")

def create_es():
    url = os.getenv("ES_SERVER_URL", "http://localhost:9200")
    return Elasticsearch(url)

def stream_all_articles(es: Elasticsearch, query_id: str, fields: list[str] = 
    ["id", "query_id", "title", "description", "url", "analysis"]) -> Iterable[dict[str, Any]]:
    """
    Stream ALL matching docs (any count) using scan (efficient deep pagination).
    """
    query = {"query": {"term": {"query_id": query_id}}}
    for h in helpers.scan(es, index=INDEX, query=query, _source=fields):
        yield h["_source"]