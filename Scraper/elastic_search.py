from elasticsearch import Elasticsearch, helpers
from retrieve_articles import Article
from dotenv import load_dotenv
import os

load_dotenv()

URL = "http://localhost:9200"

INDEX = os.getenv("ES_INDEX", "articles")

MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": { 
            "id": {"type": "keyword"},
            "query_id": {"type": "keyword"},
            "title": {"type": "text", "analyzer": "english"},
            "description": {"type": "text", "analyzer": "english"},
            "url": {"type": "keyword"},
            "analysis": {
                "properties": {
                    "claim_coverage": {"type": "float"},
                    "subjectivity": {"type": "float"}, 
                    "claims": {"type": "text", "analyzer": "english"},
                    "cluster_id": {"type": "integer"}
                }
            }
        }
    }
}

def create_es_instance() -> Elasticsearch:
    return Elasticsearch(URL)

def check_es_indices(es: Elasticsearch):
    exists = es.indices.exists(index=INDEX)
    exists_body = getattr(exists,'body', exists)
    if not exists_body:
        es.indices.create(index=INDEX, **MAPPING)

def bulk_upsert_articles(es: Elasticsearch, articles: list[Article]):
    def actions():
        for a in articles:
            # Pydantic v2: use model_dump with mode="json" to get ISO datetimes
            doc = a.model_dump(mode="json")
            yield {
                "_op_type": "update",          # update for upsert semantics
                "_index": INDEX,
                "_id": a.id,                   # stable dedup key (e.g., sha256(url))
                "doc": doc,                    # fields to write/update
                "doc_as_upsert": True          # create if missing
            }

    # Tune chunk_size to your volume; request_timeout avoids timeouts on bigger batches
    try:
        helpers.bulk(es, actions(), chunk_size=500, request_timeout=60)
    except Exception as e:
        raise
    
