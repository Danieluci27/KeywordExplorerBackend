from retrieve_articles import get_articles
from elastic_search import create_es_instance, check_es_indices, bulk_upsert_articles
from flask import Flask, request, jsonify
from typing import Any, Dict
from dotenv import load_dotenv
import os


app = Flask(__name__)

ES = create_es_instance()
check_es_indices(ES)

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/service/scraper")
def scraper():
    """
    Body (JSON):
      {
        "query": "la dodgers",
        "query_id": "12345"
      }
    """
    data: Dict[str, Any] = request.get_json(silent = True) or {}
    
    #If query doesn't exist as a key in JSON object, it will return null instead of raising an error. 
    query = data.get("query")
    query_id = data.get("query_id")
    
    if not query or not isinstance(query, str):
        return jsonify({"error": "Missing or invalid 'query' (string required)."}), 400
    elif not query_id or not isinstance(query_id, str):
        return jsonify({"error": "Missing or invalid 'query' (string required)."}), 400
    
    try:
        articles = get_articles(query, query_id)
        es = create_es_instance()
        check_es_indices(es)
        bulk_upsert_articles(es, articles)
    except Exception as e:
        return jsonify({"error": "Failed to save articles into elastic search"}), 500
    
    return jsonify({"status": "ok"}), 200

        

if __name__ == '__main__':
    load_dotenv()
    
    host = os.getenv("SCRAPER_HOST")
    
    port = os.getenv("SCRAPER_PORT")
    if not port:
        raise Exception("missing port")
    try:
        port = int(port)
    except:
        raise Exception("not valid port")
    app.run(host=host, port=port, debug=True)

    
    
        
    