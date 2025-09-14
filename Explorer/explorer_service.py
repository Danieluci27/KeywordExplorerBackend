from flask import Flask, request, jsonify
from typing import Dict, Any
from dotenv import load_dotenv
import requests
import uuid
import os

app = Flask(__name__)

SCRAPER_URL = os.getenv("SCRAPER_URL", "http://127.0.0.1:8000/service/scraper")


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/service/explorer")
def explorer():
    '''
    Input: JSON object
    {
        "query" (str)
    }
    '''
    data: Dict[str, Any] = request.get_json(silent = True) or {}
    
    query = data.get('query')
    if isinstance(query, str):
        query = query.strip()
    if not query or not isinstance(query, str):
        return jsonify({"error": "Missing or non-string query."}), 400

    payload = {
        "query": query,
        "query_id": str(uuid.uuid4())
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        # requests sets Content-Type automatically when using json=, headers are optional
        resp = requests.post(SCRAPER_URL, json=payload, headers=headers, timeout=15)
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "scraper_unreachable", "message": str(e)}), 502

    content = resp.json()

    if not resp.ok:
        # Preserve scraper status code if possible
        return jsonify({"error": "scraper_error", "status": resp.status_code, "details": content}), resp.status_code

    # Success — return query_id so the client can poll downstream services
    return jsonify({
        "status": "ok",
        "query_id": payload["query_id"],
    }), 200
    
if __name__ == '__main__':
    load_dotenv()
    
    host = os.getenv("EXPLORER_HOST")
    
    port = os.getenv("EXPLORER_PORT")
    if not port:
        raise Exception("missing port")
    try:
        port = int(port)
    except:
        raise Exception("not valid port")
    app.run(host=host, port=port, debug=True)
    