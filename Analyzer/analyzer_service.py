from flask import Flask, request, jsonify
from elastic_search import create_es, stream_all_articles
from dotenv import load_dotenv
from subj_classify import SubjectivityClassifier
from transformers import pipeline
from typing import Any
from concurrent.futures import ThreadPoolExecutor
import threading
import os
import time

load_dotenv()
start = time.time()
clf = pipeline("text-classification", model="./subjectivity-classifier", tokenizer="./subjectivity-classifier")
end = time.time()

print(f"Elapsed time: {end - start:.4f} seconds")

INDEX = os.getenv("ES_INDEX", "articles")

app = Flask(__name__)

def worker(article: dict):
    res = clf(article["description"].split('.'))
    subj_cnt = 0
    for res_obj in res:
        subj_cnt += (res_obj['label'] == 'SUBJ')
    print(subj_cnt / max(len(res), 1))     

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/service/analyzer")
def analyzer():
    #read the articles from Elastic Search.
    #take descriptions of every articles
    #split description by .
    #do batch prediction on subjectivity classification
    data: dict[str, str] = request.get_json(silent=True) or {} 
    query_id = data['query_id']
    if not query_id or not isinstance(query_id, str):
        return jsonify({"error": "Missing or invalid query_id"}), 400
    
    es = create_es()
    sc = SubjectivityClassifier()
    
    '''
    start = time.time()
    with ThreadPoolExecutor(max_workers=1) as executor:
        list(executor.map(worker, stream_all_articles(es, query_id)))
    end = time.time()
    '''
    start = time.time()
    for article in stream_all_articles(es, query_id):
        res = clf(article["description"].split('.'))
        subj_cnt = 0
        for res_obj in res:
            subj_cnt += (res_obj['label'] == 'SUBJ')
        print(subj_cnt / max(len(res), 1))  
    end = time.time()
    print("Classification:", end - start)    
    
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    host = os.getenv("ANALYZER_HOST")
    port = os.getenv("ANALYZER_PORT")
    if not port:
        raise Exception("missing port")
    try:
        port = int(port)
    except:
        raise Exception("not valid port")
    
    app.run(host=host, port=port, debug=True) 
    
#show articles first,
#then load summary and community response to avoid a delay on user side

#entity recognition
#show the opinions of notable figures and show the polarity of their opinions.

#add polarity/bias metric to evaluate/rank the articles.




        

