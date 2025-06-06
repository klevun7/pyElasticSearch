import os
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)

# --- Configuration ---
ES_HOST = "http://localhost:9200"
INDEX_NAME = "reddit_posts"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# --- Elasticsearch Client ---
ES_CLIENT = None

def get_es_client():
    global ES_CLIENT
    if ES_CLIENT is None:
        logging.info(f"Connecting to Elasticsearch at {ES_HOST}...")
        try:
            ES_CLIENT = Elasticsearch(ES_HOST, verify_certs=False, ssl_show_warn=False)
            if not ES_CLIENT.ping():
                raise ValueError("Ping to Elasticsearch failed.")
            logging.info("Connected to Elasticsearch.")
        except Exception as e:
            logging.error("Failed to connect to Elasticsearch.", exc_info=True)
            ES_CLIENT = None
    return ES_CLIENT

# Initialize client once on app startup
with app.app_context():
    get_es_client()

# --- Query Builder ---
def create_ranking_query(query_str):
    return {
        "function_score": {
            "query": {
                "multi_match": {
                    "query": query_str,
                    "fields": ["content", "title^2", "selftext"],
                    "fuzziness": "AUTO"
                }
            },
            "functions": [
                {
                    "field_value_factor": {
                        "field": "reddit_score",
                        "modifier": "log1p",
                        "factor": 0.5,
                        "missing": 1
                    },
                    "weight": 1
                },
                {
                    "gauss": {
                        "timestamp": {
                            "origin": "now",
                            "scale": "30d",
                            "offset": "7d",
                            "decay": 0.5
                        }
                    },
                    "weight": 0.5
                }
            ],
            "score_mode": "multiply",
            "boost_mode": "multiply"
        }
    }

# --- Search Function ---
def search_es_index(query_str, num_results=10):
    es = get_es_client()
    if not es:
        return []

    try:
        query = create_ranking_query(query_str)
        response = es.search(
            index=INDEX_NAME,
            body={
                "query": query,
                "size": num_results,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"timestamp": {"order": "desc"}}
                ]
            }
        )

        results = []
        for hit in response.get('hits', {}).get('hits', []):
            src = hit['_source']
            timestamp = datetime.fromtimestamp(src.get('timestamp')).strftime('%Y-%m-%d %H:%M:%S') if src.get('timestamp') else 'N/A'
            results.append({
                "score": hit['_score'],
                "doc_id": src.get("doc_id"),
                "author": src.get("author"),
                "content": src.get("content"),
                "subreddit": src.get("subreddit"),
                "reddit_score": src.get("reddit_score"),
                "timestamp": timestamp,
                "type": src.get("type"),
                "title": src.get("title")
            })
        return results

    except Exception as e:
        logging.error("Search error:", exc_info=True)
        return []

# --- Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['GET'])
def search():
    query_text = request.args.get('q', '').strip()
    results = []
    if query_text:
        logging.info(f"Search query received: '{query_text}'")
        results = search_es_index(query_text, num_results=10)
    return render_template('results.html', query=query_text, results=results)

@app.route('/api/search', methods=['GET'])
def api_search():
    query_text = request.args.get('q', '').strip()
    num_results = min(int(request.args.get('limit', 10)), 100)
    results = search_es_index(query_text, num_results=num_results) if query_text else []
    return jsonify(results)

# --- App Entry ---
if __name__ == '__main__':
    app.run(debug=False)
