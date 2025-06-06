import json
import os
import glob
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import traceback

# --- Configuration ---
ES_HOST = "http://localhost:9200"
INDEX_NAME = "reddit_posts"
DATA_DIR_PATTERN = "data/output_seed_*"  

# --- Elasticsearch Connection ---
def get_es_client():
    print(f"Connecting to Elasticsearch at {ES_HOST}...")
    try:
        es = Elasticsearch(
            ES_HOST,
            verify_certs=False,
            ssl_show_warn=False,
        )
        if not es.ping():
            raise ValueError("Connection to Elasticsearch failed!")
        print("Connected to Elasticsearch.")
        return es
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        traceback.print_exc()
        exit(1)

# --- Index Mapping ---
MAPPING = {
    "properties": {
        "doc_id": {"type": "keyword"},
        "author": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "content": {"type": "text"},
        "subreddit": {"type": "keyword"},
        "reddit_score": {"type": "integer"},
        "type": {"type": "keyword"},
        "title": {"type": "text"},
        "selftext": {"type": "text"},
    }
}

# --- Indexing Function ---
def index_reddit_data():
    es = get_es_client()

    if es.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists. Deleting and recreating...")
        es.indices.delete(index=INDEX_NAME)

    es.indices.create(index=INDEX_NAME, mappings=MAPPING, settings={"number_of_replicas": 0})
    print(f"Index '{INDEX_NAME}' created with mapping.")

    actions = []
    indexed_count = 0

    all_dirs = sorted(glob.glob(DATA_DIR_PATTERN))
    print(f"Found {len(all_dirs)} matching directories: {all_dirs}")

    for dir_path in all_dirs:
        print(f"Processing directory: {dir_path}")
        for filename in os.listdir(dir_path):
            if filename.endswith(".json") or filename.endswith(".jsonl"):
                filepath = os.path.join(dir_path, filename)
                print(f"  Reading file: {filepath}")
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f):
                        try:
                            data = json.loads(line.strip())
                            doc_body = {}

                            doc_id = data.get('id') or f"{filename}_{line_num}"
                            doc_body['doc_id'] = str(doc_id)

                            author = data.get('author')
                            if author and author != '[deleted]':
                                doc_body['author'] = author

                            created_utc = data.get('created_utc')
                            if created_utc is not None:
                                doc_body['timestamp'] = created_utc

                            content = data.get('body')
                            title = data.get('title')
                            selftext = data.get('selftext')

                            if content:
                                doc_body['content'] = content
                                doc_body['type'] = 'comment'
                            elif title:
                                doc_body['title'] = title
                                doc_body['content'] = title
                                if selftext and selftext != '[deleted]':
                                    doc_body['selftext'] = selftext
                                    doc_body['content'] += " " + selftext
                                doc_body['type'] = 'submission'

                            subreddit = data.get('subreddit')
                            if subreddit:
                                doc_body['subreddit'] = subreddit

                            score = data.get('score')
                            if score is not None:
                                doc_body['reddit_score'] = int(score)

                            actions.append({
                                "_index": INDEX_NAME,
                                "_id": doc_body['doc_id'],
                                "_source": doc_body
                            })
                            indexed_count += 1

                            if indexed_count % 10000 == 0:
                                print(f"  Prepared {indexed_count} documents so far...")

                        except json.JSONDecodeError as e:
                            print(f"Skipping malformed JSON in {filepath} line {line_num}: {e}")
                        except Exception as e:
                            print(f"Error in {filepath} line {line_num}: {e}")
                            traceback.print_exc()

    print(f"\nStarting bulk indexing of {len(actions)} documents...")
    success, failed = helpers.bulk(es, actions, index=INDEX_NAME, chunk_size=5000, request_timeout=60)
    print(f"Bulk indexing complete. Successful: {success}, Failed: {failed}")

    es.indices.refresh(index=INDEX_NAME)
    print("Index refreshed.")

# --- Main ---
if __name__ == "__main__":
    index_reddit_data()
