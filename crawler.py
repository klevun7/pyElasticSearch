import praw
import json
import os
import requests
from bs4 import BeautifulSoup
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) != 4:
    print("Usage: python crawler.py seed_data/<seed_file> <num_posts> <output_dir>")
    sys.exit(1)

seed_file = sys.argv[1]
num_posts = int(sys.argv[2])
output_dir = sys.argv[3]


MAX_FILE_SIZE_MB = 100
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
SLEEP_TIME = 0.5
MAX_WORKERS = 10
KEYWORDS = []


reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT")
)


if not os.path.exists(output_dir):
    os.makedirs(output_dir)


file_index = 0
current_size = 0
current_file_path = os.path.join(output_dir, f"posts_{file_index}.jsonl")
current_file = open(current_file_path, "w", encoding="utf-8")

def write_post(post_data):
    global current_file, current_size, file_index
    line = json.dumps(post_data) + "\n"
    size = len(line.encode("utf-8"))

    if current_size + size > MAX_FILE_SIZE_BYTES:
        current_file.close()
        file_index += 1
        current_file_path = os.path.join(output_dir, f"posts_{file_index}.jsonl")
        current_file = open(current_file_path, "w", encoding="utf-8")
        current_size = 0

    current_file.write(line)
    current_size += size

def get_page_title(url):
    try:
        headers = {'User-Agent': USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=5)
        if "text/html" in resp.headers.get("Content-Type", ""):
            soup = BeautifulSoup(resp.text, "html.parser")
            return soup.title.string.strip() if soup.title else None
    except Exception:
        return None


with open(seed_file, "r") as f:
    subreddits = [line.strip() for line in f if line.strip()]


post_count = 0
seen_ids = set()

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for sub in subreddits:
        print(f"\n Crawling r/{sub}...")

        sources = [
            reddit.subreddit(sub).hot(limit=1000),
            reddit.subreddit(sub).new(limit=1000),
            reddit.subreddit(sub).top(limit=1000),
            reddit.subreddit(sub).rising(limit=100)
        ]

        for source in sources:
            for post in source:
                if post.id in seen_ids:
                    continue
                seen_ids.add(post.id)

                if post_count >= num_posts:
                    break

                if KEYWORDS and not any(kw.lower() in post.title.lower() for kw in KEYWORDS):
                    continue

                try:
                    post.comments.replace_more(limit=0)
                    comments = [{
                        "author": str(c.author),
                        "body": c.body,
                        "score": c.score,
                        "created_utc": c.created_utc
                    } for c in post.comments.list()[:50]]
                except Exception:
                    comments = []

                data = {
                    "id": post.id,
                    "title": post.title,
                    "selftext": post.selftext,
                    "author": str(post.author),
                    "author_fullname": getattr(post, "author_fullname", None),
                    "url": post.url,
                    "created_utc": post.created_utc,
                    "permalink": f"https://reddit.com{post.permalink}",
                    "subreddit": sub,
                    "subreddit_id": post.subreddit_id,
                    "num_comments": post.num_comments,
                    "score": post.score,
                    "upvote_ratio": post.upvote_ratio,
                    "link_flair_text": post.link_flair_text,
                    "domain": post.domain,
                    "is_self": post.is_self,
                    "is_original_content": post.is_original_content,
                    "is_video": post.is_video,
                    "view_count": post.view_count,
                    "thumbnail": post.thumbnail,
                    "edited": post.edited,
                    "media": str(post.media),
                    "preview": str(post.preview) if hasattr(post, "preview") else None,
                    "media_metadata": str(getattr(post, "media_metadata", None)),
                    "all_awardings": [award['name'] for award in post.all_awardings],
                    "comments": comments
                }

                if post.url.startswith("http") and "reddit.com" not in post.url:
                    title = executor.submit(get_page_title, post.url).result()
                    if title:
                        data["linked_page_title"] = title

                write_post(data)
                post_count += 1

                if post_count % 500 == 0:
                    print(f" {post_count} posts written, ~{current_size / (1024 * 1024):.2f} MB so far.")

                time.sleep(SLEEP_TIME)

            if post_count >= num_posts:
                break


current_file.close()
print(f"\n Done. Collected {post_count} posts in {file_index + 1} files (~{current_size / (1024 * 1024):.2f} MB total).")

