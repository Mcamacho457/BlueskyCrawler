import json
import os
import time
from atproto import Client
import requests
from bs4 import BeautifulSoup

client = Client()
client.login('halls1424.bsky.social', 'Lakers1424')

seen_posts_path = 'seen_posts.txt'
target_size_mb = 100

keywords = [
    "ancient civilization", "lost civilization", "ancient ruins",
    "megalith", "pyramid mystery", "ancient technology",
    "unexplained earth", "earth mystery", "strange earth",
    "anomaly", "mysterious structure", "underwater ruins",
    "ancient maps", "stone circles", "gobekli tepe",
    "atlantis", "ley lines", "earth energy",
    "strange fossils", "unexplained phenomenon",
    "mysterious cave", "forbidden archaeology",
    "ancient aliens", "lost city", "hidden history"
]
def fetch_page_title(url):
    try:
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True)
    except Exception:
        pass
    return None

#extract URL from post if it has one
def extract_url_from_post(post_view):
    try:
        embed = post_view.post.embed
        if embed and hasattr(embed, 'external'):
            return embed.external.uri
    except Exception:
        pass
    return None

# function to get the title of an html page linked to a Bluesky post
def get_page_title(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup.title.string.strip() if soup.title else "No Title Found"
    except Exception as e:
        return f"Error fetching title: {e}"

def get_file_size_mb(path):
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0

#create output folder for stored data
os.makedirs("bluesky_strange_earth_data", exist_ok=True)

seen_posts = set()
#load previously seen posts so duplicates are not fetched again
if os.path.exists(seen_posts_path):
    with open(seen_posts_path, "r", encoding="utf-8") as s:
        seen_posts = set(line.strip() for line in s if line.strip())

cursor = None
df_num = 1
total_dataset_size = 0
data_file_path = os.path.join("bluesky_strange_earth_data", f'bluesky_strange_earth_datafile{df_num}.json')
for word in keywords:
    if total_dataset_size >= target_size_mb:
        print(f"Target of {target_size_mb}MB reached. Stopping.")
        break
    if get_file_size_mb(data_file_path) >= 10:
            total_dataset_size += get_file_size_mb(data_file_path)
            print("Current file reached 10 MB, creating new file...")
            df_num += 1
            data_file_path = os.path.join("bluesky_strange_earth_data", f'bluesky_conspiracy_datafile{df_num}.json')

    print(f"Searching for: {word}...")
    try:
        # with open(key_words_used_path, "a", encoding="utf-8") as k:
        #     k.write(word + '\n')

        for i in range(5):
            search = client.app.bsky.feed.search_posts(
                params={'q': word, 'limit': 100, 'cursor': cursor}
            )
            cursor = search.cursor

            if not cursor:
                break

            for post_view in search.posts:
                if total_dataset_size >= target_size_mb:
                    break

                if post_view.uri in seen_posts:
                    continue

                seen_posts.add(post_view.uri)

                with open(seen_posts_path, "a", encoding="utf-8") as s:
                    s.write(post_view.uri + '\n')

                thread = client.app.bsky.feed.get_post_thread(
                    params={'uri': post_view.uri, 'depth': 30}
                )

                post_data = thread.model_dump()

                url = extract_url_from_post(post_view)
                if url:
                    post_data['linked_url'] = url
                    post_data['linked_url_title'] = fetch_page_title(url)
                else:
                    post_data['linked_url'] = None
                    post_data['linked_url_title'] = None

                with open(data_file_path, "a", encoding="utf-8") as f:
                    json_format = json.dumps(thread.model_dump(), ensure_ascii=False)
                    f.write(json_format + '\n')
                if (total_dataset_size >= 10):
                        print(f"Current Progress: {total_dataset_size + get_file_size_mb(data_file_path):.2f}MB", end="\r")
                else:
                    print(f"Current Progress: {get_file_size_mb(data_file_path):.2f}MB", end="\r")
                time.sleep(0.5)

            if i == 4:
                print()

    except Exception as e:
        print(f"\nError fetching {word}: {e}")
        time.sleep(5)

print(f'\nNumber of unique posts: {len(seen_posts)}')
print("\nCollection Complete.")
