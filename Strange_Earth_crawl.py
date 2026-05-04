import json
import os
import time
from atproto import Client

client = Client()
client.login('halls1424.bsky.social', 'Lakers1424')

data_file_path = 'bluesky_strange_earth_data.json'
seen_posts_path = 'seen_posts.txt'
key_words_used_path = 'key_words_used_strange_earth.txt'
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

def get_file_size_mb(path):
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0

seen_posts = set()

cursor = None
for word in keywords:
    if get_file_size_mb(data_file_path) >= target_size_mb:
        print(f"Target of {target_size_mb}MB reached. Stopping.")
        break

    print(f"Searching for: {word}...")
    try:
        with open(key_words_used_path, "a", encoding="utf-8") as k:
            k.write(word + '\n')

        for i in range(12):
            search = client.app.bsky.feed.search_posts(
                params={'q': word, 'limit': 100, 'cursor': cursor}
            )
            cursor = search.cursor

            if not cursor:
                break

            for post_view in search.posts:
                if get_file_size_mb(data_file_path) >= target_size_mb:
                    break

                if post_view.uri in seen_posts:
                    continue

                seen_posts.add(post_view.uri)

                with open(seen_posts_path, "a", encoding="utf-8") as s:
                    s.write(post_view.uri + '\n')

                thread = client.app.bsky.feed.get_post_thread(
                    params={'uri': post_view.uri, 'depth': 30}
                )

                with open(data_file_path, "a", encoding="utf-8") as f:
                    json_format = json.dumps(thread.model_dump(), ensure_ascii=False)
                    f.write(json_format + '\n')

                current_size = get_file_size_mb(data_file_path)
                print(f"Current Progress: {current_size:.2f} MB, page = {i+1}", end="\r")

            if i == 4:
                print()

    except Exception as e:
        print(f"\nError fetching {word}: {e}")
        time.sleep(5)

print(f'\nNumber of unique posts: {len(seen_posts)}')
print("\nCollection Complete.")
