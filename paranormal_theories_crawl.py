import json
import os
import time
import requests #fetch URL titles
from bs4 import BeautifulSoup #parse HTML titles
from atproto import Client

client = Client()
client.login('azka_b.bsky.social', 'dkt2-irke-bscz-trxp')

output_dir = 'bluesky_paranormal_data'
seen_posts_path = 'seen_posts.txt'
key_words_used_path = 'key_words_used.txt'

target_size_mb = 150
max_file_size_mb = 10


keywords = [
    "ghost sighting", "UFO sighting", "alien abduction", "poltergeist", 
    "haunted house", "shadow people", "near death experience", "cryptid", 
    "bigfoot", "loch ness monster", "astral projection", "demonic possession",
    "exorcism", "interdimensional beings", "skinwalker", "mothman", "sleep paralysis",
    "time slip", "paranormal activity", "spirit communication", "ouija board",
    "haunted location", "ghost encounter", "supernatural phenomenon",
    "clairvoyance", "psychic ability", "telekinesis"
]

#getting total size of output folder
def get_folder_size_mb(folder):
    total = 0
    for f in os.listdir(folder):
        fp = os.path.join(folder, f)
        if os.path.isfile(fp):
            total += os.path.getsize(fp)
    return total / (1024 * 1024)

#file writing in 10mb splits
def get_current_file(folder):
    index = 1
    while True:
        path = os.path.join(folder, f'paranormal_data_{index}.json')
        if not os.path.exists(path):
            return path
        if os.path.getsize(path) / (1024 * 1024) < max_file_size_mb:
            return path
        index += 1

#fetch webpage title from URL
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

seen_posts = set()
#load previously seen posts so duplicates are not fetched again
if os.path.exists(seen_posts_path):
    with open(seen_posts_path, "r", encoding="utf-8") as s:
        seen_posts = set(line.strip() for line in s if line.strip())

#load already used keywords to avoid repeating searches
used_keywords = set()
if os.path.exists(key_words_used_path):
    with open(key_words_used_path, "r", encoding="utf-8") as k:
        used_keywords = set(line.strip() for line in k if line.strip())

#create output folder for stored data
os.makedirs(output_dir, exist_ok=True)

cursor = None
for word in keywords:
    if get_folder_size_mb(output_dir) >= target_size_mb:
        print(f"Target of {target_size_mb}MB reached. Stopping.")
        break

    #skip keywords that have already been used
    if word in used_keywords:
        print(f"Keyword '{word}' already used. Skipping.")
        continue
        
    print(f"Searching for: {word}...")
    try:
        with open(key_words_used_path, "a", encoding="utf-8") as k:
                k.write(word + '\n')
        #track used keywords to avoid repeating searches
        used_keywords.add(word)

        for i in range(5):
            search = client.app.bsky.feed.search_posts(
                params={'q': word, 'limit': 100, 'cursor': cursor}
            )
            cursor = search.cursor

            for post_view in search.posts:
                #checking total folder size instead of single file size
                if get_folder_size_mb(output_dir) >= target_size_mb:
                    break
                if post_view.uri in seen_posts:
                    continue
                seen_posts.add(post_view.uri)
                
                with open(seen_posts_path, "a", encoding="utf-8") as s:
                    s.write(post_view.uri + '\n')
                            
                thread = client.app.bsky.feed.get_post_thread(
                    params={'uri': post_view.uri, 'depth': 30}
                )

                #store in variable first to add URL title if it has one
                post_data = thread.model_dump()

                #fetch and attatch URL title if post has a URL
                url = extract_url_from_post(post_view)
                if url:
                    post_data['linked_url'] = url
                    post_data['linked_url_title'] = fetch_page_title(url)
                else:
                    post_data['linked_url'] = None
                    post_data['linked_url_title'] = None

                current_file = get_current_file(output_dir)
                with open(current_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(post_data, ensure_ascii=False) + '\n')

                current_size = get_folder_size_mb(output_dir)
                print(f"Current Progress: {current_size:.2f} MB, page = {i+1}", end="\r")

            if not cursor:
                break

            if i == 4:
                print()

    except Exception as e:
        print(f"\nError fetching {word}: {e}")
        time.sleep(5)

print(f'\nNumber of unique posts: {len(seen_posts)}')
print("\nCollection Complete.")