import json
import os
import time
import argparse
from atproto import Client
import requests
from bs4 import BeautifulSoup

client = Client()
client.login('mikenike034.bsky.social', 'c4dp-bbfd-7576-u6z2')

target_size_mb = 100
keywords = [
    "deep state", "false flag", "missing scientists", 
    "dead internet theory", "psyop", "smart city surveillance",
    "new world order", "cabal", "frazzledrip", "pizza gate", "skull and bones",
    "Area 51", "Roswell", "JFK assassination", "Moon landing hoax", 
    "Bermuda Triangle", "Flat Earth", "Neuralink surveillance", "Digital ID tracking",
    "Central Bank Digital Currency", "Starlink monitoring", "Alex Jones", "Reptilian",
    "missing researchers", "AI sentience"
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

def get_file_size_mb(path):
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0

# function to get the title of an html page linked to a Bluesky post
def get_page_title(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup.title.string.strip() if soup.title else "No Title Found"
    except Exception as e:
        return f"Error fetching title: {e}"


def main():
    parser = argparse.ArgumentParser(description="BlueSky Crawler")
    parser.add_argument("seen_file", help="Path to txt file with post URIs already seen")
    parser.add_argument("num_posts_per_page", type=int, help="Number of desired posts per page")
    parser.add_argument("comment_depth", type=int, help="Depth of comments thread")
    parser.add_argument("output_dir", help="Directory to save data")
    
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    print(f'Number of key words: {len(keywords)}')

    seen_posts = set()

    cursor = None
    df_num = 1
    total_dataset_size = 0
    data_file_path = os.path.join(args.output_dir, f'bluesky_conspiracy_datafile{df_num}.json')
    for word in keywords:
        if total_dataset_size >= target_size_mb:
            print(f"Target of {target_size_mb}MB reached. Stopping.")
            break
        if get_file_size_mb(data_file_path) >= 10:
            total_dataset_size += get_file_size_mb(data_file_path)
            print("Current file reached 10 MB, creating new file...")
            df_num += 1
            data_file_path = os.path.join(args.output_dir, f'bluesky_conspiracy_datafile{df_num}.json')
            
        print(f"Searching for: {word}...")
        try:
            for i in range(5):
                search = client.app.bsky.feed.search_posts(params={'q': word, 'limit': args.num_posts_per_page, 'cursor': cursor})
                cursor = search.cursor

                if not cursor:
                    break
                for post_view in search.posts:
                    if total_dataset_size >= target_size_mb:
                        break
                    if post_view.uri in seen_posts:
                        continue
                    seen_posts.add(post_view.uri)

                    with open(args.seen_file, "a", encoding="utf-8") as s:
                        s.write(post_view.uri + '\n')

                    thread = client.app.bsky.feed.get_post_thread(params={'uri': post_view.uri, 'depth': args.comment_depth})
                    post_data = thread.model_dump()

                    url = extract_url_from_post(post_view)
                    if url:
                        post_data['linked_url'] = url
                        post_data['linked_url_title'] = fetch_page_title(url)
                    else:
                        post_data['linked_url'] = None
                        post_data['linked_url_title'] = None
                    
                    with open(data_file_path, "a", encoding="utf-8") as f:
                        json_format = json.dumps(post_data, ensure_ascii=False)
                        f.write(json_format + '\n')
                    if (total_dataset_size >= 10):
                        print(f"Current Progress: {total_dataset_size + get_file_size_mb(data_file_path):.2f}MB", end="\r")
                    else:
                        print(f"Current Progress: {get_file_size_mb(data_file_path):.2f}MB", end="\r")
                if i == 4:
                    print()
        except Exception as e:
            print(f"\nError fetching {word}: {e}")
            time.sleep(5)

    print(f'\nNumber of unique posts: {len(seen_posts)}')
    print("\nCollection Complete.")

if __name__ == "__main__":
    main()