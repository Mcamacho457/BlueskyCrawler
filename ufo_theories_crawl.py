import json
import os
import time
import requests #fetch URL titles
from bs4 import BeautifulSoup #parse HTML titles
from atproto import Client

client = Client()
client.login('jasmarg.bsky.social', 'ProjectPassword!!')

output_dir = 'bluesky_ufo_data'
seen_posts_path = 'seen_posts.txt'
key_words_used_path = 'key_words_used.txt'

target_size_mb = 150
max_file_size_mb = 10

keywords = [
    "All-domain Anomaly Resolution Office", "Unidentified Anomalous Phenomena",
    "Trans-medium objects", "UAP transparency pledge", "David Grusch whistleblower",
    "Pentagon UFO footage", "Aegis UAP detection", "Anomalous Aerial Objects",
    "Fastwalkers", "Tic Tac UFO encounter", "Gimbal UAP video", "GoFast UFO video",
    "Non-Human Intelligence", "UAP Disclosure Act", "Crash retrieval program",
    "Interdimensional hypothesis", "Instantaneous acceleration", "Hypersonic velocity UAP",
    "Low observability craft", "Anti-gravity propulsion", "Interstellar object Oumuamua",
    "Project Blue Book archives", "Majestic 12 documents", "Bob Lazar S4",
    "Skinwalker Ranch activity", "Black Triangle sightings", "Orb over Lake Michigan",
    "Metallic disk craft", "Equilateral triangle formation", "Phoenix Lights anniversary",
    "Alien autopsy film", "Animal mutilations", "UFO abduction reports",
    "Groom Lake dry lake", "Nellis Air Force Base UFO", "NORAD unknown track reports",
    "Interplanetary Phenomenon Unit", "Ultra Terrestrials", "Foo fighters WWII",
    "Flying saucers", "Unconventional aircraft sightings", "Uncorrelated targets radar",
    "Alien life evidence 2026", "UAP congressional hearings", "NASA UAP independent study",
    "UAP task force", "Aviation safety UAP", "National Security State UAP",
    "UAP campaign plan 2034", "West Africa UFO sighting", "Skunk Works secret craft",
    "Reverse-engineering alien tech", "UFO record transfers", "Submerged UAP",
    "Space-based sensors UAP", "AARO PR-011 report", "High Strangeness",
    "UAP transparency caucus", "Non-ballistic flight patterns", "Cloaking technology UFO",
    "Plasma-based UAP", "UFO hotspot Map", "Military pilot UAP testimony",
    "James Webb UFO discovery", "Extraterrestrial origin theory", "Identified Alien Craft",
    "Dreamland facility", "Project Aquarius", "Unit 731 experiments",
    "Condon Committee report", "UFO disclosure project 2026", "AARO 2026 report",
    "Search for Extraterrestrial Intelligence", "Ancient Astronauts", "Contactee movement",
    "alien encounter", "UFO witness", "extraterrestrial contact", "alien spacecraft",
    "UFO crash site", "government UFO cover up", "area 51 secrets", "alien disclosure",
    "UAP evidence", "UFO video footage", "alien beings", "UFO investigation",
    "extraterrestrial life", "UFO sighting 2026", "alien technology",
    "UFO military encounter", "UAP whistleblower", "alien contact proof",
    "UFO radar tracking", "classified UFO files", "alien visitation",
    "UFO phenomenon", "UAP national security", "alien civilization",
    "UFO near miss", "UAP pilot report", "alien implants",
    "UFO landing", "extraterrestrial intelligence", "UAP sensor data",
    "alien coverup", "UFO close encounter", "UAP legislation",
    "alien signal", "UFO bermuda triangle", "UAP hearing 2026",
    "alien base moon", "UFO underwater", "UAP new evidence",
    "alien abductee testimony", "UFO crop circles", "UAP physics",
    "alien hybrid program", "UFO orb sighting", "UAP declassified",
    "alien autopsy footage", "UFO dogfight", "UAP tracking system"
]

# getting total size of output folder
def get_folder_size_mb(folder):
    total = 0
    for f in os.listdir(folder):
        fp = os.path.join(folder, f)
        if os.path.isfile(fp):
            total += os.path.getsize(fp)
    return total / (1024 * 1024)

# 10mb file write splits
def get_current_file(folder):
    index = 1
    while True:
        path = os.path.join(folder, f'ufo_data_{index}.json')
        if not os.path.exists(path):
            return path
        if os.path.getsize(path) / (1024 * 1024) < max_file_size_mb:
            return path
        index += 1

# fetch webpage title from URL
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

# extract URL from post if it has one
def extract_url_from_post(post_view):
    try:
        embed = post_view.post.embed
        if embed and hasattr(embed, 'external'):
            return embed.external.uri
    except Exception:
        pass
    return None

seen_posts = set()
# load previously seen posts so duplicates are not fetched again
if os.path.exists(seen_posts_path):
    with open(seen_posts_path, "r", encoding="utf-8") as s:
        seen_posts = set(line.strip() for line in s if line.strip())

# load already used keywords to avoid repeating searches
used_keywords = set()
if os.path.exists(key_words_used_path):
    with open(key_words_used_path, "r", encoding="utf-8") as k:
        used_keywords = set(line.strip() for line in k if line.strip())

# creates output folder for stored data
os.makedirs(output_dir, exist_ok=True)

cursor = None
for word in keywords:
    if get_folder_size_mb(output_dir) >= target_size_mb:
        print(f"Target of {target_size_mb}MB reached. Stopping.")
        break

    # skip keywords that have already been used
    if word in used_keywords:
        print(f"Keyword '{word}' already used. Skipping.")
        continue

    print(f"Searching for: {word}...")
    try:
        with open(key_words_used_path, "a", encoding="utf-8") as k:
                k.write(word + '\n')
        # track used keywords to avoid repeating searches
        used_keywords.add(word)

        for i in range(5):
            search = client.app.bsky.feed.search_posts(
                params={'q': word, 'limit': 100, 'cursor': cursor}
            )
            cursor = search.cursor

            for post_view in search.posts:
                # checking total folder size instead of single file size
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

                # store in variable first to add URL title if it has one
                post_data = thread.model_dump()

                # fetch and attatch URL title if post has a URL
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
        time.sleep(10)

print(f'\nNumber of unique posts: {len(seen_posts)}')
print("\nCollection Complete.")
