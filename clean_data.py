import json
import os
from datetime import datetime

# Input folders - all 4 topic folders
input_folders = [
    'bluesky_conspiracy_data',
    'bluesky_paranormal_data',
    'bluesky_strange_earth_data',
    'bluesky_ufo_data'
]

# Output folder for clean data
output_dir = 'bluesky_clean_data'
os.makedirs(output_dir, exist_ok=True)

def format_title(text):
    """Take first line, first 20 chars, always followed by ..."""
    first_line = text.split('\n')[0] if text else ''
    return first_line[:20] + '...'

def format_timestamp(ts):
    """Convert ISO timestamp to MM/DD/YYYY HH:MM military time"""
    try:
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return dt.strftime('%m/%d/%Y %H:%M')
    except Exception:
        return ts

def get_image_titles(embed):
    """Extract alt text/titles from any images attached to post"""
    image_titles = []
    if not embed or not isinstance(embed, dict):
        return image_titles
    images = embed.get('images', [])
    for img in images:
        if isinstance(img, dict):
            alt = img.get('alt', '').strip()
            if alt:
                image_titles.append(alt)
    return image_titles

def get_top_replies(replies, top_n=5):
    """Extract text from top 5 replies"""
    reply_texts = []
    for reply in replies[:top_n]:
        try:
            rpost = reply.get('post', {})
            text = rpost.get('record', {}).get('text', '').strip()
            if text:
                reply_texts.append(text)
        except Exception:
            continue
    return reply_texts

def clean_post(data):
    """Extract only the fields we need from a raw post"""
    try:
        thread = data.get('thread', {})
        post = thread.get('post', {})
        record = post.get('record', {})
        author = post.get('author', {})
        replies = thread.get('replies', [])
        embed = post.get('embed')

        # build post URL from URI
        uri = post.get('uri', '')
        did = author.get('did', '')
        post_id = uri.split('/')[-1] if uri else ''
        post_url = f"https://bsky.app/profile/{did}/post/{post_id}" if did and post_id else ''

        text = record.get('text', '')
        raw_ts = record.get('created_at', '')

        clean = {
            "title": format_title(text),
            "user": author.get('handle', ''),
            "display_name": author.get('display_name', ''),
            "post_url": post_url,
            "text": text,
            "time": format_timestamp(raw_ts),
            "reply_count": post.get('reply_count', 0),
            "top_5_replies": get_top_replies(replies),
            "like_count": post.get('like_count', 0),
            "repost_count": post.get('repost_count', 0),
            "image_titles": get_image_titles(embed),
            "linked_url": data.get('linked_url'),
            "linked_url_title": data.get('linked_url_title')
        }
        return clean
    except Exception as e:
        return None

# Process each folder
total_posts = 0
for folder in input_folders:
    if not os.path.exists(folder):
        print(f"Folder not found, skipping: {folder}")
        continue

    topic = folder.replace('bluesky_', '').replace('_data', '')
    output_file = os.path.join(output_dir, f'clean_{topic}.json')
    count = 0

    print(f"Cleaning {folder}...")

    with open(output_file, 'w', encoding='utf-8') as out_f:
        for filename in os.listdir(folder):
            if not filename.endswith('.json'):
                continue
            filepath = os.path.join(folder, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        clean = clean_post(data)
                        if clean and clean['text']:
                            out_f.write(json.dumps(clean, ensure_ascii=False) + '\n')
                            count += 1
                    except Exception:
                        continue

    print(f"  Done: {count} posts saved to {output_file}")
    total_posts += count

print(f"\nCleaning complete. Total posts: {total_posts}")
print(f"Clean files saved in: {output_dir}/")
