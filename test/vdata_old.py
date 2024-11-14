import os
import csv
import subprocess
import isodate
from datetime import datetime
from tqdm import tqdm
from googleapiclient.discovery import build

def load_api_keys():
    try:
        with open("api_keys.txt", "r") as f:
            return [key.strip() for key in f.readlines() if key.strip()]
    except FileNotFoundError:
        print("Error: api_keys.txt not found.")
        return []

API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
API_KEYS = load_api_keys()
current_key_index = 0

CATEGORY_MAPPING = {
    "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music",
    "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events",
    "20": "Gaming", "22": "People & Blogs", "23": "Comedy",
    "24": "Entertainment", "25": "News & Politics", "26": "How-to & Style",
    "27": "Education", "28": "Science & Technology", "29": "Nonprofits & Activism"
}
def switch_api_key():
    """Switch to the next available API key."""
    global current_key_index
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    if current_key_index == 0:
        raise Exception("All API keys have been exhausted.")
    print(f"Switching to API key {current_key_index + 1}/{len(API_KEYS)}")

def build_youtube_service():
    """Build the YouTube service using the current API key."""
    global current_key_index
    api_key = API_KEYS[current_key_index]
    try:
        print(f"Using API Key {current_key_index + 1}/{len(API_KEYS)}")
        return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key)
    except Exception as e:
        print(f"Failed to build YouTube service: {e}")
        switch_api_key()
        return build_youtube_service()

def extract_identifier(url):
    if "playlist?list=" in url:
        return url.split("list=")[1], "playlist"
    elif "watch?v=" in url:
        return url.split("watch?v=")[1].split("&")[0], "video"
    elif "youtu.be/" in url:
        return url.split("youtu.be/")[1].split("?")[0], "video"
    elif "shorts/" in url:
        return url.split("shorts/")[1].split("?")[0], "video"
    elif "/channel/" in url:
        return url.split("/channel/")[1], "channel_id"
    elif "@" in url:
        return url.split("@")[1], "handle"
    return None, None

def resolve_handle_to_channel_id(handle):
    youtube = build_youtube_service()
    try:
        request = youtube.search().list(part="snippet", q=handle, type="channel", maxResults=1)
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
    except Exception as e:
        print(f"Error resolving handle '{handle}' to channel ID: {e}")
    return None

def get_playlist_videos(playlist_id):
    youtube = build_youtube_service()
    video_ids = []
    request = youtube.playlistItems().list(part="contentDetails", playlistId=playlist_id, maxResults=50)
    while request:
        response = request.execute()
        video_ids.extend(item['contentDetails']['videoId'] for item in response.get('items', []))
        request = youtube.playlistItems().list_next(request, response)
    return video_ids

def get_channel_videos(channel_id):
    youtube = build_youtube_service()
    video_ids = []
    request = youtube.search().list(part="id", channelId=channel_id, maxResults=50, type="video")
    while request:
        response = request.execute()
        video_ids.extend(item['id']['videoId'] for item in response.get('items', []) if 'videoId' in item['id'])
        request = youtube.search().list_next(request, response)
    return video_ids

def parse_duration(duration):
    try:
        return int(isodate.parse_duration(duration).total_seconds())
    except Exception:
        return 0

def check_existing_video_ids(video_ids):
    existing_ids = set()
    for file in os.listdir("metadata"):
        if file.endswith(".csv"):
            with open(os.path.join("metadata", file), mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                existing_ids.update(row['Video ID'] for row in reader)
    return [video_id for video_id in video_ids if video_id not in existing_ids]

def get_video_metadata(video_ids):
    youtube = build_youtube_service()
    video_data = []

    for i in tqdm(range(0, len(video_ids), 50), desc="Fetching Video Metadata"):
        batch_ids = video_ids[i:i + 50]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics,topicDetails",
            id=",".join(batch_ids)
        )
        try:
            response = request.execute()
        except Exception as e:
            print(f"Error fetching metadata: {e}")
            continue

        for item in response.get('items', []):
            snippet = item.get('snippet', {})
            content_details = item.get('contentDetails', {})
            statistics = item.get('statistics', {})
            topics = item.get('topicDetails', {}).get('topicCategories', [])
            category = CATEGORY_MAPPING.get(snippet.get('categoryId', 'Unknown'), "Unknown")

            video_data.append({
                "Video ID": item['id'],
                "Title": snippet.get('title'),
                "Channel ID": snippet.get('channelId'),
                "Author": snippet.get('channelTitle'),
                "Description": snippet.get('description'),
                "Category": category,
                "Topics": ', '.join(topics),
                "Length (Seconds)": parse_duration(content_details.get('duration')),
                "Published": snippet.get('publishedAt'),
                "Audio Language": snippet.get('defaultAudioLanguage', 'Unknown'),
                "Views": statistics.get('viewCount', '0'),
                "Tags": ', '.join(snippet.get('tags', []))
            })
    return video_data

def save_to_csv(data):
    os.makedirs("metadata", exist_ok=True)
    filename = f"metadata/metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    fieldnames = ["Video ID", "Title", "Channel ID", "Author", "Description",
                  "Category", "Topics", "Length (Seconds)", "Published",
                  "Audio Language", "Views", "Tags"]

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    subprocess.run(["python", "report.py"], check=True)
    subprocess.run(["python", "cdata.py"], check=True)
    subprocess.run(["python", "deduplication.py"], check=True)

def process_urls(urls):
    all_video_data = []
    for url in tqdm(urls, desc="Processing URLs"):
        identifier, url_type = extract_identifier(url)
        video_ids = []

        if url_type == "handle":
            identifier = resolve_handle_to_channel_id(identifier)
            if identifier:
                video_ids = get_channel_videos(identifier)
        elif url_type == "channel_id":
            video_ids = get_channel_videos(identifier)
        elif url_type == "playlist":
            video_ids = get_playlist_videos(identifier)
        elif url_type == "video":
            video_ids = [identifier]

        if video_ids:
            new_video_ids = check_existing_video_ids(video_ids)
            if new_video_ids:
                video_metadata = get_video_metadata(new_video_ids)
                all_video_data.extend(video_metadata)

    if all_video_data:
        save_to_csv(all_video_data)
