import os
import csv
import subprocess
import time
import isodate
from datetime import datetime
from tqdm import tqdm
from googleapiclient.discovery import build
import re
import traceback

# Load API keys from a file or environment variable
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
API_KEYS = load_api_keys() or [API_KEY]  # Use API_KEY as fallback if no keys are loaded
current_key_index = 0

CATEGORY_MAPPING = {
    "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music",
    "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events",
    "20": "Gaming", "22": "People & Blogs", "23": "Comedy",
    "24": "Entertainment", "25": "News & Politics", "26": "How-to & Style",
    "27": "Education", "28": "Science & Technology", "29": "Nonprofits & Activism"
}

# Function to log errors to a file
def log_error(message):
    with open("error_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")

def switch_api_key():
    """Switch to the next available API key."""
    global current_key_index
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    print(f"Switching to API key {current_key_index + 1}/{len(API_KEYS)}")

def build_youtube_service():
    """Build the YouTube service using the current API key."""
    api_key = API_KEYS[current_key_index]
    try:
        print(f"Using API Key {current_key_index + 1}/{len(API_KEYS)}")
        return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key)
    except Exception as e:
        log_error(f"Failed to build YouTube service: {e}")
        switch_api_key()
        return build_youtube_service()

def extract_identifier(url):
    """Extract the identifier (video, playlist, channel, handle) from the URL."""
    match_playlist = re.search(r'playlist\?list=([^&]+)', url)
    match_video = re.search(r'(?:watch\?v=|\/v\/)([a-zA-Z0-9_-]+)', url)
    match_channel = re.search(r'\/channel\/([^/]+)', url)
    match_handle = re.search(r'@([^/]+)', url)
    
    if match_playlist:
        return match_playlist.group(1), "playlist"
    elif match_video:
        return match_video.group(1), "video"
    elif match_channel:
        return match_channel.group(1), "channel_id"
    elif match_handle:
        return match_handle.group(1), "handle"
    return None, None

def resolve_handle_to_channel_id(handle):
    """Resolve a YouTube handle to a channel ID."""
    youtube = build_youtube_service()
    try:
        request = youtube.search().list(part="snippet", q=handle, type="channel", maxResults=1)
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
    except Exception as e:
        log_error(f"Error resolving handle '{handle}' to channel ID: {e}")
    return None

# Using the error handler decorator
def handle_errors(func):
    """Decorator to handle errors and switch API keys if needed."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_error(f"Error in {func.__name__}: {e}")
            traceback.print_exc()
            switch_api_key()
            return None
    return wrapper

@handle_errors
def get_playlist_videos(playlist_id):
    """Retrieve all video IDs from a playlist."""
    youtube = build_youtube_service()
    video_ids = []
    request = youtube.playlistItems().list(part="contentDetails", playlistId=playlist_id, maxResults=50)
    while request:
        response = request.execute()
        video_ids.extend(item['contentDetails']['videoId'] for item in response.get('items', []))
        request = youtube.playlistItems().list_next(request, response)
    return video_ids

@handle_errors
def get_channel_videos(channel_id):
    """Retrieve all video IDs from a channel."""
    youtube = build_youtube_service()
    video_ids = []
    request = youtube.search().list(part="id", channelId=channel_id, maxResults=50, type="video")
    while request:
        response = request.execute()
        video_ids.extend(item['id']['videoId'] for item in response.get('items', []) if 'videoId' in item['id'])
        request = youtube.search().list_next(request, response)
    return video_ids

def parse_duration(duration):
    """Parse ISO 8601 duration into total seconds."""
    try:
        return int(isodate.parse_duration(duration).total_seconds())
    except Exception:
        return 0

def check_existing_video_ids(video_ids):
    """Check which video IDs are not already present in CSV files."""
    existing_ids = set()
    metadata_dir = "metadata"
    for file in os.listdir(metadata_dir):
        if file.endswith(".csv"):
            with open(os.path.join(metadata_dir, file), mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                existing_ids.update(row['Video ID'] for row in reader)
    return [video_id for video_id in video_ids if video_id not in existing_ids]

@handle_errors
def get_video_metadata(video_ids):
    """Fetch metadata for a batch of video IDs."""
    youtube = build_youtube_service()
    video_data = []
    for i in tqdm(range(0, len(video_ids), 50), desc="Fetching Video Metadata"):
        batch_ids = video_ids[i:i + 50]
        request = youtube.videos().list(part="snippet,contentDetails,statistics,topicDetails", id=",".join(batch_ids))
        retries = 3
        while retries > 0:
            try:
                response = request.execute()
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
                break
            except Exception as e:
                log_error(f"Error fetching metadata: {e}")
                retries -= 1
                if retries > 0:
                    print(f"Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print(f"Max retries reached, skipping batch.")
    return video_data

def save_to_csv(data):
    """Save video metadata to a CSV file."""
    os.makedirs("metadata", exist_ok=True)
    filename = f"metadata/metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}.csv"
    fieldnames = ["Video ID", "Title", "Channel ID", "Author", "Description",
                  "Category", "Topics", "Length (Seconds)", "Published",
                  "Audio Language", "Views", "Tags"]

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def process_url(url):
    """Process a single YouTube URL and fetch/save metadata."""
    identifier, url_type = extract_identifier(url)
    
    if url_type == "playlist":
        video_ids = get_playlist_videos(identifier)
    elif url_type == "channel_id":
        video_ids = get_channel_videos(identifier)
    else:
        print(f"Unsupported URL type: {url_type}")
        return
    
    video_ids = check_existing_video_ids(video_ids)
    video_data = get_video_metadata(video_ids)
    save_to_csv(video_data)
