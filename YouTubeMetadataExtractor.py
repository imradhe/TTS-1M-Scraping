import os
import re
import time
import random
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

class YouTubeMetadataExtractor:
    def __init__(self, api_key_file='api_keys.txt', input_urls_file='input_urls.txt', metadata_file='metadata.csv', final_file='videos.csv'):
        self.api_key_file = api_key_file
        self.input_urls_file = input_urls_file
        self.metadata_file = metadata_file
        self.final_file = final_file
        self.api_keys = self.load_api_keys()
        self.current_key_index = 0
        self.youtube = self.create_youtube_service()

    def load_api_keys(self):
        """Load API keys from a file."""
        with open(self.api_key_file, 'r') as f:
            return f.read().splitlines()

    def create_youtube_service(self):
        """Create YouTube API service using the current API key."""
        return build('youtube', 'v3', developerKey=self.api_keys[self.current_key_index])

    def switch_api_key(self):
        """Switch to the next API key when rate limit is exceeded."""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        if self.current_key_index == 0:
            print("All API keys exhausted. Exiting...")
            exit(1)
        self.youtube = self.create_youtube_service()
        print(f"Switched to API key {self.api_keys[self.current_key_index]}")

    def parse_url(self, url):
        """Parse the YouTube URL and determine its type (video, playlist, or channel)."""
        if 'youtube.com/watch' in url or 'youtu.be' in url:
            return self.parse_video_url(url)
        elif 'youtube.com/playlist' in url:
            return self.parse_playlist_url(url)
        elif 'youtube.com/channel' in url or '@' in url:
            return self.parse_channel_url(url)
        return None

    def parse_video_url(self, url):
        """Parse a video URL and return the video ID."""
        if 'youtu.be' in url:
            return {'type': 'video', 'video_id': url.split('/')[-1]}
        match = re.search(r'[?&]v=([a-zA-Z0-9_-]+)', url)
        if match:
            return {'type': 'video', 'video_id': match.group(1)}
        return None

    def parse_playlist_url(self, url):
        """Parse a playlist URL and return the playlist ID."""
        match = re.search(r'list=([a-zA-Z0-9_-]+)', url)
        return {'type': 'playlist', 'playlist_id': match.group(1)} if match else None

    def parse_channel_url(self, url):
        """Parse a channel URL and return the channel ID."""
        if '/channel/' in url:
            return {'type': 'channel', 'channel_id': url.split('/channel/')[-1]}
        elif '/@' in url:
            return {'type': 'channel', 'channel_id': url.split('/@')[-1]}
        return None

    def exponential_backoff(self, retries):
        """Implement exponential backoff."""
        wait_time = min(60, 2 ** retries)
        print(f"Rate limit hit. Retrying in {wait_time} seconds...")
        time.sleep(wait_time)

    def fetch_videos_from_channel(self, channel_id):
        """Fetch all video IDs from a channel using its uploads playlist."""
        try:
            print(f"Fetching videos for playlist ID {channel_id}...")
            # Step 1: Get the uploads playlist ID for the channel
            request = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            )
            response = request.execute()

            if 'items' in response and len(response['items']) > 0:
                uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            else:
                print(f"No uploads playlist found for channel ID {channel_id}.")
                return []

            # Step 2: Fetch all video IDs from the uploads playlist
            return self.fetch_videos_from_playlist(uploads_playlist_id)
        
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in e.content.decode('utf-8'):
                print(f"Quota exceeded for API key {self.api_keys[self.current_key_index]}. Switching to the next key...")
                self.switch_api_key()
                return self.fetch_videos_from_channel(channel_id)
            else:
                print(f"Error fetching videos for channel {channel_id}: {e}")
        return []

    def fetch_videos_from_playlist(self, playlist_id):
        """Fetch all video IDs from a playlist."""
        video_ids = []
        next_page_token = None
        retries = 0

        while True:
            try:
                print(f"Fetching videos for channel ID {playlist_id}...")
                request = self.youtube.playlistItems().list(
                    part='snippet',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                # Extract video IDs from the playlist items
                for item in response.get('items', []):
                    video_ids.append(item['snippet']['resourceId']['videoId'])

                # Check if there's a next page
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            except HttpError as e:
                if e.resp.status == 403 and 'quotaExceeded' in e.content.decode('utf-8'):
                    print(f"Quota exceeded for API key {self.api_keys[self.current_key_index]}. Switching to the next key...")
                    self.switch_api_key()
                    retries += 1
                    if retries > len(self.api_keys):
                        print("All API keys exhausted. Exiting...")
                        exit(1)
                else:
                    print(f"Error fetching videos from playlist {playlist_id}: {e}")
                    break

        return video_ids

    def fetch_video_metadata_batch(self, video_ids):
        """Fetch metadata for multiple videos using batch requests."""
        try:
            request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails,topicDetails',
                id=','.join(video_ids)
            )
            response = request.execute()
            metadata_list = []
            for item in response.get('items', []):
                metadata_list.append({
                    'Video ID': item['id'],
                    'Title': item['snippet']['title'],
                    'Channel ID': item['snippet']['channelId'],
                    'Author': item['snippet']['channelTitle'],
                    'Description': item['snippet'].get('description', ''),
                    'Category': item['snippet'].get('categoryId', ''),
                    'Topics': item.get('topicDetails', {}).get('topicCategories', []),
                    'Length (Seconds)': self.parse_duration(item['contentDetails']['duration']),
                    'Published': item['snippet']['publishedAt'],
                    'Audio Language': item['snippet'].get('defaultAudioLanguage', ''),
                    'Views': item['statistics'].get('viewCount', 0),
                    'Tags': item['snippet'].get('tags', [])
                })
            return metadata_list
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in e.content.decode('utf-8'):
                self.switch_api_key()
            else:
                print(f"Error fetching metadata: {e}")
        return []

    def parse_duration(self, duration):
        """Convert YouTube ISO 8601 duration format to seconds."""
        if not duration:
            return 0  # Return 0 if duration is None or empty
        
        # Match the ISO 8601 duration format (e.g., PT1H2M3S)
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if not match:
            print(f"Invalid duration format: {duration}")
            return 0  # Return 0 if the format is incorrect

        # Extract hours, minutes, and seconds from the match groups
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        return hours * 3600 + minutes * 60 + seconds


    def load_urls_from_file(self):
        """Load URLs from the input file."""
        with open(self.input_urls_file, 'r') as f:
            return f.read().splitlines()

    def get_existing_video_ids(self):
        """Get existing video IDs from the CSV file."""
        if os.path.exists(self.metadata_file):
            df = pd.read_csv(self.metadata_file)
            return set(df['Video ID'].tolist())
        return set()

    def save_metadata_to_csv(self, metadata):
        """Save metadata to CSV."""
        df = pd.DataFrame(metadata)
        df.to_csv(self.metadata_file, index=False, mode='a', header=not os.path.exists(self.metadata_file))

    def deduplicate_metadata(self):
        """Deduplicate metadata and save the final results."""
        df = pd.read_csv(self.metadata_file)
        df.drop_duplicates(subset='Video ID', keep='first', inplace=True)
        df.to_csv(self.final_file, index=False)
        print(f"Deduplicated metadata saved to {self.final_file}")

    def report(self):
        """Generate a report on the fetched metadata."""
        df = pd.read_csv(self.final_file)
        num_videos = len(df)
        num_channels = len(df['Channel ID'].unique())
        total_hours = df['Length (Seconds)'].sum() / 3600

        print(f"Number of videos: {num_videos}")
        print(f"Number of unique channels: {num_channels}")
        print(f"Total views: {total_hours:.2f} hours")

        

    def process_urls(self):
        """Process URLs, fetch video metadata, and save results."""
        urls = self.load_urls_from_file()
        existing_video_ids = self.get_existing_video_ids()
        all_video_ids = []

        # Step 1: Parse URLs and gather video IDs
        for url in tqdm(urls, desc="Processing URLs"):
            url_info = self.parse_url(url)
            if url_info:
                if url_info['type'] == 'video' and url_info['video_id'] not in existing_video_ids:
                    all_video_ids.append(url_info['video_id'])
                elif url_info['type'] == 'playlist':
                    all_video_ids.extend(self.fetch_videos_from_playlist(url_info['playlist_id']))
                elif url_info['type'] == 'channel':
                    all_video_ids.extend(self.fetch_videos_from_channel(url_info['channel_id']))

        # Step 2: Remove duplicates and filter out existing video IDs
        all_video_ids = list(set(all_video_ids) - existing_video_ids)

        # Step 3: Fetch video metadata in batches
        metadata = []
        batch_size = 50
        for i in tqdm(range(0, len(all_video_ids), batch_size), desc="Fetching video metadata"):
            batch_ids = all_video_ids[i:i + batch_size]
            metadata.extend(self.fetch_video_metadata_batch(batch_ids))

        # Step 4: Save metadata to CSV
        self.save_metadata_to_csv(metadata)
        print(f"Metadata saved to {self.metadata_file}")

        # Step 5: Deduplicate metadata
        self.deduplicate_metadata()

        # Step 6: Generate a report
        self.report()