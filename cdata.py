import os
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
from tqdm import tqdm

# Load the API key from the .env file
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

def initialize_youtube_api():
    """Initialize the YouTube Data API client."""
    if not YOUTUBE_API_KEY:
        print("Error: YouTube API key not found. Please set it in the .env file.")
        return None
    return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

def fetch_channel_metadata(youtube, channel_id):
    """Fetch channel metadata using the YouTube Data API."""
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()

        if "items" not in response or not response["items"]:
            return None

        channel_info = response["items"][0]
        channel_data = {
            "Channel ID": channel_id,
            "Channel Name": channel_info["snippet"]["title"],
            "Subscribers": int(channel_info["statistics"].get("subscriberCount", 0)),
            "Total Videos": int(channel_info["statistics"].get("videoCount", 0)),
            "Created Date": channel_info["snippet"]["publishedAt"].split("T")[0]
        }
        return channel_data
    except Exception as e:
        print(f"Error fetching channel data for Channel ID {channel_id}: {e}")
        return None

def load_existing_channel_metadata(file_path):
    """Load existing channel metadata from CSV file."""
    if os.path.exists(file_path):
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            print(f"Error reading existing channel metadata: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def load_csv_files_from_directory(directory):
    """Load and merge all CSV files from the specified directory."""
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    dataframes = []

    for file in tqdm(all_files, desc="Loading CSV Files"):
        try:
            df = pd.read_csv(file)
            df.columns = df.columns.str.strip()
            dataframes.append(df)
        except Exception as e:
            print(f"Error reading file {file}: {e}")
    
    if not dataframes:
        print("No valid CSV files found in the directory.")
        return pd.DataFrame()

    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

def extract_and_enrich_channel_data(df, youtube, existing_channels):
    """Extract unique channels and enrich with YouTube metadata."""
    if 'Channel ID' not in df.columns:
        print("Error: 'Channel ID' column not found in the CSV files.")
        return pd.DataFrame()
    
    # Calculate total hours from 'Length (Seconds)'
    df['Length (Hours)'] = df['Length (Seconds)'] / 3600

    # Group by Channel ID to aggregate existing data
    channel_data = df.groupby('Channel ID').agg(
        Total_Views=('Views', 'sum'),
        Total_Videos=('Video ID', 'nunique'),
        Total_Hours=('Length (Hours)', 'sum'),
        Tags=('Tags', lambda x: ', '.join(set(sum(x.dropna().str.split(',').tolist(), [])))),
        Topics=('Topics', lambda x: ', '.join(set(sum(x.dropna().str.split(',').tolist(), []))))
    ).reset_index()

    # Filter out channels that are already in the existing metadata
    new_channels = channel_data[~channel_data['Channel ID'].isin(existing_channels['Channel ID'])]

    enriched_data = []

    for _, row in tqdm(new_channels.iterrows(), total=len(new_channels), desc="Fetching YouTube Metadata"):
        channel_id = row['Channel ID']
        api_data = fetch_channel_metadata(youtube, channel_id)
        if api_data:
            row = {**row, **api_data}
        enriched_data.append(row)

    enriched_df = pd.DataFrame(enriched_data)

    # Ensure columns are in the correct order
    if not enriched_df.empty:
        enriched_df = enriched_df[[
            "Channel ID", "Channel Name", "Subscribers", "Total_Hours",
            "Total Videos", "Total_Views", "Tags", "Topics", "Created Date"
        ]]

    return enriched_df

def save_to_csv(df, output_file):
    """Save the DataFrame to a CSV file."""
    if not df.empty:
        try:
            if os.path.exists(output_file):
                # Append to the existing CSV file
                existing_df = pd.read_csv(output_file)
                df = pd.concat([existing_df, df]).drop_duplicates(subset=['Channel ID'], keep='last')
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"Channel metadata saved to '{output_file}'")
        except Exception as e:
            print(f"Error saving CSV file: {e}")

def main():
    metadata_dir = 'metadata'
    output_file = 'channel_metadata.csv'
    
    # Initialize YouTube API
    youtube = initialize_youtube_api()
    if not youtube:
        print("YouTube API initialization failed.")
        return

    # Load video data from all CSV files in the "metadata" directory
    print("Loading data from CSV files...")
    df = load_csv_files_from_directory(metadata_dir)
    if df.empty:
        print("No data available for processing.")
        return

    # Load existing channel metadata
    print("Loading existing channel metadata...")
    existing_channels = load_existing_channel_metadata(output_file)

    # Extract and enrich channel data
    print("Extracting and enriching channel data...")
    enriched_channel_data = extract_and_enrich_channel_data(df, youtube, existing_channels)
    if enriched_channel_data.empty:
        print("No new channel data generated.")
        return

    # Save enriched data to CSV
    print("Saving enriched data to CSV...")
    save_to_csv(enriched_channel_data, output_file)

if __name__ == "__main__":
    main()
