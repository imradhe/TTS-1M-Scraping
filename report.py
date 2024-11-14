import os
import pandas as pd
from datetime import datetime

def format_duration(seconds):
    """Automatically convert seconds to appropriate time units (milliseconds, seconds, minutes, hours)."""
    if seconds < 1:
        # Convert to milliseconds
        milliseconds = seconds * 1000
        return f"{milliseconds:.2f} milliseconds"
    elif seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"


def load_csv_files_from_directory(directory):
    """Load all CSV files in the specified directory into a single DataFrame."""
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    dataframes = []
    
    for file in all_files:
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip()  # Remove extra spaces in column names
        if 'Audio Language' not in df.columns:
            print(f"Warning: 'Audio Language' column not found in {file}. Skipping...")
            continue
        dataframes.append(df)
    
    if not dataframes:
        print("No valid CSV files with the required columns found.")
        return pd.DataFrame()
    
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

def analyze_total_duration(df):
    """Calculate total, min, max, and average duration of all videos."""
    total_seconds = df['Length (Seconds)'].sum()
    min_duration = df['Length (Seconds)'].min()
    max_duration = df['Length (Seconds)'].max()
    avg_duration = df['Length (Seconds)'].mean()
    
    print(f"Total Duration of All Videos: {format_duration(total_seconds)}")
    print(f"Min Duration: {format_duration(min_duration)}")
    print(f"Max Duration: {format_duration(max_duration)}")
    print(f"Average Duration: {format_duration(avg_duration)}")
    
    return total_seconds, min_duration, max_duration, avg_duration

def analyze_channel_and_video_count(df):
    """Analyze the total number of unique channels and videos."""
    total_videos = len(df)
    unique_channels = df['Channel ID'].nunique()
    return total_videos, unique_channels

def analyze_language_distribution(df):
    """Analyze the distribution of videos by language with duration metrics."""
    language_counts = df['Audio Language'].value_counts()
    language_duration = df.groupby('Audio Language')['Length (Seconds)']
    
    language_stats = language_duration.agg(['sum', 'min', 'max', 'mean'])
    language_stats['Total Duration'] = language_stats['sum'].apply(format_duration)
    language_stats['Min Duration'] = language_stats['min'].apply(format_duration)
    language_stats['Max Duration'] = language_stats['max'].apply(format_duration)
    language_stats['Avg Duration'] = language_stats['mean'].apply(format_duration)
    language_stats['Video Count'] = language_counts
    language_stats['Total Views'] = df.groupby('Audio Language')['Views'].sum()
    
    return language_stats.reset_index().rename(columns={'index': 'Audio Language'})

def analyze_domain_distribution(df):
    """Analyze the distribution of videos by domain (Category) with duration metrics."""
    domain_counts = df['Category'].value_counts()
    domain_duration = df.groupby('Category')['Length (Seconds)']
    
    domain_stats = domain_duration.agg(['sum', 'min', 'max', 'mean'])
    domain_stats['Total Duration'] = domain_stats['sum'].apply(format_duration)
    domain_stats['Min Duration'] = domain_stats['min'].apply(format_duration)
    domain_stats['Max Duration'] = domain_stats['max'].apply(format_duration)
    domain_stats['Avg Duration'] = domain_stats['mean'].apply(format_duration)
    domain_stats['Video Count'] = domain_counts
    domain_stats['Total Views'] = df.groupby('Category')['Views'].sum()
    
    return domain_stats.reset_index().rename(columns={'index': 'Category'})

def analyze_topic_distribution(df):
    """Analyze the distribution of videos by topic with duration metrics."""
    df['Topics'] = df['Topics'].fillna('').apply(lambda x: x.split(', ') if x else [])
    exploded_df = df.explode('Topics')
    
    topic_counts = exploded_df['Topics'].value_counts()
    topic_duration = exploded_df.groupby('Topics')['Length (Seconds)']
    
    topic_stats = topic_duration.agg(['sum', 'min', 'max', 'mean'])
    topic_stats['Total Duration'] = topic_stats['sum'].apply(format_duration)
    topic_stats['Min Duration'] = topic_stats['min'].apply(format_duration)
    topic_stats['Max Duration'] = topic_stats['max'].apply(format_duration)
    topic_stats['Avg Duration'] = topic_stats['mean'].apply(format_duration)
    topic_stats['Video Count'] = topic_counts
    topic_stats['Total Views'] = exploded_df.groupby('Topics')['Views'].sum()
    
    return topic_stats.reset_index().rename(columns={'index': 'Topic'})

def analyze_duplicates(df):
    """Analyze and count duplicate videos based on 'Video ID'."""
    duplicate_videos = df[df.duplicated(subset='Video ID', keep=False)]
    num_duplicates = len(duplicate_videos)
    unique_duplicates = duplicate_videos['Video ID'].nunique()
    
    print(f"Total Duplicate Entries: {num_duplicates}")
    print(f"Unique Duplicate Videos: {unique_duplicates}")
    
    return duplicate_videos, num_duplicates, unique_duplicates

def save_analysis_to_csv(language_analysis, domain_analysis, topic_analysis, total_seconds, total_videos, unique_channels, duplicates_df, num_duplicates, unique_duplicates, overall_stats, filename):
    """Save the analysis DataFrame to a CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        file.write(f"Total Duration of All Videos: {format_duration(total_seconds)}\n")
        file.write(f"Total Videos: {total_videos}\n")
        file.write(f"Unique Channels: {unique_channels}\n")
        file.write(f"Min Duration: {format_duration(overall_stats[1])}\n")
        file.write(f"Max Duration: {format_duration(overall_stats[2])}\n")
        file.write(f"Average Duration: {format_duration(overall_stats[3])}\n")
        file.write(f"Total Duplicate Entries: {num_duplicates}\n")
        file.write(f"Unique Duplicate Videos: {unique_duplicates}\n\n")

        file.write("Language Analysis:\n")
        language_analysis.to_csv(file, index=False)
        file.write("\n\n")

        file.write("Domain Analysis:\n")
        domain_analysis.to_csv(file, index=False)
        file.write("\n\n")

        file.write("Topic Analysis:\n")
        topic_analysis.to_csv(file, index=False)
        file.write("\n\n")
    
    print(f"Analysis saved to {filename}")

def main():
    # Load and merge all CSV files from the "metadata" directory
    metadata_dir = 'metadata'
    if not os.path.exists(metadata_dir):
        print(f"Directory '{metadata_dir}' does not exist.")
        return

    df = load_csv_files_from_directory(metadata_dir)
    
    if df.empty:
        print("No data available for analysis.")
        return

    overall_stats = analyze_total_duration(df)
    total_videos, unique_channels = analyze_channel_and_video_count(df)
    language_analysis = analyze_language_distribution(df)
    domain_analysis = analyze_domain_distribution(df)
    topic_analysis = analyze_topic_distribution(df)
    duplicates_df, num_duplicates, unique_duplicates = analyze_duplicates(df)

    output_filename = 'analysis.csv'
    save_analysis_to_csv(
        language_analysis,
        domain_analysis,
        topic_analysis,
        overall_stats[0],
        total_videos,
        unique_channels,
        duplicates_df,
        num_duplicates,
        unique_duplicates,
        overall_stats,
        output_filename
    )

if __name__ == "__main__":
    main()
