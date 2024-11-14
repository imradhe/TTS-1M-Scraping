import os
import shutil
import pandas as pd
from datetime import datetime

def load_csv_files_from_directory(directory):
    """Load all CSV files in the specified directory into a single DataFrame."""
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    if not all_files:
        print(f"No CSV files found in directory: {directory}")
        return pd.DataFrame()

    dataframes = [pd.read_csv(file) for file in all_files]
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

def deduplicate_dataframe(df, key_column="Video ID"):
    """Remove duplicates from the DataFrame based on a specific column."""
    if key_column not in df.columns:
        print(f"Column '{key_column}' not found in DataFrame.")
        return df
    
    # Remove duplicates
    before_dedup = len(df)
    deduplicated_df = df.drop_duplicates(subset=[key_column], keep='first').reset_index(drop=True)
    after_dedup = len(deduplicated_df)

    print(f"Removed {before_dedup - after_dedup} duplicate entries.")
    return deduplicated_df

def save_to_csv(df, output_directory="metadata"):
    """Save the deduplicated DataFrame to a CSV file with a timestamped filename."""
    os.makedirs(output_directory, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{output_directory}/deduplicated_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Deduplicated data saved to {filename}")
    return filename

def move_files_to_archive(input_directory, archive_directory):
    """Move all files from input_directory to archive_directory."""
    os.makedirs(archive_directory, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_subdir = os.path.join(archive_directory, f"archive_{timestamp}")
    os.makedirs(archive_subdir, exist_ok=True)

    for file in os.listdir(input_directory):
        file_path = os.path.join(input_directory, file)
        if os.path.isfile(file_path):
            shutil.move(file_path, os.path.join(archive_subdir, file))
    print(f"All files moved to archive: {archive_subdir}")

def main():
    # Specify the directories
    input_directory = "metadata"
    archive_directory = "archive"
    temp_directory = "deduplication"

    # Load CSV files from the directory
    df = load_csv_files_from_directory(input_directory)

    if df.empty:
        print("No data to deduplicate.")
        return

    # Deduplicate the DataFrame
    deduplicated_df = deduplicate_dataframe(df, key_column="Video ID")

    # Save the deduplicated DataFrame temporarily
    deduplicated_file = save_to_csv(deduplicated_df, output_directory=temp_directory)

    # Move original files to archive
    move_files_to_archive(input_directory, archive_directory)

    # Move the deduplicated file back to the metadata directory
    shutil.move(deduplicated_file, os.path.join(input_directory, os.path.basename(deduplicated_file)))
    print(f"Deduplicated file moved to {input_directory}")

    # Clean up the temporary directory
    if os.path.exists(temp_directory):
        shutil.rmtree(temp_directory)
        print(f"Temporary directory '{temp_directory}' removed.")

if __name__ == "__main__":
    main()
