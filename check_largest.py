import os
from pathlib import Path

def find_largest_wav(folder_path):
    """Find the largest WAV file in the specified folder."""
    
    # Convert to Path object
    folder = Path(folder_path)
    
    # Check if folder exists
    if not folder.exists():
        print(f"Error: Folder '{folder_path}' does not exist.")
        return None
    
    # Find all .wav files
    wav_files = list(folder.glob("*.wav"))
    
    if not wav_files:
        print(f"No WAV files found in '{folder_path}'")
        return None
    
    # Find the largest file
    largest_file = None
    largest_size = 0
    
    for wav_file in wav_files:
        file_size = wav_file.stat().st_size
        if file_size > largest_size:
            largest_size = file_size
            largest_file = wav_file
    
    return largest_file, largest_size

# Main execution
if __name__ == "__main__":
    folder_path = r"C:\temp"
    
    result = find_largest_wav(folder_path)
    
    if result:
        largest_file, largest_size = result
        # Convert bytes to MB for readability
        size_mb = largest_size / (1024 * 1024)
        
        print(f"Largest WAV file: {largest_file.name}")
        print(f"Full path: {largest_file}")
        print(f"Size: {largest_size:,} bytes ({size_mb:.2f} MB)")