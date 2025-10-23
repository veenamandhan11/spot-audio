import os
import shutil
import glob
from pathlib import Path

def copy_pcm_files_to_desktop():
    """
    Copy all *_pcm.wav files from C:\temp to Desktop/xyz folder,
    renaming them from <aircheckId>_pcm.wav to <aircheckId>.wav
    """
    
    # Source and destination paths
    source_folder = r"C:\temp"
    desktop_path = Path.home() / "Desktop"
    destination_folder = desktop_path / "xyz"
    
    try:
        # Create destination folder if it doesn't exist
        destination_folder.mkdir(exist_ok=True)
        print(f"Destination folder: {destination_folder}")
        
        # Find all *_pcm.wav files in source folder
        pcm_pattern = os.path.join(source_folder, "*_pcm.wav")
        pcm_files = glob.glob(pcm_pattern)
        
        if not pcm_files:
            print(f"No *_pcm.wav files found in {source_folder}")
            return
        
        print(f"Found {len(pcm_files)} PCM files to copy...")
        
        copied_count = 0
        failed_count = 0
        
        for pcm_file in pcm_files:
            try:
                # Extract filename and remove _pcm suffix
                filename = os.path.basename(pcm_file)
                # Remove '_pcm.wav' and add '.wav'
                new_filename = filename.replace('_pcm.wav', '.wav')
                
                # Destination file path
                destination_file = destination_folder / new_filename
                
                # Copy the file
                shutil.copy2(pcm_file, destination_file)
                print(f"Copied: {filename} → {new_filename}")
                copied_count += 1
                
            except Exception as e:
                print(f"Failed to copy {filename}: {e}")
                failed_count += 1
        
        # Summary
        print(f"\n--- COPY SUMMARY ---")
        print(f"Total files found: {len(pcm_files)}")
        print(f"Successfully copied: {copied_count}")
        print(f"Failed: {failed_count}")
        print(f"Files copied to: {destination_folder}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    copy_pcm_files_to_desktop()