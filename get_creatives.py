import json
import os
import subprocess
import time
from datetime import datetime

# Configuration
JSON_FILE_PATH = "./new_creatives.json"
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"
TARGET_PATH = r"C:\temp"

# Constants
INI_PATH = r"C:\Program Files\Media Monitors"
GETMEDIA_EXE = r"C:\Program Files\Media Monitors\Getmedia.exe"
TIMEOUT_SECONDS = 90  # 1.5 minutes

def convert_time_format(time_str):
    """
    Convert time from '2025-10-18 04:47:22.000' format to '20251018-04:47:22' format
    """
    # Handle both formats: with and without milliseconds
    if '.' in time_str:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    else:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y%m%d-%H:%M:%S")

def create_ini_file(creative):
    """
    Create .ini file for a creative
    """
    aircheck_id = creative["aircheck_id"]
    station_id = creative["station_id"]
    start_time = convert_time_format(creative["start_time"])
    end_time = convert_time_format(creative["end_time"])
    
    ini_content = f"""/u:{USERNAME}
/p:{PASSWORD}
/w:https://data.mediamonitors.com/mmwebservices/
/r:{station_id}
/i:{aircheck_id}
/s:{start_time}
/e:{end_time}
/t:{TARGET_PATH}\\
/n:{aircheck_id}
/l"""
    
    ini_filename = f"{aircheck_id}.ini"
    ini_filepath = os.path.join(INI_PATH, ini_filename)
    
    with open(ini_filepath, 'w') as f:
        f.write(ini_content)
    
    return ini_filename

def wait_for_completion(aircheck_id):
    """
    Wait for processing to complete and check if PCM file was created
    Returns True if successful, False if failed
    """
    pcm_file = os.path.join(TARGET_PATH, f"{aircheck_id}_pcm.wav")
    
    # Wait up to TIMEOUT_SECONDS for the file to appear
    for _ in range(TIMEOUT_SECONDS):
        if os.path.exists(pcm_file):
            return True
        time.sleep(1)
    
    return False

def run_getmedia(creative):
    """
    Run Getmedia.exe with the specified creative
    Returns True if successful, False if failed
    """
    aircheck_id = creative["aircheck_id"]
    ini_filename = f"{aircheck_id}.ini"
    
    try:
        # Change to the Media Monitors directory
        os.chdir(INI_PATH)
        
        # Run the command
        cmd = [GETMEDIA_EXE, f"/f:{ini_filename}", "/s"]
        subprocess.run(cmd, capture_output=True, text=True)
        
        # Wait for completion and check if PCM file was created
        return wait_for_completion(aircheck_id)
        
    except Exception as e:
        return False

def combine_out_files(creatives):
    """
    Combine all .out files into a single formatted summary file
    """
    summary_file = os.path.join(TARGET_PATH, "processing_summary.txt")
    
    with open(summary_file, 'w') as summary:
        summary.write("=== MEDIA PROCESSING SUMMARY ===\n")
        summary.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        summary.write("=" * 50 + "\n\n")
        
        for creative in creatives:
            aircheck_id = creative["aircheck_id"]
            creative_id = creative["creative_id"]
            creative_name = creative["creative_name"]
            out_file = os.path.join(TARGET_PATH, f"{aircheck_id}.out")
            
            summary.write(f"Aircheck ID: {aircheck_id}\n")
            summary.write(f"Creative ID: {creative_id}\n")
            summary.write(f"Creative Name: {creative_name}\n")
            summary.write(f"Station ID: {creative['station_id']}\n")
            summary.write("-" * 30 + "\n")
            
            if os.path.exists(out_file):
                try:
                    with open(out_file, 'r') as f:
                        summary.write(f.read())
                except Exception as e:
                    summary.write(f"Error reading .out file: {e}")
            else:
                summary.write("No .out file found for this aircheck.")
            
            summary.write("\n" + "=" * 50 + "\n\n")
    
    print(f"Combined summary saved to: {summary_file}")

def main():
    try:
        # Read JSON file
        with open(JSON_FILE_PATH, 'r') as f:
            data = json.load(f)
        
        print(f"Processing {data['count']} creatives...")
        print(f"Test mode: {data['test_mode']}")
        print(f"Date range: {data['date_range']['start']} to {data['date_range']['end']}")
        print("Starting processing...\n")
        
        # Create target directory if it doesn't exist
        os.makedirs(TARGET_PATH, exist_ok=True)
        
        success_count = 0
        failed_creatives = []
        total_count = len(data['creatives'])
        
        for i, creative in enumerate(data['creatives'], 1):
            aircheck_id = creative["aircheck_id"]
            
            # Create .ini file
            ini_filename = create_ini_file(creative)
            
            # Run Getmedia.exe and check for success
            if run_getmedia(creative):
                success_count += 1
            else:
                failed_creatives.append({
                    'aircheck_id': aircheck_id,
                    'creative_id': creative['creative_id'],
                    'name': creative['creative_name'],
                    'station': creative['station_id']
                })
            
            # Show progress
            if i % 10 == 0 or i == total_count:
                print(f"Progress: {i}/{total_count} processed")
        
        # Combine all .out files
        print("\nCombining output files...")
        combine_out_files(data['creatives'])
        
        # Print summary
        print(f"\n--- FINAL SUMMARY ---")
        print(f"Total creatives: {total_count}")
        print(f"Successfully processed: {success_count}")
        print(f"Failed: {len(failed_creatives)}")
        
        if failed_creatives:
            print(f"\nFailed creatives:")
            for failed in failed_creatives:
                print(f"  - Aircheck: {failed['aircheck_id']} | Creative: {failed['creative_id']} ({failed['name']}) - Station {failed['station']}")
        
        print(f"\nAudio files saved to: {TARGET_PATH}")
        
    except FileNotFoundError:
        print(f"Error: JSON file not found at {JSON_FILE_PATH}")
        print("Please check the file path in the configuration section.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {JSON_FILE_PATH}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()