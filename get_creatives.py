import json
import os
import subprocess
import time
import concurrent.futures
from datetime import datetime

# Configuration
JSON_FILE_PATH = "./new_creatives.json"
USERNAME = "CSugrue"
PASSWORD = "Ussu8229"
TARGET_PATH = r"C:\temp"

# Constants
INI_PATH = r"C:\Program Files\Media Monitors"
GETMEDIA_EXE = r"C:\Program Files\Media Monitors\Getmedia.exe"
BATCH_SIZE = 10
BATCH_TIMEOUT = 60  # 1 minute per batch

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

def run_single_getmedia(aircheck_id):
    """
    Run Getmedia.exe for a single aircheck_id
    Returns aircheck_id if successful, None if failed
    """
    try:
        ini_filename = f"{aircheck_id}.ini"
        
        # Run the command
        cmd = [GETMEDIA_EXE, f"/f:{ini_filename}", "/s"]
        subprocess.run(cmd, capture_output=True, text=True, cwd=INI_PATH)
        
        return aircheck_id
        
    except Exception as e:
        return None

def check_batch_success(batch_aircheck_ids):
    """
    Check which aircheck_ids in the batch were successful based on PCM file existence
    Returns list of successful aircheck_ids and list of failed ones
    """
    successful = []
    failed = []
    
    for aircheck_id in batch_aircheck_ids:
        pcm_file = os.path.join(TARGET_PATH, f"{aircheck_id}_pcm.wav")
        if os.path.exists(pcm_file):
            successful.append(aircheck_id)
        else:
            failed.append(aircheck_id)
    
    return successful, failed

def process_batch(batch_creatives, batch_num, total_batches):
    """
    Process a batch of creatives in parallel
    Returns list of successful and failed aircheck_ids
    """
    batch_aircheck_ids = [creative["aircheck_id"] for creative in batch_creatives]
    
    print(f"Processing batch {batch_num}/{total_batches} ({len(batch_creatives)} items)...")
    
    # Run all processes in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        futures = {executor.submit(run_single_getmedia, aircheck_id): aircheck_id 
                  for aircheck_id in batch_aircheck_ids}
        
        # Wait for all to complete or timeout
        try:
            concurrent.futures.wait(futures, timeout=BATCH_TIMEOUT)
        except Exception as e:
            print(f"Batch {batch_num} timeout or error: {e}")
    
    # Check which ones were successful
    successful, failed = check_batch_success(batch_aircheck_ids)
    
    print(f"Batch {batch_num}/{total_batches} completed: {len(successful)}/{len(batch_creatives)} successful")
    
    return successful, failed

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
        print(f"Batch size: {BATCH_SIZE}, Batch timeout: {BATCH_TIMEOUT} seconds\n")
        
        # Create target directory if it doesn't exist
        os.makedirs(TARGET_PATH, exist_ok=True)
        
        creatives = data['creatives']
        total_count = len(creatives)
        
        # Create all .ini files first
        print("Creating all .ini files...")
        for creative in creatives:
            create_ini_file(creative)
        print(f"All {total_count} .ini files created successfully!\n")
        
        # Process in batches
        all_successful = []
        all_failed = []
        
        # Split into batches
        batches = [creatives[i:i + BATCH_SIZE] for i in range(0, total_count, BATCH_SIZE)]
        total_batches = len(batches)
        
        for batch_num, batch_creatives in enumerate(batches, 1):
            successful, failed = process_batch(batch_creatives, batch_num, total_batches)
            all_successful.extend(successful)
            all_failed.extend(failed)
        
        # Create failed creatives list with full details
        failed_creatives = []
        for creative in creatives:
            if creative["aircheck_id"] in all_failed:
                failed_creatives.append({
                    'aircheck_id': creative["aircheck_id"],
                    'creative_id': creative['creative_id'],
                    'name': creative['creative_name'],
                    'station': creative['station_id']
                })
        
        # Combine all .out files
        print("\nCombining output files...")
        combine_out_files(creatives)
        
        # Print summary
        print(f"\n--- FINAL SUMMARY ---")
        print(f"Total creatives: {total_count}")
        print(f"Successfully processed: {len(all_successful)}")
        print(f"Failed: {len(all_failed)}")
        
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